package com.googlecode.hadoopproxy;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.googlecode.hadoopproxy.server.ProxyResultReceiver;
import com.googlecode.hadoopproxy.server.ProxyServer;
import com.googlecode.hadoopproxy.util.DateUtil;
import com.googlecode.hadoopproxy.util.FileUtil;
import com.googlecode.hadoopproxy.util.JarUtil;
import com.googlecode.hadoopproxy.util.SerializationUtil;

/**
 * 
 * 
 *  Copyright  2012, FIU 
 *  
 *  @author Liang Tang
 *
 *  @version Created: May 1, 2012 8:32:34 PM
 */
public class ProxyClient {
	
	private static final Log LOG = LogFactory.getLog(ProxyClient.class);
	
	private String serverName = null;
	
	private List<ProxyTask> taskList = new ArrayList<ProxyTask>();
	
	private String[] classPaths = null;
	
	private List<File> tmpFiles = new ArrayList<File>();
	
	public ProxyClient(String serverName) {
		this(serverName, new String[]{"bin","build"});
	}
		
	public ProxyClient(String serverName, String[] classPaths) {
		this.serverName = serverName;
		this.classPaths = classPaths;
	}

	public void addTask(ProxyTask t) {
		// TODO Auto-generated method stub
		taskList.add(t);
	}
	
	public void addTmpFile(File file) {
		tmpFiles.add(file);
	}
	
	public void addTmpFile(String fileName) throws FileNotFoundException {
		File file = new File(fileName);
		if (file.exists() == false) {
			throw new FileNotFoundException(fileName);
		}
		addTmpFile(file);
	}
	
	public void addTmpFiles(String[] fileNames) throws FileNotFoundException {
		for (String fileName : fileNames) {
			addTmpFile(fileName);
		}
	}

	public void execute() throws IOException {
		String serverResponse;
		
		// Connect the proxy server
		Socket clientSocket = new Socket(serverName, ProxyServer.COMMAND_SOCKET_PORT);
		DataOutputStream dos = new DataOutputStream(clientSocket.getOutputStream());
		DataInputStream dis = new DataInputStream(clientSocket.getInputStream());
		LOG.info("Connected to proxy server : "+serverName);
		
		try {
			// Create the jar file
			String dateId = DateUtil.createID();
			String jarTmpName = "proxyclient_"+dateId+".jar";
			String[] excludePackages = new String[] {
					"com.googlecode.hadoopproxy.server",
					"com.googlecode.hadoopproxy.util",
			};
			JarUtil.createJar(jarTmpName, classPaths, excludePackages);
			LOG.info("Created jar file : "+jarTmpName);
			
			// Send jar file
			dos.writeUTF(ProxyServer.CMD_SENDJAR);
			byte[] jarBuf = FileUtil.readFully(jarTmpName);
			dos.writeUTF(jarTmpName);
			dos.writeInt(jarBuf.length);
			dos.write(jarBuf);
			serverResponse = dis.readUTF();
			if (serverResponse.equals(ProxyServer.RESPONSE_SUCCESS) == false) {
				LOG.info("Sent jar file failed.");
				throw new Exception("Sent jar file failed.");
			}
			LOG.info("Sent jar file in bytes : "+jarBuf.length);
			
			// Send task serialization data
			dos.writeUTF(ProxyServer.CMD_SENDTASKLIST);
			String taskObjTmpName = "proxytasks_"+dateId+".bin";
			dos.writeUTF(taskObjTmpName);
			dos.writeInt(taskList.size());
			int totalProxyBufSize = 0;
			for (ProxyTask t : taskList) {
				byte[] objBuf = SerializationUtil.toBytes(t);
				dos.writeInt(objBuf.length);
				dos.write(objBuf);
				totalProxyBufSize += objBuf.length;
			}
			serverResponse = dis.readUTF();
			if (serverResponse.equals(ProxyServer.RESPONSE_SUCCESS) == false) {
				LOG.info("Sent task list failed.");
				throw new Exception("Sent task list failed.");
			}
			LOG.info("Sent "+taskList.size()+" proxy task objects of "+totalProxyBufSize+" bytes.");
			
			// Send temporal files
			dos.writeUTF(ProxyServer.CMD_SENDTMPFILES);
			dos.writeInt(tmpFiles.size());
			long totalTmpFileSize = 0;
			for (int i=0; i<tmpFiles.size(); i++) {
				File tmpFile = tmpFiles.get(i);
				dos.writeUTF(tmpFile.getName());
				long fileLength = FileUtil.getFileLength(tmpFile);
				dos.writeLong(fileLength);
				FileInputStream fos = new FileInputStream(tmpFile);				
				FileUtil.copyBuffer(fos, dos);
				fos.close();
				totalTmpFileSize += fileLength;
			}
			serverResponse = dis.readUTF();
			if (serverResponse.equals(ProxyServer.RESPONSE_SUCCESS) == false) {
				LOG.info("Sent tmp files failed.");
				throw new Exception("Sent tmp files failed.");
			}
			LOG.info("Sent "+tmpFiles.size()+" tmp files of "+totalTmpFileSize+" bytes.");
			
			// Run job
			dos.writeUTF(ProxyServer.CMD_RUNJOB);
			dos.writeUTF(jarTmpName);
			dos.writeUTF(taskObjTmpName);
			dos.writeInt(tmpFiles.size());
			for (int i=0; i<tmpFiles.size(); i++) {
				dos.writeUTF(tmpFiles.get(i).getName());
			}
			LOG.info("Started to run hadoop job");
			
			// Print intermediate output from each task
			while(true) {
				String status = dis.readUTF();
				if (status.equals(ProxyResultReceiver.STATUS_RESULT)) {
					String result = dis.readUTF();
					System.out.println(result);
					LOG.info(result);
				}
				else if (status.equals(ProxyResultReceiver.STATUS_END)) {
					LOG.info(status);
					break;
				}
				else {
					LOG.error("Unknown status from task : "+status);
					break;
				}
			}
			
			serverResponse = dis.readUTF();
			if (serverResponse.equals(ProxyServer.RESPONSE_SUCCESS) == false) {
				LOG.info("running hadoop job failed");
				throw new Exception("running hadoop job failed");
			}
			LOG.info("finished the hadoop job");
			
			// Exit
			dos.writeUTF(ProxyServer.CMD_EXIT);
			dos.flush();
			LOG.info("exit.");
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			dos.close();
			dis.close();
			clientSocket.close();
		}
	}

}
