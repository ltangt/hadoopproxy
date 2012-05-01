package com.google.code.hadoopproxy;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.code.hadoopproxy.server.ProxyResultReceiver;
import com.google.code.hadoopproxy.server.ProxyServer;
import com.google.code.hadoopproxy.util.DateUtil;
import com.google.code.hadoopproxy.util.FileUtil;
import com.google.code.hadoopproxy.util.JarUtil;
import com.google.code.hadoopproxy.util.SerializationUtil;


public class ProxyClient {
	
	private static final Log LOG = LogFactory.getLog(ProxyClient.class);
	
	private String serverName = null;
	
	private List<ProxyTask> taskList = new ArrayList<ProxyTask>();
	
	private String[] classPaths = null;
		
	public ProxyClient(String serverName, String[] classPaths) {
		this.serverName = serverName;
		this.classPaths = classPaths;
	}

	public void addTask(ProxyTask t) {
		// TODO Auto-generated method stub
		taskList.add(t);
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
			JarUtil.createJar(jarTmpName, classPaths);
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
			
			// Run job
			dos.writeUTF(ProxyServer.CMD_RUNJOB);
			dos.writeUTF(jarTmpName);
			dos.writeUTF(taskObjTmpName);
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
