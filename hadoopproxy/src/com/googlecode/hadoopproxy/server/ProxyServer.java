package com.googlecode.hadoopproxy.server;

import java.io.BufferedOutputStream;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.PatternLayout;

import com.googlecode.hadoopproxy.util.ClassLoaderUtil;
import com.googlecode.hadoopproxy.util.DateUtil;


public class ProxyServer {
	
	private static final Log LOG = LogFactory.getLog(ProxyServer.class);
	
	public final static int COMMAND_SOCKET_PORT = 38588;
		
	public final static String TASKLIST_RESULTFILENAME = "proxy_server_tasklist_results.txt";
	
	private ServerSocket commandSocket = null;
	
	private ProxyResultReceiver retReceiver = null;
		
	
	public final static String CMD_EXIT = "exit";
	
	public final static String CMD_SENDJAR = "sendjar";
	
	public final static String CMD_SENDTASKLIST = "sendtasklist";
	
	public final static String CMD_RUNJOB = "runjob";
	
	public final static String RESPONSE_SUCCESS = "sucesss";
	
	public final static String REPSONSE_FAILED = "failed";
	
	
	public ProxyServer() {
		
	}
	
	public void run() throws Exception {
		commandSocket = new ServerSocket(COMMAND_SOCKET_PORT);
		LOG.info("Hadoop Proxy server is started...");
		
		// Start the result receiver server
		retReceiver = new ProxyResultReceiver();
		retReceiver.start();
		
		
		// Start the command server
		boolean bRunning = true;
		while(bRunning) {
			Socket clientSocket = commandSocket.accept();
			LOG.info(" Accepted a client from "+ clientSocket.getInetAddress());
			DataInputStream dis = new DataInputStream(clientSocket.getInputStream());
			DataOutputStream dos = new DataOutputStream(clientSocket.getOutputStream());
			
			// Read commands and execute
			executeCommands(dis, dos);
			
			clientSocket.close();
			LOG.info(" Closed the client from "+ clientSocket.getInetAddress());
		}
	}
	
	private void executeCommands(DataInputStream dis, DataOutputStream dos) throws Exception {
		boolean alive = true;
		while(alive) {
			String command = dis.readUTF();
			LOG.info("command : "+command+" ...");
			if (command.equals(CMD_EXIT)) {
				alive = false;
			}
			else if (command.equals(CMD_SENDJAR)) {
				command_SendJar(dis, dos);
			}
			else if (command.equals(CMD_SENDTASKLIST)) {
				command_SendTaskList(dis, dos);
			}
			else if (command.equals(CMD_RUNJOB)) {
				command_RunJob(dis, dos);
			}
			else {
				LOG.error("Unknown command : "+command);
			}
			dos.flush();
		}
	}
	
	private void command_SendJar(DataInputStream dis, DataOutputStream dos) throws IOException {
		String jarFileName = dis.readUTF();
		int jarFileSize = dis.readInt();
		byte[] jarBuf = new byte[8*1024];
		int readSize = 0;
		FileOutputStream fos = new FileOutputStream(jarFileName);
		BufferedOutputStream bos = new BufferedOutputStream(fos);
		while(readSize < jarFileSize) {
			int remainSize = jarFileSize - readSize;
			int size = dis.read(jarBuf, 0, Math.min(remainSize, jarBuf.length));
			bos.write(jarBuf, 0, size);
			readSize += size;
		}
		bos.close();
		fos.close();
		
		// Add this jar into the system class loader
		try {
			ClassLoaderUtil.addJarToSystemClassLoader(jarFileName);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			dos.writeUTF(REPSONSE_FAILED);
			return;
		}
		
		dos.writeUTF(RESPONSE_SUCCESS);
	}
	
	private void command_SendTaskList(DataInputStream dis, DataOutputStream dos) throws IOException {
		String taskListFileName = dis.readUTF();
		FileOutputStream fos = new FileOutputStream(taskListFileName);
		BufferedOutputStream bos = new BufferedOutputStream(fos);
		DataOutputStream diskOut = new DataOutputStream(bos);
		byte[] buf = new byte[8*1024];
		int numTask = dis.readInt();
		diskOut.writeInt(numTask);
		for (int i=0; i<numTask; i++) {
			int objSize = dis.readInt();
			diskOut.writeInt(objSize);
			int readSize = 0;
			while(readSize < objSize) {
				int remainSize = objSize - readSize;
				int size = dis.read(buf, 0, Math.min(remainSize, buf.length));
				diskOut.write(buf, 0, size);
				readSize += size;
			}
		}
		diskOut.close();
		bos.close();
		fos.close();
		dos.writeUTF(RESPONSE_SUCCESS);
	}
	
	private void command_RunJob(DataInputStream dis, DataOutputStream dos) throws Exception {
		String clientJarName = dis.readUTF();
		String taskListFileName = dis.readUTF();
		String proxyJobID = "PROXYJOB"+DateUtil.createLongID();		
		LOG.info("[proxyJobID]="+proxyJobID+" [client jar]="+clientJarName+", [task file]="+taskListFileName);
		
		// Add this job into result receiver server
		retReceiver.addClientOut(proxyJobID, dos);
		
		// Execute the job in hadoop
		ProxyHadoopJob.executeJob(clientJarName, taskListFileName, proxyJobID);
		
		// Remove from result receiver server
		retReceiver.removeClientOut(proxyJobID);
				
		dos.writeUTF(RESPONSE_SUCCESS);
		
	}
	
	

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub				
		ProxyServer server = new ProxyServer();
		try {
			server.run();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
