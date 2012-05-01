package com.google.code.hadoopproxy.server;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class ProxyResultReceiver extends Thread {
	
	private static final Log LOG = LogFactory.getLog(ProxyResultReceiver.class);

	public final static int RESULT_RECV_SOCKET_PORT = ProxyServer.COMMAND_SOCKET_PORT + 1;

	private ServerSocket resultRecvSocket = null;
	
	private Map<String, DataOutputStream> clientOutMap = new HashMap<String, DataOutputStream>();
	
	public final static String STATUS_RESULT = "result";
	
	public final static String STATUS_END = "end";
	
	public final static String END_OF_STREAM = "  END_OF_STREAM_23:07-239$#P0442@M";

	public ProxyResultReceiver() throws IOException {
		this.resultRecvSocket = new ServerSocket(RESULT_RECV_SOCKET_PORT);
	}

	public void run() {
		try {
			while (true) {
				Socket taskOutputSocket = resultRecvSocket.accept();
				ResultReceiverThread childThread = new ResultReceiverThread(taskOutputSocket);
				childThread.start();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public synchronized void writeResult(String proxyJobID, String line) throws IOException {
		DataOutputStream dos = clientOutMap.get(proxyJobID);
		if (dos != null) {
			dos.writeUTF(STATUS_RESULT);
			dos.writeUTF(line);
		}
	}
	
	public synchronized void addClientOut(String proxyJobID, DataOutputStream dos) {
		clientOutMap.put(proxyJobID, dos);
	}
	
	public synchronized void removeClientOut(String proxyJobID) throws IOException {
		DataOutputStream dos = clientOutMap.get(proxyJobID);
		if (dos != null) {
			dos.writeUTF(STATUS_END);
			dos.flush();
		}
		clientOutMap.remove(proxyJobID);
	}
	
	
	/**
	 * 
	 * @author Liang
	 *
	 */
	class ResultReceiverThread extends Thread {
		
		Socket taskSocket = null;

		public ResultReceiverThread(Socket taskSocket) {
			this.taskSocket = taskSocket;
		}

		public void run() {
			try {
				BufferedReader reader = new BufferedReader(new InputStreamReader(taskSocket.getInputStream()));				
				String proxyJobID = reader.readLine();
				LOG.info("Received a task result sender for proxy job :"+proxyJobID);
				while(true) {
					String line = reader.readLine();
					if (line.equals(END_OF_STREAM)) {
						break;
					}
					writeResult(proxyJobID, line);
					LOG.info("Message : "+line);
				}
				LOG.info("Exit from a task result sender for proxy job :"+proxyJobID);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}finally {
				try {
					this.taskSocket.close();
					this.taskSocket = null;
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}			
		}

	}

}
