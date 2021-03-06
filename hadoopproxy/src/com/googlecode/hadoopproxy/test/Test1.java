package com.googlecode.hadoopproxy.test;

import java.io.BufferedWriter;

import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;

import com.googlecode.hadoopproxy.ProxyClient;
import com.googlecode.hadoopproxy.ProxyTask;
import com.googlecode.hadoopproxy.server.TaskWritable;
import com.googlecode.hadoopproxy.util.IPAddressUtil;


public class Test1 {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			
			ProxyClient proxy = new ProxyClient("datamining-node01.cs.fiu.edu", new String[]{"bin"});
			proxy.addTask(new TestTask("a b c d e f"));
			proxy.addTask(new TestTask("ab cd ef gf"));
			proxy.addTask(new TestTask("abcc aaascd efee gfdssssf"));
			proxy.addTask(new TestTask("11 33 22 33 11 22 33 44 55"));
			proxy.execute();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static class TestTask implements ProxyTask {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		
		private String line = null;
		
		public TestTask(String line) {
			this.line = line;
		}

		public void run(PrintStream out) throws InterruptedException {
			// TODO Auto-generated method stub
			String[] tokens = line.split("\\s");
			line = line.replaceAll("\\s", "==");
			out.println(IPAddressUtil.getIPAddress()+" : "+line);
			out.println("Num. of Tokens : "+tokens.length);
		}
		
	}

}
