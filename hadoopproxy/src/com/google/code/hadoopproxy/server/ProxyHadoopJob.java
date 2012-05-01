package com.google.code.hadoopproxy.server;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.PrintStream;
import java.net.Socket;

import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.google.code.hadoopproxy.ProxyTask;
import com.google.code.hadoopproxy.util.IPAddressUtil;
import com.google.code.hadoopproxy.util.SerializationUtil;


public class ProxyHadoopJob {
	
	private static final Log LOG = LogFactory.getLog(ProxyHadoopJob.class);

	public ProxyHadoopJob() {

	}

	public static class Map extends Mapper<LongWritable, TaskWritable, LongWritable, Text> {

		public void map(LongWritable key, TaskWritable value, Context context) throws IOException, InterruptedException {
			ProxyTask t = value.getTask();
			// Create the socket to master server
			String masterServerName = value.getMasterServerName();
			String proxyJobID = value.getProxyJobID();
			
			// Connect to the result receiver socket
			Socket taskSocket = new Socket(masterServerName, ProxyResultReceiver.RESULT_RECV_SOCKET_PORT);
			PrintStream ps = new PrintStream(taskSocket.getOutputStream(), true);
			
			// Send the proxy job ID
			ps.println(proxyJobID);
			
			// Run this task thread
			t.run(ps);
			
			// Send the end of stream 
			ps.println(ProxyResultReceiver.END_OF_STREAM);
			taskSocket.close();
			
			context.write(key, new Text("success"));
		}
		
		
	}

	public static class Reduce extends Reducer<LongWritable, Text, LongWritable, Text> {

		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException,
				InterruptedException {
			Text result = values.iterator().next();
			context.write(key, result);
		}
	}

	

	public static void executeJob(String clientJarFileName, String taskObjFileName, String proxyJobID) throws Exception {
		// Read proxy task objects
		LOG.info("Prepare to lanuch the Hadoop job.");
		
		// Fetch the client jar sent from the client
		File clientJarFile = new File(clientJarFileName);
		if (clientJarFile.exists() == false) {
			LOG.error("Client Jar File : "+clientJarFileName + " does not exist!");
			throw new FileNotFoundException("Client Jar File : "+clientJarFileName + " does not exist!");
		}
		LOG.info("Fetch the client jar sent from the client : "+clientJarFile.getAbsolutePath());
		
//		File jobJarFile = new File("hadoopproxyserver.jar");
//		if (jobJarFile.exists() == false) {
//			LOG.error("Job Jar File : hadoopproxyserver.jar does not exist!");
//			throw new FileNotFoundException("Job Jar File : hadoopproxyserver.jar does not exist!");
//		}
		
		// Create the Hadoop Job
		JobConf conf = new JobConf(ProxyHadoopJob.class);
		Job job = new Job(conf, clientJarFileName);
		// conf.setJar(jobJarFile.getAbsolutePath());
		conf.set("tmpjars", clientJarFile.getAbsolutePath());

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(ProxyTaskInputFormat.class);
		job.setOutputFormatClass(ProxyTaskOutputFormat.class);
		
		// Set the task object serialized file name
		ProxyTaskInputFormat.setTaskObjFileName(job, taskObjFileName);
		ProxyTaskInputFormat.setMasterServerName(job, IPAddressUtil.getIPAddress());
		ProxyTaskInputFormat.setProxyJobID(job, proxyJobID);
		
		LOG.info("Hadoop job is started.");
		job.waitForCompletion(true);
		LOG.info("Hadoop job is completed.");
	}

}
