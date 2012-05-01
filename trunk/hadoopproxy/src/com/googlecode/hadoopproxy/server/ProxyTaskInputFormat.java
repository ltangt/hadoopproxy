package com.googlecode.hadoopproxy.server;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.FileInputStream;
import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.googlecode.hadoopproxy.ProxyTask;
import com.googlecode.hadoopproxy.util.ClassLoaderUtil;
import com.googlecode.hadoopproxy.util.SerializationUtil;


public class ProxyTaskInputFormat extends InputFormat<LongWritable, TaskWritable> {
	
	private static final Log LOG = LogFactory.getLog(ProxyTaskInputFormat.class);
	
	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String masterServerName = getMasterServerName(context);
		String proxyJobID = getProxyJobID(context);
		String taskObjFileName = getTaskObjFileName(context);
		LOG.info("Loaded the serialized task object file : "+taskObjFileName);
		List<ProxyTask> taskList = readTaskList(context, taskObjFileName);
		LOG.info("Loaded the task list consisting of  "+taskList.size()+" proxy tasks.");
		List<InputSplit> inputSplits = new ArrayList<InputSplit>();
		for (int i=0; i<taskList.size(); i++) {
			TaskWritable task = new TaskWritable(taskList.get(i), masterServerName, proxyJobID);
			inputSplits.add(new ProxyTaskInputSplit(i, task));
		}
		return inputSplits;
	}

	@Override
	public RecordReader<LongWritable, TaskWritable> createRecordReader( InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return new ProxyTaskRecordReader();
	}
	
	public static void setProxyJobID(Job job, String proxyJobID) {
		Configuration conf = job.getConfiguration();
		conf.set("mapred.hadoopproxy.proxyjobid", proxyJobID);
	}
	
	public static String getProxyJobID(JobContext context) {
		return context.getConfiguration().get("mapred.hadoopproxy.proxyjobid", "");	
	}
	
	public static void setMasterServerName(Job job, String masterServerName) {
		Configuration conf = job.getConfiguration();
		conf.set("mapred.hadoopproxy.input.masterserver", masterServerName);
	}
	
	public static String getMasterServerName(JobContext context) {
		return context.getConfiguration().get("mapred.hadoopproxy.input.masterserver", "");	
	}
	
	public static void setTaskObjFileName(Job job, String taskObjFileName) {
		Configuration conf = job.getConfiguration();
		conf.set("mapred.hadoopproxy.input.task", taskObjFileName);
		LOG.info("mapred.hadoopproxy.input.task="+taskObjFileName);
	}
	
	public static String getTaskObjFileName(JobContext context) {
		return context.getConfiguration().get("mapred.hadoopproxy.input.task", "");		
	}
	
	private static List<ProxyTask> readTaskList(JobContext context, String diskFileName) throws IOException {
		DataInputStream dis = new DataInputStream(new FileInputStream(diskFileName));
		int numTask = dis.readInt();
		List<ProxyTask> taskList = new ArrayList<ProxyTask>(numTask);
		try {			
			for (int i=0; i<numTask; i++) {
				int objSize = dis.readInt();
				int readSize = 0;
				byte[] objBuf = new byte[objSize];
				while(readSize < objSize) {
					int remainSize = objSize - readSize;
					int size = dis.read(objBuf, readSize, remainSize);
					readSize += size;				
				}
				ProxyTask task = (ProxyTask)SerializationUtil.toObject(objBuf);
				taskList.add(task);
			}
		}catch(ClassNotFoundException e) {
			e.printStackTrace();
		}finally {
			dis.close();
		}
		return taskList;
	}
	
		
	/**
	 * An InputSplit that stores a hash key
	 * @author ltang002
	 *
	 */
	public static class ProxyTaskInputSplit extends InputSplit implements Writable{
		
		private long key = 0;
		
		private TaskWritable task = null;
		
		public ProxyTaskInputSplit() {
			
		}
		
		public ProxyTaskInputSplit(long key, TaskWritable task) {
			this.key = key;
			this.task = task;
		}

		@Override
		public long getLength() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return 1;
		}

		@Override
		public String[] getLocations() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return new String[]{};
		}
		
		public long getKey() {
			return key;
		}
		
		public TaskWritable getTask() {
			return task;
		}

		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeLong(key);
			task.write(out);
		}

		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			key = in.readLong();
			task = new TaskWritable();
			task.readFields(in);
		}
	}
	
	/**
	 * 
	 * @author ltang002
	 *
	 */
	public static class ProxyTaskRecordReader extends RecordReader<LongWritable, TaskWritable> {
		
		
		boolean hasRead = false;
		
		long key = 0;
		
		TaskWritable task = null;
				
		public ProxyTaskRecordReader() {
			
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			ProxyTaskInputSplit hSplit = (ProxyTaskInputSplit)split;
			this.key = hSplit.getKey();
			this.task = hSplit.getTask();
			this.hasRead = false;
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			if (hasRead) {
				return false;
			}
			else {
				hasRead = true;
				return true;
			}
		}

		@Override
		public LongWritable getCurrentKey() throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			return new LongWritable(key);
		}

		@Override
		public TaskWritable getCurrentValue() throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			return task;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			if (hasRead) {
				return 1.0f;
			}
			else {
				return 0;
			}
		}

		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
			
		}
		
	}

}
