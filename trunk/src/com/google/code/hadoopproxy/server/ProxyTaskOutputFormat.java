package com.google.code.hadoopproxy.server;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class ProxyTaskOutputFormat extends OutputFormat<LongWritable, Text> {
	
	public ProxyTaskOutputFormat() {
		
	}

	@Override
	public RecordWriter<LongWritable, Text> getRecordWriter(
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return new ProxyTaskRecordWriter();
	}

	@Override
	public void checkOutputSpecs(JobContext context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new NullOutputCommitter();
	}
	
	/**
	 * 
	 * @author ltang002
	 *
	 */
	public static class ProxyTaskRecordWriter extends RecordWriter<LongWritable, Text> {
		
		public ProxyTaskRecordWriter() {
			
		}

		@Override
		public void write(LongWritable key, Text value) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			System.out.println(key+" : "+value);			
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			
		}
		
	}
	
	public static class NullOutputCommitter extends OutputCommitter {
		
		public NullOutputCommitter() {
			
		}

		@Override
		public void setupJob(JobContext jobContext) throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void cleanupJob(JobContext jobContext) throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void setupTask(TaskAttemptContext taskContext)
				throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public boolean needsTaskCommit(TaskAttemptContext taskContext)
				throws IOException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public void commitTask(TaskAttemptContext taskContext)
				throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void abortTask(TaskAttemptContext taskContext)
				throws IOException {
			// TODO Auto-generated method stub
			
		}
		
	}

}
