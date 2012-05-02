package com.googlecode.hadoopproxy.server;

import java.io.DataInput;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.hadoop.io.Writable;

import com.googlecode.hadoopproxy.ProxyTask;
import com.googlecode.hadoopproxy.util.SerializationUtil;

/**
 * 
 * 
 *  Copyright  2012, 
 *  
 *  @author Liang Tang
 *
 *  @version Created: May 1, 2012 8:34:01 PM
 *
 */
public class TaskWritable implements Writable {
	
	ProxyTask t;
	
	String masterServerName;
	
	String proxyJobID;
	
	public TaskWritable() {
		this.t = null;
		this.masterServerName = null;
	}
	
	public TaskWritable(ProxyTask t, String masterServerName, String proxyJobID) {
		this.t = t;
		this.masterServerName = masterServerName;
		this.proxyJobID = proxyJobID;
	}
	
	public ProxyTask getTask() {
		return t;
	}
	
	public String getMasterServerName() {
		return masterServerName;
	}
	
	public String getProxyJobID() {
		return proxyJobID;
	}

	
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(masterServerName);
		out.writeUTF(proxyJobID);

		byte[] objBytes= SerializationUtil.toBytes(t);
		out.writeInt(objBytes.length);
		out.write(objBytes);
	}

	
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.masterServerName = in.readUTF();
		this.proxyJobID = in.readUTF();
		
		int objByteLen = in.readInt();
		byte[] objBytes = new byte[objByteLen];
		in.readFully(objBytes);
		try {	
			this.t = (ProxyTask)SerializationUtil.toObject(objBytes);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			this.t = null;
			throw new IOException(e);
		}
	}

}
