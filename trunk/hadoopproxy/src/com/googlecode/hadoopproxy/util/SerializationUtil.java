package com.googlecode.hadoopproxy.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 *  A tool class for object serialization
 * 
 *  Copyright  2012, 
 *  
 *  @author Liang Tang
 *
 *  @version Created: May 1, 2012 8:34:49 PM
 *
 */
public class SerializationUtil {
	
	public static byte[] toBytes(Object obj) throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream  oos = new ObjectOutputStream(bos);
		oos.writeObject(obj);
		oos.close();
		return bos.toByteArray();
	}
	
	public static Object toObject(byte[] buf) throws IOException, ClassNotFoundException {
		ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(buf));
		Object obj = ois.readObject();
		ois.close();
		return obj;
	}

}
