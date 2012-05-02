package com.googlecode.hadoopproxy.util;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;

/**
 *  A tool for reading/writing disk files
 * 
 *  Copyright  2012, 
 *  
 *  @author Liang Tang
 *
 *  @version Created: May 1, 2012 8:34:22 PM
 *
 */
public class FileUtil {
	
	public static byte[] readFully(String fileName) throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		BufferedInputStream bis = new BufferedInputStream(new FileInputStream(fileName));
		byte[] buf = new byte[8*1024];
		while(true) {
			int nRead = bis.read(buf, 0, buf.length);
			if (nRead <= 0) {
				break;
			}
			bos.write(buf, 0, nRead);
		}
		return bos.toByteArray();
	}
	
	
	public static void copyBuffer(InputStream in, OutputStream out, long bufferSize) throws IOException {
		byte[] buffer = new byte[8*1024];
		long readSize = 0;
		while(readSize < bufferSize) {
			long remainSize = bufferSize - readSize;
			int size = in.read(buffer, 0, (int)Math.min(remainSize, buffer.length));
			out.write(buffer, 0, size);
			readSize += size;
		}
	}
	
	public static void copyBuffer(InputStream in, OutputStream out) throws IOException {
		byte[] buffer = new byte[8*1024];
		while(true) {
			int size = in.read(buffer, 0, buffer.length);
			if (size <= 0) {
				break;
			}
			out.write(buffer, 0, size);
		}
	}
	
	public static long getFileLength(File file) throws IOException {
		RandomAccessFile raFile = new RandomAccessFile(file, "r");
		long fileLength = raFile.length();
		raFile.close();
		return fileLength;
	}
	
}
