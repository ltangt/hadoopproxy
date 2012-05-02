package com.googlecode.hadoopproxy.util;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

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
	
	
}
