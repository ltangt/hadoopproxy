package com.googlecode.hadoopproxy.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * 
 * 
 *  Copyright  2012, 
 *  
 *  @author Liang Tang
 *
 *  @version Created: May 1, 2012 8:34:33 PM
 *
 */
public class IPAddressUtil {
	
	public static String getIPAddress() {
		InetAddress thisIp;
		try {
			thisIp = InetAddress.getLocalHost();
			return thisIp.getHostAddress();		
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

}
