package com.googlecode.hadoopproxy.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

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
