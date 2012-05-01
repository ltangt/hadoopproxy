package com.googlecode.hadoopproxy.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class DateUtil {

	public static final String DATE_FORMAT_NOW = "yyyy-MM-dd HH:mm:ss";
	
	public static final String DATE_FORMAT_ID = "yyyyMMddHHmmss";
	
	public static final String DATE_FORMAT_LONGID = "yyyyMMddHHmmssSSS";

	public static String now() {
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_NOW);
		return sdf.format(cal.getTime());
	}
	
	public static String createID() {
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_ID);
		return sdf.format(cal.getTime());
	}
	
	public static String createLongID() {
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_LONGID);
		return sdf.format(cal.getTime());
	}

}
