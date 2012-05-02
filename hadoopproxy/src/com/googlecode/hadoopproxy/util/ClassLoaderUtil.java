package com.googlecode.hadoopproxy.util;

import java.io.File;
import java.io.FileNotFoundException;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.util.Enumeration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * 
 *  Copyright  2012, 
 *  
 *  @author Liang Tang
 *
 *  @version Created: May 1, 2012 8:34:13 PM
 *
 */
public class ClassLoaderUtil {
	
	private static final Log LOG = LogFactory.getLog(ClassLoaderUtil.class);
	
	public static ClassLoader createClassLoader(String jarFileName) throws IOException {
		File file = new File(jarFileName);
        URL jarUrl = new URL("jar", "","file:" + file.getAbsolutePath()+"!/");
        URLClassLoader cl = new URLClassLoader(new URL[] {jarUrl}, ClassLoader.getSystemClassLoader());
        return cl;
	}
	
	public static void addJarToThreadClassLoader(String jarFileName) throws Exception {		
		File jarFile = new File(jarFileName);
		addJarToThreadClassLoader(jarFile);
	}
	
	public static void addJarToThreadClassLoader(File jarFile) throws Exception {
		URLClassLoader loader = (URLClassLoader) Thread.currentThread().getContextClassLoader();
		Class urlClass = URLClassLoader.class;
		Method method;

		method = urlClass.getDeclaredMethod("addURL", new Class[] { URL.class });
		method.setAccessible(true);
		method.invoke(loader, new Object[] { jarFile.toURI().toURL() });
		LOG.info("Added " + jarFile.getName() + " into the thread class loader");
		Thread.currentThread().setContextClassLoader(loader);
	}
	
	public static void addJarToSystemClassLoader(String jarFileName) throws Exception {		
		File jarFile = new File(jarFileName);
		addJarToSystemClassLoader(jarFile);
	}
		
	public static void addJarToSystemClassLoader(File jarFile) throws Exception {
		URLClassLoader loader = (URLClassLoader) ClassLoader.getSystemClassLoader();
		Class urlClass = URLClassLoader.class;		
		Method method;
		method = urlClass.getDeclaredMethod("addURL", new Class[] { URL.class });
		method.setAccessible(true);
		method.invoke(loader, new Object[] { jarFile.toURI().toURL() });
		LOG.info("Added " + jarFile + " into the system class loader");
	}

	public static void addLibDirectoryToSystemClassLoader(String jarFolderName) throws Exception {
		File file = new File(jarFolderName);
		if (file.exists() == false) {
			throw new FileNotFoundException(jarFolderName);
		}
		else if (file.isDirectory() == false) {
			throw new IOException(jarFolderName + " is not a folder.");
		}
		else {
			File[] jarFiles = file.listFiles();
			for (File jarFile : jarFiles) {
				addJarToSystemClassLoader(jarFile);
			}
		}
	}
	
	public static String findContainingJar(Class my_class) {
	    ClassLoader loader = my_class.getClassLoader();
	    String class_file = my_class.getName().replaceAll("\\.", "/") + ".class";
	    try {
	      for(Enumeration itr = loader.getResources(class_file);
	          itr.hasMoreElements();) {
	        URL url = (URL) itr.nextElement();
	        if ("jar".equals(url.getProtocol())) {
	          String toReturn = url.getPath();
	          if (toReturn.startsWith("file:")) {
	            toReturn = toReturn.substring("file:".length());
	          }
	          toReturn = URLDecoder.decode(toReturn, "UTF-8");
	          return toReturn.replaceAll("!.*$", "");
	        }
	      }
	    } catch (IOException e) {
	      throw new RuntimeException(e);
	    }
	    return null;
	  }

}
