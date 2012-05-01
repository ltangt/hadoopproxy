package com.googlecode.hadoopproxy.util;

import java.io.File;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class ClassLoaderUtil {
	
	private static final Log LOG = LogFactory.getLog(ClassLoaderUtil.class);
	
	public static ClassLoader createClassLoader(String jarFileName) throws IOException {
		File file = new File(jarFileName);
        URL jarUrl = new URL("jar", "","file:" + file.getAbsolutePath()+"!/");
        URLClassLoader cl = new URLClassLoader(new URL[] {jarUrl}, ClassLoader.getSystemClassLoader());
        return cl;
	}
	
	public static void addJarToThreadClassLoader(String jarFileName) throws Exception {
		URLClassLoader loader = (URLClassLoader) Thread.currentThread().getContextClassLoader();
		Class urlClass = URLClassLoader.class;
		File jarFile = new File(jarFileName);
		Method method;

		method = urlClass.getDeclaredMethod("addURL", new Class[] { URL.class });
		method.setAccessible(true);
		method.invoke(loader, new Object[] { jarFile.toURI().toURL() });
		LOG.info("Added " + jarFileName + " into the current class loader");
		Thread.currentThread().setContextClassLoader(loader);

	}

	public static void addJarToSystemClassLoader(String jarFileName) throws Exception {
		URLClassLoader loader = (URLClassLoader) ClassLoader.getSystemClassLoader();
		Class urlClass = URLClassLoader.class;
		File jarFile = new File(jarFileName);
		Method method;

		method = urlClass.getDeclaredMethod("addURL", new Class[] { URL.class });
		method.setAccessible(true);
		method.invoke(loader, new Object[] { jarFile.toURI().toURL() });
		LOG.info("Added " + jarFileName + " into the current class loader");

	}
	

}
