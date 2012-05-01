package com.googlecode.hadoopproxy.util;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

/**
 * A Jar file creation tool class
 * @author Liang
 *
 */
public class JarUtil {
	
	public static void createJar(String jarFileName, String[] paths, String[] excludePackages) throws IOException {
		File[] files = new File[paths.length];
		for (int i=0; i<paths.length; i++) {
			files[i] = new File(paths[i]);
			if (files[i].exists() == false) {
				throw new FileNotFoundException(paths[i]);
			}
		}
		createJar(jarFileName, files, excludePackages);
	}
	
	public static void createJar(String jarFileName, File[] paths, String[] excludePackages) throws IOException {
		FileOutputStream fileOut = new FileOutputStream(jarFileName);
		JarOutputStream jarOut = new JarOutputStream(fileOut);
		if (excludePackages != null) {
			for (int i=0; i<excludePackages.length; i++) {
				excludePackages[i] = excludePackages[i].replace(".", "/");
			}
		}
		for (File f: paths) {
			add(jarOut, f, f.getPath(), excludePackages);
		}
		jarOut.close();
		fileOut.close();
	}
	
	private static void add(JarOutputStream jos, File f, String rootDir, String[] excludePackages) throws IOException {
		BufferedInputStream in = null;
		if (f.isDirectory()) {
			if (f.getPath().equals(rootDir) == false) {				
				String name = f.getPath().replace("\\", "/");
				if (name.startsWith(rootDir+"/")) {
					name = name.substring(rootDir.length()+1);
				}
				
				// If this package is excluded
				if (excludePackages != null) {
					for (String packName : excludePackages) {
						if (name.startsWith(packName)) {
							return;
						}
					}				
				}
				
				if (!name.isEmpty()) {
					if (!name.endsWith("/"))
						name += "/";
					JarEntry entry = new JarEntry(name);
					entry.setTime(f.lastModified());
					jos.putNextEntry(entry);
					jos.closeEntry();
				}
			}
			
			

			for (File nestedFile : f.listFiles()) {
				add(jos, nestedFile, rootDir, excludePackages);
			}
		}
		else {
			String name = f.getPath().replace("\\", "/");
			if (name.startsWith(rootDir+"/")) {
				name = name.substring(rootDir.length()+1);
			}
			
			// If this package is excluded
			if (excludePackages != null) {
				for (String packName : excludePackages) {
					if (name.startsWith(packName)) {
						return;
					}
				}				
			}
			
			JarEntry entry = new JarEntry(name);
			entry.setTime(f.lastModified());
			jos.putNextEntry(entry);
			in = new BufferedInputStream(new FileInputStream(f));
	
			byte[] buffer = new byte[1024];
			while (true) {
				int count = in.read(buffer);
				if (count == -1)
					break;
				jos.write(buffer, 0, count);
			}
			jos.closeEntry();
		}

	}
	
	public static void main(String[] args) {
		try {
			createJar("di.jar", new String[]{"bin"}, null);
		}catch(IOException e){
			e.printStackTrace();
		}
	}

}
