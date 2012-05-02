package com.googlecode.hadoopproxy;

import java.io.PrintStream;
import java.io.Serializable;

/**
 * 
 * 
 *  Copyright  2012, 
 *  
 *  @author Liang Tang
 *
 *  @version Created: May 1, 2012 8:33:11 PM
 *
 */
public interface ProxyTask extends Serializable{
	
	void run(PrintStream out) throws InterruptedException;

}
