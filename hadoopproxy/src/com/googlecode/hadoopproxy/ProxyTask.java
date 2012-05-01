package com.googlecode.hadoopproxy;

import java.io.PrintStream;
import java.io.Serializable;

public interface ProxyTask extends Serializable{
	
	void run(PrintStream out) throws InterruptedException;

}
