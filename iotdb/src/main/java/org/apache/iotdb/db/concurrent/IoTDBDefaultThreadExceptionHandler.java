package org.apache.iotdb.db.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class IoTDBDefaultThreadExceptionHandler implements Thread.UncaughtExceptionHandler {
	private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBDefaultThreadExceptionHandler.class);
	
	public IoTDBDefaultThreadExceptionHandler(){
	}
	
	@Override
	public void uncaughtException(Thread t, Throwable e) {
		LOGGER.error("Exception in thread {}-{}", t.getName(), t.getId(), e);
	}

}
