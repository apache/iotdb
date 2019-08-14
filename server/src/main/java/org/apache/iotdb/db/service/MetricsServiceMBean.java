package org.apache.iotdb.db.service;

import org.apache.iotdb.db.exception.StartupException;

public interface MetricsServiceMBean {
	
	int  getMetricsPort();

	void startService() throws StartupException;

	void restartService() throws StartupException;

	void stopService();

}
