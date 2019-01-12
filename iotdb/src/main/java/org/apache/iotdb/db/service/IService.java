package org.apache.iotdb.db.service;

import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.StartupException;

public interface IService {
	/**
	 * Start current service.
	 */
	void start() throws StartupException;

	/**
	 * Stop current service.
	 * If current service uses thread or thread pool,
	 * current service should guarantee to release thread or thread pool.
	 */
	void stop();

	/**
	 * @return current service name
	 */
	ServiceType getID();
}
