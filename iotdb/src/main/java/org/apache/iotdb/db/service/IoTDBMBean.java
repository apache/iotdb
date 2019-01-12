package org.apache.iotdb.db.service;

import java.io.IOException;

import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.FileNodeManagerException;

public interface IoTDBMBean {
	void stop() throws FileNodeManagerException, IOException;
}
