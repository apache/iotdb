package cn.edu.tsinghua.iotdb.service;

import java.io.IOException;

import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;

public interface IoTDBMBean {
	void stop() throws FileNodeManagerException, IOException;
}
