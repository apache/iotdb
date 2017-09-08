package cn.edu.tsinghua.iotdb.service;

public interface JDBCServerMBean {
	String getJDBCServerStatus();
	int getRPCPort();
    void startServer();
    void restartServer();
    void stopServer();
}
