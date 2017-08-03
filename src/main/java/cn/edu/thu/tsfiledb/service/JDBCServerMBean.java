package cn.edu.thu.tsfiledb.service;

public interface JDBCServerMBean {
	String getJDBCServerStatus();
	int getRPCPort();
    void startServer();
    void restartServer();
    void stopServer();
}
