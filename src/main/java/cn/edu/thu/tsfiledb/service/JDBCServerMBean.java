package cn.edu.thu.tsfiledb.service;


public interface JDBCServerMBean {
	public void startServer();
	public void restartServer();
	public void stopServer();
	public void mergeAll();
}
