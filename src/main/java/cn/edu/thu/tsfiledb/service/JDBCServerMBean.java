package cn.edu.thu.tsfiledb.service;

public interface JDBCServerMBean {
    void startServer();
    void restartServer();
    void stopServer();
    void mergeAll();
}
