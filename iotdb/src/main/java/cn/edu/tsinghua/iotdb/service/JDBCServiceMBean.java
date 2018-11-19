package cn.edu.tsinghua.iotdb.service;

import cn.edu.tsinghua.iotdb.exception.StartupException;

public interface JDBCServiceMBean {
    String getJDBCServiceStatus();
    int getRPCPort();
    void startService() throws StartupException;
    void restartService() throws StartupException;
    void stopService();
}
