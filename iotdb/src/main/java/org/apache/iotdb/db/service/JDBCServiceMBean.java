package org.apache.iotdb.db.service;

import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.StartupException;

public interface JDBCServiceMBean {
    String getJDBCServiceStatus();
    int getRPCPort();
    void startService() throws StartupException;
    void restartService() throws StartupException;
    void stopService();
}
