package org.apache.iotdb.db.newsync.transport.server;

import org.apache.iotdb.db.exception.StartupException;

public interface TransportServerManagerMBean {
  String getRPCServiceStatus();

  int getRPCPort();

  void startService() throws StartupException;

  void restartService() throws StartupException;

  void stopService();
}
