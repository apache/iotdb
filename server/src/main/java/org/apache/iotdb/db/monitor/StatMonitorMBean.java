package org.apache.iotdb.db.monitor;

public interface StatMonitorMBean {

  long getGlobalTotalPointsNum();

  String getSystemDirectory();
}

