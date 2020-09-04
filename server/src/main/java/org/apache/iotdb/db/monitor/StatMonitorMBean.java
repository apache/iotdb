package org.apache.iotdb.db.monitor;

import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.metadata.PartialPath;

public interface StatMonitorMBean {

  long getGlobalTotalPointsNum();

  long getGlobalReqSuccessNum();

  long getGlobalReqFailNum();

  long getStorageGroupTotalPointsNum(String storageGroupName);

  String getSystemDirectory();
}

