package org.apache.iotdb.db.layoutoptimize.workloadmanager.workloadlist;

import org.apache.iotdb.db.layoutoptimize.workloadmanager.queryrecord.QueryRecord;

import java.util.ArrayList;
import java.util.List;

public class WorkloadItem {
  private long startTime;
  private long endTime;
  List<QueryRecord> queryList = new ArrayList<>();
}
