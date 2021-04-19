package org.apache.iotdb.db.layoutoptimize.workloadmanager.queryrecord;

import java.util.ArrayList;
import java.util.List;

public class QueryRecord {
  String device;
  List<String> measurements;
  long span;

  public QueryRecord(String device, List<String> measurements, long span) {
    this.device = device;
    this.measurements = new ArrayList<>(measurements);
    this.span = span;
  }

  public List<String> getMeasurements() {
    return measurements;
  }
}
