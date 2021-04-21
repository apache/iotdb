package org.apache.iotdb.db.layoutoptimize.workloadmanager.queryrecord;

import org.apache.iotdb.db.metadata.PartialPath;

import java.util.ArrayList;
import java.util.List;

public class QueryRecord {
  PartialPath device;
  List<String> measurements;
  long span;

  public QueryRecord(PartialPath device, List<String> measurements, long span) {
    this.device = device;
    this.measurements = new ArrayList<>(measurements);
    this.span = span;
  }

  public List<String> getMeasurements() {
    return measurements;
  }

  public PartialPath getDevice() {
    return device;
  }

  public long getSpan() {
    return span;
  }
}
