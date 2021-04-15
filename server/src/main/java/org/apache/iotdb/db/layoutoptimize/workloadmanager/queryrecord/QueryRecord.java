package org.apache.iotdb.db.layoutoptimize.workloadmanager.queryrecord;

import java.util.*;

public class QueryRecord {
  String deviceID;
  List<String> measurements;
  private int _hashCode;

  public QueryRecord(String deviceID, List<String> measurements) {
    this.deviceID = deviceID;
    this.measurements = new ArrayList<>(measurements);
    _hashCode = 0;
  }

  public void calHashCode() {
    // sort the measurement in lexicographical order
    Collections.sort(measurements);
    StringBuilder sb = new StringBuilder();
    sb.append(deviceID);
    for (String measurement : measurements) {
      sb.append(measurement);
    }
    _hashCode = sb.toString().hashCode();
  }

  @Override
  public int hashCode() {
    return _hashCode;
  }
}
