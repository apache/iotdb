package org.apache.iotdb.db.query.workloadmanager.queryrecord;

import java.util.List;

public abstract class QueryRecord {
  protected int hashcode = 0;
  protected String device;
  protected List<String> sensors;
  protected List<String> ops;
  protected QueryRecordType recordType;
  String sql;
  String sqlWithTimestamp;
  long timestamp;


  public abstract String getSql();

  public abstract String getSqlWithTimestamp();

  public QueryRecordType getRecordType() {
    return recordType;
  }

  public List<String> getSensors() {
    return sensors;
  }

  public String getDevice() {
    return device;
  }
}
