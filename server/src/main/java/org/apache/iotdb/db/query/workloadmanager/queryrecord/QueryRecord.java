package org.apache.iotdb.db.query.workloadmanager.queryrecord;

public abstract class QueryRecord {
  protected int hashcode = 0;
  protected String device;
  protected String[] sensors;
  protected String[] ops;
  protected QueryRecordType recordType;
  String sql;
  String sqlWithTimestamp;
  long timestamp;

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (obj instanceof QueryRecord) {
      QueryRecord record = (QueryRecord) obj;

      boolean isSensorsEqual = true;
      for (String sensor : record.sensors) {
        boolean curSensorInThisRecord = false;
        for (int i = 0; i < sensors.length; ++i) {
          if (sensors[i].equals(sensor)) {
            curSensorInThisRecord = true;
            break;
          }
        }
        isSensorsEqual = isSensorsEqual && curSensorInThisRecord;
      }
      isSensorsEqual = isSensorsEqual && sensors.length == record.sensors.length;

      boolean isOpsEqual = true;
      for (String op : record.ops) {
        boolean curOpInThisRecord = false;
        for (int i = 0; i < ops.length; ++i) {
          if (ops[i].equals(op)) {
            curOpInThisRecord = true;
            break;
          }
        }
        isOpsEqual = isOpsEqual && curOpInThisRecord;
      }
      isOpsEqual = isOpsEqual && ops.length == record.ops.length;

      return device.equals(record.device) && isOpsEqual && isSensorsEqual;
    } else {
      return false;
    }
  }

  public abstract String getSql();

  public abstract String getSqlWithTimestamp();

  public QueryRecordType getRecordType() {
    return recordType;
  }
}
