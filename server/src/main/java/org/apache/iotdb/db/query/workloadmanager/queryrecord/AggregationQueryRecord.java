package org.apache.iotdb.db.query.workloadmanager.queryrecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class AggregationQueryRecord extends QueryRecord {
  public AggregationQueryRecord(String device, String[] sensors, String[] ops) {
    this.device = device;
    this.sensors = sensors;
    this.ops = ops;
    this.timestamp = new Date().getTime();
    this.recordType = QueryRecordType.AGGREGATION;
    recoverSql();
    recoverSqlWithTimestamp();
    // calHashCode();
  }

  public AggregationQueryRecord(String device, List<String> sensorList, List<String> opList) {
    sensors = new String[sensorList.size()];
    ops = new String[opList.size()];
    for (int i = 0; i < sensorList.size(); ++i) {
      sensors[i] = sensorList.get(i);
    }
    for (int i = 0; i < opList.size(); ++i) {
      ops[i] = opList.get(i);
    }
    this.timestamp = new Date().getTime();
    this.device = device;
    this.recordType = QueryRecordType.AGGREGATION;
    recoverSql();
    recoverSqlWithTimestamp();
    // calHashCode();
  }
/*
    @Override
    protected void calHashCode() {
        int deviceHashCode = device.hashCode();
        int sensorHashCode = 0;
        for(int i = 0; i < sensors.length; ++i) {
            sensorHashCode += sensors[i].hashCode();
        }
        int opHashCode = 0;
        for(int i = 0; i < ops.length; ++i) {
            opHashCode *= ops[i].hashCode();
        }
        hashcode = ((deviceHashCode << 24) & 0xff000000) + ((opHashCode << 12) & 0xfff000) + (sensorHashCode & 0xfff);
    }*/

  private void recoverSql() {
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT ");
    for (int i = 0; i < sensors.length; ++i) {
      sb.append(ops[i] + "(" + sensors[i] + ")");
      if (i != sensors.length - 1)
        sb.append(", ");
    }
    sb.append(" FROM ");
    sb.append(device);
    sql = sb.toString();
  }

  private void recoverSqlWithTimestamp() {
    Date d = new Date();
    d.setTime(timestamp);
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    sqlWithTimestamp = df.format(d) + " " + sql;
  }

  public String getSql() {
    return sql;
  }

  @Override
  public String getSqlWithTimestamp() {
    return sqlWithTimestamp;
  }
}

