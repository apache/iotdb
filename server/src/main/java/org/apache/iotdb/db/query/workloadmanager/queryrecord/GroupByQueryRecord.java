package org.apache.iotdb.db.query.workloadmanager.queryrecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class GroupByQueryRecord extends QueryRecord {
  long startTime;
  long endTime;
  long interval;
  long slidingStep;

  public GroupByQueryRecord(String device, String[] sensors, String[] ops, long startTime, long endTime, long interval, long slidingStep) {
    this.device = device;
    this.sensors = sensors;
    this.ops = ops;
    this.startTime = startTime;
    this.endTime = endTime;
    this.interval = interval;
    this.slidingStep = slidingStep;
    this.recordType = QueryRecordType.GROUP_BY;
    this.timestamp = new Date().getTime();
    recoverSql();
    recoverSqlWithTimestamp();
    // calHashCode();
  }

  public GroupByQueryRecord(String device, String[] sensors, String[] ops, long startTime, long endTime, long interval) {
    this(device, sensors, ops, startTime, endTime, interval, interval);
  }

  public GroupByQueryRecord(String device, List<String> sensors, List<String> ops, long startTime, long endTime, long interval, long slidingStep) {
    this.sensors = new String[sensors.size()];
    this.ops = new String[ops.size()];
    for(int i = 0; i < sensors.size(); ++i) {
      this.sensors[i] = sensors.get(i);
    }
    for(int i = 0; i < ops.size(); ++i) {
      this.ops[i] = ops.get(i);
    }
    this.device = device;
    this.startTime = startTime;
    this.endTime = endTime;
    this.interval = interval;
    this.slidingStep = slidingStep;
    this.recordType = QueryRecordType.GROUP_BY;
    this.timestamp = new Date().getTime();
    recoverSql();
    recoverSqlWithTimestamp();
    // calHashCode();
  }

  public GroupByQueryRecord(String device, List<String> sensors, List<String> ops, long startTime, long endTime, long interval) {
    this(device, sensors, ops, startTime, endTime, interval, interval);
  }

  private void recoverSql() {
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT ");
    for(int i = 0; i < sensors.length; ++i) {
      sb.append(ops[i] + "(" + sensors[i] +  ")");
      if (i != sensors.length - 1) {
        sb.append(", ");
      }
    }

    sb.append(" FROM " + device);
    sb.append(" GROUP BY ([");
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Date dStart = new Date();
    dStart.setTime(startTime);

    Date dEnd = new Date();
    dEnd.setTime(endTime);
    sb.append(df.format(dStart));
    sb.append(", ");
    sb.append(df.format(dEnd));
    sb.append("), " + interval + "ms");
    if (interval != slidingStep) {
      sb.append(", " + slidingStep + "ms");
    }
    sb.append(")");

    sql = sb.toString();
  }

  private void recoverSqlWithTimestamp() {
    Date d = new Date();
    d.setTime(timestamp);
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    sqlWithTimestamp = df.format(d) + " " + sql;
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
        long timeHashCode = ((startTime << 24) & 0xffff000000000000l) + ((endTime << 16) & 0xffff00000000l) + ((interval << 8) & 0xffff0000l) + (slidingStep & 0xffffl);
        hashcode = hashcode + (int)timeHashCode;
    }*/

  @Override
  public String getSql() {
    return sql;
  }


  @Override
  public String getSqlWithTimestamp() {
    return sqlWithTimestamp;
  }
}