package org.apache.iotdb.db.query.workloadmanager.queryrecord;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class GroupByQueryRecord extends QueryRecord {
  long startTime;
  long endTime;
  long interval;
  long slidingStep;

  public GroupByQueryRecord(String device, List<String> sensors, List<String> ops, long startTime, long endTime, long interval, long slidingStep) {
    this.sensors = new ArrayList<>(sensors);
    this.ops = new ArrayList<>(ops);
    this.device = device;
    this.startTime = startTime;
    this.endTime = endTime;
    this.interval = interval;
    this.slidingStep = slidingStep;
    this.recordType = QueryRecordType.GROUP_BY;
    this.timestamp = new Date().getTime();
    recoverSql();
    recoverSqlWithTimestamp();
  }

  public GroupByQueryRecord(String device, List<String> sensors, List<String> ops, long startTime, long endTime, long interval) {
    this(device, sensors, ops, startTime, endTime, interval, interval);
  }

  private void recoverSql() {
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT ");
    for(int i = 0; i < sensors.size(); ++i) {
      sb.append(ops.get(i) + "(" + sensors.get(i) +  ")");
      if (i != sensors.size() - 1) {
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

  @Override
  public String getSql() {
    return sql;
  }


  @Override
  public String getSqlWithTimestamp() {
    return sqlWithTimestamp;
  }
}