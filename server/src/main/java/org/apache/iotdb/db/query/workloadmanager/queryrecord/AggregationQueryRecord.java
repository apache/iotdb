package org.apache.iotdb.db.query.workloadmanager.queryrecord;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class AggregationQueryRecord extends QueryRecord {

  public AggregationQueryRecord(String device, List<String> sensorList, List<String> opList) {
    sensors = new ArrayList<>(sensorList);
    ops = new ArrayList<>(opList);
    this.timestamp = new Date().getTime();
    this.device = device;
    this.recordType = QueryRecordType.AGGREGATION;
    recoverSql();
    recoverSqlWithTimestamp();
  }

  private void recoverSql() {
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT ");
    for (int i = 0; i < sensors.size(); ++i) {
      sb.append(ops.get(i) + "(" + sensors.get(i) + ")");
      if (i != sensors.size() - 1)
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

