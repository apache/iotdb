package org.apache.iotdb.db.query.workloadmanager;

import org.apache.iotdb.db.query.workloadmanager.queryrecord.QueryRecord;

import java.util.ArrayList;
import java.util.List;

public class Workload {
  List<QueryRecord> records;
  List<Float> weights;

  public Workload() {
    this.records = new ArrayList<>();
    this.weights = new ArrayList<>();
  }

  public Workload(List<QueryRecord> records, List<Float> weights) {
    this.records = records;
    this.weights = weights;
  }

  public void addRecord(QueryRecord record) {
    records.add(record);
  }

  public List<QueryRecord> getRecords() {
    return records;
  }
}
