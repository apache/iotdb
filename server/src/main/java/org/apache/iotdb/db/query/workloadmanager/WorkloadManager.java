package org.apache.iotdb.db.query.workloadmanager;

import org.apache.iotdb.db.query.workloadmanager.queryrecord.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkloadManager {
  List<QueryRecord> records = new ArrayList<>();
  private final Logger QUERY_RECORD_LOGGER = LoggerFactory.getLogger("QUERY_RECORD");
  private final int RECORDS_NUM_THRESHOLD = 300;
  private final ExecutorService flushExecutor = Executors.newFixedThreadPool(1);

  private WorkloadManager() {
  }

  private static class WorkloadManagerHolder {
    private static final WorkloadManager INSTANCE = new WorkloadManager();
  }

  private class QueryRecordFlushTask implements Runnable{
    List<QueryRecord> records;
    Logger QUERY_RECORD_LOGGER;

    private QueryRecordFlushTask(List<QueryRecord> r, Logger l) {
      records = r;
      QUERY_RECORD_LOGGER = l;
    }

    @Override
    public void run() {
      for (QueryRecord record : records) {
        QUERY_RECORD_LOGGER.info(record.getSql());
      }
    }
  }

  public static WorkloadManager getInstance() {
    return WorkloadManagerHolder.INSTANCE;
  }

  public synchronized void addAggregationRecord(String device, List<String> sensors, List<String> ops) {
    // add aggregation record
    QueryRecord record = new AggregationQueryRecord(device, sensors, ops);
    this.addRecord(record);
  }

  public synchronized void addGroupByQueryRecord(String device, List<String> sensors, List<String> ops,
                                                 long startTime, long endTime, long interval, long slidingStep) {
    QueryRecord record = new GroupByQueryRecord(device, sensors, ops, startTime, endTime, interval, slidingStep);
    this.addRecord(record);
  }

  public synchronized void addRecord(QueryRecord record) {
    records.add(record);
    if (records.size() > RECORDS_NUM_THRESHOLD) {
      flushExecutor.execute(new QueryRecordFlushTask(records, QUERY_RECORD_LOGGER));
      this.records = new ArrayList<>();
    }
  }
}
