package org.apache.iotdb.db.query.workloadmanager;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.query.workloadmanager.queryrecord.*;

import java.io.*;
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
  private static final Logger LOGGER = LoggerFactory.getLogger(WorkloadManager.class);

  private WorkloadManager() {
  }

  private static class WorkloadManagerHolder {
    private static final WorkloadManager INSTANCE = new WorkloadManager();
  }

  private class QueryRecordFlushTask implements Runnable {
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

  public synchronized boolean readFromFile() {
    String filepath = IoTDBDescriptor.getInstance().getConfig().getSystemDir() + File.separator
            + "experiment" + File.separator + "query.json";
    File recordFile = new File(filepath);
    if (!recordFile.exists()) {
      LOGGER.error("Record file " + recordFile.getAbsolutePath() + " does not exist");
      return false;
    }
    LOGGER.info("Reading from " + recordFile.getAbsolutePath());
    try {
      byte[] buffer = new byte[(int) recordFile.length()];
      InputStream inputStream = new FileInputStream(recordFile);
      inputStream.read(buffer);
      String jsonText = new String(buffer);
      JSONArray recordArray = JSONArray.parseArray(jsonText);
      for (int i = 0; i < recordArray.size(); ++i) {
        JSONObject record = (JSONObject) recordArray.get(i);
        String recordType = (String) record.get("type");
        String deviceID = (String) record.get("device");
        JSONArray visitSensors = (JSONArray) record.get("sensors");
        JSONArray opsArray = (JSONArray) record.get("ops");
        List<String> measurements = new ArrayList<>();
        List<String> ops = new ArrayList<>();
        for (int j = 0; j < visitSensors.size(); ++j) {
          measurements.add((String) visitSensors.get(j));
          ops.add((String) opsArray.get(j));
        }
        if (recordType.equals("group by")) {
          long startTime = (long) (int) record.get("startTime");
          long endTime = (long) (int) record.get("endTime");
          long interval = (long) (int) record.get("interval");
          long slidingStep = (long) (int) record.get("slidingStep");
          records.add(new GroupByQueryRecord(deviceID, measurements, ops, startTime, endTime, interval, slidingStep));
        } else {
          records.add(new AggregationQueryRecord(deviceID, measurements, ops));
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return true;
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

  public synchronized List<QueryRecord> getRecord(String deviceID) {
    List<QueryRecord> recordForCurDevice = new ArrayList<>();
    for (QueryRecord record : records) {
      if (record.getDevice().equals(deviceID)) {
        recordForCurDevice.add(record);
      }
    }
    return recordForCurDevice;
  }

  public synchronized List<QueryRecord> getRecords() {
    return records;
  }
}
