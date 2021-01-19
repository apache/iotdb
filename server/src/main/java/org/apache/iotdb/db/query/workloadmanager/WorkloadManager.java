package org.apache.iotdb.db.query.workloadmanager;

import org.apache.iotdb.db.query.workloadmanager.queryrecord.*;

import java.io.*;
import java.nio.Buffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkloadManager {
  private static final WorkloadManager manager = new WorkloadManager();
  List<QueryRecord> records = Collections.synchronizedList(new ArrayList<>());
  private static final Logger logger = LoggerFactory.getLogger(WorkloadManager.class);
  private int aggregationCount = 0;
  private int groupByCount = 0;
  private static final int RECORDS_SIZE = 100;
  private static final int RECORD_FILE_MAX_SIZE = 1024 * 1024 * 1024;
  private File curWritingFile = null;

  public WorkloadManager() {
  }

  public static WorkloadManager getInstance() {
    return manager;
  }

  public void addAggregationRecord(String device, List<String> sensorList, List<String> opList) {
    // add aggregation record
    String[] sensors = new String[sensorList.size()];
    String[] ops = new String[opList.size()];

    for (int i = 0; i < sensorList.size(); ++i) {
      sensors[i] = sensorList.get(i);
    }
    for (int i = 0; i < opList.size(); ++i) {
      ops[i] = opList.get(i);
    }
    QueryRecord record = new AggregationQueryRecord(device, sensors, ops);
        /*if (!historyMap.containsKey(record)) {
            historyMap.put(record, new AggregationHistory());
        }
        try {
            historyMap.get(record).addRecord();
        } catch (MismatchRecordTypeException e) {
            e.printStackTrace();
        }*/
    records.add(record);
  }

  public void addGroupByQueryRecord(String device, List<String> sensorList, List<String> opList,
                               long startTime, long endTime, long interval, long slidingStep) {
    // add group by record
    String[] sensors = new String[sensorList.size()];
    String[] ops = new String[opList.size()];

    for (int i = 0; i < sensorList.size(); ++i) {
      sensors[i] = sensorList.get(i);
    }
    for (int i = 0; i < opList.size(); ++i) {
      ops[i] = opList.get(i);
    }

    QueryRecord record = new GroupByQueryRecord(device, sensors, ops, startTime, endTime, interval, slidingStep);
        /*if (!historyMap.containsKey(record)) {
            historyMap.put(record, new GroupByHistory());
        }
        try {
            historyMap.get(record).addRecord(startTime, endTime, interval, slidingStep);
        } catch (MismatchRecordTypeException e) {
            e.printStackTrace();
        }*/
    records.add(record);
  }

  public void addRecord(QueryRecord record) {
    records.add(record);
    if (records.size() >= RECORDS_SIZE) {
      flushRecords();
    }
    switch (record.getRecordType()) {
      case AGGREGATION: {
        aggregationCount++;
        break;
      }
      case GROUP_BY: {
        groupByCount++;
        break;
      }
    }
    // logger.info("Record: " + record.getSqlWithTimestamp());
    logger.info("Adding record type of " + record.getRecordType());
    logger.info("Aggregation count: " + aggregationCount + ", group by count: " + groupByCount);
  }

  private void flushRecords() {
    // TODO: 转换成异步的
    String cacheDirPath = "./data/workload/";
    File cacheDir = new File(cacheDirPath);
    if (!cacheDir.exists()) {
      cacheDir.mkdir();
    }
    String regexPattern = "([0-9]+)\\.record";
    Pattern r = Pattern.compile(regexPattern);
    File fileToWrite = null;
    try {
      synchronized (records) {
        if (curWritingFile == null || curWritingFile.length() >= RECORD_FILE_MAX_SIZE) {
          // 寻找当前写入的文件
          File[] fileList = cacheDir.listFiles();
          int max = -1;
          for (File f : fileList) {
            Matcher matcher = r.matcher(f.getName());
            matcher.find();
            int val = Integer.valueOf(matcher.group(1));
            if (val > max) {
              max = val;
              fileToWrite = f;
            }
          }
          // 如果无文件，或文件大小大于阈值，则创建新文件来写
          if (max == -1 || fileToWrite.length() >= RECORD_FILE_MAX_SIZE) {
            fileToWrite = new File(cacheDirPath + (max + 1) + ".record");
            try {
              if (!fileToWrite.createNewFile()) {
                logger.error("Failed to create new record cache file");
              }
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
          curWritingFile = fileToWrite;
        }
        // 写入文件
        logger.info("Writing records to " + curWritingFile.getName());
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(curWritingFile, true)));
        for (int i = 0; i < records.size(); ++i) {
          writer.write(records.get(i).getSqlWithTimestamp());
          writer.write('\n');
        }
        writer.close();
        records.clear();
        groupByCount = 0;
        aggregationCount = 0;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
