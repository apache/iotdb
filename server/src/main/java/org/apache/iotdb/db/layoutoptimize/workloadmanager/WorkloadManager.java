package org.apache.iotdb.db.layoutoptimize.workloadmanager;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.layoutoptimize.workloadmanager.queryrecord.QueryRecord;
import org.apache.iotdb.db.layoutoptimize.workloadmanager.workloadlist.WorkloadInfo;
import org.apache.iotdb.db.layoutoptimize.workloadmanager.workloadlist.WorkloadList;
import org.apache.iotdb.db.layoutoptimize.workloadmanager.workloadlist.statisitc.ListStatistic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class WorkloadManager {
  private static final Logger logger = LoggerFactory.getLogger(WorkloadManager.class);
  private static final WorkloadManager INSTANCE = new WorkloadManager();
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
  private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
  private WorkloadList workloadList = new WorkloadList();
  private final Timer persistTimer = new Timer();
  private boolean timerSet = false;
  private static long PERSIST_PERIOD = 60L * 1000L;
  private final File workloadFile =
      new File(
          IoTDBDescriptor.getInstance().getConfig().getLayoutDir()
              + File.separator
              + "workload.bin");

  public static WorkloadManager getInstance() {
    return INSTANCE;
  }

  private WorkloadManager() {
    loadFromFile();
  }

  /**
   * Add query record to the manager
   *
   * @param deviceID the device which is visited
   * @param sensors the sensors that are visited
   * @param span the time span of the query
   */
  public void addQueryRecord(String deviceID, List<String> sensors, long span) {
    writeLock.lock();
    try {
      workloadList.addRecord(deviceID, sensors, span);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * using the statistic info to judge if the workload is changed
   *
   * @param deviceID the id of the device to judge on
   * @return true if the workload changes else false
   */
  public boolean isWorkloadChanged(String deviceID) {
    readLock.lock();
    try {
      ListStatistic oriStatistic = workloadList.getStatistic();
      workloadList.dropExpiredRecord();
      workloadList.updateStatistic();
      ListStatistic newStatistic = workloadList.getStatistic();
      return !oriStatistic.isTheSame(newStatistic);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * generate a sample of query record according to the collected info in workload manager
   *
   * @param deviceID the id of the device to sample on
   * @param sampleNum the number of the sampled query record
   * @return the list of the query record
   */
  public List<QueryRecord> getSampledQueryRecord(String deviceID, int sampleNum) {
    readLock.lock();
    try {
      WorkloadInfo info = workloadList.getWorkloadInfo(deviceID);
      List<QueryRecord> records = new LinkedList<>();
      for (int i = 0; i < sampleNum; i++) {
        records.add(info.sample());
      }
      return records;
    } finally {
      readLock.unlock();
    }
  }

  public boolean loadFromFile() {
    if (!workloadFile.exists()) {
      logger.info("fail to load from {}, because it does not exist", workloadFile);
      return false;
    }
    try {
      ObjectInputStream is = new ObjectInputStream(new FileInputStream(workloadFile));
      workloadList = (WorkloadList) is.readObject();
      is.close();
      logger.info("successfully load workload manager from file");
      return true;
    } catch (IOException | ClassNotFoundException e) {
      logger.info("fail to load workload manager");
      return false;
    }
  }

  public boolean persist() {
    try {
      if (!workloadFile.exists()) {
        workloadFile.createNewFile();
      }
      ObjectOutputStream os = new ObjectOutputStream(new FileOutputStream(workloadFile));
      os.writeObject(workloadList);
      os.flush();
      os.close();
      logger.info("successfully persist workload manager");
      return true;
    } catch (IOException e) {
      logger.info("fail to persist workload manager");
      return false;
    }
  }

  private boolean isTimerSet() {
    return timerSet;
  }

  private void setUpTimer() {
    if (timerSet) return;
    timerSet = true;
    persistTimer.scheduleAtFixedRate(new PersistTask(this), 1000, PERSIST_PERIOD);
  }

  private static class PersistTask extends TimerTask {
    WorkloadManager manager;
    public PersistTask(WorkloadManager instance) {
      manager = instance;
    }

    @Override
    public void run() {
      manager.persist();
    }
  }
}
