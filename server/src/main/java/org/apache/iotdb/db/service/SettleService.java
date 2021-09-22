package org.apache.iotdb.db.service;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.settle.SettleLog;
import org.apache.iotdb.db.engine.settle.SettleTask;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.tools.settle.TsFileAndModSettleTool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class SettleService implements IService {
  private static final Logger logger = LoggerFactory.getLogger(SettleService.class);

  private AtomicInteger threadCnt = new AtomicInteger();
  private ExecutorService settleThreadPool;
  private static AtomicInteger filesToBeSettledCount = new AtomicInteger();
  private static PartialPath storageGroupPath;

  public static SettleService getINSTANCE() {
    return InstanceHolder.INSTANCE;
  }

  public static class InstanceHolder {
    private static final SettleService INSTANCE = new SettleService();

    private InstanceHolder() {}
  }

  @Override
  public void start() {
    int settleThreadNum = IoTDBDescriptor.getInstance().getConfig().getSettleThreadNum();
    settleThreadPool =
        Executors.newFixedThreadPool(
            settleThreadNum, r -> new Thread(r, "SettleThread-" + threadCnt.getAndIncrement()));
    TsFileAndModSettleTool.findFilesToBeRecovered();
    countSettleFiles();
    if (!SettleLog.createSettleLog() || filesToBeSettledCount.get() == 0) {
      stop();
      return;
    }
    settleAll();
  }

  @Override
  public void stop() {
    SettleLog.closeLogWriter();
    TsFileAndModSettleTool.clearRecoverSettleFileMap();
    setStorageGroupPath(null);
    filesToBeSettledCount.set(0);
    if (settleThreadPool != null) {
      settleThreadPool.shutdownNow();
      logger.info("Waiting for settle task pool to shut down");
      settleThreadPool = null;
      logger.info("Settle service stopped");
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.SETTLE_SERVICE;
  }

  private void settleAll() {
    try {
      StorageEngine.getInstance().settleAll(getStorageGroupPath());
    } catch (StorageEngineException e) {
      logger.error("Cannot perform a global settle because", e);
    }
  }

  public static AtomicInteger getFilesToBeSettledCount() {
    return filesToBeSettledCount;
  }

  private static void countSettleFiles() {
    filesToBeSettledCount.addAndGet(
        StorageEngine.getInstance().countSettleFiles(getStorageGroupPath()));
  }

  public void submitSettleTask(SettleTask settleTask) {
    settleThreadPool.submit(settleTask);
  }

  public static PartialPath getStorageGroupPath() {
    return storageGroupPath;
  }

  public static void setStorageGroupPath(PartialPath storageGroupPath) {
    SettleService.storageGroupPath = storageGroupPath;
  }
}
