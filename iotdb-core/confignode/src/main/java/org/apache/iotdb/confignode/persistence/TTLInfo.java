package org.apache.iotdb.confignode.persistence;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.schema.ttl.TTLManager;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.response.ttl.ShowTTLResp;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TTLInfo implements SnapshotProcessor {
  private static final String SNAPSHOT_FILENAME = "ttl_info.bin";
  private static final Logger LOGGER = LoggerFactory.getLogger(TTLInfo.class);

  private TTLManager ttlManager;

  private final ReadWriteLock lock;

  public TTLInfo() {
    ttlManager = new TTLManager();
    lock = new ReentrantReadWriteLock();
  }

  public TSStatus setTTL(SetTTLPlan plan) {
    lock.writeLock().lock();
    try {
      ttlManager.setTTL(plan.getDatabasePathPattern(), plan.getTTL());
    } finally {
      lock.writeLock().unlock();
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  public TSStatus unsetTTL(SetTTLPlan plan) {
    lock.writeLock().lock();
    try {
      ttlManager.unsetTTL(plan.getDatabasePathPattern());
    } finally {
      lock.writeLock().unlock();
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  public ShowTTLResp showAllTTL() {
    lock.readLock().lock();
    ShowTTLResp resp = new ShowTTLResp();
    try {
      Map<String, Long> pathTTLMap = ttlManager.getAllPathTTL();
      resp.setPathTTLMap(pathTTLMap);
      resp.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    } finally {
      lock.readLock().unlock();
    }
    return resp;
  }

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws TException, IOException {
    File snapshotFile = new File(snapshotDir, SNAPSHOT_FILENAME);
    if (snapshotFile.exists() && snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to take snapshot, because snapshot file [{}] is already exist.",
          snapshotFile.getAbsolutePath());
      return false;
    }
    lock.writeLock().lock();
    try (FileOutputStream fileOutputStream = new FileOutputStream(snapshotFile);
        BufferedOutputStream outputStream = new BufferedOutputStream(fileOutputStream)) {
      ttlManager.serialize(outputStream);
      fileOutputStream.getFD().sync();
    } finally {
      lock.writeLock().unlock();
    }
    return true;
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws TException, IOException {
    File snapshotFile = new File(snapshotDir, SNAPSHOT_FILENAME);
    if (snapshotFile.exists() && snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to load snapshot,snapshot file [{}] is not exist.",
          snapshotFile.getAbsolutePath());
      return;
    }
    lock.writeLock().lock();
    try (FileInputStream fileInputStream = new FileInputStream(snapshotFile);
        BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream)) {
      ttlManager.clear();
      ttlManager.deserialize(bufferedInputStream);
    } finally {
      lock.writeLock().unlock();
    }
  }
}
