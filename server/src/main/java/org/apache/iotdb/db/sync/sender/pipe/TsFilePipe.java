/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.iotdb.db.sync.sender.pipe;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.exception.sync.PipeException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.sync.pipe.PipeInfo;
import org.apache.iotdb.commons.sync.pipe.PipeStatus;
import org.apache.iotdb.commons.sync.pipe.TsFilePipeInfo;
import org.apache.iotdb.commons.sync.pipesink.PipeSink;
import org.apache.iotdb.commons.sync.utils.SyncPathUtil;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.sync.pipedata.DeletionPipeData;
import org.apache.iotdb.db.sync.pipedata.PipeData;
import org.apache.iotdb.db.sync.pipedata.TsFilePipeData;
import org.apache.iotdb.db.sync.pipedata.queue.BufferedPipeDataQueue;
import org.apache.iotdb.db.sync.pipedata.queue.PipeDataQueue;
import org.apache.iotdb.db.sync.sender.manager.ISyncManager;
import org.apache.iotdb.db.sync.sender.manager.LocalSyncManager;
import org.apache.iotdb.db.sync.sender.recovery.TsFilePipeLogger;
import org.apache.iotdb.db.sync.transport.client.SenderManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TsFilePipe implements Pipe {
  private static final Logger logger = LoggerFactory.getLogger(TsFilePipe.class);
  // <DataRegionId, ISyncManager>
  private final Map<String, ISyncManager> syncManagerMap = new ConcurrentHashMap<>();

  private final TsFilePipeInfo pipeInfo;

  private final PipeSink pipeSink;

  // <DataRegionId, PipeDataQueue>
  private final Map<String, PipeDataQueue> historyQueueMap = new ConcurrentHashMap<>();
  private final Map<String, PipeDataQueue> realTimeQueueMap = new ConcurrentHashMap<>();
  private final TsFilePipeLogger pipeLog;
  private final ReentrantLock collectRealTimeDataLock;

  /* handle rpc send logic in sender-side*/
  private final SenderManager senderManager;

  /* whether finish collect history file. If false, no need to collect realtime file*/
  private boolean isCollectFinished;
  /**
   * Write lock needs to be added to block the real-time data collection process when collecting
   * historical data, otherwise data may be lost. Because historical data collection operations are
   * rare, it will not affect write performance.
   */
  private final ReentrantReadWriteLock isCollectFinishedReadWriteLock =
      new ReentrantReadWriteLock(false);

  //  private long maxSerialNumber;
  private AtomicLong maxSerialNumber;

  public TsFilePipe(
      long createTime, String name, PipeSink pipeSink, long dataStartTime, boolean syncDelOp) {

    this.pipeInfo =
        new TsFilePipeInfo(
            name,
            pipeSink.getPipeSinkName(),
            PipeStatus.STOP,
            createTime,
            dataStartTime,
            syncDelOp);
    this.pipeSink = pipeSink;

    this.pipeLog = new TsFilePipeLogger(this);
    this.isCollectFinished = pipeLog.isCollectFinished();
    this.collectRealTimeDataLock = new ReentrantLock();
    this.senderManager = new SenderManager(this, pipeSink);

    this.maxSerialNumber = new AtomicLong(0);
    recover();
  }

  private void recover() {
    File dir =
        new File(
            SyncPathUtil.getSenderHistoryPipeLogDir(
                pipeInfo.getPipeName(), pipeInfo.getCreateTime()));
    if (dir.exists()) {
      File[] fileList = dir.listFiles();
      for (File file : fileList) {
        String dataRegionId = file.getName();
        BufferedPipeDataQueue historyQueue =
            new BufferedPipeDataQueue(
                SyncPathUtil.getSenderDataRegionHistoryPipeLogDir(
                    pipeInfo.getPipeName(), pipeInfo.getCreateTime(), dataRegionId));
        historyQueueMap.put(dataRegionId, historyQueue);
      }
    }
    dir =
        new File(
            SyncPathUtil.getSenderRealTimePipeLogDir(
                pipeInfo.getPipeName(), pipeInfo.getCreateTime()));
    if (dir.exists()) {
      File[] fileList = dir.listFiles();
      for (File file : fileList) {
        String dataRegionId = file.getName();
        BufferedPipeDataQueue realTimeQueue =
            new BufferedPipeDataQueue(
                SyncPathUtil.getSenderDataRegionRealTimePipeLogDir(
                    pipeInfo.getPipeName(), pipeInfo.getCreateTime(), dataRegionId));
        realTimeQueueMap.put(dataRegionId, realTimeQueue);
        this.maxSerialNumber.set(
            Math.max(this.maxSerialNumber.get(), realTimeQueue.getLastMaxSerialNumber()));
      }
    }
  }

  @Override
  public synchronized void start() throws PipeException {
    if (pipeInfo.getStatus() == PipeStatus.RUNNING) {
      return;
    }
    // check connection
    senderManager.checkConnection();

    // init sync manager
    List<DataRegion> dataRegions = StorageEngine.getInstance().getAllDataRegions();
    for (DataRegion dataRegion : dataRegions) {
      logger.info(
          logFormat(
              "init syncManager for %s-%s",
              dataRegion.getDatabaseName(), dataRegion.getDataRegionId()));
      getOrCreateSyncManager(dataRegion.getDataRegionId());
    }
    try {
      isCollectFinishedReadWriteLock.writeLock().lock();
      if (!isCollectFinished) {
        pipeLog.clear();
        collectHistoryData();
        pipeLog.finishCollect();
        isCollectFinished = true;
      }

      pipeInfo.setStatus(PipeStatus.RUNNING);
      senderManager.start();
    } catch (IOException e) {
      logger.error(
          logFormat(
              "Clear pipe dir %s error.",
              SyncPathUtil.getSenderPipeDir(pipeInfo.getPipeName(), pipeInfo.getCreateTime())),
          e);
      throw new PipeException("Start error, can not clear pipe log.");
    } finally {
      isCollectFinishedReadWriteLock.writeLock().unlock();
    }
  }

  /** collect data * */
  private void collectHistoryData() {
    // collect history TsFile
    for (Map.Entry<String, ISyncManager> entry : syncManagerMap.entrySet()) {
      List<File> historyTsFiles =
          entry.getValue().syncHistoryTsFile(pipeInfo.getDataStartTimestamp());
      // put history data into PipeDataQueue
      int historyTsFilesSize = historyTsFiles.size();
      for (int i = 0; i < historyTsFilesSize; i++) {
        long serialNumber = 1L - historyTsFilesSize + i;
        File tsFile = historyTsFiles.get(i);
        historyQueueMap
            .get(entry.getKey())
            .offer(new TsFilePipeData(tsFile.getParent(), tsFile.getName(), serialNumber));
      }
    }
  }

  public File createHistoryTsFileHardlink(File tsFile, long modsOffset) {
    collectRealTimeDataLock.lock(); // synchronize the pipeLog.isHardlinkExist
    try {
      if (pipeLog.isHardlinkExist(tsFile)) {
        return null;
      }

      return pipeLog.createTsFileAndModsHardlink(tsFile, modsOffset);
    } catch (IOException e) {
      logger.error(logFormat("Create hardlink for history tsfile %s error.", tsFile.getPath()), e);
      return null;
    } finally {
      collectRealTimeDataLock.unlock();
    }
  }

  public void collectRealTimeDeletion(Deletion deletion, String sgName, String dataRegionId) {
    collectRealTimeDataLock.lock();
    try {
      if (!pipeInfo.isSyncDelOp()) {
        return;
      }

      for (PartialPath deletePath : LocalSyncManager.splitPathPatternByDevice(deletion.getPath())) {
        Deletion splitDeletion =
            new Deletion(
                deletePath,
                deletion.getFileOffset(),
                deletion.getStartTime(),
                deletion.getEndTime());
        PipeData deletionData =
            new DeletionPipeData(sgName, splitDeletion, maxSerialNumber.incrementAndGet());
        realTimeQueueMap.get(dataRegionId).offer(deletionData);
      }
    } catch (MetadataException e) {
      logger.warn(logFormat("Collect deletion %s error.", deletion), e);
    } finally {
      collectRealTimeDataLock.unlock();
    }
  }

  public void collectRealTimeTsFile(File tsFile, String dataRegionId) {
    collectRealTimeDataLock.lock();
    try {
      if (pipeLog.isHardlinkExist(tsFile)) {
        return;
      }

      File hardlink = pipeLog.createTsFileHardlink(tsFile);
      PipeData tsFileData =
          new TsFilePipeData(
              hardlink.getParent(), hardlink.getName(), maxSerialNumber.incrementAndGet());
      realTimeQueueMap.get(dataRegionId).offer(tsFileData);
    } catch (IOException e) {
      logger.warn(
          logFormat(
              "Create Hardlink tsfile %s on disk error, serial number is %d.",
              tsFile.getPath(), maxSerialNumber),
          e);
    } finally {
      collectRealTimeDataLock.unlock();
    }
  }

  public void collectRealTimeResource(File tsFile) {
    try {
      pipeLog.createTsFileResourceHardlink(tsFile);
    } catch (IOException e) {
      logger.warn(logFormat("Record tsfile resource %s on disk error.", tsFile.getPath()), e);
    }
  }

  /** transport data * */
  @Override
  public PipeData take(String dataRegionId) throws InterruptedException {
    if (!historyQueueMap.get(dataRegionId).isEmpty()) {
      return historyQueueMap.get(dataRegionId).take();
    }
    return realTimeQueueMap.get(dataRegionId).take();
  }

  public List<PipeData> pull(long serialNumber) {
    List<PipeData> pullPipeData = new ArrayList<>();
    for (PipeDataQueue historyQueue : historyQueueMap.values()) {
      if (!historyQueue.isEmpty()) {
        pullPipeData.addAll(historyQueue.pull(serialNumber));
      }
    }
    for (PipeDataQueue realTimeQueue : realTimeQueueMap.values()) {
      if (serialNumber > 0) {
        pullPipeData.addAll(realTimeQueue.pull(serialNumber));
      }
    }
    return pullPipeData;
  }

  @Override
  public void commit(String dataRegionId) {
    if (!historyQueueMap.get(dataRegionId).isEmpty()) {
      historyQueueMap.get(dataRegionId).commit();
    }
    realTimeQueueMap.get(dataRegionId).commit();
  }

  @Override
  public ISyncManager getOrCreateSyncManager(String dataRegionId) {
    // Only need to deal with pipe that has finished history file collection,
    return syncManagerMap.computeIfAbsent(
        dataRegionId,
        id -> {
          registerDataRegion(id);
          return new LocalSyncManager(
              StorageEngine.getInstance().getDataRegion(new DataRegionId(Integer.parseInt(id))),
              this);
        });
  }

  private void registerDataRegion(String dataRegionId) {
    historyQueueMap.put(
        dataRegionId,
        new BufferedPipeDataQueue(
            SyncPathUtil.getSenderDataRegionHistoryPipeLogDir(
                pipeInfo.getPipeName(), pipeInfo.getCreateTime(), dataRegionId)));
    realTimeQueueMap.put(
        dataRegionId,
        new BufferedPipeDataQueue(
            SyncPathUtil.getSenderDataRegionRealTimePipeLogDir(
                pipeInfo.getPipeName(), pipeInfo.getCreateTime(), dataRegionId)));
    senderManager.registerDataRegion(dataRegionId);
  }

  @Override
  public void unregisterDataRegion(String dataRegionId) {
    ISyncManager syncManager = syncManagerMap.remove(dataRegionId);
    if (syncManager != null) {
      syncManager.delete();
      senderManager.unregisterDataRegion(dataRegionId);
      realTimeQueueMap.remove(dataRegionId).clear();
      historyQueueMap.remove(dataRegionId).clear();
    }
  }

  @Override
  public boolean isHistoryCollectFinished() {
    try {
      isCollectFinishedReadWriteLock.readLock().lock();
      return isCollectFinished;
    } finally {
      isCollectFinishedReadWriteLock.readLock().unlock();
    }
  }

  @Override
  public PipeInfo getPipeInfo() {
    return pipeInfo;
  }

  public void commit(long serialNumber) {
    for (PipeDataQueue historyQueue : historyQueueMap.values()) {
      if (!historyQueue.isEmpty()) {
        historyQueue.commit(serialNumber);
      }
    }
    for (PipeDataQueue realTimeQueue : realTimeQueueMap.values()) {
      if (serialNumber > 0) {
        realTimeQueue.commit(serialNumber);
      }
    }
  }

  @Override
  public synchronized void stop() throws PipeException {
    senderManager.stop();
    pipeInfo.setStatus(PipeStatus.STOP);
  }

  @Override
  public synchronized void drop() throws PipeException {
    close();
    clear();
  }

  private void clear() {
    try {
      historyQueueMap.values().forEach(PipeDataQueue::clear);
      realTimeQueueMap.values().forEach(PipeDataQueue::clear);
      pipeLog.clear();
    } catch (IOException e) {
      logger.warn(
          logFormat("Clear pipe %s %d error.", pipeInfo.getPipeName(), pipeInfo.getCreateTime()),
          e);
    }
  }

  private String logFormat(String format, Object... arguments) {
    return String.format(
        String.format("[%s-%s] ", pipeInfo.getPipeName(), pipeInfo.getCreateTime()) + format,
        arguments);
  }

  @Override
  public void close() throws PipeException {
    historyQueueMap.values().forEach(PipeDataQueue::close);
    realTimeQueueMap.values().forEach(PipeDataQueue::close);
    senderManager.close();
  }

  @Override
  public String getName() {
    return pipeInfo.getPipeName();
  }

  @Override
  public PipeSink getPipeSink() {
    return pipeSink;
  }

  @Override
  public long getCreateTime() {
    return pipeInfo.getCreateTime();
  }

  @Override
  public PipeStatus getStatus() {
    return pipeInfo.getStatus();
  }

  @Override
  public String toString() {
    return "TsFilePipe{"
        + ", pipeInfo="
        + pipeInfo
        + ", pipeSink="
        + pipeSink
        + ", pipeLog="
        + pipeLog
        + ", collectRealTimeDataLock="
        + collectRealTimeDataLock
        + ", maxSerialNumber="
        + maxSerialNumber
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TsFilePipe that = (TsFilePipe) o;
    return Objects.equals(pipeInfo, that.pipeInfo) && Objects.equals(pipeSink, that.pipeSink);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pipeInfo, pipeSink);
  }

  @Override
  public SenderManager getSenderManager() {
    return senderManager;
  }
}
