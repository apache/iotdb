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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.sync.SyncPathUtil;
import org.apache.iotdb.db.engine.StorageEngineV2;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.exception.sync.PipeException;
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

public class TsFilePipe implements Pipe {
  private static final Logger logger = LoggerFactory.getLogger(TsFilePipe.class);
  // <DataRegionId, ISyncManager>
  private final Map<String, ISyncManager> syncManagerMap = new ConcurrentHashMap<>();

  private final long createTime;
  private final String name;
  private final PipeSink pipeSink;
  private final long dataStartTime;
  private final boolean syncDelOp;

  // <DataRegionId, PipeDataQueue>
  private final Map<String, PipeDataQueue> historyQueueMap = new ConcurrentHashMap<>();
  private final Map<String, PipeDataQueue> realTimeQueueMap = new ConcurrentHashMap<>();
  private final TsFilePipeLogger pipeLog;
  private final ReentrantLock collectRealTimeDataLock;

  /* handle rpc send logic in sender-side*/
  private final SenderManager senderManager;

  //  private long maxSerialNumber;
  private AtomicLong maxSerialNumber;

  private PipeStatus status;

  public TsFilePipe(
      long createTime, String name, PipeSink pipeSink, long dataStartTime, boolean syncDelOp) {
    this.createTime = createTime;
    this.name = name;
    this.pipeSink = pipeSink;
    this.dataStartTime = dataStartTime;
    this.syncDelOp = syncDelOp;

    this.pipeLog = new TsFilePipeLogger(this);
    this.collectRealTimeDataLock = new ReentrantLock();
    this.senderManager = new SenderManager(this, pipeSink);

    this.maxSerialNumber = new AtomicLong(0);

    this.status = PipeStatus.STOP;
    recover();
  }

  private void recover() {
    File dir = new File(SyncPathUtil.getSenderRealTimePipeLogDir(this.name, this.createTime));
    if (dir.exists()) {
      File[] fileList = dir.listFiles();
      for (File file : fileList) {
        String dataRegionId = file.getName();
        BufferedPipeDataQueue historyQueue =
            new BufferedPipeDataQueue(
                SyncPathUtil.getSenderDataRegionHistoryPipeLogDir(
                    this.name, this.createTime, dataRegionId));
        BufferedPipeDataQueue realTimeQueue =
            new BufferedPipeDataQueue(
                SyncPathUtil.getSenderDataRegionRealTimePipeLogDir(
                    this.name, this.createTime, dataRegionId));
        historyQueueMap.put(dataRegionId, historyQueue);
        realTimeQueueMap.put(dataRegionId, realTimeQueue);
        this.maxSerialNumber.set(
            Math.max(this.maxSerialNumber.get(), realTimeQueue.getLastMaxSerialNumber()));
      }
    }
  }

  @Override
  public synchronized void start() throws PipeException {
    if (status == PipeStatus.DROP) {
      throw new PipeException(
          String.format("Can not start pipe %s, because the pipe has been drop.", name));
    } else if (status == PipeStatus.RUNNING) {
      return;
    }

    // init sync manager
    List<DataRegion> dataRegions = StorageEngineV2.getInstance().getAllDataRegions();
    for (DataRegion dataRegion : dataRegions) {
      logger.info(
          logFormat(
              "init syncManager for %s-%s",
              dataRegion.getStorageGroupName(), dataRegion.getDataRegionId()));
      getOrCreateSyncManager(dataRegion.getDataRegionId());
    }
    try {
      if (!pipeLog.isCollectFinished()) {
        pipeLog.clear();
        collectHistoryData();
        pipeLog.finishCollect();
      }

      status = PipeStatus.RUNNING;
      senderManager.start();
    } catch (IOException e) {
      logger.error(
          logFormat("Clear pipe dir %s error.", SyncPathUtil.getSenderPipeDir(name, createTime)),
          e);
      throw new PipeException("Start error, can not clear pipe log.");
    }
  }

  /** collect data * */
  private void collectHistoryData() {
    // collect history TsFile
    for (Map.Entry<String, ISyncManager> entry : syncManagerMap.entrySet()) {
      List<File> historyTsFiles = entry.getValue().syncHistoryTsFile(dataStartTime);
      // put history data into PipeDataQueue
      int historyTsFilesSize = historyTsFiles.size();
      for (int i = 0; i < historyTsFilesSize; i++) {
        long serialNumber = 1 - historyTsFilesSize + i;
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
      if (!syncDelOp) {
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
    return syncManagerMap.computeIfAbsent(
        dataRegionId,
        id -> {
          registerDataRegion(id);
          return new LocalSyncManager(
              StorageEngineV2.getInstance().getDataRegion(new DataRegionId(Integer.parseInt(id))),
              this);
        });
  }

  private void registerDataRegion(String dataRegionId) {
    historyQueueMap.put(
        dataRegionId,
        new BufferedPipeDataQueue(
            SyncPathUtil.getSenderDataRegionHistoryPipeLogDir(name, createTime, dataRegionId)));
    realTimeQueueMap.put(
        dataRegionId,
        new BufferedPipeDataQueue(
            SyncPathUtil.getSenderDataRegionRealTimePipeLogDir(name, createTime, dataRegionId)));
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
    if (status == PipeStatus.DROP) {
      throw new PipeException(
          String.format("Can not stop pipe %s, because the pipe is drop.", name));
    }
    senderManager.stop();
    status = PipeStatus.STOP;
  }

  @Override
  public synchronized void drop() throws PipeException {
    if (status == PipeStatus.DROP) {
      return;
    }
    senderManager.close();
    clear();
    status = PipeStatus.DROP;
  }

  private void clear() {
    try {
      historyQueueMap.values().forEach(PipeDataQueue::clear);
      realTimeQueueMap.values().forEach(PipeDataQueue::clear);
      pipeLog.clear();
    } catch (IOException e) {
      logger.warn(logFormat("Clear pipe %s %d error.", name, createTime), e);
    }
  }

  private String logFormat(String format, Object... arguments) {
    return String.format(String.format("[%s-%s] ", this.name, this.createTime) + format, arguments);
  }

  @Override
  public void close() throws PipeException {
    if (status == PipeStatus.DROP) {
      return;
    }
    historyQueueMap.values().forEach(PipeDataQueue::close);
    realTimeQueueMap.values().forEach(PipeDataQueue::close);
    senderManager.close();
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public PipeSink getPipeSink() {
    return pipeSink;
  }

  @Override
  public long getCreateTime() {
    return createTime;
  }

  @Override
  public synchronized PipeStatus getStatus() {
    return status;
  }

  @Override
  public String toString() {
    return "TsFilePipe{"
        + "createTime="
        + createTime
        + ", name='"
        + name
        + '\''
        + ", pipeSink="
        + pipeSink
        + ", dataStartTime="
        + dataStartTime
        + ", syncDelOp="
        + syncDelOp
        + ", pipeLog="
        + pipeLog
        + ", maxSerialNumber="
        + maxSerialNumber
        + ", status="
        + status
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TsFilePipe that = (TsFilePipe) o;
    return createTime == that.createTime
        && Objects.equals(name, that.name)
        && Objects.equals(pipeSink, that.pipeSink);
  }

  @Override
  public int hashCode() {
    return Objects.hash(createTime, name, pipeSink);
  }

  @Override
  public SenderManager getSenderManager() {
    return senderManager;
  }
}
