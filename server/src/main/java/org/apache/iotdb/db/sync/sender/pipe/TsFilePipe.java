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
import java.util.concurrent.locks.ReentrantLock;

public class TsFilePipe implements Pipe {
  private static final Logger logger = LoggerFactory.getLogger(TsFilePipe.class);
  // <dataNodeId, ISyncManager>
  private final Map<String, ISyncManager> syncManagerMap = new ConcurrentHashMap<>();

  private final long createTime;
  private final String name;
  private final PipeSink pipeSink;
  private final long dataStartTime;
  private final boolean syncDelOp;

  private final BufferedPipeDataQueue historyQueue;
  private final BufferedPipeDataQueue realTimeQueue;
  private final TsFilePipeLogger pipeLog;
  private final ReentrantLock collectRealTimeDataLock;

  /* handle rpc send logic in sender-side*/
  private final SenderManager senderManager;

  private long maxSerialNumber;

  private PipeStatus status;

  public TsFilePipe(
      long createTime, String name, PipeSink pipeSink, long dataStartTime, boolean syncDelOp) {
    this.createTime = createTime;
    this.name = name;
    this.pipeSink = pipeSink;
    this.dataStartTime = dataStartTime;
    this.syncDelOp = syncDelOp;

    this.historyQueue =
        new BufferedPipeDataQueue(SyncPathUtil.getSenderHistoryPipeLogDir(name, createTime));
    this.realTimeQueue =
        new BufferedPipeDataQueue(SyncPathUtil.getSenderRealTimePipeLogDir(name, createTime));
    this.pipeLog = new TsFilePipeLogger(this);
    this.collectRealTimeDataLock = new ReentrantLock();
    this.senderManager = new SenderManager(this, pipeSink);

    this.maxSerialNumber = Math.max(0L, realTimeQueue.getLastMaxSerialNumber());

    this.status = PipeStatus.STOP;
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
      senderManager.registerDataRegion(dataRegion.getDataRegionId());
      syncManagerMap.put(dataRegion.getDataRegionId(), new LocalSyncManager(dataRegion, this));
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
    List<File> historyTsFiles = new ArrayList<>();
    for (ISyncManager syncManager : syncManagerMap.values()) {
      historyTsFiles.addAll(syncManager.syncHistoryTsFile(dataStartTime));
    }
    // put history data into PipeDataQueue
    int historyTsFilesSize = historyTsFiles.size();
    for (int i = 0; i < historyTsFilesSize; i++) {
      long serialNumber = 1 - historyTsFilesSize + i;
      File tsFile = historyTsFiles.get(i);
      historyQueue.offer(new TsFilePipeData(tsFile.getParent(), tsFile.getName(), serialNumber));
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

  public void collectRealTimeDeletion(Deletion deletion, String sgName) {
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
        maxSerialNumber += 1L;
        PipeData deletionData = new DeletionPipeData(sgName, splitDeletion, maxSerialNumber);
        realTimeQueue.offer(deletionData);
      }
    } catch (MetadataException e) {
      logger.warn(logFormat("Collect deletion %s error.", deletion), e);
    } finally {
      collectRealTimeDataLock.unlock();
    }
  }

  public void collectRealTimeTsFile(File tsFile) {
    collectRealTimeDataLock.lock();
    try {
      if (pipeLog.isHardlinkExist(tsFile)) {
        return;
      }

      maxSerialNumber += 1L;
      File hardlink = pipeLog.createTsFileHardlink(tsFile);
      PipeData tsFileData =
          new TsFilePipeData(hardlink.getParent(), hardlink.getName(), maxSerialNumber);
      realTimeQueue.offer(tsFileData);
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
  public PipeData take() throws InterruptedException {
    if (!historyQueue.isEmpty()) {
      return historyQueue.take();
    }
    return realTimeQueue.take();
  }

  @Override
  public PipeData take(String dataRegionId) throws InterruptedException {
    return null;
  }

  public List<PipeData> pull(long serialNumber) {
    List<PipeData> pullPipeData = new ArrayList<>();
    if (!historyQueue.isEmpty()) {
      pullPipeData.addAll(historyQueue.pull(serialNumber));
    }
    if (serialNumber > 0) {
      pullPipeData.addAll(realTimeQueue.pull(serialNumber));
    }
    return pullPipeData;
  }

  @Override
  public void commit() {
    if (!historyQueue.isEmpty()) {
      historyQueue.commit();
    }
    realTimeQueue.commit();
  }

  @Override
  public void commit(String dataRegionId) {}

  @Override
  public ISyncManager getOrCreateSyncManager(String dataRegionId) {
    return syncManagerMap.computeIfAbsent(
        dataRegionId,
        id -> {
          senderManager.registerDataRegion(id);
          return new LocalSyncManager(
              StorageEngineV2.getInstance().getDataRegion(new DataRegionId(Integer.parseInt(id))),
              this);
        });
  }

  @Override
  public void unregisterDataRegion(String dataRegionId) {
    ISyncManager syncManager = syncManagerMap.remove(dataRegionId);
    if (syncManager != null) {
      syncManager.delete();
      senderManager.unregisterDataRegion(dataRegionId);
    }
  }

  public void commit(long serialNumber) {
    if (!historyQueue.isEmpty()) {
      historyQueue.commit(serialNumber);
    }
    if (serialNumber > 0) {
      realTimeQueue.commit(serialNumber);
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
      historyQueue.clear();
      realTimeQueue.clear();
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
    historyQueue.close();
    realTimeQueue.close();
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
