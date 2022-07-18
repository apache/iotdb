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

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.exception.sync.PipeException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.sync.conf.SyncPathUtil;
import org.apache.iotdb.db.sync.pipedata.DeletionPipeData;
import org.apache.iotdb.db.sync.pipedata.PipeData;
import org.apache.iotdb.db.sync.pipedata.SchemaPipeData;
import org.apache.iotdb.db.sync.pipedata.TsFilePipeData;
import org.apache.iotdb.db.sync.pipedata.queue.BufferedPipeDataQueue;
import org.apache.iotdb.db.sync.sender.manager.SchemaSyncManager;
import org.apache.iotdb.db.sync.sender.manager.TsFileSyncManager;
import org.apache.iotdb.db.sync.sender.recovery.TsFilePipeLogger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

public class TsFilePipe implements Pipe {
  private static final Logger logger = LoggerFactory.getLogger(TsFilePipe.class);
  private final SchemaSyncManager schemaSyncManager = SchemaSyncManager.getInstance();
  private final TsFileSyncManager tsFileSyncManager = TsFileSyncManager.getInstance();

  private final long createTime;
  private final String name;
  private final PipeSink pipeSink;
  private final long dataStartTime;
  private final boolean syncDelOp;

  private final BufferedPipeDataQueue historyQueue;
  private final BufferedPipeDataQueue realTimeQueue;
  private final TsFilePipeLogger pipeLog;
  private final ReentrantLock collectRealTimeDataLock;

  private boolean isCollectingRealTimeData;
  private long maxSerialNumber;
  private boolean disconnected; // true if pipe cannot connect to receiver

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

    this.isCollectingRealTimeData = false;
    this.maxSerialNumber = Math.max(0L, realTimeQueue.getLastMaxSerialNumber());

    this.status = PipeStatus.STOP;
    this.disconnected = false;
  }

  @Override
  public synchronized void start() throws PipeException {
    if (status == PipeStatus.DROP) {
      throw new PipeException(
          String.format("Can not start pipe %s, because the pipe has been drop.", name));
    } else if (status == PipeStatus.RUNNING) {
      return;
    }

    try {
      if (!pipeLog.isCollectFinished()) {
        pipeLog.clear();
        collectData();
        pipeLog.finishCollect();
      }
      if (!isCollectingRealTimeData) {
        registerMetadata();
        registerTsFile();
        isCollectingRealTimeData = true;
      }

      status = PipeStatus.RUNNING;
    } catch (IOException e) {
      logger.error(
          String.format(
              "Clear pipe dir %s error.", SyncPathUtil.getSenderPipeDir(name, createTime)),
          e);
      throw new PipeException("Start error, can not clear pipe log.");
    }
  }

  /** collect data * */
  private void collectData() {
    registerMetadata();
    List<PhysicalPlan> historyMetadata = collectHistoryMetadata();
    List<File> historyTsFiles = registerAndCollectHistoryTsFile();
    isCollectingRealTimeData = true;

    // get all history data
    int historyMetadataSize = historyMetadata.size();
    int historyTsFilesSize = historyTsFiles.size();
    for (int i = 0; i < historyMetadataSize; i++) {
      long serialNumber = 1 - historyTsFilesSize - historyMetadataSize + i;
      historyQueue.offer(new SchemaPipeData(historyMetadata.get(i), serialNumber));
    }
    for (int i = 0; i < historyTsFilesSize; i++) {
      long serialNumber = 1 - historyTsFilesSize + i;
      File tsFile = historyTsFiles.get(i);
      historyQueue.offer(new TsFilePipeData(tsFile.getParent(), tsFile.getName(), serialNumber));
    }
  }

  private void registerMetadata() {
    schemaSyncManager.registerSyncTask(this);
  }

  private void deregisterMetadata() {
    schemaSyncManager.deregisterSyncTask();
  }

  private List<PhysicalPlan> collectHistoryMetadata() {
    return schemaSyncManager.collectHistoryMetadata();
  }

  public void collectRealTimeMetaData(PhysicalPlan plan) {
    collectRealTimeDataLock.lock();
    try {
      maxSerialNumber += 1L;
      PipeData metaData = new SchemaPipeData(plan, maxSerialNumber);
      realTimeQueue.offer(metaData);
    } finally {
      collectRealTimeDataLock.unlock();
    }
  }

  private void registerTsFile() {
    tsFileSyncManager.registerSyncTask(this);
  }

  private void deregisterTsFile() {
    tsFileSyncManager.deregisterSyncTask();
  }

  private List<File> registerAndCollectHistoryTsFile() {
    return tsFileSyncManager.registerAndCollectHistoryTsFile(this, dataStartTime);
  }

  public File createHistoryTsFileHardlink(File tsFile, long modsOffset) {
    collectRealTimeDataLock.lock(); // synchronize the pipeLog.isHardlinkExist
    try {
      if (pipeLog.isHardlinkExist(tsFile)) {
        return null;
      }

      return pipeLog.createTsFileAndModsHardlink(tsFile, modsOffset);
    } catch (IOException e) {
      logger.error(
          String.format("Create hardlink for history tsfile %s error.", tsFile.getPath()), e);
      return null;
    } finally {
      collectRealTimeDataLock.unlock();
    }
  }

  public void collectRealTimeDeletion(Deletion deletion) {
    collectRealTimeDataLock.lock();
    try {
      if (!syncDelOp) {
        return;
      }

      for (PartialPath deletePath :
          schemaSyncManager.splitPathPatternByDevice(deletion.getPath())) {
        Deletion splitDeletion =
            new Deletion(
                deletePath,
                deletion.getFileOffset(),
                deletion.getStartTime(),
                deletion.getEndTime());
        maxSerialNumber += 1L;
        PipeData deletionData = new DeletionPipeData(splitDeletion, maxSerialNumber);
        realTimeQueue.offer(deletionData);
      }
    } catch (MetadataException e) {
      logger.warn(String.format("Collect deletion %s error.", deletion), e);
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
          String.format(
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
      logger.warn(String.format("Record tsfile resource %s on disk error.", tsFile.getPath()), e);
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
  public void setDisconnected(boolean disconnected) {
    this.disconnected = disconnected;
  }

  @Override
  public boolean isDisconnected() {
    return disconnected;
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

    if (!isCollectingRealTimeData) {
      registerMetadata();
      registerTsFile();
      isCollectingRealTimeData = true;
    }

    status = PipeStatus.STOP;
  }

  @Override
  public synchronized void drop() throws PipeException {
    if (status == PipeStatus.DROP) {
      return;
    }

    clear();
    status = PipeStatus.DROP;
  }

  private void clear() {
    deregisterMetadata();
    deregisterTsFile();
    isCollectingRealTimeData = false;

    try {
      historyQueue.clear();
      realTimeQueue.clear();
      pipeLog.clear();
    } catch (IOException e) {
      logger.warn(String.format("Clear pipe %s %d error.", name, createTime), e);
    }
  }

  @Override
  public void close() throws PipeException {
    if (status == PipeStatus.DROP) {
      return;
    }

    deregisterMetadata();
    deregisterTsFile();
    isCollectingRealTimeData = false;

    historyQueue.close();
    realTimeQueue.close();
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
        + ", isCollectingRealTimeData="
        + isCollectingRealTimeData
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
}
