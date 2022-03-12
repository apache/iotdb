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
package org.apache.iotdb.db.newsync.sender.pipe;

import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.storagegroup.virtualSg.StorageGroupManager;
import org.apache.iotdb.db.exception.PipeException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.newsync.conf.SyncConstant;
import org.apache.iotdb.db.newsync.conf.SyncPathUtil;
import org.apache.iotdb.db.newsync.pipedata.DeletionPipeData;
import org.apache.iotdb.db.newsync.pipedata.PipeData;
import org.apache.iotdb.db.newsync.pipedata.SchemaPipeData;
import org.apache.iotdb.db.newsync.pipedata.TsFilePipeData;
import org.apache.iotdb.db.newsync.pipedata.queue.BufferedPipeDataQueue;
import org.apache.iotdb.db.newsync.sender.recovery.TsFilePipeLogger;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.iotdb.db.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;

public class TsFilePipe implements Pipe {
  private static final Logger logger = LoggerFactory.getLogger(TsFilePipe.class);

  private final long createTime;
  private final String name;
  private final PipeSink pipeSink;
  private final long dataStartTime;
  private final boolean syncDelOp;

  private final BufferedPipeDataQueue historyQueue;
  private final BufferedPipeDataQueue realTimeQueue;
  private final TsFilePipeLogger pipeLog;
  private final ReentrantLock collectRealTimeDataLock;

  private final ExecutorService singleExecutorService;

  private boolean isCollectingRealTimeData;
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
        new BufferedPipeDataQueue(SyncPathUtil.getSenderHistoryPipeDataDir(name, createTime));
    this.realTimeQueue =
        new BufferedPipeDataQueue(SyncPathUtil.getSenderRealTimePipeDataDir(name, createTime));
    this.pipeLog = new TsFilePipeLogger(this);
    this.collectRealTimeDataLock = new ReentrantLock();

    this.singleExecutorService =
        IoTDBThreadPoolFactory.newSingleThreadExecutor(
            ThreadName.PIPE_SERVICE.getName() + "-" + name);

    this.isCollectingRealTimeData = false;
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

      //      singleExecutorService.submit(this::transport);
      status = PipeStatus.RUNNING;
    } catch (IOException e) {
      logger.error(
          String.format(
              "Clear pipe dir %s error, because %s.",
              SyncPathUtil.getSenderPipeDir(name, createTime), e));
      throw new PipeException("Start error, can not clear pipe log.");
    }
  }

  /** collect data * */
  private void collectData() {
    registerMetadata();
    List<PhysicalPlan> historyMetadata = collectHistoryMetadata();
    List<Pair<File, Long>> historyTsFiles = collectTsFile();
    isCollectingRealTimeData = true;

    // get all history data
    int historyMetadataSize = historyMetadata.size();
    int historyTsFilesSize = historyTsFiles.size();
    List<PipeData> historyData = new ArrayList<>();
    for (int i = 0; i < historyMetadataSize; i++) {
      long serialNumber = 1 - historyTsFilesSize - historyMetadataSize + i;
      historyData.add(new SchemaPipeData(historyMetadata.get(i), serialNumber));
    }
    for (int i = 0; i < historyTsFilesSize; i++) {
      long serialNumber = 1 - historyTsFilesSize + i;
      try {
        File hardLink =
            pipeLog.createTsFileAndModsHardlink(
                historyTsFiles.get(i).left, historyTsFiles.get(i).right);
        historyData.add(new TsFilePipeData(hardLink.getPath(), serialNumber));
      } catch (IOException e) {
        logger.warn(
            String.format(
                "Create hard link for tsfile %s error, serial number is %d, because %s.",
                historyTsFiles.get(i).left.getPath(), serialNumber, e));
      }
    }

    // add history data into blocking deque
    int historyDataSize = historyData.size();
    for (int i = 0; i < historyDataSize; i++) {
      historyQueue.offer(historyData.get(i));
    }
  }

  private void registerMetadata() {
    IoTDB.metaManager.registerSyncTask(this);
  }

  private void deregisterMetadata() {
    IoTDB.metaManager.deregisterSyncTask();
  }

  private List<PhysicalPlan> collectHistoryMetadata() {
    List<PhysicalPlan> historyMetadata = new ArrayList<>();
    List<SetStorageGroupPlan> storageGroupPlanList = IoTDB.metaManager.getStorageGroupAsPlan();
    for (SetStorageGroupPlan storageGroupPlan : storageGroupPlanList) {
      historyMetadata.add(storageGroupPlan);
      PartialPath sgPath = storageGroupPlan.getPath();
      try {
        historyMetadata.addAll(
            IoTDB.metaManager.getTimeseriesAsPlan(sgPath.concatNode(MULTI_LEVEL_PATH_WILDCARD)));
      } catch (MetadataException e) {
        logger.warn(
            String.format(
                "Collect history metadata from sg: %s error. Skip this sg.", sgPath.getFullPath()));
      }
    }
    return historyMetadata;
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
    StorageEngine.getInstance().registerSyncDataCollector(this);
    Iterator<Map.Entry<PartialPath, StorageGroupManager>> sgIterator =
        StorageEngine.getInstance().getProcessorMap().entrySet().iterator();
    while (sgIterator.hasNext()) {
      sgIterator.next().getValue().registerSyncDataCollector(this);
    }
  }

  private void deregisterTsFile() {
    StorageEngine.getInstance().registerSyncDataCollector(null);
    Iterator<Map.Entry<PartialPath, StorageGroupManager>> sgIterator =
        StorageEngine.getInstance().getProcessorMap().entrySet().iterator();
    while (sgIterator.hasNext()) {
      sgIterator.next().getValue().registerSyncDataCollector(null);
    }
  }

  private List<Pair<File, Long>> collectTsFile() {
    List<Pair<File, Long>> historyTsFiles = new ArrayList<>();
    StorageEngine.getInstance().registerSyncDataCollector(this);
    Iterator<Map.Entry<PartialPath, StorageGroupManager>> sgIterator =
        StorageEngine.getInstance().getProcessorMap().entrySet().iterator();
    while (sgIterator.hasNext()) {
      historyTsFiles.addAll(sgIterator.next().getValue().collectDataForSync(this, dataStartTime));
    }
    return historyTsFiles;
  }

  public void collectRealTimeDeletion(Deletion deletion) {
    collectRealTimeDataLock.lock();
    try {
      if (!syncDelOp) {
        return;
      }

      for (PartialPath deletePath :
          IoTDB.metaManager.splitPathPatternByDevice(deletion.getPath())) {
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
      logger.warn(String.format("Collect deletion %s error, because %s.", deletion, e));
    } finally {
      collectRealTimeDataLock.unlock();
    }
  }

  public void collectRealTimeTsFile(File tsFile) {
    collectRealTimeDataLock.lock();
    try {
      maxSerialNumber += 1L;
      PipeData tsFileData =
          new TsFilePipeData(pipeLog.createTsFileHardlink(tsFile).getPath(), maxSerialNumber);
      realTimeQueue.offer(tsFileData);
    } catch (IOException e) {
      logger.warn(
          String.format(
              "Create Hardlink tsfile %s on disk error, serial number is %d, because %s.",
              tsFile.getPath(), maxSerialNumber, e));
    } finally {
      collectRealTimeDataLock.unlock();
    }
  }

  public void collectRealTimeTsFileResource(File tsFile) {
    try {
      pipeLog.createTsFileResourceHardlink(tsFile);
    } catch (IOException e) {
      logger.warn(
          String.format(
              "Record tsfile resource %s on disk error, because %s.", tsFile.getPath(), e));
    }
  }

  /** transport data * */
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

  public void commit() {
    if (!historyQueue.isEmpty()) {
      historyQueue.commit();
    }
    realTimeQueue.commit();
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

    singleExecutorService.shutdownNow();
    try {
      if (!singleExecutorService.awaitTermination(
          SyncConstant.DEFAULT_WAITTING_FOR_STOP_MILLISECONDS, TimeUnit.MILLISECONDS)) {
        throw new PipeException(
            String.format(
                "Stop pipe %s when waiting for stop pipe after %s %s.",
                name,
                SyncConstant.DEFAULT_WAITTING_FOR_STOP_MILLISECONDS,
                TimeUnit.MILLISECONDS.name()));
      }
    } catch (InterruptedException e) {
      logger.warn(
          String.format(
              "Interrupted when waiting for stop pipe %s %d, because %s", name, createTime, e));
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

  private void clear() throws PipeException {
    deregisterMetadata();
    deregisterTsFile();
    isCollectingRealTimeData = false;

    singleExecutorService.shutdownNow();
    try {
      if (!singleExecutorService.awaitTermination(
          SyncConstant.DEFAULT_WAITTING_FOR_STOP_MILLISECONDS, TimeUnit.MILLISECONDS)) {
        throw new PipeException(
            String.format(
                "Clear pipe %s when waiting for stop pipe after %s %s.",
                name,
                SyncConstant.DEFAULT_WAITTING_FOR_STOP_MILLISECONDS,
                TimeUnit.MILLISECONDS.name()));
      }
    } catch (InterruptedException e) {
      logger.warn(
          String.format(
              "Interrupted when waiting for clear pipe %s %d, because %s", name, createTime, e));
    }

    try {
      historyQueue.clear();
      realTimeQueue.clear();
      pipeLog.clear();
    } catch (IOException e) {
      logger.warn(String.format("Clear pipe %s %d error, because %s.", name, createTime, e));
    }
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
}
