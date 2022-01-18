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
import org.apache.iotdb.db.exception.PipeException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.newsync.sender.conf.SenderConf;
import org.apache.iotdb.db.newsync.sender.recovery.TsFilePipeLog;
import org.apache.iotdb.db.newsync.sender.recovery.TsFilePipeLogAnalyzer;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.service.IoTDB;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;

public class TsFilePipe implements Pipe {
  private static final Logger logger = LoggerFactory.getLogger(TsFilePipe.class);

  private final long createTime;
  private final String name;
  private final IoTDBPipeSink pipeSink;
  private final long dataStartTimestamp;
  private final boolean syncDelOp;

  private ExecutorService singleExecutorService;
  private TsFilePipeLog pipeLog;

  private BlockingDeque<TsFilePipeData> pipeData;
  private long maxSerialNumber;

  private PipeStatus status;

  public TsFilePipe(
      long createTime,
      String name,
      IoTDBPipeSink pipeSink,
      long dataStartTimestamp,
      boolean syncDelOp) {
    this.createTime = createTime;
    this.name = name;
    this.pipeSink = pipeSink;
    this.dataStartTimestamp = dataStartTimestamp;
    this.syncDelOp = syncDelOp;

    this.pipeLog = new TsFilePipeLog(this);
    this.singleExecutorService =
        IoTDBThreadPoolFactory.newSingleThreadExecutor(
            ThreadName.PIPE_SERVICE.getName() + "-" + name);

    recover();

    this.status = PipeStatus.STOP;
  }

  private void recover() {
    this.pipeData = new TsFilePipeLogAnalyzer(this).recover();
    this.maxSerialNumber = 0;
    if (pipeData.size() != 0) {
      this.maxSerialNumber = Math.max(maxSerialNumber, pipeData.getLast().getSerialNumber());
    }
  }

  @Override
  public synchronized void start() throws PipeException {
    if (status == PipeStatus.DROP) {
      throw new PipeException(
          String.format("Can not start pipe %s, because the pipe is drop.", name));
    } else if (status == PipeStatus.RUNNING) {
      return;
    }

    try {
      if (!new TsFilePipeLogAnalyzer(this).isCollectFinished()) {
        pipeLog.clear();
        collectData();
        pipeLog.finishCollect();
      }

      singleExecutorService.submit(this::transport);
      status = PipeStatus.RUNNING;
    } catch (IOException e) {
      logger.error(
          String.format("Can not clear pipe dir %s, because %s", SenderConf.getPipeDir(this), e));
      throw new PipeException("Start error, can not clear pipe log.");
    }
  }

  /** collect data * */
  private void collectData() {
    collectRealTimeMetadata();
    collectHistoryMetadata();
    collectTsFileAndDeletion();
  }

  private void collectRealTimeMetadata() {
    IoTDB.metaManager.registerPipe(this);
  }

  private void collectHistoryMetadata() {
    List<SetStorageGroupPlan> storageGroupPlanList = IoTDB.metaManager.getStorageGroupAsPlan();
    for (SetStorageGroupPlan storageGroupPlan : storageGroupPlanList) {
      // todo process sg plan
      PartialPath sgPath = storageGroupPlan.getPath();
      try {
        for (PhysicalPlan timeseriesPlan :
            IoTDB.metaManager.getTimeseriesAsPlan(sgPath.concatNode(MULTI_LEVEL_PATH_WILDCARD))) {}

      } catch (MetadataException e) {

      }
    }
  }

  private void collectTsFileAndDeletion() {}

  public void collectRealTimeMetaData(PhysicalPlan plan) {
    maxSerialNumber += 1;
    TsFilePipeData metaData = new TsFilePipeData(plan, maxSerialNumber);
    try {
      pipeLog.addRealTimePipeData(metaData);
    } catch (IOException e) {
      logger.warn(String.format("Can not record plan pipe data %s.", metaData));
    }
  }

  /** transport data * */
  private void transport() {
    try {
      while (true) {
        if (status == PipeStatus.STOP || status == PipeStatus.DROP) {
          logger.info(String.format("TsFile pipe %s stops transporting data by command.", name));
          break;
        }

        TsFilePipeData data;
        try {
          synchronized (pipeData) {
            if (pipeData.isEmpty()) {
              pipeData.wait();
              pipeData.notifyAll();
            }
            data = pipeData.poll();
          }
        } catch (InterruptedException e) {
          logger.warn(String.format("TsFile pipe %s has been interrupted.", name));
          continue;
        }

        if (data == null) {
          continue;
        }
        if (data.isTsFile()) {
          // senderTransport(data.getTsFiles, data.getLoaderType());
        } else {
          // senderTransport(data.getBytes, data.getLoaderType());
        }
      }
    } catch (Exception e) {
      logger.error(String.format("TsFile pipe %s stops transportng data, because %s", name, e));
    }
  }

  @Override
  public synchronized void stop() throws PipeException {
    if (status == PipeStatus.DROP) {
      throw new PipeException(
          String.format("Can not stop pipe %s, because the pipe is drop.", name));
    }

    status = PipeStatus.STOP;
    synchronized (pipeData) {
      pipeData.notifyAll();
    }
  }

  @Override
  public synchronized void drop() {
    if (status == PipeStatus.DROP) {
      return;
    }

    status = PipeStatus.DROP;
    synchronized (pipeData) {
      pipeData.notifyAll();
    }
    clear();
  }

  private void clear() {}

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
