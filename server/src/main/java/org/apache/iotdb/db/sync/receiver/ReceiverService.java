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
 */
package org.apache.iotdb.db.sync.receiver;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.db.exception.sync.PipeServerException;
import org.apache.iotdb.db.qp.physical.sys.ShowPipePlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPipeServerPlan;
import org.apache.iotdb.db.qp.utils.DatetimeUtils;
import org.apache.iotdb.db.query.dataset.ListDataSet;
import org.apache.iotdb.db.sync.conf.SyncPathUtil;
import org.apache.iotdb.db.sync.pipedata.queue.PipeDataQueueFactory;
import org.apache.iotdb.db.sync.receiver.collector.Collector;
import org.apache.iotdb.db.sync.receiver.manager.PipeInfo;
import org.apache.iotdb.db.sync.receiver.manager.PipeMessage;
import org.apache.iotdb.db.sync.receiver.manager.ReceiverManager;
import org.apache.iotdb.db.sync.sender.pipe.Pipe.PipeStatus;
import org.apache.iotdb.db.sync.transport.server.TransportServerManager;
import org.apache.iotdb.service.transport.thrift.ResponseType;
import org.apache.iotdb.service.transport.thrift.SyncRequest;
import org.apache.iotdb.service.transport.thrift.SyncResponse;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Binary;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_PIPESERVER_STATUS;

public class ReceiverService implements IService {
  private static final Logger logger = LoggerFactory.getLogger(ReceiverService.class);
  private static final ReceiverManager receiverManager = ReceiverManager.getInstance();
  private final Collector collector;

  /**
   * start receiver service
   *
   * @param isRecovery if isRecovery, it will ignore check and force a start
   */
  public synchronized void startPipeServer(boolean isRecovery) throws PipeServerException {
    if (receiverManager.isPipeServerEnable() && !isRecovery) {
      return;
    }
    try {
      TransportServerManager.getInstance().startService();
      receiverManager.startServer();
      collector.startCollect();
    } catch (IOException | StartupException e) {
      throw new PipeServerException("Failed to start pipe server because " + e.getMessage());
    }
  }

  /** stop receiver service */
  public synchronized void stopPipeServer() throws PipeServerException {
    if (!receiverManager.isPipeServerEnable()) {
      return;
    }
    try {
      List<PipeInfo> pipeInfos = receiverManager.getAllPipeInfos();
      for (PipeInfo pipeInfo : pipeInfos) {
        if (pipeInfo.getStatus().equals(PipeStatus.RUNNING)) {
          throw new PipeServerException(
              "Failed to stop pipe server because there is pipe still running.");
        }
      }
      TransportServerManager.getInstance().stopService();
      receiverManager.stopServer();
      collector.stopCollect();
    } catch (IOException e) {
      throw new PipeServerException("Failed to start pipe server because " + e.getMessage());
    }
  }

  private void checkPipe(String pipeName, String remoteIp, long createTime) throws IOException {
    PipeInfo pipeInfo = receiverManager.getPipeInfo(pipeName, remoteIp, createTime);
    if (pipeInfo != null && pipeInfo.getStatus().equals(PipeStatus.STOP)) {
      startPipe(pipeName, remoteIp, createTime);
    }
  }

  /** heartbeat RPC handle */
  public synchronized SyncResponse receiveMsg(SyncRequest request) {
    SyncResponse response = new SyncResponse(ResponseType.INFO, "");
    try {
      switch (request.getType()) {
        case HEARTBEAT:
          checkPipe(request.getPipeName(), request.getRemoteIp(), request.getCreateTime());
          PipeMessage message =
              receiverManager.getPipeMessage(
                  request.getPipeName(), request.getRemoteIp(), request.getCreateTime(), true);
          switch (message.getType()) {
            case INFO:
              break;
            case WARN:
              response = new SyncResponse(ResponseType.WARN, "");
              break;
            case ERROR:
              response = new SyncResponse(ResponseType.ERROR, "");
              break;
            default:
              throw new UnsupportedOperationException("Wrong message type " + message.getType());
          }
          break;
        case CREATE:
          createPipe(request.getPipeName(), request.getRemoteIp(), request.getCreateTime());
          break;
        case START:
          startPipe(request.getPipeName(), request.getRemoteIp(), request.getCreateTime());
          break;
        case STOP:
          stopPipe(request.getPipeName(), request.getRemoteIp(), request.getCreateTime());
          break;
        case DROP:
          dropPipe(request.getPipeName(), request.getRemoteIp(), request.getCreateTime());
          break;
      }
    } catch (IOException e) {
      logger.warn("Cannot handle message because {}", e.getMessage());
    }
    return response;
  }

  /** create and start a new pipe named pipeName */
  private void createPipe(String pipeName, String remoteIp, long createTime) throws IOException {
    PipeInfo pipeInfo = receiverManager.getPipeInfo(pipeName, remoteIp, createTime);
    if (pipeInfo == null || pipeInfo.getStatus().equals(PipeStatus.DROP)) {
      logger.info(
          "create Pipe name={}, remoteIp={}, createTime={}", pipeName, remoteIp, createTime);
      createDir(pipeName, remoteIp, createTime);
      receiverManager.createPipe(pipeName, remoteIp, createTime);
    }
  }

  /** start an existed pipe named pipeName */
  private void startPipe(String pipeName, String remoteIp, long createTime) throws IOException {
    PipeInfo pipeInfo = receiverManager.getPipeInfo(pipeName, remoteIp, createTime);
    if (pipeInfo != null && pipeInfo.getStatus().equals(PipeStatus.STOP)) {
      logger.info("start Pipe name={}, remoteIp={}, createTime={}", pipeName, remoteIp, createTime);
      receiverManager.startPipe(pipeName, remoteIp, createTime);
      collector.startPipe(pipeName, remoteIp, createTime);
    }
  }

  /** stop an existed pipe named pipeName */
  private void stopPipe(String pipeName, String remoteIp, long createTime) throws IOException {
    PipeInfo pipeInfo = receiverManager.getPipeInfo(pipeName, remoteIp, createTime);
    if (pipeInfo != null && pipeInfo.getStatus().equals(PipeStatus.RUNNING)) {
      logger.info("stop Pipe name={}, remoteIp={}, createTime={}", pipeName, remoteIp, createTime);
      receiverManager.stopPipe(pipeName, remoteIp, createTime);
      collector.stopPipe(pipeName, remoteIp, createTime);
    }
  }

  /** drop an existed pipe named pipeName */
  private void dropPipe(String pipeName, String remoteIp, long createTime) throws IOException {
    PipeInfo pipeInfo = receiverManager.getPipeInfo(pipeName, remoteIp, createTime);
    if (pipeInfo != null && !pipeInfo.getStatus().equals(PipeStatus.DROP)) {
      logger.info("drop Pipe name={}, remoteIp={}, createTime={}", pipeName, remoteIp, createTime);
      receiverManager.dropPipe(pipeName, remoteIp, createTime);
      collector.stopPipe(pipeName, remoteIp, createTime);
      PipeDataQueueFactory.removeBufferedPipeDataQueue(
          SyncPathUtil.getReceiverPipeLogDir(pipeName, remoteIp, createTime));
      File dir = new File(SyncPathUtil.getReceiverPipeDir(pipeName, remoteIp, createTime));
      FileUtils.deleteDirectory(dir);
    }
  }

  private void createDir(String pipeName, String remoteIp, long createTime) {
    File f = new File(SyncPathUtil.getReceiverFileDataDir(pipeName, remoteIp, createTime));
    if (!f.exists()) {
      f.mkdirs();
    }
    f = new File(SyncPathUtil.getReceiverPipeLogDir(pipeName, remoteIp, createTime));
    if (!f.exists()) {
      f.mkdirs();
    }
  }

  /**
   * query by sql SHOW PIPESERVER STATUS
   *
   * @return QueryDataSet contained one column: enable
   */
  public QueryDataSet showPipeServer(ShowPipeServerPlan plan) {
    ListDataSet dataSet =
        new ListDataSet(
            Collections.singletonList(new PartialPath(COLUMN_PIPESERVER_STATUS, false)),
            Collections.singletonList(TSDataType.BOOLEAN));
    RowRecord rowRecord = new RowRecord(0);
    Field status = new Field(TSDataType.BOOLEAN);
    status.setBoolV(receiverManager.isPipeServerEnable());
    rowRecord.addField(status);
    dataSet.putRecord(rowRecord);
    return dataSet;
  }

  /** query by sql SHOW PIPE */
  public QueryDataSet showPipe(ShowPipePlan plan, ListDataSet dataSet) {
    List<PipeInfo> pipeInfos;
    if (!StringUtils.isEmpty(plan.getPipeName())) {
      pipeInfos = receiverManager.getPipeInfosByPipeName(plan.getPipeName());
    } else {
      pipeInfos = receiverManager.getAllPipeInfos();
    }
    for (PipeInfo pipeInfo : pipeInfos) {
      putPipeRecord(dataSet, pipeInfo);
    }
    return dataSet;
  }

  private void putPipeRecord(ListDataSet dataSet, PipeInfo pipeInfo) {
    RowRecord record = new RowRecord(0);
    record.addField(
        Binary.valueOf(DatetimeUtils.convertLongToDate(pipeInfo.getCreateTime())), TSDataType.TEXT);
    record.addField(Binary.valueOf(pipeInfo.getPipeName()), TSDataType.TEXT);
    record.addField(Binary.valueOf(IoTDBConstant.SYNC_RECEIVER_ROLE), TSDataType.TEXT);
    record.addField(Binary.valueOf(pipeInfo.getRemoteIp()), TSDataType.TEXT);
    record.addField(Binary.valueOf(pipeInfo.getStatus().name()), TSDataType.TEXT);
    record.addField(
        Binary.valueOf(
            receiverManager
                .getPipeMessage(
                    pipeInfo.getPipeName(), pipeInfo.getRemoteIp(), pipeInfo.getCreateTime(), false)
                .getMsg()),
        TSDataType.TEXT);
    dataSet.putRecord(record);
  }

  private ReceiverService() {
    collector = new Collector();
  }

  public static ReceiverService getInstance() {
    return ReceiverServiceHolder.INSTANCE;
  }

  /** IService * */
  @Override
  public void start() throws StartupException {
    receiverManager.init();
    if (receiverManager.isPipeServerEnable()) {
      try {
        startPipeServer(true);
      } catch (PipeServerException e) {
        throw new StartupException(e.getMessage());
      }
    }
  }

  @Override
  public void stop() {
    try {
      receiverManager.close();
      collector.stopCollect();
    } catch (IOException e) {
      logger.error(e.getMessage());
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.RECEIVER_SERVICE;
  }

  private static class ReceiverServiceHolder {
    private static final ReceiverService INSTANCE = new ReceiverService();

    private ReceiverServiceHolder() {}
  }
}
