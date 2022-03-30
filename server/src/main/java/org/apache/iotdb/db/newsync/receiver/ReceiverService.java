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
package org.apache.iotdb.db.newsync.receiver;

import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.sync.PipeServerException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.newsync.conf.SyncPathUtil;
import org.apache.iotdb.db.newsync.pipedata.queue.PipeDataQueueFactory;
import org.apache.iotdb.db.newsync.receiver.collector.Collector;
import org.apache.iotdb.db.newsync.receiver.manager.PipeInfo;
import org.apache.iotdb.db.newsync.receiver.manager.PipeMessage;
import org.apache.iotdb.db.newsync.receiver.manager.ReceiverManager;
import org.apache.iotdb.db.newsync.sender.pipe.Pipe.PipeStatus;
import org.apache.iotdb.db.newsync.transport.server.TransportServerManager;
import org.apache.iotdb.db.qp.physical.sys.ShowPipeServerPlan;
import org.apache.iotdb.db.qp.utils.DatetimeUtils;
import org.apache.iotdb.db.query.dataset.ListDataSet;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
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
import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.db.conf.IoTDBConstant.*;

public class ReceiverService implements IService {
  private static final Logger logger = LoggerFactory.getLogger(ReceiverService.class);
  private static final ReceiverManager receiverManager = ReceiverManager.getInstance();
  private Collector collector;

  /** start receiver service */
  public void startPipeServer() throws PipeServerException {
    try {
      TransportServerManager.getInstance().startService();
      receiverManager.startServer();
      collector.startCollect();
      // recover started pipe
      List<PipeInfo> pipeInfos = receiverManager.getAllPipeInfos();
      for (PipeInfo pipeInfo : pipeInfos) {
        if (pipeInfo.getStatus().equals(PipeStatus.RUNNING)) {
          collector.startPipe(
              pipeInfo.getPipeName(), pipeInfo.getRemoteIp(), pipeInfo.getCreateTime());
        }
      }
    } catch (IOException | StartupException e) {
      throw new PipeServerException("Failed to start pipe server because " + e.getMessage());
    }
  }

  /** stop receiver service */
  public void stopPipeServer() throws PipeServerException {
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

  /** heartbeat RPC handle */
  public SyncResponse recMsg(SyncRequest request) {
    SyncResponse response = new SyncResponse(ResponseType.INFO, "");
    ;
    try {
      switch (request.getType()) {
        case HEARTBEAT:
          List<PipeMessage> messageList =
              receiverManager.getPipeMessages(
                  request.getPipeName(), request.getRemoteIp(), request.getCreateTime());
          PipeMessage message = new PipeMessage(PipeMessage.MsgType.INFO, "");
          if (!messageList.isEmpty()) {
            for (PipeMessage pipeMessage : messageList) {
              if (pipeMessage.getType().getValue() > message.getType().getValue()) {
                message = pipeMessage;
              }
            }
          }
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
    logger.info("create Pipe name={}, remoteIp={}, createTime={}", pipeName, remoteIp, createTime);
    createDir(pipeName, remoteIp, createTime);
    receiverManager.createPipe(pipeName, remoteIp, createTime);
  }

  /** start an existed pipe named pipeName */
  private void startPipe(String pipeName, String remoteIp, long createTime) throws IOException {
    logger.info("start Pipe name={}, remoteIp={}, createTime={}", pipeName, remoteIp, createTime);
    receiverManager.startPipe(pipeName, remoteIp);
    collector.startPipe(pipeName, remoteIp, createTime);
  }

  /** stop an existed pipe named pipeName */
  private void stopPipe(String pipeName, String remoteIp, long createTime) throws IOException {
    logger.info("stop Pipe name={}, remoteIp={}, createTime={}", pipeName, remoteIp, createTime);
    receiverManager.stopPipe(pipeName, remoteIp);
    collector.stopPipe(pipeName, remoteIp, createTime);
  }

  /** drop an existed pipe named pipeName */
  private void dropPipe(String pipeName, String remoteIp, long createTime) throws IOException {
    logger.info("drop Pipe name={}, remoteIp={}, createTime={}", pipeName, remoteIp, createTime);
    receiverManager.dropPipe(pipeName, remoteIp);
    collector.stopPipe(pipeName, remoteIp, createTime);
    PipeDataQueueFactory.removeBufferedPipeDataQueue(
        SyncPathUtil.getReceiverPipeLogDir(pipeName, remoteIp, createTime));
    File dir = new File(SyncPathUtil.getReceiverPipeDir(pipeName, remoteIp, createTime));
    FileUtils.deleteDirectory(dir);
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
   * query by sql SHOW PIPE
   *
   * @return QueryDataSet contained three columns: pipe name, status and start time
   */
  public QueryDataSet showPipe(ShowPipeServerPlan plan) throws PipeServerException {
    if (!receiverManager.isPipeServerEnable()) {
      throw new PipeServerException("Pipe server is not started.");
    }
    ListDataSet dataSet =
        new ListDataSet(
            Arrays.asList(
                new PartialPath(COLUMN_PIPE_NAME, false),
                new PartialPath(COLUMN_PIPE_REMOTE_IP, false),
                new PartialPath(COLUMN_PIPE_STATUS, false),
                new PartialPath(COLUMN_CREATED_TIME, false)),
            Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT));
    List<PipeInfo> pipeInfos;
    if (!StringUtils.isEmpty(plan.getPipeName())) {
      pipeInfos = receiverManager.getPipeInfos(plan.getPipeName());
    } else {
      pipeInfos = receiverManager.getAllPipeInfos();
    }
    for (PipeInfo pipeInfo : pipeInfos) {
      putPipeRecord(dataSet, pipeInfo);
    }
    return dataSet;
  }

  private void putPipeRecord(ListDataSet dataSet, PipeInfo pipeInfo) {
    RowRecord rowRecord = new RowRecord(0);
    Field pipeNameField = new Field(TSDataType.TEXT);
    Field pipeRemoteIp = new Field(TSDataType.TEXT);
    Field pipeStatusField = new Field(TSDataType.TEXT);
    Field pipeCreateTimeField = new Field(TSDataType.TEXT);
    pipeNameField.setBinaryV(new Binary(pipeInfo.getPipeName()));
    pipeRemoteIp.setBinaryV(new Binary(pipeInfo.getRemoteIp()));
    pipeStatusField.setBinaryV(new Binary(pipeInfo.getStatus().name()));
    pipeCreateTimeField.setBinaryV(
        new Binary(DatetimeUtils.convertLongToDate(pipeInfo.getCreateTime())));
    rowRecord.addField(pipeNameField);
    rowRecord.addField(pipeRemoteIp);
    rowRecord.addField(pipeStatusField);
    rowRecord.addField(pipeCreateTimeField);
    dataSet.putRecord(rowRecord);
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
        startPipeServer();
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
