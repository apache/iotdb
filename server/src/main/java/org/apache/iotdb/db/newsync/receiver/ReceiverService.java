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
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.newsync.receiver.collector.Collector;
import org.apache.iotdb.db.newsync.receiver.manager.PipeInfo;
import org.apache.iotdb.db.newsync.receiver.manager.ReceiverManager;
import org.apache.iotdb.db.qp.physical.sys.ShowPipeServerPlan;
import org.apache.iotdb.db.query.dataset.ListDataSet;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Binary;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.db.conf.IoTDBConstant.*;

public class ReceiverService implements IService {
  private static final Logger logger = LoggerFactory.getLogger(ReceiverService.class);
  private final SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  private static final ReceiverManager receiverManager = ReceiverManager.getInstance();
  private Collector collector;

  /** start receiver service */
  public boolean startPipeServer() {
    try {
      receiverManager.startServer();
      // TODO: start socket and collector
    } catch (IOException e) {
      logger.error(e.getMessage());
      return false;
    }
    return true;
  }

  /** stop receiver service */
  public boolean stopPipeServer() {
    try {
      receiverManager.stopServer();
      // TODO: stop socket and collector
    } catch (IOException e) {
      logger.error(e.getMessage());
      return false;
    }
    return true;
  }

  /** create and start a new pipe named pipeName */
  public void createPipe(String pipeName, String remoteIp, long startTime) throws IOException {
    receiverManager.createPipe(pipeName, remoteIp, startTime);
  }

  /** start an existed pipe named pipeName */
  public void startPipe(String pipeName, String remoteIp) throws IOException {
    receiverManager.startPipe(pipeName, remoteIp);
  }

  /** stop an existed pipe named pipeName */
  public void stopPipe(String pipeName, String remoteIp) throws IOException {
    receiverManager.stopPipe(pipeName, remoteIp);
  }

  /** drop an existed pipe named pipeName */
  public void dropPipe(String pipeName, String remoteIp) throws IOException {
    receiverManager.dropPipe(pipeName, remoteIp);
  }

  /**
   * query by sql SHOW PIPE
   *
   * @return QueryDataSet contained three columns: pipe name, status and start time
   */
  public QueryDataSet showPipe(ShowPipeServerPlan plan) {
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
    pipeCreateTimeField.setBinaryV(new Binary(dateformat.format(pipeInfo.getCreateTime())));
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
      startPipeServer();
    }
  }

  @Override
  public void stop() {
    stopPipeServer();
    try {
      receiverManager.close();
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
