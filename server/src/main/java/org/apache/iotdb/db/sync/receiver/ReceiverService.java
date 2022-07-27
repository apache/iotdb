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
import org.apache.iotdb.commons.sync.SyncPathUtil;
import org.apache.iotdb.db.exception.sync.PipeServerException;
import org.apache.iotdb.db.qp.physical.sys.ShowPipePlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPipeServerPlan;
import org.apache.iotdb.db.qp.utils.DatetimeUtils;
import org.apache.iotdb.db.query.dataset.ListDataSet;
import org.apache.iotdb.db.sync.receiver.manager.LocalSyncInfo;
import org.apache.iotdb.db.sync.receiver.manager.PipeInfo;
import org.apache.iotdb.db.sync.transport.server.TransportServerManager;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Binary;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_PIPESERVER_STATUS;

public class ReceiverService implements IService {
  private static final Logger logger = LoggerFactory.getLogger(ReceiverService.class);
  private LocalSyncInfo receiverInfo;

  /**
   * start receiver service
   *
   * @param isRecovery if isRecovery, it will ignore check and force a start
   */
  public synchronized void startPipeServer(boolean isRecovery) throws PipeServerException {
    if (receiverInfo.isPipeServerEnable() && !isRecovery) {
      return;
    }
    try {
      TransportServerManager.getInstance().startService();
      receiverInfo.startServer();
    } catch (IOException | StartupException e) {
      throw new PipeServerException("Failed to start pipe server because " + e.getMessage());
    }
  }

  /** stop receiver service */
  public synchronized void stopPipeServer() throws PipeServerException {
    if (!receiverInfo.isPipeServerEnable()) {
      return;
    }
    try {
      TransportServerManager.getInstance().stopService();
      receiverInfo.stopServer();
    } catch (IOException e) {
      throw new PipeServerException("Failed to start pipe server because " + e.getMessage());
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
    status.setBoolV(receiverInfo.isPipeServerEnable());
    rowRecord.addField(status);
    dataSet.putRecord(rowRecord);
    return dataSet;
  }

  /** query by sql SHOW PIPE */
  public QueryDataSet showPipe(ShowPipePlan plan, ListDataSet dataSet) {
    // TODO: implement show pipe in receiver

    //    List<PipeInfo> pipeInfos;
    //    if (!StringUtils.isEmpty(plan.getPipeName())) {
    //      pipeInfos = receiverInfo.getPipeInfosByPipeName(plan.getPipeName());
    //    } else {
    //      pipeInfos = receiverInfo.getAllPipeInfos();
    //    }
    //    for (PipeInfo pipeInfo : pipeInfos) {
    //      putPipeRecord(dataSet, pipeInfo);
    //    }
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
    dataSet.putRecord(record);
  }

  private ReceiverService() {
    init();
  }

  private void init() {
    receiverInfo = new LocalSyncInfo();
  }

  public static ReceiverService getInstance() {
    return ReceiverServiceHolder.INSTANCE;
  }

  /** IService * */
  @Override
  public void start() throws StartupException {
    init();
    if (receiverInfo.isPipeServerEnable()) {
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
      receiverInfo.close();
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
