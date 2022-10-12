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
package org.apache.iotdb.db.sync;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.ShutdownException;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.exception.sync.PipeException;
import org.apache.iotdb.commons.exception.sync.PipeSinkException;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.sync.pipe.PipeInfo;
import org.apache.iotdb.commons.sync.pipe.PipeMessage;
import org.apache.iotdb.commons.sync.pipe.PipeStatus;
import org.apache.iotdb.commons.sync.pipe.TsFilePipeInfo;
import org.apache.iotdb.commons.sync.pipesink.PipeSink;
import org.apache.iotdb.commons.sync.utils.SyncConstant;
import org.apache.iotdb.commons.sync.utils.SyncPathUtil;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeInfo;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.plan.statement.sys.sync.CreatePipeSinkStatement;
import org.apache.iotdb.db.qp.physical.sys.CreatePipeSinkPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPipePlan;
import org.apache.iotdb.db.qp.utils.DatetimeUtils;
import org.apache.iotdb.db.query.dataset.ListDataSet;
import org.apache.iotdb.db.sync.common.ClusterSyncInfoFetcher;
import org.apache.iotdb.db.sync.common.ISyncInfoFetcher;
import org.apache.iotdb.db.sync.common.LocalSyncInfoFetcher;
import org.apache.iotdb.db.sync.externalpipe.ExtPipePluginManager;
import org.apache.iotdb.db.sync.externalpipe.ExtPipePluginRegister;
import org.apache.iotdb.db.sync.externalpipe.ExternalPipeStatus;
import org.apache.iotdb.db.sync.sender.manager.ISyncManager;
import org.apache.iotdb.db.sync.sender.pipe.ExternalPipeSink;
import org.apache.iotdb.db.sync.sender.pipe.Pipe;
import org.apache.iotdb.db.sync.sender.pipe.TsFilePipe;
import org.apache.iotdb.db.sync.transport.client.SenderManager;
import org.apache.iotdb.db.sync.transport.server.ReceiverManager;
import org.apache.iotdb.db.utils.sync.SyncPipeUtil;
import org.apache.iotdb.pipe.external.api.IExternalPipeSinkWriterFactory;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSyncIdentityInfo;
import org.apache.iotdb.service.rpc.thrift.TSyncTransportMetaInfo;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Binary;

import org.apache.commons.lang3.StringUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class SyncService implements IService {
  private static final Logger logger = LoggerFactory.getLogger(SyncService.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private Pipe runningPipe;

  /* handle external Pipe */
  private ExtPipePluginManager extPipePluginManager;

  private ISyncInfoFetcher syncInfoFetcher;

  /* handle rpc in receiver-side*/
  private final ReceiverManager receiverManager;

  private SyncService() {
    receiverManager = new ReceiverManager();
    if (config.isClusterMode()) {
      syncInfoFetcher = ClusterSyncInfoFetcher.getInstance();
    } else {
      syncInfoFetcher = LocalSyncInfoFetcher.getInstance();
    }
  }

  private static class SyncServiceHolder {
    private static final SyncService INSTANCE = new SyncService();

    private SyncServiceHolder() {}
  }

  public static SyncService getInstance() {
    return SyncServiceHolder.INSTANCE;
  }

  // region Interfaces and Implementation of Transport Layer

  public TSStatus handshake(TSyncIdentityInfo identityInfo) {
    return receiverManager.handshake(identityInfo);
  }

  public TSStatus transportFile(TSyncTransportMetaInfo metaInfo, ByteBuffer buff)
      throws TException {
    return receiverManager.transportFile(metaInfo, buff);
  }

  public TSStatus transportPipeData(ByteBuffer buff) throws TException {
    return receiverManager.transportPipeData(buff);
  }

  public void handleClientExit() {
    // Handle client exit here.
    receiverManager.handleClientExit();
  }

  // endregion

  // region Interfaces and Implementation of PipeSink

  public PipeSink getPipeSink(String name) throws PipeSinkException {
    return syncInfoFetcher.getPipeSink(name);
  }

  // TODO(sync): delete this in new-standalone version
  public void addPipeSink(CreatePipeSinkPlan plan) throws PipeSinkException {
    TSStatus status = syncInfoFetcher.addPipeSink(plan);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeSinkException(status.message);
    }
  }

  public void addPipeSink(CreatePipeSinkStatement createPipeSinkStatement)
      throws PipeSinkException {
    TSStatus status = syncInfoFetcher.addPipeSink(createPipeSinkStatement);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeSinkException(status.message);
    }
  }

  public void dropPipeSink(String name) throws PipeSinkException {
    TSStatus status = syncInfoFetcher.dropPipeSink(name);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeSinkException(status.message);
    }
  }

  public List<PipeSink> getAllPipeSink() {
    return syncInfoFetcher.getAllPipeSinks();
  }

  // endregion

  // region Interfaces and Implementation of Pipe

  public synchronized void addPipe(PipeInfo pipeInfo) throws PipeException {
    logger.info("Execute CREATE PIPE {}", pipeInfo.getPipeName());
    long currentTime = DatetimeUtils.currentTime();
    if (pipeInfo instanceof TsFilePipeInfo) {
      // TODO(sync): move check logic to PipeInfo#validate()
      // check statement
      if (((TsFilePipeInfo) pipeInfo).getDataStartTimestamp() > currentTime) {
        throw new PipeException(
            String.format(
                "Start time %s is later than current time %s, this is not supported yet.",
                DatetimeUtils.convertLongToDate(
                    ((TsFilePipeInfo) pipeInfo).getDataStartTimestamp()),
                DatetimeUtils.convertLongToDate(currentTime)));
      }
    }
    // add pipe
    TSStatus status = syncInfoFetcher.addPipe(pipeInfo);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(status.message);
    }
    PipeSink runningPipeSink;
    try {
      runningPipeSink = getPipeSink(pipeInfo.getPipeSinkName());
    } catch (PipeSinkException e) {
      logger.error("failed to add PIPE because {}", e.getMessage(), e);
      throw new PipeException(String.format("failed to add PIPE because %s", e.getMessage()));
    }

    runningPipe = SyncPipeUtil.parseTPipeSinkInfoAsPipeSink(pipeInfo, runningPipeSink);
    if (runningPipe.getPipeSink().getType()
        == PipeSink.PipeSinkType.ExternalPipe) { // for external pipe
      // == start ExternalPipeProcessor for send data to external pipe plugin
      startExternalPipeManager(false);
    }
  }

  public synchronized void stopPipe(String pipeName) throws PipeException {
    logger.info("Execute stop PIPE {}", pipeName);
    checkRunningPipeExistAndName(pipeName);
    if (runningPipe.getStatus() == PipeStatus.RUNNING) {
      if (runningPipe.getPipeSink().getType() == PipeSink.PipeSinkType.IoTDB) {
        runningPipe.stop();
      } else { // for external PIPE
        // == pause externalPipeProcessor's task
        if (extPipePluginManager != null) {
          try {
            String extPipeSinkTypeName =
                ((ExternalPipeSink) (runningPipe.getPipeSink())).getExtPipeSinkTypeName();
            extPipePluginManager.stopExtPipe(extPipeSinkTypeName);
          } catch (Exception e) {
            throw new PipeException("Failed to stop externalPipeProcessor. " + e.getMessage());
          }
        }

        runningPipe.stop();
      }
    }
    TSStatus status = syncInfoFetcher.stopPipe(pipeName);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(status.message);
    }
  }

  public synchronized void startPipe(String pipeName) throws PipeException {
    logger.info("Execute start PIPE {}", pipeName);
    checkRunningPipeExistAndName(pipeName);
    if (runningPipe.getStatus() == PipeStatus.STOP) {
      if (runningPipe.getPipeSink().getType() == PipeSink.PipeSinkType.IoTDB) {
        runningPipe.start();
      } else { // for external PIPE
        runningPipe.start();
        startExternalPipeManager(true);
      }
    }
    TSStatus status = syncInfoFetcher.startPipe(pipeName);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(status.message);
    }
  }

  public synchronized void dropPipe(String pipeName) throws PipeException {
    logger.info("Execute drop PIPE {}", pipeName);
    checkRunningPipeExistAndName(pipeName);
    if (runningPipe.getPipeSink().getType() == PipeSink.PipeSinkType.IoTDB) {
      runningPipe.drop();
    } else { // for external pipe
      // == drop ExternalPipeProcesser
      if (extPipePluginManager != null) {
        String extPipeSinkTypeName =
            ((ExternalPipeSink) runningPipe.getPipeSink()).getExtPipeSinkTypeName();
        extPipePluginManager.dropExtPipe(extPipeSinkTypeName);
        extPipePluginManager = null;
      }
      runningPipe.drop();
    }

    TSStatus status = syncInfoFetcher.dropPipe(pipeName);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(status.message);
    }
  }

  public List<PipeInfo> getAllPipeInfos() {
    return syncInfoFetcher.getAllPipeInfos();
  }

  private void checkRunningPipeExistAndName(String pipeName) throws PipeException {
    if (runningPipe == null || runningPipe.getStatus() == PipeStatus.DROP) {
      throw new PipeException("There is no existing PIPE.");
    }
    if (!runningPipe.getName().equals(pipeName)) {
      throw new PipeException(
          String.format(
              "PIPE %s is %s, please retry after drop it.",
              runningPipe.getName(), runningPipe.getStatus()));
    }
  }

  public synchronized void recordMessage(PipeMessage message) {
    if (runningPipe == null || runningPipe.getStatus() == PipeStatus.DROP) {
      logger.info(String.format("No running PIPE for message %s.", message));
      return;
    }
    TSStatus status = null;
    switch (message.getType()) {
      case ERROR:
        logger.error("{}", message);
        status =
            syncInfoFetcher.recordMsg(runningPipe.getName(), runningPipe.getCreateTime(), message);
        try {
          stopPipe(runningPipe.getName());
        } catch (PipeException e) {
          logger.error(
              String.format(
                  "Stop PIPE %s when meeting error in sender service.", runningPipe.getName()),
              e);
        }
        break;
      case WARN:
        logger.warn("{}", message);
        status =
            syncInfoFetcher.recordMsg(runningPipe.getName(), runningPipe.getCreateTime(), message);
        break;
      default:
        logger.error(String.format("Unknown message type: %s", message));
    }
    if (status != null && status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      logger.error(String.format("Failed to record message: %s", message));
    }
  }

  public List<TShowPipeInfo> showPipe(String pipeName) {
    boolean showAll = StringUtils.isEmpty(pipeName);
    List<TShowPipeInfo> list = new ArrayList<>();
    // show pipe in sender
    for (PipeInfo pipe : SyncService.getInstance().getAllPipeInfos()) {
      if (showAll || pipeName.equals(pipe.getPipeName())) {
        TShowPipeInfo tPipeInfo =
            new TShowPipeInfo(
                pipe.getCreateTime(),
                pipe.getPipeName(),
                SyncConstant.ROLE_SENDER,
                pipe.getPipeSinkName(),
                pipe.getStatus().name(),
                pipe.getMessageType().name());
        list.add(tPipeInfo);
      }
    }
    // show pipe in receiver
    for (TSyncIdentityInfo identityInfo : receiverManager.getAllTSyncIdentityInfos()) {
      if (showAll || pipeName.equals(identityInfo.getPipeName())) {
        TShowPipeInfo tPipeInfo =
            new TShowPipeInfo(
                identityInfo.getCreateTime(),
                identityInfo.getPipeName(),
                SyncConstant.ROLE_RECEIVER,
                identityInfo.getAddress(),
                PipeStatus.RUNNING.name(),
                // TODO: implement receiver message
                PipeMessage.PipeMessageType.NORMAL.name());
        list.add(tPipeInfo);
      }
    }
    return list;
  }

  // TODO(sync): delete this in new-standalone version
  public void showPipe(ShowPipePlan plan, ListDataSet listDataSet) {
    boolean showAll = "".equals(plan.getPipeName());
    // show pipe in sender
    for (PipeInfo pipe : SyncService.getInstance().getAllPipeInfos()) {
      if (showAll || plan.getPipeName().equals(pipe.getPipeName())) {
        try {
          RowRecord record = new RowRecord(0);
          record.addField(
              Binary.valueOf(DatetimeUtils.convertLongToDate(pipe.getCreateTime())),
              TSDataType.TEXT);
          record.addField(Binary.valueOf(pipe.getPipeName()), TSDataType.TEXT);
          record.addField(Binary.valueOf(IoTDBConstant.SYNC_SENDER_ROLE), TSDataType.TEXT);
          record.addField(Binary.valueOf(pipe.getPipeSinkName()), TSDataType.TEXT);
          record.addField(Binary.valueOf(pipe.getStatus().name()), TSDataType.TEXT);
          PipeSink pipeSink = syncInfoFetcher.getPipeSink(pipe.getPipeSinkName());
          if (pipeSink.getType() == PipeSink.PipeSinkType.ExternalPipe) { // for external pipe
            ExtPipePluginManager extPipePluginManager =
                SyncService.getInstance().getExternalPipeManager();

            if (extPipePluginManager != null) {
              String extPipeType = ((ExternalPipeSink) pipeSink).getExtPipeSinkTypeName();
              ExternalPipeStatus externalPipeStatus =
                  extPipePluginManager.getExternalPipeStatus(extPipeType);

              // TODO(ext-pipe): Adapting to the new syntax of SHOW PIPE
              if (externalPipeStatus != null) {
                record.addField(
                    Binary.valueOf(
                        externalPipeStatus.getWriterInvocationFailures().toString()
                            + ";"
                            + externalPipeStatus.getWriterStatuses().toString()),
                    TSDataType.TEXT);
              }
            }
          } else {
            record.addField(Binary.valueOf(pipe.getMessageType().name()), TSDataType.TEXT);
          }
          listDataSet.putRecord(record);
        } catch (Exception e) {
          logger.error("failed to show pipe [{}] because {}", pipe.getPipeName(), e.getMessage());
        }
      }
    }
    // show pipe in receiver
    List<TSyncIdentityInfo> identityInfoList = receiverManager.getAllTSyncIdentityInfos();
    for (TSyncIdentityInfo identityInfo : identityInfoList) {
      // TODO(sync): Removing duplicate rows
      RowRecord record = new RowRecord(0);
      record.addField(
          Binary.valueOf(DatetimeUtils.convertLongToDate(identityInfo.getCreateTime())),
          TSDataType.TEXT);
      record.addField(Binary.valueOf(identityInfo.getPipeName()), TSDataType.TEXT);
      record.addField(Binary.valueOf(IoTDBConstant.SYNC_RECEIVER_ROLE), TSDataType.TEXT);
      record.addField(Binary.valueOf(identityInfo.getAddress()), TSDataType.TEXT);
      record.addField(Binary.valueOf(PipeStatus.RUNNING.name()), TSDataType.TEXT);
      record.addField(Binary.valueOf(PipeMessage.PipeMessageType.NORMAL.name()), TSDataType.TEXT);
      listDataSet.putRecord(record);
    }
  }

  // endregion

  // region Interfaces and Implementation of External-Pipe

  /** Start ExternalPipeProcessor who handle externalPipe */
  private void startExternalPipeManager(boolean startExtPipe) throws PipeException {
    if (!(runningPipe instanceof TsFilePipe)) {
      logger.error("startExternalPipeManager(), runningPipe is not TsFilePipe. " + runningPipe);
      return;
    }

    PipeSink pipeSink = runningPipe.getPipeSink();
    if (!(pipeSink instanceof ExternalPipeSink)) {
      logger.error("startExternalPipeManager(), pipeSink is not ExternalPipeSink." + pipeSink);
      return;
    }

    String extPipeSinkTypeName = ((ExternalPipeSink) pipeSink).getExtPipeSinkTypeName();
    IExternalPipeSinkWriterFactory externalPipeSinkWriterFactory =
        ExtPipePluginRegister.getInstance().getWriteFactory(extPipeSinkTypeName);
    if (externalPipeSinkWriterFactory == null) {
      logger.error(
          String.format(
              "startExternalPipeManager(), can not found ExternalPipe plugin for %s.",
              extPipeSinkTypeName));
      throw new PipeException("Can not found ExternalPipe plugin for " + extPipeSinkTypeName + ".");
    }

    if (extPipePluginManager == null) {
      extPipePluginManager = new ExtPipePluginManager((TsFilePipe) this.runningPipe);
    }

    if (startExtPipe) {
      try {
        extPipePluginManager.startExtPipe(
            extPipeSinkTypeName, ((ExternalPipeSink) pipeSink).getSinkParams());
      } catch (IOException e) {
        logger.error("Failed to start External Pipe: {}.", extPipeSinkTypeName, e);
        throw new PipeException(
            "Failed to start External Pipe: " + extPipeSinkTypeName + ". " + e.getMessage());
      }
    }
  }

  public ExtPipePluginManager getExternalPipeManager() {
    return extPipePluginManager;
  }

  // endregion

  /** IService * */
  @Override
  public void start() throws StartupException {
    // == Check whether loading extPipe plugin successfully.
    ExtPipePluginRegister extPipePluginRegister = ExtPipePluginRegister.getInstance();
    if (extPipePluginRegister == null) {
      throw new StartupException("Load ExternalPipe Plugin error.");
    }
    logger.info(
        "Load {} ExternalPipe Plugin: {}",
        extPipePluginRegister.getAllPluginName().size(),
        extPipePluginRegister.getAllPluginName());

    File senderLog = new File(SyncPathUtil.getSysDir(), SyncConstant.SYNC_LOG_NAME);
    if (senderLog.exists()) {
      try {
        recover();
      } catch (Exception e) {
        logger.error("Recover from disk error.", e);
        throw new StartupException(e);
      }
    }
  }

  @Override
  public void stop() {
    if (runningPipe != null && !PipeStatus.DROP.equals(runningPipe.getStatus())) {
      try {
        runningPipe.close();
      } catch (PipeException e) {
        logger.warn(
            String.format("Stop PIPE %s error when stop Sender Service.", runningPipe.getName()),
            e);
      }
    }
  }

  @Override
  public void shutdown(long milliseconds) throws ShutdownException {
    if (runningPipe != null && !PipeStatus.DROP.equals(runningPipe.getStatus())) {
      try {
        runningPipe.stop();
        runningPipe.close();
      } catch (PipeException e) {
        logger.warn(
            String.format(
                "Stop pipe %s error when shutdown Sender Service.", runningPipe.getName()),
            e);
        throw new ShutdownException(e);
      }
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.SYNC_SERVICE;
  }

  private void recover() throws IOException, PipeException, PipeSinkException {
    PipeInfo runningPipeInfo = syncInfoFetcher.getRunningPipeInfo();
    if (runningPipeInfo == null || PipeStatus.DROP.equals(runningPipeInfo.getStatus())) {
      return;
    } else {
      this.runningPipe =
          SyncPipeUtil.parseTPipeSinkInfoAsPipeSink(
              runningPipeInfo, syncInfoFetcher.getPipeSink(runningPipeInfo.getPipeSinkName()));
      switch (runningPipeInfo.getStatus()) {
        case RUNNING:
          runningPipe.start();
          break;
        case STOP:
          runningPipe.stop();
          break;
        case DROP:
          runningPipe.drop();
          break;
        default:
          throw new IOException(
              String.format(
                  "Can not recognize running pipe status %s.", runningPipeInfo.getStatus()));
      }
    }

    if (runningPipe.getPipeSink().getType()
        == PipeSink.PipeSinkType.ExternalPipe) { // for external pipe
      // == start ExternalPipeProcessor for send data to external pipe plugin
      startExternalPipeManager(runningPipe.getStatus() == PipeStatus.RUNNING);
    }
  }

  public List<ISyncManager> getOrCreateSyncManager(String dataRegionId) {
    // TODO(sync): maybe add cache to accelerate
    List<ISyncManager> syncManagerList = new ArrayList<>();
    if (runningPipe != null) {
      syncManagerList.add(runningPipe.getOrCreateSyncManager(dataRegionId));
    }
    return syncManagerList;
  }

  /** This method will be called before deleting dataRegion */
  public synchronized void unregisterDataRegion(String dataRegionId) {
    if (runningPipe != null) {
      runningPipe.unregisterDataRegion(dataRegionId);
    }
  }

  @TestOnly
  public SenderManager getSenderManager() {
    return runningPipe.getSenderManager();
  }
}
