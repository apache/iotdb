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
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.sync.SyncConstant;
import org.apache.iotdb.commons.sync.SyncPathUtil;
import org.apache.iotdb.db.exception.sync.PipeException;
import org.apache.iotdb.db.exception.sync.PipeSinkException;
import org.apache.iotdb.db.qp.physical.sys.CreatePipePlan;
import org.apache.iotdb.db.qp.physical.sys.CreatePipeSinkPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPipePlan;
import org.apache.iotdb.db.qp.utils.DatetimeUtils;
import org.apache.iotdb.db.query.dataset.ListDataSet;
import org.apache.iotdb.db.sync.common.ISyncInfoFetcher;
import org.apache.iotdb.db.sync.common.LocalSyncInfoFetcher;
import org.apache.iotdb.db.sync.externalpipe.ExtPipePluginManager;
import org.apache.iotdb.db.sync.externalpipe.ExtPipePluginRegister;
import org.apache.iotdb.db.sync.externalpipe.ExternalPipeStatus;
import org.apache.iotdb.db.sync.receiver.manager.PipeMessage;
import org.apache.iotdb.db.sync.sender.pipe.ExternalPipeSink;
import org.apache.iotdb.db.sync.sender.pipe.IoTDBPipeSink;
import org.apache.iotdb.db.sync.sender.pipe.Pipe;
import org.apache.iotdb.db.sync.sender.pipe.PipeInfo;
import org.apache.iotdb.db.sync.sender.pipe.PipeSink;
import org.apache.iotdb.db.sync.sender.pipe.TsFilePipe;
import org.apache.iotdb.db.sync.sender.service.TransportHandler;
import org.apache.iotdb.db.sync.transport.server.ReceiverManager;
import org.apache.iotdb.db.utils.sync.SyncPipeUtil;
import org.apache.iotdb.pipe.external.api.IExternalPipeSinkWriterFactory;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSyncIdentityInfo;
import org.apache.iotdb.service.rpc.thrift.TSyncTransportMetaInfo;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Binary;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class SyncService implements IService {
  private static final Logger logger = LoggerFactory.getLogger(SyncService.class);

  private Pipe runningPipe;

  private TransportHandler transportHandler;

  /* handle external Pipe */
  private ExtPipePluginManager extPipePluginManager;

  private ISyncInfoFetcher syncInfoFetcher = LocalSyncInfoFetcher.getInstance();

  /* handle rpc in receiver-side*/
  private ReceiverManager receiverManager;

  private SyncService() {
    receiverManager = new ReceiverManager();
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

  public TSStatus transportData(TSyncTransportMetaInfo metaInfo, ByteBuffer buff, ByteBuffer digest)
      throws TException {
    return receiverManager.transportData(metaInfo, buff, digest);
  }

  // TODO: this will be deleted later
  public TSStatus checkFileDigest(TSyncTransportMetaInfo metaInfo, ByteBuffer digest)
      throws TException {
    return receiverManager.checkFileDigest(metaInfo, digest);
  }

  public void handleClientExit() {
    // Handle client exit here.
    receiverManager.handleClientExit();
  }

  // endregion

  // region Interfaces and Implementation of PipeSink

  public PipeSink getPipeSink(String name) {
    return syncInfoFetcher.getPipeSink(name);
  }

  public void addPipeSink(CreatePipeSinkPlan plan) throws PipeSinkException {
    TSStatus status = syncInfoFetcher.addPipeSink(plan);
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

  public synchronized void addPipe(CreatePipePlan plan) throws PipeException {
    // check plan
    long currentTime = DatetimeUtils.currentTime();
    if (plan.getDataStartTimestamp() > currentTime) {
      throw new PipeException(
          String.format(
              "Start time %s is later than current time %s, this is not supported yet.",
              DatetimeUtils.convertLongToDate(plan.getDataStartTimestamp()),
              DatetimeUtils.convertLongToDate(currentTime)));
    }
    // add pipe
    TSStatus status = syncInfoFetcher.addPipe(plan, currentTime);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(status.message);
    }

    PipeSink runningPipeSink = getPipeSink(plan.getPipeSinkName());
    runningPipe = SyncPipeUtil.parseCreatePipePlanAsPipe(plan, runningPipeSink, currentTime);
    if (runningPipe.getPipeSink().getType() == PipeSink.PipeSinkType.IoTDB) {
      try {
        transportHandler =
            TransportHandler.getNewTransportHandler(runningPipe, (IoTDBPipeSink) runningPipeSink);
      } catch (ClassCastException e) {
        logger.error(
            String.format(
                "Cast Class to %s error when create pipe %s.",
                IoTDBPipeSink.class.getName(), plan.getPipeName()),
            e);
        runningPipe = null;
        throw new PipeException(
            String.format(
                "Wrong pipeSink type %s for create pipe %s",
                runningPipeSink.getType(), runningPipeSink.getPipeSinkName()));
      }
    } else { // for external pipe
      // == start ExternalPipeProcessor for send data to external pipe plugin
      startExternalPipeManager(false);
    }
  }

  public synchronized void stopPipe(String pipeName) throws PipeException {
    checkRunningPipeExistAndName(pipeName);
    if (runningPipe.getStatus() == Pipe.PipeStatus.RUNNING) {
      if (runningPipe.getPipeSink().getType() == PipeSink.PipeSinkType.IoTDB) {
        runningPipe.stop();
        transportHandler.stop();
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
    syncInfoFetcher.stopPipe(pipeName);
  }

  public synchronized void startPipe(String pipeName) throws PipeException {
    checkRunningPipeExistAndName(pipeName);
    if (runningPipe.getStatus() == Pipe.PipeStatus.STOP) {
      if (runningPipe.getPipeSink().getType() == PipeSink.PipeSinkType.IoTDB) {
        runningPipe.start();
        transportHandler.start();
      } else { // for external PIPE
        runningPipe.start();
        startExternalPipeManager(true);
      }
    }
    syncInfoFetcher.startPipe(pipeName);
  }

  public synchronized void dropPipe(String pipeName) throws PipeException {
    checkRunningPipeExistAndName(pipeName);
    try {
      if (runningPipe.getPipeSink().getType() == PipeSink.PipeSinkType.IoTDB) {
        if (!transportHandler.close()) {
          throw new PipeException(
              String.format(
                  "Close pipe %s transport error after %s %s, please try again.",
                  runningPipe.getName(),
                  SyncConstant.DEFAULT_WAITING_FOR_STOP_MILLISECONDS,
                  TimeUnit.MILLISECONDS.name()));
        }
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

      syncInfoFetcher.dropPipe(pipeName);
    } catch (InterruptedException e) {
      logger.warn(
          String.format("Interrupted when waiting for clear transport %s.", runningPipe.getName()),
          e);
      throw new PipeException("Drop error, be interrupted, please try again.");
    }
  }

  public List<PipeInfo> getAllPipeInfos() {
    return syncInfoFetcher.getAllPipeInfos();
  }

  private void checkRunningPipeExistAndName(String pipeName) throws PipeException {
    if (runningPipe == null || runningPipe.getStatus() == Pipe.PipeStatus.DROP) {
      throw new PipeException("There is no existing pipe.");
    }
    if (!runningPipe.getName().equals(pipeName)) {
      throw new PipeException(
          String.format(
              "Pipe %s is %s, please retry after drop it.",
              runningPipe.getName(), runningPipe.getStatus()));
    }
  }

  public synchronized void receiveMsg(PipeMessage.MsgType type, String message) {
    if (runningPipe == null || runningPipe.getStatus() == Pipe.PipeStatus.DROP) {
      logger.info(String.format("No running pipe for receiving msg %s.", message));
      return;
    }
    switch (type) {
      case ERROR:
        logger.error(String.format("%s from receiver: %s", type.name(), message));
        try {
          stopPipe(runningPipe.getName());
        } catch (PipeException e) {
          logger.error(
              String.format(
                  "Stop pipe %s when meeting error in sender service.", runningPipe.getName()),
              e);
        }
      case WARN:
        logger.warn(String.format("%s from receiver: %s", type.name(), message));
        TSStatus status =
            syncInfoFetcher.recordMsg(
                runningPipe.getName(), runningPipe.getCreateTime(), new PipeMessage(type, message));
        if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          logger.warn(String.format("Failed to record message: [%s] %s", type.name(), message));
        }
        break;
    }
  }

  public void showPipe(ShowPipePlan plan, ListDataSet listDataSet) {
    boolean showAll = "".equals(plan.getPipeName());
    // show pipe in sender
    for (PipeInfo pipe : SyncService.getInstance().getAllPipeInfos()) {
      if (showAll || plan.getPipeName().equals(pipe.getPipeName())) {
        RowRecord record = new RowRecord(0);
        record.addField(
            Binary.valueOf(DatetimeUtils.convertLongToDate(pipe.getCreateTime())), TSDataType.TEXT);
        record.addField(Binary.valueOf(pipe.getPipeName()), TSDataType.TEXT);
        record.addField(Binary.valueOf(IoTDBConstant.SYNC_SENDER_ROLE), TSDataType.TEXT);
        record.addField(Binary.valueOf(pipe.getPipeSinkName()), TSDataType.TEXT);
        record.addField(Binary.valueOf(pipe.getStatus().name()), TSDataType.TEXT);
        record.addField(
            Binary.valueOf(syncInfoFetcher.getPipeMsg(pipe.getPipeName(), pipe.getCreateTime())),
            TSDataType.TEXT);
        boolean needSetFields = true;
        PipeSink pipeSink = syncInfoFetcher.getPipeSink(pipe.getPipeSinkName());
        if (pipeSink.getType() == PipeSink.PipeSinkType.ExternalPipe) { // for external pipe
          ExtPipePluginManager extPipePluginManager =
              SyncService.getInstance().getExternalPipeManager();

          if (extPipePluginManager != null) {
            String extPipeType = ((ExternalPipeSink) pipeSink).getExtPipeSinkTypeName();
            ExternalPipeStatus externalPipeStatus =
                extPipePluginManager.getExternalPipeStatus(extPipeType);

            if (externalPipeStatus != null) {
              record.addField(
                  Binary.valueOf(externalPipeStatus.getWriterInvocationFailures().toString()),
                  TSDataType.TEXT);
              record.addField(
                  Binary.valueOf(externalPipeStatus.getWriterStatuses().toString()),
                  TSDataType.TEXT);
              needSetFields = false;
            }
          }
        }

        if (needSetFields) {
          record.addField(Binary.valueOf("N/A"), TSDataType.TEXT);
          record.addField(Binary.valueOf("N/A"), TSDataType.TEXT);
        }
        listDataSet.putRecord(record);
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
      record.addField(Binary.valueOf(Pipe.PipeStatus.RUNNING.name()), TSDataType.TEXT);
      record.addField(Binary.valueOf(""), TSDataType.TEXT);
      record.addField(Binary.valueOf("N/A"), TSDataType.TEXT);
      record.addField(Binary.valueOf("N/A"), TSDataType.TEXT);
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
              "startExternalPipeManager(), can not found ExternalPipe plugin for {}.",
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
    if (runningPipe != null && !Pipe.PipeStatus.DROP.equals(runningPipe.getStatus())) {
      try {
        runningPipe.stop();
        transportHandler.stop();
      } catch (PipeException e) {
        logger.warn(
            String.format("Stop pipe %s error when stop Sender Service.", runningPipe.getName()),
            e);
      }
    }
  }

  @Override
  public void shutdown(long milliseconds) throws ShutdownException {
    if (runningPipe != null && !Pipe.PipeStatus.DROP.equals(runningPipe.getStatus())) {
      try {
        runningPipe.stop();
        transportHandler.close();
        runningPipe.close();
      } catch (PipeException | InterruptedException e) {
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

  private void recover() throws IOException, PipeException, StartupException {
    PipeInfo runningPipeInfo = syncInfoFetcher.getRunningPipeInfo();
    if (runningPipeInfo == null || Pipe.PipeStatus.DROP.equals(runningPipeInfo.getStatus())) {
      return;
    } else {
      this.runningPipe =
          SyncPipeUtil.parsePipeInfoAsPipe(
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

    if (runningPipe.getPipeSink().getType() == PipeSink.PipeSinkType.IoTDB) {
      this.transportHandler =
          TransportHandler.getNewTransportHandler(
              runningPipe, (IoTDBPipeSink) runningPipe.getPipeSink());
      if (Pipe.PipeStatus.RUNNING.equals(runningPipe.getStatus())) {
        transportHandler.start();
      }
    } else { // for external pipe
      // == start ExternalPipeProcessor for send data to external pipe plugin
      startExternalPipeManager(runningPipe.getStatus() == Pipe.PipeStatus.RUNNING);
    }
  }
}
