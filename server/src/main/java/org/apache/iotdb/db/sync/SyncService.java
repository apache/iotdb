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
import org.apache.iotdb.commons.exception.ShutdownException;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.exception.sync.PipeException;
import org.apache.iotdb.commons.exception.sync.PipeNotExistException;
import org.apache.iotdb.commons.exception.sync.PipeSinkException;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.sync.pipe.PipeInfo;
import org.apache.iotdb.commons.sync.pipe.PipeMessage;
import org.apache.iotdb.commons.sync.pipe.PipeStatus;
import org.apache.iotdb.commons.sync.pipe.TsFilePipeInfo;
import org.apache.iotdb.commons.sync.pipesink.PipeSink;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeInfo;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.mpp.plan.statement.sys.sync.CreatePipeSinkStatement;
import org.apache.iotdb.db.sync.common.ClusterSyncInfoFetcher;
import org.apache.iotdb.db.sync.common.ISyncInfoFetcher;
import org.apache.iotdb.db.sync.common.LocalSyncInfoFetcher;
import org.apache.iotdb.db.sync.externalpipe.ExtPipePluginManager;
import org.apache.iotdb.db.sync.externalpipe.ExtPipePluginRegister;
import org.apache.iotdb.db.sync.sender.manager.ISyncManager;
import org.apache.iotdb.db.sync.sender.pipe.ExternalPipeSink;
import org.apache.iotdb.db.sync.sender.pipe.Pipe;
import org.apache.iotdb.db.sync.sender.pipe.TsFilePipe;
import org.apache.iotdb.db.sync.transport.client.SenderManager;
import org.apache.iotdb.db.sync.transport.server.ReceiverManager;
import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.db.utils.sync.SyncPipeUtil;
import org.apache.iotdb.pipe.external.api.IExternalPipeSinkWriterFactory;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSyncIdentityInfo;
import org.apache.iotdb.service.rpc.thrift.TSyncTransportMetaInfo;

import org.apache.commons.lang3.StringUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SyncService implements IService {
  private static final Logger logger = LoggerFactory.getLogger(SyncService.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final Map<String, Pipe> pipes;

  /* handle external Pipe */
  // TODO(ext-pipe): adapt multi pipe
  private final Map<String, ExtPipePluginManager> extPipePluginManagers;

  private final ISyncInfoFetcher syncInfoFetcher;

  /* handle rpc in receiver-side*/
  private final ReceiverManager receiverManager;

  private SyncService() {
    receiverManager = new ReceiverManager();
    pipes = new ConcurrentHashMap<>();
    extPipePluginManagers = new ConcurrentHashMap<>();
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

  public TSStatus handshake(
      TSyncIdentityInfo identityInfo,
      String remoteAddress,
      IPartitionFetcher partitionFetcher,
      ISchemaFetcher schemaFetcher) {
    return receiverManager.handshake(identityInfo, remoteAddress, partitionFetcher, schemaFetcher);
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

  public void addPipeSink(CreatePipeSinkStatement createPipeSinkStatement)
      throws PipeSinkException {
    logger.info("Add PIPESINK {}", createPipeSinkStatement);
    TSStatus status = syncInfoFetcher.addPipeSink(createPipeSinkStatement);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeSinkException(status.message);
    }
  }

  public void dropPipeSink(String name) throws PipeSinkException {
    logger.info("Execute DROP PIPESINK {}", name);
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
    logger.info("Execute CREATE PIPE {}", pipeInfo);
    long currentTime = DateTimeUtils.currentTime();
    if (pipeInfo instanceof TsFilePipeInfo) {
      // TODO(sync): move check logic to PipeInfo#validate()
      // check statement
      if (((TsFilePipeInfo) pipeInfo).getDataStartTimestamp() > currentTime) {
        throw new PipeException(
            String.format(
                "Start time %s is later than current time %s, this is not supported yet.",
                DateTimeUtils.convertLongToDate(
                    ((TsFilePipeInfo) pipeInfo).getDataStartTimestamp()),
                DateTimeUtils.convertLongToDate(currentTime)));
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

    Pipe runningPipe = SyncPipeUtil.parsePipeInfoAsPipe(pipeInfo, runningPipeSink);
    pipes.put(pipeInfo.getPipeName(), runningPipe);
    if (runningPipe.getPipeSink().getType()
        == PipeSink.PipeSinkType.ExternalPipe) { // for external pipe
      // == start ExternalPipeProcessor for send data to external pipe plugin
      startExternalPipeManager(pipeInfo.getPipeName(), false);
    }
  }

  public synchronized void stopPipe(String pipeName) throws PipeException {
    logger.info("Execute stop PIPE {}", pipeName);
    Pipe runningPipe = getPipe(pipeName);
    if (runningPipe.getStatus() == PipeStatus.RUNNING) {
      if (runningPipe.getPipeSink().getType() != PipeSink.PipeSinkType.IoTDB) { // for external PIPE
        // == pause externalPipeProcessor's task
        if (extPipePluginManagers.containsKey(pipeName)) {
          try {
            String extPipeSinkTypeName =
                ((ExternalPipeSink) (runningPipe.getPipeSink())).getExtPipeSinkTypeName();
            extPipePluginManagers.get(pipeName).stopExtPipe(extPipeSinkTypeName);
          } catch (Exception e) {
            throw new PipeException("Failed to stop externalPipeProcessor. " + e.getMessage());
          }
        }
      }
      runningPipe.stop();
    }
    TSStatus status = syncInfoFetcher.stopPipe(pipeName);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(status.message);
    }
  }

  // only used for roll back
  public synchronized void stopPipe(String pipeName, long createTime) {
    Pipe runningPipe;
    try {
      runningPipe = getPipe(pipeName);
      if (runningPipe.getCreateTime() == createTime) {
        stopPipe(pipeName);
      } else {
        logger.warn(
            "Skip execute stop PIPE {} with createTime {} because of createTime mismatch.",
            pipeName,
            createTime);
      }
    } catch (PipeException e) {
      logger.warn(
          "Skip execute stop PIPE {} with createTime {} because {}",
          pipeName,
          createTime,
          e.getMessage());
    }
  }

  public synchronized void startPipe(String pipeName) throws PipeException {
    logger.info("Execute start PIPE {}", pipeName);
    Pipe runningPipe = getPipe(pipeName);
    if (runningPipe.getStatus() == PipeStatus.STOP) {
      if (runningPipe.getPipeSink().getType() == PipeSink.PipeSinkType.IoTDB) {
        runningPipe.start();
      } else { // for external PIPE
        runningPipe.start();
        startExternalPipeManager(pipeName, true);
      }
    }
    TSStatus status = syncInfoFetcher.startPipe(pipeName);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(status.message);
    }
  }

  // only used for roll back
  public synchronized void startPipe(String pipeName, long createTime) {
    Pipe runningPipe;
    try {
      runningPipe = getPipe(pipeName);
      if (runningPipe.getCreateTime() == createTime) {
        startPipe(pipeName);
      } else {
        logger.warn(
            "Skip execute start PIPE {} with createTime {} because of createTime mismatch.",
            pipeName,
            createTime);
      }
    } catch (PipeException e) {
      logger.warn(
          "Skip execute start PIPE {} with createTime {} because {}",
          pipeName,
          createTime,
          e.getMessage());
    }
  }

  public synchronized void dropPipe(String pipeName) throws PipeException {
    logger.info("Execute drop PIPE {}", pipeName);
    Pipe runningPipe;
    try {
      runningPipe = getPipe(pipeName);
    } catch (PipeNotExistException e) {
      return;
    }
    if (runningPipe.getPipeSink().getType() != PipeSink.PipeSinkType.IoTDB) { // for external pipe
      // == drop ExternalPipeProcessor
      if (extPipePluginManagers.containsKey(pipeName)) {
        String extPipeSinkTypeName =
            ((ExternalPipeSink) runningPipe.getPipeSink()).getExtPipeSinkTypeName();
        extPipePluginManagers.get(pipeName).dropExtPipe(extPipeSinkTypeName);
        extPipePluginManagers.remove(pipeName);
      }
    }
    runningPipe.drop();

    TSStatus status = syncInfoFetcher.dropPipe(pipeName);
    // remove dropped pipe from map
    pipes.remove(pipeName);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(status.message);
    }
  }

  // only used for roll back
  public synchronized void dropPipe(String pipeName, long createTime) {
    Pipe runningPipe;
    try {
      runningPipe = getPipe(pipeName);
      if (runningPipe.getCreateTime() == createTime) {
        dropPipe(pipeName);
      } else {
        logger.warn(
            "Skip execute drop PIPE {} with createTime {} because of createTime mismatch.",
            pipeName,
            createTime);
      }
    } catch (PipeException e) {
      logger.warn(
          "Skip execute drop PIPE {} with createTime {} because {}",
          pipeName,
          createTime,
          e.getMessage());
    }
  }

  public List<PipeInfo> getAllPipeInfos() {
    return syncInfoFetcher.getAllPipeInfos();
  }

  private Pipe getPipe(String pipeName) throws PipeNotExistException {
    if (!pipes.containsKey(pipeName)) {
      throw new PipeNotExistException(pipeName);
    } else {
      return pipes.get(pipeName);
    }
  }

  public void recordMessage(String pipeName, PipeMessage message) {
    if (!pipes.containsKey(pipeName)) {
      logger.warn("No running PIPE for message {}.", message);
      return;
    }
    TSStatus status = null;
    switch (message.getType()) {
      case ERROR:
        logger.error(
            "Error occurred when executing PIPE [{}] because {}.", pipeName, message.getMessage());
        status = syncInfoFetcher.recordMsg(pipeName, message);
        break;
      case WARN:
        logger.error(
            "Warn occurred when executing PIPE [{}] because {}.", pipeName, message.getMessage());
        status = syncInfoFetcher.recordMsg(pipeName, message);
        break;
      default:
        logger.error("Unknown message type: {}", message);
    }
    if (status != null && status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      logger.error("Failed to record message: {}", message);
    }
  }

  // TODO: Only used for LocalConfigNode. Delete it after delete LocalConfigNode.
  public List<TShowPipeInfo> showPipe(String pipeName) {
    boolean showAll = StringUtils.isEmpty(pipeName);
    List<TShowPipeInfo> list = new ArrayList<>();
    // show pipe in sender
    for (PipeInfo pipe : SyncService.getInstance().getAllPipeInfos()) {
      if (showAll || pipeName.equals(pipe.getPipeName())) {
        list.add(pipe.getTShowPipeInfo());
      }
    }
    return list;
  }

  // endregion

  // region Interfaces and Implementation of External-Pipe

  /** Start ExternalPipeProcessor who handle externalPipe */
  private void startExternalPipeManager(String pipeName, boolean startExtPipe)
      throws PipeException {
    if (!(pipes.get(pipeName) instanceof TsFilePipe)) {
      logger.error("startExternalPipeManager(), runningPipe is not TsFilePipe. {}", pipeName);
      return;
    }

    PipeSink pipeSink = pipes.get(pipeName).getPipeSink();
    if (!(pipeSink instanceof ExternalPipeSink)) {
      logger.error("startExternalPipeManager(), pipeSink is not ExternalPipeSink. {}", pipeSink);
      return;
    }

    String extPipeSinkTypeName = ((ExternalPipeSink) pipeSink).getExtPipeSinkTypeName();
    IExternalPipeSinkWriterFactory externalPipeSinkWriterFactory =
        ExtPipePluginRegister.getInstance().getWriteFactory(extPipeSinkTypeName);
    if (externalPipeSinkWriterFactory == null) {
      logger.error(
          "startExternalPipeManager(), can not found ExternalPipe plugin for {}.",
          extPipeSinkTypeName);
      throw new PipeException("Can not found ExternalPipe plugin for " + extPipeSinkTypeName + ".");
    }

    ExtPipePluginManager extPipePluginManager =
        new ExtPipePluginManager((TsFilePipe) pipes.get(pipeName));
    if (!extPipePluginManagers.containsKey(pipeName)) {
      extPipePluginManagers.put(pipeName, extPipePluginManager);
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

  public ExtPipePluginManager getExternalPipeManager(String pipeName) {
    return extPipePluginManagers.get(pipeName);
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

    try {
      recover();
    } catch (Exception e) {
      logger.error("Recovery error.", e);
      throw new StartupException(e);
    }
  }

  @Override
  public void stop() {
    for (Pipe pipe : pipes.values()) {
      try {
        pipe.close();
      } catch (PipeException e) {
        logger.warn(
            String.format("Stop PIPE %s error when stop Sender Service.", pipe.getName()), e);
      }
    }
  }

  @Override
  public void shutdown(long milliseconds) throws ShutdownException {

    for (Pipe pipe : pipes.values()) {
      try {
        pipe.close();
      } catch (PipeException e) {
        logger.warn(
            String.format("Stop PIPE %s error when shutdown Sender Service.", pipe.getName()), e);
        throw new ShutdownException(e);
      }
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.SYNC_SERVICE;
  }

  /**
   * If run on standalone version, recover from disk.
   *
   * <p>If run on MPP version, init or recover from ConfigNode.
   */
  private void recover() throws IOException, PipeException, PipeSinkException {
    List<PipeInfo> allPipeInfos = syncInfoFetcher.getAllPipeInfos();
    for (PipeInfo pipeInfo : allPipeInfos) {
      logger.info(
          "Recover PIPE [{}] whose status is {}",
          pipeInfo.getPipeName(),
          pipeInfo.getStatus().name());
      if (PipeStatus.PARTIAL_CREATE.equals(pipeInfo.getStatus())
          || PipeStatus.DROP.equals(pipeInfo.getStatus())) {
        // skip
        logger.info(
            "Skip PIPE [{}] because its status is {}",
            pipeInfo.getPipeName(),
            pipeInfo.getStatus().name());
        continue;
      }
      Pipe pipe =
          SyncPipeUtil.parsePipeInfoAsPipe(
              pipeInfo, syncInfoFetcher.getPipeSink(pipeInfo.getPipeSinkName()));
      pipes.put(pipeInfo.getPipeName(), pipe);
      switch (pipeInfo.getStatus()) {
        case RUNNING:
          pipe.start();
          break;
        case STOP:
        case PARTIAL_START:
        case PARTIAL_STOP:
          pipe.stop();
          break;
        case PARTIAL_CREATE:
        case DROP:
          throw new PipeException("Unexpected status " + pipeInfo.getStatus().name());
        default:
          throw new IOException(
              String.format("Can not recognize running pipe status %s.", pipe.getStatus()));
      }
      if (pipe.getPipeSink().getType() == PipeSink.PipeSinkType.ExternalPipe) { // for external pipe
        // == start ExternalPipeProcessor for send data to external pipe plugin
        startExternalPipeManager(pipeInfo.getPipeName(), pipe.getStatus() == PipeStatus.RUNNING);
      }
    }
  }

  public List<ISyncManager> getOrCreateSyncManager(String dataRegionId) {
    // TODO(sync): maybe add cache to accelerate
    List<ISyncManager> syncManagerList = new ArrayList<>();
    for (Pipe pipe : pipes.values()) {
      if (pipe.isHistoryCollectFinished()) {
        // Only need to deal with pipe that has finished history file collection,
        syncManagerList.add(pipe.getOrCreateSyncManager(dataRegionId));
      }
    }
    return syncManagerList;
  }

  /** This method will be called before deleting dataRegion */
  public synchronized void unregisterDataRegion(String dataRegionId) {
    for (Pipe pipe : pipes.values()) {
      pipe.unregisterDataRegion(dataRegionId);
    }
  }

  @TestOnly
  public SenderManager getSenderManager() {
    return null;
  }
}
