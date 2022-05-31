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
package org.apache.iotdb.db.sync.sender.service;

import org.apache.iotdb.commons.exception.ShutdownException;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.db.exception.SyncConnectionException;
import org.apache.iotdb.db.exception.sync.PipeException;
import org.apache.iotdb.db.exception.sync.PipeSinkException;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.sys.CreatePipePlan;
import org.apache.iotdb.db.qp.physical.sys.CreatePipeSinkPlan;
import org.apache.iotdb.db.qp.utils.DatetimeUtils;
import org.apache.iotdb.db.sync.conf.SyncConstant;
import org.apache.iotdb.db.sync.conf.SyncPathUtil;
import org.apache.iotdb.db.sync.externalpipe.ExtPipePluginManager;
import org.apache.iotdb.db.sync.externalpipe.ExtPipePluginRegister;
import org.apache.iotdb.db.sync.sender.pipe.ExternalPipeSink;
import org.apache.iotdb.db.sync.sender.pipe.IoTDBPipeSink;
import org.apache.iotdb.db.sync.sender.pipe.Pipe;
import org.apache.iotdb.db.sync.sender.pipe.PipeSink;
import org.apache.iotdb.db.sync.sender.pipe.TsFilePipe;
import org.apache.iotdb.db.sync.sender.recovery.SenderLogAnalyzer;
import org.apache.iotdb.db.sync.sender.recovery.SenderLogger;
import org.apache.iotdb.pipe.external.api.IExternalPipeSinkWriterFactory;
import org.apache.iotdb.service.transport.thrift.RequestType;
import org.apache.iotdb.service.transport.thrift.SyncResponse;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class SenderService implements IService {
  private static final Logger logger = LoggerFactory.getLogger(SenderService.class);
  private SenderLogger senderLogger;

  private Map<String, PipeSink> pipeSinks;
  private List<Pipe> pipes;

  private Pipe runningPipe;
  private MsgManager msgManager;

  private TransportHandler transportHandler;

  /* handle external Pipe */
  private ExtPipePluginManager extPipePluginManager;

  private SenderService() {}

  private static class SenderServiceHolder {
    private static final SenderService INSTANCE = new SenderService();

    private SenderServiceHolder() {}
  }

  public static SenderService getInstance() {
    return SenderService.SenderServiceHolder.INSTANCE;
  }

  /** pipesink * */
  public PipeSink getPipeSink(String name) {
    return pipeSinks.getOrDefault(name, null);
  }

  public ExtPipePluginManager getExternalPipeManager() {
    return extPipePluginManager;
  }

  public boolean isPipeSinkExist(String name) {
    return pipeSinks.containsKey(name);
  }

  public void addPipeSink(CreatePipeSinkPlan plan) throws PipeSinkException {
    if (isPipeSinkExist(plan.getPipeSinkName())) {
      throw new PipeSinkException(
          "There is a pipeSink named " + plan.getPipeSinkName() + " in IoTDB, please drop it.");
    }

    addPipeSink(parseCreatePipeSinkPlan(plan));
    senderLogger.addPipeSink(plan);
  }

  public PipeSink parseCreatePipeSinkPlan(CreatePipeSinkPlan plan) throws PipeSinkException {
    PipeSink pipeSink;
    try {
      pipeSink =
          PipeSink.PipeSinkFactory.createPipeSink(plan.getPipeSinkType(), plan.getPipeSinkName());
    } catch (UnsupportedOperationException e) {
      throw new PipeSinkException(e.getMessage());
    }

    pipeSink.setAttribute(plan.getPipeSinkAttributes());
    return pipeSink;
  }

  // should guarantee the adding pipesink is not exist.
  public void addPipeSink(PipeSink pipeSink) {
    pipeSinks.put(pipeSink.getPipeSinkName(), pipeSink);
  }

  public void dropPipeSink(String name) throws PipeSinkException {
    if (!isPipeSinkExist(name)) {
      throw new PipeSinkException("PipeSink " + name + " is not exist.");
    }
    if (runningPipe != null
        && runningPipe.getStatus() != Pipe.PipeStatus.DROP
        && runningPipe.getPipeSink().getPipeSinkName().equals(name)) {
      throw new PipeSinkException(
          String.format(
              "Can not drop pipeSink %s, because pipe %s is using it.",
              name, runningPipe.getName()));
    }

    pipeSinks.remove(name);
    senderLogger.dropPipeSink(name);
  }

  public List<PipeSink> getAllPipeSink() {
    List<PipeSink> allPipeSinks = new ArrayList<>();
    for (Map.Entry<String, PipeSink> entry : pipeSinks.entrySet()) {
      allPipeSinks.add(entry.getValue());
    }
    return allPipeSinks;
  }

  /** pipe * */
  public synchronized void addPipe(CreatePipePlan plan) throws PipeException {
    // common check
    if (runningPipe != null && runningPipe.getStatus() != Pipe.PipeStatus.DROP) {
      throw new PipeException(
          String.format(
              "Pipe %s is %s, please retry after drop it.",
              runningPipe.getName(), runningPipe.getStatus().name()));
    }
    if (!isPipeSinkExist(plan.getPipeSinkName())) {
      throw new PipeException(String.format("Can not find pipeSink %s.", plan.getPipeSinkName()));
    }
    long currentTime = DatetimeUtils.currentTime();
    if (plan.getDataStartTimestamp() > currentTime) {
      throw new PipeException(
          String.format(
              "Start time %s is later than current time %s, this is not supported yet.",
              DatetimeUtils.convertLongToDate(plan.getDataStartTimestamp()),
              DatetimeUtils.convertLongToDate(currentTime)));
    }

    PipeSink runningPipeSink = getPipeSink(plan.getPipeSinkName());
    runningPipe = parseCreatePipePlan(plan, runningPipeSink, currentTime);
    if (runningPipe.getPipeSink().getType() == PipeSink.PipeSinkType.IoTDB) {
      try {
        transportHandler =
            TransportHandler.getNewTransportHandler(runningPipe, (IoTDBPipeSink) runningPipeSink);
        sendMsg(RequestType.CREATE);
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
      } catch (PipeException e) {
        runningPipe = null;
        throw e;
      }
    } else { // for external pipe
      // == start ExternalPipeProcessor for send data to external pipe plugin
      startExternalPipeManager(false);
    }

    msgManager.addPipe(runningPipe);
    pipes.add(runningPipe);
    senderLogger.addPipe(plan, currentTime);
  }

  public Pipe parseCreatePipePlan(CreatePipePlan plan, PipeSink pipeSink, long pipeCreateTime)
      throws PipeException {
    boolean syncDelOp = true;
    for (Pair<String, String> pair : plan.getPipeAttributes()) {
      pair.left = pair.left.toLowerCase();
      if ("syncdelop".equals(pair.left)) {
        syncDelOp = Boolean.parseBoolean(pair.right);
      } else {
        throw new PipeException(String.format("Can not recognition attribute %s", pair.left));
      }
    }

    return new TsFilePipe(
        pipeCreateTime, plan.getPipeName(), pipeSink, plan.getDataStartTimestamp(), syncDelOp);
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
    senderLogger.operatePipe(pipeName, Operator.OperatorType.STOP_PIPE);
  }

  public synchronized void startPipe(String pipeName) throws PipeException {
    checkRunningPipeExistAndName(pipeName);
    if (runningPipe.getStatus() == Pipe.PipeStatus.STOP) {
      if (runningPipe.getPipeSink().getType() == PipeSink.PipeSinkType.IoTDB) {
        sendMsg(RequestType.START);
        runningPipe.start();
        transportHandler.start();
      } else { // for external PIPE
        runningPipe.start();
        startExternalPipeManager(true);
      }
    }
    senderLogger.operatePipe(pipeName, Operator.OperatorType.START_PIPE);
  }

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
        msgManager.removeAllPipe();
        sendMsg(RequestType.DROP);
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

      senderLogger.operatePipe(pipeName, Operator.OperatorType.DROP_PIPE);
    } catch (InterruptedException e) {
      logger.warn(
          String.format("Interrupted when waiting for clear transport %s.", runningPipe.getName()),
          e);
      throw new PipeException("Drop error, be interrupted, please try again.");
    }
  }

  public List<Pipe> getAllPipes() {
    return new ArrayList<>(pipes);
  }

  public synchronized String getPipeMsg(Pipe pipe) {
    return msgManager.getPipeMsg(pipe);
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

  public void setConnecting(boolean isConnecting) {
    runningPipe.setDisconnected(isConnecting);
  }

  /** transport */
  private void sendMsg(RequestType type) throws PipeException {
    try {
      receiveMsg(transportHandler.sendMsg(type));
    } catch (SyncConnectionException e) {
      logger.warn(
          String.format(
              "Connect to pipeSink %s error when %s pipe.", runningPipe.getPipeSink(), type.name()),
          e);
      throw new PipeException(
          String.format(
              "Can not connect to pipeSink %s, please check net and receiver is available, and try again.",
              runningPipe.getPipeSink()));
    }
  }

  public synchronized void receiveMsg(SyncResponse response) {
    if (runningPipe == null || runningPipe.getStatus() == Pipe.PipeStatus.DROP) {
      logger.info(String.format("No running pipe for receiving msg %s.", response));
      return;
    }
    switch (response.type) {
      case INFO:
        break;
      case ERROR:
        logger.warn(String.format("%s from receiver: %s", response.type.name(), response.msg));
        try {
          stopPipe(runningPipe.getName());
        } catch (PipeException e) {
          logger.warn(
              String.format(
                  "Stop pipe %s when meeting error in sender service.", runningPipe.getName()),
              e);
        }
      case WARN:
        msgManager.recordMsg(
            runningPipe,
            runningPipe.getStatus() == Pipe.PipeStatus.RUNNING
                ? Operator.OperatorType.START_PIPE
                : Operator.OperatorType.STOP_PIPE,
            response.type,
            response.msg);
        break;
    }
  }

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

    this.pipeSinks = new HashMap<>();
    this.pipes = new ArrayList<>();
    this.senderLogger = new SenderLogger();
    this.msgManager = new MsgManager(senderLogger);

    File senderLog = new File(SyncPathUtil.getSysDir(), SyncConstant.SENDER_LOG_NAME);
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
    pipeSinks = null;
    pipes = null;
    msgManager = null;
    senderLogger.close();

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
    return ServiceType.SENDER_SERVICE;
  }

  private void recover() throws IOException, PipeException {
    SenderLogAnalyzer analyzer = new SenderLogAnalyzer();
    analyzer.recover();
    this.pipeSinks = analyzer.getRecoveryAllPipeSinks();
    this.pipes = analyzer.getRecoveryAllPipes();
    this.runningPipe = analyzer.getRecoveryRunningPipe();
    this.msgManager = analyzer.getMsgManager();

    if (runningPipe == null || Pipe.PipeStatus.DROP.equals(runningPipe.getStatus())) {
      return;
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
