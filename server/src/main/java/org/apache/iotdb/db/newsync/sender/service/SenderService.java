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
package org.apache.iotdb.db.newsync.sender.service;

import org.apache.iotdb.db.exception.PipeException;
import org.apache.iotdb.db.exception.PipeSinkException;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.newsync.conf.SyncConstant;
import org.apache.iotdb.db.newsync.conf.SyncPathUtil;
import org.apache.iotdb.db.newsync.sender.pipe.IoTDBPipeSink;
import org.apache.iotdb.db.newsync.sender.pipe.Pipe;
import org.apache.iotdb.db.newsync.sender.pipe.PipeSink;
import org.apache.iotdb.db.newsync.sender.pipe.TransportHandler;
import org.apache.iotdb.db.newsync.sender.pipe.TsFilePipe;
import org.apache.iotdb.db.newsync.sender.recovery.SenderLogAnalyzer;
import org.apache.iotdb.db.newsync.sender.recovery.SenderLogger;
import org.apache.iotdb.db.newsync.transport.client.ITransportClient;
import org.apache.iotdb.db.newsync.transport.client.TransportClient;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.sys.CreatePipePlan;
import org.apache.iotdb.db.qp.physical.sys.CreatePipeSinkPlan;
import org.apache.iotdb.db.qp.utils.DatetimeUtils;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.utils.TestOnly;
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

public class SenderService implements IService {
  private static final Logger logger = LoggerFactory.getLogger(SenderService.class);
  private final SenderLogger senderLogger;

  private Map<String, PipeSink> pipeSinks;
  private List<Pipe> pipes;

  private Pipe runningPipe;
  private String runningMsg;

  private SenderService() {
    this.pipeSinks = new HashMap<>();
    this.pipes = new ArrayList<>();
    this.senderLogger = new SenderLogger();
    this.runningMsg = "";
  }

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
    for (Pair<String, String> pair : plan.getPipeSinkAttributes()) {
      pipeSink.setAttribute(pair.left, pair.right);
    }
    return pipeSink;
  }

  // should guarantee the adding pipesink is not exist.
  public void addPipeSink(PipeSink pipeSink) {
    pipeSinks.put(pipeSink.getName(), pipeSink);
  }

  public void dropPipeSink(String name) throws PipeSinkException {
    if (!isPipeSinkExist(name)) {
      throw new PipeSinkException("PipeSink " + name + " is not exist.");
    }
    if (runningPipe != null
        && runningPipe.getStatus() != Pipe.PipeStatus.DROP
        && runningPipe.getPipeSink().getName().equals(name)) {
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
              "start time %s is later than current time %s, this is not supported yet.",
              DatetimeUtils.convertLongToDate(plan.getDataStartTimestamp()),
              DatetimeUtils.convertLongToDate(currentTime)));
    }

    runningPipe = parseCreatePipePlan(plan, getPipeSink(plan.getPipeSinkName()), currentTime);
    pipes.add(runningPipe);
    senderLogger.addPipe(plan, currentTime);
  }

  public Pipe parseCreatePipePlan(CreatePipePlan plan, PipeSink pipeSink, long pipeCreateTime)
      throws PipeException {
    boolean syncDelOp = true;
    for (Pair<String, String> pair : plan.getPipeAttributes()) {
      pair.right = pair.right.toLowerCase();
      if ("syncdelop".equals(pair.left)) {
        syncDelOp = Boolean.parseBoolean(pair.right);
      } else {
        throw new PipeException(String.format("Can not recognition attribute %s", pair.left));
      }
    }

    TsFilePipe pipe =
        new TsFilePipe(
            pipeCreateTime, plan.getPipeName(), pipeSink, plan.getDataStartTimestamp(), syncDelOp);
    try {
      if (!(pipeSink instanceof IoTDBPipeSink)) {
        throw new PipeException(
            String.format(
                "Wrong pipeSink type %s for create pipe %s", pipeSink.getType(), pipe.getName()));
      }
      ITransportClient transportClient =
          new TransportClient(
              pipe, ((IoTDBPipeSink) pipeSink).getIp(), ((IoTDBPipeSink) pipeSink).getPort());
      pipe.setTransportHandler(new TransportHandler(transportClient, pipe.getName(), pipe.getCreateTime()));
    } catch (IOException e) {
      throw new PipeException(
          String.format(
              "Create transport for pipe %s error, because %s.", pipe.getName(), e.getMessage()));
    }
    return pipe;
  }

  public synchronized void stopPipe(String pipeName) throws PipeException {
    checkRunningPipeExistAndName(pipeName);
    if (runningPipe.getStatus() == Pipe.PipeStatus.RUNNING) {
      runningPipe.stop();
    }
    senderLogger.operatePipe(pipeName, Operator.OperatorType.STOP_PIPE);
  }

  public synchronized void startPipe(String pipeName) throws PipeException {
    checkRunningPipeExistAndName(pipeName);
    if (runningPipe.getStatus() == Pipe.PipeStatus.STOP) {
      runningPipe.start();
    }
    senderLogger.operatePipe(pipeName, Operator.OperatorType.START_PIPE);
  }

  public synchronized void dropPipe(String pipeName) throws PipeException {
    checkRunningPipeExistAndName(pipeName);
    runningPipe.drop();
    senderLogger.operatePipe(pipeName, Operator.OperatorType.DROP_PIPE);
  }

  public List<Pipe> getAllPipes() {
    return new ArrayList<>(pipes);
  }

  public synchronized String getPipeMsg(Pipe pipe) {
    return pipe == runningPipe ? runningMsg : "";
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

  /** transport */
  public synchronized void recMsg(SyncResponse response) {
    if (runningPipe == null || runningPipe.getStatus() == Pipe.PipeStatus.DROP) {
      logger.warn(String.format("No running pipe for receiving msg %s.", response));
      return;
    }
    logger.info(String.format("%s from receiver: %s", response.type.name(), response.msg));
    switch (response.type) {
      case INFO:
        break;
      case ERROR:
        try {
          runningPipe.stop();
        } catch (PipeException e) {
          logger.error(
              String.format(
                  "Stop pipe %s when meeting error in sender service, because %s.",
                  runningPipe.getName(), e));
        }
      case WARN:
        if (runningMsg.length() > 0) {
          runningMsg += System.lineSeparator();
        }
        runningMsg += (response.type.name() + " " + response.msg);
        senderLogger.recordMsg(
            runningPipe.getName(),
            runningPipe.getStatus() == Pipe.PipeStatus.RUNNING
                ? Operator.OperatorType.START_PIPE
                : Operator.OperatorType.STOP_PIPE,
            response.msg);
        break;
    }
  }

  /** IService * */
  @Override
  public void start() throws StartupException {
    File senderLog = new File(SyncPathUtil.getSysDir(), SyncConstant.SENDER_LOG_NAME);
    if (senderLog.exists()) {
      try {
        recover();
      } catch (IOException e) {
        throw new StartupException(e.getMessage());
      }
    }
  }

  @Override
  public void stop() {
    if (runningPipe != null) {
      try {
        runningPipe.stop();
      } catch (PipeException e) {
        logger.warn(String.format("Running pipe %s has been dropped.", runningPipe.getName()));
      }
    }
    senderLogger.close();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.SENDER_SERVICE;
  }

  private void recover() throws IOException {
    SenderLogAnalyzer analyzer = new SenderLogAnalyzer();
    analyzer.recover();
    this.pipeSinks = analyzer.getRecoveryAllPipeSinks();
    this.pipes = analyzer.getRecoveryAllPipes();
    this.runningPipe = analyzer.getRecoveryRunningPipe();
    this.runningMsg = analyzer.getRecoveryRunningMsg();
  }

  /** test */
  @TestOnly
  public Pipe getRunningPipe() {
    return runningPipe;
  }
}
