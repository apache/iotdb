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

import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.sync.SyncPathUtil;
import org.apache.iotdb.db.exception.sync.PipeException;
import org.apache.iotdb.db.exception.sync.PipeSinkException;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.sys.CreatePipePlan;
import org.apache.iotdb.db.qp.physical.sys.CreatePipeSinkPlan;
import org.apache.iotdb.db.sync.SyncUtils;
import org.apache.iotdb.db.sync.receiver.manager.PipeMessage;
import org.apache.iotdb.db.sync.receiver.recovery.SyncLogAnalyzer;
import org.apache.iotdb.db.sync.receiver.recovery.SyncLogger;
import org.apache.iotdb.db.sync.sender.pipe.Pipe;
import org.apache.iotdb.db.sync.sender.pipe.PipeSink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractSyncInfo {

  protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractSyncInfo.class);

  protected boolean pipeServerEnable;
  // <pipeName, <remoteIp, <createTime, status>>>
  //  protected Map<String, Map<String, Map<Long, Pipe.PipeStatus>>> pipeInfos;
  // <pipeFolderName, pipeMsg>
  protected Map<String, List<PipeMessage>> pipeMessageMap;

  private Map<String, PipeSink> pipeSinks;

  // TODO: extract pipe info
  private Pipe runningPipe;
  private List<Pipe> pipes;

  // TODO: combine
  protected SyncLogger syncLogger;
  //  private SenderLogger senderLogger;

  public AbstractSyncInfo() {
    syncLogger = new SyncLogger();
    SyncLogAnalyzer analyzer = new SyncLogAnalyzer();
    try {
      analyzer.recover();
      pipeSinks = analyzer.getAllPipeSinks();
      pipes = analyzer.getAllPipes();
      runningPipe = analyzer.getRunningPipe();
      pipeServerEnable = analyzer.isPipeServerEnable();
      pipeMessageMap = analyzer.getPipeMessageMap();
    } catch (StartupException e) {
      LOGGER.error(
          "Cannot recover ReceiverInfo because {}. Use default info values.", e.getMessage());
      pipeSinks = new ConcurrentHashMap<>();
      pipes = new ArrayList<>();
      pipeMessageMap = new ConcurrentHashMap<>();
      pipeServerEnable = false;
    }
  }

  protected abstract void afterStartPipe(String pipeName, String remoteIp, long createTime);

  protected abstract void afterStopPipe(String pipeName, String remoteIp, long createTime);

  protected abstract void afterDropPipe(String pipeName, String remoteIp, long createTime);

  public void close() throws IOException {
    syncLogger.close();
  }

  // region Implement of PipeServer

  public void startServer() throws IOException {
    syncLogger.startPipeServer();
    pipeServerEnable = true;
  }

  public void stopServer() throws IOException {
    syncLogger.stopPipeServer();
    pipeServerEnable = false;
  }

  // endregion

  // region Implement of PipeSink
  private boolean isPipeSinkExist(String name) {
    return pipeSinks.containsKey(name);
  }

  public void addPipeSink(CreatePipeSinkPlan plan) throws PipeSinkException, IOException {
    if (isPipeSinkExist(plan.getPipeSinkName())) {
      throw new PipeSinkException(
          "There is a pipeSink named " + plan.getPipeSinkName() + " in IoTDB, please drop it.");
    }

    PipeSink pipeSink = SyncUtils.parseCreatePipeSinkPlan(plan);
    // should guarantee the adding pipesink is not exist.
    pipeSinks.put(pipeSink.getPipeSinkName(), pipeSink);
    syncLogger.addPipeSink(plan);
  }

  public void dropPipeSink(String name) throws PipeSinkException, IOException {
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
    syncLogger.dropPipeSink(name);
  }

  public PipeSink getPipeSink(String name) {
    return pipeSinks.getOrDefault(name, null);
  }

  public List<PipeSink> getAllPipeSink() {
    List<PipeSink> allPipeSinks = new ArrayList<>();
    for (Map.Entry<String, PipeSink> entry : pipeSinks.entrySet()) {
      allPipeSinks.add(entry.getValue());
    }
    return allPipeSinks;
  }

  // endregion

  // region Implement of Pipe

  public void addPipe(CreatePipePlan plan, long createTime) throws PipeException, IOException {
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

    PipeSink runningPipeSink = getPipeSink(plan.getPipeSinkName());
    runningPipe = SyncUtils.parseCreatePipePlan(plan, runningPipeSink, createTime);
    pipes.add(runningPipe);
    syncLogger.addPipe(plan, createTime);
  }

  public void operatePipe(String pipeName, Operator.OperatorType operatorType)
      throws PipeException, IOException {
    checkRunningPipeExistAndName(pipeName);
    syncLogger.operatePipe(pipeName, operatorType);
  }

  public List<Pipe> getAllPipes() {
    return pipes;
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

  // endregion

  //  public List<PipeInfo> getPipeInfosByPipeName(String pipeName) {
  //    if (!pipeInfos.containsKey(pipeName)) {
  //      return Collections.emptyList();
  //    }
  //    List<PipeInfo> res = new ArrayList<>();
  //    for (Map.Entry<String, Map<Long, Pipe.PipeStatus>> remoteIpEntry :
  //        pipeInfos.get(pipeName).entrySet()) {
  //      for (Map.Entry<Long, Pipe.PipeStatus> createTimeEntry :
  // remoteIpEntry.getValue().entrySet()) {
  //        res.add(
  //            new PipeInfo(
  //                pipeName,
  //                remoteIpEntry.getKey(),
  //                createTimeEntry.getValue(),
  //                createTimeEntry.getKey()));
  //      }
  //    }
  //    return res;
  //  }
  //
  //  public PipeInfo getPipeInfo(String pipeName, String remoteIp, long createTime) {
  //    if (pipeInfos.containsKey(pipeName)
  //        && pipeInfos.get(pipeName).containsKey(remoteIp)
  //        && pipeInfos.get(pipeName).get(remoteIp).containsKey(createTime)) {
  //      return new PipeInfo(
  //          pipeName, remoteIp, pipeInfos.get(pipeName).get(remoteIp).get(createTime),
  // createTime);
  //    }
  //    return null;
  //  }
  //
  //  public List<PipeInfo> getAllPipeInfos() {
  //    List<PipeInfo> res = new ArrayList<>();
  //    for (String pipeName : pipeInfos.keySet()) {
  //      res.addAll(getPipeInfosByPipeName(pipeName));
  //    }
  //    return res;
  //  }

  /**
   * write a single message and serialize to disk
   *
   * @param pipeName name of pipe
   * @param createTime createTime of pipe
   * @param message pipe message
   */
  public synchronized void writePipeMessage(String pipeName, long createTime, PipeMessage message) {
    String pipeIdentifier = SyncPathUtil.getSenderPipeDir(pipeName, createTime);
    try {
      syncLogger.writePipeMsg(pipeIdentifier, message);
    } catch (IOException e) {
      LOGGER.error(
          "Can not write pipe message {} from {} to disk because {}",
          message,
          pipeIdentifier,
          e.getMessage());
    }
    pipeMessageMap.computeIfAbsent(pipeIdentifier, i -> new ArrayList<>()).add(message);
  }

  /**
   * read recent messages about one pipe
   *
   * @param pipeName name of pipe
   * @param createTime createTime of pipe
   * @param consume if consume is true, these messages will not be deleted. Otherwise, these
   *     messages can be read next time.
   * @return recent messages
   */
  public synchronized List<PipeMessage> getPipeMessages(
      String pipeName, long createTime, boolean consume) {
    List<PipeMessage> pipeMessageList = new ArrayList<>();
    String pipeIdentifier = SyncPathUtil.getSenderPipeDir(pipeName, createTime);
    if (consume) {
      try {
        syncLogger.comsumePipeMsg(pipeIdentifier);
      } catch (IOException e) {
        LOGGER.error(
            "Can not read pipe message about {} from disk because {}",
            pipeIdentifier,
            e.getMessage());
      }
    }
    if (pipeMessageMap.containsKey(pipeIdentifier)) {
      pipeMessageList = pipeMessageMap.get(pipeIdentifier);
      if (consume) {
        pipeMessageMap.remove(pipeIdentifier);
      }
    }
    return pipeMessageList;
  }

  /**
   * read the most important message about one pipe. ERROR > WARN > INFO.
   *
   * @param pipeName name of pipe
   * @param createTime createTime of pipe
   * @param consume if consume is true, recent messages will not be deleted. Otherwise, these
   *     messages can be read next time.
   * @return the most important message
   */
  public PipeMessage getPipeMessage(String pipeName, long createTime, boolean consume) {
    List<PipeMessage> pipeMessageList = getPipeMessages(pipeName, createTime, consume);
    PipeMessage message = new PipeMessage(PipeMessage.MsgType.INFO, "");
    if (!pipeMessageList.isEmpty()) {
      for (PipeMessage pipeMessage : pipeMessageList) {
        if (pipeMessage.getType().getValue() > message.getType().getValue()) {
          message = pipeMessage;
        }
      }
    }
    return message;
  }

  public boolean isPipeServerEnable() {
    return pipeServerEnable;
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
}
