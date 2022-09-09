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
package org.apache.iotdb.db.sync.common;

import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.sync.SyncPathUtil;
import org.apache.iotdb.db.exception.sync.PipeException;
import org.apache.iotdb.db.exception.sync.PipeSinkException;
import org.apache.iotdb.db.mpp.plan.constant.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.sys.sync.CreatePipeSinkStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.sync.CreatePipeStatement;
import org.apache.iotdb.db.qp.physical.sys.CreatePipePlan;
import org.apache.iotdb.db.qp.physical.sys.CreatePipeSinkPlan;
import org.apache.iotdb.db.sync.common.persistence.SyncLogReader;
import org.apache.iotdb.db.sync.common.persistence.SyncLogWriter;
import org.apache.iotdb.db.sync.sender.pipe.Pipe;
import org.apache.iotdb.db.sync.sender.pipe.PipeInfo;
import org.apache.iotdb.db.sync.sender.pipe.PipeMessage;
import org.apache.iotdb.db.sync.sender.pipe.PipeSink;
import org.apache.iotdb.db.utils.sync.SyncPipeUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SyncInfo {

  protected static final Logger LOGGER = LoggerFactory.getLogger(SyncInfo.class);

  private Map<String, PipeSink> pipeSinks;

  private PipeInfo runningPipe;
  // <PipeName, <CreateTime, PipeInfo>>
  private Map<String, Map<Long, PipeInfo>> pipes;

  protected SyncLogWriter syncLogWriter;

  public SyncInfo() {
    syncLogWriter = SyncLogWriter.getInstance();
    SyncLogReader logReader = new SyncLogReader();
    this.pipes = new ConcurrentHashMap<>();
    try {
      logReader.recover();
      pipeSinks = logReader.getAllPipeSinks();
      pipes = logReader.getAllPipeInfos();
      runningPipe = logReader.getRunningPipeInfo();
    } catch (StartupException e) {
      LOGGER.error(
          "Cannot recover ReceiverInfo because {}. Use default info values.", e.getMessage());
      pipeSinks = new ConcurrentHashMap<>();
    }
  }

  public void close() throws IOException {
    syncLogWriter.close();
  }

  // region Implement of PipeSink
  private boolean isPipeSinkExist(String name) {
    return pipeSinks.containsKey(name);
  }

  // TODO: delete this in new-standalone version
  public void addPipeSink(CreatePipeSinkPlan plan) throws PipeSinkException, IOException {
    if (isPipeSinkExist(plan.getPipeSinkName())) {
      throw new PipeSinkException(
          "There is a pipeSink named " + plan.getPipeSinkName() + " in IoTDB, please drop it.");
    }

    PipeSink pipeSink = SyncPipeUtil.parseCreatePipeSinkPlan(plan);
    // should guarantee the adding pipesink is not exist.
    pipeSinks.put(pipeSink.getPipeSinkName(), pipeSink);
    syncLogWriter.addPipeSink(plan);
  }

  public void addPipeSink(CreatePipeSinkStatement createPipeSinkStatement)
      throws PipeSinkException, IOException {
    if (isPipeSinkExist(createPipeSinkStatement.getPipeSinkName())) {
      throw new PipeSinkException(
          "There is a pipeSink named "
              + createPipeSinkStatement.getPipeSinkName()
              + " in IoTDB, please drop it.");
    }

    PipeSink pipeSink = SyncPipeUtil.parseCreatePipeSinkStatement(createPipeSinkStatement);
    // should guarantee the adding pipesink is not exist.
    pipeSinks.put(pipeSink.getPipeSinkName(), pipeSink);
    syncLogWriter.addPipeSink(createPipeSinkStatement);
  }

  public void dropPipeSink(String name) throws PipeSinkException, IOException {
    if (!isPipeSinkExist(name)) {
      throw new PipeSinkException("PipeSink " + name + " is not exist.");
    }
    if (runningPipe != null
        && runningPipe.getStatus() != Pipe.PipeStatus.DROP
        && runningPipe.getPipeSinkName().equals(name)) {
      throw new PipeSinkException(
          String.format(
              "Can not drop pipeSink %s, because pipe %s is using it.",
              name, runningPipe.getPipeName()));
    }
    pipeSinks.remove(name);
    syncLogWriter.dropPipeSink(name);
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
  // TODO: delete this in new-standalone version
  public void addPipe(CreatePipePlan plan, long createTime) throws PipeException, IOException {
    // common check
    if (runningPipe != null && runningPipe.getStatus() != Pipe.PipeStatus.DROP) {
      throw new PipeException(
          String.format(
              "Pipe %s is %s, please retry after drop it.",
              runningPipe.getPipeName(), runningPipe.getStatus().name()));
    }
    if (!isPipeSinkExist(plan.getPipeSinkName())) {
      throw new PipeException(String.format("Can not find pipeSink %s.", plan.getPipeSinkName()));
    }

    PipeSink runningPipeSink = getPipeSink(plan.getPipeSinkName());
    runningPipe = SyncPipeUtil.parseCreatePipePlanAsPipeInfo(plan, runningPipeSink, createTime);
    pipes
        .computeIfAbsent(runningPipe.getPipeName(), i -> new ConcurrentHashMap<>())
        .computeIfAbsent(runningPipe.getCreateTime(), i -> runningPipe);
    syncLogWriter.addPipe(plan, createTime);
  }

  public void addPipe(CreatePipeStatement createPipeStatement, long createTime)
      throws PipeException, IOException {
    // common check
    if (runningPipe != null && runningPipe.getStatus() != Pipe.PipeStatus.DROP) {
      throw new PipeException(
          String.format(
              "Pipe %s is %s, please retry after drop it.",
              runningPipe.getPipeName(), runningPipe.getStatus().name()));
    }
    if (!isPipeSinkExist(createPipeStatement.getPipeSinkName())) {
      throw new PipeException(
          String.format("Can not find pipeSink %s.", createPipeStatement.getPipeSinkName()));
    }

    PipeSink runningPipeSink = getPipeSink(createPipeStatement.getPipeSinkName());
    runningPipe =
        SyncPipeUtil.parseCreatePipePlanAsPipeInfo(
            createPipeStatement, runningPipeSink, createTime);
    pipes
        .computeIfAbsent(runningPipe.getPipeName(), i -> new ConcurrentHashMap<>())
        .computeIfAbsent(runningPipe.getCreateTime(), i -> runningPipe);
    syncLogWriter.addPipe(createPipeStatement, createTime);
  }

  public void operatePipe(String pipeName, StatementType statementType)
      throws PipeException, IOException {
    checkIfPipeExistAndRunning(pipeName);
    switch (statementType) {
      case START_PIPE:
        runningPipe.start();
        break;
      case STOP_PIPE:
        runningPipe.stop();
        break;
      case DROP_PIPE:
        runningPipe.drop();
        break;
      default:
        throw new PipeException("Unknown operatorType " + statementType);
    }
    syncLogWriter.operatePipe(pipeName, statementType);
  }

  public PipeInfo getPipeInfo(String pipeName, long createTime) {
    return pipes.get(pipeName).get(createTime);
  }

  public List<PipeInfo> getAllPipeInfos() {
    List<PipeInfo> pipeInfos = new ArrayList<>();
    for (Map<Long, PipeInfo> timePipeInfoMap : pipes.values()) {
      pipeInfos.addAll(timePipeInfoMap.values());
    }
    return pipeInfos;
  }

  /** @return null if no pipe has been created */
  public PipeInfo getRunningPipeInfo() {
    return runningPipe;
  }

  private void checkIfPipeExistAndRunning(String pipeName) throws PipeException {
    if (runningPipe == null || runningPipe.getStatus() == Pipe.PipeStatus.DROP) {
      throw new PipeException("There is no existing pipe.");
    }
    if (!runningPipe.getPipeName().equals(pipeName)) {
      throw new PipeException(
          String.format(
              "Pipe %s is %s, please retry after drop it.",
              runningPipe.getPipeName(), runningPipe.getStatus()));
    }
  }

  // endregion

  /**
   * Change Pipe Message. It will record the most important message about one pipe. ERROR > WARN >
   * NORMAL.
   *
   * @param pipeName name of pipe
   * @param createTime createTime of pipe
   * @param messageType pipe message type
   */
  public void changePipeMessage(
      String pipeName, long createTime, PipeMessage.PipeMessageType messageType) {
    if (messageType.compareTo(pipes.get(pipeName).get(createTime).getMessageType()) > 0) {
      pipes.get(pipeName).get(createTime).setMessageType(messageType);
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
}
