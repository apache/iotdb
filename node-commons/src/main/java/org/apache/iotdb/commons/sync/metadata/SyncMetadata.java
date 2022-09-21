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
package org.apache.iotdb.commons.sync.metadata;

import org.apache.iotdb.commons.exception.sync.PipeException;
import org.apache.iotdb.commons.exception.sync.PipeSinkException;
import org.apache.iotdb.commons.sync.pipe.PipeInfo;
import org.apache.iotdb.commons.sync.pipe.PipeMessage;
import org.apache.iotdb.commons.sync.pipe.PipeOperation;
import org.apache.iotdb.commons.sync.pipe.PipeStatus;
import org.apache.iotdb.commons.sync.pipesink.PipeSink;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SyncMetadata {

  // <PipeSinkName, PipeSink>
  private Map<String, PipeSink> pipeSinks;

  private PipeInfo runningPipe;
  // <PipeName, <CreateTime, PipeInfo>>
  private Map<String, Map<Long, PipeInfo>> pipes;

  public SyncMetadata() {
    this.pipes = new ConcurrentHashMap<>();
    this.pipeSinks = new ConcurrentHashMap<>();
  }

  // ======================================================
  // region Implement of Getter and Setter
  // ======================================================

  public Map<String, PipeSink> getPipeSinks() {
    return pipeSinks;
  }

  public void setPipeSinks(Map<String, PipeSink> pipeSinks) {
    this.pipeSinks = pipeSinks;
  }

  public PipeInfo getRunningPipe() {
    return runningPipe;
  }

  public void setRunningPipe(PipeInfo runningPipe) {
    this.runningPipe = runningPipe;
  }

  public Map<String, Map<Long, PipeInfo>> getPipes() {
    return pipes;
  }

  public void setPipes(Map<String, Map<Long, PipeInfo>> pipes) {
    this.pipes = pipes;
  }

  // endregion

  // ======================================================
  // region Implement of PipeSink
  // ======================================================

  public boolean isPipeSinkExist(String name) {
    return pipeSinks.containsKey(name);
  }

  public void checkAddPipeSink(String pipeSinkName) throws PipeSinkException {
    if (isPipeSinkExist(pipeSinkName)) {
      throw new PipeSinkException(
          "There is a PipeSink named "
              + pipeSinkName
              + " in IoTDB, please drop it before recreation.");
    }
  }

  public void addPipeSink(PipeSink pipeSink) {
    pipeSinks.put(pipeSink.getPipeSinkName(), pipeSink);
  }

  public void checkDropPipeSink(String pipeSinkName) throws PipeSinkException {
    if (!isPipeSinkExist(pipeSinkName)) {
      throw new PipeSinkException("PipeSink " + pipeSinkName + " does not exist.");
    }
    if (runningPipe != null
        && runningPipe.getStatus() != PipeStatus.DROP
        && runningPipe.getPipeSinkName().equals(pipeSinkName)) {
      throw new PipeSinkException(
          String.format(
              "Can not drop PipeSink %s, because Pipe %s is using it.",
              pipeSinkName, runningPipe.getPipeName()));
    }
  }

  public void dropPipeSink(String name) {
    pipeSinks.remove(name);
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

  // ======================================================
  // region Implement of Pipe
  // ======================================================

  public void addPipe(PipeInfo pipeInfo, PipeSink pipeSink) throws PipeException {
    // common check
    if (runningPipe != null && runningPipe.getStatus() != PipeStatus.DROP) {
      throw new PipeException(
          String.format(
              "Pipe %s is %s, please retry after drop it.",
              runningPipe.getPipeName(), runningPipe.getStatus().name()));
    }
    runningPipe = pipeInfo;
    pipes
        .computeIfAbsent(runningPipe.getPipeName(), i -> new ConcurrentHashMap<>())
        .computeIfAbsent(runningPipe.getCreateTime(), i -> runningPipe);
  }

  public void operatePipe(String pipeName, PipeOperation pipeOperation) throws PipeException {
    checkIfPipeExistAndRunning(pipeName);
    switch (pipeOperation) {
      case START:
        runningPipe.start();
        break;
      case STOP:
        runningPipe.stop();
        break;
      case DROP:
        runningPipe.drop();
        break;
      default:
        throw new PipeException("Unknown operatorType " + pipeOperation);
    }
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
    if (runningPipe == null || runningPipe.getStatus() == PipeStatus.DROP) {
      throw new PipeException("There is no existing pipe.");
    }
    if (!runningPipe.getPipeName().equals(pipeName)) {
      throw new PipeException(
          String.format(
              "Pipe %s is %s, please retry after drop it.",
              runningPipe.getPipeName(), runningPipe.getStatus()));
    }
  }

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

  // endregion

}
