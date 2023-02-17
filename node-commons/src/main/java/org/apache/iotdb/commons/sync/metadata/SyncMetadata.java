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

import org.apache.iotdb.commons.exception.sync.PipeAlreadyExistException;
import org.apache.iotdb.commons.exception.sync.PipeException;
import org.apache.iotdb.commons.exception.sync.PipeNotExistException;
import org.apache.iotdb.commons.exception.sync.PipeSinkAlreadyExistException;
import org.apache.iotdb.commons.exception.sync.PipeSinkBeingUsedException;
import org.apache.iotdb.commons.exception.sync.PipeSinkException;
import org.apache.iotdb.commons.exception.sync.PipeSinkNotExistException;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.commons.sync.persistence.SyncLogReader;
import org.apache.iotdb.commons.sync.persistence.SyncLogWriter;
import org.apache.iotdb.commons.sync.pipe.PipeInfo;
import org.apache.iotdb.commons.sync.pipe.PipeMessage;
import org.apache.iotdb.commons.sync.pipe.PipeStatus;
import org.apache.iotdb.commons.sync.pipe.SyncOperation;
import org.apache.iotdb.commons.sync.pipesink.PipeSink;
import org.apache.iotdb.commons.sync.utils.SyncConstant;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class SyncMetadata implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(SyncMetadata.class);

  // <PipeSinkName, PipeSink>
  private Map<String, PipeSink> pipeSinks;

  // <PipeName, PipeInfo>
  private Map<String, PipeInfo> pipes;

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

  public Map<String, PipeInfo> getPipes() {
    return pipes;
  }

  public void setPipes(Map<String, PipeInfo> pipes) {
    this.pipes = pipes;
  }

  // endregion

  // ======================================================
  // region Implement of PipeSink
  // ======================================================

  public boolean isPipeSinkExist(String name) {
    return pipeSinks.containsKey(name);
  }

  public void checkPipeSinkNoExist(String pipeSinkName) throws PipeSinkException {
    if (isPipeSinkExist(pipeSinkName)) {
      throw new PipeSinkAlreadyExistException(pipeSinkName);
    }
  }

  public void addPipeSink(PipeSink pipeSink) {
    pipeSinks.put(pipeSink.getPipeSinkName(), pipeSink);
  }

  public void checkDropPipeSink(String pipeSinkName) throws PipeSinkException {
    if (!isPipeSinkExist(pipeSinkName)) {
      throw new PipeSinkNotExistException(pipeSinkName);
    }
    for (PipeInfo pipeInfo : pipes.values()) {
      if (pipeInfo.getPipeSinkName().equals(pipeSinkName)) {
        throw new PipeSinkBeingUsedException(pipeSinkName, pipeInfo.getPipeName());
      }
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

  public void checkAddPipe(PipeInfo pipeInfo) throws PipeException, PipeSinkNotExistException {
    // check PipeSink exists
    if (!isPipeSinkExist(pipeInfo.getPipeSinkName())) {
      throw new PipeSinkNotExistException(pipeInfo.getPipeSinkName());
    }
    // check Pipe does not exist
    if (pipes.containsKey(pipeInfo.getPipeName())) {
      PipeInfo runningPipe = pipes.get(pipeInfo.getPipeName());
      throw new PipeAlreadyExistException(runningPipe.getPipeName(), runningPipe.getStatus());
    }
  }

  public void addPipe(PipeInfo pipeInfo) {
    pipes.putIfAbsent(pipeInfo.getPipeName(), pipeInfo);
  }

  public void dropPipe(String pipeName) {
    pipes.remove(pipeName);
  }

  public void setPipeStatus(String pipeName, PipeStatus status) {
    pipes.get(pipeName).setStatus(status);
    if (status.equals(PipeStatus.RUNNING)) {
      pipes.get(pipeName).setMessageType(PipeMessage.PipeMessageType.NORMAL);
    }
  }

  public PipeInfo getPipeInfo(String pipeName) {
    return pipes.get(pipeName);
  }

  public List<PipeInfo> getAllPipeInfos() {
    return new ArrayList<>(pipes.values());
  }

  public void checkIfPipeExist(String pipeName) throws PipeException {
    if (!pipes.containsKey(pipeName)) {
      throw new PipeNotExistException(pipeName);
    }
  }

  /**
   * Change Pipe Message. It will record the most important message about one pipe. ERROR > WARN >
   * NORMAL.
   *
   * @param pipeName name of pipe
   * @param messageType pipe message type
   */
  public void changePipeMessage(String pipeName, PipeMessage.PipeMessageType messageType) {
    if (messageType.compareTo(pipes.get(pipeName).getMessageType()) > 0) {
      pipes.get(pipeName).setMessageType(messageType);
    }
  }

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws TException, IOException {
    File snapshotFile = new File(snapshotDir, SyncConstant.SYNC_LOG_NAME);
    if (snapshotFile.exists() && snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to take snapshot, because snapshot file [{}] is already exist.",
          snapshotFile.getAbsolutePath());
      return false;
    }
    File tmpFile = new File(snapshotFile.getAbsolutePath() + "-" + UUID.randomUUID());
    try (SyncLogWriter writer = new SyncLogWriter(snapshotDir, tmpFile.getName())) {
      writer.initOutputStream();
      for (PipeSink pipeSink : pipeSinks.values()) {
        writer.addPipeSink(pipeSink);
      }
      for (PipeInfo pipeInfo : pipes.values()) {
        writer.addPipe(pipeInfo);
        switch (pipeInfo.getStatus()) {
          case RUNNING:
            writer.operatePipe(pipeInfo.getPipeName(), SyncOperation.START_PIPE);
            break;
          case STOP:
            writer.operatePipe(pipeInfo.getPipeName(), SyncOperation.STOP_PIPE);
            break;
          default:
            break;
        }
      }
    }
    return tmpFile.renameTo(snapshotFile);
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws TException, IOException {
    File snapshotFile = new File(snapshotDir, SyncConstant.SYNC_LOG_NAME);
    if (!snapshotFile.exists() || !snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to load snapshot,snapshot file [{}] is not exist.",
          snapshotFile.getAbsolutePath());
      return;
    }
    SyncLogReader reader = new SyncLogReader(snapshotDir, snapshotFile.getName());
    reader.recover();
    setPipes(reader.getPipes());
    setPipeSinks(reader.getAllPipeSinks());
  }

  // endregion

}
