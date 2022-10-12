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
package org.apache.iotdb.commons.sync.persistence;

import org.apache.iotdb.commons.sync.pipe.PipeInfo;
import org.apache.iotdb.commons.sync.pipe.PipeStatus;
import org.apache.iotdb.commons.sync.pipe.SyncOperation;
import org.apache.iotdb.commons.sync.pipesink.PipeSink;
import org.apache.iotdb.commons.sync.utils.SyncConstant;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SyncLogReader {
  private static final Logger logger = LoggerFactory.getLogger(SyncLogReader.class);
  // <pipeSinkName, PipeSink>
  private final Map<String, PipeSink> pipeSinks = new ConcurrentHashMap<>();
  private final Map<String, Map<Long, PipeInfo>> pipes = new ConcurrentHashMap<>();
  private PipeInfo runningPipe;
  private final File dir;
  private final String fileName;

  public SyncLogReader(File dir) {
    this.dir = dir;
    this.fileName = SyncConstant.SYNC_LOG_NAME;
  }

  public SyncLogReader(File dir, String fileName) {
    this.dir = dir;
    this.fileName = fileName;
  }

  public void recover() throws IOException {
    logger.info("Start to recover all sync state for sync.");
    File serviceLogFile = new File(dir, fileName);
    if (!serviceLogFile.exists()) {
      logger.warn("Sync service log file not found");
    } else {
      try (InputStream inputStream = new FileInputStream(serviceLogFile)) {
        recoverPipe(inputStream);
      }
    }
  }

  public Map<String, PipeSink> getAllPipeSinks() {
    return pipeSinks;
  }

  public Map<String, Map<Long, PipeInfo>> getAllPipeInfos() {
    return pipes;
  }

  public PipeInfo getRunningPipeInfo() {
    return runningPipe;
  }

  private void recoverPipe(InputStream inputStream) throws IOException {
    byte nextByte;
    while ((nextByte = ReadWriteIOUtils.readByte(inputStream)) != -1) {
      SyncOperation operationType = SyncOperation.values()[nextByte];
      switch (operationType) {
        case CREATE_PIPESINK:
          PipeSink pipeSink = PipeSink.deserializePipeSink(inputStream);
          pipeSinks.put(pipeSink.getPipeSinkName(), pipeSink);
          break;
        case DROP_PIPESINK:
          pipeSinks.remove(ReadWriteIOUtils.readString(inputStream));
          break;
        case CREATE_PIPE:
          runningPipe = PipeInfo.deserializePipeInfo(inputStream);
          pipes
              .computeIfAbsent(runningPipe.getPipeName(), i -> new ConcurrentHashMap<>())
              .computeIfAbsent(runningPipe.getCreateTime(), i -> runningPipe);
          break;
        case STOP_PIPE:
          // TODO: support multiple pipe
          ReadWriteIOUtils.readString(inputStream);
          runningPipe.setStatus(PipeStatus.STOP);
          break;
        case START_PIPE:
          // TODO: support multiple pipe
          ReadWriteIOUtils.readString(inputStream);
          runningPipe.setStatus(PipeStatus.RUNNING);
          break;
        case DROP_PIPE:
          // TODO: support multiple pipe
          ReadWriteIOUtils.readString(inputStream);
          runningPipe.setStatus(PipeStatus.DROP);
          break;
        default:
          throw new UnsupportedOperationException(
              String.format("Can not recognize SyncOperation %s.", operationType.name()));
      }
    }
  }
}
