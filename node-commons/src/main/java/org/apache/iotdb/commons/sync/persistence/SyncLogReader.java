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
  // <pipeName, Pipe>
  private final Map<String, PipeInfo> pipes = new ConcurrentHashMap<>();
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

  public Map<String, PipeInfo> getPipes() {
    return pipes;
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
          PipeInfo pipeInfo = PipeInfo.deserializePipeInfo(inputStream);
          pipes.putIfAbsent(pipeInfo.getPipeName(), pipeInfo);
          break;
        case STOP_PIPE:
          String pipeName = ReadWriteIOUtils.readString(inputStream);
          pipes.get(pipeName).setStatus(PipeStatus.STOP);
          break;
        case START_PIPE:
          pipeName = ReadWriteIOUtils.readString(inputStream);
          pipes.get(pipeName).setStatus(PipeStatus.RUNNING);
          break;
        case DROP_PIPE:
          pipeName = ReadWriteIOUtils.readString(inputStream);
          pipes.remove(pipeName);
          break;
        default:
          throw new UnsupportedOperationException(
              String.format("Can not recognize SyncOperation %s.", operationType.name()));
      }
    }
  }
}
