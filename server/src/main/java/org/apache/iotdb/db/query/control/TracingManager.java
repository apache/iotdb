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
package org.apache.iotdb.db.query.control;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TracingManager {

  private static final Logger logger = LoggerFactory.getLogger(TracingManager.class);
  private BufferedWriter writer;

  public TracingManager(String dirName, String logFileName){
    File performanceDir = SystemFileFactory.INSTANCE.getFile(dirName);
    if (!performanceDir.exists()) {
      if (performanceDir.mkdirs()) {
        logger.info("create performance folder {}.", performanceDir);
      } else {
        logger.info("create performance folder {} failed.", performanceDir);
      }
    }
    File logFile = SystemFileFactory.INSTANCE.getFile(dirName + File.separator + logFileName);

    FileWriter fileWriter = null;
    try {
      fileWriter = new FileWriter(logFile, true);
    } catch (IOException e) {
      logger.error("Meeting error while creating TracingManager: {}", e);
    }
    writer = new BufferedWriter(fileWriter);
  }

  public static TracingManager getInstance() {
    return TracingManagerHelper.INSTANCE;
  }

  public void writeSeperator() throws IOException {
    writer.write("-----------------------------");
    writer.newLine();
    writer.flush();
  }

  public void writeStatement(String statement) throws IOException {
    writer.write(new StringBuilder("Query Statement: ").append(statement).toString());
    writer.newLine();
    writer.flush();
  }

  public void writeStartTime() throws IOException {
    writer.write(new StringBuilder("Start time: ").append(
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(System.currentTimeMillis()))
        .toString());
    writer.newLine();
    writer.flush();
  }

  public void writePathsNum(int pathsNum) throws IOException {
    writer.write(new StringBuilder("Number of series paths: ").append(pathsNum).toString());
    writer.newLine();
    writer.flush();
  }

  public void writeQueryId(Long queryId) throws IOException {
    writer.write(new StringBuilder("Query Id: ").append(queryId).toString());
    writer.newLine();
    writer.flush();
  }

  public void writeTsFileInfo(int seqFileNum, int unseqFileNum) throws IOException {
    writer.write(new StringBuilder("Number of tsfiles: ").append(seqFileNum + unseqFileNum)
        .append("\nNumber of sequence files: ").append(seqFileNum)
        .append("\nNumber of unsequence files: ").append(unseqFileNum).toString());
    writer.newLine();
    writer.flush();
  }

  public void writeChunksInfo(long totalChunkNum, long totalChunkSize) throws IOException {
    writer.write(new StringBuilder("Number of chunks: ").append(totalChunkNum)
        .append("\nAverage size of chunks: ").append(totalChunkSize / totalChunkNum).toString());
    writer.newLine();
    writer.flush();
  }

  private static class TracingManagerHelper {

    private static final TracingManager INSTANCE = new TracingManager(
        IoTDBDescriptor.getInstance().getConfig().getPerformanceDir(),
        IoTDBConstant.PERFORMANCE_LOG);

    private TracingManagerHelper() {
    }
  }
}
