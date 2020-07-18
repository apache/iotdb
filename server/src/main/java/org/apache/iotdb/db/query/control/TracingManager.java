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

  public TracingManager(String dirName, String logFileName) {
    File tracingDir = SystemFileFactory.INSTANCE.getFile(dirName);
    if (!tracingDir.exists()) {
      if (tracingDir.mkdirs()) {
        logger.info("create performance folder {}.", tracingDir);
      } else {
        logger.info("create performance folder {} failed.", tracingDir);
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

  public void writeQueryInfo(long queryId, String statement, int pathsNum) throws IOException {
    StringBuilder builder = new StringBuilder();
    builder.append("Query Id: ").append(queryId)
        .append(" - Query Statement: ").append(statement)
        .append("\nQuery Id: ").append(queryId)
        .append(" - Start time: ")
        .append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(System.currentTimeMillis()))
        .append("\nQuery Id: ").append(queryId)
        .append(" - Number of series paths: ").append(pathsNum)
        .append("\n");
    writer.write(builder.toString());
  }

  // for align by device query
  public void writeQueryInfo(long queryId, String statement) throws IOException {
    StringBuilder builder = new StringBuilder();
    builder.append("Query Id: ").append(queryId)
        .append(" - Query Statement: ").append(statement)
        .append("\nQuery Id: ").append(queryId)
        .append(" - Start time: ")
        .append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(System.currentTimeMillis()))
        .append("\n");
    writer.write(builder.toString());
  }

  public void writePathsNum(long queryId, int pathsNum) throws IOException {
    StringBuilder builder = new StringBuilder("Query Id: ").append(queryId)
        .append(" - Number of series paths: ").append(pathsNum)
        .append("\n");
    writer.write(builder.toString());
  }

  public void writeTsFileInfo(long queryId, int seqFileNum, int unseqFileNum) throws IOException {
    // to avoid the disorder info of multi query
    // add query id as prefix of each info
    StringBuilder builder = new StringBuilder("Query Id: ").append(queryId)
        .append(" - Number of tsfiles: ").append(seqFileNum + unseqFileNum)
        .append("\nQuery Id: ").append(queryId)
        .append(" - Number of sequence files: ").append(seqFileNum)
        .append("\nQuery Id: ").append(queryId)
        .append(" - Number of unsequence files: ").append(unseqFileNum)
        .append("\n");
    writer.write(builder.toString());
  }

  public void writeChunksInfo(long queryId, long totalChunkNum, long totalChunkSize)
      throws IOException {
    StringBuilder builder = new StringBuilder("Query Id: ").append(queryId)
        .append(" - Number of chunks: ").append(totalChunkNum)
        .append("\nQuery Id: ").append(queryId)
        .append(" - Average size of chunks: ").append(totalChunkSize / totalChunkNum)
        .append("\n");
    writer.write(builder.toString());
  }

  public void writeEndTime(long queryId) throws IOException {
    StringBuilder builder = new StringBuilder("Query Id: ").append(queryId)
        .append(" - End time: ")
        .append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(System.currentTimeMillis()))
        .append("\n");
    writer.write(builder.toString());
    writer.flush();
  }

  public void close() throws IOException {
    writer.close();
  }

  private static class TracingManagerHelper {

    private static final TracingManager INSTANCE = new TracingManager(
        IoTDBDescriptor.getInstance().getConfig().getTracingDir(),
        IoTDBConstant.TRACING_LOG);

    private TracingManagerHelper() {
    }
  }
}
