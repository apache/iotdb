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
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TracingManager {

  private static final Logger logger = LoggerFactory.getLogger(TracingManager.class);
  private static final String QUERY_ID = "Query Id: ";
  private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
  private BufferedWriter writer;
  private Map<Long, Long> queryStartTime = new ConcurrentHashMap<>();

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
      logger.error("Meeting error while creating TracingManager: {}", e.getMessage());
    }
    writer = new BufferedWriter(fileWriter);
  }

  public static TracingManager getInstance() {
    return TracingManagerHelper.INSTANCE;
  }

  public void writeQueryInfo(long queryId, String statement, long startTime, int pathsNum)
      throws IOException {
    queryStartTime.put(queryId, startTime);
    StringBuilder builder = new StringBuilder();
    builder.append(QUERY_ID).append(queryId)
        .append(" - Query Statement: ").append(statement)
        .append("\n" + QUERY_ID).append(queryId)
        .append(" - Start time: ").append(new SimpleDateFormat(DATE_FORMAT).format(startTime))
        .append("\n" + QUERY_ID).append(queryId)
        .append(" - Number of series paths: ").append(pathsNum)
        .append("\n");
    writer.write(builder.toString());
  }

  // for align by device query
  public void writeQueryInfo(long queryId, String statement, long startTime) throws IOException {
    queryStartTime.put(queryId, startTime);
    StringBuilder builder = new StringBuilder();
    builder.append(QUERY_ID).append(queryId)
        .append(" - Query Statement: ").append(statement)
        .append("\n" + QUERY_ID).append(queryId)
        .append(" - Start time: ").append(new SimpleDateFormat(DATE_FORMAT).format(startTime))
        .append("\n");
    writer.write(builder.toString());
  }

  public void writePathsNum(long queryId, int pathsNum) throws IOException {
    StringBuilder builder = new StringBuilder(QUERY_ID).append(queryId)
        .append(" - Number of series paths: ").append(pathsNum)
        .append("\n");
    writer.write(builder.toString());
  }

  public void writeTsFileInfo(long queryId, Set<TsFileResource> seqFileResources,
      Set<TsFileResource> unSeqFileResources) throws IOException {
    // to avoid the disorder info of multi query
    // add query id as prefix of each info
    StringBuilder builder = new StringBuilder(QUERY_ID).append(queryId)
        .append(" - Number of sequence files: ").append(seqFileResources.size());
    for (TsFileResource seqFileResource : seqFileResources) {
      builder.append("\n" + QUERY_ID).append(queryId)
          .append(" - SeqFile_").append(seqFileResource.getTsFile().getName());
      printTsFileStatistics(builder, seqFileResource);
    }

    builder.append("\n" + QUERY_ID).append(queryId)
        .append(" - Number of unSequence files: ").append(unSeqFileResources.size());
    for (TsFileResource unSeqFileResource : unSeqFileResources) {
      builder.append("\n" + QUERY_ID).append(queryId)
          .append(" - UnSeqFile_").append(unSeqFileResource.getTsFile().getName());
      printTsFileStatistics(builder, unSeqFileResource);
    }
    builder.append("\n");
    writer.write(builder.toString());
  }

  // print startTime and endTime of each device, format e.g.: device1[1, 10000]
  private void printTsFileStatistics(StringBuilder builder, TsFileResource tsFileResource) {
    Iterator<String> deviceIter = tsFileResource.getDevices().iterator();
    while (deviceIter.hasNext()) {
      String device = deviceIter.next();
      builder.append(" ").append(device)
          .append("[").append(tsFileResource.getStartTime(device))
          .append(", ").append(tsFileResource.getEndTime(device)).append("]");
      if (deviceIter.hasNext()) {
        builder.append(",");
      }
    }
  }

  public void writeChunksInfo(long queryId, long totalChunkNum, long totalChunkSize)
      throws IOException {
    StringBuilder builder = new StringBuilder(QUERY_ID).append(queryId)
        .append(" - Number of chunks: ").append(totalChunkNum)
        .append("\n" + QUERY_ID).append(queryId)
        .append(" - Average size of chunks: ").append(totalChunkSize / totalChunkNum)
        .append("\n");
    writer.write(builder.toString());
  }

  public void writeEndTime(long queryId) throws IOException {
    long endTime = System.currentTimeMillis();
    StringBuilder builder = new StringBuilder(QUERY_ID).append(queryId)
        .append(" - Total cost time: ").append(endTime - queryStartTime.remove(queryId))
        .append("ms\n");
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
