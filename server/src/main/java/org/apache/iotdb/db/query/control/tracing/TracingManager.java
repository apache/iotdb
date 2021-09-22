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
package org.apache.iotdb.db.query.control.tracing;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TracingManager {

  private static final Logger logger = LoggerFactory.getLogger(TracingManager.class);

  private BufferedWriter writer;

  // (statementId -> TracingInfo)
  private final Map<Long, TracingInfo> statementIdToTracingInfo = new ConcurrentHashMap<>();

  public TracingManager(String dirName, String logFileName) {
    initTracingManager(dirName, logFileName);
  }

  public void initTracingManager(String dirName, String logFileName) {
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

  public TracingInfo getTracingInfo(long statementId) {
    return statementIdToTracingInfo.computeIfAbsent(statementId, k -> new TracingInfo());
  }

  public void writeQueryInfo(long statementId, String statement, int pathsNum)
      throws IOException {
    long startTime = statementIdToTracingInfo.get(statementId).getStartTime();
    statementIdToTracingInfo.get(statementId).setSeriesPathNum(pathsNum);
    StringBuilder builder = new StringBuilder();
    builder
        .append(String.format(TracingConstant.STATEMENT_ID, statementId))
        .append(String.format(" - Statement: %s", statement))
        .append("\n")
        .append(String.format(TracingConstant.STATEMENT_ID, statementId))
        .append(" - Start time: ")
        .append(new SimpleDateFormat(TracingConstant.DATE_FORMAT).format(startTime))
        .append("\n")
        .append(String.format(TracingConstant.STATEMENT_ID, statementId))
        .append(String.format(" - Number of series paths: %d", pathsNum))
        .append("\n");
    writer.write(builder.toString());
  }

  // for align by device query
  public void writeQueryInfo(long statementId, String statement)
      throws IOException {
    long startTime = statementIdToTracingInfo.get(statementId).getStartTime();
    StringBuilder builder = new StringBuilder();
    builder
        .append(String.format(TracingConstant.STATEMENT_ID, statementId))
        .append(String.format(" - Statement: %s", statement))
        .append("\n")
        .append(String.format(TracingConstant.STATEMENT_ID, statementId))
        .append(" - Start time: ")
        .append(new SimpleDateFormat(TracingConstant.DATE_FORMAT).format(startTime))
        .append("\n");
    writer.write(builder.toString());
  }

  // for align by device query
  public void writePathsNum(long statementId, int pathsNum) throws IOException {
    statementIdToTracingInfo.get(statementId).setSeriesPathNum(pathsNum);
    StringBuilder builder =
        new StringBuilder(String.format(TracingConstant.STATEMENT_ID, statementId))
            .append(String.format(" - Number of series paths: %d", pathsNum))
            .append("\n");
    writer.write(builder.toString());
  }

  public void writeTracingInfo(long statementId) throws IOException {
    TracingInfo tracingInfo = statementIdToTracingInfo.get(statementId);
    writeTsFileInfo(statementId, tracingInfo.getSeqFileSet(), tracingInfo.getUnSeqFileSet());
    writeSeqChunksInfo(
        statementId, tracingInfo.getSequenceChunkNum(), tracingInfo.getSequenceChunkPoints());
    writeUnSeqChunksInfo(
        statementId, tracingInfo.getUnsequenceChunkNum(), tracingInfo.getUnsequenceChunkPoints());
    writeOverlappedPageInfo(
        statementId, tracingInfo.getTotalPageNum(), tracingInfo.getOverlappedPageNum());
  }

  public void writeTsFileInfo(
      long statementId,
      Set<TsFileResource> seqFileResources,
      Set<TsFileResource> unSeqFileResources)
      throws IOException {
    // to avoid the disorder info of multi query
    // add query id as prefix of each info
    StringBuilder builder =
        new StringBuilder(String.format(TracingConstant.STATEMENT_ID, statementId))
            .append(String.format(" - Number of sequence files read: %d", seqFileResources.size()));
    if (!seqFileResources.isEmpty()) {
      builder.append("\n").append(String.format(TracingConstant.STATEMENT_ID, statementId)).append(" - SeqFiles: ");
      Iterator<TsFileResource> seqFileIterator = seqFileResources.iterator();
      while (seqFileIterator.hasNext()) {
        builder.append(seqFileIterator.next().getTsFile().getName());
        if (seqFileIterator.hasNext()) {
          builder.append(", ");
        }
      }
    }

    builder
        .append("\n")
        .append(String.format(TracingConstant.STATEMENT_ID, statementId))
        .append(String.format(" - Number of unSequence files read: %d", unSeqFileResources.size()));
    if (!unSeqFileResources.isEmpty()) {
      builder
          .append("\n")
          .append(String.format(TracingConstant.STATEMENT_ID, statementId))
          .append(" - UnSeqFiles: ");
      Iterator<TsFileResource> unSeqFileIterator = unSeqFileResources.iterator();
      while (unSeqFileIterator.hasNext()) {
        builder.append(unSeqFileIterator.next().getTsFile().getName());
        if (unSeqFileIterator.hasNext()) {
          builder.append(", ");
        }
      }
    }
    builder.append("\n");
    writer.write(builder.toString());
  }

  public void writeSeqChunksInfo(long statementId, long totalChunkNum, long totalChunkPoints)
      throws IOException {
    double avgChunkPoints = (double) totalChunkPoints / totalChunkNum;
    StringBuilder builder =
        new StringBuilder(String.format(TracingConstant.STATEMENT_ID, statementId))
            .append(String.format(" - Num of sequence chunks: %d", totalChunkNum))
            .append(String.format(", avg points: %.1f", avgChunkPoints))
            .append("\n");
    writer.write(builder.toString());
  }

  public void writeUnSeqChunksInfo(long statementId, long totalChunkNum, long totalChunkPoints)
      throws IOException {
    double avgChunkPoints = (double) totalChunkPoints / totalChunkNum;
    StringBuilder builder =
        new StringBuilder(String.format(TracingConstant.STATEMENT_ID, statementId))
            .append(String.format(" - Num of unsequence chunks: %d", totalChunkNum))
            .append(String.format(", avg points: %.1f", avgChunkPoints))
            .append("\n");
    writer.write(builder.toString());
  }

  public void writeOverlappedPageInfo(long statementId, int totalPageNum, int overlappedPageNum)
      throws IOException {
    StringBuilder builder =
        new StringBuilder(String.format(TracingConstant.STATEMENT_ID, statementId))
            .append(String.format(" - Num of Pages: %d, ", totalPageNum))
            .append(
                String.format(
                    "overlapped pages: %d (%.1f%%)",
                    overlappedPageNum, (double) overlappedPageNum / totalPageNum * 100))
    .append("\n");
    writer.write(builder.toString());
  }

  public void writeEndTime(long statementId, long endTime) throws IOException {
    StringBuilder builder =
        new StringBuilder(String.format(TracingConstant.STATEMENT_ID, statementId))
            .append(" - Total cost time: ")
            .append(endTime - statementIdToTracingInfo.get(statementId).getStartTime())
            .append("ms\n");
    writer.write(builder.toString());
    writer.flush();
  }

  public void close() {
    try {
      writer.close();
    } catch (IOException e) {
      logger.error("Meeting error while Close the tracing log stream : {}", e.getMessage());
    }
  }

  public boolean getWriterStatus() {
    try {
      writer.flush();
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  public void openTracingWriteStream() {
    initTracingManager(
        IoTDBDescriptor.getInstance().getConfig().getTracingDir(), IoTDBConstant.TRACING_LOG);
  }

  private static class TracingManagerHelper {

    private static final TracingManager INSTANCE =
        new TracingManager(
            IoTDBDescriptor.getInstance().getConfig().getTracingDir(), IoTDBConstant.TRACING_LOG);

    private TracingManagerHelper() {}
  }
}
