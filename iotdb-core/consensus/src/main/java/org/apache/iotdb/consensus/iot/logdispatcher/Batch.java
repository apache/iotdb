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

package org.apache.iotdb.consensus.iot.logdispatcher;

import org.apache.iotdb.consensus.config.IoTConsensusConfig;
import org.apache.iotdb.consensus.iot.thrift.TLogEntry;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Batch {

  private final IoTConsensusConfig config;

  private long startIndex;
  private long endIndex;

  private final List<TLogEntry> logEntries = new ArrayList<>();

  private long logEntriesNumFromWAL = 0L;

  private long memorySize;
  // indicates whether this batch has been successfully synchronized to another node
  private boolean synced;

  public Batch(IoTConsensusConfig config) {
    this.config = config;
  }

  /*
  Note: this method must be called once after all the `addTLogBatch` functions have been called
   */
  public void buildIndex() {
    if (!logEntries.isEmpty()) {
      this.startIndex = logEntries.get(0).getSearchIndex();
      this.endIndex = logEntries.get(logEntries.size() - 1).getSearchIndex();
    }
  }

  public void addTLogEntry(TLogEntry entry) {
    logEntries.add(entry);
    if (entry.fromWAL) {
      logEntriesNumFromWAL++;
    }
    memorySize += entry.getMemorySize();
  }

  public boolean canAccumulate() {
    // When reading entries from the WAL, the memory size is calculated based on the serialized
    // size, which can be significantly smaller than the actual size.
    // Thus, we add a multiplier to sender's memory size to estimate the receiver's memory cost.
    // The multiplier is calculated based on the receiver's feedback.
    long receiverMemSize = LogDispatcher.getReceiverMemSizeSum().get();
    long senderMemSize = LogDispatcher.getSenderMemSizeSum().get();
    double multiplier = senderMemSize > 0 ? (double) receiverMemSize / senderMemSize : 1.0;
    multiplier = Math.max(multiplier, 1.0);
    return logEntries.size() < config.getReplication().getMaxLogEntriesNumPerBatch()
        && ((long) (memorySize * multiplier)) < config.getReplication().getMaxSizePerBatch();
  }

  public long getStartIndex() {
    return startIndex;
  }

  public long getEndIndex() {
    return endIndex;
  }

  public List<TLogEntry> getLogEntries() {
    return logEntries;
  }

  public boolean isSynced() {
    return synced;
  }

  public void setSynced(boolean synced) {
    this.synced = synced;
  }

  public boolean isEmpty() {
    return logEntries.isEmpty();
  }

  public long getMemorySize() {
    return memorySize;
  }

  public long getLogEntriesNumFromWAL() {
    return logEntriesNumFromWAL;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Batch{")
        .append("startIndex=")
        .append(startIndex)
        .append(", endIndex=")
        .append(endIndex)
        .append(", size=")
        .append(logEntries.size())
        .append(", memorySize=")
        .append(memorySize)
        .append(", logEntriesNumFromWAL=")
        .append(logEntriesNumFromWAL)
        .append(", synced=")
        .append(synced)
        .append(", config=")
        .append(config != null ? config.toString() : "null");

    // 添加 logEntries 的详细信息
    if (!logEntries.isEmpty()) {
      sb.append(", logEntries=[");
      for (int i = 0; i < Math.min(logEntries.size(), 10); i++) {
        TLogEntry entry = logEntries.get(i);
        sb.append("{index=")
            .append(entry.getSearchIndex())
            .append(", fromWAL=")
            .append(entry.fromWAL)
            .append(", memorySize=")
            .append(entry.getMemorySize());

        // 添加 data (byte) 信息
        if (entry.getData() != null && !entry.getData().isEmpty()) {
          sb.append(", data=[");
          for (int j = 0; j < entry.getData().size(); j++) {
            ByteBuffer buffer = entry.getData().get(j);
            if (buffer != null) {
              // 使用 duplicate() 创建副本，避免修改原始 buffer
              ByteBuffer bufferCopy = buffer.duplicate();
              bufferCopy.rewind();
              int dataSize = bufferCopy.remaining();
              // 只打印前256字节，避免输出过长
              int printSize = Math.min(dataSize, 256);
              sb.append("{size=").append(dataSize);
              if (printSize > 0) {
                sb.append(", bytes=[");
                for (int k = 0; k < printSize; k++) {
                  if (k > 0) {
                    sb.append(", ");
                  }
                  sb.append(String.format("0x%02X", bufferCopy.get() & 0xFF));
                }
                if (dataSize > printSize) {
                  sb.append(", ... (").append(dataSize - printSize).append(" more bytes)");
                }
                sb.append("]");
              }
              sb.append("}");
            } else {
              sb.append("{null}");
            }
            if (j < entry.getData().size() - 1) {
              sb.append(", ");
            }
          }
          sb.append("]");
        } else {
          sb.append(", data=[]");
        }

        sb.append("}");
        if (i < Math.min(logEntries.size(), 10) - 1) {
          sb.append(", ");
        }
      }
      if (logEntries.size() > 10) {
        sb.append(", ... (total ").append(logEntries.size()).append(" entries)");
      }
      sb.append("]");
    } else {
      sb.append(", logEntries=[]");
    }

    // 添加堆栈跟踪信息
    sb.append(", stackTrace=[");
    StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
    // 跳过前几个元素（getStackTrace, toString, 调用者）
    int stackStartIndex = 2;
    int maxDepth = Math.min(stackTrace.length, stackStartIndex + 20); // 最多显示20层堆栈
    for (int i = stackStartIndex; i < maxDepth; i++) {
      StackTraceElement element = stackTrace[i];
      sb.append("\n  at ")
          .append(element.getClassName())
          .append(".")
          .append(element.getMethodName())
          .append("(")
          .append(element.getFileName())
          .append(":")
          .append(element.getLineNumber())
          .append(")");
    }
    if (stackTrace.length > maxDepth) {
      sb.append("\n  ... (").append(stackTrace.length - maxDepth).append(" more frames)");
    }
    sb.append("\n]");

    sb.append('}');
    return sb.toString();
  }
}
