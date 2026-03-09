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
    return "Batch{"
        + "startIndex="
        + startIndex
        + ", endIndex="
        + endIndex
        + ", size="
        + logEntries.size()
        + ", memorySize="
        + memorySize
        + '}';
  }
}
