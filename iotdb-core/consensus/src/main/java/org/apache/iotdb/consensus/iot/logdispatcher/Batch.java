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

import java.nio.Buffer;
import java.util.ArrayList;
import java.util.List;

public class Batch {

  private final IoTConsensusConfig config;

  private long startIndex;
  private long endIndex;

  private final List<TLogEntry> logEntries = new ArrayList<>();

  private long logEntriesNumFromWAL = 0L;

  private long serializedSize;
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
    // TODO Maybe we need to add in additional fields for more accurate calculations
    serializedSize +=
        entry.getData() == null ? 0 : entry.getData().stream().mapToInt(Buffer::capacity).sum();
  }

  public boolean canAccumulate() {
    return logEntries.size() < config.getReplication().getMaxLogEntriesNumPerBatch()
        && serializedSize < config.getReplication().getMaxSizePerBatch();
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

  public long getSerializedSize() {
    return serializedSize;
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
        + ", serializedSize="
        + serializedSize
        + '}';
  }
}
