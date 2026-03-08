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

package org.apache.iotdb.consensus.traft;

import java.util.Arrays;

public class TRaftLogEntry {

  private final long timestamp;
  private final long partitionIndex;
  private final long logIndex;
  private final long logTerm;
  private final long interPartitionIndex;
  private final long lastPartitionCount;
  private final byte[] data;

  public TRaftLogEntry(
      long timestamp,
      long partitionIndex,
      long logIndex,
      long logTerm,
      long interPartitionIndex,
      long lastPartitionCount,
      byte[] data) {
    this.timestamp = timestamp;
    this.partitionIndex = partitionIndex;
    this.logIndex = logIndex;
    this.logTerm = logTerm;
    this.interPartitionIndex = interPartitionIndex;
    this.lastPartitionCount = lastPartitionCount;
    this.data = data;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public long getPartitionIndex() {
    return partitionIndex;
  }

  public long getLogIndex() {
    return logIndex;
  }

  public long getLogTerm() {
    return logTerm;
  }

  public long getInterPartitionIndex() {
    return interPartitionIndex;
  }

  public long getLastPartitionCount() {
    return lastPartitionCount;
  }

  public byte[] getData() {
    return data;
  }

  public TRaftLogEntry copy() {
    return new TRaftLogEntry(
        timestamp,
        partitionIndex,
        logIndex,
        logTerm,
        interPartitionIndex,
        lastPartitionCount,
        Arrays.copyOf(data, data.length));
  }
}
