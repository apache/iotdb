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

package org.apache.iotdb.consensus.common.request;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DeserializedBatchIndexedConsensusRequest
    implements IConsensusRequest, Comparable<DeserializedBatchIndexedConsensusRequest> {
  private final long startSyncIndex;
  private final long endSyncIndex;
  private final List<IConsensusRequest> insertNodes;

  public DeserializedBatchIndexedConsensusRequest(
      long startSyncIndex, long endSyncIndex, int size) {
    this.startSyncIndex = startSyncIndex;
    this.endSyncIndex = endSyncIndex;
    // use arraylist here because we know the number of requests
    this.insertNodes = new ArrayList<>(size);
  }

  public long getStartSyncIndex() {
    return startSyncIndex;
  }

  public long getEndSyncIndex() {
    return endSyncIndex;
  }

  public List<IConsensusRequest> getInsertNodes() {
    return insertNodes;
  }

  public void add(IConsensusRequest insertNode) {
    this.insertNodes.add(insertNode);
  }

  @Override
  public int compareTo(DeserializedBatchIndexedConsensusRequest o) {
    return Long.compare(startSyncIndex, o.startSyncIndex);
  }

  @Override
  public ByteBuffer serializeToByteBuffer() {
    return null;
  }
}
