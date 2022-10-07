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
import java.util.LinkedList;
import java.util.List;

public class BatchIndexedConsensusRequest implements IConsensusRequest {

  private boolean isStartSyncIndexInitialized;
  private long startSyncIndex;
  private long endSyncIndex;
  private final List<IndexedConsensusRequest> requests;
  private final String sourcePeerId;

  public BatchIndexedConsensusRequest(String sourcePeerId) {
    this.sourcePeerId = sourcePeerId;
    this.requests = new LinkedList<>();
    this.isStartSyncIndexInitialized = false;
  }

  public void add(IndexedConsensusRequest request) {
    if (!isStartSyncIndexInitialized) {
      startSyncIndex = request.getSyncIndex();
      isStartSyncIndexInitialized = true;
    }
    endSyncIndex = request.getSyncIndex();
    this.requests.add(request);
  }

  public long getStartSyncIndex() {
    return startSyncIndex;
  }

  public long getEndSyncIndex() {
    return endSyncIndex;
  }

  public String getSourcePeerId() {
    return sourcePeerId;
  }

  public List<IndexedConsensusRequest> getRequests() {
    return requests;
  }

  @Override
  public ByteBuffer serializeToByteBuffer() {
    return null;
  }
}
