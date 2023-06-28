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
import java.util.Objects;

/** only used for iot consensus. */
public class IndexedConsensusRequest implements IConsensusRequest {

  /** we do not need to serialize these two fields as they are useless in other nodes. */
  private final long searchIndex;

  private final long syncIndex;
  private final List<IConsensusRequest> requests;
  private final List<ByteBuffer> serializedRequests;
  private long serializedSize = 0;

  public IndexedConsensusRequest(long searchIndex, List<IConsensusRequest> requests) {
    this.searchIndex = searchIndex;
    this.requests = requests;
    this.syncIndex = -1L;
    this.serializedRequests = new ArrayList<>(requests.size());
  }

  public IndexedConsensusRequest(
      long searchIndex, long syncIndex, List<IConsensusRequest> requests) {
    this.searchIndex = searchIndex;
    this.requests = requests;
    this.syncIndex = syncIndex;
    this.serializedRequests = new ArrayList<>(requests.size());
  }

  public void buildSerializedRequests() {
    this.requests.forEach(
        r -> {
          ByteBuffer buffer = r.serializeToByteBuffer();
          this.serializedRequests.add(buffer);
          this.serializedSize += buffer.capacity();
        });
  }

  @Override
  public ByteBuffer serializeToByteBuffer() {
    throw new UnsupportedOperationException();
  }

  public List<IConsensusRequest> getRequests() {
    return requests;
  }

  public List<ByteBuffer> getSerializedRequests() {
    return serializedRequests;
  }

  public long getSerializedSize() {
    return serializedSize;
  }

  public long getSearchIndex() {
    return searchIndex;
  }

  public long getSyncIndex() {
    return syncIndex;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    IndexedConsensusRequest that = (IndexedConsensusRequest) o;
    return searchIndex == that.searchIndex && requests.equals(that.requests);
  }

  @Override
  public int hashCode() {
    return Objects.hash(searchIndex, requests);
  }
}
