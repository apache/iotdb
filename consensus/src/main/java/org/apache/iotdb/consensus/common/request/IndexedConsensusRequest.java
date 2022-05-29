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

import org.apache.iotdb.consensus.multileader.thrift.TLogType;

import java.nio.ByteBuffer;
import java.util.Objects;

/** only used for multi-leader consensus. */
public class IndexedConsensusRequest implements IConsensusRequest {

  /** we do not need to serialize these two fields as they are useless in other nodes. */
  private final long searchIndex;

  private final long safelyDeletedSearchIndex;

  /** we do not need to serialize this field as it will be serialized by TLogBatch. */
  private final TLogType type;

  private final IConsensusRequest request;

  public IndexedConsensusRequest(
      long searchIndex, long safelyDeletedSearchIndex, TLogType type, IConsensusRequest request) {
    this.searchIndex = searchIndex;
    this.safelyDeletedSearchIndex = safelyDeletedSearchIndex;
    this.type = type;
    this.request = request;
  }

  @Override
  public void serializeRequest(ByteBuffer buffer) {
    request.serializeRequest(buffer);
  }

  public IConsensusRequest getRequest() {
    return request;
  }

  public long getSearchIndex() {
    return searchIndex;
  }

  public long getSafelyDeletedSearchIndex() {
    return safelyDeletedSearchIndex;
  }

  public TLogType getType() {
    return type;
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
    return searchIndex == that.searchIndex
        && safelyDeletedSearchIndex == that.safelyDeletedSearchIndex
        && type == that.type
        && Objects.equals(request, that.request);
  }

  @Override
  public int hashCode() {
    return Objects.hash(searchIndex, safelyDeletedSearchIndex, type, request);
  }
}
