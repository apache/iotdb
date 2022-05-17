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

/** only used for multi-leader consensus */
public class IndexedConsensusRequest implements IConsensusRequest {

  private final long minSyncIndex;
  private final long currentIndex;
  private final IConsensusRequest request;

  public IndexedConsensusRequest(long minSyncIndex, long currentIndex, IConsensusRequest request) {
    this.minSyncIndex = minSyncIndex;
    this.currentIndex = currentIndex;
    this.request = request;
  }

  @Override
  public void serializeRequest(ByteBuffer buffer) {
    buffer.putLong(minSyncIndex);
    buffer.putLong(currentIndex);
    request.serializeRequest(buffer);
  }

  public IConsensusRequest getRequest() {
    return request;
  }

  public long getMinSyncIndex() {
    return minSyncIndex;
  }

  public long getCurrentIndex() {
    return currentIndex;
  }
}
