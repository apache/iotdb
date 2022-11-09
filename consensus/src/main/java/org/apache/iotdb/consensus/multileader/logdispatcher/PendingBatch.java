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

package org.apache.iotdb.consensus.multileader.logdispatcher;

import org.apache.iotdb.consensus.config.MultiLeaderConfig;
import org.apache.iotdb.consensus.multileader.thrift.TLogBatch;

import java.util.ArrayList;
import java.util.List;

public class PendingBatch {

  private final MultiLeaderConfig config;

  private long startIndex;
  private long endIndex;

  private final List<TLogBatch> batches = new ArrayList<>();

  private long serializedSize;
  // indicates whether this batch has been successfully synchronized to another node
  private boolean synced;

  public PendingBatch(MultiLeaderConfig config) {
    this.config = config;
  }

  /*
  Note: this method must be called once after all the `addTLogBatch` functions have been called
   */
  public void buildIndex() {
    if (!batches.isEmpty()) {
      this.startIndex = batches.get(0).getSearchIndex();
      this.endIndex = batches.get(batches.size() - 1).getSearchIndex();
    }
  }

  public void addTLogBatch(TLogBatch batch) {
    batches.add(batch);
    // TODO Maybe we need to add in additional fields for more accurate calculations
    serializedSize += batch.getData() == null ? 0 : batch.getData().length;
  }

  public boolean canAccumulate() {
    return batches.size() < config.getReplication().getMaxRequestNumPerBatch()
        && serializedSize < config.getReplication().getMaxSizePerBatch();
  }

  public long getStartIndex() {
    return startIndex;
  }

  public long getEndIndex() {
    return endIndex;
  }

  public List<TLogBatch> getBatches() {
    return batches;
  }

  public boolean isSynced() {
    return synced;
  }

  public void setSynced(boolean synced) {
    this.synced = synced;
  }

  public boolean isEmpty() {
    return batches.isEmpty();
  }

  public long getSerializedSize() {
    return serializedSize;
  }

  @Override
  public String toString() {
    return "PendingBatch{"
        + "startIndex="
        + startIndex
        + ", endIndex="
        + endIndex
        + ", size="
        + batches.size()
        + ", serializedSize="
        + serializedSize
        + '}';
  }
}
