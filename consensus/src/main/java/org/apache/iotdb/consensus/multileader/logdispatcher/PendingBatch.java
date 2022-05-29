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

import org.apache.iotdb.consensus.multileader.thrift.TLogBatch;

import java.util.List;

public class PendingBatch {

  private final long startIndex;
  private final long endIndex;
  private final List<TLogBatch> batches;
  // indicates whether this batch has been successfully synchronized to another node
  private boolean synced;

  public PendingBatch(long startIndex, long endIndex, List<TLogBatch> batches) {
    this.startIndex = startIndex;
    this.endIndex = endIndex;
    this.batches = batches;
    this.synced = false;
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

  @Override
  public String toString() {
    return "PendingBatch{"
        + "startIndex="
        + startIndex
        + ", endIndex="
        + endIndex
        + ", size="
        + batches.size()
        + '}';
  }
}
