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
package org.apache.iotdb.db.metadata.rescon;

import java.util.concurrent.atomic.AtomicLong;

/**
 * This class is used to record global statistics for SchemaEngine in Schema_File mode, which is a
 * superset of the statistics in Memory mode
 */
public class CachedSchemaEngineStatistics extends MemSchemaEngineStatistics {

  private final AtomicLong unpinnedMemorySize = new AtomicLong(0);
  private final AtomicLong pinnedMemorySize = new AtomicLong(0);
  private final AtomicLong unpinnedMNodeNum = new AtomicLong(0);
  private final AtomicLong pinnedMNodeNum = new AtomicLong(0);

  public void updatePinnedMNodeNum(long delta) {
    this.pinnedMNodeNum.addAndGet(delta);
  }

  public void updateUnpinnedMNodeNum(long delta) {
    this.unpinnedMNodeNum.addAndGet(delta);
  }

  public void updatePinnedMemorySize(long delta) {
    this.pinnedMemorySize.addAndGet(delta);
  }

  public void updateUnpinnedMemorySize(long delta) {
    this.unpinnedMemorySize.addAndGet(delta);
  }

  public long getUnpinnedMemorySize() {
    return unpinnedMemorySize.get();
  }

  public long getPinnedMemorySize() {
    return pinnedMemorySize.get();
  }

  public long getUnpinnedMNodeNum() {
    return unpinnedMNodeNum.get();
  }

  public long getPinnedMNodeNum() {
    return pinnedMNodeNum.get();
  }

  @Override
  public MemSchemaEngineStatistics getAsMemSchemaEngineStatistics() {
    return this;
  }

  @Override
  public CachedSchemaEngineStatistics getAsCachedSchemaEngineStatistics() {
    return this;
  }
}
