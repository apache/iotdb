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

package org.apache.iotdb.db.schemaengine.rescon;

import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memory.IMemoryManager;

import java.util.concurrent.atomic.AtomicLong;

/**
 * This class is used to record statistics within a SchemaRegion in PBTree mode, which is a superset
 * of the statistics in Memory mode.
 */
public class CachedSchemaRegionStatistics extends MemSchemaRegionStatistics {

  private final AtomicLong unpinnedMemorySize = new AtomicLong(0);
  private final AtomicLong pinnedMemorySize = new AtomicLong(0);
  private final AtomicLong unpinnedMNodeNum = new AtomicLong(0);
  private final AtomicLong pinnedMNodeNum = new AtomicLong(0);
  private final AtomicLong volatileMNodeNum = new AtomicLong(0);

  private long mlogCheckPoint = 0;

  private IMemoryManager memoryManager;

  private final CachedSchemaEngineStatistics cachedEngineStatistics;

  public CachedSchemaRegionStatistics(
      int schemaRegionId, ISchemaEngineStatistics engineStatistics) {
    super(schemaRegionId, engineStatistics);
    cachedEngineStatistics = schemaEngineStatistics.getAsCachedSchemaEngineStatistics();
  }

  public void updatePinnedMNodeNum(int delta) {
    this.pinnedMNodeNum.addAndGet(delta);
    cachedEngineStatistics.updatePinnedMNodeNum(delta);
  }

  public void updateUnpinnedMNodeNum(int delta) {
    this.unpinnedMNodeNum.addAndGet(delta);
    cachedEngineStatistics.updateUnpinnedMNodeNum(delta);
  }

  public void updatePinnedMemorySize(int delta) {
    this.pinnedMemorySize.addAndGet(delta);
    cachedEngineStatistics.updatePinnedMemorySize(delta);
  }

  public void updateUnpinnedMemorySize(int delta) {
    this.unpinnedMemorySize.addAndGet(delta);
    cachedEngineStatistics.updateUnpinnedMemorySize(delta);
  }

  public void updateVolatileMNodeNum(int delta) {
    this.volatileMNodeNum.addAndGet(delta);
  }

  public void setMlogCheckPoint(long mlogCheckPoint) {
    this.mlogCheckPoint = mlogCheckPoint;
  }

  public void setMemoryManager(IMemoryManager memoryManager) {
    this.memoryManager = memoryManager;
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

  public long getMlogCheckPoint() {
    return mlogCheckPoint;
  }

  public long getVolatileMNodeNum() {
    return volatileMNodeNum.get();
  }

  public long getCacheNodeNum() {
    return memoryManager == null ? 0 : memoryManager.getCacheNodeNum();
  }

  @Override
  public CachedSchemaRegionStatistics getAsCachedSchemaRegionStatistics() {
    return this;
  }

  @Override
  public MemSchemaRegionStatistics getAsMemSchemaRegionStatistics() {
    return this;
  }

  @Override
  public void clear() {
    super.clear();
    cachedEngineStatistics.updatePinnedMNodeNum(-pinnedMNodeNum.get());
    cachedEngineStatistics.updateUnpinnedMNodeNum(-unpinnedMNodeNum.get());
    cachedEngineStatistics.updatePinnedMemorySize(-pinnedMemorySize.get());
    cachedEngineStatistics.updateUnpinnedMemorySize(-unpinnedMemorySize.get());
  }
}
