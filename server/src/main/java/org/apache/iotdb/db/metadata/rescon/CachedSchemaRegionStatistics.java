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

public class CachedSchemaRegionStatistics extends SchemaRegionStatistics {

  private final AtomicLong unpinnedSize = new AtomicLong(0);
  private final AtomicLong pinnedSize = new AtomicLong(0);
  private final AtomicLong unpinnedNum = new AtomicLong(0);
  private final AtomicLong pinnedNum = new AtomicLong(0);
  private final CachedSchemaEngineStatistics cachedEngineStatistics;

  public CachedSchemaRegionStatistics(int schemaRegionId) {
    super(schemaRegionId);
    cachedEngineStatistics = (CachedSchemaEngineStatistics) schemaEngineStatistics;
  }

  @Override
  public CachedSchemaEngineStatistics getSchemaEngineStatistics() {
    return cachedEngineStatistics;
  }

  public void updatePinnedNum(int delta) {
    this.pinnedNum.getAndUpdate(i -> i += delta);
    cachedEngineStatistics.updatePinnedNum(delta);
  }

  public void updateUnpinnedNum(int delta) {
    this.unpinnedNum.getAndUpdate(i -> i += delta);
    cachedEngineStatistics.updateUnpinnedNum(delta);
  }

  public void updatePinnedSize(int delta) {
    this.pinnedSize.getAndUpdate(i -> i += delta);
    cachedEngineStatistics.updatePinnedSize(delta);
  }

  public void updateUnpinnedSize(int delta) {
    this.unpinnedSize.getAndUpdate(i -> i += delta);
    cachedEngineStatistics.updateUnpinnedSize(delta);
  }

  public long getCachedSize() {
    return unpinnedSize.get();
  }

  @Override
  public void clear() {
    super.clear();
    cachedEngineStatistics.updatePinnedNum((int) -pinnedNum.get());
    cachedEngineStatistics.updateUnpinnedNum((int) -unpinnedNum.get());
    cachedEngineStatistics.updatePinnedSize((int) -pinnedSize.get());
    cachedEngineStatistics.updateUnpinnedSize((int) -unpinnedSize.get());
  }
}
