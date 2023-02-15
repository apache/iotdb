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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.mtree.store.disk.memcontrol.IReleaseFlushStrategy;
import org.apache.iotdb.db.metadata.mtree.store.disk.memcontrol.ReleaseFlushStrategyNumBasedImpl;
import org.apache.iotdb.db.metadata.mtree.store.disk.memcontrol.ReleaseFlushStrategySizeBasedImpl;

import java.util.concurrent.atomic.AtomicLong;

public class CachedSchemaEngineStatistics extends SchemaEngineStatistics {

  private final AtomicLong unpinnedSize = new AtomicLong(0);
  private final AtomicLong pinnedSize = new AtomicLong(0);
  private final AtomicLong unpinnedNum = new AtomicLong(0);
  private final AtomicLong pinnedNum = new AtomicLong(0);

  private IReleaseFlushStrategy releaseFlushStrategy;

  @Override
  public void init() {
    super.init();
    unpinnedSize.getAndSet(0);
    pinnedSize.getAndSet(0);
    unpinnedNum.getAndSet(0);
    pinnedNum.getAndSet(0);
    if (IoTDBDescriptor.getInstance().getConfig().getCachedMNodeSizeInSchemaFileMode() >= 0) {
      releaseFlushStrategy = new ReleaseFlushStrategyNumBasedImpl(unpinnedNum, pinnedNum);
    } else {
      releaseFlushStrategy = new ReleaseFlushStrategySizeBasedImpl(memoryUsage);
    }
  }

  public boolean isExceedReleaseThreshold() {
    return releaseFlushStrategy.isExceedReleaseThreshold();
  }

  public boolean isExceedFlushThreshold() {
    return releaseFlushStrategy.isExceedFlushThreshold();
  }

  public void updatePinnedNum(int delta) {
    this.pinnedNum.getAndUpdate(i -> i += delta);
  }

  public void updateUnpinnedNum(int delta) {
    this.unpinnedNum.getAndUpdate(i -> i += delta);
  }

  public void updatePinnedSize(int delta) {
    this.pinnedSize.getAndUpdate(i -> i += delta);
  }

  public void updateUnpinnedSize(int delta) {
    this.unpinnedSize.getAndUpdate(i -> i += delta);
  }

  public long getUnpinnedSize() {
    return unpinnedSize.get();
  }

  public long getPinnedSize() {
    return pinnedSize.get();
  }

  @Override
  public void clear() {
    super.clear();
    unpinnedSize.getAndSet(0);
    pinnedSize.getAndSet(0);
    unpinnedNum.getAndSet(0);
    pinnedNum.getAndSet(0);
  }
}
