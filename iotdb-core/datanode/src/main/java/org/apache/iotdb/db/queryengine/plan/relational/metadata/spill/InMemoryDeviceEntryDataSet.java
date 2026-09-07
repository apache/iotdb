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

package org.apache.iotdb.db.queryengine.plan.relational.metadata.spill;

import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public final class InMemoryDeviceEntryDataSet implements DeviceEntryDataSet {

  private final List<DeviceEntry> entries;
  private final Set<TSeriesPartitionSlot> seriesPartitionSlots;

  public InMemoryDeviceEntryDataSet(List<DeviceEntry> entries) {
    this(entries, Set.of());
  }

  public InMemoryDeviceEntryDataSet(
      List<DeviceEntry> entries, Set<TSeriesPartitionSlot> seriesPartitionSlots) {
    this.entries = Collections.unmodifiableList(entries);
    this.seriesPartitionSlots = Set.copyOf(seriesPartitionSlots);
  }

  @Override
  public int getEntryCount() {
    return entries.size();
  }

  @Override
  public boolean isSpilled() {
    return false;
  }

  @Override
  public Set<TSeriesPartitionSlot> getSeriesPartitionSlots() {
    return seriesPartitionSlots;
  }

  @Override
  public DeviceEntryReader openReader() {
    Iterator<DeviceEntry> iterator = entries.iterator();
    return new DeviceEntryReader() {
      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public DeviceEntry next() {
        return iterator.next();
      }

      @Override
      public void close() {
        // No resource to release.
      }
    };
  }

  @Override
  public List<DeviceEntry> getInlineEntries() {
    return entries;
  }

  @Override
  public void close() {
    // The query context owns memory accounting for inline entries.
  }
}
