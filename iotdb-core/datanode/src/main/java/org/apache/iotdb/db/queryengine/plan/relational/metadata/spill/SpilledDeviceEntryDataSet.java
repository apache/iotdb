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

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

public final class SpilledDeviceEntryDataSet implements DeviceEntryDataSet {

  private final String queryId;
  private final Path ownerDirectory;
  private final List<Path> segments;
  private final int entryCount;
  private final DeviceEntryIOContext ioContext;
  private final Set<TSeriesPartitionSlot> seriesPartitionSlots;

  public SpilledDeviceEntryDataSet(
      String queryId,
      Path ownerDirectory,
      List<Path> segments,
      int entryCount,
      DeviceEntryIOContext ioContext,
      Set<TSeriesPartitionSlot> seriesPartitionSlots) {
    this.queryId = queryId;
    this.ownerDirectory = ownerDirectory;
    this.segments = segments;
    this.entryCount = entryCount;
    this.ioContext = ioContext;
    this.seriesPartitionSlots = Set.copyOf(seriesPartitionSlots);
  }

  public SpilledDeviceEntryDataSet(
      String queryId,
      Path ownerDirectory,
      List<Path> segments,
      int entryCount,
      DeviceEntryIOContext ioContext) {
    this(queryId, ownerDirectory, segments, entryCount, ioContext, Set.of());
  }

  @Override
  public int getEntryCount() {
    return entryCount;
  }

  @Override
  public boolean isSpilled() {
    return true;
  }

  @Override
  public Set<TSeriesPartitionSlot> getSeriesPartitionSlots() {
    return seriesPartitionSlots;
  }

  public List<Path> getSegments() {
    return segments;
  }

  @Override
  public DeviceEntryReader openReader() {
    return new DeviceEntryFileSpillerReader(segments, false, ioContext, false);
  }

  @Override
  public DeviceEntryReader openConsumingReader() {
    return new DeviceEntryFileSpillerReader(segments, true, ioContext, false);
  }

  @Override
  public void close() throws IOException {
    DeviceEntrySpillManager.getInstance().deregisterOwner(queryId, ownerDirectory);
  }
}
