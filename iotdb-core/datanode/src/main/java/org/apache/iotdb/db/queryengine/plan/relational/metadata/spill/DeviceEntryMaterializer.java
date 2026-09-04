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

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;

import java.io.IOException;
import java.nio.file.Path;

public final class DeviceEntryMaterializer extends AbstractDeviceEntryMaterializer {

  private DeviceEntryDiskSpiller spiller;

  public DeviceEntryMaterializer(
      String queryId, PlanNodeId planNodeId, long thresholdInBytes, boolean rawSegment) {
    super(queryId, planNodeId, thresholdInBytes, rawSegment);
  }

  public DeviceEntryMaterializer(
      String queryId,
      PlanNodeId planNodeId,
      long thresholdInBytes,
      boolean rawSegment,
      MPPQueryContext queryContext) {
    this(queryId, planNodeId, thresholdInBytes, rawSegment);
    setQueryContext(queryContext);
  }

  @Override
  public void append(DeviceEntry entry) throws IOException {
    checkNotFinished();
    appendToBuffer(entry);
  }

  /** Returns the RAM bytes released when Coordinator Raw Fetch switches to spill mode. */
  @Override
  public long appendWithMemoryControl(DeviceEntry entry) throws IOException {
    checkNotFinished();
    if (spiller == null) {
      long ramBytesUsed = entry.ramBytesUsed();
      if (getBufferedRamBytes() + ramBytesUsed <= thresholdInBytes()) {
        appendToBuffer(entry);
        addBufferedRamBytes(ramBytesUsed);
        return 0;
      }
    }
    long releasedRamBytes = getBufferedRamBytes() + entry.ramBytesUsed();
    ensureSpiller(false);
    spiller.append(entry.serializeToBytes());
    incrementEntryCount();
    return releasedRamBytes;
  }

  @Override
  public void forceSpill() throws IOException {
    checkNotFinished();
    ensureSpiller(true);
  }

  @Override
  public boolean isSpilled() {
    return spiller != null;
  }

  @Override
  public DeviceEntryDataSet finish() throws IOException {
    checkNotFinished();
    DeviceEntryDataSet dataSet;
    if (spiller == null) {
      dataSet = new InMemoryDeviceEntryDataSet(copyBufferedEntries(), seriesPartitionSlots());
    } else {
      ensureSpiller(true);
      dataSet =
          new SpilledDeviceEntryDataSet(
              queryId(),
              ownerDirectory(),
              spiller.finish(),
              entryCount(),
              ioContextOnSpill(),
              seriesPartitionSlots());
    }
    recordDeviceEntryCount();
    markFinished();
    return dataSet;
  }

  private void ensureSpiller(boolean skipIfBufferEmpty) throws IOException {
    if (isBufferEmpty() && skipIfBufferEmpty) {
      return;
    }

    if (spiller == null) {
      Path ownerDirectory = ensureOwnerDirectory();
      spiller = createSpiller(spillDirectory(ownerDirectory));
    }

    for (DeviceEntry entry : bufferedEntries()) {
      spiller.append(entry.serializeToBytes());
    }
    clearBuffer();
  }

  @Override
  public void close() throws IOException {
    try {
      if (spiller != null) {
        spiller.close();
      }
    } finally {
      super.close();
    }
  }
}
