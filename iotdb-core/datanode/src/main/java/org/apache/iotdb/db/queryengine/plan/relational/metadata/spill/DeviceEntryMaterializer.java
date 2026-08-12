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

  private final boolean rawSegment;
  private DeviceEntryDiskSpiller spiller;
  // Only be used in fetchDeviceSchema, manages memory itself
  private long rawBufferedRamBytes;

  public DeviceEntryMaterializer(
      String queryId, PlanNodeId planNodeId, long thresholdInBytes, boolean rawSegment) {
    super(queryId, planNodeId, thresholdInBytes);
    this.rawSegment = rawSegment;
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
  public long appendWithMemoryControl(DeviceEntry entry) throws IOException {
    checkNotFinished();
    long ramBytesUsed = entry.ramBytesUsed();
    if (spiller == null && rawBufferedRamBytes + ramBytesUsed <= thresholdInBytes()) {
      appendToBuffer(entry);
      rawBufferedRamBytes += ramBytesUsed;
      return 0;
    }
    long releasedRamBytes = rawBufferedRamBytes;
    ensureSpiller();
    rawBufferedRamBytes = 0;
    spiller.append(entry.serializeToBytes());
    incrementEntryCount();
    return releasedRamBytes;
  }

  @Override
  public void forceSpill() throws IOException {
    checkNotFinished();
    if (spiller == null && !isBufferEmpty()) {
      ensureSpiller();
    }
    rawBufferedRamBytes = 0;
  }

  public boolean isSpilled() {
    return spiller != null;
  }

  @Override
  public DeviceEntryDataSet finish() throws IOException {
    checkNotFinished();
    DeviceEntryDataSet dataSet;
    if (spiller == null) {
      dataSet = new InMemoryDeviceEntryDataSet(copyBufferedEntries());
    } else {
      dataSet =
          new SpilledDeviceEntryDataSet(
              queryId(), ownerDirectory(), spiller.finish(), entryCount(), true);
    }
    if (rawSegment && getQueryContext() != null) {
      getQueryContext().recordDeviceEntryCount(entryCount());
    }
    markFinished();
    return dataSet;
  }

  private void ensureSpiller() throws IOException {
    if (spiller != null) {
      return;
    }
    Path ownerDirectory = ensureOwnerDirectory();
    if (rawSegment) {
      createIOContextOnSpill(true);
    }
    spiller =
        new DeviceEntryDiskSpiller(
            ownerDirectory.resolve(rawSegment ? "raw" : "fi"), thresholdInBytes(), ioContext());
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
