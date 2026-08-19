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

import org.apache.tsfile.external.commons.io.FileUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public abstract class AbstractDeviceEntryMaterializer implements AutoCloseable {

  private final String queryId;
  private final PlanNodeId planNodeId;
  private final long thresholdInBytes;
  private List<DeviceEntry> bufferedEntries = new ArrayList<>();

  private int entryCount;
  private long bufferedRamBytes;
  private Path ownerDirectory;
  private boolean ownerRegistered;
  private boolean finished;
  private DeviceEntryIOContext ioContext;
  private MPPQueryContext queryContext;

  protected AbstractDeviceEntryMaterializer(
      String queryId, PlanNodeId planNodeId, long thresholdInBytes) {
    if (thresholdInBytes <= 0) {
      throw new IllegalArgumentException();
    }
    this.queryId = queryId;
    this.planNodeId = planNodeId;
    this.thresholdInBytes = thresholdInBytes;
  }

  /**
   * Appends a DeviceEntry to this materializer's in-memory buffer. Spill decisions are external.
   */
  public abstract void append(DeviceEntry entry) throws IOException;

  /**
   * Appends a DeviceEntry while controlling this materializer's in-memory buffer.
   *
   * @return RAM bytes released when buffered entries are spilled
   */
  public abstract long appendWithMemoryControl(DeviceEntry entry) throws IOException;

  public abstract boolean isSpilled();

  public final long getBufferedRamBytes() {
    return bufferedRamBytes;
  }

  public abstract DeviceEntryDataSet finish() throws IOException;

  protected final String queryId() {
    return queryId;
  }

  protected final long thresholdInBytes() {
    return thresholdInBytes;
  }

  protected final void appendToBuffer(DeviceEntry entry) {
    bufferedEntries.add(entry);
    entryCount++;
  }

  protected final void addBufferedRamBytes(long ramBytes) {
    bufferedRamBytes += ramBytes;
  }

  protected final void clearBufferedRamBytes() {
    bufferedRamBytes = 0;
  }

  protected final void incrementEntryCount() {
    entryCount++;
  }

  protected final Iterable<DeviceEntry> bufferedEntries() {
    return bufferedEntries;
  }

  protected final boolean isBufferEmpty() {
    return bufferedEntries.isEmpty();
  }

  protected final List<DeviceEntry> copyBufferedEntries() {
    return new ArrayList<>(bufferedEntries);
  }

  protected final void replaceBufferedEntries(List<DeviceEntry> entries) {
    bufferedEntries = entries;
    entryCount = entries.size();
  }

  protected final void sortBufferedEntries(Comparator<DeviceEntry> comparator) {
    bufferedEntries.sort(comparator);
  }

  public abstract void forceSpill() throws IOException;

  protected final void setQueryContext(MPPQueryContext queryContext) {
    this.queryContext = queryContext;
  }

  protected final DeviceEntryIOContext ioContext() {
    return ioContext;
  }

  protected final DeviceEntryIOContext createIOContextOnSpill(boolean duringFetchSchema) {
    if (ioContext == null && queryContext != null) {
      ioContext = queryContext.getOrCreateDeviceEntryIOContext(duringFetchSchema);
    }
    return ioContext;
  }

  protected final int entryCount() {
    return entryCount;
  }

  protected final void setEntryCount(int entryCount) {
    this.entryCount = entryCount;
  }

  protected final void clearBuffer() {
    bufferedEntries.clear();
  }

  protected final Path ownerDirectory() {
    return ownerDirectory;
  }

  protected final Path ensureOwnerDirectory() throws IOException {
    if (ownerDirectory == null) {
      ownerDirectory = DeviceEntrySpillManager.getInstance().register(queryId, planNodeId);
      ownerRegistered = true;
    }
    return ownerDirectory;
  }

  protected final void checkNotFinished() {
    if (finished) {
      throw new IllegalStateException();
    }
  }

  public MPPQueryContext getQueryContext() {
    return queryContext;
  }

  protected final void markFinished() {
    finished = true;
  }

  protected final void cleanupOwnerDirectory() throws IOException {
    if (ownerDirectory != null) {
      if (ownerRegistered) {
        DeviceEntrySpillManager.getInstance().deregisterOwner(queryId, ownerDirectory);
      } else {
        FileUtils.deleteDirectory(ownerDirectory.toFile());
      }
      ownerDirectory = null;
      ownerRegistered = false;
    }
  }

  @Override
  public void close() throws IOException {
    if (!finished) {
      cleanupOwnerDirectory();
    }
  }
}
