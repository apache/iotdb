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

import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public final class LocalSegmentDeviceEntrySource extends SegmentDeviceEntrySource {

  private final DeviceEntrySpillManager spillManager;

  public LocalSegmentDeviceEntrySource(DeviceEntryDataSetHandle handle) {
    this(handle, DeviceEntrySpillManager.getInstance());
  }

  public LocalSegmentDeviceEntrySource(
      DeviceEntryDataSetHandle handle, DeviceEntrySpillManager spillManager) {
    super(handle);
    this.spillManager = spillManager;
  }

  @Override
  public List<DeviceEntry> nextBatch() throws IOException {
    int segmentId = nextSegmentId;
    Path segment = acquireSegment(segmentId);
    List<DeviceEntry> result = deserialize(Files.readAllBytes(segment));
    releaseSegment(segmentId);
    nextSegmentId++;
    return result;
  }

  private Path acquireSegment(int segmentId) throws IOException {
    return spillManager.resolveSegment(handle.getQueryId(), handle.getPlanNodeId(), segmentId);
  }

  private void releaseSegment(int segmentId) throws IOException {
    if (segmentId + 1 == handle.getSegmentCount()) {
      spillManager.finishSegmentDataSet(handle.getQueryId(), handle.getPlanNodeId().getId());
    } else {
      spillManager.deleteSegment(handle.getQueryId(), handle.getPlanNodeId(), segmentId);
    }
  }
}
