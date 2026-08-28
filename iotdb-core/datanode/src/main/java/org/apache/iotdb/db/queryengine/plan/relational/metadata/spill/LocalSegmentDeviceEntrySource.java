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
import java.util.List;

public final class LocalSegmentDeviceEntrySource extends SegmentDeviceEntrySource {

  private final DeviceEntrySpillManager spillManager;
  private boolean finished;

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
    List<DeviceEntry> result =
        deserialize(
            spillManager.readSegment(
                handle.getQueryId(), handle.getPlanNodeId().getId(), segmentId));
    releaseSegment(segmentId);
    nextSegmentId++;
    return result;
  }

  private void releaseSegment(int segmentId) throws IOException {
    if (segmentId + 1 == handle.getSegmentCount()) {
      finish();
    } else {
      spillManager.deleteSegment(handle.getQueryId(), handle.getPlanNodeId(), segmentId);
    }
  }

  private void finish() throws IOException {
    if (!finished) {
      spillManager.finishSegmentDataSet(handle.getQueryId(), handle.getPlanNodeId().getId());
      finished = true;
    }
  }

  @Override
  public void close() throws IOException {
    finish();
  }
}
