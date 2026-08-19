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

public final class RemoteSegmentDeviceEntrySource extends SegmentDeviceEntrySource {

  private final DeviceEntrySegmentFetcher fetcher;
  private boolean finished;

  public RemoteSegmentDeviceEntrySource(DeviceEntryDataSetHandle handle) {
    this(handle, DeviceEntryRpcSegmentFetcher.getInstance());
  }

  public RemoteSegmentDeviceEntrySource(
      DeviceEntryDataSetHandle handle, DeviceEntrySegmentFetcher fetcher) {
    super(handle);
    this.fetcher = fetcher;
  }

  @Override
  public List<DeviceEntry> nextBatch() throws IOException {
    int segmentId = nextSegmentId;
    byte[] payload = fetcher.fetch(handle, segmentId);
    List<DeviceEntry> result = deserialize(payload);
    nextSegmentId++;
    if (!hasNextBatch()) {
      finish();
    }
    return result;
  }

  private void finish() {
    fetcher.finish(handle);
    finished = true;
  }

  @Override
  public void close() throws IOException {
    if (!finished) {
      finish();
    }
  }
}
