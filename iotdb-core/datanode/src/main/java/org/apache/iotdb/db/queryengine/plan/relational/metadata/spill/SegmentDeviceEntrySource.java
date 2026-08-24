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

import org.apache.iotdb.db.queryengine.common.DataNodeEndPoints;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class SegmentDeviceEntrySource implements BatchDeviceEntrySource {

  protected final DeviceEntryDataSetHandle handle;
  protected int nextSegmentId;

  protected SegmentDeviceEntrySource(DeviceEntryDataSetHandle handle) {
    this.handle = handle;
  }

  public static SegmentDeviceEntrySource create(DeviceEntryDataSetHandle handle) {
    return handle
            .getCoordinatorMppDataExchangeEndPoint()
            .equals(DataNodeEndPoints.LOCAL_HOST_DATA_BLOCK_ENDPOINT)
        ? new LocalSegmentDeviceEntrySource(handle)
        : new RemoteSegmentDeviceEntrySource(handle);
  }

  @Override
  public final boolean hasNextBatch() {
    return nextSegmentId < handle.getSegmentCount();
  }

  protected final List<DeviceEntry> deserialize(byte[] segmentBytes) throws IOException {
    List<DeviceEntry> result = new ArrayList<>();
    int segmentLength = segmentBytes.length;
    try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(segmentBytes))) {
      int consumedBytes = 0;
      while (consumedBytes < segmentLength) {
        if (segmentLength - consumedBytes < Integer.BYTES) {
          throw new IOException();
        }
        int length = input.readInt();
        consumedBytes += Integer.BYTES;
        if (length < 0 || length > segmentLength - consumedBytes) {
          throw new IOException();
        }
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        consumedBytes += length;
        result.add(DeviceEntry.deserialize(bytes));
      }
    }
    return result;
  }

  @Override
  public void close() throws IOException {
    // No local cache is created by a segment source.
  }
}
