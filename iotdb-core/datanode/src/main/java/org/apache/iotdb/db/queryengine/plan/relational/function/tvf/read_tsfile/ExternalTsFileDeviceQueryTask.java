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

package org.apache.iotdb.db.queryengine.plan.relational.function.tvf.read_tsfile;

import org.apache.tsfile.utils.Accountable;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public record ExternalTsFileDeviceQueryTask(int deviceEntryIndex, List<DeviceOffset> deviceOffsets)
    implements Accountable {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(ExternalTsFileDeviceQueryTask.class);

  void serialize(DataOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(deviceEntryIndex, outputStream);
    ReadWriteIOUtils.write(deviceOffsets.size(), outputStream);
    for (DeviceOffset offset : deviceOffsets) {
      ReadWriteIOUtils.write(offset.fileIndex, outputStream);
      ReadWriteIOUtils.write(offset.startOffset, outputStream);
      ReadWriteIOUtils.write(offset.endOffset, outputStream);
    }
  }

  static ExternalTsFileDeviceQueryTask deserialize(DataInputStream inputStream) throws IOException {
    int deviceEntryIndex = ReadWriteIOUtils.readInt(inputStream);
    int offsetSize = ReadWriteIOUtils.readInt(inputStream);
    List<DeviceOffset> offsets = new ArrayList<>(offsetSize);
    for (int i = 0; i < offsetSize; i++) {
      int fileIndex = ReadWriteIOUtils.readInt(inputStream);
      long startOffset = inputStream.readLong();
      long endOffset = inputStream.readLong();
      offsets.add(new DeviceOffset(fileIndex, startOffset, endOffset));
    }
    return new ExternalTsFileDeviceQueryTask(deviceEntryIndex, offsets);
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + RamUsageEstimator.shallowSizeOfInstance(ArrayList.class)
        + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER
        + (long) RamUsageEstimator.NUM_BYTES_OBJECT_REF * deviceOffsets.size()
        + deviceOffsets.size() * DeviceOffset.INSTANCE_SIZE;
  }

  public static class DeviceOffset {

    private static final long INSTANCE_SIZE =
        RamUsageEstimator.shallowSizeOfInstance(DeviceOffset.class);

    private final int fileIndex;
    private final long startOffset;
    private final long endOffset;

    DeviceOffset(int fileIndex, long startOffset, long endOffset) {
      this.fileIndex = fileIndex;
      this.startOffset = startOffset;
      this.endOffset = endOffset;
    }

    public int getFileIndex() {
      return fileIndex;
    }

    public long getStartOffset() {
      return startOffset;
    }

    public long getEndOffset() {
      return endOffset;
    }
  }
}
