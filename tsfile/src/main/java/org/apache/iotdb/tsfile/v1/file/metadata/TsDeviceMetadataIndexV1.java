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

package org.apache.iotdb.tsfile.v1.file.metadata;

import java.nio.ByteBuffer;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

public class TsDeviceMetadataIndexV1 {

  /**
   * The offset of the TsDeviceMetadata.
   */
  private long offset;
  /**
   * The size of the TsDeviceMetadata in the disk.
   */
  private int len;
  /**
   * The start time of the device.
   */
  private long startTime;
  /**
   * The end time of the device.
   */
  private long endTime;

  public TsDeviceMetadataIndexV1() {
    //do nothing
  }

  /**
   * use buffer to get a TsDeviceMetadataIndex.
   *
   * @param buffer -determine the index's source
   * @return -a TsDeviceMetadataIndex
   */
  public static TsDeviceMetadataIndexV1 deserializeFrom(ByteBuffer buffer) {
    TsDeviceMetadataIndexV1 index = new TsDeviceMetadataIndexV1();
    index.offset = ReadWriteIOUtils.readLong(buffer);
    index.len = ReadWriteIOUtils.readInt(buffer);
    index.startTime = ReadWriteIOUtils.readLong(buffer);
    index.endTime = ReadWriteIOUtils.readLong(buffer);
    return index;
  }

  public long getOffset() {
    return offset;
  }

  public int getLen() {
    return len;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }
}
