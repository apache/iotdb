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

package org.apache.iotdb.tsfile.file.metadata.enums;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public enum ChildMetadataIndexType {
  DEVICE_INDEX, DEVICE, MEASUREMENT_INDEX, MEASUREMENT;

  /**
   * deserialize short number.
   *
   * @param i short number
   * @return ChildMetadataIndexType
   */
  public static ChildMetadataIndexType deserialize(short i) {
    if (i >= 4) {
      throw new IllegalArgumentException("Invalid input: " + i);
    }
    switch (i) {
      case 0:
        return DEVICE_INDEX;
      case 1:
        return DEVICE;
      case 2:
        return MEASUREMENT_INDEX;
      default:
        return MEASUREMENT;
    }
  }

  public static ChildMetadataIndexType deserializeFrom(ByteBuffer buffer) {
    return deserialize(buffer.getShort());
  }

  public static int getSerializedSize() {
    return Short.BYTES;
  }

  public void serializeTo(ByteBuffer byteBuffer) {
    byteBuffer.putShort(serialize());
  }

  public void serializeTo(DataOutputStream outputStream) throws IOException {
    outputStream.writeShort(serialize());
  }

  /**
   * return a serialize child metadata index type.
   *
   * @return -enum type
   */
  public short serialize() {
    switch (this) {
      case DEVICE_INDEX:
        return 0;
      case DEVICE:
        return 1;
      case MEASUREMENT_INDEX:
        return 2;
      case MEASUREMENT:
        return 3;
      default:
        return -1;
    }
  }
}
