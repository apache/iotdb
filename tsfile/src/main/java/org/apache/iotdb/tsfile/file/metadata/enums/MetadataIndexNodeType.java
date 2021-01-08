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


/**
 * The type of MetadataIndexNode
 *
 * INTERNAL_DEVICE: internal nodes of the index tree's device level
 * LEAF_DEVICE: leaf nodes of the index tree's device level, points to measurement level
 * INTERNAL_MEASUREMENT: internal nodes of the index tree's measurement level
 * LEAF_MEASUREMENT: leaf nodes of the index tree's device level, points to TimeseriesMetadata
 */
public enum MetadataIndexNodeType {
  INTERNAL_DEVICE, LEAF_DEVICE, INTERNAL_MEASUREMENT, LEAF_MEASUREMENT;

  /**
   * deserialize byte number.
   *
   * @param i byte number
   * @return MetadataIndexNodeType
   */
  public static MetadataIndexNodeType deserialize(byte i) {
    if (i >= 4) {
      throw new IllegalArgumentException("Invalid input: " + i);
    }
    switch (i) {
      case 0:
        return INTERNAL_DEVICE;
      case 1:
        return LEAF_DEVICE;
      case 2:
        return INTERNAL_MEASUREMENT;
      default:
        return LEAF_MEASUREMENT;
    }
  }

  public static MetadataIndexNodeType deserializeFrom(ByteBuffer buffer) {
    return deserialize(buffer.get());
  }

  public static int getSerializedSize() {
    return Byte.BYTES;
  }

  public void serializeTo(ByteBuffer byteBuffer) {
    byteBuffer.put(serialize());
  }

  public void serializeTo(DataOutputStream outputStream) throws IOException {
    outputStream.write(serialize());
  }

  /**
   * return a serialize child metadata index type.
   *
   * @return -enum type
   */
  public byte serialize() {
    switch (this) {
      case INTERNAL_DEVICE:
        return 0;
      case LEAF_DEVICE:
        return 1;
      case INTERNAL_MEASUREMENT:
        return 2;
      case LEAF_MEASUREMENT:
        return 3;
      default:
        return -1;
    }
  }
}
