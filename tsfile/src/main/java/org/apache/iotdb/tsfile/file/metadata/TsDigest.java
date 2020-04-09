
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

package org.apache.iotdb.tsfile.file.metadata;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

/**
 * Digest/statistics per chunk group and per page.
 */
public class TsDigest {

  private ByteBuffer[] statistics;

  /**
   * size of valid values in statistics. Note that some values in statistics can be null and thus
   * invalid.
   */
  private int validSizeOfArray = 0;

  private int serializedSize = Integer.BYTES; // initialize for number of statistics

  public TsDigest() {
    // allowed to declare an empty TsDigest whose fields will be assigned later.
  }

  public static int getNullDigestSize() {
    return Integer.BYTES;
  }

  /**
   * use given buffer to deserialize.
   *
   * @param buffer -given buffer
   * @return -an instance of TsDigest
   */
  public static TsDigest deserializeFrom(ByteBuffer buffer) {
    TsDigest digest = new TsDigest();
    int size = ReadWriteIOUtils.readInt(buffer);
    digest.validSizeOfArray = size;
    digest.serializedSize = Integer.BYTES;
    if (size > 0) {
      digest.statistics = new ByteBuffer[StatisticType.getTotalTypeNum()];
      ByteBuffer value;
      for (int i = 0; i < size; i++) {
        short n = ReadWriteIOUtils.readShort(buffer);
        value = ReadWriteIOUtils.readByteBufferWithSelfDescriptionLength(buffer);
        digest.statistics[n] = value;
        digest.serializedSize += Short.BYTES + Integer.BYTES + value.remaining();
      }
    } // else left digest.statistics as null

    return digest;
  }

  private void reCalculate() {
    validSizeOfArray = 0;
    serializedSize = Integer.BYTES;
    if (statistics != null) {
      for (ByteBuffer value : statistics) {
        if (value != null) {
          // StatisticType serialized value, byteBuffer.capacity and byteBuffer.array
          serializedSize += Short.BYTES + Integer.BYTES + value.remaining();
          validSizeOfArray++;
        }
      }
    }
  }

  /**
   * get statistics of the current object.
   */
  public ByteBuffer[] getStatistics() {
    return statistics; //TODO unmodifiable
  }

  public void setStatistics(ByteBuffer[] statistics) throws IOException {
    if (statistics != null && statistics.length != StatisticType.getTotalTypeNum()) {
      throw new IOException(String.format(
          "The length of array of statistics doesn't equal StatisticType.getTotalTypeNum() %d",
          StatisticType.getTotalTypeNum()));
    }
    this.statistics = statistics;
    reCalculate(); // DO NOT REMOVE THIS
  }

  @Override
  public String toString() {
    return statistics != null ? Arrays.toString(statistics) : "";
  }


  public enum StatisticType {
    min_value, max_value, first_value, last_value, sum_value;

    public static int getTotalTypeNum() {
      return StatisticType.values().length;
    }

    public static StatisticType deserialize(short i) {
      return StatisticType.values()[i];
    }

    public static int getSerializedSize() {
      return Short.BYTES;
    }

    public short serialize() {
      return (short) this.ordinal();
    }
  }
}