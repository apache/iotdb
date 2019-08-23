/**
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
import java.io.InputStream;
import java.io.OutputStream;
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
    // allowed to clair an empty TsDigest whose fields will be assigned later.
  }

  public static int getNullDigestSize() {
    return Integer.BYTES;
  }

  public static int serializeNullTo(OutputStream outputStream) throws IOException {
    return ReadWriteIOUtils.write(0, outputStream);
  }

  public static int serializeNullTo(ByteBuffer buffer) {
    return ReadWriteIOUtils.write(0, buffer);
  }

  /**
   * use given input stream to deserialize.
   *
   * @param inputStream -given input stream
   * @return -an instance of TsDigest
   */
  public static TsDigest deserializeFrom(InputStream inputStream) throws IOException {
    TsDigest digest = new TsDigest();

    int size = ReadWriteIOUtils.readInt(inputStream);
    digest.validSizeOfArray = size;
    digest.serializedSize = Integer.BYTES;
    if (size > 0) {
      digest.statistics = new ByteBuffer[StatisticType.getTotalTypeNum()];
      ByteBuffer value;
      for (int i = 0; i < size; i++) {
        short n = ReadWriteIOUtils.readShort(inputStream);
        value = ReadWriteIOUtils.readByteBufferWithSelfDescriptionLength(inputStream);
        digest.statistics[n] = value;
        digest.serializedSize += Short.BYTES + Integer.BYTES + value.remaining();
      }
    } // else left digest.statistics as null

    return digest;
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

  /**
   * use given outputStream to serialize.
   *
   * @param outputStream -given outputStream
   * @return -byte length
   */
  public int serializeTo(OutputStream outputStream) throws IOException {
    int byteLen = 0;
    if (validSizeOfArray == 0) {
      byteLen += ReadWriteIOUtils.write(0, outputStream);
    } else {
      byteLen += ReadWriteIOUtils.write(validSizeOfArray, outputStream);
      for (int i = 0; i < statistics.length; i++) {
        if (statistics[i] != null) {
          byteLen += ReadWriteIOUtils.write((short) i, outputStream);
          byteLen += ReadWriteIOUtils.write(statistics[i], outputStream);
        }
      }
    }
    return byteLen;
  }

  /**
   * use given buffer to serialize.
   *
   * @param buffer -given buffer
   * @return -byte length
   */
  public int serializeTo(ByteBuffer buffer) {
    int byteLen = 0;
    if (validSizeOfArray == 0) {
      byteLen += ReadWriteIOUtils.write(0, buffer);
    } else {
      byteLen += ReadWriteIOUtils.write(validSizeOfArray, buffer);
      for (int i = 0; i < statistics.length; i++) {
        if (statistics[i] != null) {
          byteLen += ReadWriteIOUtils.write((short) i, buffer);
          byteLen += ReadWriteIOUtils.write(statistics[i], buffer);
        }
      }
    }
    return byteLen;
  }

  /**
   * get the serializedSize of the current object.
   *
   * @return -serializedSize
   */
  public int getSerializedSize() {
    return serializedSize;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TsDigest digest = (TsDigest) o;
    if (serializedSize != digest.serializedSize || validSizeOfArray != digest.validSizeOfArray
        || ((statistics == null) ^ (digest.statistics == null))) {
      return false;
    }

    if (statistics != null) {
      for (int i = 0; i < statistics.length; i++) {
        if ((statistics[i] == null) ^ (digest.statistics[i] == null)) {
          // one is null and the other is not null
          return false;
        }
        if (statistics[i] != null) {
          if (!statistics[i].equals(digest.statistics[i])) {
            return false;
          }
        }
      }
    }
    return true;
  }

  public enum StatisticType {
    max_value, min_value, first, sum, last;

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
