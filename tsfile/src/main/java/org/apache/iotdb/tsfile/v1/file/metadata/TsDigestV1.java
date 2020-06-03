
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

/**
 * Digest/statistics per chunk group and per page.
 */
public class TsDigestV1 {

  private ByteBuffer[] statistics;

  public TsDigestV1() {
    // allowed to declare an empty TsDigest whose fields will be assigned later.
  }

  /**
   * use given buffer to deserialize.
   *
   * @param buffer -given buffer
   * @return -an instance of TsDigest
   */
  public static TsDigestV1 deserializeFrom(ByteBuffer buffer) {
    TsDigestV1 digest = new TsDigestV1();
    int size = ReadWriteIOUtils.readInt(buffer);
    if (size > 0) {
      digest.statistics = new ByteBuffer[StatisticType.getTotalTypeNum()];
      ByteBuffer value;
      for (int i = 0; i < size; i++) {
        short n = ReadWriteIOUtils.readShort(buffer);
        value = ReadWriteIOUtils.readByteBufferWithSelfDescriptionLength(buffer);
        digest.statistics[n] = value;
      }
    } // else left digest.statistics as null

    return digest;
  }

  /**
   * get statistics of the current object.
   */
  public ByteBuffer[] getStatistics() {
    return statistics;
  }

  public enum StatisticType {
    MIN_VALUE, MAX_VALUE, FIRST_VALUE, LAST_VALUE, SUM_VALUE;

    public static int getTotalTypeNum() {
      return StatisticType.values().length;
    }

  }
}