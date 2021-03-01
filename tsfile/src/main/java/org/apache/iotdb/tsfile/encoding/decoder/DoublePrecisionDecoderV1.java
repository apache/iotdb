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

package org.apache.iotdb.tsfile.encoding.decoder;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/** Decoder for value value using gorilla. */
public class DoublePrecisionDecoderV1 extends GorillaDecoderV1 {

  private static final Logger logger = LoggerFactory.getLogger(DoublePrecisionDecoderV1.class);
  private long preValue;

  public DoublePrecisionDecoderV1() {
    // do nothing
  }

  @Override
  public double readDouble(ByteBuffer buffer) {
    if (!flag) {
      flag = true;
      try {
        int[] buf = new int[8];
        for (int i = 0; i < 8; i++) {
          buf[i] = ReadWriteIOUtils.read(buffer);
        }
        long res = 0L;
        for (int i = 0; i < 8; i++) {
          res += ((long) buf[i] << (i * 8));
        }
        preValue = res;
        double tmp = Double.longBitsToDouble(preValue);
        leadingZeroNum = Long.numberOfLeadingZeros(preValue);
        tailingZeroNum = Long.numberOfTrailingZeros(preValue);
        fillBuffer(buffer);
        getNextValue(buffer);
        return tmp;
      } catch (IOException e) {
        logger.error("DoublePrecisionDecoderV1 cannot read first double number", e);
      }
    } else {
      try {
        double tmp = Double.longBitsToDouble(preValue);
        getNextValue(buffer);
        return tmp;
      } catch (IOException e) {
        logger.error("DoublePrecisionDecoderV1 cannot read following double number", e);
      }
    }
    return Double.NaN;
  }

  /**
   * check whether there is any value to encode left.
   *
   * @param buffer stream to read
   * @throws IOException cannot read from stream
   */
  private void getNextValue(ByteBuffer buffer) throws IOException {
    nextFlag1 = readBit(buffer);
    // case: '0'
    if (!nextFlag1) {
      return;
    }
    nextFlag2 = readBit(buffer);

    if (!nextFlag2) {
      // case: '10'
      long tmp = 0;
      for (int i = 0;
          i < TSFileConfig.VALUE_BITS_LENGTH_64BIT - leadingZeroNum - tailingZeroNum;
          i++) {
        long bit = readBit(buffer) ? 1 : 0;
        tmp |= (bit << (TSFileConfig.VALUE_BITS_LENGTH_64BIT - 1 - leadingZeroNum - i));
      }
      tmp ^= preValue;
      preValue = tmp;
    } else {
      // case: '11'
      int leadingZeroNumTmp =
          readIntFromStream(buffer, TSFileConfig.LEADING_ZERO_BITS_LENGTH_64BIT);
      int lenTmp = readIntFromStream(buffer, TSFileConfig.DOUBLE_VALUE_LENGTH);
      long tmp = readLongFromStream(buffer, lenTmp);
      tmp <<= (TSFileConfig.VALUE_BITS_LENGTH_64BIT - leadingZeroNumTmp - lenTmp);
      tmp ^= preValue;
      preValue = tmp;
    }
    leadingZeroNum = Long.numberOfLeadingZeros(preValue);
    tailingZeroNum = Long.numberOfTrailingZeros(preValue);
    if (Double.isNaN(Double.longBitsToDouble(preValue))) {
      isEnd = true;
    }
  }
}
