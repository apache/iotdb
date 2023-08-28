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
public class SinglePrecisionDecoderV1 extends GorillaDecoderV1 {

  private static final Logger logger = LoggerFactory.getLogger(SinglePrecisionDecoderV1.class);
  private int preValue;

  public SinglePrecisionDecoderV1() {
    // do nothing
  }

  @Override
  public float readFloat(ByteBuffer buffer) {
    if (!flag) {
      flag = true;
      try {
        int ch1 = ReadWriteIOUtils.read(buffer);
        int ch2 = ReadWriteIOUtils.read(buffer);
        int ch3 = ReadWriteIOUtils.read(buffer);
        int ch4 = ReadWriteIOUtils.read(buffer);
        preValue = ch1 + (ch2 << 8) + (ch3 << 16) + (ch4 << 24);
        leadingZeroNum = Integer.numberOfLeadingZeros(preValue);
        tailingZeroNum = Integer.numberOfTrailingZeros(preValue);
        float tmp = Float.intBitsToFloat(preValue);
        fillBuffer(buffer);
        getNextValue(buffer);
        return tmp;
      } catch (IOException e) {
        logger.error("SinglePrecisionDecoderV1 cannot read first float number", e);
      }
    } else {
      try {
        float tmp = Float.intBitsToFloat(preValue);
        getNextValue(buffer);
        return tmp;
      } catch (IOException e) {
        logger.error("SinglePrecisionDecoderV1 cannot read following float number", e);
      }
    }
    return Float.NaN;
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
      int tmp = 0;
      for (int i = 0;
          i < TSFileConfig.VALUE_BITS_LENGTH_32BIT - leadingZeroNum - tailingZeroNum;
          i++) {
        int bit = readBit(buffer) ? 1 : 0;
        tmp |= bit << (TSFileConfig.VALUE_BITS_LENGTH_32BIT - 1 - leadingZeroNum - i);
      }
      tmp ^= preValue;
      preValue = tmp;
    } else {
      // case: '11'
      int leadingZeroNumTmp =
          readIntFromStream(buffer, TSFileConfig.LEADING_ZERO_BITS_LENGTH_32BIT);
      int lenTmp = readIntFromStream(buffer, TSFileConfig.FLOAT_VALUE_LENGTH);
      int tmp = readIntFromStream(buffer, lenTmp);
      tmp <<= (TSFileConfig.VALUE_BITS_LENGTH_32BIT - leadingZeroNumTmp - lenTmp);
      tmp ^= preValue;
      preValue = tmp;
    }
    leadingZeroNum = Integer.numberOfLeadingZeros(preValue);
    tailingZeroNum = Integer.numberOfTrailingZeros(preValue);
    if (Float.isNaN(Float.intBitsToFloat(preValue))) {
      isEnd = true;
    }
  }
}
