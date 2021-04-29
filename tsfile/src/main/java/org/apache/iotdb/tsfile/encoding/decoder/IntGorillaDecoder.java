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

import java.nio.ByteBuffer;

import static org.apache.iotdb.tsfile.common.conf.TSFileConfig.GORILLA_ENCODING_ENDING_INTEGER;
import static org.apache.iotdb.tsfile.common.conf.TSFileConfig.LEADING_ZERO_BITS_LENGTH_32BIT;
import static org.apache.iotdb.tsfile.common.conf.TSFileConfig.MEANINGFUL_XOR_BITS_LENGTH_32BIT;
import static org.apache.iotdb.tsfile.common.conf.TSFileConfig.VALUE_BITS_LENGTH_32BIT;

/**
 * This class includes code modified from Michael Burman's gorilla-tsc project.
 *
 * <p>Copyright: 2016-2018 Michael Burman and/or other contributors
 *
 * <p>Project page: https://github.com/burmanm/gorilla-tsc
 *
 * <p>License: http://www.apache.org/licenses/LICENSE-2.0
 */
public class IntGorillaDecoder extends GorillaDecoderV2 {

  protected int storedValue = 0;

  @Override
  public void reset() {
    super.reset();
    storedValue = 0;
  }

  @Override
  public final int readInt(ByteBuffer in) {
    int returnValue = storedValue;
    if (!firstValueWasRead) {
      flipByte(in);
      storedValue = (int) readLong(VALUE_BITS_LENGTH_32BIT, in);
      firstValueWasRead = true;
      returnValue = storedValue;
    }
    cacheNext(in);
    return returnValue;
  }

  protected int cacheNext(ByteBuffer in) {
    readNext(in);
    if (storedValue == GORILLA_ENCODING_ENDING_INTEGER) {
      hasNext = false;
    }
    return storedValue;
  }

  @SuppressWarnings("squid:S128")
  protected int readNext(ByteBuffer in) {
    byte controlBits = readNextClearBit(2, in);

    switch (controlBits) {
      case 3: // case '11': use new leading and trailing zeros
        storedLeadingZeros = (int) readLong(LEADING_ZERO_BITS_LENGTH_32BIT, in);
        byte significantBits = (byte) readLong(MEANINGFUL_XOR_BITS_LENGTH_32BIT, in);
        significantBits++;
        storedTrailingZeros = VALUE_BITS_LENGTH_32BIT - significantBits - storedLeadingZeros;
        // missing break is intentional, we want to overflow to next one
      case 2: // case '10': use stored leading and trailing zeros
        int xor =
            (int) readLong(VALUE_BITS_LENGTH_32BIT - storedLeadingZeros - storedTrailingZeros, in);
        xor <<= storedTrailingZeros;
        storedValue ^= xor;
        // missing break is intentional, we want to overflow to next one
      default: // case '0': use stored value
        return storedValue;
    }
  }
}
