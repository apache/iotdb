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

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.nio.ByteBuffer;

import static org.apache.iotdb.tsfile.common.conf.TSFileConfig.VALUE_BITS_LENGTH_64BIT;

/**
 * This class includes code modified from Panagiotis Liakos chimp project.
 *
 * <p>Copyright: 2022- Panagiotis Liakos, Katia Papakonstantinopoulou and Yannis Kotidis
 *
 * <p>Project page: https://github.com/panagiotisl/chimp
 *
 * <p>License: http://www.apache.org/licenses/LICENSE-2.0
 */
public class LongChimpDecoder extends GorillaDecoderV2 {

  private static final short[] LEADING_REPRESENTATION = {0, 8, 12, 16, 18, 20, 22, 24};
  private static final int PREVIOUS_VALUES = 128;
  private static final int PREVIOUS_VALUES_LOG2 = (int) (Math.log(PREVIOUS_VALUES) / Math.log(2));
  private static final int CASE_ONE_METADATA_LENGTH = PREVIOUS_VALUES_LOG2 + 9;

  private long storedValue = 0;
  protected long storedValues[] = new long[PREVIOUS_VALUES];
  protected int current = 0;

  public LongChimpDecoder() {
    this.setType(TSEncoding.CHIMP);
    this.hasNext = true;
    firstValueWasRead = false;
    storedLeadingZeros = Integer.MAX_VALUE;
    storedTrailingZeros = 0;
    this.current = 0;
    this.storedValue = 0;
    this.storedValues = new long[PREVIOUS_VALUES];
  }

  @Override
  public void reset() {
    super.reset();

    this.current = 0;
    this.storedValue = 0;
    this.storedValues = new long[PREVIOUS_VALUES];
  }

  @Override
  public final long readLong(ByteBuffer in) {
    long returnValue = storedValue;
    if (!firstValueWasRead) {
      flipByte(in);
      storedValue = readLong(VALUE_BITS_LENGTH_64BIT, in);
      storedValues[current] = storedValue;
      firstValueWasRead = true;
      returnValue = storedValue;
    }
    cacheNext(in);
    return returnValue;
  }

  protected long cacheNext(ByteBuffer in) {
    readNext(in);
    if (storedValues[current] == Long.MIN_VALUE) {
      hasNext = false;
    }
    return storedValues[current];
  }

  protected long readNext(ByteBuffer in) {
    // read the two control bits
    byte controlBits = readNextNBits(2, in);
    long value;
    switch (controlBits) {
        // case 11: read the length of the number of leading
        // zeros in the next 3 bits, then read the
        // meaningful bits of the XORed value.
      case 3:
        storedLeadingZeros = LEADING_REPRESENTATION[(int) readLong(3, in)];
        value = readLong(VALUE_BITS_LENGTH_64BIT - storedLeadingZeros, in);
        storedValue = storedValue ^ value;
        current = (current + 1) % PREVIOUS_VALUES;
        storedValues[current] = storedValue;
        return storedValue;
        // case 10: use the previous leading zeros and
        // and just read the meaningful XORed value.
      case 2:
        value = readLong(VALUE_BITS_LENGTH_64BIT - storedLeadingZeros, in);
        storedValue = storedValue ^ value;
        current = (current + 1) % PREVIOUS_VALUES;
        storedValues[current] = storedValue;
        return storedValue;
        // case 01:  read the index of the previous value, the length of
        // the number of leading zeros in the next 3 bits, then read
        // the length of the meaningful XORed value in the next 6
        // bits. Finally read the meaningful bits of the XORed value.
      case 1:
        int fill = CASE_ONE_METADATA_LENGTH;
        int temp = (int) readLong(fill, in);
        int index = temp >>> (fill -= PREVIOUS_VALUES_LOG2) & (1 << PREVIOUS_VALUES_LOG2) - 1;
        storedLeadingZeros = LEADING_REPRESENTATION[temp >>> (fill -= 3) & (1 << 3) - 1];
        int significantBits = temp >>> (fill -= 6) & (1 << 6) - 1;
        storedValue = storedValues[index];
        if (significantBits == 0) {
          significantBits = VALUE_BITS_LENGTH_64BIT;
        }
        storedTrailingZeros = VALUE_BITS_LENGTH_64BIT - significantBits - storedLeadingZeros;
        value = readLong(VALUE_BITS_LENGTH_64BIT - storedLeadingZeros - storedTrailingZeros, in);
        value <<= storedTrailingZeros;
        storedValue = storedValue ^ value;
        current = (current + 1) % PREVIOUS_VALUES;
        storedValues[current] = storedValue;
        return storedValue;
        // case 00: the values are identical, just read
        // the index of the previous value
      default:
        int previousIndex = (int) readLong(PREVIOUS_VALUES_LOG2, in);
        storedValue = storedValues[previousIndex];
        current = (current + 1) % PREVIOUS_VALUES;
        storedValues[current] = storedValue;
        return storedValue;
    }
  }

  private byte readNextNBits(int n, ByteBuffer in) {
    byte value = 0x00;
    for (int i = 0; i < n; i++) {
      value <<= 1;
      if (readBit(in)) {
        value |= 0x01;
      }
    }
    return value;
  }
}
