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

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import static org.apache.iotdb.tsfile.common.conf.TSFileConfig.VALUE_BITS_LENGTH_64BIT;

/**
 * This class includes code modified from Michael Burman's gorilla-tsc project.
 *
 * <p>Copyright: 2016-2018 Michael Burman and/or other contributors
 *
 * <p>Project page: https://github.com/burmanm/gorilla-tsc
 *
 * <p>License: http://www.apache.org/licenses/LICENSE-2.0
 */
public class ChimpDecoder extends Decoder {

  private static final long CHIMP_ENCODING_ENDING =
      Double.doubleToRawLongBits(Double.NaN);


  protected boolean firstValueWasRead = false;
  protected int storedLeadingZeros = Integer.MAX_VALUE;
  protected int storedTrailingZeros = 0;
  protected boolean hasNext = true;

  private byte buffer = 0;
  private int bitsLeft = 0;
  protected long storedValue = 0;

  public final static short[] leadingRepresentation = {0, 8, 12, 16, 18, 20, 22, 24};
	
  public ChimpDecoder() {
    super(TSEncoding.CHIMP);
  }

  @Override
  public final boolean hasNext(ByteBuffer in) {
    return hasNext;
  }

  @Override
  public void reset() {
    firstValueWasRead = false;
    storedLeadingZeros = Integer.MAX_VALUE;
    storedTrailingZeros = 0;
    hasNext = true;

    buffer = 0;
    bitsLeft = 0;
    storedValue = 0;
  }

  /**
   * Reads the next bit and returns a boolean representing it.
   *
   * @return true if the next bit is 1, otherwise 0.
   */
  protected boolean readBit(ByteBuffer in) {
    boolean bit = ((buffer >> (bitsLeft - 1)) & 1) == 1;
    bitsLeft--;
    flipByte(in);
    return bit;
  }
  
  @Override
  public final double readDouble(ByteBuffer in) {
    return Double.longBitsToDouble(readLong(in));
  }

  @Override
  public final long readLong(ByteBuffer in) {
    long returnValue = storedValue;
    if (!firstValueWasRead) {
      flipByte(in);
      storedValue = readLong(VALUE_BITS_LENGTH_64BIT, in);
      firstValueWasRead = true;
      returnValue = storedValue;
    }
    cacheNext(in);
    return returnValue;
  }
  
  protected long cacheNext(ByteBuffer in) {
    readNext(in);
    if (storedValue == CHIMP_ENCODING_ENDING) {
      hasNext = false;
    }
    return storedValue;
  }
  protected long readNext(ByteBuffer in) {
	  int significantBits;
	  long value;
	  byte controlBits = readNextNBits(2, in);
	  switch(controlBits) {
		case 3:
	      // New leading zeros
	      storedLeadingZeros = leadingRepresentation[(int) readLong(3, in)];
	      significantBits = 64 - storedLeadingZeros;
	      if(significantBits == 0) {
	          significantBits = 64;
	      }
	      value = readLong(64 - storedLeadingZeros, in);
	      storedValue = storedValue ^ value;
	      return storedValue;
		case 2:
			significantBits = 64 - storedLeadingZeros;
	      if(significantBits == 0) {
	          significantBits = 64;
	      }
	      value = readLong(64 - storedLeadingZeros, in);
	      storedValue = storedValue ^ value;
	      return storedValue;
		case 1:
			storedLeadingZeros = leadingRepresentation[(int) readLong(3, in)];
	  	significantBits = (int) readLong(6, in);
	  	if(significantBits == 0) {
	          significantBits = 64;
	      }
	      storedTrailingZeros = 64 - significantBits - storedLeadingZeros;
	      value = readLong(64 - storedLeadingZeros - storedTrailingZeros, in);
	      value <<= storedTrailingZeros;
	      storedValue = storedValue ^ value;
	      return storedValue;
		default:
	      return storedValue;
	  }
	}
  
  /**
   * Reads a long from the next X bits that represent the least significant bits in the long value.
   *
   * @param bits How many next bits are read from the stream
   * @return long value that was read from the stream
   */
  protected long readLong(int bits, ByteBuffer in) {
    long value = 0;
    while (bits > 0) {
      if (bits > bitsLeft || bits == Byte.SIZE) {
        // Take only the bitsLeft "least significant" bits
        byte d = (byte) (buffer & ((1 << bitsLeft) - 1));
        value = (value << bitsLeft) + (d & 0xFF);
        bits -= bitsLeft;
        bitsLeft = 0;
      } else {
        // Shift to correct position and take only least significant bits
        byte d = (byte) ((buffer >>> (bitsLeft - bits)) & ((1 << bits) - 1));
        value = (value << bits) + (d & 0xFF);
        bitsLeft -= bits;
        bits = 0;
      }
      flipByte(in);
    }
    return value;
  }

  protected byte readNextNBits(int n, ByteBuffer in) {
    byte value = 0x00;
    for (int i = 0; i < n; i++) {
      value <<= 1;
      if (readBit(in)) {
        value |= 0x01;
      }
    }
    return value;
  }

  protected void flipByte(ByteBuffer in) {
    if (bitsLeft == 0) {
      buffer = in.get();
      bitsLeft = Byte.SIZE;
    }
  }
}
