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

import static org.apache.iotdb.tsfile.common.conf.TSFileConfig.GORILLA_ENCODING_ENDING_INTEGER;
import static org.apache.iotdb.tsfile.common.conf.TSFileConfig.LEADING_ZERO_BITS_LENGTH_32BIT;
import static org.apache.iotdb.tsfile.common.conf.TSFileConfig.MEANINGFUL_XOR_BITS_LENGTH_32BIT;
import static org.apache.iotdb.tsfile.common.conf.TSFileConfig.VALUE_BITS_LENGTH_32BIT;
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
public class IntChimpDecoder extends GorillaDecoderV2 {

    private static final int CHIMP_ENCODING_ENDING =
        Float.floatToRawIntBits(Float.NaN);

    private int previousValues = 64;
    private int storedValue = 0;
    private int storedValues[] = new int[previousValues];
    private int current = 0;
    private int previousValuesLog2;
    private int initialFill;

    public final static short[] leadingRepresentation = {0, 8, 12, 16, 18, 20, 22, 24};

    public IntChimpDecoder() {
      this.setType(TSEncoding.CHIMP);
      this.previousValuesLog2 =  (int)(Math.log(previousValues) / Math.log(2));
      this.initialFill = previousValuesLog2 + 8;
      this.hasNext = true;
      firstValueWasRead = false;
      storedLeadingZeros = Integer.MAX_VALUE;
      storedTrailingZeros = 0;
      this.current = 0;
      this.storedValue = 0;
      this.storedValues = new int[previousValues];
    }

    @Override
    public void reset() {
      super.reset();

      this.current = 0;
      this.storedValue = 0;
      this.storedValues = new int[previousValues];
    }

    @Override
    public final int readInt(ByteBuffer in) {
      int returnValue = storedValue;
      if (!firstValueWasRead) {
        flipByte(in);
        storedValue = (int) readLong(VALUE_BITS_LENGTH_32BIT, in);
        storedValues[current] = storedValue;
        firstValueWasRead = true;
        returnValue = storedValue;
      }
      cacheNext(in);
      return returnValue;
    }

    protected int cacheNext(ByteBuffer in) {
      readNext(in);
      if (storedValues[current] == CHIMP_ENCODING_ENDING) {
        hasNext = false;
      }
      return storedValues[current];
    }

    protected int readNext(ByteBuffer in) {
      // Read value
      byte controlBits = readNextNBits(2, in);
      int value;
      switch (controlBits) {
          case 3:
            storedLeadingZeros = leadingRepresentation[(int) readLong(3, in)];
            value = (int) readLong(32 - storedLeadingZeros, in);
            storedValue = storedValue ^ value;
            current = (current + 1) % previousValues;
            storedValues[current] = storedValue;
            return storedValue;
          case 2:
            value = (int) readLong(32 - storedLeadingZeros, in);
            storedValue = storedValue ^ value;
            current = (current + 1) % previousValues;
            storedValues[current] = storedValue;
            return storedValue;
          case 1:
          int fill = this.initialFill;
          int temp = (int) readLong(fill, in);
          int index = temp >>> (fill -= previousValuesLog2) & (1 << previousValuesLog2) - 1;
          storedLeadingZeros = leadingRepresentation[temp >>> (fill -= 3) & (1 << 3) - 1];
          int significantBits = temp >>> (fill -= 5) & (1 << 5) - 1;
          storedValue = storedValues[index];
          if(significantBits == 0) {
                significantBits = 32;
            }
            storedTrailingZeros = 32 - significantBits - storedLeadingZeros;
            value = (int) readLong(32 - storedLeadingZeros - storedTrailingZeros, in);
            value <<= storedTrailingZeros;
            storedValue = storedValue ^ value;
            current = (current + 1) % previousValues;
            storedValues[current] = storedValue;
            return storedValue;
          default:
              int previousIndex = (int) readLong(previousValuesLog2, in);
            storedValue = storedValues[previousIndex];
            current = (current + 1) % previousValues;
            storedValues[current] = storedValue;
            return storedValue;
          }
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
}
