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

package org.apache.iotdb.tsfile.encoding.encoder;

import java.io.ByteArrayOutputStream;

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import static org.apache.iotdb.tsfile.common.conf.TSFileConfig.LEADING_ZERO_BITS_LENGTH_64BIT;
import static org.apache.iotdb.tsfile.common.conf.TSFileConfig.MEANINGFUL_XOR_BITS_LENGTH_64BIT;
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
public class LongChimpEncoder extends GorillaEncoderV2 {

    private int previousValues = 128;
    private int previousValuesLog2;
    private int threshold;
    private long storedValues[];
    private int setLsb;
    private int[] indices;
    private int index = 0;
    private int current = 0;
    private int flagOneSize;
    private int flagZeroSize;

    public LongChimpEncoder() {
      this.setType(TSEncoding.CHIMP);
      this.previousValuesLog2 =  (int)(Math.log(previousValues) / Math.log(2));
      this.threshold = THRESHOLD + previousValuesLog2;
      this.setLsb = (int) Math.pow(2, threshold + 1) - 1;
      this.indices = new int[(int) Math.pow(2, threshold + 1)];
      this.storedValues = new long[previousValues];
      this.flagZeroSize = previousValuesLog2 + 2;
      this.flagOneSize = previousValuesLog2 + 11;
    }

    private static final long CHIMP_ENCODING_ENDING =
            Double.doubleToRawLongBits(Double.NaN);

    private static final int ONE_ITEM_MAX_SIZE =
            (2
                        + LEADING_ZERO_BITS_LENGTH_64BIT
                        + MEANINGFUL_XOR_BITS_LENGTH_64BIT
                        + VALUE_BITS_LENGTH_64BIT)
                    / Byte.SIZE
                + 1;

    public final static int THRESHOLD = 6;

    public final static short[] leadingRepresentation = {0, 0, 0, 0, 0, 0, 0, 0,
              1, 1, 1, 1, 2, 2, 2, 2,
              3, 3, 4, 4, 5, 5, 6, 6,
              7, 7, 7, 7, 7, 7, 7, 7,
              7, 7, 7, 7, 7, 7, 7, 7,
              7, 7, 7, 7, 7, 7, 7, 7,
              7, 7, 7, 7, 7, 7, 7, 7,
              7, 7, 7, 7, 7, 7, 7, 7
          };

    public final static short[] leadingRound = {0, 0, 0, 0, 0, 0, 0, 0,
              8, 8, 8, 8, 12, 12, 12, 12,
              16, 16, 18, 18, 20, 20, 22, 22,
              24, 24, 24, 24, 24, 24, 24, 24,
              24, 24, 24, 24, 24, 24, 24, 24,
              24, 24, 24, 24, 24, 24, 24, 24,
              24, 24, 24, 24, 24, 24, 24, 24,
              24, 24, 24, 24, 24, 24, 24, 24
          };

    @Override
    public final int getOneItemMaxSize() {
      return ONE_ITEM_MAX_SIZE;
    }

    @Override
    protected void reset() {
      super.reset();
      this.current = 0;
      this.index = 0;
      this.indices = new int[(int) Math.pow(2, threshold + 1)];
      this.storedValues = new long[previousValues];
    }

    @Override
    public void flush(ByteArrayOutputStream out) {
      // ending stream
      encode(CHIMP_ENCODING_ENDING, out);

      // flip the byte no matter it is empty or not
      // the empty ending byte is necessary when decoding
      bitsLeft = 0;
      flipByte(out);

      // the encoder may be reused, so let us reset it
      reset();
    }

    @Override
    public final void encode(long value, ByteArrayOutputStream out) {
      if (firstValueWasWritten) {
        compressValue(value, out);
      } else {
        writeFirst(value, out);
        firstValueWasWritten = true;
      }
    }

    private void writeFirst(long value, ByteArrayOutputStream out) {
        storedValues[current] = value;
          writeBits(value, VALUE_BITS_LENGTH_64BIT, out);
          indices[(int) value & setLsb] = index;
        }

    private void compressValue(long value, ByteArrayOutputStream out) {
      int key = (int) value & setLsb;
      long xor;
      int previousIndex;
      int trailingZeros = 0;
      int currIndex = indices[key];
      if ((index - currIndex) < previousValues) {
          long tempXor = value ^ storedValues[currIndex % previousValues];
          trailingZeros = Long.numberOfTrailingZeros(tempXor);
          if (trailingZeros > threshold) {
              previousIndex = currIndex % previousValues;
              xor = tempXor;
          } else {
              previousIndex =  index % previousValues;
              xor = storedValues[previousIndex] ^ value;
          }
      } else {
          previousIndex =  index % previousValues;
          xor = storedValues[previousIndex] ^ value;
      }

        if(xor == 0) {
            writeBits(previousIndex, this.flagZeroSize, out);
            storedLeadingZeros = 65;
        } else {
            int leadingZeros = leadingRound[Long.numberOfLeadingZeros(xor)];

            if (trailingZeros > threshold) {
              int significantBits = 64 - leadingZeros - trailingZeros;
              writeBits(512 * (previousValues + previousIndex) + 64 * leadingRepresentation[leadingZeros] + significantBits, this.flagOneSize, out);
              writeBits(xor >>> trailingZeros, significantBits, out); // Store the meaningful bits of XOR
              storedLeadingZeros = 65;
          } else if (leadingZeros == storedLeadingZeros) {
              writeBit(out);
              skipBit(out);
              int significantBits = 64 - leadingZeros;
              writeBits(xor, significantBits, out);
          } else {
              storedLeadingZeros = leadingZeros;
              int significantBits = 64 - leadingZeros;
              writeBits(24 + leadingRepresentation[leadingZeros], 5, out);
              writeBits(xor, significantBits, out);
          }
      }
        current = (current + 1) % previousValues;
        storedValues[current] = value;
        index++;
        indices[key] = index;
    }
}
