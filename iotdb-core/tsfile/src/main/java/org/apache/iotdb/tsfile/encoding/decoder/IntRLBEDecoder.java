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

public class IntRLBEDecoder extends Decoder {
  /** constructor of IntRLBEDecoder */
  public IntRLBEDecoder() {
    super(TSEncoding.RLBE);
    numberLeftInBuffer = 0;
    byteBuffer = 0;
  }

  /** how many numbers expected to decode in following block */
  private int blocksize;

  /** origin values are stored in data */
  private int[] data;

  /** whether the first value is decoded */
  private int writeindex = -1;

  /** the pointer of current last value */
  private int readindex = -1;

  /** fibonacci values are stored in fibonacci */
  private int[] fibonacci;

  /** read bits from input stream to byteBuffer and get bit from byteBuffer */
  private byte byteBuffer;

  /** valid bits in byteBuffer */
  private int numberLeftInBuffer;

  /**
   * read the header of a block, determine the size of block and malloc space values
   *
   * @param buffer inputstream buffer
   */
  private void readhead(ByteBuffer buffer) {
    for (int i = 0; i <= writeindex; i++) {
      data[i] = 0;
    }
    writeindex = -1;
    readindex = -1;
    clearBuffer(buffer);
    readblocksize(buffer);
    data = new int[blocksize * 2 + 1];
    fibonacci = new int[blocksize * 2 + 1];
    for (int i = 0; i < blocksize * 2; i++) {
      data[i] = 0;
      fibonacci[i] = 0;
    }
    fibonacci[0] = 1;
    fibonacci[1] = 1;
  }

  /**
   * read a block from inputstream buffer
   *
   * @param buffer inputstream buffer
   */
  private void readT(ByteBuffer buffer) {
    // read the header of the block
    readhead(buffer);
    while (writeindex < blocksize - 1) {
      int seglength = 0, runlength = 0;
      // read first 6 bits: length of each binary words.
      for (int j = 5; j >= 0; j--) {
        seglength |= (readbit(buffer) << j);
      }

      // generate repeat time of rle on delta
      int now = readbit(buffer);
      int next = readbit(buffer);

      int j = 1;
      while (true) {
        if (j > 1) fibonacci[j] = fibonacci[j - 1] + fibonacci[j - 2];
        if (now == 1) runlength += fibonacci[j];
        // when now and next are both 1, the 1 of next is the symbol of ending of fibonacci code
        if (now == 1 && next == 1) break;
        j++;
        now = next;
        next = readbit(buffer);
      }
      // read the delta value one by one
      for (int i = 1; i <= runlength; i++) {

        int readinttemp = 0;
        for (int k = seglength - 1; k >= 0; k--) {
          readinttemp += (readbit(buffer) << k);
        }
        if (seglength == 32) readinttemp -= (1 << 31);
        if (writeindex == -1) {
          data[++writeindex] = readinttemp;
        } else {
          ++writeindex;
          data[writeindex] = data[writeindex - 1] + readinttemp;
        }
      }
    }
  }

  @Override
  public int readInt(ByteBuffer buffer) {
    if (readindex < writeindex) {
      return data[++readindex];
    } else {
      readT(buffer);
      return data[++readindex];
    }
  }

  @Override
  public boolean hasNext(ByteBuffer buffer) {
    return (buffer.remaining() > 0 || readindex < writeindex);
  }

  @Override
  public void reset() {
    // do nothing
  }

  /**
   * get a bit from byteBuffer, when there is no bit in byteBuffer, get new 8 bits from inputstream
   * buffer
   *
   * @param buffer inputstream buffer
   * @return the top bit of byteBuffer
   */
  private int readbit(ByteBuffer buffer) {
    if (numberLeftInBuffer == 0) {
      loadBuffer(buffer);
      numberLeftInBuffer = 8;
    }
    int top = ((byteBuffer >> 7) & 1);
    byteBuffer <<= 1;
    numberLeftInBuffer--;
    return top;
  }

  /**
   * get 8 bits from inputstream buffer to byteBuffer
   *
   * @param buffer inputstream buffer
   */
  private void loadBuffer(ByteBuffer buffer) {
    byteBuffer = buffer.get();
  }

  /**
   * clear all remaining bits in byteBuffer
   *
   * @param buffer inputstream buffer
   */
  private void clearBuffer(ByteBuffer buffer) {
    while (numberLeftInBuffer > 0) {
      readbit(buffer);
    }
  }

  /**
   * read the first integer of the block: blocksize
   *
   * @param buffer inputstream buffer
   */
  private void readblocksize(ByteBuffer buffer) {
    blocksize = 0;
    for (int i = 31; i >= 0; i--) {
      if (readbit(buffer) == 1) {
        blocksize |= (1 << i);
      }
    }
  }
}
