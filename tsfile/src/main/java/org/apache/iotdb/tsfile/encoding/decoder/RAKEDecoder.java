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
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;

import static java.lang.Math.pow;

public abstract class RAKEDecoder extends Decoder {
  // private static final Logger logger = LoggerFactory.getLogger(RAKEDecoder.class);
  protected boolean isReadFinish;
  // 8-bit buffer of bits to read in
  protected String rakeBuffer;
  protected String numBuffer;
  // decompression data is 8-bit buffer
  protected int deleteRakeBuffer;
  // number of bits remaining in buffer
  protected int numberLeftInBuffer;
  protected int L;
  private int T;

  public RAKEDecoder() {
    super(TSEncoding.RAKE);
    reset();
  }

  protected void readBuffer(ByteBuffer buffer) {
    String str = Integer.toBinaryString(ReadWriteIOUtils.read(buffer));
    int zeros = 8 - str.length();
    for (int i = 0; i < zeros; i++) {
      rakeBuffer += '0';
    }
    this.rakeBuffer += str;
    this.numberLeftInBuffer += 8;
    if (!hasNext(buffer)) {
      isReadFinish = true;
    }
  }

  protected void parseBuffer(ByteBuffer buffer, int len) {
    while (!isReadFinish || numBuffer.length() % len != 0) {
      readBuffer(buffer);
      int i = 0;
      while (i + L < this.numberLeftInBuffer) {
        if (rakeBuffer.charAt(i) == '0') {
          for (int j = 0; j < T; j++) numBuffer += '0';
          i++;
        } else {
          String sub = rakeBuffer.substring(i + 1, i + L + 1);
          int bias = Integer.parseInt(sub, 2);
          for (int j = 0; j < bias; j++) numBuffer += '0';
          numBuffer += '1';
          i += (L + 1);
        }
        // process the remaining end of long number
        int lengthNumBuffer = numBuffer.length();
        if (lengthNumBuffer > len - T && lengthNumBuffer < len) {
          while (i + len - numBuffer.length() > numberLeftInBuffer) {
            numBuffer += this.rakeBuffer.substring(i, numberLeftInBuffer);
            i = numberLeftInBuffer;
            this.rakeBuffer = this.rakeBuffer.substring(i, numberLeftInBuffer);
            deleteRakeBuffer += i;
            numberLeftInBuffer -= i;
            readBuffer(buffer);
            i = 0;
          }
          lengthNumBuffer = numBuffer.length();
          numBuffer += this.rakeBuffer.substring(i, i + len - numBuffer.length());
          i += len - lengthNumBuffer;
        }
        if (numBuffer.length() % len == 0) {
          this.rakeBuffer = this.rakeBuffer.substring(i, numberLeftInBuffer);
          numberLeftInBuffer -= i;
          deleteRakeBuffer += i;
          i = 0;
          deleteRakeBuffer %= 8;

          if (deleteRakeBuffer != 0) {
            if (i + 8 - deleteRakeBuffer > numberLeftInBuffer) {
              deleteRakeBuffer += numberLeftInBuffer - i;
              i = numberLeftInBuffer;
              this.rakeBuffer = this.rakeBuffer.substring(i);
              numberLeftInBuffer -= i;
              readBuffer(buffer);
              i = 0;
            }
            i += (8 - deleteRakeBuffer);
            this.rakeBuffer = this.rakeBuffer.substring(i);
            deleteRakeBuffer = 0;
            numberLeftInBuffer -= i;
          }
          return;
        }
      }
      this.rakeBuffer = this.rakeBuffer.substring(i, numberLeftInBuffer);
      deleteRakeBuffer += i;
      numberLeftInBuffer -= i;
    }
  }

  //  @Override
  //  public int readInt(ByteBuffer buffer) {
  //    parseBuffer(buffer, 32);
  //    String subNumBuffer = numBuffer.substring(0, 32);
  //    this.numBuffer = "";
  //    if (subNumBuffer.charAt(0) == '0') return Integer.parseInt(subNumBuffer, 2);
  //    else {
  //      String tmpSubNumBuffer = "0";
  //      for (int i = 1; i < subNumBuffer.length(); i++) {
  //        if (subNumBuffer.charAt(i) == '1') tmpSubNumBuffer += "0";
  //        else tmpSubNumBuffer += "1";
  //      }
  //
  //      return -Integer.parseInt(tmpSubNumBuffer, 2) - 1;
  //    }
  //  }

  //  @Override
  //  public long readLong(ByteBuffer buffer) {
  //    parseBuffer(buffer, 64);
  //    String subNumBuffer = numBuffer.substring(0, 64);
  //    this.numBuffer = "";
  //    if (subNumBuffer.charAt(0) == '0') return Long.parseLong(subNumBuffer, 2);
  //    else {
  //      String tmpSubNumBuffer = "0";
  //      for (int i = 1; i < subNumBuffer.length(); i++) {
  //        if (subNumBuffer.charAt(i) == '1') tmpSubNumBuffer += "0";
  //        else tmpSubNumBuffer += "1";
  //      }
  //
  //      return -Long.parseLong(tmpSubNumBuffer, 2) - 1;
  //    }
  //  }

  //  @Override
  //  public float readFloat(ByteBuffer buffer) {
  //    parseBuffer(buffer, 32);
  //    String subNumBuffer = numBuffer.substring(0, 32);
  //    this.numBuffer = "";
  //    if (subNumBuffer.charAt(0) == '0')
  //      return Float.intBitsToFloat(Integer.parseUnsignedInt(numBuffer, 2));
  //    else {
  //      String tmpSubNumBuffer = "0";
  //      for (int i = 1; i < subNumBuffer.length(); i++) {
  //        tmpSubNumBuffer += subNumBuffer.charAt(i);
  //      }
  //      return -Float.intBitsToFloat(Integer.parseUnsignedInt(tmpSubNumBuffer, 2));
  //    }
  //  }

  //  @Override
  //  public double readDouble(ByteBuffer buffer) {
  //    parseBuffer(buffer, 64);
  //    String subNumBuffer = numBuffer.substring(0, 64);
  //    this.numBuffer = "";
  //    if (subNumBuffer.charAt(0) == '0')
  //      return Double.longBitsToDouble(Long.parseUnsignedLong(numBuffer, 2));
  //    else {
  //      String tmpSubNumBuffer = "0";
  //      for (int i = 1; i < subNumBuffer.length(); i++) {
  //        tmpSubNumBuffer += subNumBuffer.charAt(i);
  //      }
  //      return -Double.longBitsToDouble(Long.parseUnsignedLong(tmpSubNumBuffer, 2));
  //    }
  //  }

  @Override
  public boolean hasNext(ByteBuffer buffer) {
    return buffer.remaining() > 0;
  }

  @Override
  public void reset() {
    this.isReadFinish = false;
    this.numberLeftInBuffer = 0;
    this.rakeBuffer = "";
    this.numBuffer = "";
    this.L = 2;
    this.T = (int) pow(2, L);
    this.deleteRakeBuffer = 0;
  }
}
