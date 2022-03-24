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

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static java.lang.Math.pow;

public abstract class RAKEEncoder extends Encoder {
  public String rakeBuffer;
  private byte byteBuffer;
  protected int numberLeftInBuffer;
  /** whether to finish reading int, long, double or float * */
  protected boolean isReadFinish;
  /** group size maximum * */
  protected int groupMax = 16;

  /** group number * */
  protected int groupNum;

  private int T;
  private int L;
  protected ByteArrayOutputStream byteCache;
  protected static final Logger logger = LoggerFactory.getLogger(RAKEEncoder.class);

  /** constructor. * */
  public RAKEEncoder() {

    super(TSEncoding.RAKE);
    L = 2;
    T = (int) pow(2, L);
    isReadFinish = false;
    rakeBuffer = "";
    byteCache = new ByteArrayOutputStream();
    groupNum = 0;
  }

  protected void reset() {
    byteCache.reset();
    isReadFinish = false;
    groupNum = 0;
  }

  /** encode binary digits of the string type into the byte stream */
  protected void encodeNumber(String bit_value, int length, ByteArrayOutputStream out) {
    int zeros = length - bit_value.length();
    for (int i = 0; i < zeros; i++) {
      rakeBuffer += '0';
    }
    rakeBuffer += bit_value;
    int len = rakeBuffer.length();
    int i = 0;
    boolean found = false;
    int bias = 0;
    while (i + T <= len) {
      for (int j = 0; j < T; j++) {
        if (rakeBuffer.charAt(i + j) == '1') {
          found = true;
          bias = j;
          break;
        }
      }
      if (found) {
        String b = Integer.toBinaryString(bias);
        writeBit(true, byteCache);
        int zero_b = L - b.length();
        for (int k = 0; k < zero_b; k++) {
          writeBit(false, byteCache);
        }
        for (int k = 0; k < b.length() - 1; k++) {
          writeBit(b.charAt(k) == '1', byteCache);
        }
        if (i == len - bias - 1) {
          isReadFinish = true;
        }
        writeBit(b.charAt(b.length() - 1) == '1', byteCache);
        i += (bias + 1);
      } else {
        if (i == len - T) {
          isReadFinish = true;
        }
        writeBit(false, byteCache);
        i += T;
      }
      if (i + T > len && i < len && !isReadFinish) {
        for (int k = i; k < len - 1; k++) {
          writeBit(rakeBuffer.charAt(k) == '1', byteCache);
        }
        isReadFinish = true;
        writeBit(rakeBuffer.charAt(len - 1) == '1', byteCache);
        break;
      }
      found = false;
    }
    isReadFinish = true;
    rakeBuffer = "";
    groupNum++;
    if (groupNum > groupMax) {
      try {
        this.flush(out);
        groupNum = 0;
      } catch (IOException e) {
        logger.error("Error occured when encoding INT32 Type value with with RAKE", e);
      }
    }
  }

  protected void writeBit(boolean b, ByteArrayOutputStream out) {
    // add bit to buffer
    byteBuffer <<= 1;
    if (b) {
      byteBuffer |= 1;
    }

    // if buffer is full (8 bits), write out as a single byte
    numberLeftInBuffer++;
    if (numberLeftInBuffer == 8 || isReadFinish) {
      clearBuffer(out);
    }
  }

  /** clean all useless value in bufferedValues and set 0. */
  protected void clearBuffer(ByteArrayOutputStream out) {
    if (numberLeftInBuffer == 0) {
      return;
    }
    if (numberLeftInBuffer > 0) {
      byteBuffer <<= (8 - numberLeftInBuffer);
    }
    // write a byte into out
    out.write(byteBuffer);
    numberLeftInBuffer = 0;
    byteBuffer = 0;
  }

  @Override
  public void flush(ByteArrayOutputStream out) throws IOException {
    logger.error("Flush RAKE start");
    if (byteCache.size() > 0) {
      byteCache.writeTo(out);
    }
    byteCache.reset();
    logger.error("Flush RAKE stop");
  }
}
