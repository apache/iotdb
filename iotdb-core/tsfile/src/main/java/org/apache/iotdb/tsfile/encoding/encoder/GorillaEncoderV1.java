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

import java.io.ByteArrayOutputStream;

/**
 * Gorilla encoding. For more information about how it works, please see
 * http://www.vldb.org/pvldb/vol8/p1816-teller.pdf
 */
public abstract class GorillaEncoderV1 extends Encoder {

  // flag to indicate whether the first value is saved
  protected boolean flag;
  protected int leadingZeroNum;
  protected int tailingZeroNum;
  // 8-bit buffer of bits to write out
  protected byte buffer;
  // number of bits remaining in buffer
  protected int numberLeftInBuffer;

  protected GorillaEncoderV1() {
    super(TSEncoding.GORILLA_V1);
    this.flag = false;
  }

  protected void writeBit(boolean b, ByteArrayOutputStream out) {
    // add bit to buffer
    buffer <<= 1;
    if (b) {
      buffer |= 1;
    }

    // if buffer is full (8 bits), write out as a single byte
    numberLeftInBuffer++;
    if (numberLeftInBuffer == 8) {
      clearBuffer(out);
    }
  }

  protected void writeBit(int i, ByteArrayOutputStream out) {
    writeBit(i != 0, out);
  }

  protected void writeBit(long i, ByteArrayOutputStream out) {
    writeBit(i != 0, out);
  }

  protected void clearBuffer(ByteArrayOutputStream out) {
    if (numberLeftInBuffer == 0) {
      return;
    }
    if (numberLeftInBuffer > 0) {
      buffer <<= (8 - numberLeftInBuffer);
    }
    out.write(buffer);
    numberLeftInBuffer = 0;
    buffer = 0;
  }

  protected void reset() {
    this.flag = false;
    this.numberLeftInBuffer = 0;
    this.buffer = 0;
  }
}
