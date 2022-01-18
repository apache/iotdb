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
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class ZigzagDecoder extends Decoder {
  private static final Logger logger = LoggerFactory.getLogger(ZigzagDecoder.class);

  /** how many bytes for all encoded data in input stream. */
  private int length;

  /** number of encoded data. */
  private int number;

  /** number of data left for reading in current buffer. */
  private int currentCount;

  /**
   * each time decoder receives a inputstream, decoder creates a buffer to save all encoded data.
   */
  private ByteBuffer byteCache;

  public ZigzagDecoder() {
    super(TSEncoding.ZIGZAG);
    this.reset();
    logger.debug("tsfile-encoding ZigzagDecoder: init bitmap decoder");
  }

  /** decoding */
  @Override
  public int readInt(ByteBuffer buffer) {
    if (currentCount == 0) {
      reset();
      getLengthAndNumber(buffer);
      currentCount = number;
    }
    int n = ReadWriteForEncodingUtils.readUnsignedVarInt(byteCache);
    currentCount--;
    return (n >>> 1) ^ -(n & 1); // back to two's-complement
  }

  private void getLengthAndNumber(ByteBuffer buffer) {
    this.length = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
    this.number = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
    // TODO maybe this.byteCache = buffer is faster, but not safe
    byte[] tmp = new byte[length];
    buffer.get(tmp, 0, length);
    this.byteCache = ByteBuffer.wrap(tmp);
  }

  @Override
  public boolean hasNext(ByteBuffer buffer) {
    if (currentCount > 0 || buffer.remaining() > 0) {
      return true;
    }
    return false;
  }

  @Override
  public void reset() {
    this.length = 0;
    this.number = 0;
    this.currentCount = 0;
    if (this.byteCache == null) {
      this.byteCache = ByteBuffer.allocate(0);
    } else {
      this.byteCache.position(0);
    }
  }
}
