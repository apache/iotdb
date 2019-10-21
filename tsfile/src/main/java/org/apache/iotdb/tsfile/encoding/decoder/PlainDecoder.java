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

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import org.apache.iotdb.tsfile.encoding.common.EndianType;
import org.apache.iotdb.tsfile.exception.encoding.TsFileDecodingException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PlainDecoder extends Decoder {

  private static final Logger logger = LoggerFactory.getLogger(PlainDecoder.class);
  private EndianType endianType;

  public EndianType getEndianType() {
    return endianType;
  }

  public void setEndianType(EndianType endianType) {
    this.endianType = endianType;
  }

  public PlainDecoder(EndianType endianType) {
    super(TSEncoding.PLAIN);
    this.endianType = endianType;
  }

  @Override
  public boolean readBoolean(ByteBuffer buffer) {
    return buffer.get() != 0;
  }

  @Override
  public short readShort(ByteBuffer buffer) {
    if (this.endianType == EndianType.LITTLE_ENDIAN) {
      int ch1 = ReadWriteIOUtils.read(buffer);
      int ch2 = ReadWriteIOUtils.read(buffer);
      return (short) (ch1 + (ch2 << 8));
    }
    return buffer.getShort();
  }

  @Override
  public int readInt(ByteBuffer buffer) {
    if (this.endianType == EndianType.LITTLE_ENDIAN) {
      int ch1 = ReadWriteIOUtils.read(buffer);
      int ch2 = ReadWriteIOUtils.read(buffer);
      int ch3 = ReadWriteIOUtils.read(buffer);
      int ch4 = ReadWriteIOUtils.read(buffer);
      return ch1 + (ch2 << 8) + (ch3 << 16) + (ch4 << 24);
    }
    return buffer.getInt();
  }

  @Override
  public long readLong(ByteBuffer buffer) {
    if (this.endianType == EndianType.LITTLE_ENDIAN) {
      int[] buf = new int[8];
      for (int i = 0; i < 8; i++) {
        buf[i] = ReadWriteIOUtils.read(buffer);
      }
      Long res = 0L;
      for (int i = 0; i < 8; i++) {
        res += ((long) buf[i] << (i * 8));
      }
      return res;
    }
    return buffer.getLong();
  }

  @Override
  public float readFloat(ByteBuffer buffer) {
	if (this.endianType == EndianType.LITTLE_ENDIAN) {
	  return Float.intBitsToFloat(readInt(buffer));
	}
    return buffer.getFloat();
  }

  @Override
  public double readDouble(ByteBuffer buffer) {
    if (this.endianType == EndianType.LITTLE_ENDIAN) {
      return Double.longBitsToDouble(readLong(buffer));
    }
    return buffer.getDouble();
  }

  @Override
  public Binary readBinary(ByteBuffer buffer) {
    int length = readInt(buffer);
    byte[] buf = new byte[length];
    buffer.get(buf, 0, buf.length);
    return new Binary(buf);
  }

  @Override
  public boolean hasNext(ByteBuffer buffer) throws IOException {
    return buffer.remaining() > 0;
  }

  @Override
  public BigDecimal readBigDecimal(ByteBuffer buffer) {
    throw new TsFileDecodingException("Method readBigDecimal is not supproted by PlainDecoder");
  }

  @Override
  public void reset() {
    // do nothing
  }
}
