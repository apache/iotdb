/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

/**
 * @author Zhang Jinrui
 */
public class PlainDecoder extends Decoder {

  private static final Logger LOGGER = LoggerFactory.getLogger(PlainDecoder.class);
  public EndianType endianType;

  public PlainDecoder(EndianType endianType) {
    super(TSEncoding.PLAIN);
    this.endianType = endianType;
  }

  @Override
  public boolean readBoolean(ByteBuffer buffer) {
    int ch1 = ReadWriteIOUtils.read(buffer);
    if (ch1 == 0) {
      return false;
    } else {
      return true;
    }
  }

  @Override
  public short readShort(ByteBuffer buffer) {
    int ch1 = ReadWriteIOUtils.read(buffer);
    int ch2 = ReadWriteIOUtils.read(buffer);
    if (this.endianType == EndianType.LITTLE_ENDIAN) {
      return (short) ((ch2 << 8) + ch1);
    } else {
      LOGGER.error(
          "tsfile-encoding PlainEncoder: current version does not support short value decoding");
    }
    return -1;
  }

  @Override
  public int readInt(ByteBuffer buffer) {
    int ch1 = ReadWriteIOUtils.read(buffer);
    int ch2 = ReadWriteIOUtils.read(buffer);
    int ch3 = ReadWriteIOUtils.read(buffer);
    int ch4 = ReadWriteIOUtils.read(buffer);
    if (this.endianType == EndianType.LITTLE_ENDIAN) {
      return ch1 + (ch2 << 8) + (ch3 << 16) + (ch4 << 24);
    } else {
      LOGGER.error(
          "tsfile-encoding PlainEncoder: current version does not support int value encoding");
    }
    return -1;
  }

  @Override
  public long readLong(ByteBuffer buffer) {
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

  @Override
  public float readFloat(ByteBuffer buffer) {
    return Float.intBitsToFloat(readInt(buffer));
  }

  @Override
  public double readDouble(ByteBuffer buffer) {
    return Double.longBitsToDouble(readLong(buffer));
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
  }
}
