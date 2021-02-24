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

import org.apache.iotdb.tsfile.exception.encoding.TsFileDecodingException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

public class PlainDecoder extends Decoder {

  public PlainDecoder() {
    super(TSEncoding.PLAIN);
  }

  @Override
  public boolean readBoolean(ByteBuffer buffer) {
    return buffer.get() != 0;
  }

  @Override
  public short readShort(ByteBuffer buffer) {
    return buffer.getShort();
  }

  @Override
  public int readInt(ByteBuffer buffer) {
    return ReadWriteForEncodingUtils.readVarInt(buffer);
  }

  @Override
  public long readLong(ByteBuffer buffer) {
    return buffer.getLong();
  }

  @Override
  public float readFloat(ByteBuffer buffer) {
    return buffer.getFloat();
  }

  @Override
  public double readDouble(ByteBuffer buffer) {
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
  public boolean hasNext(ByteBuffer buffer) {
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
