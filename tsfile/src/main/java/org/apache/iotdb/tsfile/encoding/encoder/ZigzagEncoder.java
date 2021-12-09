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
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Encoder for int value using Zigzag . For more information, see
 * https://gist.github.com/mfuerstenau/ba870a29e16536fdbaba
 */
public class ZigzagEncoder extends Encoder {
  private static final Logger logger = LoggerFactory.getLogger(DictionaryEncoder.class);
  private List<Integer> values;
  byte[] buf = new byte[5];
  private int temp_int = 0;
  private int temp_byte = 0;

  public ZigzagEncoder() {
    super(TSEncoding.ZIGZAG);
    this.values = new ArrayList<>();
    logger.debug("tsfile-encoding ZigzagEncoder: init zigzag encoder");
  }

  /** encoding */
  private byte[] encodeInt(int n) {
    n = (n << 1) ^ (n >> 31);
    int idx = 0;
    if ((n & ~0x7F) != 0) {
      buf[idx++] = (byte) ((n | 0x80) & 0xFF);
      n >>>= 7;
      if (n > 0x7F) {
        buf[idx++] = (byte) ((n | 0x80) & 0xFF);
        n >>>= 7;
        if (n > 0x7F) {
          buf[idx++] = (byte) ((n | 0x80) & 0xFF);
          n >>>= 7;
          if (n > 0x7F) {
            buf[idx++] = (byte) ((n | 0x80) & 0xFF);
            n >>>= 7;
          }
        }
      }
    }
    buf[idx++] = (byte) n;
    return Arrays.copyOfRange(buf, 0, idx);
  }

  public void encode(int value, ByteArrayOutputStream out) {
    values.add(value);
  }

  @Override
  public void flush(ByteArrayOutputStream out) throws IOException {
    // byteCache stores all <encoded-data> and we know its size
    ByteArrayOutputStream byteCache = new ByteArrayOutputStream();
    int len = values.size();
    if (values.size() == 0) {
      return;
    }
    for (int value : values) {
      byte[] bytes = encodeInt(value);
      byteCache.write(bytes, 0, bytes.length);
    }
    ReadWriteForEncodingUtils.writeUnsignedVarInt(byteCache.size(), out);
    ReadWriteForEncodingUtils.writeUnsignedVarInt(len, out);
    System.out.println("before " + len + " after " + byteCache.size());
    out.write(byteCache.toByteArray());
    temp_int += len;
    temp_byte += byteCache.size();
    System.out.println("total before " + temp_int + " total after " + temp_byte);
    reset();
  }

  private void reset() {
    values.clear();
  }

  @Override
  public long getMaxByteSize() {
    if (values == null) {
      return 0;
    }
    // try to caculate max value
    return (long) 8 + values.size() * 5;
  }
}
