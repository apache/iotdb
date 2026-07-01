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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Encoder for INT64 values. This keeps SUBCOLUMN schema support lossless for long series. */
public class LongSubColumnEncoder extends Encoder {

  private final List<Long> values = new ArrayList<>();

  public LongSubColumnEncoder() {
    super(TSEncoding.SUBCOLUMN);
  }

  @Override
  public void encode(long value, ByteArrayOutputStream out) {
    values.add(value);
  }

  @Override
  public void flush(ByteArrayOutputStream out) throws IOException {
    ReadWriteForEncodingUtils.writeUnsignedVarInt(values.size(), out);
    for (long value : values) {
      writeLong(out, value);
    }
    values.clear();
  }

  private static void writeLong(ByteArrayOutputStream out, long value) {
    for (int shift = 56; shift >= 0; shift -= 8) {
      out.write((int) ((value >>> shift) & 0xFF));
    }
  }

  @Override
  public int getOneItemMaxSize() {
    return Long.BYTES;
  }

  @Override
  public long getMaxByteSize() {
    return (long) values.size() * Long.BYTES + 8;
  }
}
