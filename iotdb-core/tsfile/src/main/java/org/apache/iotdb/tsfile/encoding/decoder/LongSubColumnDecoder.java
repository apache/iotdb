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

import java.nio.ByteBuffer;

/** Decoder for INT64 values encoded by {@link org.apache.iotdb.tsfile.encoding.encoder.LongSubColumnEncoder}. */
public class LongSubColumnDecoder extends Decoder {

  private long[] values;
  private int index;

  public LongSubColumnDecoder() {
    super(TSEncoding.SUBCOLUMN);
  }

  @Override
  public long readLong(ByteBuffer buffer) {
    ensureLoaded(buffer);
    return values[index++];
  }

  @Override
  public boolean hasNext(ByteBuffer buffer) {
    ensureLoaded(buffer);
    return values != null && index < values.length;
  }

  @Override
  public void reset() {
    values = null;
    index = 0;
  }

  private void ensureLoaded(ByteBuffer buffer) {
    if (values != null || !buffer.hasRemaining()) {
      return;
    }
    int count = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
    values = new long[count];
    for (int i = 0; i < count; i++) {
      values[i] = buffer.getLong();
    }
  }
}
