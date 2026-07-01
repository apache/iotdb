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

/** Decoder for INT32 values encoded by {@link org.apache.iotdb.tsfile.encoding.encoder.IntSubColumnEncoder}. */
public class IntSubColumnDecoder extends Decoder {

  private int[] values;
  private int index;

  public IntSubColumnDecoder() {
    super(TSEncoding.SUBCOLUMN);
  }

  @Override
  public int readInt(ByteBuffer buffer) {
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
    int blockSize = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
    values = new int[count];
    int offset = 0;
    while (offset < count) {
      int length = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
      int min = buffer.getInt();
      int bitWidth = buffer.get() & 0xFF;
      int bytesPerPlane = (length + Byte.SIZE - 1) / Byte.SIZE;
      for (int bit = 0; bit < bitWidth; bit++) {
        for (int byteIndex = 0; byteIndex < bytesPerPlane; byteIndex++) {
          int mask = buffer.get() & 0xFF;
          for (int j = 0; j < Byte.SIZE; j++) {
            int localIndex = (byteIndex << 3) + j;
            if (localIndex < length && ((mask >>> j) & 1) != 0) {
              values[offset + localIndex] |= 1 << bit;
            }
          }
        }
      }
      for (int i = 0; i < length; i++) {
        values[offset + i] += min;
      }
      offset += Math.min(blockSize, length);
    }
  }
}
