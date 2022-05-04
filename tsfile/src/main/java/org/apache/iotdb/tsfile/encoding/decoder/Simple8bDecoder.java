/*
 * Copyright 2022 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.tsfile.encoding.decoder;

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.BytesUtils;

import java.io.IOException;
import java.nio.ByteBuffer;

/** @author Wang Haoyu */
public class Simple8bDecoder extends Decoder {

  private static final int[] ITEMWIDTH = {
    0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 15, 20, 30, 60, Integer.MAX_VALUE
  };
  private static final int[] GROUPSIZE = {
    240, 120, 60, 30, 20, 15, 12, 10, 8, 7, 6, 5, 4, 3, 2, 1, 0
  };

  private final long data[];

  private int readTotalCount = 0;

  private int nextReadIndex = 0;

  public Simple8bDecoder() {
    super(TSEncoding.SIMPLE8B);
    data = new long[240];
  }

  @Override
  public boolean hasNext(ByteBuffer buffer) throws IOException {
    return (nextReadIndex < readTotalCount) || buffer.hasRemaining();
  }

  @Override
  public void reset() {
    nextReadIndex = 0;
    readTotalCount = 0;
  }

  @Override
  public int readInt(ByteBuffer buffer) {
    return (int) readLong(buffer);
  }

  @Override
  public long readLong(ByteBuffer buffer) {
    if (nextReadIndex == readTotalCount) {
      loadBlock(buffer);
      nextReadIndex = 0;
    }
    long value = data[nextReadIndex++];
    return (value >> 1) ^ -(value & 1L);
  }

  private void loadBlock(ByteBuffer buffer) {
    byte[] word = new byte[8];
    buffer.get(word);
    int selector = BytesUtils.bytesToInt(word, 0, 4);
    //        System.out.println(selector);
    readTotalCount = GROUPSIZE[selector];
    for (int i = 0; i < GROUPSIZE[selector]; i++) {
      data[i] = BytesUtils.bytesToLong(word, 4 + i * ITEMWIDTH[selector], ITEMWIDTH[selector]);
    }
  }
}
