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
package org.apache.iotdb.tsfile.encoding.encoder;

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.BytesUtils;

import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/** @author Wang Haoyu */
public class Simple8bEncoder extends Encoder {

  private static final int[] ITEMWIDTH = {
    0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 15, 20, 30, 60, Integer.MAX_VALUE
  };
  private static final int[] GROUPSIZE = {
    240, 120, 60, 30, 20, 15, 12, 10, 8, 7, 6, 5, 4, 3, 2, 1, 0
  };
  private static final int[] WASTED = {60, 60, 0, 0, 0, 0, 0, 0, 4, 4, 0, 0, 0, 0, 0, 0};
  private static final int LEN = 240;
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(Simple8bEncoder.class);
  private final long[] dataBuffer;
  protected int startIndex = 0, endIndex = 0, cnt = 0, selector = 16, maxWidth = 0;

  public Simple8bEncoder() {
    super(TSEncoding.SIMPLE8B);
    this.dataBuffer = new long[250];
  }

  @Override
  public void encode(long value, ByteArrayOutputStream out) {
    value = (value << 1) ^ (value >> 63);
    cnt++;
    maxWidth = Math.max(maxWidth, getValueWidth(value));
    dataBuffer[endIndex] = value;
    endIndex = (endIndex + 1) % LEN;
    //        System.out.println("Current selector: "+ selector);
    if (cnt >= GROUPSIZE[selector - 1]) {
      if (maxWidth > ITEMWIDTH[selector - 1]) { // 使用当前的selector
        write(out);
      } else { // 使用新的selector
        selector--;
        if (selector == 0) {
          write(out);
        }
      }
    }
  }

  private void write(ByteArrayOutputStream out) {
    //        System.out.println("Selector: " + selector);
    byte[] word = new byte[8];
    BytesUtils.intToBytes(selector, word, 0, 4);
    for (int i = 0; i < GROUPSIZE[selector]; i++) {
      //            System.out.println(dataBuffer[startIndex]);
      BytesUtils.longToBytes(
          dataBuffer[startIndex], word, 4 + i * ITEMWIDTH[selector], ITEMWIDTH[selector]);
      startIndex = (startIndex + 1) % LEN;
    }
    BytesUtils.longToBytes(0, word, 64 - WASTED[selector], WASTED[selector]);
    cnt -= GROUPSIZE[selector];
    update();
    try {
      out.write(word);
    } catch (IOException e) {
      logger.error("Write data to stream failed!", e);
    }
  }

  private void update() {
    maxWidth = 0;
    selector = 16;
    int k = startIndex;
    for (int i = 0; i < cnt; i++) {
      maxWidth = Math.max(maxWidth, getValueWidth(dataBuffer[k]));
      k = (k + 1) % LEN;
      if (i + 1 == GROUPSIZE[selector - 1]) {
        if (maxWidth <= ITEMWIDTH[selector - 1]) {
          selector--;
        } else {
          break;
        }
      }
    }
  }

  @Override
  public void encode(int value, ByteArrayOutputStream out) {
    encode((long) value, out);
  }

  @Override
  public void flush(ByteArrayOutputStream out) throws IOException {
    while (cnt > 0) {
      write(out);
    }
  }

  /**
   * Get the valid bit width of x
   *
   * @param x
   * @return valid bit width
   */
  private int getValueWidth(long x) {
    return 64 - Long.numberOfLeadingZeros(x);
  }

  @Override
  public int getOneItemMaxSize() {
    return 8;
  }

  @Override
  public long getMaxByteSize() {
    return 8 * cnt;
  }
}
