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

import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.encoding.encoder.LongZigzagEncoder;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class LongZigzagDecoderTest {
  private static final Logger logger = LoggerFactory.getLogger(LongZigzagDecoderTest.class);
  private List<Long> longList;
  private Random rand = new Random();
  private List<Long> randomLongList;

  @Before
  public void setUp() {
    longList = new ArrayList<>();
    int int_num = 10000;
    randomLongList = new ArrayList<>();
    for (int i = 0; i < int_num; i++) {
      longList.add((long) (i + ((long) 1 << 31)));
    }
    for (int i = 0; i < int_num; i++) {
      randomLongList.add(rand.nextLong());
    }
  }

  @After
  public void tearDown() {
    randomLongList.clear();
    longList.clear();
  }

  @Test
  public void testZigzagReadLong() throws Exception {
    for (int i = 1; i < 10; i++) {
      testLong(longList, false, i);
      testLong(randomLongList, false, i);
    }
  }

  private void testLong(List<Long> list, boolean isDebug, int repeatCount) throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Encoder encoder = new LongZigzagEncoder();
    for (int i = 0; i < repeatCount; i++) {
      for (long value : list) {
        encoder.encode(value, baos);
      }
      encoder.flush(baos);
    }

    ByteBuffer bais = ByteBuffer.wrap(baos.toByteArray());
    Decoder decoder = new LongZigzagDecoder();
    for (int i = 0; i < repeatCount; i++) {
      for (long value : list) {
        long value_ = decoder.readLong(bais);
        if (isDebug) {
          logger.debug("{} // {}", value_, value);
        }
        assertEquals(value, value_);
      }
    }
  }
}
