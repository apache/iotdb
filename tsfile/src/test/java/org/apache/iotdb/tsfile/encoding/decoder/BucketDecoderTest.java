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

import org.apache.iotdb.tsfile.encoding.encoder.BucketEncoder;
import org.apache.iotdb.tsfile.encoding.encoder.HuffmanEncoderV2;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BucketDecoderTest {
  private BucketEncoder encoder = new BucketEncoder();
  private BucketDecoder decoder = new BucketDecoder();
  private ByteArrayOutputStream baos = new ByteArrayOutputStream();

  @Test
  public void testSingle() {
    int a[] = {1};
    testAll(a);
    a[0] = 2;
    testAll(a);
    a[0] = 1023;
    testAll(a);
  }

  @Test
  public void testAllUnique() {
    int a[] = {1, 2, 3};
    int b[] = {52, 123, 432};
    int c[] = {54, 76, 42, 27, 35};
    testAll(a);
    testAll(b);
    testAll(c);
  }

  @Test
  public void testAllSame() {
    int a[] = {20, 20, 20};
    int b[] = {166, 166, 166};
    testAll(a);
    testAll(b);
  }

  @Test
  public void testMixed() {
    // all characters
    int[] allChars = new int[1000000];
    for (int i = 0; i < 1000000; i++) {
      allChars[i] = (int)(1000000 * Math.random());
    }
    testAll(allChars);
  }

  private void testAll(int[] all) {
    for (int s : all) {
      encoder.encode(s, baos);
    }
    encoder.flush(baos);

    ByteBuffer out = ByteBuffer.wrap(baos.toByteArray());

    for (int s : all) {
      assertTrue(decoder.hasNext(out));
      int b = decoder.readInt(out);
      assertEquals(s, b);
    }

    decoder.reset();
    baos.reset();
  }
}
