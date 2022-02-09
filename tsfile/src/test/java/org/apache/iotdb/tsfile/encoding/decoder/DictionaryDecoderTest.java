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

import org.apache.iotdb.tsfile.encoding.encoder.DictionaryEncoder;
import org.apache.iotdb.tsfile.utils.Binary;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DictionaryDecoderTest {
  private DictionaryEncoder encoder = new DictionaryEncoder();
  private DictionaryDecoder decoder = new DictionaryDecoder();
  private ByteArrayOutputStream baos = new ByteArrayOutputStream();

  @Test
  public void testSingle() {
    testAll("a");
    testAll("b");
    testAll("c");
  }

  @Test
  public void testAllUnique() {
    testAll("a", "b", "c");
    testAll("x", "o", "q");
    testAll(",", ".", "c", "b", "e");
  }

  @Test
  public void testAllSame() {
    testAll("a", "a", "a");
    testAll("b", "b", "b");
  }

  @Test
  public void testMixed() {
    // all characters
    String[] allChars = new String[256];
    allChars[0] = "" + (char) ('a' + 1);
    for (int i = 0; i < 256; i++) {
      allChars[i] = "" + (char) (i) + (char) (i) + (char) (i);
    }
    testAll(allChars);
  }

  private void testAll(String... all) {
    for (String s : all) {
      encoder.encode(new Binary(s), baos);
    }
    encoder.flush(baos);

    ByteBuffer out = ByteBuffer.wrap(baos.toByteArray());

    for (String s : all) {
      assertTrue(decoder.hasNext(out));
      assertEquals(s, decoder.readBinary(out).getStringValue());
    }

    decoder.reset();
    baos.reset();
  }
}
