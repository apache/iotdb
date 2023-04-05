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

package org.apache.iotdb.tsfile.compress;

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;

public class LZMA2Test {

  private String randomString(int length) {
    StringBuilder builder = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      builder.append((char) (ThreadLocalRandom.current().nextInt(33, 128)));
    }
    return builder.toString();
  }

  @Before
  public void setUp() {}

  @After
  public void tearDown() {}

  @Test
  public void testBytes1() throws IOException {
    ICompressor compressor = new ICompressor.LZMA2Compressor();
    IUnCompressor unCompressor = new IUnCompressor.LZMA2UnCompressor();
    int n = 500000;
    String input = randomString(n);
    byte[] uncom = input.getBytes(StandardCharsets.UTF_8);
    byte[] compressed = compressor.compress(uncom);
    byte[] uncompressed = unCompressor.uncompress(compressed);

    Assert.assertArrayEquals(uncom, uncompressed);
  }

}