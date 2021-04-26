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
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;

public class SnappyTest {

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
  public void testBytes() throws IOException {
    int n = 500000;
    String input = randomString(n);
    byte[] uncom = input.getBytes(StandardCharsets.UTF_8);
    long time = System.nanoTime();
    byte[] compressed = Snappy.compress(uncom);
    System.out.println("compression time cost:" + ((System.nanoTime() - time)) / 1000 / 1000);
    System.out.println("ratio: " + (double) compressed.length / uncom.length);
    time = System.nanoTime();
    byte[] uncompressed = Snappy.uncompress(compressed);
    System.out.println("decompression time cost:" + ((System.nanoTime() - time)) / 1000 / 1000);

    Assert.assertArrayEquals(uncom, uncompressed);
  }

  @Test
  public void testByteBuffer() throws IOException {
    String input = randomString(5000);
    ByteBuffer source = ByteBuffer.allocateDirect(input.getBytes().length);
    source.put(input.getBytes());
    source.flip();

    long time = System.currentTimeMillis();
    ByteBuffer compressed =
        ByteBuffer.allocateDirect(Snappy.maxCompressedLength(source.remaining()));
    Snappy.compress(source, compressed);
    System.out.println("compression time cost:" + (System.currentTimeMillis() - time));
    Snappy.uncompressedLength(compressed);
    time = System.currentTimeMillis();
    ByteBuffer uncompressedByteBuffer =
        ByteBuffer.allocateDirect(Snappy.uncompressedLength(compressed) + 1);
    Snappy.uncompress(compressed, uncompressedByteBuffer);
    System.out.println("decompression time cost:" + (System.currentTimeMillis() - time));
    assert input.equals(ReadWriteIOUtils.readStringFromDirectByteBuffer(uncompressedByteBuffer));
  }
}
