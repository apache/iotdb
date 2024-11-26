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
package org.apache.tsfile.compress;

import org.apache.tsfile.compress.ICompressor.LZ4Compressor;
import org.apache.tsfile.compress.IUnCompressor.LZ4UnCompressor;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;

public class LZ4Test {

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
    String input = randomString(2000000);
    byte[] uncom = input.getBytes(StandardCharsets.UTF_8);
    long time = System.currentTimeMillis();
    ICompressor compressor = new LZ4Compressor();

    byte[] compressed = compressor.compress(uncom);
    System.out.println("compression time cost:" + (System.currentTimeMillis() - time));
    time = System.currentTimeMillis();
    System.out.println("ratio: " + (double) compressed.length / uncom.length);

    IUnCompressor unCompressor = new LZ4UnCompressor();
    byte[] uncompressed = new byte[uncom.length];
    unCompressor.uncompress(compressed, 0, compressed.length, uncompressed, 0);
    System.out.println("decompression time cost:" + (System.currentTimeMillis() - time));

    Assert.assertArrayEquals(uncom, uncompressed);
  }

  @Test
  public void testBytes2() throws IOException {
    LZ4Compressor compressor = new LZ4Compressor();
    IUnCompressor.LZ4UnCompressor unCompressor = new IUnCompressor.LZ4UnCompressor();

    int n = 500000;
    String input = randomString(n);
    byte[] uncom = input.getBytes(StandardCharsets.UTF_8);
    byte[] compressed = compressor.compress(uncom, 0, uncom.length);
    // length should be same
    Assert.assertEquals(compressor.compress(uncom).length, compressed.length);
    byte[] uncompressed = unCompressor.uncompress(compressed);
    Assert.assertArrayEquals(uncom, uncompressed);
  }

  @Test
  public void testBytes3() throws IOException {
    LZ4Compressor compressor = new LZ4Compressor();
    IUnCompressor.LZ4UnCompressor unCompressor = new IUnCompressor.LZ4UnCompressor();

    int n = 500000;
    int offset = 100;
    String input = randomString(n);
    byte[] origin = input.getBytes(StandardCharsets.UTF_8);
    byte[] compressed = new byte[origin.length * 2];
    int compressedLength = compressor.compress(origin, 0, origin.length, compressed);
    System.arraycopy(compressed, 0, compressed, offset, compressedLength);
    for (int i = 0; i < offset; i++) {
      compressed[i] = 0;
    }

    byte[] uncompressed = new byte[origin.length];
    unCompressor.uncompress(compressed, offset, compressedLength, uncompressed, 0);
    Assert.assertArrayEquals(origin, uncompressed);
  }

  @Test
  public void testByteBuffer() throws IOException {
    for (int i = 1; i < 500000; i += 100000) {
      String input = randomString(i);
      ByteBuffer source = ByteBuffer.allocateDirect(input.getBytes().length);
      source.put(input.getBytes());
      source.flip();

      ICompressor.LZ4Compressor compressor = new ICompressor.LZ4Compressor();
      ByteBuffer compressed =
          ByteBuffer.allocateDirect(Math.max(source.remaining() * 3 + 1, 28 + source.remaining()));
      compressor.compress(source, compressed);

      IUnCompressor.LZ4UnCompressor unCompressor = new IUnCompressor.LZ4UnCompressor();
      ByteBuffer uncompressedByteBuffer =
          ByteBuffer.allocateDirect(compressed.remaining() + 28 * 2);
      compressed.flip();
      unCompressor.uncompress(compressed, uncompressedByteBuffer);

      uncompressedByteBuffer.flip();
      String afterDecode = ReadWriteIOUtils.readStringFromDirectByteBuffer(uncompressedByteBuffer);
      assert input.equals(afterDecode);
    }
  }
}
