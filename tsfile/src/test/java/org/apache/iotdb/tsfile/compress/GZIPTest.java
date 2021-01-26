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

/**
 * @author yuqi
 * @mail yuqi4733@gmail.com
 * @description your description
 * @time 13/12/20 下午10:08
 **/
public class GZIPTest {
  private String randomString(int length) {
    StringBuilder builder = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      builder.append((char) (ThreadLocalRandom.current().nextInt(33, 128)));
    }
    return builder.toString();
  }

  @Before
  public void setUp() {
  }

  @After
  public void tearDown() {
  }

  @Test
  public void testBytes() throws IOException {
    int n = 500000;
    String input = randomString(n);
    byte[] uncom = input.getBytes(StandardCharsets.UTF_8);
    long time = System.nanoTime();
    byte[] compressed = ICompressor.GZIPCompress.compress(uncom);
    System.out.println("compression time cost:" + ((System.nanoTime() - time)) / 1000 / 1000);
    System.out.println("ratio: " + (double) compressed.length / uncom.length);
    time = System.nanoTime();
    byte[] uncompressed = ICompressor.GZIPCompress.uncompress(compressed);
    System.out.println("decompression time cost:" + ((System.nanoTime() - time)) / 1000 / 1000);

    Assert.assertArrayEquals(uncom, uncompressed);
  }

  @Test
  public void testByteBuffer() throws IOException {
    for (int i = 1; i < 500000; i+= 1000) {
      String input = randomString(i);
      //String input = "this is test";
      ByteBuffer source = ByteBuffer.allocateDirect(input.getBytes().length);
      source.put(input.getBytes());
      source.flip();

      ICompressor.GZIPCompressor compressor = new ICompressor.GZIPCompressor();
      long time = System.currentTimeMillis();
      ByteBuffer compressed = ByteBuffer.allocateDirect(Math.max(source.remaining() * 3 + 1, 28 + source.remaining()));
      int compressSize = compressor.compress(source, compressed);
      System.out.println("compression time cost:" + (System.currentTimeMillis() - time));
      System.out.println("ratio: " + (double) compressSize / i);

      IUnCompressor.GZIPUnCompressor unCompressor = new IUnCompressor.GZIPUnCompressor();
      time = System.currentTimeMillis();
      ByteBuffer uncompressedByteBuffer = ByteBuffer.allocateDirect(compressed.remaining() + 28 * 2);
      compressed.flip();
      unCompressor.uncompress(compressed, uncompressedByteBuffer);
      System.out.println("decompression time cost:" + (System.currentTimeMillis() - time));

      uncompressedByteBuffer.flip();
      String afterDecode = ReadWriteIOUtils.readStringFromDirectByteBuffer(uncompressedByteBuffer);
      assert input.equals(afterDecode);
    }
  }
}
