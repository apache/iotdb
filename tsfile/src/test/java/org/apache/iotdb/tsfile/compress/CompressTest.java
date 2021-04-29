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

import org.apache.iotdb.tsfile.utils.PublicBAOS;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class CompressTest {

  private final String inputString =
      "Hello snappy-java! Snappy-java is a JNI-based wrapper of "
          + "Snappy, a fast compressor/decompressor.";

  @Before
  public void setUp() {}

  @After
  public void tearDown() {}

  @Test
  public void snappyCompressorTest1() throws IOException {
    PublicBAOS out = new PublicBAOS();
    out.write(inputString.getBytes(StandardCharsets.UTF_8));
    ICompressor.SnappyCompressor compressor = new ICompressor.SnappyCompressor();
    IUnCompressor.SnappyUnCompressor unCompressor = new IUnCompressor.SnappyUnCompressor();
    byte[] compressed = compressor.compress(out.getBuf());
    byte[] uncompressed = unCompressor.uncompress(compressed);
    String result = new String(uncompressed, StandardCharsets.UTF_8);
    assertEquals(inputString, result);
  }

  @Test
  public void snappyCompressorTest2() throws IOException {
    PublicBAOS out = new PublicBAOS();
    out.write(inputString.getBytes(StandardCharsets.UTF_8));
    ICompressor.SnappyCompressor compressor = new ICompressor.SnappyCompressor();
    IUnCompressor.SnappyUnCompressor unCompressor = new IUnCompressor.SnappyUnCompressor();
    byte[] compressed = new byte[compressor.getMaxBytesForCompression(out.size())];
    int size = compressor.compress(out.getBuf(), 0, out.size(), compressed);
    byte[] bytes = Arrays.copyOfRange(compressed, 0, size);
    byte[] uncompressed = unCompressor.uncompress(bytes);
    String result = new String(uncompressed, StandardCharsets.UTF_8);
    assertEquals(inputString, result);
  }

  @Test
  public void snappyTest() throws IOException {
    byte[] compressed = Snappy.compress(inputString.getBytes(StandardCharsets.UTF_8));
    byte[] uncompressed = Snappy.uncompress(compressed);

    String result = new String(uncompressed, StandardCharsets.UTF_8);
    assertEquals(inputString, result);
  }

  @Test
  public void lz4CompressorTest1() throws IOException {
    PublicBAOS out = new PublicBAOS();
    out.write(inputString.getBytes(StandardCharsets.UTF_8));
    ICompressor compressor = new ICompressor.IOTDBLZ4Compressor();
    IUnCompressor unCompressor = new IUnCompressor.LZ4UnCompressor();
    byte[] compressed = compressor.compress(out.getBuf());
    byte[] uncompressed = unCompressor.uncompress(compressed);
    String result = new String(uncompressed, StandardCharsets.UTF_8);
    assertEquals(inputString, result);
  }

  @Test
  public void lz4CompressorTest2() throws IOException {
    PublicBAOS out = new PublicBAOS();
    out.write(inputString.getBytes(StandardCharsets.UTF_8));
    ICompressor compressor = new ICompressor.IOTDBLZ4Compressor();
    IUnCompressor unCompressor = new IUnCompressor.LZ4UnCompressor();
    byte[] compressed = new byte[compressor.getMaxBytesForCompression(out.size())];
    int size = compressor.compress(out.getBuf(), 0, out.size(), compressed);
    byte[] bytes = Arrays.copyOfRange(compressed, 0, size);
    byte[] uncompressed = unCompressor.uncompress(bytes);
    String result = new String(uncompressed, StandardCharsets.UTF_8);
    assertEquals(inputString, result);
  }
}
