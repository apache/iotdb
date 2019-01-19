/**
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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xerial.snappy.Snappy;

/**
 *
 * @author kangrong
 *
 */
public class CompressTest {

  private final String inputString = "Hello snappy-java! Snappy-java is a JNI-based wrapper of "
      + "Snappy, a fast compressor/decompressor.";

  @Before
  public void setUp() {
  }

  @After
  public void tearDown() {
  }

  @Test
  public void snappyCompressorTest1() throws IOException {
    PublicBAOS out = new PublicBAOS();
    out.write(inputString.getBytes("UTF-8"));
    Compressor.SnappyCompressor compressor = new Compressor.SnappyCompressor();
    UnCompressor.SnappyUnCompressor unCompressor = new UnCompressor.SnappyUnCompressor();
    byte[] compressed = compressor.compress(out.getBuf());
    byte[] uncompressed = unCompressor.uncompress(compressed);
    String result = new String(uncompressed, "UTF-8");
    assertEquals(inputString, result);
  }

  @Test
  public void snappyCompressorTest2() throws IOException {
    PublicBAOS out = new PublicBAOS();
    out.write(inputString.getBytes("UTF-8"));
    Compressor.SnappyCompressor compressor = new Compressor.SnappyCompressor();
    UnCompressor.SnappyUnCompressor unCompressor = new UnCompressor.SnappyUnCompressor();
    byte[] compressed = new byte[compressor.getMaxBytesForCompression(out.size())];
    int size = compressor.compress(out.getBuf(), 0, out.size(), compressed);
    byte[] bytes = Arrays.copyOfRange(compressed, 0, size);
    byte[] uncompressed = unCompressor.uncompress(bytes);
    String result = new String(uncompressed, "UTF-8");
    assertEquals(inputString, result);
  }

  @Test
  public void snappyTest() throws IOException {
    byte[] compressed = Snappy.compress(inputString.getBytes("UTF-8"));
    byte[] uncompressed = Snappy.uncompress(compressed);

    String result = new String(uncompressed, "UTF-8");
    assertEquals(inputString, result);
  }

}
