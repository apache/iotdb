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
package org.apache.iotdb.db.utils;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TabletDecoderTest {

  @Test
  public void testDecodeUncompressedPlainTablet() {
    TSDataType[] dataTypes = {
      TSDataType.BOOLEAN,
      TSDataType.INT32,
      TSDataType.INT64,
      TSDataType.FLOAT,
      TSDataType.DOUBLE,
      TSDataType.TEXT
    };
    TabletDecoder decoder =
        new TabletDecoder(
            CompressionType.UNCOMPRESSED,
            dataTypes,
            Collections.nCopies(dataTypes.length + 1, TSEncoding.PLAIN),
            2);

    ByteBuffer timeBuffer = ByteBuffer.allocate(2 * Long.BYTES);
    timeBuffer.putLong(1L).putLong(2L).flip();
    assertArrayEquals(new long[] {1L, 2L}, decoder.decodeTime(timeBuffer));
    assertFalse(timeBuffer.hasRemaining());

    ByteBuffer valueBuffer = ByteBuffer.allocate(128);
    valueBuffer.put((byte) 1).put((byte) 0);
    valueBuffer.putInt(3).putInt(4);
    valueBuffer.putLong(5L).putLong(6L);
    valueBuffer.putFloat(7.0F).putFloat(8.0F);
    valueBuffer.putDouble(9.0D).putDouble(10.0D);
    valueBuffer.putInt(1).put((byte) 'a').putInt(1).put((byte) 'b');
    valueBuffer.flip();

    Pair<Object[], ByteBuffer> result = decoder.decodeValues(valueBuffer);

    assertArrayEquals(new boolean[] {true, false}, (boolean[]) result.left[0]);
    assertArrayEquals(new int[] {3, 4}, (int[]) result.left[1]);
    assertArrayEquals(new long[] {5L, 6L}, (long[]) result.left[2]);
    assertArrayEquals(new float[] {7.0F, 8.0F}, (float[]) result.left[3], 0.0F);
    assertArrayEquals(new double[] {9.0D, 10.0D}, (double[]) result.left[4], 0.0D);
    assertArrayEquals(
        new Binary[] {new Binary(new byte[] {'a'}), new Binary(new byte[] {'b'})},
        (Binary[]) result.left[5]);
    assertEquals(valueBuffer, result.right);
    assertFalse(result.right.hasRemaining());
  }
}
