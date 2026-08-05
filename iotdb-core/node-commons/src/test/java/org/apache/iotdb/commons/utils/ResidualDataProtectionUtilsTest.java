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

package org.apache.iotdb.commons.utils;

import org.apache.iotdb.commons.conf.CommonDescriptor;

import org.apache.tsfile.utils.Binary;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ResidualDataProtectionUtilsTest {

  private static final String TEST_SOURCE = ResidualDataProtectionUtilsTest.class.getSimpleName();

  private boolean originalEnableSecureErase;

  @Before
  public void setUp() {
    originalEnableSecureErase = CommonDescriptor.getInstance().getConfig().isEnableSecureErase();
  }

  @After
  public void tearDown() {
    CommonDescriptor.getInstance().getConfig().setEnableSecureErase(originalEnableSecureErase);
  }

  @Test
  public void testEraseArraysWhenEnabled() {
    CommonDescriptor.getInstance().getConfig().setEnableSecureErase(true);

    byte[] bytes = {1, 2};
    boolean[] booleans = {true, true};
    int[] ints = {1, 2};
    long[] longs = {1L, 2L};
    float[] floats = {1.0F, 2.0F};
    double[] doubles = {1.0D, 2.0D};
    byte[] firstBinaryValues = {1, 2};
    byte[] secondBinaryValues = {3, 4};
    Binary[] binaries = {
      new Binary(firstBinaryValues), null, new Binary((byte[]) null), new Binary(secondBinaryValues)
    };

    ResidualDataProtectionUtils.eraseArrayIfEnabled(bytes, TEST_SOURCE);
    ResidualDataProtectionUtils.eraseArrayIfEnabled(booleans, TEST_SOURCE);
    ResidualDataProtectionUtils.eraseArrayIfEnabled(ints, TEST_SOURCE);
    ResidualDataProtectionUtils.eraseArrayIfEnabled(longs, TEST_SOURCE);
    ResidualDataProtectionUtils.eraseArrayIfEnabled(floats, TEST_SOURCE);
    ResidualDataProtectionUtils.eraseArrayIfEnabled(doubles, TEST_SOURCE);
    ResidualDataProtectionUtils.eraseArrayIfEnabled(binaries, TEST_SOURCE);

    assertArrayEquals(new byte[2], bytes);
    assertArrayEquals(new boolean[2], booleans);
    assertArrayEquals(new int[2], ints);
    assertArrayEquals(new long[2], longs);
    assertArrayEquals(new float[2], floats, 0.0F);
    assertArrayEquals(new double[2], doubles, 0.0D);
    assertArrayEquals(new byte[2], firstBinaryValues);
    assertArrayEquals(new byte[2], secondBinaryValues);
  }

  @Test
  public void testEraseByteBuffersWithoutChangingState() {
    CommonDescriptor.getInstance().getConfig().setEnableSecureErase(true);
    assertBufferErased(ByteBuffer.allocate(8));
    assertBufferErased(ByteBuffer.allocateDirect(8));
  }

  @Test
  public void testDisabledProtectionDoesNotEraseGeneralMemory() {
    CommonDescriptor.getInstance().getConfig().setEnableSecureErase(false);
    byte[] bytes = {1, 2};
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2});
    byte[] binaryValues = {1, 2};
    Binary[] binaries = {new Binary(binaryValues)};

    ResidualDataProtectionUtils.eraseArrayIfEnabled(bytes, TEST_SOURCE);
    ResidualDataProtectionUtils.eraseByteBufferIfEnabled(buffer, TEST_SOURCE);
    ResidualDataProtectionUtils.eraseArrayIfEnabled(binaries, TEST_SOURCE);

    assertArrayEquals(new byte[] {1, 2}, bytes);
    assertArrayEquals(new byte[] {1, 2}, buffer.array());
    assertArrayEquals(new byte[] {1, 2}, binaryValues);
  }

  @Test
  public void testPasswordBuffersAreAlwaysErased() {
    CommonDescriptor.getInstance().getConfig().setEnableSecureErase(false);
    byte[] bytes = {1, 2};
    char[] chars = {'a', 'b'};

    ResidualDataProtectionUtils.erasePassword(bytes, TEST_SOURCE);
    ResidualDataProtectionUtils.erasePassword(chars, TEST_SOURCE);

    assertArrayEquals(new byte[2], bytes);
    assertArrayEquals(new char[2], chars);
  }

  private static void assertBufferErased(ByteBuffer buffer) {
    for (int i = 0; i < buffer.capacity(); i++) {
      buffer.put(i, (byte) (i + 1));
    }
    buffer.position(2);
    buffer.limit(6);

    ResidualDataProtectionUtils.eraseByteBufferIfEnabled(buffer, TEST_SOURCE);

    assertEquals(2, buffer.position());
    assertEquals(6, buffer.limit());
    ByteBuffer duplicate = buffer.duplicate();
    duplicate.clear();
    while (duplicate.hasRemaining()) {
      assertEquals(0, duplicate.get());
    }
  }
}
