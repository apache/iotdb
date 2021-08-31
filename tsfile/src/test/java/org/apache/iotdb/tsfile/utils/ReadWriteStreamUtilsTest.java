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
package org.apache.iotdb.tsfile.utils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ReadWriteStreamUtilsTest {

  private List<Integer> unsignedVarIntList;
  private List<Integer> littleEndianIntList;
  private List<Long> littleEndianLongList;

  @Before
  public void setUp() {
    unsignedVarIntList = new ArrayList<Integer>();
    littleEndianIntList = new ArrayList<Integer>();
    littleEndianLongList = new ArrayList<Long>();

    int uvInt = 123;
    for (int i = 0; i < 10; i++) {
      unsignedVarIntList.add(uvInt);
      unsignedVarIntList.add(uvInt - 1);
      uvInt *= 3;
    }

    int leInt = 17;
    for (int i = 0; i < 17; i++) {
      littleEndianIntList.add(leInt);
      littleEndianIntList.add(leInt - 1);
      leInt *= 3;
    }

    long leLong = 13;
    for (int i = 0; i < 38; i++) {
      littleEndianLongList.add(leLong);
      littleEndianLongList.add(leLong - 1);
      leLong *= 3;
    }
  }

  @After
  public void tearDown() {}

  @Test
  public void testGetIntMinBitWidth() {
    List<Integer> uvIntList = new ArrayList<Integer>();
    uvIntList.add(0);
    assertEquals(1, ReadWriteForEncodingUtils.getIntMaxBitWidth(uvIntList));
    uvIntList.add(1);
    assertEquals(1, ReadWriteForEncodingUtils.getIntMaxBitWidth(uvIntList));
    int uvInt = 123;
    for (int i = 0; i < 10; i++) {
      uvIntList.add(uvInt);
      uvIntList.add(uvInt - 1);
      assertEquals(
          32 - Integer.numberOfLeadingZeros(uvInt),
          ReadWriteForEncodingUtils.getIntMaxBitWidth(uvIntList));
      uvInt *= 3;
    }
  }

  @Test
  public void testGetLongMinBitWidth() {
    List<Long> uvLongList = new ArrayList<Long>();
    uvLongList.add(0L);
    assertEquals(1, ReadWriteForEncodingUtils.getLongMaxBitWidth(uvLongList));
    uvLongList.add(1L);
    assertEquals(1, ReadWriteForEncodingUtils.getLongMaxBitWidth(uvLongList));
    long uvLong = 123;
    for (int i = 0; i < 10; i++) {
      uvLongList.add(uvLong);
      uvLongList.add(uvLong - 1);
      assertEquals(
          64 - Long.numberOfLeadingZeros(uvLong),
          ReadWriteForEncodingUtils.getLongMaxBitWidth(uvLongList));
      uvLong *= 7;
    }
  }

  @Test
  public void testReadUnsignedVarInt() throws IOException {
    for (int uVarInt : unsignedVarIntList) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ReadWriteForEncodingUtils.writeUnsignedVarInt(uVarInt, baos);
      ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
      int value_read = ReadWriteForEncodingUtils.readUnsignedVarInt(bais);
      assertEquals(value_read, uVarInt);
    }
  }

  /** @see {@link #testReadUnsignedVarInt()} */
  @Test
  public void testWriteUnsignedVarInt() {}

  /** @see {@link #testReadIntLittleEndianPaddedOnBitWidth()} */
  @Test
  public void testWriteIntLittleEndianPaddedOnBitWidth() {}

  /** @see {@link #testReadLongLittleEndianPaddedOnBitWidth()} */
  @Test
  public void testWriteLongLittleEndianPaddedOnBitWidth() {}

  @Test
  public void testReadIntLittleEndianPaddedOnBitWidth() throws IOException {
    for (int value : littleEndianIntList) {
      int bitWidth = 32 - Integer.numberOfLeadingZeros(value);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ReadWriteForEncodingUtils.writeIntLittleEndianPaddedOnBitWidth(value, baos, bitWidth);
      ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());

      int value_read =
          ReadWriteForEncodingUtils.readIntLittleEndianPaddedOnBitWidth(buffer, bitWidth);
      assertEquals(value_read, value);
    }
  }

  @Test
  public void testReadLongLittleEndianPaddedOnBitWidth() throws IOException {
    for (long value : littleEndianLongList) {
      int bitWidth = 64 - Long.numberOfLeadingZeros(value);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ReadWriteForEncodingUtils.writeLongLittleEndianPaddedOnBitWidth(value, baos, bitWidth);
      ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());

      long value_read =
          ReadWriteForEncodingUtils.readLongLittleEndianPaddedOnBitWidth(buffer, bitWidth);
      assertEquals(value_read, value);
    }
  }
}
