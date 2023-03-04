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

import org.apache.iotdb.tsfile.constant.TestConstant;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BytesUtilsTest {

  private Random r = new Random(System.currentTimeMillis());

  @Test
  public void testIntToBytes() {
    int b = 123;
    byte[] bb = BytesUtils.intToBytes(b);
    int bf = BytesUtils.bytesToInt(bb);
    assertEquals("testBytesToFloat", b, bf);
  }

  @Test
  public void testIntToBytesWithBytesArray() {
    int b1 = 123;
    int b2 = 234;
    byte[] ret = new byte[8];
    BytesUtils.intToBytes(b1, ret, 0);
    BytesUtils.intToBytes(b2, ret, 4);
    int rb1 = BytesUtils.bytesToInt(ret, 0);
    int rb2 = BytesUtils.bytesToInt(ret, 4);
    assertEquals("testBytesToFloat", b1, rb1);
    assertEquals("testBytesToFloat", b2, rb2);
  }

  @Test
  public void testFloatToBytes() {
    // fail("NotFilter yet implemented");
    float b = 25.0f;
    byte[] bb = BytesUtils.floatToBytes(b);
    float bf = BytesUtils.bytesToFloat(bb);
    assertEquals("testBytesToFloat", b, bf, TestConstant.float_min_delta);
  }

  @Test
  public void testFloatToBytesWithBytesArray() {
    float b1 = 123.123f;
    float b2 = 234.323f;
    byte[] ret = new byte[8];
    BytesUtils.floatToBytes(b1, ret, 0);
    BytesUtils.floatToBytes(b2, ret, 4);
    float rb1 = BytesUtils.bytesToFloat(ret, 0);
    float rb2 = BytesUtils.bytesToFloat(ret, 4);
    assertEquals(b1, rb1, TestConstant.float_min_delta);
    assertEquals(b2, rb2, TestConstant.float_min_delta);
  }

  @Test
  public void testBoolToBytes() {
    boolean b = true;
    byte[] bb = BytesUtils.boolToBytes(b);
    boolean bf = BytesUtils.bytesToBool(bb);
    assertEquals("testBoolToBytes", b, bf);
  }

  @Test
  public void testBytesToBool() {
    boolean b = false;
    byte[] bb = BytesUtils.boolToBytes(b);
    boolean bf = BytesUtils.bytesToBool(bb);
    assertEquals("testBytesToBool", b, bf);
  }

  @Test
  public void testBoolToBytesWithBytesArray() {
    boolean b1 = true;
    boolean b2 = false;
    byte[] ret = new byte[2];
    BytesUtils.boolToBytes(b1, ret, 0);
    BytesUtils.boolToBytes(b2, ret, 1);
    boolean rb1 = BytesUtils.bytesToBool(ret, 0);
    boolean rb2 = BytesUtils.bytesToBool(ret, 1);
    assertEquals(b1, rb1);
    assertEquals(b2, rb2);
  }

  @Test
  public void testlongToBytes() {
    long lNum = 32143422454243342L;
    long iNum = 1032423424L;
    long lSNum = 10;
    assertEquals(lNum, BytesUtils.bytesToLong(BytesUtils.longToBytes(lNum, 8), 8));
    assertEquals(iNum, BytesUtils.bytesToLong(BytesUtils.longToBytes(iNum, 8), 8));
    assertEquals(iNum, BytesUtils.bytesToLong(BytesUtils.longToBytes(iNum, 4), 4));
    assertEquals(lSNum, BytesUtils.bytesToLong(BytesUtils.longToBytes(lSNum, 1), 1));
  }

  @Test
  public void testLongToBytesWithBytesArray() {
    long b1 = 3214342243342L;
    long b2 = 1032423424L;
    byte[] ret = new byte[16];
    BytesUtils.longToBytes(b1, ret, 0);
    BytesUtils.longToBytes(b2, ret, 8);
    long rb1 = BytesUtils.bytesToLongFromOffset(ret, 8, 0);
    long rb2 = BytesUtils.bytesToLongFromOffset(ret, 8, 8);
    assertEquals(b1, rb1);
    assertEquals(b2, rb2);
  }

  @Test
  public void bytesToLongOffsetTest() {
    long l = 2454243342L;
    int width = 64 - Long.numberOfLeadingZeros(l);
    byte[] bs = new byte[width * 2];
    BytesUtils.longToBytes(l, bs, 0, width);
    assertEquals(l, BytesUtils.bytesToLong(bs, 0, width));
  }

  @Test
  public void readLongTest() throws IOException {
    long l = 32143422454243342L;
    byte[] bs = BytesUtils.longToBytes(l);
    InputStream in = new ByteArrayInputStream(bs);
    assertEquals(l, BytesUtils.readLong(in));
  }

  @Test
  public void testDoubleToBytes() {
    double b1 = 2745687.1253123d;
    byte[] ret = BytesUtils.doubleToBytes(b1);
    double rb1 = BytesUtils.bytesToDouble(ret);
    assertEquals(b1, rb1, TestConstant.float_min_delta);
  }

  @Test
  public void testDoubleToBytesWithBytesArray() {
    double b1 = 112357.548799d;
    double b2 = 2745687.1253123d;
    byte[] ret = new byte[16];
    BytesUtils.doubleToBytes(b1, ret, 0);
    BytesUtils.doubleToBytes(b2, ret, 8);
    double rb1 = BytesUtils.bytesToDouble(ret, 0);
    double rb2 = BytesUtils.bytesToDouble(ret, 8);
    assertEquals(b1, rb1, TestConstant.double_min_delta);
    assertEquals(b2, rb2, TestConstant.double_min_delta);
  }

  @Test
  public void testStringToBytes() {
    String b = "lqfkgv12KLDJSL1@#%";
    byte[] ret = BytesUtils.stringToBytes(b);
    String rb1 = BytesUtils.bytesToString(ret);
    assertTrue(b.equals(rb1));
  }

  @Test
  public void testConcatByteArray() {
    List<byte[]> list = new ArrayList<byte[]>();
    float f1 = 12.4f;
    boolean b1 = true;
    list.add(BytesUtils.floatToBytes(f1));
    list.add(BytesUtils.boolToBytes(b1));
    byte[] ret = BytesUtils.concatByteArray(list.get(0), list.get(1));
    float rf1 = BytesUtils.bytesToFloat(ret, 0);
    boolean rb1 = BytesUtils.bytesToBool(ret, 4);
    assertEquals(f1, rf1, TestConstant.float_min_delta);
    assertEquals(b1, rb1);
  }

  @Test
  public void testConcatByteArrayList() {
    List<byte[]> list = new ArrayList<byte[]>();
    float f1 = 12.4f;
    boolean b1 = true;
    int i1 = 12;
    list.add(BytesUtils.floatToBytes(f1));
    list.add(BytesUtils.boolToBytes(b1));
    list.add(BytesUtils.intToBytes(i1));
    byte[] ret = BytesUtils.concatByteArrayList(list);
    float rf1 = BytesUtils.bytesToFloat(ret, 0);
    boolean rb1 = BytesUtils.bytesToBool(ret, 4);
    int ri1 = BytesUtils.bytesToInt(ret, 5);
    assertEquals(f1, rf1, TestConstant.float_min_delta);
    assertEquals(b1, rb1);
    assertEquals(i1, ri1);
  }

  @Test
  public void testSubBytes() {
    List<byte[]> list = new ArrayList<byte[]>();
    float f1 = 12.4f;
    boolean b1 = true;
    int i1 = 12;
    list.add(BytesUtils.floatToBytes(f1));
    list.add(BytesUtils.boolToBytes(b1));
    list.add(BytesUtils.intToBytes(i1));
    byte[] ret = BytesUtils.concatByteArrayList(list);
    boolean rb1 = BytesUtils.bytesToBool(BytesUtils.subBytes(ret, 4, 1));
    int ri1 = BytesUtils.bytesToInt(BytesUtils.subBytes(ret, 5, 4));
    assertEquals(b1, rb1);
    assertEquals(i1, ri1);
  }

  @Test
  public void testGetByteN() {
    byte src = 120;
    byte dest = 0;
    for (int i = 0; i < 64; i++) {
      int a = BytesUtils.getByteN(src, i);
      dest = BytesUtils.setByteN(dest, i, a);
    }
    assertEquals(src, dest);
  }

  @Test
  public void testGetLongN() {
    long src = (long) Math.pow(2, 33);
    long dest = 0;
    for (int i = 0; i < 64; i++) {
      int a = BytesUtils.getLongN(src, i);
      dest = BytesUtils.setLongN(dest, i, a);
    }
    assertEquals(src, dest);
  }

  @Test
  public void testGetIntN() {
    int src = 54243342;
    int dest = 0;
    for (int i = 0; i < 32; i++) {
      int a = BytesUtils.getIntN(src, i);
      dest = BytesUtils.setIntN(dest, i, a);
    }
    assertEquals(src, dest);
  }

  @Test
  public void testIntToBytesWithWidth() {
    int b1 = (1 << 22) - 1413;
    int b2 = (1 << 22) - 3588;
    int b3 = (1 << 22) - 1435;
    int b4 = (1 << 22) - 85476;
    byte[] ret = new byte[12];
    BytesUtils.intToBytes(b1, ret, 0, 24);
    BytesUtils.intToBytes(b2, ret, 24, 24);
    BytesUtils.intToBytes(b3, ret, 48, 24);
    BytesUtils.intToBytes(b4, ret, 72, 24);
    int rb1 = BytesUtils.bytesToInt(ret, 0, 24);
    int rb2 = BytesUtils.bytesToInt(ret, 24, 24);
    int rb3 = BytesUtils.bytesToInt(ret, 48, 24);
    int rb4 = BytesUtils.bytesToInt(ret, 72, 24);
    assertEquals("testIntToBytesWithWidth1", b1, rb1);
    assertEquals("testIntToBytesWithWidth2", b2, rb2);
    assertEquals("testIntToBytesWithWidth3", b3, rb3);
    assertEquals("testIntToBytesWithWidth4", b4, rb4);
  }

  @Test
  public void testLongToBytesWithWidth() {
    int bitLen = 42;
    long basic = (1 << 30) * 2l;
    long b1 = (1 << (bitLen % 32)) * basic + r.nextInt();
    long b2 = (1 << (bitLen % 32)) * basic + r.nextInt();
    long b3 = (1 << (bitLen % 32)) * basic + r.nextInt();
    long b4 = (1 << (bitLen % 32)) * basic + r.nextInt();
    byte[] ret = new byte[(int) Math.ceil(bitLen * 4.0 / 8.0)];
    BytesUtils.longToBytes(b1, ret, bitLen * 0, bitLen);
    BytesUtils.longToBytes(b2, ret, bitLen * 1, bitLen);
    BytesUtils.longToBytes(b3, ret, bitLen * 2, bitLen);
    BytesUtils.longToBytes(b4, ret, bitLen * 3, bitLen);
    long rb1 = BytesUtils.bytesToLong(ret, bitLen * 0, bitLen);
    long rb2 = BytesUtils.bytesToLong(ret, bitLen * 1, bitLen);
    long rb3 = BytesUtils.bytesToLong(ret, bitLen * 2, bitLen);
    long rb4 = BytesUtils.bytesToLong(ret, bitLen * 3, bitLen);
    assertEquals("testIntToBytesWithWidth1", b1, rb1);
    assertEquals("testIntToBytesWithWidth2", b2, rb2);
    assertEquals("testIntToBytesWithWidth3", b3, rb3);
    assertEquals("testIntToBytesWithWidth4", b4, rb4);
  }

  private void intToBinaryShowForTest(int src) {
    for (int i = 31; i >= 0; i--) {
      if ((src & (1 << i)) != 0) {
        System.out.print(1);
      } else {
        System.out.print(0);
      }
      if ((i % 8) == 0) {
        System.out.print(" ");
      }
    }
    System.out.print("\n");
  }

  private void longToBinaryShowForTest(long src) {
    for (int i = 63; i >= 0; i--) {
      System.out.print(BytesUtils.getLongN(src, i));
      if ((i % 8) == 0) {
        System.out.print(" ");
      }
    }
    System.out.print("\n");
  }

  private void byteArrayToBinaryShowForTest(byte[] src) {
    for (byte b : src) {
      for (int i = 8; i >= 0; i--) {
        if ((b & (1 << i)) != 0) {
          System.out.print(1);
        } else {
          System.out.print(0);
        }
      }
      System.out.print(" ");
    }
    System.out.print("\n");
  }

  @Test
  public void testReadInt() throws IOException {
    int l = r.nextInt();
    byte[] bs = BytesUtils.intToBytes(l);
    InputStream in = new ByteArrayInputStream(bs);
    assertEquals(l, BytesUtils.readInt(in));
  }

  @Test
  public void testReadLong() throws IOException {
    long l = r.nextLong();
    byte[] bs = BytesUtils.longToBytes(l);
    InputStream in = new ByteArrayInputStream(bs);
    assertEquals(l, BytesUtils.readLong(in));
  }

  @Test
  public void testReadFloat() throws IOException {
    float l = r.nextFloat();
    byte[] bs = BytesUtils.floatToBytes(l);
    InputStream in = new ByteArrayInputStream(bs);
    assertEquals(l, BytesUtils.readFloat(in), TestConstant.float_min_delta);
  }

  @Test
  public void testReadDouble() throws IOException {
    double l = r.nextDouble();
    byte[] bs = BytesUtils.doubleToBytes(l);
    InputStream in = new ByteArrayInputStream(bs);
    assertEquals(l, BytesUtils.readDouble(in), TestConstant.double_min_delta);
  }

  @Test
  public void testReadBool() throws IOException {
    boolean l = r.nextBoolean();
    byte[] bs = BytesUtils.boolToBytes(l);
    InputStream in = new ByteArrayInputStream(bs);
    assertEquals(l, BytesUtils.readBool(in));
  }
}
