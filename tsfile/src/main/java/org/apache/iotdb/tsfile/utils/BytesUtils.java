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

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * BytesUtils is a utility class. It provide conversion among byte array and other type including
 * integer, long, float, boolean, double and string. <br>
 * It also provide other usable function as follow:<br>
 * reading function which receives InputStream. <br>
 * concat function to join a list of byte array to one.<br>
 * get and set one bit in a byte array.
 */
public class BytesUtils {

  private BytesUtils() {}

  private static final Logger LOG = LoggerFactory.getLogger(BytesUtils.class);

  /**
   * integer convert to byte[4].
   *
   * @param i integer to convert
   * @return byte[4] for integer
   */
  public static byte[] intToBytes(int i) {
    return new byte[] {
      (byte) ((i >> 24) & 0xFF),
      (byte) ((i >> 16) & 0xFF),
      (byte) ((i >> 8) & 0xFF),
      (byte) (i & 0xFF)
    };
  }

  /**
   * integer convert to byte array, then write four bytes to parameter desc start from index:offset.
   *
   * @param i integer to convert
   * @param desc byte array be written
   * @param offset position in desc byte array that conversion result should start
   * @return byte array
   */
  public static byte[] intToBytes(int i, byte[] desc, int offset) {
    if (desc.length - offset < 4) {
      throw new IllegalArgumentException("Invalid input: desc.length - offset < 4");
    }
    desc[0 + offset] = (byte) ((i >> 24) & 0xFF);
    desc[1 + offset] = (byte) ((i >> 16) & 0xFF);
    desc[2 + offset] = (byte) ((i >> 8) & 0xFF);
    desc[3 + offset] = (byte) (i & 0xFF);
    return desc;
  }

  /**
   * convert an integer to a byte array which length is width, then copy this array to the parameter
   * result from pos.
   *
   * @param srcNum input integer variable
   * @param result byte array to convert
   * @param pos start position
   * @param width bit-width
   */
  public static void intToBytes(int srcNum, byte[] result, int pos, int width) {
    int temp = 0;
    for (int i = 0; i < width; i++) {
      temp = (pos + width - 1 - i) / 8;
      try {
        result[temp] = setByteN(result[temp], pos + width - 1 - i, getIntN(srcNum, i));
      } catch (Exception e) {
        LOG.error(
            "tsfile-common BytesUtils: cannot convert an integer {} to a byte array, "
                + "pos {}, width {}",
            srcNum,
            pos,
            width,
            e);
      }
    }
  }

  /**
   * divide int to two bytes.
   *
   * @param i int
   * @return two bytes in byte[] structure
   */
  public static byte[] intToTwoBytes(int i) {
    if (i > 0xFFFF) {
      throw new IllegalArgumentException("Invalid input: " + i + " > 0xFFFF");
    }
    byte[] ret = new byte[2];
    ret[1] = (byte) (i & 0xFF);
    ret[0] = (byte) ((i >> 8) & 0xFF);
    return ret;
  }

  /**
   * concatenate two bytes to int.
   *
   * @param ret byte[]
   * @return int value
   */
  public static int twoBytesToInt(byte[] ret) {
    if (ret.length != 2) {
      throw new IllegalArgumentException("Invalid input: ret.length != 2");
    }
    int value = 0;
    value |= ret[0];
    value = value << 8;
    value |= ret[1];
    return value;
  }

  /**
   * byte[4] convert to integer.
   *
   * @param bytes input byte[]
   * @return integer
   */
  public static int bytesToInt(byte[] bytes) {
    // compatible to long

    int length = bytes.length;
    long r = 0;
    for (int i = 0; i < length; i++) {
      r += ((bytes[length - 1 - i] & 0xFF) << (i * 8));
    }

    if (r > Integer.MAX_VALUE) {
      throw new RuntimeException("Row count is larger than Integer.MAX_VALUE");
    }

    return (int) r;
  }

  /**
   * convert four-bytes byte array cut from parameters to integer.
   *
   * @param bytes source bytes which length should be greater than 4
   * @param offset position in parameter byte array that conversion result should start
   * @return integer
   */
  public static int bytesToInt(byte[] bytes, int offset) {
    if (bytes.length - offset < 4) {
      throw new IllegalArgumentException("Invalid input: bytes.length - offset < 4");
    }

    int value = 0;
    // high bit to low
    for (int i = 0; i < 4; i++) {
      int shift = (4 - 1 - i) * 8;
      value += (bytes[offset + i] & 0x000000FF) << shift;
    }
    return value;
  }

  /**
   * given a byte array, read width bits from specified position bits and convert it to an integer.
   *
   * @param result input byte array
   * @param pos bit offset rather than byte offset
   * @param width bit-width
   * @return integer variable
   */
  public static int bytesToInt(byte[] result, int pos, int width) {

    int value = 0;
    int temp = 0;

    for (int i = 0; i < width; i++) {
      temp = (pos + width - 1 - i) / 8;
      value = setIntN(value, i, getByteN(result[temp], pos + width - 1 - i));
    }
    return value;
  }

  /**
   * convert float to byte array.
   *
   * @param x float
   * @return byte[4]
   */
  public static byte[] floatToBytes(float x) {
    byte[] b = new byte[4];
    int l = Float.floatToIntBits(x);
    for (int i = 3; i >= 0; i--) {
      b[i] = (byte) l;
      l = l >> 8;
    }
    return b;
  }

  /**
   * float convert to boolean, then write four bytes to parameter desc start from index:offset.
   *
   * @param x float
   * @param desc byte array be written
   * @param offset position in desc byte array that conversion result should start
   */
  public static void floatToBytes(float x, byte[] desc, int offset) {
    if (desc.length - offset < 4) {
      throw new IllegalArgumentException("Invalid input: desc.length - offset < 4");
    }
    int l = Float.floatToIntBits(x);
    for (int i = 3 + offset; i >= offset; i--) {
      desc[i] = (byte) l;
      l = l >> 8;
    }
  }

  /**
   * convert byte[4] to float.
   *
   * @param b byte[4]
   * @return float
   */
  public static float bytesToFloat(byte[] b) {
    if (b.length != 4) {
      throw new IllegalArgumentException("Invalid input: b.length != 4");
    }

    int l;
    l = b[3];
    l &= 0xff;
    l |= ((long) b[2] << 8);
    l &= 0xffff;
    l |= ((long) b[1] << 16);
    l &= 0xffffff;
    l |= ((long) b[0] << 24);
    return Float.intBitsToFloat(l);
  }

  /**
   * convert four-bytes byte array cut from parameters to float.
   *
   * @param b source bytes which length should be greater than 4
   * @param offset position in parameter byte array that conversion result should start
   * @return float
   */
  public static float bytesToFloat(byte[] b, int offset) {
    if (b.length - offset < 4) {
      throw new IllegalArgumentException("Invalid input: b.length - offset < 4");
    }

    int l;
    l = b[offset + 3];
    l &= 0xff;
    l |= ((long) b[offset + 2] << 8);
    l &= 0xffff;
    l |= ((long) b[offset + 1] << 16);
    l &= 0xffffff;
    l |= ((long) b[offset] << 24);
    return Float.intBitsToFloat(l);
  }

  /**
   * convert double to byte array.
   *
   * @param data double
   * @return byte[8]
   */
  public static byte[] doubleToBytes(double data) {
    byte[] bytes = new byte[8];
    long value = Double.doubleToLongBits(data);
    for (int i = 7; i >= 0; i--) {
      bytes[i] = (byte) value;
      value = value >> 8;
    }
    return bytes;
  }

  /**
   * convert double to byte into the given byte array started from offset.
   *
   * @param d input double
   * @param bytes target byte[]
   * @param offset start pos
   */
  public static void doubleToBytes(double d, byte[] bytes, int offset) {
    if (bytes.length - offset < 8) {
      throw new IllegalArgumentException("Invalid input: bytes.length - offset < 8");
    }

    long value = Double.doubleToLongBits(d);
    for (int i = 7; i >= 0; i--) {
      bytes[offset + i] = (byte) value;
      value = value >> 8;
    }
  }

  /**
   * convert byte array to double.
   *
   * @param bytes byte[8]
   * @return double
   */
  public static double bytesToDouble(byte[] bytes) {
    long value = bytes[7];
    value &= 0xff;
    value |= ((long) bytes[6] << 8);
    value &= 0xffff;
    value |= ((long) bytes[5] << 16);
    value &= 0xffffff;
    value |= ((long) bytes[4] << 24);
    value &= 0xffffffffL;
    value |= ((long) bytes[3] << 32);
    value &= 0xffffffffffL;
    value |= ((long) bytes[2] << 40);
    value &= 0xffffffffffffL;
    value |= ((long) bytes[1] << 48);
    value &= 0xffffffffffffffL;
    value |= ((long) bytes[0] << 56);
    return Double.longBitsToDouble(value);
  }

  /**
   * convert eight-bytes byte array cut from parameters to double.
   *
   * @param bytes source bytes which length should be greater than 8
   * @param offset position in parameter byte array that conversion result should start
   * @return double
   */
  public static double bytesToDouble(byte[] bytes, int offset) {
    if (bytes.length - offset < 8) {
      throw new IllegalArgumentException("Invalid input: bytes.length - offset < 8");
    }
    long value = bytes[offset + 7];
    value &= 0xff;
    value |= ((long) bytes[offset + 6] << 8);
    value &= 0xffff;
    value |= ((long) bytes[offset + 5] << 16);
    value &= 0xffffff;
    value |= ((long) bytes[offset + 4] << 24);
    value &= 0xffffffffL;
    value |= ((long) bytes[offset + 3] << 32);
    value &= 0xffffffffffL;
    value |= ((long) bytes[offset + 2] << 40);
    value &= 0xffffffffffffL;
    value |= ((long) bytes[offset + 1] << 48);
    value &= 0xffffffffffffffL;
    value |= ((long) bytes[offset] << 56);
    return Double.longBitsToDouble(value);
  }

  /**
   * convert boolean to byte[1].
   *
   * @param x boolean
   * @return byte[]
   */
  public static byte[] boolToBytes(boolean x) {
    byte[] b = new byte[1];
    if (x) {
      b[0] = 1;
    } else {
      b[0] = 0;
    }
    return b;
  }

  public static byte boolToByte(boolean x) {
    if (x) {
      return 1;
    } else {
      return 0;
    }
  }

  public static boolean byteToBool(byte b) {
    return b == 1;
  }

  /**
   * boolean convert to byte array, then write four bytes to parameter desc start from index:offset.
   *
   * @param x input boolean
   * @param desc byte array be written
   * @param offset position in desc byte array that conversion result should start
   * @return byte[1]
   */
  public static byte[] boolToBytes(boolean x, byte[] desc, int offset) {
    if (x) {
      desc[offset] = 1;
    } else {
      desc[offset] = 0;
    }
    return desc;
  }

  /**
   * byte array to boolean.
   *
   * @param b input byte[1]
   * @return boolean
   */
  public static boolean bytesToBool(byte[] b) {
    if (b.length != 1) {
      throw new IllegalArgumentException("Invalid input: b.length != 1");
    }

    return b[0] != 0;
  }

  /**
   * convert one-bytes byte array cut from parameters to boolean.
   *
   * @param b source bytes which length should be greater than 1
   * @param offset position in parameter byte array that conversion result should start
   * @return boolean
   */
  public static boolean bytesToBool(byte[] b, int offset) {
    if (b.length - offset < 1) {
      throw new IllegalArgumentException("Invalid input: b.length - offset < 1");
    }
    return b[offset] != 0;
  }

  /**
   * long to byte array with default converting length 8. It means the length of result byte array
   * is 8.
   *
   * @param num long variable to be converted
   * @return byte[8]
   */
  public static byte[] longToBytes(long num) {
    return longToBytes(num, 8);
  }

  /**
   * specify the result array length. then, convert long to Big-Endian byte from low to high. <br>
   * e.g.<br>
   * the binary presentation of long number 1000L is {6 bytes equal 0000000} 00000011 11101000<br>
   * if len = 2, it will return byte array :{00000011 11101000}(Big-Endian) if len = 1, it will
   * return byte array :{11101000}.
   *
   * @param num long variable to be converted
   * @param len length of result byte array
   * @return byte array which length equals with parameter len
   */
  public static byte[] longToBytes(long num, int len) {
    byte[] byteNum = new byte[len];
    for (int ix = 0; ix < len; ix++) {
      byteNum[len - ix - 1] = (byte) ((num >> ix * 8) & 0xFF);
    }
    return byteNum;
  }

  /**
   * long convert to byte array, then write four bytes to parameter desc start from index:offset.
   *
   * @param num input long variable
   * @param desc byte array be written
   * @param offset position in desc byte array that conversion result should start
   * @return byte array
   */
  public static byte[] longToBytes(long num, byte[] desc, int offset) {
    for (int ix = 0; ix < 8; ++ix) {
      int i = 64 - (ix + 1) * 8;
      desc[ix + offset] = (byte) ((num >> i) & 0xff);
    }
    return desc;
  }

  /**
   * convert an long to a byte array which length is width, then copy this array to the parameter
   * result from pos.
   *
   * @param srcNum input long variable
   * @param result byte array to convert
   * @param pos start position
   * @param width bit-width
   */
  public static void longToBytes(long srcNum, byte[] result, int pos, int width) {
    int temp = 0;
    for (int i = 0; i < width; i++) {
      temp = (pos + width - 1 - i) / 8;
      try {
        result[temp] = setByteN(result[temp], pos + width - 1 - i, getLongN(srcNum, i));
      } catch (Exception e) {
        LOG.error(
            "tsfile-common BytesUtils: cannot convert a long {} to a byte array, pos {}, width {}",
            srcNum,
            pos,
            width,
            e);
      }
    }
  }

  /**
   * convert byte array to long with default length 8. namely.
   *
   * @param byteNum input byte array
   * @return long
   */
  public static long bytesToLong(byte[] byteNum) {
    return bytesToLong(byteNum, byteNum.length);
  }

  /**
   * specify the input byte array length. then, convert byte array to long value from low to high.
   * <br>
   * e.g.<br>
   * the input byte array is {00000011 11101000}. if len = 2, return 1000 if len = 1, return
   * 232(only calculate the low byte).
   *
   * @param byteNum byte array to be converted
   * @param len length of input byte array to be converted
   * @return long
   */
  public static long bytesToLong(byte[] byteNum, int len) {
    long num = 0;
    for (int ix = 0; ix < len; ix++) {
      num <<= 8;
      num |= (byteNum[ix] & 0xff);
    }
    return num;
  }

  /**
   * given a byte array, read width bits from specified pos bits and convert it to an long.
   *
   * @param result input byte array
   * @param pos bit offset rather than byte offset
   * @param width bit-width
   * @return long variable
   */
  public static long bytesToLong(byte[] result, int pos, int width) {
    long value = 0;
    int temp = 0;
    for (int i = 0; i < width; i++) {
      temp = (pos + width - 1 - i) / 8;
      value = setLongN(value, i, getByteN(result[temp], pos + width - 1 - i));
    }
    return value;
  }

  /**
   * convert eight-bytes byte array cut from parameters to long.
   *
   * @param byteNum source bytes which length should be greater than 8
   * @param len length of input byte array to be converted
   * @param offset position in parameter byte array that conversion result should start
   * @return long
   */
  public static long bytesToLongFromOffset(byte[] byteNum, int len, int offset) {
    if (byteNum.length - offset < len) {
      throw new IllegalArgumentException("Invalid input: byteNum.length - offset < len");
    }
    long num = 0;
    for (int ix = 0; ix < len; ix++) {
      num <<= 8;
      num |= (byteNum[offset + ix] & 0xff);
    }
    return num;
  }

  /**
   * convert string to byte array using UTF-8 encoding.
   *
   * @param str input string
   * @return byte array
   */
  public static byte[] stringToBytes(String str) {
    return str.getBytes(TSFileConfig.STRING_CHARSET);
  }

  /**
   * convert byte array to string using UTF-8 encoding.
   *
   * @param byteStr input byte array
   * @return string
   */
  public static String bytesToString(byte[] byteStr) {
    return new String(byteStr, TSFileConfig.STRING_CHARSET);
  }

  /**
   * join two byte arrays to one.
   *
   * @param a one of byte array
   * @param b another byte array
   * @return byte array after joining
   */
  public static byte[] concatByteArray(byte[] a, byte[] b) {
    byte[] c = new byte[a.length + b.length];
    System.arraycopy(a, 0, c, 0, a.length);
    System.arraycopy(b, 0, c, a.length, b.length);
    return c;
  }

  /**
   * join a list of byte arrays into one array.
   *
   * @param list a list of byte array to join
   * @return byte array after joining
   */
  public static byte[] concatByteArrayList(List<byte[]> list) {
    int size = list.size();
    int len = 0;
    for (byte[] cs : list) {
      len += cs.length;
    }
    byte[] result = new byte[len];
    int pos = 0;
    for (int i = 0; i < size; i++) {
      int l = list.get(i).length;
      System.arraycopy(list.get(i), 0, result, pos, l);
      pos += l;
    }
    return result;
  }

  /**
   * cut out specified length byte array from parameter start from input byte array src and return.
   *
   * @param src input byte array
   * @param start start index of src
   * @param length cut off length
   * @return byte array
   */
  public static byte[] subBytes(byte[] src, int start, int length) {
    if ((start + length) > src.length) {
      return null;
    }
    if (length <= 0) {
      return null;
    }
    byte[] result = new byte[length];
    System.arraycopy(src, start, result, 0, length);
    return result;
  }

  /**
   * get one bit in input integer. the offset is from low to high and start with 0<br>
   * e.g.<br>
   * data:1000(00000000 00000000 00000011 11101000), if offset is 4, return 0(111 "0" 1000) if
   * offset is 9, return 1(00000 "1" 1 11101000).
   *
   * @param data input int variable
   * @param offset bit offset
   * @return 0 or 1
   */
  public static int getIntN(int data, int offset) {
    offset %= 32;
    if ((data & (1 << (offset))) != 0) {
      return 1;
    } else {
      return 0;
    }
  }

  /**
   * set one bit in input integer. the offset is from low to high and start with index 0<br>
   * e.g.<br>
   * data:1000({00000000 00000000 00000011 11101000}), if offset is 4, value is 1, return
   * 1016({00000000 00000000 00000011 111 "1" 1000}) if offset is 9, value is 0 return 488({00000000
   * 00000000 000000 "0" 1 11101000}) if offset is 0, value is 0 return 1000(no change).
   *
   * @param data input int variable
   * @param offset bit offset
   * @param value value to set
   * @return int variable
   */
  public static int setIntN(int data, int offset, int value) {
    offset %= 32;
    if (value == 1) {
      return (data | (1 << (offset)));
    } else {
      return (data & ~(1 << (offset)));
    }
  }

  /**
   * get one bit in input byte. the offset is from low to high and start with 0<br>
   * e.g.<br>
   * data:16(00010000), if offset is 4, return 1(000 "1" 0000) if offset is 7, return 0("0"
   * 0010000).
   *
   * @param data input byte variable
   * @param offset bit offset
   * @return 0/1
   */
  public static int getByteN(byte data, int offset) {
    offset %= 8;
    if (((0xff & data) & (1 << (7 - offset))) != 0) {
      return 1;
    } else {
      return 0;
    }
  }

  /**
   * set one bit in input byte. the offset is from low to high and start with index 0<br>
   * e.g.<br>
   * data:16(00010000), if offset is 4, value is 0, return 0({000 "0" 0000}) if offset is 1, value
   * is 1, return 18({00010010}) if offset is 0, value is 0, return 16(no change).
   *
   * @param data input byte variable
   * @param offset bit offset
   * @param value value to set
   * @return byte variable
   */
  public static byte setByteN(byte data, int offset, int value) {
    offset %= 8;
    if (value == 1) {
      return (byte) ((0xff & data) | (1 << (7 - offset)));
    } else {
      return (byte) ((0xff & data) & ~(1 << (7 - offset)));
    }
  }

  /**
   * get one bit in input long. the offset is from low to high and start with 0.
   *
   * @param data input long variable
   * @param offset bit offset
   * @return 0/1
   */
  public static int getLongN(long data, int offset) {
    offset %= 64;
    if ((data & (1L << (offset))) != 0) {
      return 1;
    } else {
      return 0;
    }
  }

  /**
   * set one bit in input long. the offset is from low to high and start with index 0.
   *
   * @param data input long variable
   * @param offset bit offset
   * @param value value to set
   * @return long variable
   */
  public static long setLongN(long data, int offset, int value) {
    offset %= 64;
    if (value == 1) {
      return (data | (1L << (offset)));
    } else {
      return (data & ~(1L << (offset)));
    }
  }

  /**
   * read 8-byte array from an InputStream and convert it to a double number.
   *
   * @param in InputStream
   * @return double
   * @throws IOException cannot read double from InputStream
   */
  public static double readDouble(InputStream in) throws IOException {
    byte[] b = safeReadInputStreamToBytes(8, in);
    return BytesUtils.bytesToDouble(b);
  }

  /**
   * read 4-byte array from an InputStream and convert it to a float number.
   *
   * @param in InputStream
   * @return float
   * @throws IOException cannot read float from InputStream
   */
  public static float readFloat(InputStream in) throws IOException {
    byte[] b = safeReadInputStreamToBytes(4, in);
    return BytesUtils.bytesToFloat(b);
  }

  /**
   * read 1-byte array from an InputStream and convert it to a integer number.
   *
   * @param in InputStream
   * @return boolean
   * @throws IOException cannot read boolean from InputStream
   */
  public static boolean readBool(InputStream in) throws IOException {
    byte[] b = safeReadInputStreamToBytes(1, in);
    return BytesUtils.bytesToBool(b);
  }

  /**
   * read 4-byte array from an InputStream and convert it to a integer number.
   *
   * @param in InputStream
   * @return integer
   * @throws IOException cannot read int from InputStream
   */
  public static int readInt(InputStream in) throws IOException {
    byte[] b = safeReadInputStreamToBytes(4, in);
    return BytesUtils.bytesToInt(b);
  }

  /**
   * read 8-byte array from an InputStream and convert it to a long number.
   *
   * @param in InputStream
   * @return long
   * @throws IOException cannot read long from InputStream
   */
  public static long readLong(InputStream in) throws IOException {
    byte[] b = safeReadInputStreamToBytes(8, in);
    return BytesUtils.bytesToLong(b);
  }

  /**
   * read bytes specified length from InputStream safely.
   *
   * @param count number of byte to read
   * @param in InputStream
   * @return byte array
   * @throws IOException cannot read from InputStream
   */
  public static byte[] safeReadInputStreamToBytes(int count, InputStream in) throws IOException {
    byte[] bytes = new byte[count];
    int readCount = 0;
    while (readCount < count) {
      readCount += in.read(bytes, readCount, count - readCount);
    }
    return bytes;
  }

  /**
   * we modify the order of serialization for fitting ByteBuffer.putShort()
   *
   * @param number input short number
   * @return Bytes
   */
  public static byte[] shortToBytes(short number) {
    int temp = number;
    byte[] b = new byte[2];
    for (int i = b.length - 1; i >= 0; i--) {
      b[i] = (byte) temp;
      temp = temp >> 8;
    }

    return b;
  }

  /**
   * we modify the order of serialization for fitting ByteBuffer.getShort()
   *
   * @param b bytes
   * @return short number
   */
  public static short bytesToShort(byte[] b) {
    short s0 = (short) (b[1] & 0xff);
    short s1 = (short) (b[0] & 0xff);
    s1 <<= 8;
    short s = (short) (s0 | s1);
    return s;
  }
}
