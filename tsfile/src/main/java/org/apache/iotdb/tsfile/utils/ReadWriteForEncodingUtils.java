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
package org.apache.iotdb.tsfile.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Utils to read/write stream.
 */
public class ReadWriteForEncodingUtils {
  private static final String TOO_LONG_BYTE_FORMAT = "tsfile-common BytesUtils: encountered value (%d) that requires more than 4 bytes";
  private ReadWriteForEncodingUtils(){}
  /**
   * check all number in a int list and find max bit width.
   *
   * @param list input list
   * @return max bit width
   */
  public static int getIntMaxBitWidth(List<Integer> list) {
    int max = 1;
    for (int num : list) {
      int bitWidth = 32 - Integer.numberOfLeadingZeros(num);
      max = bitWidth > max ? bitWidth : max;
    }
    return max;
  }

  /**
   * check all number in a long list and find max bit width.
   *
   * @param list input list
   * @return max bit width
   */
  public static int getLongMaxBitWidth(List<Long> list) {
    int max = 1;
    for (long num : list) {
      int bitWidth = 64 - Long.numberOfLeadingZeros(num);
      max = bitWidth > max ? bitWidth : max;
    }
    return max;
  }

  /**
   * transform an int var to byte[] format.
   */
  public static byte[] getUnsignedVarInt(int value) {
    int preValue = value;
    int length = 0;
    while ((value & 0xFFFFFF80) != 0L) {
      length++;
      value >>>= 7;
    }
    length++;

    byte[] res = new byte[length];
    value = preValue;
    int i = 0;
    while ((value & 0xFFFFFF80) != 0L) {
      res[i] = (byte) ((value & 0x7F) | 0x80);
      value >>>= 7;
      i++;
    }
    res[i] = (byte) (value & 0x7F);
    return res;
  }

  /**
   * read an unsigned var int in stream and transform it to int format.
   *
   * @param in stream to read an unsigned var int
   * @return integer value
   * @throws IOException exception in IO
   */
  public static int readUnsignedVarInt(InputStream in) throws IOException {
    int value = 0;
    int i = 0;
    int b;
    while (((b = in.read()) & 0x80) != 0) {
      value |= (b & 0x7F) << i;
      i += 7;
    }
    return value | (b << i);
  }

  /**
   * read an unsigned var int in stream and transform it to int format.
   *
   * @param buffer stream to read an unsigned var int
   * @return integer value
   */
  public static int readUnsignedVarInt(ByteBuffer buffer) {
    int value = 0;
    int i = 0;
    int b = 0;
    while (buffer.hasRemaining() && ((b = buffer.get()) & 0x80) != 0) {
      value |= (b & 0x7F) << i;
      i += 7;
    }
    return value | (b << i);
  }

  /**
   * write a value to stream using unsigned var int format. for example, int 123456789 has its
   * binary format 00000111-01011011-11001101-00010101 (if we omit the first 5 0, then it is
   * 111010-1101111-0011010-0010101), function writeUnsignedVarInt will split every seven bits and
   * write them to stream from low bit to high bit like: 1-0010101 1-0011010 1-1101111 0-0111010 1
   * represents has next byte to write, 0 represents number end.
   *
   * @param value value to write into stream
   * @param out output stream
   * @throws IOException exception in IO
   */
  public static void writeUnsignedVarInt(int value, OutputStream out) throws IOException {
    while ((value & 0xFFFFFF80) != 0L) {
      out.write((value & 0x7F) | 0x80);
      value >>>= 7;
    }
    out.write(value & 0x7F);
  }

  /**
   * write a value to stream using unsigned var int format. for example, int 123456789 has its
   * binary format 111010-1101111-0011010-0010101, function writeUnsignedVarInt will split every
   * seven bits and write them to stream from low bit to high bit like: 1-0010101 1-0011010
   * 1-1101111 0-0111010 1 represents has next byte to write, 0 represents number end.
   *
   * @param value value to write into stream
   * @param buffer where to store the result. buffer.remaining() needs to >= 32. Notice: (1) this
   * function does not check buffer's remaining(). (2) the position will be updated.
   * @return the number of bytes that the value consume.
   * @throws IOException exception in IO
   */
  public static int writeUnsignedVarInt(int value, ByteBuffer buffer) {
    int position = 1;
    while ((value & 0xFFFFFF80) != 0L) {
      buffer.put((byte) ((value & 0x7F) | 0x80));
      value >>>= 7;
      position++;
    }
    buffer.put((byte) (value & 0x7F));
    return position;
  }

  /**
   * write integer value using special bit to output stream.
   *
   * @param value value to write to stream
   * @param out output stream
   * @param bitWidth bit length
   * @throws IOException exception in IO
   */
  public static void writeIntLittleEndianPaddedOnBitWidth(int value, OutputStream out, int bitWidth)
      throws IOException {
    int paddedByteNum = (bitWidth + 7) / 8;
    if (paddedByteNum > 4) {
      throw new IOException(String.format(
              TOO_LONG_BYTE_FORMAT, paddedByteNum));
    }
    int offset = 0;
    while (paddedByteNum > 0) {
      out.write((value >>> offset) & 0xFF);
      offset += 8;
      paddedByteNum--;
    }
  }

  /**
   * write long value using special bit to output stream.
   *
   * @param value value to write to stream
   * @param out output stream
   * @param bitWidth bit length
   * @throws IOException exception in IO
   */
  public static void writeLongLittleEndianPaddedOnBitWidth(long value, OutputStream out,
      int bitWidth)
      throws IOException {
    int paddedByteNum = (bitWidth + 7) / 8;
    if (paddedByteNum > 8) {
      throw new IOException(String.format(
              TOO_LONG_BYTE_FORMAT, paddedByteNum));
    }
    out.write(BytesUtils.longToBytes(value, paddedByteNum));
  }

  /**
   * read integer value using special bit from input stream.
   *
   * @param buffer byte buffer
   * @param bitWidth bit length
   * @return integer value
   * @throws IOException exception in IO
   */
  public static int readIntLittleEndianPaddedOnBitWidth(ByteBuffer buffer, int bitWidth)
      throws IOException {
    int paddedByteNum = (bitWidth + 7) / 8;
    if (paddedByteNum > 4) {
      throw new IOException(String.format(
              TOO_LONG_BYTE_FORMAT, paddedByteNum));
    }
    int result = 0;
    int offset = 0;
    while (paddedByteNum > 0) {
      int ch = ReadWriteIOUtils.read(buffer);
      result += ch << offset;
      offset += 8;
      paddedByteNum--;
    }
    return result;
  }

  /**
   * read long value using special bit from input stream.
   *
   * @param buffer byte buffer
   * @param bitWidth bit length
   * @return long long value
   * @throws IOException exception in IO
   */
  public static long readLongLittleEndianPaddedOnBitWidth(ByteBuffer buffer, int bitWidth)
      throws IOException {
    int paddedByteNum = (bitWidth + 7) / 8;
    if (paddedByteNum > 8) {
      throw new IOException(String.format(
              TOO_LONG_BYTE_FORMAT, paddedByteNum));
    }
    long result = 0;
    for (int i = 0; i < paddedByteNum; i++) {
      int ch = ReadWriteIOUtils.read(buffer);
      result <<= 8;
      result |= (ch & 0xff);
    }
    return result;
  }
}
