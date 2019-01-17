/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.tsfile.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.enums.TSFreqType;

/**
 * ConverterUtils is a utility class. It provide conversion between normal datatype and byte array.
 *
 * @author East
 */
public class ReadWriteIOUtils {

  private static int SHORT_LEN = 2;
  private static int INT_LEN = 4;
  private static int LONG_LEN = 8;
  private static int DOUBLE_LEN = 8;
  private static int FLOAT_LEN = 4;
  private static int BOOLEAN_LEN = 1;

  /**
   * read a bool from inputStream.
   */
  public static boolean readBool(InputStream inputStream) throws IOException {
    int flag = inputStream.read();
    return flag == 1;
  }

  /**
   * read a bool from byteBuffer.
   */
  public static boolean readBool(ByteBuffer buffer) {
    byte a = buffer.get();
    return a == 1;
  }

  /**
   * write if the object not equals null. Eg, object eauals null, then write false.
   */
  public static int writeIsNull(Object object, OutputStream outputStream) throws IOException {
    return write(object != null, outputStream);
  }

  /**
   * write if the object not equals null. Eg, object eauals null, then write false.
   */
  public static int writeIsNull(Object object, ByteBuffer buffer) {
    return write(object != null, buffer);
  }

  /**
   * read a bool from byteBuffer.
   */
  public static boolean readIsNull(InputStream inputStream) throws IOException {
    return readBool(inputStream);
  }

  /**
   * read a bool from byteBuffer.
   */
  public static boolean readIsNull(ByteBuffer buffer) {
    return readBool(buffer);
  }

  /**
   * write a int value to outputStream according to flag. If flag is true, write 1, else write 0.
   */
  public static int write(Boolean flag, OutputStream outputStream) throws IOException {
    if (flag) {
      outputStream.write(1);
    } else {
      outputStream.write(0);
    }
    return 1;
  }

  /**
   * write a byte to byteBuffer according to flag. If flag is true, write 1, else write 0.
   */
  public static int write(Boolean flag, ByteBuffer buffer) {
    byte a;
    if (flag) {
      a = 1;
    } else {
      a = 0;
    }

    buffer.put(a);
    return 1;
  }

  /**
   * write a byte n.
   *
   * @return The number of bytes used to represent a {@code byte} value in two's complement binary
   * form.
   */
  public static int write(byte n, OutputStream outputStream) throws IOException {
    outputStream.write(n);
    return Byte.BYTES;
  }

  /**
   * write a short n.
   *
   * @return The number of bytes used to represent n.
   */
  public static int write(short n, OutputStream outputStream) throws IOException {
    byte[] bytes = BytesUtils.shortToBytes(n);
    outputStream.write(bytes);
    return bytes.length;
  }

  /**
   * write a byte n to byteBuffer.
   *
   * @return The number of bytes used to represent a {@code byte} value in two's complement binary
   * form.
   */
  public static int write(byte n, ByteBuffer buffer) {
    buffer.put(n);
    return Byte.BYTES;
  }

  /**
   * write a short n to byteBuffer.
   *
   * @return The number of bytes used to represent n.
   */
  public static int write(short n, ByteBuffer buffer) {
    buffer.putShort(n);
    return SHORT_LEN;
  }

  /**
   * write a int n to outputStream.
   *
   * @return The number of bytes used to represent n.
   */
  public static int write(int n, OutputStream outputStream) throws IOException {
    byte[] bytes = BytesUtils.intToBytes(n);
    outputStream.write(bytes);
    return bytes.length;
  }

  /**
   * write a int n to byteBuffer.
   *
   * @return The number of bytes used to represent n.
   */
  public static int write(int n, ByteBuffer buffer) {
    buffer.putInt(n);
    return INT_LEN;
  }

  /**
   * write a float n to outputStream.
   *
   * @return The number of bytes used to represent n.
   */
  public static int write(float n, OutputStream outputStream) throws IOException {
    byte[] bytes = BytesUtils.floatToBytes(n);
    outputStream.write(bytes);
    return FLOAT_LEN;
  }

  /**
   * write a double n to outputStream.
   *
   * @return The number of bytes used to represent n.
   */
  public static int write(double n, OutputStream outputStream) throws IOException {
    byte[] bytes = BytesUtils.doubleToBytes(n);
    outputStream.write(bytes);
    return DOUBLE_LEN;
  }

  /**
   * write a long n to outputStream.
   *
   * @return The number of bytes used to represent n.
   */
  public static int write(long n, OutputStream outputStream) throws IOException {
    byte[] bytes = BytesUtils.longToBytes(n);
    outputStream.write(bytes);
    return bytes.length;
  }

  /**
   * write a long n to byteBuffer.
   */
  public static int write(long n, ByteBuffer buffer) {
    buffer.putLong(n);
    return LONG_LEN;
  }

  /**
   * write string to outputStream.
   *
   * @return the length of string represented by byte[].
   */
  public static int write(String s, OutputStream outputStream) throws IOException {
    int len = 0;
    len += write(s.length(), outputStream);
    byte[] bytes = s.getBytes();
    outputStream.write(bytes);
    len += bytes.length;
    return len;
  }

  /**
   * write string to byteBuffer.
   *
   * @return the length of string represented by byte[].
   */
  public static int write(String s, ByteBuffer buffer) {
    int len = 0;
    len += write(s.length(), buffer);
    byte[] bytes = s.getBytes();
    buffer.put(bytes);
    len += bytes.length;
    return len;
  }

  /**
   * write byteBuffer.capacity and byteBuffer.array to outputStream.
   */
  public static int write(ByteBuffer byteBuffer, OutputStream outputStream) throws IOException {
    int len = 0;
    len += write(byteBuffer.capacity(), outputStream);
    byte[] bytes = byteBuffer.array();
    outputStream.write(bytes);
    len += bytes.length;
    return len;
  }

  /**
   * write byteBuffer.capacity and byteBuffer.array to byteBuffer.
   */
  public static int write(ByteBuffer byteBuffer, ByteBuffer buffer) {
    int len = 0;
    len += write(byteBuffer.capacity(), buffer);
    byte[] bytes = byteBuffer.array();
    buffer.put(bytes);
    len += bytes.length;
    return len;
  }

  /**
   * CompressionType.
   */
  public static int write(CompressionType compressionType, OutputStream outputStream)
      throws IOException {
    short n = compressionType.serialize();
    return write(n, outputStream);
  }

  /**
   * write compressionType to byteBuffer.
   */
  public static int write(CompressionType compressionType, ByteBuffer buffer) {
    short n = compressionType.serialize();
    return write(n, buffer);
  }

  /**
   * TSDataType.
   */
  public static int write(TSDataType dataType, OutputStream outputStream) throws IOException {
    short n = dataType.serialize();
    return write(n, outputStream);
  }

  public static int write(TSDataType dataType, ByteBuffer buffer) {
    short n = dataType.serialize();
    return write(n, buffer);
  }

  /**
   * TSEncoding.
   */
  public static int write(TSEncoding encoding, OutputStream outputStream) throws IOException {
    short n = encoding.serialize();
    return write(n, outputStream);
  }

  public static int write(TSEncoding encoding, ByteBuffer buffer) {
    short n = encoding.serialize();
    return write(n, buffer);
  }

  /**
   * TSFreqType.
   */
  public static int write(TSFreqType freqType, OutputStream outputStream) throws IOException {
    short n = freqType.serialize();
    return write(n, outputStream);
  }

  public static int write(TSFreqType freqType, ByteBuffer buffer) {
    short n = freqType.serialize();
    return write(n, buffer);
  }

  /**
   * read a short var from inputStream.
   */
  public static short readShort(InputStream inputStream) throws IOException {
    byte[] bytes = new byte[SHORT_LEN];
    inputStream.read(bytes);
    return BytesUtils.bytesToShort(bytes);
  }

  /**
   * read a short var from byteBuffer.
   */
  public static short readShort(ByteBuffer buffer) {
    short n = buffer.getShort();
    return n;
  }

  /**
   * read a float var from inputStream.
   */
  public static float readFloat(InputStream inputStream) throws IOException {
    byte[] bytes = new byte[FLOAT_LEN];
    inputStream.read(bytes);
    return BytesUtils.bytesToFloat(bytes);
  }

  /**
   * read a float var from byteBuffer.
   */
  public static float readFloat(ByteBuffer byteBuffer) {
    byte[] bytes = new byte[FLOAT_LEN];
    byteBuffer.get(bytes);
    return BytesUtils.bytesToFloat(bytes);
  }

  /**
   * read a double var from inputStream.
   */
  public static double readDouble(InputStream inputStream) throws IOException {
    byte[] bytes = new byte[DOUBLE_LEN];
    inputStream.read(bytes);
    return BytesUtils.bytesToDouble(bytes);
  }

  /**
   * read a double var from byteBuffer.
   */
  public static double readDouble(ByteBuffer byteBuffer) {
    byte[] bytes = new byte[DOUBLE_LEN];
    byteBuffer.get(bytes);
    return BytesUtils.bytesToDouble(bytes);
  }

  /**
   * read a int var from inputStream.
   */
  public static int readInt(InputStream inputStream) throws IOException {
    byte[] bytes = new byte[INT_LEN];
    inputStream.read(bytes);
    return BytesUtils.bytesToInt(bytes);
  }

  /**
   * read a int var from byteBuffer.
   */
  public static int readInt(ByteBuffer buffer) {
    int n = buffer.getInt();
    return n;
  }

  /**
   * read an unsigned byte(0 ~ 255) as InputStream does.
   *
   * @return the byte or -1(means there is no byte to read)
   */
  public static int read(ByteBuffer buffer) {
    if (!buffer.hasRemaining()) {
      return -1;
    }
    return buffer.get() & 0xFF;
  }

  /**
   * read a long var from inputStream.
   */
  public static long readLong(InputStream inputStream) throws IOException {
    byte[] bytes = new byte[LONG_LEN];
    inputStream.read(bytes);
    return BytesUtils.bytesToLong(bytes);
  }

  /**
   * read a long var from byteBuffer.
   */
  public static long readLong(ByteBuffer buffer) {
    long n = buffer.getLong();
    return n;
  }

  /**
   * read string from inputStream.
   */
  public static String readString(InputStream inputStream) throws IOException {
    int strLength = readInt(inputStream);
    byte[] bytes = new byte[strLength];
    inputStream.read(bytes, 0, strLength);
    return new String(bytes, 0, strLength);
  }

  /**
   * read string from byteBuffer.
   */
  public static String readString(ByteBuffer buffer) {
    int strLength = readInt(buffer);
    byte[] bytes = new byte[strLength];
    buffer.get(bytes, 0, strLength);
    return new String(bytes, 0, strLength);
  }

  /**
   * read string from byteBuffer with user define length.
   */
  public static String readStringWithoutLength(ByteBuffer buffer, int length) {
    byte[] bytes = new byte[length];
    buffer.get(bytes, 0, length);
    return new String(bytes, 0, length);
  }

  public static ByteBuffer getByteBuffer(String s) {
    return ByteBuffer.wrap(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }

  public static ByteBuffer getByteBuffer(int i) {
    return ByteBuffer.allocate(4).putInt(0, i);
  }

  public static ByteBuffer getByteBuffer(long n) {
    return ByteBuffer.allocate(8).putLong(0, n);
  }

  public static ByteBuffer getByteBuffer(float f) {
    return ByteBuffer.allocate(4).putFloat(0, f);
  }

  public static ByteBuffer getByteBuffer(double d) {
    return ByteBuffer.allocate(8).putDouble(0, d);
  }

  public static ByteBuffer getByteBuffer(boolean i) {
    return ByteBuffer.allocate(1).put(i ? (byte) 1 : (byte) 0);
  }

  public static String readStringFromDirectByteBuffer(ByteBuffer buffer)
      throws CharacterCodingException {
    return java.nio.charset.StandardCharsets.UTF_8.newDecoder().decode(buffer.duplicate())
        .toString();
  }

  /**
   * unlike InputStream.read(bytes), this method makes sure that you can read length bytes or reach
   * to the end of the stream.
   */
  public static byte[] readBytes(InputStream inputStream, int length) throws IOException {
    byte[] bytes = new byte[length];
    int offset = 0;
    int len = 0;
    while (bytes.length - offset > 0
        && (len = inputStream.read(bytes, offset, bytes.length - offset)) != -1) {
      offset += len;
    }
    return bytes;
  }

  /**
   * unlike InputStream.read(bytes), this method makes sure that you can read length bytes or reach
   * to the end of the stream.
   */
  public static byte[] readBytesWithSelfDescriptionLength(InputStream inputStream)
      throws IOException {
    int length = readInt(inputStream);
    return readBytes(inputStream, length);
  }

  /**
   * read bytes from inputStream, this method makes sure that you can read length bytes or reach to
   * the end of the stream.
   */
  public static ByteBuffer readByteBufferWithSelfDescriptionLength(InputStream inputStream)
      throws IOException {
    byte[] bytes = readBytesWithSelfDescriptionLength(inputStream);
    ByteBuffer byteBuffer = ByteBuffer.allocate(bytes.length);
    byteBuffer.put(bytes);
    byteBuffer.flip();
    return byteBuffer;
  }

  /**
   * read bytes from byteBuffer, this method makes sure that you can read length bytes or reach to
   * the end of the buffer.
   */
  public static ByteBuffer readByteBufferWithSelfDescriptionLength(ByteBuffer buffer) {
    int byteLength = readInt(buffer);
    byte[] bytes = new byte[byteLength];
    buffer.get(bytes);
    ByteBuffer byteBuffer = ByteBuffer.allocate(byteLength);
    byteBuffer.put(bytes);
    byteBuffer.flip();
    return byteBuffer;
  }

  /**
   * read bytes from buffer with offset position to the end of buffer.
   */
  public static int readAsPossible(FileChannel channel, long position, ByteBuffer buffer)
      throws IOException {
    int length = 0;
    int read;
    while (buffer.hasRemaining() && (read = channel.read(buffer, position)) != -1) {
      length += read;
      position += read;
      read = channel.read(buffer, position);
    }
    return length;
  }

  /**
   * read util to the end of buffer.
   */
  public static int readAsPossible(FileChannel channel, ByteBuffer buffer) throws IOException {
    int length = 0;
    int read;
    while (buffer.hasRemaining() && (read = channel.read(buffer)) != -1) {
      length += read;
    }
    return length;
  }

  /**
   * read util to the end of buffer or up to len.
   */
  public static int readAsPossible(FileChannel channel, ByteBuffer buffer, int len)
      throws IOException {
    int length = 0;
    int limit = buffer.limit();
    if (buffer.remaining() > len) {
      buffer.limit(buffer.position() + len);
    }
    int read;
    while (length < len && buffer.hasRemaining() && (read = channel.read(buffer)) != -1) {
      length += read;
    }
    buffer.limit(limit);
    return length;
  }

  /**
   * read bytes from buffer with offset position to the end of buffer or up to len.
   */
  public static int readAsPossible(FileChannel channel, ByteBuffer target, long offset, int len)
      throws IOException {
    int length = 0;
    int limit = target.limit();
    if (target.remaining() > len) {
      target.limit(target.position() + len);
    }
    int read;
    while (length < len && target.hasRemaining() && (read = channel.read(target, offset)) != -1) {
      length += read;
      offset += read;
    }
    target.limit(limit);
    return length;
  }

  /**
   * List&lt;Integer&gt;.
   */
  public static List<Integer> readIntegerList(InputStream inputStream) throws IOException {
    int size = readInt(inputStream);
    if (size <= 0) {
      return null;
    }

    List<Integer> list = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      list.add(readInt(inputStream));
    }

    return list;
  }

  /**
   * read integer list with self define length.
   */
  public static List<Integer> readIntegerList(ByteBuffer buffer) throws IOException {
    int size = readInt(buffer);
    if (size <= 0) {
      return null;
    }

    List<Integer> list = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      list.add(readInt(buffer));
    }
    return list;
  }

  /**
   * read string list with self define length.
   */
  public static List<String> readStringList(InputStream inputStream) throws IOException {
    List<String> list = new ArrayList<>();
    int size = readInt(inputStream);

    for (int i = 0; i < size; i++) {
      list.add(readString(inputStream));
    }

    return list;
  }

  /**
   * read string list with self define length.
   */
  public static List<String> readStringList(ByteBuffer buffer) throws IOException {
    int size = readInt(buffer);
    if (size <= 0) {
      return null;
    }

    List<String> list = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      list.add(readString(buffer));
    }

    return list;
  }

  public static CompressionType readCompressionType(InputStream inputStream) throws IOException {
    short n = readShort(inputStream);
    return CompressionType.deserialize(n);
  }

  public static CompressionType readCompressionType(ByteBuffer buffer) {
    short n = readShort(buffer);
    return CompressionType.deserialize(n);
  }

  public static TSDataType readDataType(InputStream inputStream) throws IOException {
    short n = readShort(inputStream);
    return TSDataType.deserialize(n);
  }

  public static TSDataType readDataType(ByteBuffer buffer) {
    short n = readShort(buffer);
    return TSDataType.deserialize(n);
  }

  public static TSEncoding readEncoding(InputStream inputStream) throws IOException {
    short n = readShort(inputStream);
    return TSEncoding.deserialize(n);
  }

  public static TSEncoding readEncoding(ByteBuffer buffer) {
    short n = readShort(buffer);
    return TSEncoding.deserialize(n);
  }

  public static TSFreqType readFreqType(InputStream inputStream) throws IOException {
    short n = readShort(inputStream);
    return TSFreqType.deserialize(n);
  }

  public static TSFreqType readFreqType(ByteBuffer buffer) {
    short n = readShort(buffer);
    return TSFreqType.deserialize(n);
  }
}
