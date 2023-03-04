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

import org.apache.iotdb.db.auth.entity.PathPrivilege;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class IOUtils {

  private IOUtils() {}

  /**
   * Write a string into the given stream.
   *
   * @param outputStream the destination to insert.
   * @param str the string to be written.
   * @param encoding string encoding like 'utf-8'.
   * @param encodingBufferLocal a ThreadLocal buffer may be passed to avoid frequently memory
   *     allocations. A null may also be passed to use a local buffer.
   * @throws IOException when an exception raised during operating the stream.
   */
  public static void writeString(
      OutputStream outputStream,
      String str,
      String encoding,
      ThreadLocal<ByteBuffer> encodingBufferLocal)
      throws IOException {
    if (str != null) {
      byte[] strBuffer = str.getBytes(encoding);
      writeInt(outputStream, strBuffer.length, encodingBufferLocal);
      outputStream.write(strBuffer);
    } else {
      writeInt(outputStream, 0, encodingBufferLocal);
    }
  }

  /**
   * Write an integer into the given stream.
   *
   * @param outputStream the destination to insert.
   * @param i the integer to be written.
   * @param encodingBufferLocal a ThreadLocal buffer may be passed to avoid frequently memory
   *     allocations. A null may also be passed to use a local buffer.
   * @throws IOException when an exception raised during operating the stream.
   */
  public static void writeInt(
      OutputStream outputStream, int i, ThreadLocal<ByteBuffer> encodingBufferLocal)
      throws IOException {
    ByteBuffer encodingBuffer;
    if (encodingBufferLocal != null) {
      encodingBuffer = encodingBufferLocal.get();
      if (encodingBuffer == null) {
        // set to 8 because this buffer may be applied to other types
        encodingBuffer = ByteBuffer.allocate(8);
        encodingBufferLocal.set(encodingBuffer);
      }
    } else {
      encodingBuffer = ByteBuffer.allocate(4);
    }
    encodingBuffer.clear();
    encodingBuffer.putInt(i);
    outputStream.write(encodingBuffer.array(), 0, Integer.BYTES);
  }

  /**
   * Read a string from the given stream.
   *
   * @param inputStream the source to read.
   * @param encoding string encoding like 'utf-8'.
   * @param strBufferLocal a ThreadLocal buffer may be passed to avoid frequently memory
   *     allocations. A null may also be passed to use a local buffer.
   * @return a string read from the stream.
   * @throws IOException when an exception raised during operating the stream.
   */
  public static String readString(
      DataInputStream inputStream, String encoding, ThreadLocal<byte[]> strBufferLocal)
      throws IOException {
    byte[] strBuffer;
    int length = inputStream.readInt();
    if (length > 0) {
      if (strBufferLocal != null) {
        strBuffer = strBufferLocal.get();
        if (strBuffer == null || length > strBuffer.length) {
          strBuffer = new byte[length];
          strBufferLocal.set(strBuffer);
        }
      } else {
        strBuffer = new byte[length];
      }

      inputStream.read(strBuffer, 0, length);
      return new String(strBuffer, 0, length, encoding);
    }
    return null;
  }

  /**
   * Read a PathPrivilege from the given stream.
   *
   * @param inputStream the source to read.
   * @param encoding string encoding like 'utf-8'.
   * @param strBufferLocal a ThreadLocal buffer may be passed to avoid frequently memory
   *     allocations. A null may also be passed to use a local buffer.
   * @return a PathPrivilege read from the stream.
   * @throws IOException when an exception raised during operating the stream.
   */
  public static PathPrivilege readPathPrivilege(
      DataInputStream inputStream, String encoding, ThreadLocal<byte[]> strBufferLocal)
      throws IOException {
    String path = IOUtils.readString(inputStream, encoding, strBufferLocal);
    int privilegeNum = inputStream.readInt();
    PathPrivilege pathPrivilege = new PathPrivilege(path);
    for (int i = 0; i < privilegeNum; i++) {
      pathPrivilege.getPrivileges().add(inputStream.readInt());
    }
    return pathPrivilege;
  }

  /**
   * Write a PathPrivilege to the given stream.
   *
   * @param outputStream the destination to insert.
   * @param pathPrivilege the PathPrivilege to be written.
   * @param encoding string encoding like 'utf-8'.
   * @param encodingBufferLocal a ThreadLocal buffer may be passed to avoid frequently memory
   *     allocations. A null may also be passed to use a local buffer.
   * @throws IOException when an exception raised during operating the stream.
   */
  public static void writePathPrivilege(
      OutputStream outputStream,
      PathPrivilege pathPrivilege,
      String encoding,
      ThreadLocal<ByteBuffer> encodingBufferLocal)
      throws IOException {
    writeString(outputStream, pathPrivilege.getPath(), encoding, encodingBufferLocal);
    writeInt(outputStream, pathPrivilege.getPrivileges().size(), encodingBufferLocal);
    for (Integer i : pathPrivilege.getPrivileges()) {
      writeInt(outputStream, i, encodingBufferLocal);
    }
  }

  /**
   * Replace newFile with oldFile. If the file system does not support atomic file replacement then
   * delete the old file first.
   *
   * @param newFile the new file.
   * @param oldFile the file to be replaced.
   */
  public static void replaceFile(File newFile, File oldFile) throws IOException {
    if (!newFile.renameTo(oldFile)) {
      // some OSs need to delete the old file before renaming to it
      if (!oldFile.delete()) {
        throw new IOException(String.format("Cannot delete old user file : %s", oldFile.getPath()));
      }
      if (!newFile.renameTo(oldFile)) {
        throw new IOException(
            String.format("Cannot replace old user file with new one : %s", newFile.getPath()));
      }
    }
  }

  public static ByteBuffer clone(ByteBuffer original) {
    ByteBuffer clone = ByteBuffer.allocate(original.capacity());
    original.rewind();
    clone.put(original);
    original.rewind();
    clone.flip();
    return clone;
  }
}
