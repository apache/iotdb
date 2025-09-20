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

import org.apache.iotdb.commons.auth.entity.DatabasePrivilege;
import org.apache.iotdb.commons.auth.entity.PathPrivilege;
import org.apache.iotdb.commons.auth.entity.TablePrivilege;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;

import com.google.common.base.Supplier;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

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
   * Write a long (8-byte) into the given stream.
   *
   * @param outputStream the destination to insert.
   * @param i the long value to be written.
   * @param encodingBufferLocal a ThreadLocal buffer may be passed to avoid frequent memory
   *     allocations. A null may also be passed to use a local buffer.
   * @throws IOException when an exception raised during operating the stream.
   */
  public static void writeLong(
      OutputStream outputStream, long i, ThreadLocal<ByteBuffer> encodingBufferLocal)
      throws IOException {

    ByteBuffer encodingBuffer;
    if (encodingBufferLocal != null) {
      encodingBuffer = encodingBufferLocal.get();
      if (encodingBuffer == null) {
        // 8 bytes is exactly what we need for a long
        encodingBuffer = ByteBuffer.allocate(8);
        encodingBufferLocal.set(encodingBuffer);
      }
    } else {
      encodingBuffer = ByteBuffer.allocate(8);
    }

    encodingBuffer.clear();
    encodingBuffer.putLong(i);
    outputStream.write(encodingBuffer.array(), 0, Long.BYTES);
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
    int length = inputStream.readInt();
    return readString(inputStream, encoding, strBufferLocal, length);
  }

  public static String readString(
      DataInputStream inputStream, String encoding, ThreadLocal<byte[]> strBufferLocal, int length)
      throws IOException {
    byte[] strBuffer;
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
      throws IOException, IllegalPathException {
    String path = IOUtils.readString(inputStream, encoding, strBufferLocal);
    PathPrivilege pathPrivilege = new PathPrivilege(new PartialPath(path));
    int privileges = inputStream.readInt();
    pathPrivilege.setAllPrivileges(privileges);
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
    writeString(outputStream, pathPrivilege.getPath().getFullPath(), encoding, encodingBufferLocal);
    writeInt(outputStream, pathPrivilege.getAllPrivileges(), encodingBufferLocal);
  }

  public static DatabasePrivilege readRelationalPrivilege(
      DataInputStream inputStream, String encoding, ThreadLocal<byte[]> strBufferLocal)
      throws IOException {
    String databaseName = IOUtils.readString(inputStream, encoding, strBufferLocal);
    DatabasePrivilege databasePrivilege = new DatabasePrivilege(databaseName);
    databasePrivilege.setPrivileges(inputStream.readInt());
    int tableNum = inputStream.readInt();
    for (int i = 0; i < tableNum; i++) {
      String tableName = IOUtils.readString(inputStream, encoding, strBufferLocal);
      TablePrivilege tablePrivilege = new TablePrivilege(tableName);
      tablePrivilege.setPrivileges(inputStream.readInt());
      databasePrivilege.getTablePrivilegeMap().put(tableName, tablePrivilege);
    }
    return databasePrivilege;
  }

  public static void writeObjectPrivilege(
      OutputStream outputStream,
      DatabasePrivilege databasePrivilege,
      String encoding,
      ThreadLocal<ByteBuffer> encodingBufferLocal)
      throws IOException {
    writeString(outputStream, databasePrivilege.getDatabaseName(), encoding, encodingBufferLocal);
    writeInt(outputStream, databasePrivilege.getAllPrivileges(), encodingBufferLocal);
    writeInt(outputStream, databasePrivilege.getTablePrivilegeMap().size(), encodingBufferLocal);
    for (Map.Entry<String, TablePrivilege> entry :
        databasePrivilege.getTablePrivilegeMap().entrySet()) {
      writeString(outputStream, entry.getValue().getTableName(), encoding, encodingBufferLocal);
      writeInt(outputStream, entry.getValue().getAllPrivileges(), encodingBufferLocal);
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

  /**
   * Retry a method at most 'maxRetry' times, each time calling 'retryFunc' and passing the result
   * to 'validator'. If 'validator' returns true, returns the result.
   *
   * @param maxRetry maximum number of retires
   * @param retryIntervalMS retry interval in milliseconds
   * @param retryFunc function to be retried
   * @param validator validating the result of 'retryFunc'
   * @return true if the result from 'retryFunc' passes validation, false if all retries fail or is
   *     interrupted
   * @param <T>
   */
  public static <T> Optional<T> retryNoException(
      int maxRetry, long retryIntervalMS, Supplier<T> retryFunc, Function<T, Boolean> validator) {
    for (int i = 0; i < maxRetry; i++) {
      T result = retryFunc.get();
      if (Boolean.TRUE.equals(validator.apply(result))) {
        return Optional.of(result);
      }
      try {
        Thread.sleep(retryIntervalMS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return Optional.empty();
      }
    }
    return Optional.empty();
  }
}
