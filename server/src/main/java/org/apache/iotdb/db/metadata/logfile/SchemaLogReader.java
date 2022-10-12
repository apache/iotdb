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

package org.apache.iotdb.db.metadata.logfile;

import org.apache.iotdb.commons.file.SystemFileFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.NoSuchElementException;

public class SchemaLogReader<T> implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaLogReader.class);

  private final File logFile;

  private final DataInputStream inputStream;

  private final IDeserializer<T> deserializer;

  private T nextSchemaPlan;

  private int index = 0;
  private boolean isFileCorrupted = false;

  public SchemaLogReader(String schemaDir, String logFileName, IDeserializer<T> deserializer)
      throws IOException {
    logFile = SystemFileFactory.INSTANCE.getFile(schemaDir + File.separator + logFileName);
    inputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(logFile)));
    this.deserializer = deserializer;
  }

  public SchemaLogReader(String logFilePath, IDeserializer<T> deserializer) throws IOException {
    logFile = SystemFileFactory.INSTANCE.getFile(logFilePath);
    inputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(logFile)));
    this.deserializer = deserializer;
  }

  public boolean hasNext() {
    if (isFileCorrupted()) {
      return false;
    }

    if (nextSchemaPlan == null) {
      readNext();
      return nextSchemaPlan != null;
    } else {
      return true;
    }
  }

  public T next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    T result = nextSchemaPlan;
    nextSchemaPlan = null;
    return result;
  }

  private void readNext() {
    try {
      int logLength = inputStream.readInt();
      index += Integer.BYTES;
      if (logLength <= 0) {
        LOGGER.error(
            "File {} is corrupted. Read log length {} is negative.", logFile.getPath(), logLength);
        throw new IOException(String.format("File %s is corrupted.", logFile.getPath()));
      }

      byte[] logBuffer = new byte[logLength];
      if (logLength < inputStream.read(logBuffer, 0, logLength)) {
        throw new EOFException();
      }

      nextSchemaPlan = deserializer.deserialize(ByteBuffer.wrap(logBuffer));
      index += logLength;
    } catch (EOFException e) {
      nextSchemaPlan = null;
      truncateBrokenLogs();
    } catch (IOException e) {
      nextSchemaPlan = null;
      isFileCorrupted = true;
      LOGGER.error(
          "File {} is corrupted. The uncorrupted size is {}.", logFile.getPath(), index, e);
    }
  }

  @Override
  public void close() throws IOException {
    inputStream.close();
  }

  public boolean isFileCorrupted() {
    return isFileCorrupted;
  }

  private void truncateBrokenLogs() {
    try (FileOutputStream outputStream = new FileOutputStream(logFile, true);
        FileChannel channel = outputStream.getChannel()) {
      channel.truncate(index);
      isFileCorrupted = false;
    } catch (IOException e) {
      LOGGER.error("Fail to truncate log file to size {}", index, e);
    }
  }
}
