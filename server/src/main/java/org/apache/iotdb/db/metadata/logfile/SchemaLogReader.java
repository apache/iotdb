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
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.util.NoSuchElementException;

/**
 * This class provides the common ability to read a log storing T. The corrupted file ending will be
 * truncate during read process. If some middle part of the log file is corrupted, the read process
 * will end and the file will be marked corrupted.
 *
 * @param <T>
 */
public class SchemaLogReader<T> implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaLogReader.class);

  private final File logFile;

  private final RecordableInputStream inputStream;

  private final IDeserializer<T> deserializer;

  private T nextSchemaPlan;

  private long currentIndex = 0;
  private boolean isFileCorrupted = false;

  public SchemaLogReader(String schemaDir, String logFileName, IDeserializer<T> deserializer)
      throws IOException {
    logFile = SystemFileFactory.INSTANCE.getFile(schemaDir + File.separator + logFileName);
    inputStream = new RecordableInputStream(new BufferedInputStream(new FileInputStream(logFile)));
    this.deserializer = deserializer;
  }

  public SchemaLogReader(String logFilePath, IDeserializer<T> deserializer) throws IOException {
    logFile = SystemFileFactory.INSTANCE.getFile(logFilePath);
    inputStream = new RecordableInputStream(new BufferedInputStream(new FileInputStream(logFile)));
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
    currentIndex = inputStream.getReadBytes();
    try {
      nextSchemaPlan = deserializer.deserialize(inputStream);
    } catch (EOFException e) {
      // meet the end of the file, truncate the broken ending
      nextSchemaPlan = null;
      truncateBrokenLogs();
    } catch (IOException e) {
      // failed to read file
      nextSchemaPlan = null;
      isFileCorrupted = true;
      LOGGER.error(
          "File {} is corrupted. The uncorrupted size is {}.", logFile.getPath(), currentIndex, e);
    } catch (Exception e) {
      // error occurred when deserializing the entry
      nextSchemaPlan = null;
      try {
        if (inputStream.available() > 0) {
          // error occurred when deserializing some middle part of the file
          isFileCorrupted = true;
          LOGGER.error(
              "File {} is corrupted. The uncorrupted size is {}.",
              logFile.getPath(),
              currentIndex,
              e);
        } else {
          // the file has already been all read out, but error occurred during deserializing the
          // last entry in file ending.
          LOGGER.warn(e.getMessage(), e);
          truncateBrokenLogs();
        }
      } catch (IOException ex) {
        // failed to read file
        isFileCorrupted = true;
        LOGGER.error(
            "File {} is corrupted. The uncorrupted size is {}.",
            logFile.getPath(),
            currentIndex,
            e);
      }
    }
  }

  @Override
  public void close() throws IOException {
    inputStream.close();
    currentIndex = 0;
  }

  public boolean isFileCorrupted() {
    return isFileCorrupted;
  }

  private void truncateBrokenLogs() {
    try (FileOutputStream outputStream = new FileOutputStream(logFile, true);
        FileChannel channel = outputStream.getChannel()) {
      if (currentIndex != channel.size()) {
        LOGGER.warn(
            "The end of log file {} is corrupted. Start truncate it. The unbroken size is {}. The file size is {}.",
            logFile.getName(),
            currentIndex,
            channel.size());
        channel.truncate(currentIndex);
        channel.force(true);
      }
      isFileCorrupted = false;
    } catch (IOException e) {
      isFileCorrupted = true;
      LOGGER.error("Fail to truncate log file to size {}", currentIndex, e);
    }
  }

  public void skip(long n) throws IOException {
    ReadWriteIOUtils.skip(inputStream, n);
  }

  /** This class provides the ability to record the num of read bytes from the nested InputStream */
  private static class RecordableInputStream extends InputStream {

    private final InputStream inputStream;

    private long readBytes = 0;

    private long mark;

    public RecordableInputStream(InputStream inputStream) {
      this.inputStream = inputStream;
    }

    @Override
    public int read() throws IOException {
      int result = inputStream.read();
      readBytes += Byte.BYTES;
      return result;
    }

    @Override
    public int read(@NotNull byte[] b) throws IOException {
      int num = inputStream.read(b);
      if (num < 0) {
        return num;
      }
      readBytes += num;
      return num;
    }

    @Override
    public int read(@NotNull byte[] b, int off, int len) throws IOException {
      int num = inputStream.read(b, off, len);
      if (num < 0) {
        return num;
      }
      readBytes += num;
      return num;
    }

    @Override
    public long skip(long n) throws IOException {
      long num = inputStream.skip(n);
      readBytes += num;
      return num;
    }

    @Override
    public synchronized void mark(int readlimit) {
      this.mark = readBytes;
      inputStream.mark(readlimit);
    }

    @Override
    public boolean markSupported() {
      return inputStream.markSupported();
    }

    @Override
    public int available() throws IOException {
      return inputStream.available();
    }

    @Override
    public synchronized void reset() throws IOException {
      inputStream.reset();
      readBytes = mark;
    }

    @Override
    public void close() throws IOException {
      inputStream.close();
      readBytes = 0;
    }

    public long getReadBytes() {
      return readBytes;
    }
  }
}
