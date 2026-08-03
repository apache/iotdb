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
package org.apache.iotdb.db.utils.writelog;

import org.apache.iotdb.db.i18n.DataNodeMiscMessages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;

/**
 * LogWriter writes the binary logs into a file using FileChannel together with check sums of each
 * log calculated using CRC32.
 */
public class LogWriter implements ILogWriter {
  private static final Logger logger = LoggerFactory.getLogger(LogWriter.class);

  private final File logFile;
  private FileChannel channel;
  private final CRC32 checkSummer = new CRC32();
  private final ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
  private final ByteBuffer checkSumBuffer = ByteBuffer.allocate(8);
  private final boolean forceEachWrite;

  public LogWriter(File logFile, boolean forceEachWrite) throws FileNotFoundException {
    this.logFile = logFile;
    this.forceEachWrite = forceEachWrite;

    channel = openChannel(logFile);
  }

  @Override
  public void write(ByteBuffer logBuffer) throws IOException {
    if (channel == null) {
      channel = openChannel(logFile);
    }
    logBuffer.flip();
    int logSize = logBuffer.limit();
    // 4 bytes size and 8 bytes check sum

    checkSummer.reset();
    checkSummer.update(logBuffer);
    long checkSum = checkSummer.getValue();

    logBuffer.flip();

    lengthBuffer.clear();
    checkSumBuffer.clear();
    lengthBuffer.putInt(logSize);
    checkSumBuffer.putLong(checkSum);
    lengthBuffer.flip();
    checkSumBuffer.flip();

    try {
      channel.write(lengthBuffer);
      channel.write(logBuffer);
      channel.write(checkSumBuffer);

      if (this.forceEachWrite) {
        channel.force(true);
      }
    } catch (ClosedChannelException ignored) {
      logger.warn(DataNodeMiscMessages.INTERRUPTED_NO_WRITE);
    }
  }

  @Override
  public void force() throws IOException {
    if (channel != null && channel.isOpen()) {
      channel.force(true);
    }
  }

  @Override
  public void close() throws IOException {
    if (channel != null) {
      if (channel.isOpen()) {
        channel.force(true);
      }
      channel.close();
      channel = null;
    }
  }

  @Override
  public String toString() {
    return "LogWriter{" + "logFile=" + logFile + '}';
  }

  private static FileChannel openChannel(File file) throws FileNotFoundException {
    try {
      return FileChannel.open(
          file.toPath(),
          StandardOpenOption.CREATE,
          StandardOpenOption.WRITE,
          StandardOpenOption.APPEND);
    } catch (IOException e) {
      FileNotFoundException exception = new FileNotFoundException(e.getMessage());
      exception.initCause(e);
      throw exception;
    }
  }
}
