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

package org.apache.iotdb.commons.trigger.utils;

import org.apache.commons.io.FileUtils;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.trigger.TriggerRegistrationInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.util.zip.CRC32;

public class TriggerLogWriter implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(TriggerLogWriter.class);

  private final ByteBuffer logBuffer;
  private final File logFile;

  private FileOutputStream fileOutputStream;
  private FileChannel channel;
  private final CRC32 checkSummer = new CRC32();
  private final ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
  private final ByteBuffer checkSumBuffer = ByteBuffer.allocate(8);

  public TriggerLogWriter(String logFilePath, int triggerLogBufferSize) throws IOException {
    logBuffer = ByteBuffer.allocate(triggerLogBufferSize);
    logFile = SystemFileFactory.INSTANCE.getFile(logFilePath);

    fileOutputStream = new FileOutputStream(logFile, true);
    channel = fileOutputStream.getChannel();
  }

  public synchronized void write(TriggerRegistrationInformation registrationInformation)
      throws IOException {
    try {
      registrationInformation.serialize(logBuffer);
      write(logBuffer);
    } catch (IOException e) {
      throw new IOException(
          "Current trigger registration information is too large to write into buffer, please increase "
              + "tlog_buffer_size.",
          e);
    } finally {
      logBuffer.clear();
    }
  }

  private void write(ByteBuffer buffer) throws IOException {
    if (channel == null) {
      fileOutputStream = new FileOutputStream(logFile, true);
      channel = fileOutputStream.getChannel();
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
    } catch (ClosedChannelException ignored) {
      logger.warn("someone interrupt current thread, so no need to do write for io safety");
    }
  }

  @Override
  public void close() throws IOException {
    if (channel != null) {
      if (channel.isOpen()) {
        channel.force(true);
      }
      fileOutputStream.close();
      fileOutputStream = null;
      channel.close();
      channel = null;
    }
  }

  public void deleteLogFile() throws IOException {
    FileUtils.forceDelete(logFile);
  }
}
