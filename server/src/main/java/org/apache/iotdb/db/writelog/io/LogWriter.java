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
package org.apache.iotdb.db.writelog.io;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.CRC32;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;

/**
 * LogWriter writes the binarized logs into a file using FileChannel together with check sums of
 * each log calculated using CRC32.
 */
public class LogWriter implements ILogWriter {

  private File logFile;
  private FileOutputStream fileOutputStream;
  private FileChannel channel;
  private CRC32 checkSummer = new CRC32();
  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
  private ByteBuffer checkSumBuffer = ByteBuffer.allocate(8);

  public LogWriter(String logFilePath) {
    logFile = SystemFileFactory.INSTANCE.getFile(logFilePath);
  }

  public LogWriter(File logFile) {
    this.logFile = logFile;
  }

  @Override
  public void write(ByteBuffer logBuffer) throws IOException {
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

    channel.write(lengthBuffer);
    channel.write(logBuffer);
    channel.write(checkSumBuffer);

    if (config.getForceWalPeriodInMs() == 0) {
      channel.force(true);
    }
  }

  @Override
  public void force() throws IOException {
    if (channel != null) {
      channel.force(true);
    }
  }

  @Override
  public void close() throws IOException {
    if (channel != null) {
      fileOutputStream.close();
      fileOutputStream = null;
      channel.close();
      channel = null;
    }
  }
}
