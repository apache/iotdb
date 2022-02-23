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
package org.apache.iotdb.db.doublewrite.log;

import org.apache.iotdb.db.writelog.io.ILogWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;

/**
 * DoubleWriteLogWriter is used for persisting PhysicalPlan that are transmitted failure. Note that
 * there is only one DoubleWriteLogWriter for one DoubleWriteLog
 */
public class DoubleWriteLogWriter implements ILogWriter {

  private static final Logger LOGGER = LoggerFactory.getLogger(DoubleWriteLogWriter.class);

  // log file and channel
  private final File logFile;
  private FileOutputStream fileOutputStream;
  private FileChannel writeLogChannel;

  private final ByteBuffer lengthBuffer = ByteBuffer.allocate(4);

  public DoubleWriteLogWriter(File logFile) {
    this.logFile = logFile;
    try {
      // open file channel
      fileOutputStream = new FileOutputStream(logFile);
      writeLogChannel = fileOutputStream.getChannel();
    } catch (IOException e) {
      LOGGER.error("DoubleWriteLogWriter create double write log failed", e);
    }
  }

  public long size() {
    return logFile.length();
  }

  @Override
  public void write(ByteBuffer logBuffer) throws IOException {
    // first write the length of PhysicalPlan, then write the PhysicalPlan
    lengthBuffer.clear();
    lengthBuffer.putInt(logBuffer.limit());
    lengthBuffer.position(0);

    writeLogChannel.write(lengthBuffer);
    writeLogChannel.write(logBuffer);
  }

  @Override
  public void force() throws IOException {
    // empty body
  }

  /* release file channel and output stream */
  @Override
  public void close() throws IOException {
    writeLogChannel.force(true);
    try {
      TimeUnit.SECONDS.sleep(10);
    } catch (InterruptedException ignore) {
      // ignore
    }
    writeLogChannel.close();
    writeLogChannel = null;
    fileOutputStream.close();
    fileOutputStream = null;
  }
}
