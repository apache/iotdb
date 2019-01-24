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
package org.apache.iotdb.db.writelog.io;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.zip.CRC32;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

public class LogWriter implements ILogWriter {

  private File logFile;
  private FileOutputStream fileOutputStream;
  private FileChannel outputStream;
  private CRC32 checkSummer = new CRC32();
  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  public LogWriter(String logFilePath) {
    logFile = new File(logFilePath);
  }

  @Override
  public void write(List<byte[]> logCache) throws IOException {
    if (outputStream == null) {
      fileOutputStream = new FileOutputStream(logFile, true);
      outputStream = fileOutputStream.getChannel();
    }
    int totalSize = 0;
    for (byte[] bytes : logCache) {
      totalSize += 4 + 8 + bytes.length;
    }
    ByteBuffer buffer = ByteBuffer.allocate(totalSize);
    for (byte[] bytes : logCache) {
      buffer.putInt(bytes.length);
      checkSummer.reset();
      checkSummer.update(bytes);
      buffer.putLong(checkSummer.getValue());
      buffer.put(bytes);
    }
    buffer.flip();
    outputStream.write(buffer);
    if (config.forceWalPeriodInMs == 0) {
      outputStream.force(true);
    }
  }

  @Override
  public void force() throws IOException {
    if (outputStream != null) {
      outputStream.force(true);
    }
  }

  @Override
  public void close() throws IOException {
    if (outputStream != null) {
      fileOutputStream.close();
      fileOutputStream = null;
      outputStream.close();
      outputStream = null;
    }
  }
}
