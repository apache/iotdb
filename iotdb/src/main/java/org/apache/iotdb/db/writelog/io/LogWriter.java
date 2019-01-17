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
package org.apache.iotdb.db.writelog.io;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.zip.CRC32;

public class LogWriter implements ILogWriter {

  private File logFile;
  private FileChannel outputStream;
  private CRC32 checkSummer = new CRC32();

  public LogWriter(String logFilePath) {
    logFile = new File(logFilePath);
  }

  @Override
  public void write(List<byte[]> logCache) throws IOException {
    if (outputStream == null) {
      outputStream = new FileOutputStream(logFile, true).getChannel();
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
    outputStream.force(true);
  }

  @Override
  public void close() throws IOException {
    if (outputStream != null) {
      outputStream.close();
      outputStream = null;
    }
  }
}
