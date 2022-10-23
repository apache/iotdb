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
package org.apache.iotdb.lsm.wal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;

/** write records to wal file */
public class WALWriter implements IWALWriter {
  private static final Logger logger = LoggerFactory.getLogger(WALWriter.class);
  // wal file
  private File logFile;
  private FileOutputStream fileOutputStream;
  private FileChannel channel;
  // 4-bit buffer
  private final ByteBuffer lengthBuffer;
  // save wal record serialized byte data
  private final ByteBuffer walBuffer;
  private final boolean forceEachWrite;

  public WALWriter(File logFile, int walBufferSize, boolean forceEachWrite)
      throws FileNotFoundException {
    this.logFile = logFile;
    this.forceEachWrite = forceEachWrite;
    fileOutputStream = new FileOutputStream(logFile, true);
    channel = fileOutputStream.getChannel();
    lengthBuffer = ByteBuffer.allocate(4);
    walBuffer = ByteBuffer.allocate(walBufferSize);
  }

  /**
   * write walRecord to wal file
   *
   * @param walRecord record to be written
   * @throws IOException
   */
  @Override
  public void write(IWALRecord walRecord) throws IOException {
    if (channel == null) {
      fileOutputStream = new FileOutputStream(logFile, true);
      channel = fileOutputStream.getChannel();
    }
    walBuffer.clear();
    walRecord.serialize(walBuffer);
    walBuffer.flip();
    int logSize = walBuffer.limit();
    lengthBuffer.clear();
    lengthBuffer.putInt(logSize);
    lengthBuffer.flip();

    try {
      channel.write(lengthBuffer);
      channel.write(walBuffer);

      if (this.forceEachWrite) {
        channel.force(true);
      }
    } catch (ClosedChannelException ignored) {
      logger.warn("someone interrupt current thread, so no need to do write for io safety");
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
      fileOutputStream.close();
      fileOutputStream = null;
      channel.close();
      channel = null;
    }
  }

  @Override
  public String toString() {
    return "WALLogWriter{" + "logFile=" + logFile + '}';
  }
}
