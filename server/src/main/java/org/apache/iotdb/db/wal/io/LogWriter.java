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
package org.apache.iotdb.db.wal.io;

import org.apache.iotdb.db.wal.buffer.WALEdit;
import org.apache.iotdb.db.wal.checkpoint.Checkpoint;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.util.regex.Pattern;

/**
 * LogWriter writes the binary logs into a file, including writing {@link WALEdit} into .wal file
 * and writing {@link Checkpoint} into .checkpoint file.
 */
public abstract class LogWriter implements ILogWriter {
  public static final String FILE_PREFIX = "_";
  public static final Pattern FILE_NAME_PATTERN = Pattern.compile("_(?<versionId>\\d+)\\.");
  private static final Logger logger = LoggerFactory.getLogger(LogWriter.class);

  private final File logFile;
  private final FileOutputStream logStream;
  private final FileChannel logChannel;

  public LogWriter(File logFile) throws FileNotFoundException {
    this.logFile = logFile;
    this.logStream = new FileOutputStream(logFile, true);
    this.logChannel = this.logStream.getChannel();
  }

  @Override
  public void write(ByteBuffer buffer) throws IOException {
    buffer.flip();
    try {
      logChannel.write(buffer);
    } catch (ClosedChannelException e) {
      logger.warn("Cannot write to {}", logFile, e);
    }
  }

  @Override
  public void force() throws IOException {
    force(true);
  }

  @Override
  public void force(boolean metaData) throws IOException {
    if (logChannel != null && logChannel.isOpen()) {
      logChannel.force(metaData);
    }
  }

  @Override
  public long size() throws IOException {
    return logChannel.size();
  }

  @Override
  public void close() throws IOException {
    if (logChannel != null) {
      try {
        if (logChannel.isOpen()) {
          logChannel.force(true);
        }
      } finally {
        logChannel.close();
        logStream.close();
      }
    }
  }
}
