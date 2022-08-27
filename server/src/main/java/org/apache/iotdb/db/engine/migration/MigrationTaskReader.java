/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.migration;

import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.migration.MigrationTaskWriter.MigrationLog;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * MigrationTaskReader reads binarized MigrationLog from file using FileInputStream from head to
 * tail.
 */
public class MigrationTaskReader implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(MigrationTaskReader.class);
  private File logFile;
  private FileInputStream logFileInStream;
  private MigrationLog log;
  private long unbrokenLogsSize = 0;

  public MigrationTaskReader(String logFilePath) throws IOException {
    logFile = SystemFileFactory.INSTANCE.getFile(logFilePath);
    logFileInStream = new FileInputStream(logFile);
  }

  public MigrationTaskReader(File logFile) throws IOException {
    this.logFile = logFile;
    logFileInStream = new FileInputStream(logFile);
  }

  /** @return MigrateLog parsed from log file, null if nothing left in file */
  private MigrationLog readLog() throws IOException, IllegalPathException {
    if (logFileInStream.available() == 0) {
      return null;
    }

    MigrationLog log = new MigrationLog();

    int typeNum = ReadWriteIOUtils.readByte(logFileInStream);
    if (typeNum >= 0 && typeNum < MigrationLog.LogType.values().length)
      log.type = MigrationLog.LogType.values()[typeNum];
    else throw new IOException();
    log.taskId = ReadWriteIOUtils.readLong(logFileInStream);

    if (log.type == MigrationLog.LogType.SET) {
      log.storageGroup = new PartialPath(ReadWriteIOUtils.readString(logFileInStream));
      log.targetDirPath = ReadWriteIOUtils.readString(logFileInStream);
      log.startTime = ReadWriteIOUtils.readLong(logFileInStream);
      log.ttl = ReadWriteIOUtils.readLong(logFileInStream);
    }

    unbrokenLogsSize = logFileInStream.getChannel().position();

    return log;
  }

  public MigrationLog next() {
    MigrationLog ret = log;
    log = null;
    return ret;
  }

  public boolean hasNext() {
    if (log != null) {
      return true;
    }
    try {
      return (log = readLog()) != null;
    } catch (IOException | IllegalPathException e) {
      logger.warn("Read migration log error.");
      truncateBrokenLogs();
      log = null;
      return false;
    }
  }

  /** Keeps 0...unbrokenLogSize bytes of the Log File and discards the rest */
  private void truncateBrokenLogs() {
    try (FileOutputStream outputStream = new FileOutputStream(logFile, true);
        FileChannel channel = outputStream.getChannel()) {
      channel.truncate(unbrokenLogsSize);
    } catch (IOException e) {
      logger.error("Fail to truncate log file to size {}", unbrokenLogsSize, e);
    }
  }

  @Override
  public void close() throws Exception {
    try {
      logFileInStream.close();
    } catch (IOException e) {
      logger.error("Failed to close migrate log");
    }
  }
}
