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
package org.apache.iotdb.db.engine.archiving;

import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * ArchivingOperateReader reads binarized ArchivingOperate from file using FileInputStream from head
 * to tail.
 */
public class ArchivingOperateReader implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(ArchivingOperateReader.class);
  private final File logFile;
  private FileInputStream logFileInStream;
  private ArchivingOperate operate;
  private long unbrokenLogsSize = 0;

  public ArchivingOperateReader(String logFilePath) throws IOException {
    this.logFile = SystemFileFactory.INSTANCE.getFile(logFilePath);
    logFileInStream = new FileInputStream(logFile);
  }

  public ArchivingOperateReader(File logFile) throws IOException {
    this.logFile = logFile;
    logFileInStream = new FileInputStream(logFile);
  }

  /** @return ArchivingOperate parsed from log file, null if nothing left in file */
  private ArchivingOperate readOperate() {
    try {
      ArchivingOperate log = ArchivingOperate.deserialize(logFileInStream);

      unbrokenLogsSize = logFileInStream.getChannel().position();
      return log;
    } catch (IllegalPathException | IOException e) {
      return null;
    }
  }

  public ArchivingOperate next() {
    ArchivingOperate ret = operate;
    operate = null;
    return ret;
  }

  public boolean hasNext() {
    if (operate != null) {
      return true;
    }

    // try reading
    operate = readOperate();

    if (operate == null) {
      truncateBrokenLogs();
      operate = null;
      return false;
    }
    return true;
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
      logger.error("Failed to close archiving log");
    }
  }
}
