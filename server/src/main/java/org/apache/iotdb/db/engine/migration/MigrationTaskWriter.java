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
import org.apache.iotdb.db.metadata.logfile.MLogTxtWriter;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

/** MigrationTasKWriter writes the binary logs of MigrationTask into file using FileOutputStream */
public class MigrationTaskWriter implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(MLogTxtWriter.class);
  private final File logFile;
  private FileOutputStream logFileOutStream;

  public MigrationTaskWriter(String logFileName) throws FileNotFoundException {
    this(SystemFileFactory.INSTANCE.getFile(logFileName));
  }

  public MigrationTaskWriter(File logFile) throws FileNotFoundException {
    this.logFile = logFile;
    if (!logFile.exists()) {
      if (logFile.getParentFile() != null) {
        if (logFile.getParentFile().mkdirs()) {
          logger.info("created migrate log folder");
        } else {
          logger.info("create migrate log folder failed");
        }
      }
    }
    logFileOutStream = new FileOutputStream(logFile, true);
  }

  private void putLog(MigrationLog log) {
    try {
      int type = log.type.ordinal();
      ReadWriteIOUtils.write((byte) type, logFileOutStream);
      ReadWriteIOUtils.write(log.taskId, logFileOutStream);

      if (log.type == MigrationLog.LogType.SET) {
        ReadWriteIOUtils.write(log.storageGroup.getFullPath(), logFileOutStream);
        ReadWriteIOUtils.write(log.targetDirPath, logFileOutStream);
        ReadWriteIOUtils.write(log.startTime, logFileOutStream);
        ReadWriteIOUtils.write(log.ttl, logFileOutStream);
      }

      logFileOutStream.flush();
    } catch (IOException e) {
      logger.error("unable to write to migrate log");
    }
  }

  public void setMigration(MigrationTask migrationTask) throws IOException {
    MigrationLog log =
        new MigrationLog(
            MigrationLog.LogType.SET,
            migrationTask.getTaskId(),
            migrationTask.getStorageGroup(),
            migrationTask.getTargetDir().getPath(),
            migrationTask.getStartTime(),
            migrationTask.getTTL());
    putLog(log);
  }

  public void startMigration(MigrationTask migrationTask) throws IOException {
    MigrationLog log = new MigrationLog(MigrationLog.LogType.START, migrationTask.getTaskId());
    putLog(log);
  }

  public void finishMigration(MigrationTask migrationTask) throws IOException {
    MigrationLog log = new MigrationLog(MigrationLog.LogType.FINISHED, migrationTask.getTaskId());
    putLog(log);
  }

  public void unsetMigration(MigrationTask migrationTask) throws IOException {
    MigrationLog log = new MigrationLog(MigrationLog.LogType.CANCEL, migrationTask.getTaskId());
    putLog(log);
  }

  public void pauseMigration(MigrationTask migrationTask) throws IOException {
    MigrationLog log = new MigrationLog(MigrationLog.LogType.PAUSE, migrationTask.getTaskId());
    putLog(log);
  }

  public void unpauseMigration(MigrationTask migrationTask) throws IOException {
    MigrationLog log = new MigrationLog(MigrationLog.LogType.RESUME, migrationTask.getTaskId());
    putLog(log);
  }

  public void error(MigrationTask migrationTask) throws IOException {
    MigrationLog log = new MigrationLog(MigrationLog.LogType.ERROR, migrationTask.getTaskId());
    putLog(log);
  }

  @Override
  public void close() throws Exception {
    logFileOutStream.close();
  }

  public static class MigrationLog {
    public LogType type;
    public long taskId;
    public PartialPath storageGroup;
    public String targetDirPath;
    public long startTime;
    public long ttl;

    public MigrationLog() {}

    public MigrationLog(LogType type, long taskId) {
      this.type = type;
      this.taskId = taskId;
    }

    public MigrationLog(
        LogType type,
        long taskId,
        PartialPath storageGroup,
        String targetDirPath,
        long startTime,
        long ttl) {
      this.type = type;
      this.taskId = taskId;
      this.storageGroup = storageGroup;
      this.targetDirPath = targetDirPath;
      this.startTime = startTime;
      this.ttl = ttl;
    }

    public MigrationLog(LogType type, MigrationTask task) {
      this.type = type;
      this.taskId = task.getTaskId();
      this.storageGroup = task.getStorageGroup();
      this.targetDirPath = task.getTargetDir().getPath();
      this.startTime = task.getStartTime();
      this.ttl = task.getTTL();
    }

    public enum LogType {
      SET,
      CANCEL,
      START,
      PAUSE,
      RESUME,
      FINISHED,
      ERROR
    }
  }
}
