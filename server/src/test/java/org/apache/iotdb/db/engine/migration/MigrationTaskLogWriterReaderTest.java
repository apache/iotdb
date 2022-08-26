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

package org.apache.iotdb.db.engine.migration;

import org.apache.iotdb.db.engine.migration.MigrationTaskLogWriter.MigrationLog;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.LogicalOperatorException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.utils.DatetimeUtils;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MigrationTaskLogWriterReaderTest {

  private static final String filePath = "logtest.test";
  private final String sg1 = "root.MIGRATE_SG1";
  private final String sg2 = "root.MIGRATE_SG1";
  private long startTime; // 2023-01-01
  private final long ttl = 2000;
  private final String targetDirPath = Paths.get("data", "separated").toString();
  List<MigrationLog> migrateLogs;
  MigrationTask task1, task2;

  @Before
  public void prepare() throws IllegalPathException, LogicalOperatorException {
    if (new File(filePath).exists()) {
      new File(filePath).delete();
    }
    task1 = new MigrationTask(120, new PartialPath(sg1), new File(targetDirPath), startTime, ttl);
    task2 = new MigrationTask(999, new PartialPath(sg2), new File(targetDirPath), startTime, ttl);

    migrateLogs = new ArrayList<>();
    migrateLogs.add(new MigrationLog(MigrationLog.LogType.START, task1));
    migrateLogs.add(new MigrationLog(MigrationLog.LogType.SET, task1));
    migrateLogs.add(new MigrationLog(MigrationLog.LogType.UNSET, task2));
    migrateLogs.add(new MigrationLog(MigrationLog.LogType.PAUSE, task2));
    migrateLogs.add(new MigrationLog(MigrationLog.LogType.UNPAUSE, task2));

    startTime = DatetimeUtils.convertDatetimeStrToLong("2023-01-01", ZoneId.systemDefault());
  }

  public void writeLog(MigrationTaskLogWriter writer) throws IOException {
    writer.startMigration(task1);
    writer.setMigration(task1);
    writer.unsetMigration(task2);
    writer.pauseMigration(task2);
    writer.unpauseMigration(task2);
  }

  /** check if two logs have equal fields */
  public boolean logEquals(MigrationLog log1, MigrationLog log2) {
    if (log1.type != log2.type) {
      return false;
    }
    if (log1.taskId != log2.taskId) {
      return false;
    }

    if (log1.type == MigrationLog.LogType.SET) {
      // check other fields only if SET
      if (log1.startTime != log2.startTime) {
        return false;
      }
      if (log1.ttl != log2.ttl) {
        return false;
      }
      if (!log1.storageGroup.getFullPath().equals(log2.storageGroup.getFullPath())) {
        return false;
      }
      if (!log1.targetDirPath.equals(log2.targetDirPath)) {
        return false;
      }
    }

    return true;
  }

  @Test
  public void testWriteAndRead() throws Exception {
    MigrationTaskLogWriter writer = new MigrationTaskLogWriter(filePath);
    writeLog(writer);
    try {
      writer.close();
      MigrationTaskLogReader reader = new MigrationTaskLogReader(new File(filePath));
      List<MigrationLog> res = new ArrayList<>();
      while (reader.hasNext()) {
        res.add(reader.next());
      }
      for (int i = 0; i < migrateLogs.size(); i++) {
        assertTrue(logEquals(migrateLogs.get(i), res.get(i)));
      }
      reader.close();
    } finally {
      new File(filePath).delete();
    }
  }

  @Test
  public void testTruncateBrokenLogs() throws Exception {
    try {
      // write normal data
      MigrationTaskLogWriter writer = new MigrationTaskLogWriter(filePath);
      try {
        writeLog(writer);
      } finally {
        writer.close();
      }
      long expectedLength = new File(filePath).length();

      // just write partial content
      try (FileOutputStream outputStream = new FileOutputStream(filePath, true);
          FileChannel channel = outputStream.getChannel()) {
        ByteBuffer logBuffer = ByteBuffer.allocate(4 * 30);
        for (int i = 0; i < 20; ++i) {
          logBuffer.putInt(Integer.MIN_VALUE);
        }
        logBuffer.flip();
        ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
        lengthBuffer.putInt(logBuffer.capacity());
        lengthBuffer.flip();

        channel.write(lengthBuffer);
        channel.write(logBuffer);
        channel.force(true);
      }

      // read & check
      MigrationTaskLogReader reader = new MigrationTaskLogReader(new File(filePath));
      try {
        List<MigrationLog> res = new ArrayList<>();
        while (reader.hasNext()) {
          res.add(reader.next());
        }
        for (int i = 0; i < migrateLogs.size(); i++) {
          assertTrue(logEquals(migrateLogs.get(i), res.get(i)));
        }
      } finally {
        reader.close();
      }
      assertEquals(expectedLength, new File(filePath).length());
    } finally {
      new File(filePath).delete();
    }
  }
}
