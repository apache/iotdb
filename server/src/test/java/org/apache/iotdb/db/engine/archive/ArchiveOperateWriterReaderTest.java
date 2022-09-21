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

package org.apache.iotdb.db.engine.archive;

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

public class ArchiveOperateWriterReaderTest {
  private static final String filePath = "logtest.test";
  private final String sg1 = "root.ARCHIVE_SG1";
  private final String sg2 = "root.ARCHIVE_SG1";
  private long startTime; // 2023-01-01
  private final long ttl = 2000;
  private final String targetDirPath = Paths.get("data", "separated").toString();
  List<ArchiveOperate> archiveOperate;
  ArchiveTask task1, task2;

  @Before
  public void prepare() throws IllegalPathException, LogicalOperatorException {
    if (new File(filePath).exists()) {
      new File(filePath).delete();
    }
    task1 = new ArchiveTask(120, new PartialPath(sg1), new File(targetDirPath), startTime, ttl);
    task2 = new ArchiveTask(999, new PartialPath(sg2), new File(targetDirPath), startTime, ttl);

    archiveOperate = new ArrayList<>();
    archiveOperate.add(new ArchiveOperate(ArchiveOperate.ArchiveOperateType.START, task1));
    archiveOperate.add(new ArchiveOperate(ArchiveOperate.ArchiveOperateType.SET, task1));
    archiveOperate.add(new ArchiveOperate(ArchiveOperate.ArchiveOperateType.CANCEL, task2));
    archiveOperate.add(new ArchiveOperate(ArchiveOperate.ArchiveOperateType.PAUSE, task2));
    archiveOperate.add(new ArchiveOperate(ArchiveOperate.ArchiveOperateType.RESUME, task2));

    startTime = DatetimeUtils.convertDatetimeStrToLong("2023-01-01", ZoneId.systemDefault());
    task1.close();
    task2.close();
  }

  public void writeLog(ArchiveOperateWriter writer) throws IOException {
    writer.log(ArchiveOperate.ArchiveOperateType.START, task1);
    writer.log(ArchiveOperate.ArchiveOperateType.SET, task1);
    writer.log(ArchiveOperate.ArchiveOperateType.CANCEL, task2);
    writer.log(ArchiveOperate.ArchiveOperateType.PAUSE, task2);
    writer.log(ArchiveOperate.ArchiveOperateType.RESUME, task2);
  }

  /** check if two logs have equal fields */
  public boolean logEquals(ArchiveOperate log1, ArchiveOperate log2) {
    if (log1.getType() != log2.getType()) {
      return false;
    }
    if (log1.getTask().getTaskId() != log2.getTask().getTaskId()) {
      return false;
    }

    if (log1.getType() == ArchiveOperate.ArchiveOperateType.SET) {
      // check other fields only if SET
      if (log1.getTask().getStartTime() != log2.getTask().getStartTime()) {
        return false;
      }
      if (log1.getTask().getTTL() != log2.getTask().getTTL()) {
        return false;
      }
      if (!log1.getTask()
          .getStorageGroup()
          .getFullPath()
          .equals(log2.getTask().getStorageGroup().getFullPath())) {
        return false;
      }
      if (!log1.getTask()
          .getTargetDir()
          .getPath()
          .equals(log2.getTask().getTargetDir().getPath())) {
        return false;
      }
    }

    return true;
  }

  @Test
  public void testWriteAndRead() throws Exception {
    ArchiveOperateWriter writer = new ArchiveOperateWriter(filePath);
    writeLog(writer);
    try (ArchiveOperateReader reader = new ArchiveOperateReader(new File(filePath))) {
      writer.close();
      List<ArchiveOperate> res = new ArrayList<>();
      while (reader.hasNext()) {
        res.add(reader.next());
      }
      for (int i = 0; i < archiveOperate.size(); i++) {
        assertTrue(logEquals(archiveOperate.get(i), res.get(i)));
      }
    } finally {
      new File(filePath).delete();
    }
  }

  @Test
  public void testTruncateBrokenLogs() throws Exception {
    try {
      // write normal data
      try (ArchiveOperateWriter writer = new ArchiveOperateWriter(filePath)) {
        writeLog(writer);
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
      try (ArchiveOperateReader reader = new ArchiveOperateReader(new File(filePath))) {
        List<ArchiveOperate> res = new ArrayList<>();
        while (reader.hasNext()) {
          res.add(reader.next());
        }
        for (int i = 0; i < archiveOperate.size(); i++) {
          assertTrue(logEquals(archiveOperate.get(i), res.get(i)));
        }
      }
      assertEquals(expectedLength, new File(filePath).length());
    } finally {
      new File(filePath).delete();
    }
  }
}
