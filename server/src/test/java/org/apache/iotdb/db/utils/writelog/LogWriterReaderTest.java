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

package org.apache.iotdb.db.utils.writelog;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

import static org.junit.Assert.assertEquals;

public class LogWriterReaderTest {

  private static String filePath = "logtest.test";
  ByteBuffer logsBuffer = ByteBuffer.allocate(64 * 1024);
  List<PhysicalPlan> plans = new ArrayList<>();

  @Before
  public void prepare() throws IllegalPathException {
    if (new File(filePath).exists()) {
      new File(filePath).delete();
    }
    InsertRowPlan insertRowPlan1 =
        new InsertRowPlan(
            new PartialPath("d1"),
            10L,
            new String[] {"s1", "s2"},
            new TSDataType[] {TSDataType.INT64, TSDataType.INT64},
            new String[] {"1", "2"});
    InsertRowPlan insertRowPlan2 =
        new InsertRowPlan(
            new PartialPath("d1"),
            10L,
            new String[] {"s1", "s2"},
            new TSDataType[] {TSDataType.INT64, TSDataType.INT64},
            new String[] {"1", "2"});
    DeletePlan deletePlan = new DeletePlan(Long.MIN_VALUE, 10L, new PartialPath("root.d1.s1"));
    plans.add(insertRowPlan1);
    plans.add(insertRowPlan2);
    plans.add(deletePlan);
    for (PhysicalPlan plan : plans) {
      plan.serialize(logsBuffer);
    }
  }

  @Test
  public void testWriteAndRead() throws IOException {
    LogWriter writer = new LogWriter(filePath, false);
    writer.write(logsBuffer);
    try {
      writer.force();
      writer.close();
      SingleFileLogReader reader = new SingleFileLogReader(new File(filePath));
      List<PhysicalPlan> res = new ArrayList<>();
      while (reader.hasNext()) {
        res.add(reader.next());
      }
      for (int i = 0; i < plans.size(); i++) {
        assertEquals(plans.get(i), res.get(i));
      }
      reader.close();
    } finally {
      new File(filePath).delete();
    }
  }

  @Test
  public void testReachEOF() throws IOException {
    try {
      // write normal data
      LogWriter writer = new LogWriter(filePath, false);
      try {
        writer.write(logsBuffer);
        writer.force();
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
      SingleFileLogReader reader = new SingleFileLogReader(new File(filePath));
      try {
        List<PhysicalPlan> res = new ArrayList<>();
        while (reader.hasNext()) {
          res.add(reader.next());
        }
        for (int i = 0; i < plans.size(); i++) {
          assertEquals(plans.get(i), res.get(i));
        }
      } finally {
        reader.close();
      }
      assertEquals(expectedLength, new File(filePath).length());
    } finally {
      new File(filePath).delete();
    }
  }

  @Test
  public void testTruncateBrokenLogs() throws IOException {
    try {
      // write normal data
      LogWriter writer = new LogWriter(filePath, false);
      try {
        writer.write(logsBuffer);
        writer.force();
      } finally {
        writer.close();
      }
      long expectedLength = new File(filePath).length();

      // write broken data
      try (FileOutputStream outputStream = new FileOutputStream(filePath, true);
          FileChannel channel = outputStream.getChannel()) {
        ByteBuffer logBuffer = ByteBuffer.allocate(4 * 30);
        for (int i = 0; i < 30; ++i) {
          logBuffer.putInt(Integer.MIN_VALUE);
        }
        logBuffer.flip();

        ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
        lengthBuffer.putInt(logBuffer.limit());
        lengthBuffer.flip();

        CRC32 checkSummer = new CRC32();
        checkSummer.reset();
        checkSummer.update(logBuffer);
        ByteBuffer checkSumBuffer = ByteBuffer.allocate(8);
        checkSumBuffer.putLong(checkSummer.getValue());
        logBuffer.flip();
        checkSumBuffer.flip();

        channel.write(lengthBuffer);
        channel.write(logBuffer);
        channel.write(checkSumBuffer);
        channel.force(true);
      }

      // read & check
      SingleFileLogReader reader = new SingleFileLogReader(new File(filePath));
      try {
        List<PhysicalPlan> res = new ArrayList<>();
        while (reader.hasNext()) {
          res.add(reader.next());
        }
        for (int i = 0; i < plans.size(); i++) {
          assertEquals(plans.get(i), res.get(i));
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
