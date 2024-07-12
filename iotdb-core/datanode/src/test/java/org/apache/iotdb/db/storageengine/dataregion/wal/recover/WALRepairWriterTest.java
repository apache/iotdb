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
package org.apache.iotdb.db.storageengine.dataregion.wal.recover;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALInfoEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALByteBufReader;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALMetaData;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALWriter;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALByteBufferForTest;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileStatus;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;

public class WALRepairWriterTest {
  private final File logFile =
      new File(
          TestConstant.BASE_OUTPUT_PATH.concat(
              WALFileUtils.getLogFileName(1, 1, WALFileStatus.CONTAINS_SEARCH_INDEX)));

  @After
  public void tearDown() throws Exception {
    logFile.delete();
  }

  @Test
  public void testEmptyFile() throws IOException {
    // prepare file
    logFile.createNewFile();
    long firstSearchIndex = WALFileUtils.parseStartSearchIndex(logFile.getName());
    WALMetaData walMetaData = new WALMetaData(firstSearchIndex, new ArrayList<>(), new HashSet<>());
    // repair
    new WALRepairWriter(logFile).repair(walMetaData);
    // verify file, marker + metadata(search index + size number) + metadata size + head magic
    // string + tail magic string
    Assert.assertEquals(
        Byte.BYTES
            + (Long.BYTES + Integer.BYTES)
            + Integer.BYTES
            + WALWriter.MAGIC_STRING_V2_BYTES * 2,
        logFile.length());
    try (WALByteBufReader reader = new WALByteBufReader(logFile)) {
      Assert.assertFalse(reader.hasNext());
      Assert.assertEquals(firstSearchIndex, reader.getFirstSearchIndex());
    }
  }

  @Test
  public void testFileWithoutMagicString() throws IOException {
    // prepare file
    logFile.createNewFile();
    try (OutputStream stream = Files.newOutputStream(logFile.toPath())) {
      stream.write(1);
    }
    long firstSearchIndex = WALFileUtils.parseStartSearchIndex(logFile.getName());
    WALMetaData walMetaData = new WALMetaData(firstSearchIndex, new ArrayList<>(), new HashSet<>());
    // repair
    new WALRepairWriter(logFile).repair(walMetaData);
    // verify file, marker + metadata(search index + size number) + metadata size + magic string
    Assert.assertEquals(
        Byte.BYTES
            + (Long.BYTES + Integer.BYTES)
            + Integer.BYTES
            + WALWriter.MAGIC_STRING_V2_BYTES * 2,
        logFile.length());
    try (WALByteBufReader reader = new WALByteBufReader(logFile)) {
      Assert.assertFalse(reader.hasNext());
      Assert.assertEquals(firstSearchIndex, reader.getFirstSearchIndex());
    }
  }

  @Test
  public void testCompleteFile1() throws IOException, IllegalPathException {
    // prepare file
    WALMetaData walMetaData = new WALMetaData();
    WALEntry walEntry = new WALInfoEntry(1, getInsertRowNode());
    int size = walEntry.serializedSize();
    WALByteBufferForTest buffer = new WALByteBufferForTest(ByteBuffer.allocate(size));
    walEntry.serialize(buffer);
    walMetaData.add(size, 1, walEntry.getMemTableId());
    try (WALWriter walWriter = new WALWriter(logFile)) {
      walWriter.write(buffer.getBuffer(), walMetaData);
    }
    // repair
    new WALRepairWriter(logFile).repair(walMetaData);
    // verify file
    try (WALByteBufReader reader = new WALByteBufReader(logFile)) {
      Assert.assertTrue(reader.hasNext());
      Assert.assertEquals(size, reader.next().capacity());
      Assert.assertFalse(reader.hasNext());
      Assert.assertEquals(1, reader.getFirstSearchIndex());
    }
  }

  @Test
  public void testCompleteFile2() throws IOException, IllegalPathException {
    // prepare file
    WALMetaData walMetaData = new WALMetaData();
    WALEntry walEntry = new WALInfoEntry(1, getInsertRowsNode());
    int size = walEntry.serializedSize();
    WALByteBufferForTest buffer = new WALByteBufferForTest(ByteBuffer.allocate(size));
    walEntry.serialize(buffer);
    walMetaData.add(size, 1, walEntry.getMemTableId());
    try (WALWriter walWriter = new WALWriter(logFile)) {
      walWriter.write(buffer.getBuffer(), walMetaData);
    }
    // repair
    new WALRepairWriter(logFile).repair(walMetaData);
    // verify file
    try (WALByteBufReader reader = new WALByteBufReader(logFile)) {
      Assert.assertTrue(reader.hasNext());
      Assert.assertEquals(size, reader.next().capacity());
      Assert.assertFalse(reader.hasNext());
      Assert.assertEquals(1, reader.getFirstSearchIndex());
    }
  }

  @Test
  public void testFileWithBrokenMagicString() throws IOException, IllegalPathException {
    // prepare file
    WALMetaData walMetaData = new WALMetaData();
    WALEntry walEntry = new WALInfoEntry(1, getInsertRowNode());
    int size = walEntry.serializedSize();
    WALByteBufferForTest buffer = new WALByteBufferForTest(ByteBuffer.allocate(size));
    walEntry.serialize(buffer);
    walMetaData.add(size, 1, walEntry.getMemTableId());
    try (WALWriter walWriter = new WALWriter(logFile)) {
      walWriter.write(buffer.getBuffer(), walMetaData);
      walMetaData.setTruncateOffSet(walWriter.getOffset());
    }
    long len = logFile.length();
    try (FileChannel channel = FileChannel.open(logFile.toPath(), StandardOpenOption.APPEND)) {
      channel.truncate(len - 1);
    }
    // repair
    new WALRepairWriter(logFile).repair(walMetaData);
    // verify file
    try (WALByteBufReader reader = new WALByteBufReader(logFile)) {
      Assert.assertTrue(reader.hasNext());
      Assert.assertEquals(size, reader.next().capacity());
      Assert.assertFalse(reader.hasNext());
      Assert.assertEquals(1, reader.getFirstSearchIndex());
    }
  }

  public static InsertRowNode getInsertRowNode() throws IllegalPathException {
    String devicePath = "root.test_sg.test_d";
    long time = 110L;
    TSDataType[] dataTypes =
        new TSDataType[] {
          TSDataType.DOUBLE,
          TSDataType.FLOAT,
          TSDataType.INT64,
          TSDataType.INT32,
          TSDataType.BOOLEAN,
          TSDataType.TEXT
        };

    Object[] columns = new Object[6];
    columns[0] = 1.0;
    columns[1] = 2.0f;
    columns[2] = 10000L;
    columns[3] = 100;
    columns[4] = false;
    columns[5] = new Binary("hh" + 0, TSFileConfig.STRING_CHARSET);

    InsertRowNode insertRowNode =
        new InsertRowNode(
            new PlanNodeId(""),
            new PartialPath(devicePath),
            false,
            new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
            dataTypes,
            time,
            columns,
            false);
    insertRowNode.setSearchIndex(1);
    insertRowNode.setMeasurementSchemas(
        new MeasurementSchema[] {
          new MeasurementSchema("s1", TSDataType.DOUBLE),
          new MeasurementSchema("s2", TSDataType.FLOAT),
          new MeasurementSchema("s3", TSDataType.INT64),
          new MeasurementSchema("s4", TSDataType.INT32),
          new MeasurementSchema("s5", TSDataType.BOOLEAN),
          new MeasurementSchema("s6", TSDataType.TEXT)
        });
    return insertRowNode;
  }

  public static InsertRowsNode getInsertRowsNode() throws IllegalPathException {
    String devicePath = "root.test_sg.test_d";
    TSDataType[] dataTypes =
        new TSDataType[] {
          TSDataType.DOUBLE,
          TSDataType.FLOAT,
          TSDataType.INT64,
          TSDataType.INT32,
          TSDataType.BOOLEAN,
          TSDataType.TEXT
        };

    Object[] columns = new Object[6];
    columns[0] = 1.0;
    columns[1] = 2.0f;
    columns[2] = 10000L;
    columns[3] = 100;
    columns[4] = false;
    columns[5] = new Binary("hh" + 0, TSFileConfig.STRING_CHARSET);

    InsertRowNode insertRowNode =
        new InsertRowNode(
            new PlanNodeId(""),
            new PartialPath(devicePath),
            false,
            new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
            dataTypes,
            111L,
            columns,
            false);
    insertRowNode.setMeasurementSchemas(
        new MeasurementSchema[] {
          new MeasurementSchema("s1", TSDataType.DOUBLE),
          new MeasurementSchema("s2", TSDataType.FLOAT),
          new MeasurementSchema("s3", TSDataType.INT64),
          new MeasurementSchema("s4", TSDataType.INT32),
          new MeasurementSchema("s5", TSDataType.BOOLEAN),
          new MeasurementSchema("s6", TSDataType.TEXT)
        });

    InsertRowsNode insertRowsNode = new InsertRowsNode(new PlanNodeId(""));
    insertRowsNode.addOneInsertRowNode(insertRowNode, 0);
    insertRowNode =
        new InsertRowNode(
            new PlanNodeId(""),
            new PartialPath(devicePath),
            false,
            new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
            dataTypes,
            112L,
            columns,
            false);
    insertRowNode.setMeasurementSchemas(
        new MeasurementSchema[] {
          new MeasurementSchema("s1", TSDataType.DOUBLE),
          new MeasurementSchema("s2", TSDataType.FLOAT),
          new MeasurementSchema("s3", TSDataType.INT64),
          new MeasurementSchema("s4", TSDataType.INT32),
          new MeasurementSchema("s5", TSDataType.BOOLEAN),
          new MeasurementSchema("s6", TSDataType.TEXT)
        });
    insertRowsNode.addOneInsertRowNode(insertRowNode, 2);
    insertRowsNode.setSearchIndex(1);
    return insertRowsNode;
  }
}
