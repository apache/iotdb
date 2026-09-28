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
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALInfoEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALByteBufReader;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALFileVersion;
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
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.stream.Stream;

public class WALRepairWriterTest {
  private final File logFile =
      new File(
          TestConstant.BASE_OUTPUT_PATH.concat(
              WALFileUtils.getLogFileName(1, 1, WALFileStatus.CONTAINS_SEARCH_INDEX)));

  @Before
  public void setUp() throws IOException {
    Files.createDirectories(logFile.toPath().getParent());
  }

  @After
  public void tearDown() throws Exception {
    logFile.delete();
    File brokenFile = new File(logFile.getPath() + ".broken");
    brokenFile.delete();
    for (int suffix = 1; suffix < 10; suffix++) {
      new File(logFile.getPath() + ".broken." + suffix).delete();
    }
  }

  @Test
  public void testEmptyFile() throws IOException {
    // prepare file
    logFile.createNewFile();
    long firstSearchIndex = WALFileUtils.parseStartSearchIndex(logFile.getName());
    WALMetaData walMetaData = new WALMetaData(firstSearchIndex, new ArrayList<>(), new HashSet<>());
    // repair
    new WALRepairWriter(logFile).repair(walMetaData);
    Assert.assertEquals(0, logFile.length());
    try (WALByteBufReader reader = new WALByteBufReader(logFile)) {
      Assert.assertFalse(reader.hasNext());
      Assert.assertTrue(reader.getMetaData().getMemTablesId().isEmpty());
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
    Assert.assertFalse(new WALRepairWriter(logFile).repair(walMetaData));
    Assert.assertFalse(logFile.exists());
    Assert.assertArrayEquals(
        new byte[] {1}, Files.readAllBytes(new File(logFile + ".broken").toPath()));
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

  @Test
  public void testUnrecoverableFileIsQuarantined() throws IOException {
    Files.write(logFile.toPath(), new byte[] {1, 2, 3, 4});

    Assert.assertFalse(new WALRepairWriter(logFile).repair(new WALMetaData()));
    Assert.assertFalse(logFile.exists());
    Assert.assertTrue(new File(logFile.getPath() + ".broken").exists());
  }

  @Test
  public void testCorruptedMetadataIsRebuilt() throws IOException, IllegalPathException {
    WALMetaData walMetaData = new WALMetaData();
    WALEntry walEntry = new WALInfoEntry(1, getInsertRowNode());
    int size = walEntry.serializedSize();
    WALByteBufferForTest buffer = new WALByteBufferForTest(ByteBuffer.allocate(size));
    walEntry.serialize(buffer);
    walMetaData.add(size, 1, walEntry.getMemTableId());

    long truncateOffset;
    try (WALWriter walWriter = new WALWriter(logFile)) {
      walWriter.write(buffer.getBuffer(), walMetaData);
      truncateOffset = walWriter.getOffset();
    }

    byte[] fileBytes = Files.readAllBytes(logFile.toPath());
    int metadataSizeOffset =
        fileBytes.length - WALFileVersion.V3.getVersionBytes().length - Integer.BYTES;
    int metadataSize = ByteBuffer.wrap(fileBytes, metadataSizeOffset, Integer.BYTES).getInt();
    int metadataOffset = metadataSizeOffset - metadataSize;
    ByteBuffer.wrap(fileBytes).putInt(metadataOffset + Long.BYTES, -1);
    Files.write(logFile.toPath(), fileBytes);

    WALMetaData recoveredMetadata = walMetaData.copy();
    recoveredMetadata.setTruncateOffSet(truncateOffset);
    Assert.assertTrue(new WALRepairWriter(logFile).repair(recoveredMetadata));

    try (WALByteBufReader reader = new WALByteBufReader(logFile)) {
      Assert.assertTrue(reader.hasNext());
      Assert.assertEquals(size, reader.next().capacity());
      Assert.assertFalse(reader.hasNext());
    }
  }

  @Test
  public void testFailedRepairPreservesOriginalFile() throws Exception {
    WALEntry entry = new WALInfoEntry(1, getInsertRowNode());
    WALByteBufferForTest buffer =
        new WALByteBufferForTest(ByteBuffer.allocate(entry.serializedSize()));
    entry.serialize(buffer);
    long dataEnd;
    try (WALWriter writer = new WALWriter(logFile)) {
      writer.write(buffer.getBuffer(), false);
      dataEnd = writer.getOffset();
    }
    try (FileChannel channel = FileChannel.open(logFile.toPath(), StandardOpenOption.WRITE)) {
      channel.truncate(dataEnd);
    }
    byte[] original = Files.readAllBytes(logFile.toPath());
    WALMetaData invalidSnapshot = new WALMetaData();
    invalidSnapshot.add(entry.serializedSize(), 1, 1);
    invalidSnapshot.add(entry.serializedSize(), 2, 1);
    // A stale snapshot requests one entry beyond EOF. The original must survive a failed rewrite.
    Assert.assertThrows(
        IOException.class, () -> new WALRepairWriter(logFile).repair(invalidSnapshot));
    Assert.assertArrayEquals(original, Files.readAllBytes(logFile.toPath()));
    try (Stream<Path> files = Files.list(logFile.toPath().getParent())) {
      Assert.assertFalse(
          files.anyMatch(path -> path.getFileName().toString().startsWith("wal-repair-")));
    }
  }

  @Test
  public void testStartupCleanupRetainsQuarantinedFile() throws Exception {
    Path directory = Files.createTempDirectory(logFile.toPath().getParent(), "wal-cleanup-");
    Path broken = directory.resolve(logFile.getName() + ".broken.1");
    Path wal = directory.resolve(logFile.getName());
    Path checkpoint = directory.resolve("_0.checkpoint");
    try {
      Files.write(broken, new byte[] {1, 2});
      Files.write(wal, new byte[] {3});
      Files.write(checkpoint, new byte[] {4});
      Assert.assertFalse(WALNodeRecoverTask.cleanupRecoveredDirectory(directory.toFile()));
      Assert.assertArrayEquals(new byte[] {1, 2}, Files.readAllBytes(broken));
      Assert.assertFalse(Files.exists(wal));
      Assert.assertFalse(Files.exists(checkpoint));
      Files.delete(broken);
      Assert.assertTrue(WALNodeRecoverTask.cleanupRecoveredDirectory(directory.toFile()));
      Assert.assertFalse(Files.exists(directory));
    } finally {
      Files.deleteIfExists(broken);
      Files.deleteIfExists(wal);
      Files.deleteIfExists(checkpoint);
      Files.deleteIfExists(directory);
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
