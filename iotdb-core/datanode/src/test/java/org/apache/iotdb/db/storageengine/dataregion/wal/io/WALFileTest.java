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
package org.apache.iotdb.db.storageengine.dataregion.wal.io;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertTabletNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryType;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALInfoEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALByteBufferForTest;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileStatus;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class WALFileTest {

  private final File walFile =
      new File(
          TestConstant.BASE_OUTPUT_PATH.concat(
              WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)));
  private final String devicePath = "root.test_sg.test_d";

  @Before
  public void setUp() throws Exception {
    if (walFile.exists()) {
      Files.delete(walFile.toPath());
    } else {
      walFile.getParentFile().mkdirs();
    }
  }

  @After
  public void tearDown() throws Exception {
    if (walFile.exists()) {
      Files.delete(walFile.toPath());
    }
  }

  @Test
  public void testReadNormalFile() throws IOException, IllegalPathException {
    int fakeMemTableId = 1;
    List<WALEntry> expectedWALEntries = new ArrayList<>();
    expectedWALEntries.add(new WALInfoEntry(fakeMemTableId, getInsertRowNode(devicePath)));
    expectedWALEntries.add(new WALInfoEntry(fakeMemTableId, getInsertRowsNode(devicePath)));
    expectedWALEntries.add(new WALInfoEntry(fakeMemTableId, getInsertTabletNode(devicePath)));
    expectedWALEntries.add(new WALInfoEntry(fakeMemTableId, getDeleteDataNode(devicePath)));
    expectedWALEntries.add(
        new WALInfoEntry(
            fakeMemTableId,
            getRelationalInsertTabletNode("table1"),
            Arrays.asList(new int[] {0, 2}, new int[] {2, 4})));

    // test WALEntry.serializedSize
    int size = 0;
    for (WALEntry walEntry : expectedWALEntries) {
      size += walEntry.serializedSize();
    }
    WALByteBufferForTest buffer = new WALByteBufferForTest(ByteBuffer.allocate(size));
    // test WALEntry.serialize
    for (WALEntry walEntry : expectedWALEntries) {
      walEntry.serialize(buffer);
    }
    assertEquals(0, buffer.getBuffer().remaining());
    // test WALEntry.write
    try (ILogWriter walWriter = new WALWriter(walFile)) {
      walWriter.write(buffer.getBuffer());
    }
    // test WALReader.readAll
    List<WALEntry> actualWALEntries = new ArrayList<>();
    try (WALReader walReader = new WALReader(walFile)) {
      while (walReader.hasNext()) {
        actualWALEntries.add(walReader.next());
      }
    }

    // set tablet info of the RelationalInsertTablet to range [0, 4)
    expectedWALEntries.set(
        4,
        new WALInfoEntry(
            fakeMemTableId,
            getRelationalInsertTabletNode("table1"),
            Collections.singletonList(new int[] {0, 4})));
    assertEquals(expectedWALEntries, actualWALEntries);
  }

  @Test
  public void testReadNotExistFile() throws IOException {
    if (walFile.createNewFile()) {
      List<WALEntry> actualWALEntries = new ArrayList<>();
      try (WALReader walReader = new WALReader(walFile)) {
        while (walReader.hasNext()) {
          actualWALEntries.add(walReader.next());
        }
      }
      assertEquals(0, actualWALEntries.size());
    }
  }

  @Test
  public void testReadBrokenFile() throws IOException, IllegalPathException {
    int fakeMemTableId = 1;
    List<WALEntry> expectedWALEntries = new ArrayList<>();
    expectedWALEntries.add(new WALInfoEntry(fakeMemTableId, getInsertRowNode(devicePath)));
    expectedWALEntries.add(new WALInfoEntry(fakeMemTableId, getInsertRowsNode(devicePath)));
    expectedWALEntries.add(new WALInfoEntry(fakeMemTableId, getInsertTabletNode(devicePath)));
    // test WALEntry.serializedSize
    int size = Byte.BYTES;
    for (WALEntry walEntry : expectedWALEntries) {
      size += walEntry.serializedSize();
    }
    WALByteBufferForTest buffer = new WALByteBufferForTest(ByteBuffer.allocate(size));
    // test WALEntry.serialize
    for (WALEntry walEntry : expectedWALEntries) {
      walEntry.serialize(buffer);
    }
    // add broken part
    buffer.put(WALEntryType.DELETE_DATA_NODE.getCode());
    assertEquals(0, buffer.getBuffer().remaining());
    // test WALEntry.write
    try (ILogWriter walWriter = new WALWriter(walFile)) {
      walWriter.write(buffer.getBuffer());
    }
    // test WALReader.readAll
    List<WALEntry> actualWALEntries = new ArrayList<>();
    try (WALReader walReader = new WALReader(walFile)) {
      while (walReader.hasNext()) {
        actualWALEntries.add(walReader.next());
      }
    }
    assertEquals(expectedWALEntries, actualWALEntries);
  }

  @Test
  public void testReadMetadataFromBrokenFile() throws IOException {
    ILogWriter walWriter = new WALWriter(walFile);
    final FileChannel fileChannel1 = FileChannel.open(walFile.toPath());
    assertThrows(IOException.class, () -> WALMetaData.readFromWALFile(walFile, fileChannel1));
    walWriter.close();
    FileChannel fileChannel2 = FileChannel.open(walFile.toPath());
    WALMetaData walMetaData = WALMetaData.readFromWALFile(walFile, fileChannel2);
    fileChannel2.close();
    assertTrue(walMetaData.getMemTablesId().isEmpty());
  }

  public static InsertRowNode getInsertRowNode(String devicePath) throws IllegalPathException {
    long time = 110L;
    TSDataType[] dataTypes =
        new TSDataType[] {
          TSDataType.DOUBLE,
          TSDataType.FLOAT,
          TSDataType.INT64,
          TSDataType.INT32,
          TSDataType.BOOLEAN,
          TSDataType.TEXT,
          TSDataType.STRING,
          TSDataType.BLOB
        };

    Object[] columns = new Object[8];
    columns[0] = 1.0;
    columns[1] = 2.0f;
    columns[2] = 10000L;
    columns[3] = 100;
    columns[4] = false;
    columns[5] = new Binary("hh" + 0, TSFileConfig.STRING_CHARSET);
    columns[6] = new Binary("jj" + 0, TSFileConfig.STRING_CHARSET);
    columns[7] = new Binary("kk" + 0, TSFileConfig.STRING_CHARSET);

    InsertRowNode insertRowNode =
        new InsertRowNode(
            new PlanNodeId(""),
            new PartialPath(devicePath),
            false,
            new String[] {"s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"},
            dataTypes,
            time,
            columns,
            false);

    MeasurementSchema[] schemas =
        new MeasurementSchema[] {
          new MeasurementSchema("s1", dataTypes[0]),
          new MeasurementSchema("s2", dataTypes[1]),
          new MeasurementSchema("s3", dataTypes[2]),
          new MeasurementSchema("s4", dataTypes[3]),
          new MeasurementSchema("s5", dataTypes[4]),
          new MeasurementSchema("s6", dataTypes[5]),
          new MeasurementSchema("s7", dataTypes[6]),
          new MeasurementSchema("s8", dataTypes[7]),
        };
    insertRowNode.setMeasurementSchemas(schemas);
    return insertRowNode;
  }

  public static InsertRowsNode getInsertRowsNode(String devicePath) throws IllegalPathException {
    InsertRowsNode insertRowsNode = new InsertRowsNode(new PlanNodeId(""));
    long time = 111L;
    TSDataType[] dataTypes =
        new TSDataType[] {
          TSDataType.DOUBLE,
          TSDataType.FLOAT,
          TSDataType.INT64,
          TSDataType.INT32,
          TSDataType.BOOLEAN,
          TSDataType.TEXT,
          TSDataType.STRING,
          TSDataType.BLOB
        };

    Object[] columns = new Object[8];
    columns[0] = 1.0;
    columns[1] = 2.0f;
    columns[2] = 10000L;
    columns[3] = 100;
    columns[4] = false;
    columns[5] = new Binary("hh" + 0, TSFileConfig.STRING_CHARSET);
    columns[6] = new Binary("jj" + 0, TSFileConfig.STRING_CHARSET);
    columns[7] = new Binary("kk" + 0, TSFileConfig.STRING_CHARSET);

    InsertRowNode insertRowNode =
        new InsertRowNode(
            new PlanNodeId(""),
            new PartialPath(devicePath),
            false,
            new String[] {"s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"},
            dataTypes,
            time,
            columns,
            false);

    MeasurementSchema[] schemas =
        new MeasurementSchema[] {
          new MeasurementSchema("s1", dataTypes[0]),
          new MeasurementSchema("s2", dataTypes[1]),
          new MeasurementSchema("s3", dataTypes[2]),
          new MeasurementSchema("s4", dataTypes[3]),
          new MeasurementSchema("s5", dataTypes[4]),
          new MeasurementSchema("s6", dataTypes[5]),
          new MeasurementSchema("s7", dataTypes[6]),
          new MeasurementSchema("s8", dataTypes[7]),
        };
    insertRowNode.setMeasurementSchemas(schemas);
    insertRowsNode.addOneInsertRowNode(insertRowNode, 0);

    time = 112L;
    insertRowNode =
        new InsertRowNode(
            new PlanNodeId(""),
            new PartialPath(devicePath),
            false,
            new String[] {"s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"},
            dataTypes,
            time,
            columns,
            false);

    insertRowNode.setMeasurementSchemas(schemas);

    insertRowsNode.addOneInsertRowNode(insertRowNode, 1);

    return insertRowsNode;
  }

  public static InsertTabletNode getInsertTabletNode(String devicePath)
      throws IllegalPathException {
    long[] times = new long[] {110L, 111L, 112L, 113L};
    TSDataType[] dataTypes =
        new TSDataType[] {
          TSDataType.DOUBLE,
          TSDataType.FLOAT,
          TSDataType.INT64,
          TSDataType.INT32,
          TSDataType.BOOLEAN,
          TSDataType.TEXT,
          TSDataType.STRING,
          TSDataType.BLOB
        };

    Object[] columns = new Object[8];
    columns[0] = new double[4];
    columns[1] = new float[4];
    columns[2] = new long[4];
    columns[3] = new int[4];
    columns[4] = new boolean[4];
    columns[5] = new Binary[4];
    columns[6] = new Binary[4];
    columns[7] = new Binary[4];

    for (int r = 0; r < 4; r++) {
      ((double[]) columns[0])[r] = 1.0 + r;
      ((float[]) columns[1])[r] = 2 + r;
      ((long[]) columns[2])[r] = 10000 + r;
      ((int[]) columns[3])[r] = 100 + r;
      ((boolean[]) columns[4])[r] = (r % 2 == 0);
      ((Binary[]) columns[5])[r] = new Binary("hh" + r, TSFileConfig.STRING_CHARSET);
      ((Binary[]) columns[6])[r] = new Binary("jj" + r, TSFileConfig.STRING_CHARSET);
      ((Binary[]) columns[7])[r] = new Binary("kk" + r, TSFileConfig.STRING_CHARSET);
    }

    BitMap[] bitMaps = new BitMap[dataTypes.length];
    for (int i = 0; i < dataTypes.length; i++) {
      if (bitMaps[i] == null) {
        bitMaps[i] = new BitMap(times.length);
      }
      bitMaps[i].mark(i % times.length);
    }
    MeasurementSchema[] schemas =
        new MeasurementSchema[] {
          new MeasurementSchema("s1", dataTypes[0]),
          new MeasurementSchema("s2", dataTypes[1]),
          new MeasurementSchema("s3", dataTypes[2]),
          new MeasurementSchema("s4", dataTypes[3]),
          new MeasurementSchema("s5", dataTypes[4]),
          new MeasurementSchema("s6", dataTypes[5]),
          new MeasurementSchema("s7", dataTypes[6]),
          new MeasurementSchema("s8", dataTypes[7]),
        };

    return new InsertTabletNode(
        new PlanNodeId(""),
        new PartialPath(devicePath),
        false,
        new String[] {"s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"},
        dataTypes,
        schemas,
        times,
        bitMaps,
        columns,
        times.length);
  }

  public static RelationalInsertTabletNode getRelationalInsertTabletNode(String tableName)
      throws IllegalPathException {
    long[] times = new long[] {110L, 111L, 112L, 113L};
    TSDataType[] dataTypes =
        new TSDataType[] {
          TSDataType.STRING, TSDataType.FLOAT,
        };

    Object[] columns = new Object[2];
    columns[0] = new Binary[4];
    columns[1] = new float[4];

    for (int r = 0; r < 4; r++) {
      ((Binary[]) columns[0])[r] = new Binary("hh" + r, TSFileConfig.STRING_CHARSET);
      ((float[]) columns[1])[r] = 2 + r;
    }

    BitMap[] bitMaps = new BitMap[dataTypes.length];
    for (int i = 0; i < dataTypes.length; i++) {
      if (bitMaps[i] == null) {
        bitMaps[i] = new BitMap(times.length);
      }
      bitMaps[i].mark(i % times.length);
    }
    MeasurementSchema[] schemas =
        new MeasurementSchema[] {
          new MeasurementSchema("s1", dataTypes[0]), new MeasurementSchema("s2", dataTypes[1]),
        };

    return new RelationalInsertTabletNode(
        new PlanNodeId(""),
        new PartialPath(tableName, false),
        false,
        new String[] {
          "s1", "s2",
        },
        dataTypes,
        schemas,
        times,
        bitMaps,
        columns,
        times.length,
        new TsTableColumnCategory[] {
          TsTableColumnCategory.ID, TsTableColumnCategory.MEASUREMENT,
        });
  }

  public static DeleteDataNode getDeleteDataNode(String devicePath) throws IllegalPathException {
    DeleteDataNode deleteDataNode =
        new DeleteDataNode(
            new PlanNodeId(""),
            Collections.singletonList(new MeasurementPath(devicePath, "**")),
            Long.MIN_VALUE,
            Long.MAX_VALUE);
    deleteDataNode.setSearchIndex(100L);
    return deleteDataNode;
  }
}
