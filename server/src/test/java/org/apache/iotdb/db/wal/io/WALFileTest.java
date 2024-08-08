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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.wal.buffer.WALEntry;
import org.apache.iotdb.db.wal.buffer.WALEntryType;
import org.apache.iotdb.db.wal.buffer.WALInfoEntry;
import org.apache.iotdb.db.wal.utils.WALByteBufferForTest;
import org.apache.iotdb.db.wal.utils.WALFileStatus;
import org.apache.iotdb.db.wal.utils.WALFileUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class WALFileTest {

  boolean prevIsCluster;
  private final File walFile =
      new File(
          TestConstant.BASE_OUTPUT_PATH.concat(
              WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)));
  private final String devicePath = "root.test_sg.test_d";

  @Before
  public void setUp() throws Exception {
    if (walFile.exists()) {
      Files.delete(walFile.toPath());
    }
    prevIsCluster = IoTDBDescriptor.getInstance().getConfig().isClusterMode();
    IoTDBDescriptor.getInstance().getConfig().setClusterMode(true);
  }

  @After
  public void tearDown() throws Exception {
    if (walFile.exists()) {
      Files.delete(walFile.toPath());
    }
    IoTDBDescriptor.getInstance().getConfig().setClusterMode(prevIsCluster);
  }

  @Test
  public void testReadNormalFile() throws IOException, IllegalPathException {
    int fakeMemTableId = 1;
    List<WALEntry> expectedWALEntries = new ArrayList<>();
    expectedWALEntries.add(new WALInfoEntry(fakeMemTableId, getInsertRowNode(devicePath)));
    expectedWALEntries.add(new WALInfoEntry(fakeMemTableId, getInsertTabletNode(devicePath)));
    expectedWALEntries.add(new WALInfoEntry(fakeMemTableId, getDeleteDataNode(devicePath)));
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

  public static InsertRowNode getInsertRowNode(String devicePath) throws IllegalPathException {
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
    columns[5] = new Binary("hh" + 0);

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

    MeasurementSchema[] schemas =
        new MeasurementSchema[] {
          new MeasurementSchema("s1", dataTypes[0]),
          new MeasurementSchema("s2", dataTypes[1]),
          new MeasurementSchema("s3", dataTypes[2]),
          new MeasurementSchema("s4", dataTypes[3]),
          new MeasurementSchema("s5", dataTypes[4]),
          new MeasurementSchema("s6", dataTypes[5]),
        };
    insertRowNode.setMeasurementSchemas(schemas);
    return insertRowNode;
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
          TSDataType.TEXT
        };

    Object[] columns = new Object[6];
    columns[0] = new double[4];
    columns[1] = new float[4];
    columns[2] = new long[4];
    columns[3] = new int[4];
    columns[4] = new boolean[4];
    columns[5] = new Binary[4];

    for (int r = 0; r < 4; r++) {
      ((double[]) columns[0])[r] = 1.0 + r;
      ((float[]) columns[1])[r] = 2 + r;
      ((long[]) columns[2])[r] = 10000 + r;
      ((int[]) columns[3])[r] = 100 + r;
      ((boolean[]) columns[4])[r] = (r % 2 == 0);
      ((Binary[]) columns[5])[r] = new Binary("hh" + r);
    }

    BitMap[] bitMaps = new BitMap[dataTypes.length];
    for (int i = 0; i < dataTypes.length; i++) {
      if (bitMaps[i] == null) {
        bitMaps[i] = new BitMap(times.length);
      }
      bitMaps[i].mark(i % times.length);
    }

    InsertTabletNode insertTabletNode =
        new InsertTabletNode(
            new PlanNodeId(""),
            new PartialPath(devicePath),
            false,
            new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
            dataTypes,
            times,
            bitMaps,
            columns,
            times.length);
    MeasurementSchema[] schemas =
        new MeasurementSchema[] {
          new MeasurementSchema("s1", dataTypes[0]),
          new MeasurementSchema("s2", dataTypes[1]),
          new MeasurementSchema("s3", dataTypes[2]),
          new MeasurementSchema("s4", dataTypes[3]),
          new MeasurementSchema("s5", dataTypes[4]),
          new MeasurementSchema("s6", dataTypes[5]),
        };
    insertTabletNode.setMeasurementSchemas(schemas);

    return insertTabletNode;
  }

  public static DeleteDataNode getDeleteDataNode(String devicePath) throws IllegalPathException {
    DeleteDataNode deleteDataNode =
        new DeleteDataNode(
            new PlanNodeId(""),
            Collections.singletonList(new PartialPath(devicePath)),
            Long.MIN_VALUE,
            Long.MAX_VALUE);
    deleteDataNode.setSearchIndex(100L);
    return deleteDataNode;
  }
}
