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

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertMultiTabletPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowsOfOneDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowsPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.wal.buffer.WALEdit;
import org.apache.iotdb.db.wal.buffer.WALEditType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class WALFileTest {
  private final File walFile = new File("_0.wal");
  private final String devicePath = "root.test_sg.test_d";

  @Before
  public void setUp() throws Exception {
    if (walFile.exists()) {
      Files.delete(walFile.toPath());
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
    List<WALEdit> expectedWALEdits = new ArrayList<>();
    expectedWALEdits.add(new WALEdit(fakeMemTableId, getInsertRowPlan(devicePath)));
    expectedWALEdits.add(new WALEdit(fakeMemTableId, getInsertRowsOfOneDevicePlan(devicePath)));
    expectedWALEdits.add(new WALEdit(fakeMemTableId, getInsertRowsPlan(devicePath)));
    expectedWALEdits.add(new WALEdit(fakeMemTableId, getInsertTabletPlan(devicePath)));
    expectedWALEdits.add(new WALEdit(fakeMemTableId, getInsertMultiTabletPlan(devicePath)));
    expectedWALEdits.add(new WALEdit(fakeMemTableId, getDeletePlan(devicePath)));
    // test WALEdit.serializedSize
    int size = 0;
    for (WALEdit walEdit : expectedWALEdits) {
      size += walEdit.serializedSize();
    }
    WALByteBuffer buffer = new WALByteBuffer(ByteBuffer.allocate(size));
    // test WALEdit.serialize
    for (WALEdit walEdit : expectedWALEdits) {
      walEdit.serialize(buffer);
    }
    assertEquals(0, buffer.buffer.remaining());
    // test WALEdit.write
    try (ILogWriter walWriter = new WALWriter(walFile)) {
      walWriter.write(buffer.buffer);
    }
    // test WALReader.readAll
    List<WALEdit> actualWALEdits = new ArrayList<>();
    try (WALReader walReader = new WALReader(walFile)) {
      while (walReader.hasNext()) {
        actualWALEdits.add(walReader.next());
      }
    }
    assertEquals(expectedWALEdits, actualWALEdits);
  }

  @Test
  public void testReadNotExistFile() throws IOException {
    if (walFile.createNewFile()) {
      List<WALEdit> actualWALEdits = new ArrayList<>();
      try (WALReader walReader = new WALReader(walFile)) {
        while (walReader.hasNext()) {
          actualWALEdits.add(walReader.next());
        }
      }
      assertEquals(0, actualWALEdits.size());
    }
  }

  @Test
  public void testReadBrokenFile() throws IOException, IllegalPathException {
    int fakeMemTableId = 1;
    List<WALEdit> expectedWALEdits = new ArrayList<>();
    expectedWALEdits.add(new WALEdit(fakeMemTableId, getInsertRowPlan(devicePath)));
    expectedWALEdits.add(new WALEdit(fakeMemTableId, getInsertRowsOfOneDevicePlan(devicePath)));
    expectedWALEdits.add(new WALEdit(fakeMemTableId, getInsertRowsPlan(devicePath)));
    expectedWALEdits.add(new WALEdit(fakeMemTableId, getInsertTabletPlan(devicePath)));
    expectedWALEdits.add(new WALEdit(fakeMemTableId, getInsertMultiTabletPlan(devicePath)));
    expectedWALEdits.add(new WALEdit(fakeMemTableId, getDeletePlan(devicePath)));
    // test WALEdit.serializedSize
    int size = Byte.BYTES;
    for (WALEdit walEdit : expectedWALEdits) {
      size += walEdit.serializedSize();
    }
    WALByteBuffer buffer = new WALByteBuffer(ByteBuffer.allocate(size));
    // test WALEdit.serialize
    for (WALEdit walEdit : expectedWALEdits) {
      walEdit.serialize(buffer);
    }
    // add broken part
    buffer.put(WALEditType.DELETE_PLAN.getCode());
    assertEquals(0, buffer.buffer.remaining());
    // test WALEdit.write
    try (ILogWriter walWriter = new WALWriter(walFile)) {
      walWriter.write(buffer.buffer);
    }
    // test WALReader.readAll
    List<WALEdit> actualWALEdits = new ArrayList<>();
    try (WALReader walReader = new WALReader(walFile)) {
      while (walReader.hasNext()) {
        actualWALEdits.add(walReader.next());
      }
    }
    assertEquals(expectedWALEdits, actualWALEdits);
  }

  public static InsertRowPlan getInsertRowPlan(String devicePath) throws IllegalPathException {
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

    String[] columns = new String[6];
    columns[0] = 1.0 + "";
    columns[1] = 2 + "";
    columns[2] = 10000 + "";
    columns[3] = 100 + "";
    columns[4] = false + "";
    columns[5] = "hh" + 0;

    return new InsertRowPlan(
        new PartialPath(devicePath),
        time,
        new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
        dataTypes,
        columns);
  }

  public static InsertRowsOfOneDevicePlan getInsertRowsOfOneDevicePlan(String devicePath)
      throws IllegalPathException {
    InsertRowPlan insertRowPlan1 = getInsertRowPlan(devicePath);
    InsertRowPlan insertRowPlan2 = getInsertRowPlan(devicePath);
    insertRowPlan2.setTime(200L);
    InsertRowPlan[] rowPlans = {insertRowPlan1, insertRowPlan2};
    return new InsertRowsOfOneDevicePlan(
        insertRowPlan1.getDevicePath(), rowPlans, new int[] {0, 1});
  }

  public static InsertRowsPlan getInsertRowsPlan(String devicePath) throws IllegalPathException {
    InsertRowsPlan insertRowsPlan = new InsertRowsPlan();
    InsertRowPlan insertRowPlan1 = getInsertRowPlan(devicePath);
    insertRowsPlan.addOneInsertRowPlan(insertRowPlan1, 0);
    InsertRowPlan insertRowPlan2 = getInsertRowPlan(devicePath);
    insertRowPlan2.setTime(200L);
    insertRowsPlan.addOneInsertRowPlan(insertRowPlan2, 1);
    return insertRowsPlan;
  }

  public static InsertTabletPlan getInsertTabletPlan(String devicePath)
      throws IllegalPathException {
    long[] times = new long[] {110L, 111L, 112L, 113L};
    List<Integer> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.DOUBLE.ordinal());
    dataTypes.add(TSDataType.FLOAT.ordinal());
    dataTypes.add(TSDataType.INT64.ordinal());
    dataTypes.add(TSDataType.INT32.ordinal());
    dataTypes.add(TSDataType.BOOLEAN.ordinal());
    dataTypes.add(TSDataType.TEXT.ordinal());

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

    BitMap[] bitMaps = new BitMap[dataTypes.size()];
    for (int i = 0; i < dataTypes.size(); i++) {
      if (bitMaps[i] == null) {
        bitMaps[i] = new BitMap(times.length);
      }
      bitMaps[i].mark(i % times.length);
    }

    InsertTabletPlan insertTabletPlan =
        new InsertTabletPlan(
            new PartialPath(devicePath),
            new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
            dataTypes);
    insertTabletPlan.setTimes(times);
    insertTabletPlan.setColumns(columns);
    insertTabletPlan.setRowCount(times.length);
    insertTabletPlan.setBitMaps(bitMaps);
    return insertTabletPlan;
  }

  public static InsertMultiTabletPlan getInsertMultiTabletPlan(String devicePath)
      throws IllegalPathException {
    List<InsertTabletPlan> insertTabletPlans = new ArrayList<>();
    InsertTabletPlan insertTabletPlan1 = getInsertTabletPlan(devicePath);
    insertTabletPlans.add(insertTabletPlan1);
    InsertTabletPlan insertTabletPlan2 = getInsertTabletPlan(devicePath);
    insertTabletPlan2.setTimes(new long[] {114L, 115L, 116L, 117L});
    insertTabletPlans.add(insertTabletPlan2);
    return new InsertMultiTabletPlan(insertTabletPlans);
  }

  public static DeletePlan getDeletePlan(String devicePath) throws IllegalPathException {
    return new DeletePlan(Long.MIN_VALUE, Long.MAX_VALUE, new PartialPath(devicePath));
  }

  public static class WALByteBuffer implements IWALByteBufferView {
    private final ByteBuffer buffer;

    public WALByteBuffer(ByteBuffer buffer) {
      this.buffer = buffer;
    }

    @Override
    public void put(byte b) {
      buffer.put(b);
    }

    @Override
    public void put(byte[] src) {
      buffer.put(src);
    }

    @Override
    public void putChar(char value) {
      buffer.putChar(value);
    }

    @Override
    public void putShort(short value) {
      buffer.putShort(value);
    }

    @Override
    public void putInt(int value) {
      buffer.putInt(value);
    }

    @Override
    public void putLong(long value) {
      buffer.putLong(value);
    }

    @Override
    public void putFloat(float value) {
      buffer.putFloat(value);
    }

    @Override
    public void putDouble(double value) {
      buffer.putDouble(value);
    }

    public ByteBuffer getBuffer() {
      return buffer;
    }
  }
}
