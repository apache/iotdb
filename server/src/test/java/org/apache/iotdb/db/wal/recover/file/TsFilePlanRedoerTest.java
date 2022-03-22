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
package org.apache.iotdb.db.wal.recover.file;

import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.wal.recover.utils.TsFileUtilsForRecoverTest;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DoubleDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class TsFilePlanRedoerTest {
  private static final String SG_NAME = "root.recover_sg";
  private static final String DEVICE1_NAME = SG_NAME.concat(".d1");
  private static final String DEVICE2_NAME = SG_NAME.concat(".d2");
  private static final String FILE_NAME =
      TsFileUtilsForRecoverTest.getTestTsFilePath(SG_NAME, 0, 0, 1);
  private TsFileResource tsFileResource;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    IoTDB.metaManager.setStorageGroup(new PartialPath(SG_NAME));
    IoTDB.metaManager.createTimeseries(
        new PartialPath(DEVICE1_NAME.concat(".s1")),
        TSDataType.INT32,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    IoTDB.metaManager.createTimeseries(
        new PartialPath(DEVICE1_NAME.concat(".s2")),
        TSDataType.INT64,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    IoTDB.metaManager.createTimeseries(
        new PartialPath(DEVICE2_NAME.concat(".s1")),
        TSDataType.FLOAT,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    IoTDB.metaManager.createTimeseries(
        new PartialPath(DEVICE2_NAME.concat(".s2")),
        TSDataType.DOUBLE,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
  }

  @After
  public void tearDown() throws Exception {
    if (tsFileResource != null) {
      tsFileResource.close();
    }
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testRedoInsertRowPlan() throws Exception {
    // generate .tsfile and update resource in memory
    File file = new File(FILE_NAME);
    generateCompleteFile(file);
    tsFileResource = new TsFileResource(file);
    tsFileResource.updateStartTime(DEVICE1_NAME, 1);
    tsFileResource.updateEndTime(DEVICE1_NAME, 2);
    tsFileResource.updateStartTime(DEVICE2_NAME, 3);
    tsFileResource.updateEndTime(DEVICE2_NAME, 4);

    // generate InsertRowPlan
    long time = 5;
    TSDataType[] dataTypes = new TSDataType[] {TSDataType.FLOAT, TSDataType.DOUBLE};
    String[] columns = new String[] {1 + "", 1.0 + ""};
    InsertRowPlan insertRowPlan =
        new InsertRowPlan(
            new PartialPath(DEVICE2_NAME), time, new String[] {"s1", "s2"}, dataTypes, columns);

    // redo InsertTabletPlan, vsg processor is used to test IdTable, don't test IdTable here
    TsFilePlanRedoer planRedoer = new TsFilePlanRedoer(tsFileResource, true, null);
    planRedoer.redoInsert(insertRowPlan);

    // check data in memTable
    IMemTable recoveryMemTable = planRedoer.getRecoveryMemTable();
    // check d2.s1
    MeasurementPath fullPath =
        new MeasurementPath(
            DEVICE2_NAME, "s1", new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    ReadOnlyMemChunk memChunk = recoveryMemTable.query(fullPath, Long.MIN_VALUE, null);
    IPointReader iterator = memChunk.getPointReader();
    time = 5;
    while (iterator.hasNextTimeValuePair()) {
      TimeValuePair timeValuePair = iterator.nextTimeValuePair();
      assertEquals(time, timeValuePair.getTimestamp());
      assertEquals(1, timeValuePair.getValue().getFloat(), 0.0001);
      ++time;
    }
    assertEquals(6, time);
    // check d2.s2
    fullPath =
        new MeasurementPath(
            DEVICE2_NAME, "s2", new MeasurementSchema("s2", TSDataType.DOUBLE, TSEncoding.RLE));
    memChunk = recoveryMemTable.query(fullPath, Long.MIN_VALUE, null);
    iterator = memChunk.getPointReader();
    time = 5;
    while (iterator.hasNextTimeValuePair()) {
      TimeValuePair timeValuePair = iterator.nextTimeValuePair();
      assertEquals(time, timeValuePair.getTimestamp());
      assertEquals(1, timeValuePair.getValue().getDouble(), 0.0001);
      ++time;
    }
    assertEquals(6, time);
  }

  @Test
  public void testRedoInsertTabletPlan() throws Exception {
    // generate .tsfile and update resource in memory
    File file = new File(FILE_NAME);
    generateCompleteFile(file);
    tsFileResource = new TsFileResource(file);
    tsFileResource.updateStartTime(DEVICE1_NAME, 1);
    tsFileResource.updateEndTime(DEVICE1_NAME, 2);
    tsFileResource.updateStartTime(DEVICE2_NAME, 3);
    tsFileResource.updateEndTime(DEVICE2_NAME, 4);

    // generate InsertTabletPlan
    long[] times = {5, 6, 7, 8};
    List<Integer> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.INT32.ordinal());
    dataTypes.add(TSDataType.INT64.ordinal());

    Object[] columns = new Object[2];
    columns[0] = new int[times.length];
    columns[1] = new long[times.length];

    for (int r = 0; r < times.length; r++) {
      ((int[]) columns[0])[r] = 100;
      ((long[]) columns[1])[r] = 10000;
    }

    BitMap[] bitMaps = new BitMap[dataTypes.size()];
    for (int i = 0; i < dataTypes.size(); i++) {
      if (bitMaps[i] == null) {
        bitMaps[i] = new BitMap(times.length);
      }
      // mark value of time=8 as null
      bitMaps[i].mark(3);
    }

    InsertTabletPlan insertTabletPlan =
        new InsertTabletPlan(new PartialPath(DEVICE1_NAME), new String[] {"s1", "s2"}, dataTypes);
    insertTabletPlan.setTimes(times);
    insertTabletPlan.setColumns(columns);
    insertTabletPlan.setRowCount(times.length);
    insertTabletPlan.setBitMaps(bitMaps);

    // redo InsertTabletPlan, vsg processor is used to test IdTable, don't test IdTable here
    TsFilePlanRedoer planRedoer = new TsFilePlanRedoer(tsFileResource, true, null);
    planRedoer.redoInsert(insertTabletPlan);

    // check data in memTable
    IMemTable recoveryMemTable = planRedoer.getRecoveryMemTable();
    // check d1.s1
    MeasurementPath fullPath =
        new MeasurementPath(
            DEVICE1_NAME, "s1", new MeasurementSchema("s1", TSDataType.INT32, TSEncoding.RLE));
    ReadOnlyMemChunk memChunk = recoveryMemTable.query(fullPath, Long.MIN_VALUE, null);
    IPointReader iterator = memChunk.getPointReader();
    int time = 5;
    while (iterator.hasNextTimeValuePair()) {
      TimeValuePair timeValuePair = iterator.nextTimeValuePair();
      assertEquals(time, timeValuePair.getTimestamp());
      assertEquals(100, timeValuePair.getValue().getInt());
      ++time;
    }
    assertEquals(8, time);
    // check d1.s2
    fullPath =
        new MeasurementPath(
            DEVICE1_NAME, "s2", new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE));
    memChunk = recoveryMemTable.query(fullPath, Long.MIN_VALUE, null);
    iterator = memChunk.getPointReader();
    time = 5;
    while (iterator.hasNextTimeValuePair()) {
      TimeValuePair timeValuePair = iterator.nextTimeValuePair();
      assertEquals(time, timeValuePair.getTimestamp());
      assertEquals(10000, timeValuePair.getValue().getLong());
      ++time;
    }
    assertEquals(8, time);
  }

  @Test
  public void testRedoOverLapPlanIntoSeqFile() throws Exception {
    // generate .tsfile and update resource in memory
    File file = new File(FILE_NAME);
    generateCompleteFile(file);
    tsFileResource = new TsFileResource(file);
    tsFileResource.updateStartTime(DEVICE1_NAME, 1);
    tsFileResource.updateEndTime(DEVICE1_NAME, 2);
    tsFileResource.updateStartTime(DEVICE2_NAME, 3);
    tsFileResource.updateEndTime(DEVICE2_NAME, 4);

    // generate InsertTabletPlan, time=3 and time=4 are overlap
    long[] times = {1, 2};
    List<Integer> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.INT32.ordinal());
    dataTypes.add(TSDataType.INT64.ordinal());

    Object[] columns = new Object[2];
    columns[0] = new int[times.length];
    columns[1] = new long[times.length];

    for (int r = 0; r < times.length; r++) {
      ((int[]) columns[0])[r] = 100;
      ((long[]) columns[1])[r] = 10000;
    }

    InsertTabletPlan insertTabletPlan =
        new InsertTabletPlan(new PartialPath(DEVICE1_NAME), new String[] {"s1", "s2"}, dataTypes);
    insertTabletPlan.setTimes(times);
    insertTabletPlan.setColumns(columns);
    insertTabletPlan.setRowCount(times.length);

    // redo InsertTabletPlan, vsg processor is used to test IdTable, don't test IdTable here
    TsFilePlanRedoer planRedoer = new TsFilePlanRedoer(tsFileResource, true, null);
    planRedoer.redoInsert(insertTabletPlan);

    // check data in memTable
    IMemTable recoveryMemTable = planRedoer.getRecoveryMemTable();
    assertTrue(recoveryMemTable.isEmpty());
  }

  @Test
  public void testRedoOverLapPlanIntoUnseqFile() throws Exception {
    // generate .tsfile and update resource in memory
    File file = new File(FILE_NAME);
    generateCompleteFile(file);
    tsFileResource = new TsFileResource(file);
    tsFileResource.updateStartTime(DEVICE1_NAME, 1);
    tsFileResource.updateEndTime(DEVICE1_NAME, 2);
    tsFileResource.updateStartTime(DEVICE2_NAME, 3);
    tsFileResource.updateEndTime(DEVICE2_NAME, 4);

    // generate InsertTabletPlan, time=3 and time=4 are overlap
    long[] times = {1, 2};
    List<Integer> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.INT32.ordinal());
    dataTypes.add(TSDataType.INT64.ordinal());

    Object[] columns = new Object[2];
    columns[0] = new int[times.length];
    columns[1] = new long[times.length];

    for (int r = 0; r < times.length; r++) {
      ((int[]) columns[0])[r] = 100;
      ((long[]) columns[1])[r] = 10000;
    }

    InsertTabletPlan insertTabletPlan =
        new InsertTabletPlan(new PartialPath(DEVICE1_NAME), new String[] {"s1", "s2"}, dataTypes);
    insertTabletPlan.setTimes(times);
    insertTabletPlan.setColumns(columns);
    insertTabletPlan.setRowCount(times.length);

    // redo InsertTabletPlan, vsg processor is used to test IdTable, don't test IdTable here
    TsFilePlanRedoer planRedoer = new TsFilePlanRedoer(tsFileResource, false, null);
    planRedoer.redoInsert(insertTabletPlan);

    // check data in memTable
    IMemTable recoveryMemTable = planRedoer.getRecoveryMemTable();
    // check d1.s1
    MeasurementPath fullPath =
        new MeasurementPath(
            DEVICE1_NAME, "s1", new MeasurementSchema("s1", TSDataType.INT32, TSEncoding.RLE));
    ReadOnlyMemChunk memChunk = recoveryMemTable.query(fullPath, Long.MIN_VALUE, null);
    IPointReader iterator = memChunk.getPointReader();
    int time = 1;
    while (iterator.hasNextTimeValuePair()) {
      TimeValuePair timeValuePair = iterator.nextTimeValuePair();
      assertEquals(time, timeValuePair.getTimestamp());
      assertEquals(100, timeValuePair.getValue().getInt());
      ++time;
    }
    assertEquals(3, time);
    // check d1.s2
    fullPath =
        new MeasurementPath(
            DEVICE1_NAME, "s2", new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE));
    memChunk = recoveryMemTable.query(fullPath, Long.MIN_VALUE, null);
    iterator = memChunk.getPointReader();
    time = 1;
    while (iterator.hasNextTimeValuePair()) {
      TimeValuePair timeValuePair = iterator.nextTimeValuePair();
      assertEquals(time, timeValuePair.getTimestamp());
      assertEquals(10000, timeValuePair.getValue().getLong());
      ++time;
    }
    assertEquals(3, time);
  }

  @Test
  public void testRedoDeletePlan() throws Exception {
    // generate .tsfile and update resource in memory
    File file = new File(FILE_NAME);
    generateCompleteFile(file);
    tsFileResource = new TsFileResource(file);
    tsFileResource.updateStartTime(DEVICE1_NAME, 1);
    tsFileResource.updateEndTime(DEVICE1_NAME, 2);
    tsFileResource.updateStartTime(DEVICE2_NAME, 3);
    tsFileResource.updateEndTime(DEVICE2_NAME, 4);

    // generate DeletePlan
    DeletePlan deletePlan =
        new DeletePlan(Long.MIN_VALUE, Long.MAX_VALUE, new PartialPath(DEVICE1_NAME));

    // redo DeletePlan, vsg processor is used to test IdTable, don't test IdTable here
    File modsFile = new File(FILE_NAME.concat(ModificationFile.FILE_SUFFIX));
    assertFalse(modsFile.exists());
    TsFilePlanRedoer planRedoer = new TsFilePlanRedoer(tsFileResource, false, null);
    planRedoer.redoDelete(deletePlan);
    assertTrue(modsFile.exists());
  }

  private void generateCompleteFile(File tsFile) throws IOException, WriteProcessException {
    try (TsFileWriter writer = new TsFileWriter(tsFile)) {
      writer.registerTimeseries(
          new Path(DEVICE1_NAME), new MeasurementSchema("s1", TSDataType.INT32, TSEncoding.RLE));
      writer.registerTimeseries(
          new Path(DEVICE1_NAME), new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE));
      writer.registerTimeseries(
          new Path(DEVICE2_NAME), new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
      writer.registerTimeseries(
          new Path(DEVICE2_NAME), new MeasurementSchema("s2", TSDataType.DOUBLE, TSEncoding.RLE));
      writer.write(
          new TSRecord(1, DEVICE1_NAME)
              .addTuple(new IntDataPoint("s1", 1))
              .addTuple(new LongDataPoint("s2", 1)));
      writer.write(
          new TSRecord(2, DEVICE1_NAME)
              .addTuple(new IntDataPoint("s1", 2))
              .addTuple(new LongDataPoint("s2", 2)));
      writer.write(
          new TSRecord(3, DEVICE2_NAME)
              .addTuple(new FloatDataPoint("s1", 3))
              .addTuple(new DoubleDataPoint("s2", 3)));
      writer.write(
          new TSRecord(4, DEVICE2_NAME)
              .addTuple(new FloatDataPoint("s1", 4))
              .addTuple(new DoubleDataPoint("s2", 4)));
    }
  }
}
