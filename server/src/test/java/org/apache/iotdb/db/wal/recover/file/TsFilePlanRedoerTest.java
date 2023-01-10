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

import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.wal.utils.TsFileUtilsForRecoverTest;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.BooleanDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.DoubleDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.StringDataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TsFilePlanRedoerTest {
  private static final String SG_NAME = "root.recover_sg";
  private static final String DEVICE1_NAME = SG_NAME.concat(".d1");
  private static final String DEVICE2_NAME = SG_NAME.concat(".d2");
  private static final String DEVICE3_NAME = SG_NAME.concat(".d3");
  private static final String FILE_NAME =
      TsFileUtilsForRecoverTest.getTestTsFilePath(SG_NAME, 0, 0, 1);
  private TsFileResource tsFileResource;
  private CompressionType compressionType;
  boolean prevIsAutoCreateSchemaEnabled;
  boolean prevIsEnablePartialInsert;
  boolean prevIsCluster;

  @Before
  public void setUp() throws Exception {
    prevIsCluster = IoTDBDescriptor.getInstance().getConfig().isClusterMode();
    EnvironmentUtils.envSetUp();
    IoTDBDescriptor.getInstance().getConfig().setClusterMode(true);

    // set recover config, avoid creating deleted time series when recovering wal
    prevIsAutoCreateSchemaEnabled =
        IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled();
    //    IoTDBDescriptor.getInstance().getConfig().setAutoCreateSchemaEnabled(false);
    prevIsEnablePartialInsert = IoTDBDescriptor.getInstance().getConfig().isEnablePartialInsert();
    IoTDBDescriptor.getInstance().getConfig().setEnablePartialInsert(true);
    compressionType = TSFileDescriptor.getInstance().getConfig().getCompressor();
  }

  @After
  public void tearDown() throws Exception {
    if (tsFileResource != null) {
      tsFileResource.close();
    }
    File modsFile = new File(FILE_NAME.concat(ModificationFile.FILE_SUFFIX));
    if (modsFile.exists()) {
      modsFile.delete();
    }
    IoTDBDescriptor.getInstance().getConfig().setClusterMode(prevIsCluster);
    EnvironmentUtils.cleanEnv();
    // reset config
    //    IoTDBDescriptor.getInstance()
    //        .getConfig()
    //        .setAutoCreateSchemaEnabled(prevIsAutoCreateSchemaEnabled);
    IoTDBDescriptor.getInstance().getConfig().setEnablePartialInsert(prevIsEnablePartialInsert);
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
    Object[] columns = new Object[] {1f, 1.0d};
    InsertRowNode insertRowNode =
        new InsertRowNode(
            new PlanNodeId("0"),
            new PartialPath(DEVICE2_NAME),
            false,
            new String[] {"s1", "s2"},
            dataTypes,
            time,
            columns,
            false);
    insertRowNode.setMeasurementSchemas(
        new MeasurementSchema[] {
          new MeasurementSchema("s1", TSDataType.FLOAT),
          new MeasurementSchema("s2", TSDataType.DOUBLE),
        });

    // redo InsertTabletPlan, vsg processor is used to test IdTable, don't test IdTable here
    TsFilePlanRedoer planRedoer = new TsFilePlanRedoer(tsFileResource, true, null);
    planRedoer.redoInsert(insertRowNode);

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
  public void testRedoInsertAlignedRowPlan() throws Exception {
    // generate .tsfile and update resource in memory
    File file = new File(FILE_NAME);
    generateCompleteFile(file);
    tsFileResource = new TsFileResource(file);
    tsFileResource.updateStartTime(DEVICE3_NAME, 5);
    tsFileResource.updateStartTime(DEVICE3_NAME, 5);

    // generate InsertRowPlan
    long time = 6;
    TSDataType[] dataTypes =
        new TSDataType[] {
          TSDataType.INT32, TSDataType.INT64, TSDataType.BOOLEAN, TSDataType.FLOAT, TSDataType.TEXT
        };
    Object[] columns = new Object[] {1, 1L, true, 1.0f, new Binary("1")};

    InsertRowNode insertRowNode =
        new InsertRowNode(
            new PlanNodeId("0"),
            new PartialPath(DEVICE3_NAME),
            true,
            new String[] {"s1", "s2", "s3", "s4", "s5"},
            dataTypes,
            time,
            columns,
            false);
    insertRowNode.setMeasurementSchemas(
        new MeasurementSchema[] {
          new MeasurementSchema("s1", TSDataType.INT32),
          new MeasurementSchema("s2", TSDataType.INT64),
          new MeasurementSchema("s3", TSDataType.BOOLEAN),
          new MeasurementSchema("s4", TSDataType.FLOAT),
          new MeasurementSchema("s5", TSDataType.TEXT),
        });

    // redo InsertTabletPlan, vsg processor is used to test IdTable, don't test IdTable here
    TsFilePlanRedoer planRedoer = new TsFilePlanRedoer(tsFileResource, true, null);
    planRedoer.redoInsert(insertRowNode);

    // check data in memTable
    IMemTable recoveryMemTable = planRedoer.getRecoveryMemTable();
    // check d3
    AlignedPath fullPath =
        new AlignedPath(
            DEVICE3_NAME,
            Arrays.asList("s1", "s2", "s3", "s4", "s5"),
            Arrays.asList(
                new MeasurementSchema("s1", TSDataType.INT32, TSEncoding.RLE),
                new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE),
                new MeasurementSchema("s3", TSDataType.BOOLEAN, TSEncoding.RLE),
                new MeasurementSchema("s4", TSDataType.FLOAT, TSEncoding.RLE),
                new MeasurementSchema("s5", TSDataType.TEXT, TSEncoding.PLAIN)));
    ReadOnlyMemChunk memChunk = recoveryMemTable.query(fullPath, Long.MIN_VALUE, null);
    IPointReader iterator = memChunk.getPointReader();
    time = 6;
    while (iterator.hasNextTimeValuePair()) {
      TimeValuePair timeValuePair = iterator.nextTimeValuePair();
      assertEquals(time, timeValuePair.getTimestamp());
      assertEquals(1, timeValuePair.getValue().getVector()[0].getInt());
      assertEquals(1L, timeValuePair.getValue().getVector()[1].getLong());
      assertEquals(true, timeValuePair.getValue().getVector()[2].getBoolean());
      assertEquals(1, timeValuePair.getValue().getVector()[3].getFloat(), 0.00001);
      assertEquals(Binary.valueOf("1"), timeValuePair.getValue().getVector()[4].getBinary());
      ++time;
    }
    assertEquals(7, time);
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

    InsertTabletNode insertTabletNode =
        new InsertTabletNode(
            new PlanNodeId("0"),
            new PartialPath(DEVICE1_NAME),
            false,
            new String[] {"s1", "s2"},
            new TSDataType[] {TSDataType.INT32, TSDataType.INT64},
            times,
            bitMaps,
            columns,
            times.length);
    insertTabletNode.setMeasurementSchemas(
        new MeasurementSchema[] {
          new MeasurementSchema("s1", TSDataType.INT32),
          new MeasurementSchema("s2", TSDataType.INT64),
        });

    // redo InsertTabletPlan, vsg processor is used to test IdTable, don't test IdTable here
    TsFilePlanRedoer planRedoer = new TsFilePlanRedoer(tsFileResource, true, null);
    planRedoer.redoInsert(insertTabletNode);

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
  public void testRedoInsertAlignedTabletPlan() throws Exception {
    // generate .tsfile and update resource in memory
    File file = new File(FILE_NAME);
    generateCompleteFile(file);
    tsFileResource = new TsFileResource(file);
    tsFileResource.updateStartTime(DEVICE3_NAME, 5);
    tsFileResource.updateStartTime(DEVICE3_NAME, 5);

    // generate InsertTabletPlan
    long[] times = {6, 7, 8, 9};
    List<Integer> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.INT32.ordinal());
    dataTypes.add(TSDataType.INT64.ordinal());
    dataTypes.add(TSDataType.BOOLEAN.ordinal());
    dataTypes.add(TSDataType.FLOAT.ordinal());
    dataTypes.add(TSDataType.TEXT.ordinal());

    Object[] columns = new Object[5];
    columns[0] = new int[times.length];
    columns[1] = new long[times.length];
    columns[2] = new boolean[times.length];
    columns[3] = new float[times.length];
    columns[4] = new Binary[times.length];

    for (int r = 0; r < times.length; r++) {
      ((int[]) columns[0])[r] = (r + 1) * 100;
      ((long[]) columns[1])[r] = (r + 1) * 100;
      ((boolean[]) columns[2])[r] = true;
      ((float[]) columns[3])[r] = (r + 1) * 100;
      ((Binary[]) columns[4])[r] = Binary.valueOf((r + 1) * 100 + "");
    }

    BitMap[] bitMaps = new BitMap[dataTypes.size()];
    for (int i = 0; i < dataTypes.size(); i++) {
      if (bitMaps[i] == null) {
        bitMaps[i] = new BitMap(times.length);
      }
      // mark value of time=9 as null
      bitMaps[i].mark(3);
    }

    InsertTabletNode insertTabletNode =
        new InsertTabletNode(
            new PlanNodeId("0"),
            new PartialPath(DEVICE3_NAME),
            true,
            new String[] {"s1", "s2", "s3", "s4", "s5"},
            new TSDataType[] {
              TSDataType.INT32,
              TSDataType.INT64,
              TSDataType.BOOLEAN,
              TSDataType.FLOAT,
              TSDataType.TEXT
            },
            times,
            bitMaps,
            columns,
            times.length);
    insertTabletNode.setMeasurementSchemas(
        new MeasurementSchema[] {
          new MeasurementSchema("s1", TSDataType.INT32),
          new MeasurementSchema("s2", TSDataType.INT64),
          new MeasurementSchema("s3", TSDataType.BOOLEAN),
          new MeasurementSchema("s4", TSDataType.FLOAT),
          new MeasurementSchema("s5", TSDataType.TEXT),
        });

    // redo InsertTabletPlan, vsg processor is used to test IdTable, don't test IdTable here
    TsFilePlanRedoer planRedoer = new TsFilePlanRedoer(tsFileResource, true, null);
    planRedoer.redoInsert(insertTabletNode);

    // check data in memTable
    IMemTable recoveryMemTable = planRedoer.getRecoveryMemTable();
    // check d3
    AlignedPath fullPath =
        new AlignedPath(
            DEVICE3_NAME,
            Arrays.asList("s1", "s2", "s3", "s4", "s5"),
            Arrays.asList(
                new MeasurementSchema("s1", TSDataType.INT32, TSEncoding.RLE),
                new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE),
                new MeasurementSchema("s3", TSDataType.BOOLEAN, TSEncoding.RLE),
                new MeasurementSchema("s4", TSDataType.FLOAT, TSEncoding.RLE),
                new MeasurementSchema("s5", TSDataType.TEXT, TSEncoding.PLAIN)));
    ReadOnlyMemChunk memChunk = recoveryMemTable.query(fullPath, Long.MIN_VALUE, null);
    IPointReader iterator = memChunk.getPointReader();
    int time = 6;
    while (iterator.hasNextTimeValuePair()) {
      TimeValuePair timeValuePair = iterator.nextTimeValuePair();
      assertEquals(time, timeValuePair.getTimestamp());
      assertEquals((time - 5) * 100, timeValuePair.getValue().getVector()[0].getInt());
      assertEquals((time - 5) * 100L, timeValuePair.getValue().getVector()[1].getLong());
      assertEquals(true, timeValuePair.getValue().getVector()[2].getBoolean());
      assertEquals((time - 5) * 100, timeValuePair.getValue().getVector()[3].getFloat(), 0.00001);
      assertEquals(
          Binary.valueOf((time - 5) * 100 + ""),
          timeValuePair.getValue().getVector()[4].getBinary());
      ++time;
    }
    assertEquals(9, time);
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

    InsertTabletNode insertTabletNode =
        new InsertTabletNode(
            new PlanNodeId("0"),
            new PartialPath(DEVICE1_NAME),
            false,
            new String[] {"s1", "s2"},
            new TSDataType[] {TSDataType.INT32, TSDataType.INT64},
            times,
            null,
            columns,
            times.length);

    // redo InsertTabletPlan, vsg processor is used to test IdTable, don't test IdTable here
    TsFilePlanRedoer planRedoer = new TsFilePlanRedoer(tsFileResource, true, null);
    planRedoer.redoInsert(insertTabletNode);

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

    InsertTabletNode insertTabletNode =
        new InsertTabletNode(
            new PlanNodeId("0"),
            new PartialPath(DEVICE1_NAME),
            false,
            new String[] {"s1", "s2"},
            new TSDataType[] {TSDataType.INT32, TSDataType.INT64},
            times,
            null,
            columns,
            times.length);
    insertTabletNode.setMeasurementSchemas(
        new MeasurementSchema[] {
          new MeasurementSchema("s1", TSDataType.INT32),
          new MeasurementSchema("s2", TSDataType.INT64),
        });

    // redo InsertTabletPlan, vsg processor is used to test IdTable, don't test IdTable here
    TsFilePlanRedoer planRedoer = new TsFilePlanRedoer(tsFileResource, false, null);
    planRedoer.redoInsert(insertTabletNode);

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
  public void testRedoDeleteDataNode() throws Exception {
    // generate .tsfile and update resource in memory
    File file = new File(FILE_NAME);
    generateCompleteFile(file);
    tsFileResource = new TsFileResource(file);
    tsFileResource.updateStartTime(DEVICE1_NAME, 1);
    tsFileResource.updateEndTime(DEVICE1_NAME, 2);
    tsFileResource.updateStartTime(DEVICE2_NAME, 3);
    tsFileResource.updateEndTime(DEVICE2_NAME, 4);

    // generate DeleteDataNode
    DeleteDataNode deleteDataNode =
        new DeleteDataNode(
            new PlanNodeId(""),
            Collections.singletonList(new PartialPath(DEVICE1_NAME)),
            Long.MIN_VALUE,
            Long.MAX_VALUE);

    // redo DeleteDataNode, vsg processor is used to test IdTable, don't test IdTable here
    File modsFile = new File(FILE_NAME.concat(ModificationFile.FILE_SUFFIX));
    assertFalse(modsFile.exists());
    TsFilePlanRedoer planRedoer = new TsFilePlanRedoer(tsFileResource, false, null);
    planRedoer.redoDelete(deleteDataNode);
    assertTrue(modsFile.exists());
  }

  @Test
  public void testRedoAlignedInsertAfterDeleteTimeseries() throws Exception {
    // generate .tsfile and update resource in memory
    File file = new File(FILE_NAME);
    generateCompleteFile(file);
    tsFileResource = new TsFileResource(file);
    tsFileResource.updateStartTime(DEVICE3_NAME, 5);
    tsFileResource.updateStartTime(DEVICE3_NAME, 5);

    // generate InsertTabletPlan
    long[] times = {6, 7, 8, 9};
    List<Integer> dataTypes =
        Arrays.asList(
            TSDataType.INT32.ordinal(),
            TSDataType.INT64.ordinal(),
            TSDataType.BOOLEAN.ordinal(),
            TSDataType.FLOAT.ordinal(),
            TSDataType.TEXT.ordinal());
    Object[] columns =
        new Object[] {
          new int[times.length],
          new long[times.length],
          new boolean[times.length],
          new float[times.length],
          new Binary[times.length]
        };
    for (int r = 0; r < times.length; r++) {
      ((int[]) columns[0])[r] = (r + 1) * 100;
      ((long[]) columns[1])[r] = (r + 1) * 100;
      ((boolean[]) columns[2])[r] = true;
      ((float[]) columns[3])[r] = (r + 1) * 100;
      ((Binary[]) columns[4])[r] = Binary.valueOf((r + 1) * 100 + "");
    }
    BitMap[] bitMaps = new BitMap[dataTypes.size()];
    for (int i = 0; i < dataTypes.size(); i++) {
      if (bitMaps[i] == null) {
        bitMaps[i] = new BitMap(times.length);
      }
      // mark value of time=9 as null
      bitMaps[i].mark(3);
    }
    InsertTabletNode insertTabletNode =
        new InsertTabletNode(
            new PlanNodeId(""),
            new PartialPath(DEVICE3_NAME),
            true,
            new String[] {null, "s2", "s3", "s4", null},
            new TSDataType[] {
              TSDataType.INT32,
              TSDataType.INT64,
              TSDataType.BOOLEAN,
              TSDataType.FLOAT,
              TSDataType.TEXT
            },
            times,
            bitMaps,
            columns,
            times.length);
    // redo InsertTabletPlan, data region is used to test IdTable, don't test IdTable here
    TsFilePlanRedoer planRedoer = new TsFilePlanRedoer(tsFileResource, true, null);
    MeasurementSchema[] schemas =
        new MeasurementSchema[] {
          null,
          new MeasurementSchema("s2", TSDataType.INT64),
          new MeasurementSchema("s3", TSDataType.BOOLEAN),
          new MeasurementSchema("s4", TSDataType.FLOAT),
          null
        };
    insertTabletNode.setMeasurementSchemas(schemas);
    planRedoer.redoInsert(insertTabletNode);

    // generate InsertRowPlan
    int time = 9;
    TSDataType[] dataTypes2 =
        new TSDataType[] {
          TSDataType.INT32, TSDataType.INT64, TSDataType.BOOLEAN, TSDataType.FLOAT, TSDataType.TEXT
        };
    Object[] columns2 = new Object[] {400, 400L, true, 400.0f, new Binary("400")};
    // redo InsertTabletPlan, data region is used to test IdTable, don't test IdTable here
    InsertRowNode insertRowNode =
        new InsertRowNode(
            new PlanNodeId(""),
            new PartialPath(DEVICE3_NAME),
            true,
            new String[] {null, "s2", "s3", "s4", null},
            dataTypes2,
            time,
            columns2,
            false);
    insertRowNode.setMeasurementSchemas(schemas);
    planRedoer.redoInsert(insertRowNode);

    // check data in memTable
    IMemTable recoveryMemTable = planRedoer.getRecoveryMemTable();
    // check d3
    AlignedPath fullPath =
        new AlignedPath(
            DEVICE3_NAME,
            Arrays.asList("s1", "s2", "s3", "s4", "s5"),
            Arrays.asList(
                new MeasurementSchema("s1", TSDataType.INT32, TSEncoding.RLE),
                new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE),
                new MeasurementSchema("s3", TSDataType.BOOLEAN, TSEncoding.RLE),
                new MeasurementSchema("s4", TSDataType.FLOAT, TSEncoding.RLE),
                new MeasurementSchema("s5", TSDataType.TEXT, TSEncoding.PLAIN)));
    ReadOnlyMemChunk memChunk = recoveryMemTable.query(fullPath, Long.MIN_VALUE, null);
    IPointReader iterator = memChunk.getPointReader();
    time = 6;
    while (iterator.hasNextTimeValuePair()) {
      TimeValuePair timeValuePair = iterator.nextTimeValuePair();
      assertEquals(time, timeValuePair.getTimestamp());
      assertEquals(null, timeValuePair.getValue().getVector()[0]);
      assertEquals((time - 5) * 100L, timeValuePair.getValue().getVector()[1].getLong());
      assertEquals(true, timeValuePair.getValue().getVector()[2].getBoolean());
      assertEquals((time - 5) * 100, timeValuePair.getValue().getVector()[3].getFloat(), 0.00001);
      assertEquals(null, timeValuePair.getValue().getVector()[4]);
      time++;
    }
    assertEquals(10, time);
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
      writer.registerAlignedTimeseries(
          new Path(DEVICE3_NAME),
          Arrays.asList(
              new MeasurementSchema("s1", TSDataType.INT32, TSEncoding.RLE),
              new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE),
              new MeasurementSchema("s3", TSDataType.BOOLEAN, TSEncoding.RLE),
              new MeasurementSchema("s4", TSDataType.FLOAT, TSEncoding.RLE),
              new MeasurementSchema("s5", TSDataType.TEXT, TSEncoding.PLAIN)));
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
      writer.writeAligned(
          new TSRecord(5, DEVICE3_NAME)
              .addTuple(new IntDataPoint("s1", 5))
              .addTuple(new LongDataPoint("s2", 5))
              .addTuple(new BooleanDataPoint("s3", true))
              .addTuple(new FloatDataPoint("s4", 5))
              .addTuple(new StringDataPoint("s5", Binary.valueOf("5"))));
    }
  }
}
