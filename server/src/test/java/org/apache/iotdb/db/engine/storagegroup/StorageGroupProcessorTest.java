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
package org.apache.iotdb.db.engine.storagegroup;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.MetadataManagerHelper;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.flush.FlushManager;
import org.apache.iotdb.db.engine.flush.TsFileFlushPolicy;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.exception.ShutdownException;
import org.apache.iotdb.db.exception.StorageGroupProcessorException;
import org.apache.iotdb.db.exception.TriggerExecutionException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.rescon.MemTableManager;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class StorageGroupProcessorTest {
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static Logger logger = LoggerFactory.getLogger(StorageGroupProcessorTest.class);

  private String storageGroup = "root.vehicle.d0";
  private String systemDir = TestConstant.OUTPUT_DATA_DIR.concat("info");
  private String deviceId = "root.vehicle.d0";
  private String measurementId = "s0";
  private VirtualStorageGroupProcessor processor;
  private QueryContext context = EnvironmentUtils.TEST_QUERY_CONTEXT;

  @Before
  public void setUp() throws Exception {
    MetadataManagerHelper.initMetadata();
    EnvironmentUtils.envSetUp();
    processor = new DummySGP(systemDir, storageGroup);
    CompactionTaskManager.getInstance().start();
  }

  @After
  public void tearDown() throws Exception {
    processor.syncDeleteDataFiles();
    EnvironmentUtils.cleanEnv();
    EnvironmentUtils.cleanDir(TestConstant.OUTPUT_DATA_DIR);
    CompactionTaskManager.getInstance().stop();
    EnvironmentUtils.cleanEnv();
  }

  private void insertToStorageGroupProcessor(TSRecord record)
      throws WriteProcessException, IllegalPathException, TriggerExecutionException {
    InsertRowPlan insertRowPlan = new InsertRowPlan(record);
    processor.insert(insertRowPlan);
  }

  @Test
  public void testUnseqUnsealedDelete()
      throws WriteProcessException, IOException, MetadataException, TriggerExecutionException {
    TSRecord record = new TSRecord(10000, deviceId);
    record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(1000)));
    processor.insert(new InsertRowPlan(record));
    processor.syncCloseAllWorkingTsFileProcessors();

    for (int j = 1; j <= 10; j++) {
      record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      processor.insert(new InsertRowPlan(record));
    }

    for (TsFileProcessor tsfileProcessor : processor.getWorkUnsequenceTsFileProcessors()) {
      tsfileProcessor.syncFlush();
    }

    for (int j = 11; j <= 20; j++) {
      record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      processor.insert(new InsertRowPlan(record));
    }

    PartialPath fullPath =
        new MeasurementPath(
            deviceId,
            measurementId,
            new MeasurementSchema(
                measurementId,
                TSDataType.INT32,
                TSEncoding.RLE,
                CompressionType.UNCOMPRESSED,
                Collections.emptyMap()));

    processor.delete(new PartialPath(deviceId, measurementId), 0, 15L, -1, null);

    List<TsFileResource> tsfileResourcesForQuery = new ArrayList<>();
    for (TsFileProcessor tsfileProcessor : processor.getWorkUnsequenceTsFileProcessors()) {
      tsfileProcessor.query(
          Collections.singletonList(fullPath),
          EnvironmentUtils.TEST_QUERY_CONTEXT,
          tsfileResourcesForQuery);
    }

    Assert.assertEquals(1, tsfileResourcesForQuery.size());
    List<ReadOnlyMemChunk> memChunks = tsfileResourcesForQuery.get(0).getReadOnlyMemChunk(fullPath);
    long time = 16;
    for (ReadOnlyMemChunk memChunk : memChunks) {
      IPointReader iterator = memChunk.getPointReader();
      while (iterator.hasNextTimeValuePair()) {
        TimeValuePair timeValuePair = iterator.nextTimeValuePair();
        Assert.assertEquals(time++, timeValuePair.getTimestamp());
      }
    }
  }

  @Test
  public void testSequenceSyncClose()
      throws WriteProcessException, QueryProcessException, IllegalPathException,
          TriggerExecutionException {
    for (int j = 1; j <= 10; j++) {
      TSRecord record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      processor.insert(new InsertRowPlan(record));
      processor.asyncCloseAllWorkingTsFileProcessors();
    }

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    processor.syncCloseAllWorkingTsFileProcessors();
    QueryDataSource queryDataSource =
        processor.query(
            Collections.singletonList(new PartialPath(deviceId, measurementId)),
            deviceId,
            context,
            null,
            null);
    Assert.assertEquals(10, queryDataSource.getSeqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      Assert.assertTrue(resource.isClosed());
    }
  }

  @Test
  public void testInsertDataAndRemovePartitionAndInsert()
      throws WriteProcessException, QueryProcessException, IllegalPathException,
          TriggerExecutionException {
    for (int j = 0; j < 10; j++) {
      TSRecord record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      processor.insert(new InsertRowPlan(record));
      processor.asyncCloseAllWorkingTsFileProcessors();
    }
    processor.syncCloseAllWorkingTsFileProcessors();

    processor.removePartitions((storageGroupName, timePartitionId) -> true);

    for (int j = 0; j < 10; j++) {
      TSRecord record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      processor.insert(new InsertRowPlan(record));
      processor.asyncCloseAllWorkingTsFileProcessors();
    }
    processor.syncCloseAllWorkingTsFileProcessors();

    QueryDataSource queryDataSource =
        processor.query(
            Collections.singletonList(new PartialPath(deviceId, measurementId)),
            deviceId,
            context,
            null,
            null);
    Assert.assertEquals(0, queryDataSource.getUnseqResources().size());
  }

  @Test
  public void testIoTDBTabletWriteAndSyncClose()
      throws QueryProcessException, IllegalPathException, TriggerExecutionException {
    String[] measurements = new String[2];
    measurements[0] = "s0";
    measurements[1] = "s1";
    List<Integer> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.INT32.ordinal());
    dataTypes.add(TSDataType.INT64.ordinal());

    IMeasurementMNode[] measurementMNodes = new IMeasurementMNode[2];
    measurementMNodes[0] =
        MeasurementMNode.getMeasurementMNode(
            null, "s0", new MeasurementSchema("s0", TSDataType.INT32, TSEncoding.PLAIN), null);
    measurementMNodes[1] =
        MeasurementMNode.getMeasurementMNode(
            null, "s1", new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN), null);

    InsertTabletPlan insertTabletPlan1 =
        new InsertTabletPlan(new PartialPath("root.vehicle.d0"), measurements, dataTypes);
    insertTabletPlan1.setMeasurementMNodes(measurementMNodes);

    long[] times = new long[100];
    Object[] columns = new Object[2];
    columns[0] = new int[100];
    columns[1] = new long[100];

    for (int r = 0; r < 100; r++) {
      times[r] = r;
      ((int[]) columns[0])[r] = 1;
      ((long[]) columns[1])[r] = 1;
    }
    insertTabletPlan1.setTimes(times);
    insertTabletPlan1.setColumns(columns);
    insertTabletPlan1.setRowCount(times.length);

    processor.insertTablet(insertTabletPlan1);
    processor.asyncCloseAllWorkingTsFileProcessors();

    InsertTabletPlan insertTabletPlan2 =
        new InsertTabletPlan(new PartialPath("root.vehicle.d0"), measurements, dataTypes);
    insertTabletPlan2.setMeasurementMNodes(measurementMNodes);

    for (int r = 50; r < 149; r++) {
      times[r - 50] = r;
      ((int[]) columns[0])[r - 50] = 1;
      ((long[]) columns[1])[r - 50] = 1;
    }
    insertTabletPlan2.setTimes(times);
    insertTabletPlan2.setColumns(columns);
    insertTabletPlan2.setRowCount(times.length);

    processor.insertTablet(insertTabletPlan2);
    processor.asyncCloseAllWorkingTsFileProcessors();
    processor.syncCloseAllWorkingTsFileProcessors();

    QueryDataSource queryDataSource =
        processor.query(
            Collections.singletonList(new PartialPath(deviceId, measurementId)),
            deviceId,
            context,
            null,
            null);

    Assert.assertEquals(2, queryDataSource.getSeqResources().size());
    Assert.assertEquals(1, queryDataSource.getUnseqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      Assert.assertTrue(resource.isClosed());
    }
  }

  @Test
  public void testSeqAndUnSeqSyncClose()
      throws WriteProcessException, QueryProcessException, IllegalPathException,
          TriggerExecutionException {
    for (int j = 21; j <= 30; j++) {
      TSRecord record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      processor.insert(new InsertRowPlan(record));
      processor.asyncCloseAllWorkingTsFileProcessors();
    }
    processor.syncCloseAllWorkingTsFileProcessors();

    for (int j = 10; j >= 1; j--) {
      TSRecord record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      processor.insert(new InsertRowPlan(record));
      processor.asyncCloseAllWorkingTsFileProcessors();
    }

    processor.syncCloseAllWorkingTsFileProcessors();

    QueryDataSource queryDataSource =
        processor.query(
            Collections.singletonList(new PartialPath(deviceId, measurementId)),
            deviceId,
            context,
            null,
            null);
    Assert.assertEquals(10, queryDataSource.getSeqResources().size());
    Assert.assertEquals(10, queryDataSource.getUnseqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      Assert.assertTrue(resource.isClosed());
    }
    for (TsFileResource resource : queryDataSource.getUnseqResources()) {
      Assert.assertTrue(resource.isClosed());
    }
  }

  @Test
  public void testEnableDiscardOutOfOrderDataForInsertRowPlan()
      throws WriteProcessException, QueryProcessException, IllegalPathException, IOException,
          TriggerExecutionException {
    boolean defaultValue = config.isEnableDiscardOutOfOrderData();
    config.setEnableDiscardOutOfOrderData(true);

    for (int j = 21; j <= 30; j++) {
      TSRecord record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      insertToStorageGroupProcessor(record);
      processor.asyncCloseAllWorkingTsFileProcessors();
    }
    processor.syncCloseAllWorkingTsFileProcessors();

    for (int j = 10; j >= 1; j--) {
      TSRecord record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      insertToStorageGroupProcessor(record);
      processor.asyncCloseAllWorkingTsFileProcessors();
    }

    processor.syncCloseAllWorkingTsFileProcessors();

    for (TsFileProcessor tsfileProcessor : processor.getWorkUnsequenceTsFileProcessors()) {
      tsfileProcessor.syncFlush();
    }

    QueryDataSource queryDataSource =
        processor.query(
            Collections.singletonList(new PartialPath(deviceId, measurementId)),
            deviceId,
            context,
            null,
            null);
    Assert.assertEquals(10, queryDataSource.getSeqResources().size());
    Assert.assertEquals(0, queryDataSource.getUnseqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      Assert.assertTrue(resource.isClosed());
    }
    for (TsFileResource resource : queryDataSource.getUnseqResources()) {
      Assert.assertTrue(resource.isClosed());
    }

    config.setEnableDiscardOutOfOrderData(defaultValue);
  }

  @Test
  public void testEnableDiscardOutOfOrderDataForInsertTablet1()
      throws QueryProcessException, IllegalPathException, IOException, TriggerExecutionException {
    boolean defaultEnableDiscard = config.isEnableDiscardOutOfOrderData();
    long defaultTimePartition = config.getPartitionInterval();
    boolean defaultEnablePartition = config.isEnablePartition();
    config.setEnableDiscardOutOfOrderData(true);
    config.setEnablePartition(true);
    config.setPartitionInterval(100);

    String[] measurements = new String[2];
    measurements[0] = "s0";
    measurements[1] = "s1";
    List<Integer> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.INT32.ordinal());
    dataTypes.add(TSDataType.INT64.ordinal());

    IMeasurementMNode[] measurementMNodes = new IMeasurementMNode[2];
    measurementMNodes[0] =
        MeasurementMNode.getMeasurementMNode(
            null, "s0", new MeasurementSchema("s0", TSDataType.INT32, TSEncoding.PLAIN), null);
    measurementMNodes[1] =
        MeasurementMNode.getMeasurementMNode(
            null, "s1", new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN), null);

    InsertTabletPlan insertTabletPlan1 =
        new InsertTabletPlan(new PartialPath("root.vehicle.d0"), measurements, dataTypes);

    long[] times = new long[100];
    Object[] columns = new Object[2];
    columns[0] = new int[100];
    columns[1] = new long[100];

    for (int r = 0; r < 100; r++) {
      times[r] = r;
      ((int[]) columns[0])[r] = 1;
      ((long[]) columns[1])[r] = 1;
    }
    insertTabletPlan1.setTimes(times);
    insertTabletPlan1.setColumns(columns);
    insertTabletPlan1.setRowCount(times.length);
    insertTabletPlan1.setMeasurementMNodes(measurementMNodes);

    processor.insertTablet(insertTabletPlan1);
    processor.asyncCloseAllWorkingTsFileProcessors();

    InsertTabletPlan insertTabletPlan2 =
        new InsertTabletPlan(new PartialPath("root.vehicle.d0"), measurements, dataTypes);

    for (int r = 149; r >= 50; r--) {
      times[r - 50] = r;
      ((int[]) columns[0])[r - 50] = 1;
      ((long[]) columns[1])[r - 50] = 1;
    }
    insertTabletPlan2.setTimes(times);
    insertTabletPlan2.setColumns(columns);
    insertTabletPlan2.setRowCount(times.length);
    insertTabletPlan2.setMeasurementMNodes(measurementMNodes);

    processor.insertTablet(insertTabletPlan2);
    processor.asyncCloseAllWorkingTsFileProcessors();
    processor.syncCloseAllWorkingTsFileProcessors();

    for (TsFileProcessor tsfileProcessor : processor.getWorkUnsequenceTsFileProcessors()) {
      tsfileProcessor.syncFlush();
    }

    QueryDataSource queryDataSource =
        processor.query(
            Collections.singletonList(new PartialPath(deviceId, measurementId)),
            deviceId,
            context,
            null,
            null);

    Assert.assertEquals(2, queryDataSource.getSeqResources().size());
    Assert.assertEquals(0, queryDataSource.getUnseqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      Assert.assertTrue(resource.isClosed());
    }

    config.setEnableDiscardOutOfOrderData(defaultEnableDiscard);
    config.setPartitionInterval(defaultTimePartition);
    config.setEnablePartition(defaultEnablePartition);
  }

  @Test
  public void testEnableDiscardOutOfOrderDataForInsertTablet2()
      throws QueryProcessException, IllegalPathException, IOException, TriggerExecutionException {
    boolean defaultEnableDiscard = config.isEnableDiscardOutOfOrderData();
    long defaultTimePartition = config.getPartitionInterval();
    boolean defaultEnablePartition = config.isEnablePartition();
    config.setEnableDiscardOutOfOrderData(true);
    config.setEnablePartition(true);
    config.setPartitionInterval(1200);

    String[] measurements = new String[2];
    measurements[0] = "s0";
    measurements[1] = "s1";
    List<Integer> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.INT32.ordinal());
    dataTypes.add(TSDataType.INT64.ordinal());

    IMeasurementMNode[] measurementMNodes = new IMeasurementMNode[2];
    measurementMNodes[0] =
        MeasurementMNode.getMeasurementMNode(
            null, "s0", new MeasurementSchema("s0", TSDataType.INT32, TSEncoding.PLAIN), null);
    measurementMNodes[1] =
        MeasurementMNode.getMeasurementMNode(
            null, "s1", new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN), null);

    InsertTabletPlan insertTabletPlan1 =
        new InsertTabletPlan(new PartialPath("root.vehicle.d0"), measurements, dataTypes);

    long[] times = new long[1200];
    Object[] columns = new Object[2];
    columns[0] = new int[1200];
    columns[1] = new long[1200];

    for (int r = 0; r < 1200; r++) {
      times[r] = r;
      ((int[]) columns[0])[r] = 1;
      ((long[]) columns[1])[r] = 1;
    }
    insertTabletPlan1.setTimes(times);
    insertTabletPlan1.setColumns(columns);
    insertTabletPlan1.setRowCount(times.length);
    insertTabletPlan1.setMeasurementMNodes(measurementMNodes);

    processor.insertTablet(insertTabletPlan1);
    processor.asyncCloseAllWorkingTsFileProcessors();

    InsertTabletPlan insertTabletPlan2 =
        new InsertTabletPlan(new PartialPath("root.vehicle.d0"), measurements, dataTypes);

    for (int r = 1249; r >= 50; r--) {
      times[r - 50] = r;
      ((int[]) columns[0])[r - 50] = 1;
      ((long[]) columns[1])[r - 50] = 1;
    }
    insertTabletPlan2.setTimes(times);
    insertTabletPlan2.setColumns(columns);
    insertTabletPlan2.setRowCount(times.length);
    insertTabletPlan2.setMeasurementMNodes(measurementMNodes);

    processor.insertTablet(insertTabletPlan2);
    processor.asyncCloseAllWorkingTsFileProcessors();
    processor.syncCloseAllWorkingTsFileProcessors();

    for (TsFileProcessor tsfileProcessor : processor.getWorkUnsequenceTsFileProcessors()) {
      tsfileProcessor.syncFlush();
    }

    QueryDataSource queryDataSource =
        processor.query(
            Collections.singletonList(new PartialPath(deviceId, measurementId)),
            deviceId,
            context,
            null,
            null);

    Assert.assertEquals(2, queryDataSource.getSeqResources().size());
    Assert.assertEquals(0, queryDataSource.getUnseqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      Assert.assertTrue(resource.isClosed());
    }

    config.setEnableDiscardOutOfOrderData(defaultEnableDiscard);
    config.setPartitionInterval(defaultTimePartition);
    config.setEnablePartition(defaultEnablePartition);
  }

  @Test
  public void testEnableDiscardOutOfOrderDataForInsertTablet3()
      throws QueryProcessException, IllegalPathException, IOException, TriggerExecutionException {
    boolean defaultEnableDiscard = config.isEnableDiscardOutOfOrderData();
    long defaultTimePartition = config.getPartitionInterval();
    boolean defaultEnablePartition = config.isEnablePartition();
    config.setEnableDiscardOutOfOrderData(true);
    config.setEnablePartition(true);
    config.setPartitionInterval(1000);

    String[] measurements = new String[2];
    measurements[0] = "s0";
    measurements[1] = "s1";
    List<Integer> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.INT32.ordinal());
    dataTypes.add(TSDataType.INT64.ordinal());

    IMeasurementMNode[] measurementMNodes = new IMeasurementMNode[2];
    measurementMNodes[0] =
        MeasurementMNode.getMeasurementMNode(
            null, "s0", new MeasurementSchema("s0", TSDataType.INT32, TSEncoding.PLAIN), null);
    measurementMNodes[1] =
        MeasurementMNode.getMeasurementMNode(
            null, "s1", new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN), null);

    InsertTabletPlan insertTabletPlan1 =
        new InsertTabletPlan(new PartialPath("root.vehicle.d0"), measurements, dataTypes);

    long[] times = new long[1200];
    Object[] columns = new Object[2];
    columns[0] = new int[1200];
    columns[1] = new long[1200];

    for (int r = 0; r < 1200; r++) {
      times[r] = r;
      ((int[]) columns[0])[r] = 1;
      ((long[]) columns[1])[r] = 1;
    }
    insertTabletPlan1.setTimes(times);
    insertTabletPlan1.setColumns(columns);
    insertTabletPlan1.setRowCount(times.length);
    insertTabletPlan1.setMeasurementMNodes(measurementMNodes);

    processor.insertTablet(insertTabletPlan1);
    processor.asyncCloseAllWorkingTsFileProcessors();

    InsertTabletPlan insertTabletPlan2 =
        new InsertTabletPlan(new PartialPath("root.vehicle.d0"), measurements, dataTypes);

    for (int r = 1249; r >= 50; r--) {
      times[r - 50] = r;
      ((int[]) columns[0])[r - 50] = 1;
      ((long[]) columns[1])[r - 50] = 1;
    }
    insertTabletPlan2.setTimes(times);
    insertTabletPlan2.setColumns(columns);
    insertTabletPlan2.setRowCount(times.length);
    insertTabletPlan2.setMeasurementMNodes(measurementMNodes);

    processor.insertTablet(insertTabletPlan2);
    processor.asyncCloseAllWorkingTsFileProcessors();
    processor.syncCloseAllWorkingTsFileProcessors();

    for (TsFileProcessor tsfileProcessor : processor.getWorkUnsequenceTsFileProcessors()) {
      tsfileProcessor.syncFlush();
    }

    QueryDataSource queryDataSource =
        processor.query(
            Collections.singletonList(new PartialPath(deviceId, measurementId)),
            deviceId,
            context,
            null,
            null);

    Assert.assertEquals(2, queryDataSource.getSeqResources().size());
    Assert.assertEquals(0, queryDataSource.getUnseqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      Assert.assertTrue(resource.isClosed());
    }

    config.setEnableDiscardOutOfOrderData(defaultEnableDiscard);
    config.setPartitionInterval(defaultTimePartition);
    config.setEnablePartition(defaultEnablePartition);
  }

  @Test
  public void testMerge()
      throws WriteProcessException, QueryProcessException, IllegalPathException,
          TriggerExecutionException {
    int originCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getMaxInnerCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setMaxInnerCompactionCandidateFileNum(9);
    boolean originEnableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    boolean originEnableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(true);
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(true);
    for (int j = 21; j <= 30; j++) {
      TSRecord record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      processor.insert(new InsertRowPlan(record));
      processor.asyncCloseAllWorkingTsFileProcessors();
    }
    processor.syncCloseAllWorkingTsFileProcessors();

    for (int j = 10; j >= 1; j--) {
      TSRecord record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      processor.insert(new InsertRowPlan(record));
      processor.asyncCloseAllWorkingTsFileProcessors();
    }

    processor.syncCloseAllWorkingTsFileProcessors();
    processor.compact();
    long totalWaitingTime = 0;
    while (CompactionTaskManager.getInstance().getExecutingTaskCount() > 0) {
      // wait
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      totalWaitingTime += 100;
      if (totalWaitingTime % 1000 == 0) {
        logger.warn("has waited for {} seconds", totalWaitingTime / 1000);
      }
      if (totalWaitingTime > 120_000) {
        Assert.fail();
        break;
      }
    }

    QueryDataSource queryDataSource =
        processor.query(
            Collections.singletonList(new PartialPath(deviceId, measurementId)),
            deviceId,
            context,
            null,
            null);
    Assert.assertEquals(2, queryDataSource.getSeqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      Assert.assertTrue(resource.isClosed());
    }
    for (TsFileResource resource : queryDataSource.getUnseqResources()) {
      Assert.assertTrue(resource.isClosed());
    }
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setMaxInnerCompactionCandidateFileNum(originCandidateFileNum);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableSeqSpaceCompaction(originEnableSeqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableUnseqSpaceCompaction(originEnableUnseqSpaceCompaction);
  }

  @Test
  public void testTimedFlushSeqMemTable()
      throws IllegalPathException, InterruptedException, WriteProcessException,
          TriggerExecutionException, ShutdownException {
    // create one sequence memtable
    TSRecord record = new TSRecord(10000, deviceId);
    record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(1000)));
    processor.insert(new InsertRowPlan(record));
    Assert.assertEquals(1, MemTableManager.getInstance().getCurrentMemtableNumber());

    // change config & reboot timed service
    boolean prevEnableTimedFlushSeqMemtable = config.isEnableTimedFlushSeqMemtable();
    long preFLushInterval = config.getSeqMemtableFlushInterval();
    config.setEnableTimedFlushSeqMemtable(true);
    config.setSeqMemtableFlushInterval(5);
    StorageEngine.getInstance().rebootTimedService();

    Thread.sleep(500);

    Assert.assertEquals(1, processor.getWorkSequenceTsFileProcessors().size());
    TsFileProcessor tsFileProcessor = processor.getWorkSequenceTsFileProcessors().iterator().next();
    FlushManager flushManager = FlushManager.getInstance();

    // flush the sequence memtable
    processor.timedFlushSeqMemTable();

    // wait until memtable flush task is done
    int waitCnt = 0;
    while (tsFileProcessor.getFlushingMemTableSize() != 0
        || tsFileProcessor.isManagedByFlushManager()
        || flushManager.getNumberOfPendingTasks() != 0
        || flushManager.getNumberOfPendingSubTasks() != 0
        || flushManager.getNumberOfWorkingTasks() != 0
        || flushManager.getNumberOfWorkingSubTasks() != 0) {
      Thread.sleep(500);
      ++waitCnt;
      if (waitCnt % 10 == 0) {
        logger.info("already wait {} s", waitCnt / 2);
      }
    }

    Assert.assertEquals(0, MemTableManager.getInstance().getCurrentMemtableNumber());

    config.setEnableTimedFlushSeqMemtable(prevEnableTimedFlushSeqMemtable);
    config.setSeqMemtableFlushInterval(preFLushInterval);
  }

  @Test
  public void testTimedFlushUnseqMemTable()
      throws IllegalPathException, InterruptedException, WriteProcessException,
          TriggerExecutionException, ShutdownException {
    // create one sequence memtable & close
    TSRecord record = new TSRecord(10000, deviceId);
    record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(1000)));
    processor.insert(new InsertRowPlan(record));
    Assert.assertEquals(1, MemTableManager.getInstance().getCurrentMemtableNumber());
    processor.syncCloseAllWorkingTsFileProcessors();
    Assert.assertEquals(0, MemTableManager.getInstance().getCurrentMemtableNumber());

    // create one unsequence memtable
    record = new TSRecord(1, deviceId);
    record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(1000)));
    processor.insert(new InsertRowPlan(record));
    Assert.assertEquals(1, MemTableManager.getInstance().getCurrentMemtableNumber());

    // change config & reboot timed service
    boolean prevEnableTimedFlushUnseqMemtable = config.isEnableTimedFlushUnseqMemtable();
    long preFLushInterval = config.getUnseqMemtableFlushInterval();
    config.setEnableTimedFlushUnseqMemtable(true);
    config.setUnseqMemtableFlushInterval(5);
    StorageEngine.getInstance().rebootTimedService();

    Thread.sleep(500);

    Assert.assertEquals(1, processor.getWorkUnsequenceTsFileProcessors().size());
    TsFileProcessor tsFileProcessor =
        processor.getWorkUnsequenceTsFileProcessors().iterator().next();
    FlushManager flushManager = FlushManager.getInstance();

    // flush the unsequence memtable
    processor.timedFlushUnseqMemTable();

    // wait until memtable flush task is done
    int waitCnt = 0;
    while (tsFileProcessor.getFlushingMemTableSize() != 0
        || tsFileProcessor.isManagedByFlushManager()
        || flushManager.getNumberOfPendingTasks() != 0
        || flushManager.getNumberOfPendingSubTasks() != 0
        || flushManager.getNumberOfWorkingTasks() != 0
        || flushManager.getNumberOfWorkingSubTasks() != 0) {
      Thread.sleep(500);
      ++waitCnt;
      if (waitCnt % 10 == 0) {
        logger.info("already wait {} s", waitCnt / 2);
      }
    }

    Assert.assertEquals(0, MemTableManager.getInstance().getCurrentMemtableNumber());

    config.setEnableTimedFlushUnseqMemtable(prevEnableTimedFlushUnseqMemtable);
    config.setUnseqMemtableFlushInterval(preFLushInterval);
  }

  @Test
  public void testTimedCloseTsFile()
      throws IllegalPathException, InterruptedException, WriteProcessException,
          TriggerExecutionException, ShutdownException {
    // create one sequence memtable
    TSRecord record = new TSRecord(10000, deviceId);
    record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(1000)));
    processor.insert(new InsertRowPlan(record));
    Assert.assertEquals(1, MemTableManager.getInstance().getCurrentMemtableNumber());

    // change config & reboot timed service
    long prevSeqTsFileSize = config.getSeqTsFileSize();
    config.setSeqTsFileSize(1);
    boolean prevEnableTimedFlushSeqMemtable = config.isEnableTimedFlushSeqMemtable();
    long preFLushInterval = config.getSeqMemtableFlushInterval();
    config.setEnableTimedFlushSeqMemtable(true);
    config.setSeqMemtableFlushInterval(5);
    boolean prevEnableTimedCloseTsFile = config.isEnableTimedCloseTsFile();
    long prevCloseTsFileInterval = config.getCloseTsFileIntervalAfterFlushing();
    config.setEnableTimedCloseTsFile(true);
    config.setCloseTsFileIntervalAfterFlushing(5);
    StorageEngine.getInstance().rebootTimedService();

    Thread.sleep(500);

    // flush the sequence memtable
    processor.timedFlushSeqMemTable();

    // wait until memtable flush task is done
    Assert.assertEquals(1, processor.getWorkSequenceTsFileProcessors().size());
    TsFileProcessor tsFileProcessor = processor.getWorkSequenceTsFileProcessors().iterator().next();
    FlushManager flushManager = FlushManager.getInstance();
    int waitCnt = 0;
    while (tsFileProcessor.getFlushingMemTableSize() != 0
        || tsFileProcessor.isManagedByFlushManager()
        || flushManager.getNumberOfPendingTasks() != 0
        || flushManager.getNumberOfPendingSubTasks() != 0
        || flushManager.getNumberOfWorkingTasks() != 0
        || flushManager.getNumberOfWorkingSubTasks() != 0) {
      Thread.sleep(500);
      ++waitCnt;
      if (waitCnt % 10 == 0) {
        logger.info("already wait {} s", waitCnt / 2);
      }
    }

    Assert.assertEquals(0, MemTableManager.getInstance().getCurrentMemtableNumber());
    Assert.assertFalse(tsFileProcessor.alreadyMarkedClosing());

    // close the tsfile
    processor.timedCloseTsFileProcessor();

    Thread.sleep(500);

    Assert.assertTrue(tsFileProcessor.alreadyMarkedClosing());

    config.setSeqTsFileSize(prevSeqTsFileSize);
    config.setEnableTimedFlushSeqMemtable(prevEnableTimedFlushSeqMemtable);
    config.setSeqMemtableFlushInterval(preFLushInterval);
    config.setEnableTimedCloseTsFile(prevEnableTimedCloseTsFile);
    config.setCloseTsFileIntervalAfterFlushing(prevCloseTsFileInterval);
  }

  /**
   * Totally 5 tsfiles<br>
   * file 0, file 2 and file 4 has d0 ~ d1, time range is 0 ~ 99, 200 ~ 299, 400 ~ 499<br>
   * file 1, file 3 has d0 ~ d2, time range is 100 ~ 199, 300 ~ 399<br>
   * delete d2 in time range 50 ~ 150 and 150 ~ 450. Therefore, only file 1 and file 3 has mods.
   */
  @Test
  public void testDeleteDataNotInFile()
      throws IllegalPathException, WriteProcessException, TriggerExecutionException,
          InterruptedException, IOException {
    for (int i = 0; i < 5; i++) {
      if (i % 2 == 0) {
        for (int d = 0; d < 2; d++) {
          for (int count = i * 100; count < i * 100 + 100; count++) {
            TSRecord record = new TSRecord(count, "root.vehicle.d" + d);
            record.addTuple(
                DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(count)));
            processor.insert(new InsertRowPlan(record));
          }
        }
      } else {
        for (int d = 0; d < 3; d++) {
          for (int count = i * 100; count < i * 100 + 100; count++) {
            TSRecord record = new TSRecord(count, "root.vehicle.d" + d);
            record.addTuple(
                DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(count)));
            processor.insert(new InsertRowPlan(record));
          }
        }
      }
      processor.syncCloseAllWorkingTsFileProcessors();
    }

    // delete root.vehicle.d2.s0 data in the second file
    processor.delete(new PartialPath("root.vehicle.d2.s0"), 50, 150, 0, null);

    // delete root.vehicle.d2.s0 data in the third file
    processor.delete(new PartialPath("root.vehicle.d2.s0"), 150, 450, 0, null);

    for (int i = 0; i < processor.getTsFileResourceManager().getTsFileList(true).size(); i++) {
      TsFileResource resource = processor.getTsFileResourceManager().getTsFileList(true).get(i);
      if (i == 1) {
        Assert.assertTrue(resource.getModFile().exists());
        Assert.assertEquals(2, resource.getModFile().getModifications().size());
      } else if (i == 3) {
        Assert.assertTrue(resource.getModFile().exists());
        Assert.assertEquals(1, resource.getModFile().getModifications().size());
      } else {
        Assert.assertFalse(resource.getModFile().exists());
      }
    }
  }

  @Test
  public void testDeleteDataNotInFlushingMemtable()
      throws IllegalPathException, WriteProcessException, TriggerExecutionException, IOException {
    for (int j = 0; j < 100; j++) {
      TSRecord record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      processor.insert(new InsertRowPlan(record));
    }
    TsFileResource tsFileResource = processor.getTsFileResourceManager().getTsFileList(true).get(0);
    TsFileProcessor tsFileProcessor = tsFileResource.getProcessor();
    tsFileProcessor.getFlushingMemTable().addLast(tsFileProcessor.getWorkMemTable());

    // delete data which is not in memtable
    processor.delete(new PartialPath("root.vehicle.d2.s0"), 50, 70, 0, null);

    // delete data which is not in memtable
    processor.delete(new PartialPath("root.vehicle.d200.s0"), 50, 70, 0, null);

    processor.syncCloseAllWorkingTsFileProcessors();
    Assert.assertFalse(tsFileResource.getModFile().exists());
  }

  @Test
  public void testDeleteDataInSeqFlushingMemtable()
      throws IllegalPathException, WriteProcessException, TriggerExecutionException, IOException {
    for (int j = 100; j < 200; j++) {
      TSRecord record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      processor.insert(new InsertRowPlan(record));
    }
    TsFileResource tsFileResource = processor.getTsFileResourceManager().getTsFileList(true).get(0);
    TsFileProcessor tsFileProcessor = tsFileResource.getProcessor();
    tsFileProcessor.getFlushingMemTable().addLast(tsFileProcessor.getWorkMemTable());

    // delete data which is not in flushing memtable
    processor.delete(new PartialPath("root.vehicle.d0.s0"), 50, 99, 0, null);
    processor.delete(new PartialPath("root.vehicle.d200.s0"), 50, 70, 0, null);

    // delete data which is in flushing memtable
    processor.delete(new PartialPath("root.vehicle.d0.s0"), 50, 100, 0, null);
    processor.delete(new PartialPath("root.vehicle.d0.s0"), 50, 150, 0, null);
    processor.delete(new PartialPath("root.vehicle.d0.s0"), 100, 300, 0, null);

    processor.syncCloseAllWorkingTsFileProcessors();
    Assert.assertTrue(tsFileResource.getModFile().exists());
    Assert.assertEquals(3, tsFileResource.getModFile().getModifications().size());
  }

  @Test
  public void testDeleteDataInUnSeqFlushingMemtable()
      throws IllegalPathException, WriteProcessException, TriggerExecutionException, IOException {
    for (int j = 100; j < 200; j++) {
      TSRecord record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      processor.insert(new InsertRowPlan(record));
    }
    TsFileResource tsFileResource = processor.getTsFileResourceManager().getTsFileList(true).get(0);

    // delete data which is not in work memtable
    processor.delete(new PartialPath("root.vehicle.d0.s0"), 50, 99, 0, null);
    processor.delete(new PartialPath("root.vehicle.d200.s0"), 50, 70, 0, null);

    // delete data which is in work memtable
    processor.delete(new PartialPath("root.vehicle.d0.s0"), 50, 100, 0, null);
    processor.delete(new PartialPath("root.vehicle.d0.s0"), 50, 150, 0, null);
    processor.delete(new PartialPath("root.vehicle.d0.s0"), 100, 300, 0, null);

    processor.syncCloseAllWorkingTsFileProcessors();
    Assert.assertFalse(tsFileResource.getModFile().exists());

    // insert unseq data points
    for (int j = 50; j < 100; j++) {
      TSRecord record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      processor.insert(new InsertRowPlan(record));
    }
    // delete data which is not in work memtable
    processor.delete(new PartialPath("root.vehicle.d0.s0"), 200, 299, 0, null);
    processor.delete(new PartialPath("root.vehicle.d200.s0"), 50, 70, 0, null);

    // delete data which is in work memtable
    processor.delete(new PartialPath("root.vehicle.d0.s0"), 80, 85, 0, null);

    Assert.assertFalse(tsFileResource.getModFile().exists());

    tsFileResource = processor.getTsFileResourceManager().getTsFileList(false).get(0);
    TsFileProcessor tsFileProcessor = tsFileResource.getProcessor();
    tsFileProcessor.getFlushingMemTable().addLast(tsFileProcessor.getWorkMemTable());

    // delete data which is not in flushing memtable
    processor.delete(new PartialPath("root.vehicle.d0.s0"), 0, 49, 0, null);
    processor.delete(new PartialPath("root.vehicle.d0.s0"), 100, 200, 0, null);
    processor.delete(new PartialPath("root.vehicle.d200.s0"), 50, 70, 0, null);

    // delete data which is in flushing memtable
    processor.delete(new PartialPath("root.vehicle.d0.s0"), 25, 50, 0, null);
    processor.delete(new PartialPath("root.vehicle.d0.s0"), 50, 80, 0, null);
    processor.delete(new PartialPath("root.vehicle.d0.s0"), 99, 150, 0, null);

    processor.syncCloseAllWorkingTsFileProcessors();
    Assert.assertTrue(tsFileResource.getModFile().exists());
    Assert.assertEquals(3, tsFileResource.getModFile().getModifications().size());
  }

  class DummySGP extends VirtualStorageGroupProcessor {

    DummySGP(String systemInfoDir, String storageGroupName) throws StorageGroupProcessorException {
      super(
          systemInfoDir,
          storageGroupName,
          new TsFileFlushPolicy.DirectFlushPolicy(),
          storageGroupName);
    }
  }
}
