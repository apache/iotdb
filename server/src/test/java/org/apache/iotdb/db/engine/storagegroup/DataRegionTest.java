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

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.exception.ShutdownException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.compaction.execute.performer.ICompactionPerformer;
import org.apache.iotdb.db.engine.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.execute.utils.log.CompactionLogger;
import org.apache.iotdb.db.engine.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.utils.CompactionConfigRestorer;
import org.apache.iotdb.db.engine.flush.FlushManager;
import org.apache.iotdb.db.engine.flush.TsFileFlushPolicy;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertTabletNode;
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
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class DataRegionTest {
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final Logger logger = LoggerFactory.getLogger(DataRegionTest.class);

  private String storageGroup = "root.vehicle.d0";
  private String systemDir = TestConstant.OUTPUT_DATA_DIR.concat("info");
  private String deviceId = "root.vehicle.d0";
  private String measurementId = "s0";
  private DataRegion dataRegion;
  private QueryContext context = EnvironmentUtils.TEST_QUERY_CONTEXT;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    dataRegion = new DummyDataRegion(systemDir, storageGroup);
    StorageEngine.getInstance().setDataRegion(new DataRegionId(0), dataRegion);
    CompactionTaskManager.getInstance().start();
  }

  @After
  public void tearDown() throws Exception {
    if (dataRegion != null) {
      dataRegion.syncDeleteDataFiles();
      StorageEngine.getInstance().deleteDataRegion(new DataRegionId(0));
    }
    EnvironmentUtils.cleanDir(TestConstant.OUTPUT_DATA_DIR);
    CompactionTaskManager.getInstance().stop();
    EnvironmentUtils.cleanEnv();
  }

  public static InsertRowNode buildInsertRowNodeByTSRecord(TSRecord record)
      throws IllegalPathException {
    String[] measurements = new String[record.dataPointList.size()];
    MeasurementSchema[] measurementSchemas = new MeasurementSchema[record.dataPointList.size()];
    TSDataType[] dataTypes = new TSDataType[record.dataPointList.size()];
    Object[] values = new Object[record.dataPointList.size()];
    for (int i = 0; i < record.dataPointList.size(); i++) {
      measurements[i] = record.dataPointList.get(i).getMeasurementId();
      measurementSchemas[i] =
          new MeasurementSchema(
              measurements[i],
              record.dataPointList.get(i).getType(),
              TSEncoding.PLAIN,
              CompressionType.UNCOMPRESSED);
      dataTypes[i] = record.dataPointList.get(i).getType();
      values[i] = record.dataPointList.get(i).getValue();
    }
    QueryId queryId = new QueryId("test_write");
    InsertRowNode insertRowNode =
        new InsertRowNode(
            queryId.genPlanNodeId(),
            new PartialPath(record.deviceId),
            false,
            measurements,
            dataTypes,
            record.time,
            values,
            false);
    insertRowNode.setMeasurementSchemas(measurementSchemas);
    return insertRowNode;
  }

  @Test
  public void testUnseqUnsealedDelete()
      throws WriteProcessException, IOException, MetadataException {
    TSRecord record = new TSRecord(10000, deviceId);
    record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(1000)));
    dataRegion.insert(buildInsertRowNodeByTSRecord(record));
    dataRegion.syncCloseAllWorkingTsFileProcessors();

    for (int j = 1; j <= 10; j++) {
      record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      dataRegion.insert(buildInsertRowNodeByTSRecord(record));
    }

    for (TsFileProcessor tsfileProcessor : dataRegion.getWorkUnsequenceTsFileProcessors()) {
      tsfileProcessor.syncFlush();
    }

    for (int j = 11; j <= 20; j++) {
      record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      dataRegion.insert(buildInsertRowNodeByTSRecord(record));
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

    dataRegion.deleteByDevice(new PartialPath(deviceId, measurementId), 0, 15L, -1, null);

    List<TsFileResource> tsfileResourcesForQuery = new ArrayList<>();
    for (TsFileProcessor tsfileProcessor : dataRegion.getWorkUnsequenceTsFileProcessors()) {
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
      throws WriteProcessException, QueryProcessException, IllegalPathException {
    for (int j = 1; j <= 10; j++) {
      TSRecord record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      dataRegion.insert(buildInsertRowNodeByTSRecord(record));
      dataRegion.asyncCloseAllWorkingTsFileProcessors();
    }

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    dataRegion.syncCloseAllWorkingTsFileProcessors();
    QueryDataSource queryDataSource =
        dataRegion.query(
            Collections.singletonList(new PartialPath(deviceId, measurementId)),
            deviceId,
            context,
            null);
    Assert.assertEquals(10, queryDataSource.getSeqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      Assert.assertTrue(resource.isClosed());
    }
  }

  @Test
  public void testInsertDataAndRemovePartitionAndInsert()
      throws WriteProcessException, QueryProcessException, IllegalPathException {
    for (int j = 0; j < 10; j++) {
      TSRecord record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      dataRegion.insert(buildInsertRowNodeByTSRecord(record));
      dataRegion.asyncCloseAllWorkingTsFileProcessors();
    }
    dataRegion.syncCloseAllWorkingTsFileProcessors();

    dataRegion.removePartitions((storageGroupName, timePartitionId) -> true);

    for (int j = 0; j < 10; j++) {
      TSRecord record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      dataRegion.insert(buildInsertRowNodeByTSRecord(record));
      dataRegion.asyncCloseAllWorkingTsFileProcessors();
    }
    dataRegion.syncCloseAllWorkingTsFileProcessors();

    QueryDataSource queryDataSource =
        dataRegion.query(
            Collections.singletonList(new PartialPath(deviceId, measurementId)),
            deviceId,
            context,
            null);
    Assert.assertEquals(0, queryDataSource.getUnseqResources().size());
  }

  @Test
  public void testIoTDBTabletWriteAndSyncClose()
      throws QueryProcessException, IllegalPathException, WriteProcessException {
    String[] measurements = new String[2];
    measurements[0] = "s0";
    measurements[1] = "s1";
    TSDataType[] dataTypes = new TSDataType[2];
    dataTypes[0] = TSDataType.INT32;
    dataTypes[1] = TSDataType.INT64;

    MeasurementSchema[] measurementSchemas = new MeasurementSchema[2];
    measurementSchemas[0] = new MeasurementSchema("s0", TSDataType.INT32, TSEncoding.PLAIN);
    measurementSchemas[1] = new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN);

    long[] times = new long[100];
    Object[] columns = new Object[2];
    columns[0] = new int[100];
    columns[1] = new long[100];

    for (int r = 0; r < 100; r++) {
      times[r] = r;
      ((int[]) columns[0])[r] = 1;
      ((long[]) columns[1])[r] = 1;
    }

    InsertTabletNode insertTabletNode1 =
        new InsertTabletNode(
            new QueryId("test_write").genPlanNodeId(),
            new PartialPath("root.vehicle.d0"),
            false,
            measurements,
            dataTypes,
            times,
            null,
            columns,
            times.length);
    insertTabletNode1.setMeasurementSchemas(measurementSchemas);

    dataRegion.insertTablet(insertTabletNode1);
    dataRegion.asyncCloseAllWorkingTsFileProcessors();

    for (int r = 50; r < 149; r++) {
      times[r - 50] = r;
      ((int[]) columns[0])[r - 50] = 1;
      ((long[]) columns[1])[r - 50] = 1;
    }

    InsertTabletNode insertTabletNode2 =
        new InsertTabletNode(
            new QueryId("test_write").genPlanNodeId(),
            new PartialPath("root.vehicle.d0"),
            false,
            measurements,
            dataTypes,
            times,
            null,
            columns,
            times.length);
    insertTabletNode2.setMeasurementSchemas(measurementSchemas);

    dataRegion.insertTablet(insertTabletNode2);
    dataRegion.asyncCloseAllWorkingTsFileProcessors();
    dataRegion.syncCloseAllWorkingTsFileProcessors();

    QueryDataSource queryDataSource =
        dataRegion.query(
            Collections.singletonList(new PartialPath(deviceId, measurementId)),
            deviceId,
            context,
            null);

    Assert.assertEquals(2, queryDataSource.getSeqResources().size());
    Assert.assertEquals(1, queryDataSource.getUnseqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      Assert.assertTrue(resource.isClosed());
    }
  }

  @Test
  public void testSeqAndUnSeqSyncClose()
      throws WriteProcessException, QueryProcessException, IllegalPathException {
    for (int j = 21; j <= 30; j++) {
      TSRecord record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      dataRegion.insert(buildInsertRowNodeByTSRecord(record));
      dataRegion.asyncCloseAllWorkingTsFileProcessors();
    }
    dataRegion.syncCloseAllWorkingTsFileProcessors();

    for (int j = 10; j >= 1; j--) {
      TSRecord record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      dataRegion.insert(buildInsertRowNodeByTSRecord(record));
      dataRegion.asyncCloseAllWorkingTsFileProcessors();
    }

    dataRegion.syncCloseAllWorkingTsFileProcessors();

    QueryDataSource queryDataSource =
        dataRegion.query(
            Collections.singletonList(new PartialPath(deviceId, measurementId)),
            deviceId,
            context,
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
      throws WriteProcessException, QueryProcessException, IllegalPathException, IOException {
    boolean defaultValue = config.isEnableDiscardOutOfOrderData();
    config.setEnableDiscardOutOfOrderData(true);

    for (int j = 21; j <= 30; j++) {
      TSRecord record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      dataRegion.insert(buildInsertRowNodeByTSRecord(record));
      dataRegion.asyncCloseAllWorkingTsFileProcessors();
    }
    dataRegion.syncCloseAllWorkingTsFileProcessors();

    for (int j = 10; j >= 1; j--) {
      TSRecord record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      dataRegion.insert(buildInsertRowNodeByTSRecord(record));
      dataRegion.asyncCloseAllWorkingTsFileProcessors();
    }

    dataRegion.syncCloseAllWorkingTsFileProcessors();

    for (TsFileProcessor tsfileProcessor : dataRegion.getWorkUnsequenceTsFileProcessors()) {
      tsfileProcessor.syncFlush();
    }

    QueryDataSource queryDataSource =
        dataRegion.query(
            Collections.singletonList(new PartialPath(deviceId, measurementId)),
            deviceId,
            context,
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
      throws QueryProcessException, IllegalPathException, IOException, WriteProcessException {
    boolean defaultEnableDiscard = config.isEnableDiscardOutOfOrderData();
    long defaultTimePartition = config.getTimePartitionInterval();
    config.setEnableDiscardOutOfOrderData(true);
    config.setTimePartitionInterval(100000);

    String[] measurements = new String[2];
    measurements[0] = "s0";
    measurements[1] = "s1";
    TSDataType[] dataTypes = new TSDataType[2];
    dataTypes[0] = TSDataType.INT32;
    dataTypes[1] = TSDataType.INT64;

    MeasurementSchema[] measurementSchemas = new MeasurementSchema[2];
    measurementSchemas[0] = new MeasurementSchema("s0", TSDataType.INT32, TSEncoding.PLAIN);
    measurementSchemas[1] = new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN);

    long[] times = new long[100];
    Object[] columns = new Object[2];
    columns[0] = new int[100];
    columns[1] = new long[100];

    for (int r = 0; r < 100; r++) {
      times[r] = r;
      ((int[]) columns[0])[r] = 1;
      ((long[]) columns[1])[r] = 1;
    }
    InsertTabletNode insertTabletNode1 =
        new InsertTabletNode(
            new QueryId("test_write").genPlanNodeId(),
            new PartialPath("root.vehicle.d0"),
            false,
            measurements,
            dataTypes,
            times,
            null,
            columns,
            times.length);
    insertTabletNode1.setMeasurementSchemas(measurementSchemas);

    dataRegion.insertTablet(insertTabletNode1);
    dataRegion.asyncCloseAllWorkingTsFileProcessors();

    for (int r = 149; r >= 50; r--) {
      times[r - 50] = r;
      ((int[]) columns[0])[r - 50] = 1;
      ((long[]) columns[1])[r - 50] = 1;
    }
    InsertTabletNode insertTabletNode2 =
        new InsertTabletNode(
            new QueryId("test_write").genPlanNodeId(),
            new PartialPath("root.vehicle.d0"),
            false,
            measurements,
            dataTypes,
            times,
            null,
            columns,
            times.length);
    insertTabletNode2.setMeasurementSchemas(measurementSchemas);

    dataRegion.insertTablet(insertTabletNode2);
    dataRegion.asyncCloseAllWorkingTsFileProcessors();
    dataRegion.syncCloseAllWorkingTsFileProcessors();

    for (TsFileProcessor tsfileProcessor : dataRegion.getWorkUnsequenceTsFileProcessors()) {
      tsfileProcessor.syncFlush();
    }

    QueryDataSource queryDataSource =
        dataRegion.query(
            Collections.singletonList(new PartialPath(deviceId, measurementId)),
            deviceId,
            context,
            null);

    Assert.assertEquals(2, queryDataSource.getSeqResources().size());
    Assert.assertEquals(0, queryDataSource.getUnseqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      Assert.assertTrue(resource.isClosed());
    }

    config.setEnableDiscardOutOfOrderData(defaultEnableDiscard);
    config.setTimePartitionInterval(defaultTimePartition);
  }

  @Test
  public void testEnableDiscardOutOfOrderDataForInsertTablet2()
      throws QueryProcessException, IllegalPathException, IOException, WriteProcessException {
    boolean defaultEnableDiscard = config.isEnableDiscardOutOfOrderData();
    long defaultTimePartition = config.getTimePartitionInterval();
    config.setEnableDiscardOutOfOrderData(true);
    config.setTimePartitionInterval(1200000);

    String[] measurements = new String[2];
    measurements[0] = "s0";
    measurements[1] = "s1";
    TSDataType[] dataTypes = new TSDataType[2];
    dataTypes[0] = TSDataType.INT32;
    dataTypes[1] = TSDataType.INT64;

    MeasurementSchema[] measurementSchemas = new MeasurementSchema[2];
    measurementSchemas[0] = new MeasurementSchema("s0", TSDataType.INT32, TSEncoding.PLAIN);
    measurementSchemas[1] = new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN);

    long[] times = new long[1200];
    Object[] columns = new Object[2];
    columns[0] = new int[1200];
    columns[1] = new long[1200];

    for (int r = 0; r < 1200; r++) {
      times[r] = r;
      ((int[]) columns[0])[r] = 1;
      ((long[]) columns[1])[r] = 1;
    }
    InsertTabletNode insertTabletNode1 =
        new InsertTabletNode(
            new QueryId("test_write").genPlanNodeId(),
            new PartialPath("root.vehicle.d0"),
            false,
            measurements,
            dataTypes,
            times,
            null,
            columns,
            times.length);
    insertTabletNode1.setMeasurementSchemas(measurementSchemas);

    dataRegion.insertTablet(insertTabletNode1);
    dataRegion.asyncCloseAllWorkingTsFileProcessors();

    for (int r = 1249; r >= 50; r--) {
      times[r - 50] = r;
      ((int[]) columns[0])[r - 50] = 1;
      ((long[]) columns[1])[r - 50] = 1;
    }
    InsertTabletNode insertTabletNode2 =
        new InsertTabletNode(
            new QueryId("test_write").genPlanNodeId(),
            new PartialPath("root.vehicle.d0"),
            false,
            measurements,
            dataTypes,
            times,
            null,
            columns,
            times.length);
    insertTabletNode2.setMeasurementSchemas(measurementSchemas);

    dataRegion.insertTablet(insertTabletNode2);
    dataRegion.asyncCloseAllWorkingTsFileProcessors();
    dataRegion.syncCloseAllWorkingTsFileProcessors();

    for (TsFileProcessor tsfileProcessor : dataRegion.getWorkUnsequenceTsFileProcessors()) {
      tsfileProcessor.syncFlush();
    }

    QueryDataSource queryDataSource =
        dataRegion.query(
            Collections.singletonList(new PartialPath(deviceId, measurementId)),
            deviceId,
            context,
            null);

    Assert.assertEquals(2, queryDataSource.getSeqResources().size());
    Assert.assertEquals(0, queryDataSource.getUnseqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      Assert.assertTrue(resource.isClosed());
    }

    config.setEnableDiscardOutOfOrderData(defaultEnableDiscard);
    config.setTimePartitionInterval(defaultTimePartition);
  }

  @Test
  public void testEnableDiscardOutOfOrderDataForInsertTablet3()
      throws QueryProcessException, IllegalPathException, IOException, WriteProcessException {
    boolean defaultEnableDiscard = config.isEnableDiscardOutOfOrderData();
    long defaultTimePartition = config.getTimePartitionInterval();
    config.setEnableDiscardOutOfOrderData(true);
    config.setTimePartitionInterval(1000000);

    String[] measurements = new String[2];
    measurements[0] = "s0";
    measurements[1] = "s1";
    TSDataType[] dataTypes = new TSDataType[2];
    dataTypes[0] = TSDataType.INT32;
    dataTypes[1] = TSDataType.INT64;

    MeasurementSchema[] measurementSchemas = new MeasurementSchema[2];
    measurementSchemas[0] = new MeasurementSchema("s0", TSDataType.INT32, TSEncoding.PLAIN);
    measurementSchemas[1] = new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN);

    long[] times = new long[1200];
    Object[] columns = new Object[2];
    columns[0] = new int[1200];
    columns[1] = new long[1200];

    for (int r = 0; r < 1200; r++) {
      times[r] = r;
      ((int[]) columns[0])[r] = 1;
      ((long[]) columns[1])[r] = 1;
    }
    InsertTabletNode insertTabletNode1 =
        new InsertTabletNode(
            new QueryId("test_write").genPlanNodeId(),
            new PartialPath("root.vehicle.d0"),
            false,
            measurements,
            dataTypes,
            times,
            null,
            columns,
            times.length);
    insertTabletNode1.setMeasurementSchemas(measurementSchemas);

    dataRegion.insertTablet(insertTabletNode1);
    dataRegion.asyncCloseAllWorkingTsFileProcessors();

    for (int r = 1249; r >= 50; r--) {
      times[r - 50] = r;
      ((int[]) columns[0])[r - 50] = 1;
      ((long[]) columns[1])[r - 50] = 1;
    }
    InsertTabletNode insertTabletNode2 =
        new InsertTabletNode(
            new QueryId("test_write").genPlanNodeId(),
            new PartialPath("root.vehicle.d0"),
            false,
            measurements,
            dataTypes,
            times,
            null,
            columns,
            times.length);
    insertTabletNode2.setMeasurementSchemas(measurementSchemas);

    dataRegion.insertTablet(insertTabletNode2);
    dataRegion.asyncCloseAllWorkingTsFileProcessors();
    dataRegion.syncCloseAllWorkingTsFileProcessors();

    for (TsFileProcessor tsfileProcessor : dataRegion.getWorkUnsequenceTsFileProcessors()) {
      tsfileProcessor.syncFlush();
    }

    QueryDataSource queryDataSource =
        dataRegion.query(
            Collections.singletonList(new PartialPath(deviceId, measurementId)),
            deviceId,
            context,
            null);

    Assert.assertEquals(2, queryDataSource.getSeqResources().size());
    Assert.assertEquals(0, queryDataSource.getUnseqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      Assert.assertTrue(resource.isClosed());
    }

    config.setEnableDiscardOutOfOrderData(defaultEnableDiscard);
    config.setTimePartitionInterval(defaultTimePartition);
  }

  @Test
  public void testSmallReportProportionInsertRow()
      throws WriteProcessException, QueryProcessException, IllegalPathException, IOException,
          DataRegionException {
    double defaultValue = config.getWriteMemoryVariationReportProportion();
    config.setWriteMemoryVariationReportProportion(0);
    DataRegion dataRegion1 = new DummyDataRegion(systemDir, "root.ln22");

    for (int j = 21; j <= 30; j++) {
      TSRecord record = new TSRecord(j, "root.ln22");
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      dataRegion1.insert(buildInsertRowNodeByTSRecord(record));
      dataRegion1.asyncCloseAllWorkingTsFileProcessors();
    }
    dataRegion1.syncCloseAllWorkingTsFileProcessors();

    for (TsFileProcessor tsfileProcessor : dataRegion1.getWorkUnsequenceTsFileProcessors()) {
      tsfileProcessor.syncFlush();
    }

    QueryDataSource queryDataSource =
        dataRegion1.query(
            Collections.singletonList(new PartialPath("root.ln22", measurementId)),
            "root.ln22",
            context,
            null);
    Assert.assertEquals(10, queryDataSource.getSeqResources().size());
    Assert.assertEquals(0, queryDataSource.getUnseqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      Assert.assertTrue(resource.isClosed());
    }
    for (TsFileResource resource : queryDataSource.getUnseqResources()) {
      Assert.assertTrue(resource.isClosed());
    }

    dataRegion1.syncDeleteDataFiles();
    config.setWriteMemoryVariationReportProportion(defaultValue);
  }

  @Test
  public void testMerge()
      throws WriteProcessException, QueryProcessException, IllegalPathException {
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
      dataRegion.insert(buildInsertRowNodeByTSRecord(record));
      dataRegion.asyncCloseAllWorkingTsFileProcessors();
    }
    dataRegion.syncCloseAllWorkingTsFileProcessors();

    for (int j = 10; j >= 1; j--) {
      TSRecord record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      dataRegion.insert(buildInsertRowNodeByTSRecord(record));
      dataRegion.asyncCloseAllWorkingTsFileProcessors();
    }

    dataRegion.syncCloseAllWorkingTsFileProcessors();
    dataRegion.compact();
    long totalWaitingTime = 0;
    do {
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
    } while (CompactionTaskManager.getInstance().getExecutingTaskCount() > 0);

    QueryDataSource queryDataSource =
        dataRegion.query(
            Collections.singletonList(new PartialPath(deviceId, measurementId)),
            deviceId,
            context,
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

  @Ignore
  @Test
  public void testDeleteStorageGroupWhenCompacting() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setMaxInnerCompactionCandidateFileNum(10);
    try {
      for (int j = 0; j < 10; j++) {
        TSRecord record = new TSRecord(j, deviceId);
        record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
        dataRegion.insert(buildInsertRowNodeByTSRecord(record));
        dataRegion.asyncCloseAllWorkingTsFileProcessors();
      }
      dataRegion.syncCloseAllWorkingTsFileProcessors();
      ICompactionPerformer performer = new FastCompactionPerformer(false);
      performer.setSourceFiles(dataRegion.getSequenceFileList());
      InnerSpaceCompactionTask task =
          new InnerSpaceCompactionTask(
              0,
              dataRegion.getTsFileManager(),
              dataRegion.getSequenceFileList(),
              true,
              performer,
              new AtomicInteger(0),
              0);
      CompactionTaskManager.getInstance().addTaskToWaitingQueue(task);
      Thread.sleep(20);
      List<DataRegion> dataRegions = StorageEngine.getInstance().getAllDataRegions();
      List<DataRegion> regionsToBeDeleted = new ArrayList<>();
      for (DataRegion region : dataRegions) {
        if (region.getDatabaseName().equals(storageGroup)) {
          regionsToBeDeleted.add(region);
        }
      }
      for (DataRegion region : regionsToBeDeleted) {
        StorageEngine.getInstance()
            .deleteDataRegion(new DataRegionId(Integer.parseInt(region.getDataRegionId())));
      }
      Thread.sleep(500);

      for (TsFileResource resource : dataRegion.getSequenceFileList()) {
        Assert.assertFalse(resource.getTsFile().exists());
      }
      TsFileResource targetTsFileResource =
          TsFileNameGenerator.getInnerCompactionTargetFileResource(
              dataRegion.getSequenceFileList(), true);
      Assert.assertFalse(targetTsFileResource.getTsFile().exists());
      String dataDirectory = targetTsFileResource.getTsFile().getParent();
      File logFile =
          new File(
              dataDirectory
                  + File.separator
                  + targetTsFileResource.getTsFile().getName()
                  + CompactionLogger.INNER_COMPACTION_LOG_NAME_SUFFIX);
      Assert.assertFalse(logFile.exists());
      Assert.assertFalse(CommonDescriptor.getInstance().getConfig().isReadOnly());
      Assert.assertTrue(dataRegion.getTsFileManager().isAllowCompaction());
    } finally {
      new CompactionConfigRestorer().restoreCompactionConfig();
    }
  }

  @Test
  public void testTimedFlushSeqMemTable()
      throws IllegalPathException, InterruptedException, WriteProcessException, ShutdownException {
    // create one sequence memtable
    TSRecord record = new TSRecord(10000, deviceId);
    record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(1000)));
    dataRegion.insert(buildInsertRowNodeByTSRecord(record));
    Assert.assertEquals(1, MemTableManager.getInstance().getCurrentMemtableNumber());

    // change config & reboot timed service
    boolean prevEnableTimedFlushSeqMemtable = config.isEnableTimedFlushSeqMemtable();
    long preFLushInterval = config.getSeqMemtableFlushInterval();
    config.setEnableTimedFlushSeqMemtable(true);
    config.setSeqMemtableFlushInterval(5);
    StorageEngine.getInstance().rebootTimedService();

    Thread.sleep(500);

    Assert.assertEquals(1, dataRegion.getWorkSequenceTsFileProcessors().size());
    TsFileProcessor tsFileProcessor =
        dataRegion.getWorkSequenceTsFileProcessors().iterator().next();
    FlushManager flushManager = FlushManager.getInstance();

    // flush the sequence memtable
    dataRegion.timedFlushSeqMemTable();

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
      throws IllegalPathException, InterruptedException, WriteProcessException, ShutdownException {
    // create one sequence memtable & close
    TSRecord record = new TSRecord(10000, deviceId);
    record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(1000)));
    dataRegion.insert(buildInsertRowNodeByTSRecord(record));
    Assert.assertEquals(1, MemTableManager.getInstance().getCurrentMemtableNumber());
    dataRegion.syncCloseAllWorkingTsFileProcessors();
    Assert.assertEquals(0, MemTableManager.getInstance().getCurrentMemtableNumber());

    // create one unsequence memtable
    record = new TSRecord(1, deviceId);
    record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(1000)));
    dataRegion.insert(buildInsertRowNodeByTSRecord(record));
    Assert.assertEquals(1, MemTableManager.getInstance().getCurrentMemtableNumber());

    // change config & reboot timed service
    boolean prevEnableTimedFlushUnseqMemtable = config.isEnableTimedFlushUnseqMemtable();
    long preFLushInterval = config.getUnseqMemtableFlushInterval();
    config.setEnableTimedFlushUnseqMemtable(true);
    config.setUnseqMemtableFlushInterval(5);
    StorageEngine.getInstance().rebootTimedService();

    Thread.sleep(500);

    Assert.assertEquals(1, dataRegion.getWorkUnsequenceTsFileProcessors().size());
    TsFileProcessor tsFileProcessor =
        dataRegion.getWorkUnsequenceTsFileProcessors().iterator().next();
    FlushManager flushManager = FlushManager.getInstance();

    // flush the unsequence memtable
    dataRegion.timedFlushUnseqMemTable();

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

  /**
   * Totally 5 tsfiles<br>
   * file 0, file 2 and file 4 has d0 ~ d1, time range is 0 ~ 99, 200 ~ 299, 400 ~ 499<br>
   * file 1, file 3 has d0 ~ d2, time range is 100 ~ 199, 300 ~ 399<br>
   * delete d2 in time range 50 ~ 150 and 150 ~ 450. Therefore, only file 1 and file 3 has mods.
   */
  @Test
  public void testDeleteDataNotInFile()
      throws IllegalPathException, WriteProcessException, InterruptedException, IOException {
    for (int i = 0; i < 5; i++) {
      if (i % 2 == 0) {
        for (int d = 0; d < 2; d++) {
          for (int count = i * 100; count < i * 100 + 100; count++) {
            TSRecord record = new TSRecord(count, "root.vehicle.d" + d);
            record.addTuple(
                DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(count)));
            dataRegion.insert(buildInsertRowNodeByTSRecord(record));
          }
        }
      } else {
        for (int d = 0; d < 3; d++) {
          for (int count = i * 100; count < i * 100 + 100; count++) {
            TSRecord record = new TSRecord(count, "root.vehicle.d" + d);
            record.addTuple(
                DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(count)));
            dataRegion.insert(buildInsertRowNodeByTSRecord(record));
          }
        }
      }
      dataRegion.syncCloseAllWorkingTsFileProcessors();
    }

    // delete root.vehicle.d2.s0 data in the second file
    dataRegion.deleteByDevice(new PartialPath("root.vehicle.d2.s0"), 50, 150, 0, null);

    // delete root.vehicle.d2.s0 data in the third file
    dataRegion.deleteByDevice(new PartialPath("root.vehicle.d2.s0"), 150, 450, 0, null);

    for (int i = 0; i < dataRegion.getSequenceFileList().size(); i++) {
      TsFileResource resource = dataRegion.getSequenceFileList().get(i);
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

    List<DataRegion> dataRegions = StorageEngine.getInstance().getAllDataRegions();
    List<DataRegion> regionsToBeDeleted = new ArrayList<>();
    for (DataRegion region : dataRegions) {
      if (region.getDatabaseName().equals(storageGroup)) {
        regionsToBeDeleted.add(region);
      }
    }
    for (DataRegion region : regionsToBeDeleted) {
      StorageEngine.getInstance()
          .deleteDataRegion(new DataRegionId(Integer.parseInt(region.getDataRegionId())));
    }
    Thread.sleep(500);

    for (TsFileResource resource : dataRegion.getSequenceFileList()) {
      Assert.assertFalse(resource.getTsFile().exists());
    }
  }

  @Test
  public void testDeleteDataNotInFlushingMemtable()
      throws IllegalPathException, WriteProcessException, IOException {
    for (int j = 0; j < 100; j++) {
      TSRecord record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      dataRegion.insert(buildInsertRowNodeByTSRecord(record));
    }
    TsFileResource tsFileResource = dataRegion.getTsFileManager().getTsFileList(true).get(0);
    TsFileProcessor tsFileProcessor = tsFileResource.getProcessor();
    tsFileProcessor.getFlushingMemTable().addLast(tsFileProcessor.getWorkMemTable());

    // delete data which is in memtable
    dataRegion.deleteByDevice(new PartialPath("root.vehicle.d2.s0"), 50, 70, 0, null);

    // delete data which is not in memtable
    dataRegion.deleteByDevice(new PartialPath("root.vehicle.d200.s0"), 50, 70, 0, null);

    dataRegion.syncCloseAllWorkingTsFileProcessors();
    Assert.assertFalse(tsFileResource.getModFile().exists());
  }

  @Test
  public void testDeleteDataInSeqFlushingMemtable()
      throws IllegalPathException, WriteProcessException, IOException {
    for (int j = 100; j < 200; j++) {
      TSRecord record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      dataRegion.insert(buildInsertRowNodeByTSRecord(record));
    }
    TsFileResource tsFileResource = dataRegion.getTsFileManager().getTsFileList(true).get(0);
    TsFileProcessor tsFileProcessor = tsFileResource.getProcessor();
    tsFileProcessor.getFlushingMemTable().addLast(tsFileProcessor.getWorkMemTable());

    // delete data which is not in flushing memtable
    dataRegion.deleteByDevice(new PartialPath("root.vehicle.d0.s0"), 50, 99, 0, null);
    dataRegion.deleteByDevice(new PartialPath("root.vehicle.d200.s0"), 50, 70, 0, null);

    // delete data which is in flushing memtable
    dataRegion.deleteByDevice(new PartialPath("root.vehicle.d0.s0"), 50, 100, 0, null);
    dataRegion.deleteByDevice(new PartialPath("root.vehicle.d0.s0"), 50, 150, 0, null);
    dataRegion.deleteByDevice(new PartialPath("root.vehicle.d0.s0"), 100, 190, 0, null);

    dataRegion.syncCloseAllWorkingTsFileProcessors();
    Assert.assertTrue(tsFileResource.getModFile().exists());
    Assert.assertEquals(3, tsFileResource.getModFile().getModifications().size());
  }

  @Test
  public void testDeleteDataInUnSeqFlushingMemtable()
      throws IllegalPathException, WriteProcessException, IOException {
    for (int j = 100; j < 200; j++) {
      TSRecord record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      dataRegion.insert(buildInsertRowNodeByTSRecord(record));
    }
    TsFileResource tsFileResource = dataRegion.getTsFileManager().getTsFileList(true).get(0);

    // delete data which is not in work memtable
    dataRegion.deleteByDevice(new PartialPath("root.vehicle.d0.s0"), 50, 99, 0, null);
    dataRegion.deleteByDevice(new PartialPath("root.vehicle.d200.s0"), 50, 70, 0, null);

    // delete data which is in work memtable
    dataRegion.deleteByDevice(new PartialPath("root.vehicle.d0.s0"), 50, 100, 0, null);
    dataRegion.deleteByDevice(new PartialPath("root.vehicle.d0.s0"), 50, 150, 0, null);
    dataRegion.deleteByDevice(new PartialPath("root.vehicle.d0.s0"), 100, 190, 0, null);

    dataRegion.syncCloseAllWorkingTsFileProcessors();
    Assert.assertFalse(tsFileResource.getModFile().exists());

    // insert unseq data points
    for (int j = 50; j < 100; j++) {
      TSRecord record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      dataRegion.insert(buildInsertRowNodeByTSRecord(record));
    }
    // delete data which is not in work memtable
    dataRegion.deleteByDevice(new PartialPath("root.vehicle.d0.s0"), 200, 299, 0, null);
    dataRegion.deleteByDevice(new PartialPath("root.vehicle.d200.s0"), 50, 70, 0, null);

    // delete data which is in work memtable
    dataRegion.deleteByDevice(new PartialPath("root.vehicle.d0.s0"), 80, 85, 0, null);

    Assert.assertFalse(tsFileResource.getModFile().exists());

    tsFileResource = dataRegion.getTsFileManager().getTsFileList(false).get(0);
    TsFileProcessor tsFileProcessor = tsFileResource.getProcessor();
    tsFileProcessor.getFlushingMemTable().addLast(tsFileProcessor.getWorkMemTable());

    // delete data which is not in flushing memtable
    dataRegion.deleteByDevice(new PartialPath("root.vehicle.d0.s0"), 0, 49, 0, null);
    dataRegion.deleteByDevice(new PartialPath("root.vehicle.d0.s0"), 100, 200, 0, null);
    dataRegion.deleteByDevice(new PartialPath("root.vehicle.d200.s0"), 50, 70, 0, null);

    // delete data which is in flushing memtable
    dataRegion.deleteByDevice(new PartialPath("root.vehicle.d0.s0"), 25, 50, 0, null);
    dataRegion.deleteByDevice(new PartialPath("root.vehicle.d0.s0"), 50, 80, 0, null);
    dataRegion.deleteByDevice(new PartialPath("root.vehicle.d0.s0"), 99, 150, 0, null);

    dataRegion.syncCloseAllWorkingTsFileProcessors();
    Assert.assertTrue(tsFileResource.getModFile().exists());
    Assert.assertEquals(3, tsFileResource.getModFile().getModifications().size());
  }

  @Test
  public void testFlushingEmptyMemtable()
      throws IllegalPathException, WriteProcessException, IOException {
    for (int j = 100; j < 200; j++) {
      TSRecord record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      dataRegion.insert(buildInsertRowNodeByTSRecord(record));
    }
    TsFileResource tsFileResource = dataRegion.getTsFileManager().getTsFileList(true).get(0);

    // delete all data which is in flushing memtable
    dataRegion.deleteByDevice(new PartialPath("root.vehicle.d0.s0"), 100, 200, 0, null);

    dataRegion.syncCloseAllWorkingTsFileProcessors();
    Assert.assertFalse(tsFileResource.getTsFile().exists());
    Assert.assertFalse(tsFileResource.getModFile().exists());
    Assert.assertFalse(dataRegion.getTsFileManager().contains(tsFileResource, true));
    Assert.assertFalse(
        dataRegion.getWorkSequenceTsFileProcessors().contains(tsFileResource.getProcessor()));
  }

  static class DummyDataRegion extends DataRegion {

    DummyDataRegion(String systemInfoDir, String storageGroupName) throws DataRegionException {
      super(systemInfoDir, "0", new TsFileFlushPolicy.DirectFlushPolicy(), storageGroupName);
    }
  }
}
