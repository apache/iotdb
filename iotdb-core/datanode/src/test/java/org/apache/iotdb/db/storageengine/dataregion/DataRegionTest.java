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

package org.apache.iotdb.db.storageengine.dataregion;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.exception.ShutdownException;
import org.apache.iotdb.commons.path.AlignedFullPath;
import org.apache.iotdb.commons.path.IFullPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.NonAlignedFullPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.WriteProcessRejectException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementTestUtils;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.constant.InnerSequenceCompactionSelector;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.constant.InnerUnsequenceCompactionSelector;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionConfigRestorer;
import org.apache.iotdb.db.storageengine.dataregion.flush.FlushManager;
import org.apache.iotdb.db.storageengine.dataregion.flush.TsFileFlushPolicy.DirectFlushPolicy;
import org.apache.iotdb.db.storageengine.dataregion.memtable.ReadOnlyMemChunk;
import org.apache.iotdb.db.storageengine.dataregion.memtable.TsFileProcessor;
import org.apache.iotdb.db.storageengine.dataregion.modification.DeletionPredicate;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.TagPredicate.NOP;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource.ModIterator;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.evolution.ColumnRename;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.evolution.TableRename;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.db.storageengine.rescon.memory.MemTableManager;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.IDeviceID.Factory;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.reader.IPointReader;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.DataPoint;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.queryengine.plan.statement.StatementTestUtils.genInsertRowNode;
import static org.apache.iotdb.db.queryengine.plan.statement.StatementTestUtils.genInsertTabletNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DataRegionTest {
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();
  private static final Logger logger = LoggerFactory.getLogger(DataRegionTest.class);

  private String storageGroup = "root.vehicle.d0";
  private String systemDir = TestConstant.OUTPUT_DATA_DIR.concat("info");
  private String deviceId = "root.vehicle.d0";

  private IDeviceID device = Factory.DEFAULT_FACTORY.create(deviceId);
  private String measurementId = "s0";

  private NonAlignedFullPath nonAlignedFullPath =
      new NonAlignedFullPath(device, new MeasurementSchema(measurementId, TSDataType.INT32));

  private DataRegion dataRegion;
  private QueryContext context = EnvironmentUtils.TEST_QUERY_CONTEXT;

  private double preWriteMemoryVariationReportProportion =
      config.getWriteMemoryVariationReportProportion();
  private InnerSequenceCompactionSelector seqSelector;
  private InnerUnsequenceCompactionSelector unseqSelector;

  @Before
  public void setUp() throws Exception {
    config.setWriteMemoryVariationReportProportion(0);
    EnvironmentUtils.envSetUp();
    dataRegion = new DummyDataRegion(systemDir, storageGroup);
    StorageEngine.getInstance().setDataRegion(new DataRegionId(0), dataRegion);
    CompactionTaskManager.getInstance().start();
    seqSelector = config.getInnerSequenceCompactionSelector();
    unseqSelector = config.getInnerUnsequenceCompactionSelector();
    config.setInnerSequenceCompactionSelector(
        InnerSequenceCompactionSelector.SIZE_TIERED_SINGLE_TARGET);
    config.setInnerUnsequenceCompactionSelector(
        InnerUnsequenceCompactionSelector.SIZE_TIERED_SINGLE_TARGET);
    DataNodeTableCache.getInstance()
        .preUpdateTable(dataRegion.getDatabaseName(), StatementTestUtils.genTsTable(1), null);
    DataNodeTableCache.getInstance()
        .commitUpdateTable(dataRegion.getDatabaseName(), StatementTestUtils.tableName(1), null);
    DataNodeTableCache.getInstance()
        .preUpdateTable(dataRegion.getDatabaseName(), StatementTestUtils.genTsTable(2), null);
    DataNodeTableCache.getInstance()
        .commitUpdateTable(dataRegion.getDatabaseName(), StatementTestUtils.tableName(2), null);
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
    config.setWriteMemoryVariationReportProportion(preWriteMemoryVariationReportProportion);
    config.setInnerSequenceCompactionSelector(seqSelector);
    config.setInnerUnsequenceCompactionSelector(unseqSelector);
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
    TSRecord record = new TSRecord(deviceId, 10000);
    record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(1000)));
    dataRegion.insert(buildInsertRowNodeByTSRecord(record));
    dataRegion.syncCloseAllWorkingTsFileProcessors();

    for (int j = 1; j <= 10; j++) {
      record = new TSRecord(deviceId, j);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      dataRegion.insert(buildInsertRowNodeByTSRecord(record));
    }
    dataRegion.syncCloseWorkingTsFileProcessors(false);

    for (int j = 11; j <= 20; j++) {
      record = new TSRecord(deviceId, j);
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

    MeasurementPath path = new MeasurementPath(deviceId, measurementId);
    DeleteDataNode deleteDataNode1 =
        new DeleteDataNode(new PlanNodeId("1"), Collections.singletonList(path), 0, 15L);
    dataRegion.deleteByDevice(new MeasurementPath(deviceId, measurementId), deleteDataNode1);

    List<TsFileResource> tsfileResourcesForQuery = new ArrayList<>();
    for (TsFileProcessor tsfileProcessor : dataRegion.getWorkUnsequenceTsFileProcessors()) {
      tsfileProcessor.query(
          Collections.singletonList(IFullPath.convertToIFullPath(fullPath)),
          EnvironmentUtils.TEST_QUERY_CONTEXT,
          tsfileResourcesForQuery,
          null);
    }

    assertEquals(1, tsfileResourcesForQuery.size());
    List<ReadOnlyMemChunk> memChunks =
        tsfileResourcesForQuery.get(0).getReadOnlyMemChunk(IFullPath.convertToIFullPath(fullPath));
    long time = 16;
    for (ReadOnlyMemChunk memChunk : memChunks) {
      IPointReader iterator = memChunk.getPointReader();
      while (iterator.hasNextTimeValuePair()) {
        TimeValuePair timeValuePair = iterator.nextTimeValuePair();
        assertEquals(time++, timeValuePair.getTimestamp());
      }
    }
  }

  @Test
  public void testSequenceSyncClose()
      throws WriteProcessException, QueryProcessException, IllegalPathException {
    for (int j = 1; j <= 10; j++) {
      TSRecord record = new TSRecord(deviceId, j);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      dataRegion.insert(buildInsertRowNodeByTSRecord(record));
      dataRegion.syncCloseAllWorkingTsFileProcessors();
    }

    IDeviceID device = Factory.DEFAULT_FACTORY.create(deviceId);
    QueryDataSource queryDataSource =
        dataRegion.query(
            Collections.singletonList(
                new NonAlignedFullPath(
                    device, new MeasurementSchema(measurementId, TSDataType.INT32))),
            device,
            context,
            null,
            null);
    assertEquals(10, queryDataSource.getSeqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      assertTrue(resource.isClosed());
    }
  }

  @Test
  public void testRelationalTabletWriteAndSyncClose()
      throws QueryProcessException, WriteProcessException {
    RelationalInsertTabletNode insertTabletNode1 = genInsertTabletNode(10, 0);
    dataRegion.insertTablet(insertTabletNode1);
    dataRegion.syncCloseAllWorkingTsFileProcessors();

    RelationalInsertTabletNode insertTabletNode2 = genInsertTabletNode(10, 10);
    dataRegion.insertTablet(insertTabletNode2);
    dataRegion.syncCloseAllWorkingTsFileProcessors();

    String measurementName = "m1";
    MeasurementSchema measurementSchema = new MeasurementSchema(measurementName, TSDataType.DOUBLE);
    final IDeviceID deviceID1 = insertTabletNode1.getDeviceID(0);
    final IDeviceID deviceID2 = insertTabletNode2.getDeviceID(0);

    QueryDataSource queryDataSource =
        dataRegion.query(
            Collections.singletonList(
                new AlignedFullPath(
                    deviceID1,
                    Collections.singletonList(measurementName),
                    Collections.singletonList(measurementSchema))),
            deviceID1,
            context,
            null,
            null);
    assertEquals(1, queryDataSource.getSeqResources().size());
    assertEquals(0, queryDataSource.getUnseqResources().size());

    queryDataSource =
        dataRegion.query(
            Collections.singletonList(
                new AlignedFullPath(
                    deviceID2,
                    Collections.singletonList(measurementName),
                    Collections.singletonList(measurementSchema))),
            deviceID2,
            context,
            null,
            null);
    assertEquals(1, queryDataSource.getSeqResources().size());
    assertEquals(0, queryDataSource.getUnseqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      assertTrue(resource.isClosed());
    }
  }

  @Test
  public void testRelationRowWriteAndSyncClose()
      throws QueryProcessException, WriteProcessException {
    RelationalInsertRowNode insertNode1 = genInsertRowNode(0);
    dataRegion.insert(insertNode1);
    dataRegion.syncCloseAllWorkingTsFileProcessors();

    RelationalInsertRowNode insertRowNode2 = genInsertRowNode(10);
    dataRegion.insert(insertRowNode2);
    dataRegion.syncCloseAllWorkingTsFileProcessors();

    String measurementName = "m1";
    MeasurementSchema measurementSchema = new MeasurementSchema(measurementName, TSDataType.DOUBLE);
    final IDeviceID deviceID1 = insertNode1.getDeviceID();
    final IDeviceID deviceID2 = insertRowNode2.getDeviceID();

    QueryDataSource queryDataSource =
        dataRegion.query(
            Collections.singletonList(
                new AlignedFullPath(
                    deviceID1,
                    Collections.singletonList(measurementName),
                    Collections.singletonList(measurementSchema))),
            deviceID1,
            context,
            null,
            null);
    assertEquals(1, queryDataSource.getSeqResources().size());
    assertEquals(0, queryDataSource.getUnseqResources().size());

    queryDataSource =
        dataRegion.query(
            Collections.singletonList(
                new AlignedFullPath(
                    deviceID2,
                    Collections.singletonList(measurementName),
                    Collections.singletonList(measurementSchema))),
            deviceID2,
            context,
            null,
            null);
    assertEquals(1, queryDataSource.getSeqResources().size());
    assertEquals(0, queryDataSource.getUnseqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      assertTrue(resource.isClosed());
    }
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
      ((int[]) columns[0])[r] = r;
      ((long[]) columns[1])[r] = r;
    }

    BitMap[] bitMaps = new BitMap[2];
    bitMaps[0] = new BitMap(100);
    bitMaps[1] = new BitMap(100);
    for (int r = 0; r < 100; r++) {
      if (r % 2 == 0) {
        bitMaps[0].mark(r);
      } else {
        bitMaps[1].mark(r);
      }
    }

    InsertTabletNode insertTabletNode1 =
        new InsertTabletNode(
            new QueryId("test_write").genPlanNodeId(),
            new PartialPath("root.vehicle.d0"),
            false,
            measurements,
            dataTypes,
            measurementSchemas,
            times,
            bitMaps,
            columns,
            times.length);
    int hashCode1 = Arrays.hashCode((int[]) columns[0]);
    int hashCode2 = Arrays.hashCode((long[]) columns[1]);
    dataRegion.insertTablet(insertTabletNode1);
    // the hashCode should not be changed when insert
    assertEquals(hashCode1, Arrays.hashCode((int[]) columns[0]));
    assertEquals(hashCode2, Arrays.hashCode((long[]) columns[1]));
    dataRegion.syncCloseAllWorkingTsFileProcessors();

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
            measurementSchemas,
            times,
            null,
            columns,
            times.length);

    dataRegion.insertTablet(insertTabletNode2);
    dataRegion.syncCloseAllWorkingTsFileProcessors();

    QueryDataSource queryDataSource =
        dataRegion.query(
            Collections.singletonList(nonAlignedFullPath), device, context, null, null);

    assertEquals(2, queryDataSource.getSeqResources().size());
    assertEquals(1, queryDataSource.getUnseqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      assertTrue(resource.isClosed());
    }
  }

  @Test
  public void testIoTDBTabletWriteAndDeleteDataRegion()
      throws QueryProcessException,
          IllegalPathException,
          WriteProcessException,
          TsFileProcessorException {
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
            measurementSchemas,
            times,
            null,
            columns,
            times.length);

    dataRegion.insertTablet(insertTabletNode1);

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
            measurementSchemas,
            times,
            null,
            columns,
            times.length);

    dataRegion.insertTablet(insertTabletNode2);
    assertTrue(SystemInfo.getInstance().getTotalMemTableSize() > 0);
    dataRegion.syncDeleteDataFiles();
    assertEquals(0, SystemInfo.getInstance().getTotalMemTableSize());

    QueryDataSource queryDataSource =
        dataRegion.query(
            Collections.singletonList(nonAlignedFullPath), device, context, null, null);

    assertEquals(0, queryDataSource.getSeqResources().size());
    assertEquals(0, queryDataSource.getUnseqResources().size());
  }

  @Test
  public void testEmptyTabletWriteAndSyncClose()
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
    BitMap[] bitMaps = new BitMap[2];
    bitMaps[0] = new BitMap(100);
    bitMaps[1] = new BitMap(100);

    for (int r = 0; r < 100; r++) {
      times[r] = r;
      bitMaps[0].mark(r);
      bitMaps[1].mark(r);
    }

    InsertTabletNode insertTabletNode1 =
        new InsertTabletNode(
            new QueryId("test_write").genPlanNodeId(),
            new PartialPath("root.vehicle.d0"),
            false,
            measurements,
            dataTypes,
            measurementSchemas,
            times,
            bitMaps,
            columns,
            times.length);

    dataRegion.insertTablet(insertTabletNode1);
    dataRegion.syncCloseAllWorkingTsFileProcessors();

    for (int r = 50; r < 149; r++) {
      times[r - 50] = r;
      bitMaps[0].mark(r - 50);
      bitMaps[1].mark(r - 50);
    }

    InsertTabletNode insertTabletNode2 =
        new InsertTabletNode(
            new QueryId("test_write").genPlanNodeId(),
            new PartialPath("root.vehicle.d0"),
            false,
            measurements,
            dataTypes,
            measurementSchemas,
            times,
            bitMaps,
            columns,
            times.length);

    dataRegion.insertTablet(insertTabletNode2);
    dataRegion.syncCloseAllWorkingTsFileProcessors();

    QueryDataSource queryDataSource =
        dataRegion.query(
            Collections.singletonList(nonAlignedFullPath), device, context, null, null);

    assertEquals(0, queryDataSource.getSeqResources().size());
    assertEquals(0, queryDataSource.getUnseqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      assertTrue(resource.isClosed());
    }
  }

  @Test
  public void testAllMeasurementsFailedTabletWriteAndSyncClose()
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
            measurementSchemas,
            times,
            null,
            columns,
            times.length);
    insertTabletNode1.setFailedMeasurementNumber(2);

    dataRegion.insertTablet(insertTabletNode1);
    dataRegion.syncCloseAllWorkingTsFileProcessors();

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
            measurementSchemas,
            times,
            null,
            columns,
            times.length);
    insertTabletNode2.setFailedMeasurementNumber(2);

    dataRegion.insertTablet(insertTabletNode2);
    dataRegion.syncCloseAllWorkingTsFileProcessors();

    QueryDataSource queryDataSource =
        dataRegion.query(
            Collections.singletonList(nonAlignedFullPath), device, context, null, null);

    assertEquals(0, queryDataSource.getSeqResources().size());
    assertEquals(0, queryDataSource.getUnseqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      assertTrue(resource.isClosed());
    }
  }

  @Test
  public void testSeqAndUnSeqSyncClose()
      throws WriteProcessException, QueryProcessException, IllegalPathException {
    for (int j = 21; j <= 30; j++) {
      TSRecord record = new TSRecord(deviceId, j);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      dataRegion.insert(buildInsertRowNodeByTSRecord(record));
      dataRegion.syncCloseAllWorkingTsFileProcessors();
    }

    for (int j = 10; j >= 1; j--) {
      TSRecord record = new TSRecord(deviceId, j);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      dataRegion.insert(buildInsertRowNodeByTSRecord(record));
      dataRegion.syncCloseAllWorkingTsFileProcessors();
    }

    QueryDataSource queryDataSource =
        dataRegion.query(
            Collections.singletonList(nonAlignedFullPath), device, context, null, null);
    assertEquals(10, queryDataSource.getSeqResources().size());
    assertEquals(10, queryDataSource.getUnseqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      assertTrue(resource.isClosed());
    }
    for (TsFileResource resource : queryDataSource.getUnseqResources()) {
      assertTrue(resource.isClosed());
    }
  }

  @Test
  public void testAllMeasurementsFailedRecordSeqAndUnSeqSyncClose()
      throws WriteProcessException, QueryProcessException, IllegalPathException {
    for (int j = 21; j <= 30; j++) {
      TSRecord record = new TSRecord(deviceId, j);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      InsertRowNode rowNode = buildInsertRowNodeByTSRecord(record);
      rowNode.setFailedMeasurementNumber(1);
      dataRegion.insert(rowNode);
      dataRegion.syncCloseAllWorkingTsFileProcessors();
    }

    for (int j = 10; j >= 1; j--) {
      TSRecord record = new TSRecord(deviceId, j);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      InsertRowNode rowNode = buildInsertRowNodeByTSRecord(record);
      rowNode.setFailedMeasurementNumber(1);
      dataRegion.insert(rowNode);
      dataRegion.syncCloseAllWorkingTsFileProcessors();
    }

    QueryDataSource queryDataSource =
        dataRegion.query(
            Collections.singletonList(nonAlignedFullPath), device, context, null, null);
    assertEquals(0, queryDataSource.getSeqResources().size());
    assertEquals(0, queryDataSource.getUnseqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      assertTrue(resource.isClosed());
    }
    for (TsFileResource resource : queryDataSource.getUnseqResources()) {
      assertTrue(resource.isClosed());
    }
  }

  @Test
  public void testDisableSeparateDataForInsertRowPlan()
      throws WriteProcessException, QueryProcessException, IllegalPathException, IOException {
    boolean defaultValue = config.isEnableSeparateData();
    config.setEnableSeparateData(false);

    for (int j = 21; j <= 30; j++) {
      TSRecord record = new TSRecord(deviceId, j);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      dataRegion.insert(buildInsertRowNodeByTSRecord(record));
      dataRegion.syncCloseAllWorkingTsFileProcessors();
    }

    for (int j = 10; j >= 1; j--) {
      TSRecord record = new TSRecord(deviceId, j);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      dataRegion.insert(buildInsertRowNodeByTSRecord(record));
      dataRegion.syncCloseAllWorkingTsFileProcessors();
    }

    QueryDataSource queryDataSource =
        dataRegion.query(
            Collections.singletonList(nonAlignedFullPath), device, context, null, null);
    assertEquals(0, queryDataSource.getSeqResources().size());
    assertEquals(20, queryDataSource.getUnseqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      assertTrue(resource.isClosed());
    }
    for (TsFileResource resource : queryDataSource.getUnseqResources()) {
      assertTrue(resource.isClosed());
    }

    config.setEnableSeparateData(defaultValue);
  }

  @Test
  public void testDisableSeparateDataForInsertTablet1()
      throws QueryProcessException, IllegalPathException, IOException, WriteProcessException {
    boolean defaultEnableDiscard = config.isEnableSeparateData();
    long defaultTimePartition = COMMON_CONFIG.getTimePartitionInterval();
    config.setEnableSeparateData(false);
    COMMON_CONFIG.setTimePartitionInterval(100000);

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
            measurementSchemas,
            times,
            null,
            columns,
            times.length);

    dataRegion.insertTablet(insertTabletNode1);
    dataRegion.syncCloseAllWorkingTsFileProcessors();

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
            measurementSchemas,
            times,
            null,
            columns,
            times.length);

    dataRegion.insertTablet(insertTabletNode2);
    dataRegion.syncCloseAllWorkingTsFileProcessors();

    QueryDataSource queryDataSource =
        dataRegion.query(
            Collections.singletonList(nonAlignedFullPath), device, context, null, null);

    assertEquals(0, queryDataSource.getSeqResources().size());
    assertEquals(2, queryDataSource.getUnseqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      assertTrue(resource.isClosed());
    }

    config.setEnableSeparateData(defaultEnableDiscard);
    COMMON_CONFIG.setTimePartitionInterval(defaultTimePartition);
  }

  @Test
  public void testDisableSeparateDataForInsertTablet2()
      throws QueryProcessException, IllegalPathException, IOException, WriteProcessException {
    boolean defaultEnableDiscard = config.isEnableSeparateData();
    long defaultTimePartition = COMMON_CONFIG.getTimePartitionInterval();
    config.setEnableSeparateData(false);
    COMMON_CONFIG.setTimePartitionInterval(1200000);

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
            measurementSchemas,
            times,
            null,
            columns,
            times.length);

    dataRegion.insertTablet(insertTabletNode1);
    dataRegion.syncCloseAllWorkingTsFileProcessors();

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
            measurementSchemas,
            times,
            null,
            columns,
            times.length);

    dataRegion.insertTablet(insertTabletNode2);
    dataRegion.syncCloseAllWorkingTsFileProcessors();

    QueryDataSource queryDataSource =
        dataRegion.query(
            Collections.singletonList(nonAlignedFullPath), device, context, null, null);

    assertEquals(0, queryDataSource.getSeqResources().size());
    assertEquals(2, queryDataSource.getUnseqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      assertTrue(resource.isClosed());
    }

    config.setEnableSeparateData(defaultEnableDiscard);
    COMMON_CONFIG.setTimePartitionInterval(defaultTimePartition);
  }

  @Test
  public void testDisableSeparateDataForInsertTablet3()
      throws QueryProcessException, IllegalPathException, IOException, WriteProcessException {
    boolean defaultEnableDiscard = config.isEnableSeparateData();
    long defaultTimePartition = COMMON_CONFIG.getTimePartitionInterval();
    config.setEnableSeparateData(false);
    COMMON_CONFIG.setTimePartitionInterval(1000000);

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
            measurementSchemas,
            times,
            null,
            columns,
            times.length);

    dataRegion.insertTablet(insertTabletNode1);
    dataRegion.syncCloseAllWorkingTsFileProcessors();

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
            measurementSchemas,
            times,
            null,
            columns,
            times.length);

    dataRegion.insertTablet(insertTabletNode2);
    dataRegion.syncCloseAllWorkingTsFileProcessors();

    QueryDataSource queryDataSource =
        dataRegion.query(
            Collections.singletonList(nonAlignedFullPath), device, context, null, null);

    assertEquals(0, queryDataSource.getSeqResources().size());
    assertEquals(2, queryDataSource.getUnseqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      assertTrue(resource.isClosed());
    }

    config.setEnableSeparateData(defaultEnableDiscard);
    COMMON_CONFIG.setTimePartitionInterval(defaultTimePartition);
  }

  @Test
  public void testInsertUnSequenceRows()
      throws IllegalPathException,
          WriteProcessRejectException,
          QueryProcessException,
          DataRegionException,
          TsFileProcessorException {
    DataRegion dataRegion1 = new DummyDataRegion(systemDir, "root.Rows");
    long[] time = new long[] {3, 4, 1, 2};
    List<Integer> indexList = new ArrayList<>();
    List<InsertRowNode> nodes = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      TSRecord record = new TSRecord("root.Rows", time[i]);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(i)));
      nodes.add(buildInsertRowNodeByTSRecord(record));
      indexList.add(i);
    }
    InsertRowsNode insertRowsNode = new InsertRowsNode(new PlanNodeId(""), indexList, nodes);
    dataRegion1.insert(insertRowsNode);
    dataRegion1.syncCloseAllWorkingTsFileProcessors();
    IDeviceID tmpDeviceId = Factory.DEFAULT_FACTORY.create("root.Rows");
    QueryDataSource queryDataSource =
        dataRegion1.query(
            Collections.singletonList(
                new NonAlignedFullPath(
                    tmpDeviceId, new MeasurementSchema(measurementId, TSDataType.INT32))),
            tmpDeviceId,
            context,
            null,
            null);
    assertEquals(1, queryDataSource.getSeqResources().size());
    assertEquals(0, queryDataSource.getUnseqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      assertTrue(resource.isClosed());
    }
    dataRegion1.syncDeleteDataFiles();
  }

  @Test
  public void testSmallReportProportionInsertRow()
      throws WriteProcessException,
          QueryProcessException,
          IllegalPathException,
          IOException,
          DataRegionException,
          TsFileProcessorException {
    double defaultValue = config.getWriteMemoryVariationReportProportion();
    config.setWriteMemoryVariationReportProportion(0);
    DataRegion dataRegion1 = new DummyDataRegion(systemDir, "root.ln22");

    for (int j = 21; j <= 30; j++) {
      TSRecord record = new TSRecord("root.ln22", j);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      dataRegion1.insert(buildInsertRowNodeByTSRecord(record));
      dataRegion1.syncCloseAllWorkingTsFileProcessors();
    }
    dataRegion1.syncCloseAllWorkingTsFileProcessors();
    IDeviceID tmpDeviceId = Factory.DEFAULT_FACTORY.create("root.ln22");
    QueryDataSource queryDataSource =
        dataRegion1.query(
            Collections.singletonList(
                new NonAlignedFullPath(
                    tmpDeviceId, new MeasurementSchema(measurementId, TSDataType.INT32))),
            tmpDeviceId,
            context,
            null,
            null);
    assertEquals(10, queryDataSource.getSeqResources().size());
    assertEquals(0, queryDataSource.getUnseqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      assertTrue(resource.isClosed());
    }
    for (TsFileResource resource : queryDataSource.getUnseqResources()) {
      assertTrue(resource.isClosed());
    }

    dataRegion1.syncDeleteDataFiles();
    config.setWriteMemoryVariationReportProportion(defaultValue);
  }

  @Test
  public void testMerge()
      throws WriteProcessException, QueryProcessException, IllegalPathException {
    int originCandidateFileNum =
        IoTDBDescriptor.getInstance().getConfig().getInnerCompactionCandidateFileNum();
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(9);
    boolean originEnableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    boolean originEnableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    boolean originEnableCrossSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableCrossSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(true);
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(true);
    IoTDBDescriptor.getInstance().getConfig().setEnableCrossSpaceCompaction(false);
    long finishedCompactionTaskNumWhenTestStart =
        CompactionTaskManager.getInstance().getFinishedTaskNum();
    for (int j = 21; j <= 30; j++) {
      TSRecord record = new TSRecord(deviceId, j);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      dataRegion.insert(buildInsertRowNodeByTSRecord(record));
      dataRegion.syncCloseAllWorkingTsFileProcessors();
    }
    dataRegion.syncCloseAllWorkingTsFileProcessors();

    for (int j = 10; j >= 1; j--) {
      TSRecord record = new TSRecord(deviceId, j);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      dataRegion.insert(buildInsertRowNodeByTSRecord(record));
      dataRegion.syncCloseAllWorkingTsFileProcessors();
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
    } while (CompactionTaskManager.getInstance().getFinishedTaskNum()
        <= finishedCompactionTaskNumWhenTestStart + 1);

    QueryDataSource queryDataSource =
        dataRegion.query(
            Collections.singletonList(nonAlignedFullPath), device, context, null, null);
    assertEquals(2, queryDataSource.getSeqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      assertTrue(resource.isClosed());
    }
    for (TsFileResource resource : queryDataSource.getUnseqResources()) {
      assertTrue(resource.isClosed());
    }
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setInnerCompactionCandidateFileNum(originCandidateFileNum);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableSeqSpaceCompaction(originEnableSeqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableCrossSpaceCompaction(originEnableCrossSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableUnseqSpaceCompaction(originEnableUnseqSpaceCompaction);
  }

  @Ignore
  @Test
  public void testDeleteStorageGroupWhenCompacting() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setInnerCompactionCandidateFileNum(10);
    try {
      for (int j = 0; j < 10; j++) {
        TSRecord record = new TSRecord(deviceId, j);
        record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
        dataRegion.insert(buildInsertRowNodeByTSRecord(record));
        dataRegion.syncCloseAllWorkingTsFileProcessors();
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
      assertTrue(dataRegion.getTsFileManager().isAllowCompaction());
    } finally {
      new CompactionConfigRestorer().restoreCompactionConfig();
    }
  }

  @Test
  public void testTimedFlushSeqMemTable()
      throws IllegalPathException, InterruptedException, WriteProcessException, ShutdownException {
    // create one sequence memtable
    TSRecord record = new TSRecord(deviceId, 10000);
    record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(1000)));
    dataRegion.insert(buildInsertRowNodeByTSRecord(record));
    assertEquals(1, MemTableManager.getInstance().getCurrentMemtableNumber());

    // change config & reboot timed service
    boolean prevEnableTimedFlushSeqMemtable = config.isEnableTimedFlushSeqMemtable();
    long preFLushInterval = config.getSeqMemtableFlushInterval();
    config.setEnableTimedFlushSeqMemtable(true);
    config.setSeqMemtableFlushInterval(5);
    StorageEngine.getInstance().rebootTimedService();

    Thread.sleep(500);

    assertEquals(1, dataRegion.getWorkSequenceTsFileProcessors().size());
    TsFileProcessor tsFileProcessor =
        dataRegion.getWorkSequenceTsFileProcessors().iterator().next();
    FlushManager flushManager = FlushManager.getInstance();

    // flush the sequence memtable
    tsFileProcessor.getWorkMemTable().getUpdateTime();
    Thread.sleep(500);
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

    assertEquals(0, MemTableManager.getInstance().getCurrentMemtableNumber());

    config.setEnableTimedFlushSeqMemtable(prevEnableTimedFlushSeqMemtable);
    config.setSeqMemtableFlushInterval(preFLushInterval);
  }

  @Test
  public void testTimedFlushUnseqMemTable()
      throws IllegalPathException, InterruptedException, WriteProcessException, ShutdownException {
    // create one sequence memtable & close
    TSRecord record = new TSRecord(deviceId, 10000);
    record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(1000)));
    dataRegion.insert(buildInsertRowNodeByTSRecord(record));
    assertEquals(1, MemTableManager.getInstance().getCurrentMemtableNumber());
    dataRegion.syncCloseAllWorkingTsFileProcessors();
    assertEquals(0, MemTableManager.getInstance().getCurrentMemtableNumber());

    // create one unsequence memtable
    record = new TSRecord(deviceId, 1);
    record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(1000)));
    dataRegion.insert(buildInsertRowNodeByTSRecord(record));
    assertEquals(1, MemTableManager.getInstance().getCurrentMemtableNumber());

    // change config & reboot timed service
    boolean prevEnableTimedFlushUnseqMemtable = config.isEnableTimedFlushUnseqMemtable();
    long preFLushInterval = config.getUnseqMemtableFlushInterval();
    config.setEnableTimedFlushUnseqMemtable(true);
    config.setUnseqMemtableFlushInterval(5);
    StorageEngine.getInstance().rebootTimedService();

    Thread.sleep(500);

    assertEquals(1, dataRegion.getWorkUnsequenceTsFileProcessors().size());
    TsFileProcessor tsFileProcessor =
        dataRegion.getWorkUnsequenceTsFileProcessors().iterator().next();
    FlushManager flushManager = FlushManager.getInstance();

    // flush the unsequence memtable
    tsFileProcessor.getWorkMemTable().getUpdateTime();
    Thread.sleep(500);
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

    assertEquals(0, MemTableManager.getInstance().getCurrentMemtableNumber());

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
            TSRecord record = new TSRecord("root.vehicle.d" + d, count);
            record.addTuple(
                DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(count)));
            dataRegion.insert(buildInsertRowNodeByTSRecord(record));
          }
        }
      } else {
        for (int d = 0; d < 3; d++) {
          for (int count = i * 100; count < i * 100 + 100; count++) {
            TSRecord record = new TSRecord("root.vehicle.d" + d, count);
            record.addTuple(
                DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(count)));
            dataRegion.insert(buildInsertRowNodeByTSRecord(record));
          }
        }
      }
      dataRegion.syncCloseAllWorkingTsFileProcessors();
    }

    MeasurementPath path = new MeasurementPath("root.vehicle.d2.s0");
    DeleteDataNode deleteDataNode1 =
        new DeleteDataNode(new PlanNodeId("1"), Collections.singletonList(path), 50, 150);
    deleteDataNode1.setSearchIndex(0);
    // delete root.vehicle.d2.s0 data in the second file
    dataRegion.deleteByDevice(new MeasurementPath("root.vehicle.d2.s0"), deleteDataNode1);

    DeleteDataNode deleteDataNode2 =
        new DeleteDataNode(new PlanNodeId("2"), Collections.singletonList(path), 150, 450);
    deleteDataNode2.setSearchIndex(0);
    // delete root.vehicle.d2.s0 data in the third file
    dataRegion.deleteByDevice(new MeasurementPath("root.vehicle.d2.s0"), deleteDataNode2);

    for (int i = 0; i < dataRegion.getSequenceFileList().size(); i++) {
      TsFileResource resource = dataRegion.getSequenceFileList().get(i);
      if (i == 1) {
        assertTrue(resource.anyModFileExists());
        assertEquals(2, resource.getAllModEntries().size());
      } else if (i == 3) {
        assertTrue(resource.anyModFileExists());
        assertEquals(1, resource.getAllModEntries().size());
      } else {
        Assert.assertFalse(resource.anyModFileExists());
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
      TSRecord record = new TSRecord(deviceId, j);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      dataRegion.insert(buildInsertRowNodeByTSRecord(record));
    }
    TsFileResource tsFileResource = dataRegion.getTsFileManager().getTsFileList(true).get(0);
    TsFileProcessor tsFileProcessor = tsFileResource.getProcessor();
    tsFileProcessor.getFlushingMemTable().addLast(tsFileProcessor.getWorkMemTable());

    MeasurementPath path = new MeasurementPath("root.vehicle.d2.s0");
    DeleteDataNode deleteDataNode1 =
        new DeleteDataNode(new PlanNodeId("1"), Collections.singletonList(path), 50, 70);
    deleteDataNode1.setSearchIndex(0);
    // delete data which is in memtable
    dataRegion.deleteByDevice(new MeasurementPath("root.vehicle.d2.s0"), deleteDataNode1);

    MeasurementPath path2 = new MeasurementPath("root.vehicle.d200.s0");
    DeleteDataNode deleteDataNode2 =
        new DeleteDataNode(new PlanNodeId("2"), Collections.singletonList(path2), 50, 70);
    deleteDataNode2.setSearchIndex(0);
    // delete data which is not in memtable
    dataRegion.deleteByDevice(new MeasurementPath("root.vehicle.d200.s0"), deleteDataNode2);

    dataRegion.syncCloseAllWorkingTsFileProcessors();
    Assert.assertFalse(tsFileResource.anyModFileExists());
  }

  @Test
  public void testDeleteDataInSeqFlushingMemtable()
      throws IllegalPathException, WriteProcessException, IOException {
    for (int j = 100; j < 200; j++) {
      TSRecord record = new TSRecord(deviceId, j);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      dataRegion.insert(buildInsertRowNodeByTSRecord(record));
    }
    TsFileResource tsFileResource = dataRegion.getTsFileManager().getTsFileList(true).get(0);
    TsFileProcessor tsFileProcessor = tsFileResource.getProcessor();
    tsFileProcessor.getFlushingMemTable().addLast(tsFileProcessor.getWorkMemTable());

    MeasurementPath path = new MeasurementPath("root.vehicle.d0.s0");
    DeleteDataNode deleteDataNode1 =
        new DeleteDataNode(new PlanNodeId("1"), Collections.singletonList(path), 50, 99);
    deleteDataNode1.setSearchIndex(0);
    MeasurementPath path2 = new MeasurementPath("root.vehicle.d200.s0");
    DeleteDataNode deleteDataNode2 =
        new DeleteDataNode(new PlanNodeId("2"), Collections.singletonList(path2), 50, 70);
    deleteDataNode2.setSearchIndex(0);
    // delete data which is not in flushing memtable
    dataRegion.deleteByDevice(new MeasurementPath("root.vehicle.d0.s0"), deleteDataNode1);
    dataRegion.deleteByDevice(new MeasurementPath("root.vehicle.d0.s0"), deleteDataNode2);

    DeleteDataNode deleteDataNode3 =
        new DeleteDataNode(new PlanNodeId("3"), Collections.singletonList(path), 50, 100);
    deleteDataNode3.setSearchIndex(0);
    DeleteDataNode deleteDataNode4 =
        new DeleteDataNode(new PlanNodeId("4"), Collections.singletonList(path), 50, 150);
    deleteDataNode4.setSearchIndex(0);
    DeleteDataNode deleteDataNode5 =
        new DeleteDataNode(new PlanNodeId("5"), Collections.singletonList(path), 100, 190);
    deleteDataNode5.setSearchIndex(0);
    // delete data which is in flushing memtable
    dataRegion.deleteByDevice(new MeasurementPath("root.vehicle.d0.s0"), deleteDataNode3);
    dataRegion.deleteByDevice(new MeasurementPath("root.vehicle.d0.s0"), deleteDataNode4);
    dataRegion.deleteByDevice(new MeasurementPath("root.vehicle.d0.s0"), deleteDataNode4);

    dataRegion.syncCloseAllWorkingTsFileProcessors();
    assertTrue(tsFileResource.anyModFileExists());
    assertEquals(3, tsFileResource.getAllModEntries().size());
  }

  @Test
  public void testDeleteDataInUnSeqFlushingMemtable()
      throws IllegalPathException, WriteProcessException, IOException {
    for (int j = 100; j < 200; j++) {
      TSRecord record = new TSRecord(deviceId, j);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      dataRegion.insert(buildInsertRowNodeByTSRecord(record));
    }
    TsFileResource tsFileResource = dataRegion.getTsFileManager().getTsFileList(true).get(0);

    MeasurementPath path = new MeasurementPath("root.vehicle.d0.s0");
    DeleteDataNode deleteDataNode1 =
        new DeleteDataNode(new PlanNodeId("1"), Collections.singletonList(path), 50, 99);
    deleteDataNode1.setSearchIndex(0);
    MeasurementPath path2 = new MeasurementPath("root.vehicle.d200.s0");
    DeleteDataNode deleteDataNode2 =
        new DeleteDataNode(new PlanNodeId("2"), Collections.singletonList(path2), 50, 70);
    deleteDataNode2.setSearchIndex(0);
    // delete data which is not in work memtable
    dataRegion.deleteByDevice(new MeasurementPath("root.vehicle.d0.s0"), deleteDataNode1);
    dataRegion.deleteByDevice(new MeasurementPath("root.vehicle.d0.s0"), deleteDataNode2);

    DeleteDataNode deleteDataNode3 =
        new DeleteDataNode(new PlanNodeId("3"), Collections.singletonList(path), 50, 100);
    deleteDataNode3.setSearchIndex(0);
    DeleteDataNode deleteDataNode4 =
        new DeleteDataNode(new PlanNodeId("4"), Collections.singletonList(path), 50, 150);
    deleteDataNode4.setSearchIndex(0);
    DeleteDataNode deleteDataNode5 =
        new DeleteDataNode(new PlanNodeId("5"), Collections.singletonList(path), 100, 190);
    deleteDataNode5.setSearchIndex(0);
    // delete data which is in work memtable
    dataRegion.deleteByDevice(new MeasurementPath("root.vehicle.d0.s0"), deleteDataNode3);
    dataRegion.deleteByDevice(new MeasurementPath("root.vehicle.d0.s0"), deleteDataNode4);
    dataRegion.deleteByDevice(new MeasurementPath("root.vehicle.d0.s0"), deleteDataNode5);

    dataRegion.syncCloseAllWorkingTsFileProcessors();
    Assert.assertFalse(tsFileResource.anyModFileExists());

    // insert unseq data points
    for (int j = 50; j < 100; j++) {
      TSRecord record = new TSRecord(deviceId, j);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      dataRegion.insert(buildInsertRowNodeByTSRecord(record));
    }

    DeleteDataNode deleteDataNode6 =
        new DeleteDataNode(new PlanNodeId("6"), Collections.singletonList(path), 200, 299);
    deleteDataNode6.setSearchIndex(0);
    // delete data which is not in work memtable
    dataRegion.deleteByDevice(new MeasurementPath("root.vehicle.d0.s0"), deleteDataNode6);
    dataRegion.deleteByDevice(new MeasurementPath("root.vehicle.d200.s0"), deleteDataNode2);

    DeleteDataNode deleteDataNode7 =
        new DeleteDataNode(new PlanNodeId("7"), Collections.singletonList(path), 80, 85);
    deleteDataNode7.setSearchIndex(0);
    // delete data which is in work memtable
    dataRegion.deleteByDevice(new MeasurementPath("root.vehicle.d0.s0"), deleteDataNode7);

    Assert.assertFalse(tsFileResource.anyModFileExists());

    tsFileResource = dataRegion.getTsFileManager().getTsFileList(false).get(0);
    TsFileProcessor tsFileProcessor = tsFileResource.getProcessor();
    tsFileProcessor.getFlushingMemTable().addLast(tsFileProcessor.getWorkMemTable());

    DeleteDataNode deleteDataNode8 =
        new DeleteDataNode(new PlanNodeId("8"), Collections.singletonList(path), 0, 49);
    deleteDataNode8.setSearchIndex(0);
    DeleteDataNode deleteDataNode9 =
        new DeleteDataNode(new PlanNodeId("9"), Collections.singletonList(path), 100, 200);
    deleteDataNode9.setSearchIndex(0);
    // delete data which is not in flushing memtable
    dataRegion.deleteByDevice(new MeasurementPath("root.vehicle.d0.s0"), deleteDataNode8);
    dataRegion.deleteByDevice(new MeasurementPath("root.vehicle.d0.s0"), deleteDataNode9);
    dataRegion.deleteByDevice(new MeasurementPath("root.vehicle.d200.s0"), deleteDataNode2);

    DeleteDataNode deleteDataNode10 =
        new DeleteDataNode(new PlanNodeId("10"), Collections.singletonList(path2), 25, 50);
    deleteDataNode10.setSearchIndex(0);
    DeleteDataNode deleteDataNode11 =
        new DeleteDataNode(new PlanNodeId("11"), Collections.singletonList(path2), 50, 80);
    deleteDataNode11.setSearchIndex(0);
    DeleteDataNode deleteDataNode12 =
        new DeleteDataNode(new PlanNodeId("12"), Collections.singletonList(path2), 99, 150);
    deleteDataNode12.setSearchIndex(0);
    // delete data which is in flushing memtable
    dataRegion.deleteByDevice(new MeasurementPath("root.vehicle.d0.s0"), deleteDataNode10);
    dataRegion.deleteByDevice(new MeasurementPath("root.vehicle.d0.s0"), deleteDataNode11);
    dataRegion.deleteByDevice(new MeasurementPath("root.vehicle.d0.s0"), deleteDataNode12);

    dataRegion.syncCloseAllWorkingTsFileProcessors();
    assertTrue(tsFileResource.anyModFileExists());
    assertEquals(3, tsFileResource.getAllModEntries().size());
  }

  @Test
  public void testDeleteDataInSeqWorkingMemtable()
      throws IllegalPathException, WriteProcessException, IOException {
    for (int j = 100; j < 200; j++) {
      TSRecord record = new TSRecord("root.vehicle.d0", j);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      dataRegion.insert(buildInsertRowNodeByTSRecord(record));
    }
    for (int j = 100; j < 200; j++) {
      TSRecord record = new TSRecord("root.vehicle.d199", j);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      dataRegion.insert(buildInsertRowNodeByTSRecord(record));
    }
    TsFileResource tsFileResource = dataRegion.getTsFileManager().getTsFileList(true).get(0);

    MeasurementPath path = new MeasurementPath("root.vehicle.d0.s0");
    DeleteDataNode deleteDataNode1 =
        new DeleteDataNode(new PlanNodeId("1"), Collections.singletonList(path), 50, 99);
    deleteDataNode1.setSearchIndex(0);
    MeasurementPath path2 = new MeasurementPath("root.vehicle.d200.s0");
    DeleteDataNode deleteDataNode2 =
        new DeleteDataNode(new PlanNodeId("2"), Collections.singletonList(path2), 50, 70);
    deleteDataNode2.setSearchIndex(0);
    // delete data which is not in working memtable
    dataRegion.deleteByDevice(new MeasurementPath("root.vehicle.d0.s0"), deleteDataNode1);
    dataRegion.deleteByDevice(new MeasurementPath("root.vehicle.d200.s0"), deleteDataNode2);

    MeasurementPath path3 = new MeasurementPath("root.vehicle.d199.*");
    DeleteDataNode deleteDataNode3 =
        new DeleteDataNode(new PlanNodeId("3"), Collections.singletonList(path3), 50, 500);
    deleteDataNode3.setSearchIndex(0);
    // delete data which is in working memtable
    dataRegion.deleteByDevice(new MeasurementPath("root.vehicle.d199.*"), deleteDataNode3);

    dataRegion.syncCloseAllWorkingTsFileProcessors();
    Assert.assertFalse(tsFileResource.anyModFileExists());
    Assert.assertFalse(
        tsFileResource.getDevices().contains(Factory.DEFAULT_FACTORY.create("root.vehicle.d199")));
  }

  @Test
  public void testFlushingEmptyMemtable()
      throws IllegalPathException, WriteProcessException, IOException {
    for (int j = 100; j < 200; j++) {
      TSRecord record = new TSRecord(deviceId, j);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      dataRegion.insert(buildInsertRowNodeByTSRecord(record));
    }
    TsFileResource tsFileResource = dataRegion.getTsFileManager().getTsFileList(true).get(0);

    MeasurementPath path = new MeasurementPath("root.vehicle.d0.s0");
    DeleteDataNode deleteDataNode =
        new DeleteDataNode(new PlanNodeId("1"), Collections.singletonList(path), 100, 200);
    deleteDataNode.setSearchIndex(0);
    // delete all data which is in flushing memtable
    dataRegion.deleteByDevice(new MeasurementPath("root.vehicle.d0.s0"), deleteDataNode);

    dataRegion.syncCloseAllWorkingTsFileProcessors();
    Assert.assertFalse(tsFileResource.getTsFile().exists());
    Assert.assertFalse(tsFileResource.anyModFileExists());
    Assert.assertFalse(dataRegion.getTsFileManager().contains(tsFileResource, true));
    Assert.assertFalse(
        dataRegion.getWorkSequenceTsFileProcessors().contains(tsFileResource.getProcessor()));
  }

  public static class DummyDataRegion extends DataRegion {

    public DummyDataRegion(String systemInfoDir, String storageGroupName)
        throws DataRegionException {
      super(systemInfoDir, "0", new DirectFlushPolicy(), storageGroupName);
    }
  }

  // -- test for deleting data directly
  // -- delete data and file only when:
  // 1. tsfile is closed
  // 2. tsfile is not compating
  // 3. tsfile's start time and end time must be a subinterval
  // of the given time range.

  @Test
  public void testDeleteDataDirectlySeqWriteModsOrDeleteFiles()
      throws IllegalPathException, WriteProcessException, IOException {
    for (int j = 100; j < 200; j++) {
      TSRecord record = new TSRecord(deviceId, j);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      dataRegion.insert(buildInsertRowNodeByTSRecord(record));
    }

    TsFileResource tsFileResource = dataRegion.getTsFileManager().getTsFileList(true).get(0);
    // delete data in work mem, no mods.
    MeasurementPath path = new MeasurementPath("root.vehicle.d0.**");
    DeleteDataNode deleteDataNode1 =
        new DeleteDataNode(new PlanNodeId("1"), Collections.singletonList(path), 50, 100);
    deleteDataNode1.setSearchIndex(0);
    dataRegion.deleteDataDirectly(new MeasurementPath("root.vehicle.d0.**"), deleteDataNode1);
    assertTrue(tsFileResource.getTsFile().exists());
    Assert.assertFalse(tsFileResource.anyModFileExists());

    dataRegion.syncCloseAllWorkingTsFileProcessors();

    // delete data in closed file, but time not match
    DeleteDataNode deleteDataNode2 =
        new DeleteDataNode(new PlanNodeId("2"), Collections.singletonList(path), 100, 120);
    deleteDataNode2.setSearchIndex(0);
    dataRegion.deleteDataDirectly(new MeasurementPath("root.vehicle.d0.**"), deleteDataNode2);
    assertTrue(tsFileResource.getTsFile().exists());
    assertTrue(tsFileResource.anyModFileExists());

    // delete data in closed file, and time all match
    DeleteDataNode deleteDataNode3 =
        new DeleteDataNode(new PlanNodeId("3"), Collections.singletonList(path), 100, 199);
    deleteDataNode3.setSearchIndex(0);
    dataRegion.deleteDataDirectly(new MeasurementPath("root.vehicle.d0.**"), deleteDataNode3);
    Assert.assertFalse(tsFileResource.getTsFile().exists());
    Assert.assertFalse(tsFileResource.anyModFileExists());
  }

  @Test
  public void testDeleteDataDirectlyUnseqWriteModsOrDeleteFiles()
      throws IllegalPathException, WriteProcessException, IOException {
    for (int j = 100; j < 200; j++) {
      TSRecord record = new TSRecord(deviceId, j);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      dataRegion.insert(buildInsertRowNodeByTSRecord(record));
    }
    TsFileResource tsFileResourceSeq = dataRegion.getTsFileManager().getTsFileList(true).get(0);
    dataRegion.syncCloseAllWorkingTsFileProcessors();
    for (int j = 30; j < 100; j++) {
      TSRecord record = new TSRecord(deviceId, j);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      dataRegion.insert(buildInsertRowNodeByTSRecord(record));
    }

    dataRegion.syncCloseWorkingTsFileProcessors(true);
    TsFileResource tsFileResourceUnSeq = dataRegion.getTsFileManager().getTsFileList(false).get(0);

    assertTrue(tsFileResourceSeq.getTsFile().exists());
    assertTrue(tsFileResourceUnSeq.getTsFile().exists());

    // already closed, will have a mods file.
    MeasurementPath path = new MeasurementPath("root.vehicle.d0.**");
    DeleteDataNode deleteDataNode1 =
        new DeleteDataNode(new PlanNodeId("1"), Collections.singletonList(path), 40, 60);
    deleteDataNode1.setSearchIndex(0);
    dataRegion.deleteDataDirectly(new MeasurementPath("root.vehicle.d0.**"), deleteDataNode1);
    // not close yet, just delete in memory.
    DeleteDataNode deleteDataNode2 =
        new DeleteDataNode(new PlanNodeId("2"), Collections.singletonList(path), 140, 160);
    deleteDataNode2.setSearchIndex(0);
    dataRegion.deleteDataDirectly(new MeasurementPath("root.vehicle.d0.**"), deleteDataNode2);

    // delete data in mem table, there is no mods
    assertTrue(tsFileResourceSeq.getTsFile().exists());
    assertTrue(tsFileResourceUnSeq.getTsFile().exists());
    assertTrue(tsFileResourceSeq.anyModFileExists());
    Assert.assertFalse(tsFileResourceUnSeq.anyModFileExists());
    dataRegion.syncCloseAllWorkingTsFileProcessors();

    DeleteDataNode deleteDataNode3 =
        new DeleteDataNode(new PlanNodeId("3"), Collections.singletonList(path), 40, 80);
    deleteDataNode3.setSearchIndex(0);
    dataRegion.deleteDataDirectly(new MeasurementPath("root.vehicle.d0.**"), deleteDataNode3);
    assertTrue(tsFileResourceUnSeq.getTsFile().exists());
    assertTrue(tsFileResourceUnSeq.anyModFileExists());

    // seq file and unseq file have data file and mod file now,
    // this deletion will remove data file and mod file.
    DeleteDataNode deleteDataNode4 =
        new DeleteDataNode(new PlanNodeId("4"), Collections.singletonList(path), 30, 100);
    deleteDataNode4.setSearchIndex(0);
    DeleteDataNode deleteDataNode5 =
        new DeleteDataNode(new PlanNodeId("5"), Collections.singletonList(path), 100, 199);
    deleteDataNode5.setSearchIndex(0);
    dataRegion.deleteDataDirectly(new MeasurementPath("root.vehicle.d0.**"), deleteDataNode4);
    dataRegion.deleteDataDirectly(new MeasurementPath("root.vehicle.d0.**"), deleteDataNode5);

    Assert.assertFalse(tsFileResourceSeq.getTsFile().exists());
    Assert.assertFalse(tsFileResourceUnSeq.getTsFile().exists());
    Assert.assertFalse(tsFileResourceSeq.anyModFileExists());
    Assert.assertFalse(tsFileResourceUnSeq.anyModFileExists());
  }

  @Test
  public void testSchemaEvolution()
      throws WriteProcessException, QueryProcessException, IOException {
    String[] measurements = {"tag1", "s1", "s2"};
    MeasurementSchema[] measurementSchemas = {
      new MeasurementSchema("tag1", TSDataType.STRING),
      new MeasurementSchema("s1", TSDataType.INT64),
      new MeasurementSchema("s2", TSDataType.DOUBLE)
    };
    RelationalInsertRowNode insertRowNode =
        new RelationalInsertRowNode(
            new PlanNodeId(""),
            new PartialPath(new String[] {"table1"}),
            true,
            measurements,
            new TSDataType[] {TSDataType.STRING, TSDataType.INT64, TSDataType.DOUBLE},
            measurementSchemas,
            10,
            new Object[] {new Binary("tag1".getBytes(StandardCharsets.UTF_8)), 1L, 1.0},
            false,
            new TsTableColumnCategory[] {
              TsTableColumnCategory.TAG, TsTableColumnCategory.FIELD, TsTableColumnCategory.FIELD
            });
    dataRegion.insert(insertRowNode);

    // table1 -> table2
    dataRegion.applySchemaEvolution(Collections.singletonList(new TableRename("table1", "table2")));

    // cannot query with the old name
    IDeviceID deviceID1 = Factory.DEFAULT_FACTORY.create(new String[] {"table1", "tag1"});
    List<IFullPath> fullPaths =
        Arrays.asList(
            new AlignedFullPath(
                deviceID1, Arrays.asList(measurements), Arrays.asList(measurementSchemas)));
    QueryDataSource dataSource =
        dataRegion.query(
            fullPaths,
            deviceID1,
            new QueryContext(),
            null,
            Collections.singletonList(0L),
            Long.MAX_VALUE);
    assertTrue(dataSource.getSeqResources().isEmpty());

    // can query with the new name
    IDeviceID deviceID2 = Factory.DEFAULT_FACTORY.create(new String[] {"table2", "tag1"});
    fullPaths =
        Arrays.asList(
            new AlignedFullPath(
                deviceID2, Arrays.asList(measurements), Arrays.asList(measurementSchemas)));
    dataSource =
        dataRegion.query(
            fullPaths,
            deviceID2,
            new QueryContext(),
            null,
            Collections.singletonList(0L),
            Long.MAX_VALUE);
    assertEquals(1, dataSource.getSeqResources().size());

    DataNodeTableCache.getInstance()
        .preUpdateTable(dataRegion.getDatabaseName(), StatementTestUtils.genTsTable(1), null);
    DataNodeTableCache.getInstance()
        .commitUpdateTable(dataRegion.getDatabaseName(), StatementTestUtils.tableName(1), null);

    // write again with table1
    insertRowNode =
        new RelationalInsertRowNode(
            new PlanNodeId(""),
            new PartialPath(new String[] {"table1"}),
            true,
            measurements,
            new TSDataType[] {TSDataType.STRING, TSDataType.INT64, TSDataType.DOUBLE},
            measurementSchemas,
            10,
            new Object[] {new Binary("tag1".getBytes(StandardCharsets.UTF_8)), 1L, 1.0},
            false,
            new TsTableColumnCategory[] {
              TsTableColumnCategory.TAG, TsTableColumnCategory.FIELD, TsTableColumnCategory.FIELD
            });
    dataRegion.insert(insertRowNode);

    // can query with table1
    fullPaths =
        Arrays.asList(
            new AlignedFullPath(
                deviceID1, Arrays.asList(measurements), Arrays.asList(measurementSchemas)));
    dataSource =
        dataRegion.query(
            fullPaths,
            deviceID1,
            new QueryContext(),
            null,
            Collections.singletonList(0L),
            Long.MAX_VALUE);
    assertEquals(1, dataSource.getUnseqResources().size());

    // can query with table2
    fullPaths =
        Arrays.asList(
            new AlignedFullPath(
                deviceID2, Arrays.asList(measurements), Arrays.asList(measurementSchemas)));
    dataSource =
        dataRegion.query(
            fullPaths,
            deviceID2,
            new QueryContext(),
            null,
            Collections.singletonList(0L),
            Long.MAX_VALUE);
    assertEquals(1, dataSource.getSeqResources().size());
  }

  @Test
  public void testSchemaEvolutionWithPartialDeletion() throws WriteProcessException, IOException {
    String[] measurements = {"tag1", "s1", "s2"};
    MeasurementSchema[] measurementSchemas = {
      new MeasurementSchema("tag1", TSDataType.STRING),
      new MeasurementSchema("s1", TSDataType.INT64),
      new MeasurementSchema("s2", TSDataType.DOUBLE)
    };
    RelationalInsertRowNode insertRowNode =
        new RelationalInsertRowNode(
            new PlanNodeId(""),
            new PartialPath(new String[] {"table1"}),
            true,
            measurements,
            new TSDataType[] {TSDataType.STRING, TSDataType.INT64, TSDataType.DOUBLE},
            measurementSchemas,
            10,
            new Object[] {new Binary("tag1".getBytes(StandardCharsets.UTF_8)), 1L, 1.0},
            false,
            new TsTableColumnCategory[] {
              TsTableColumnCategory.TAG, TsTableColumnCategory.FIELD, TsTableColumnCategory.FIELD
            });
    dataRegion.insert(insertRowNode);
    insertRowNode.setTime(20);
    dataRegion.insert(insertRowNode);

    // table1 -> table2
    dataRegion.applySchemaEvolution(Collections.singletonList(new TableRename("table1", "table2")));
    // s1 -> s3
    dataRegion.applySchemaEvolution(
        Collections.singletonList(new ColumnRename("table2", "s1", "s3", null)));

    // delete with table2
    TableDeletionEntry tableDeletionEntry =
        new TableDeletionEntry(new DeletionPredicate("table2"), new TimeRange(0, 15));
    RelationalDeleteDataNode relationalDeleteDataNode =
        new RelationalDeleteDataNode(
            new PlanNodeId(""), tableDeletionEntry, dataRegion.getDatabaseName());
    dataRegion.deleteByTable(relationalDeleteDataNode);
    // delete with s3
    tableDeletionEntry =
        new TableDeletionEntry(
            new DeletionPredicate("table2", new NOP(), Collections.singletonList("s3")),
            new TimeRange(0, 15));
    relationalDeleteDataNode =
        new RelationalDeleteDataNode(
            new PlanNodeId(""), tableDeletionEntry, dataRegion.getDatabaseName());
    dataRegion.deleteByTable(relationalDeleteDataNode);
    // delete with table1
    tableDeletionEntry =
        new TableDeletionEntry(new DeletionPredicate("table1"), new TimeRange(0, 15));
    relationalDeleteDataNode =
        new RelationalDeleteDataNode(
            new PlanNodeId(""), tableDeletionEntry, dataRegion.getDatabaseName());
    dataRegion.deleteByTable(relationalDeleteDataNode);
    // delete with s1
    tableDeletionEntry =
        new TableDeletionEntry(
            new DeletionPredicate("table2", new NOP(), Collections.singletonList("s1")),
            new TimeRange(0, 15));
    relationalDeleteDataNode =
        new RelationalDeleteDataNode(
            new PlanNodeId(""), tableDeletionEntry, dataRegion.getDatabaseName());
    dataRegion.deleteByTable(relationalDeleteDataNode);

    List<TsFileResource> sequenceFileList = dataRegion.getSequenceFileList();
    assertEquals(1, sequenceFileList.size());
    ModIterator modEntryIterator = sequenceFileList.get(0).getModEntryIterator();
    ModEntry next = modEntryIterator.next();
    // the table2 modification should be rewritten to table1
    assertEquals("table1", ((TableDeletionEntry) next).getTableName());
    next = modEntryIterator.next();
    // the s3 modification should be rewritten to s1
    assertEquals(
        Collections.singletonList("s1"),
        ((TableDeletionEntry) next).getPredicate().getMeasurementNames());
    next = modEntryIterator.next();
    // the table1 modification should be skipped
    // the s1 modification should be rewritten to empty
    assertEquals(
        Collections.singletonList(""),
        ((TableDeletionEntry) next).getPredicate().getMeasurementNames());
    assertFalse(modEntryIterator.hasNext());
  }

  @Test
  public void testSchemaEvolutionWithFullDeletion() throws WriteProcessException, IOException {
    String[] measurements = {"tag1", "s1", "s2"};
    MeasurementSchema[] measurementSchemas = {
      new MeasurementSchema("tag1", TSDataType.STRING),
      new MeasurementSchema("s1", TSDataType.INT64),
      new MeasurementSchema("s2", TSDataType.DOUBLE)
    };
    RelationalInsertRowNode insertRowNode =
        new RelationalInsertRowNode(
            new PlanNodeId(""),
            new PartialPath(new String[] {"table1"}),
            true,
            measurements,
            new TSDataType[] {TSDataType.STRING, TSDataType.INT64, TSDataType.DOUBLE},
            measurementSchemas,
            10,
            new Object[] {new Binary("tag1".getBytes(StandardCharsets.UTF_8)), 1L, 1.0},
            false,
            new TsTableColumnCategory[] {
              TsTableColumnCategory.TAG, TsTableColumnCategory.FIELD, TsTableColumnCategory.FIELD
            });
    dataRegion.insert(insertRowNode);
    insertRowNode.setTime(20);
    dataRegion.insert(insertRowNode);

    // table1 -> table2
    dataRegion.applySchemaEvolution(Collections.singletonList(new TableRename("table1", "table2")));
    // s1 -> s3
    dataRegion.applySchemaEvolution(
        Collections.singletonList(new ColumnRename("table2", "s1", "s3", null)));

    // delete with table1
    TableDeletionEntry tableDeletionEntry =
        new TableDeletionEntry(new DeletionPredicate("table1"), new TimeRange(0, 30));
    RelationalDeleteDataNode relationalDeleteDataNode =
        new RelationalDeleteDataNode(
            new PlanNodeId(""), tableDeletionEntry, dataRegion.getDatabaseName());
    dataRegion.deleteByTable(relationalDeleteDataNode);
    // nothing should be deleted
    List<TsFileResource> sequenceFileList = dataRegion.getSequenceFileList();
    assertEquals(1, sequenceFileList.size());
    ModIterator modEntryIterator = sequenceFileList.get(0).getModEntryIterator();
    assertFalse(modEntryIterator.hasNext());

    // delete with table2
    tableDeletionEntry =
        new TableDeletionEntry(new DeletionPredicate("table2"), new TimeRange(0, 30));
    relationalDeleteDataNode =
        new RelationalDeleteDataNode(
            new PlanNodeId(""), tableDeletionEntry, dataRegion.getDatabaseName());
    dataRegion.deleteByTable(relationalDeleteDataNode);
    // the file should be deleted
    sequenceFileList = dataRegion.getSequenceFileList();
    assertEquals(0, sequenceFileList.size());
  }
}
