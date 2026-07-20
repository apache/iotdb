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
package org.apache.iotdb.db.storageengine.dataregion.memtable;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.path.AlignedFullPath;
import org.apache.iotdb.commons.path.NonAlignedFullPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowNode;
import org.apache.iotdb.db.storageengine.dataregion.DataRegionInfo;
import org.apache.iotdb.db.storageengine.dataregion.DataRegionTest;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.rescon.memory.PrimitiveArrayManager;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.external.commons.io.FileUtils;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.read.reader.IPointReader;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.DataPoint;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
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
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static junit.framework.TestCase.assertTrue;
import static org.apache.iotdb.db.storageengine.dataregion.DataRegionTest.buildInsertRowNodeByTSRecord;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@SuppressWarnings("OptionalGetWithoutIsPresent")
public class TsFileProcessorTest {

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private TsFileProcessor processor;
  private final String storageGroup = "root.vehicle";
  private DataRegionInfo sgInfo;
  private final String filePath = TestConstant.getTestTsFilePath("root.vehicle", 0, 0, 0);
  private final String deviceId = "root.vehicle.d0";
  private final String measurementId = "s0";
  private final TSDataType dataType = TSDataType.INT32;
  private final TSEncoding encoding = TSEncoding.RLE;
  private final Map<String, String> props = Collections.emptyMap();
  private QueryContext context;
  private final String systemDir = TestConstant.OUTPUT_DATA_DIR.concat("info");
  private long defaultTargetChunkPointNum;
  private long defaultTargetChunkSize;
  private static final Logger logger = LoggerFactory.getLogger(TsFileProcessorTest.class);

  public TsFileProcessorTest() {}

  @Before
  public void setUp() throws DataRegionException {
    File file = new File(filePath);
    if (!file.getParentFile().exists()) {
      Assert.assertTrue(file.getParentFile().mkdirs());
    }
    defaultTargetChunkPointNum = config.getTargetChunkPointNum();
    defaultTargetChunkSize = config.getTargetChunkSize();
    EnvironmentUtils.envSetUp();
    sgInfo = new DataRegionInfo(new DataRegionTest.DummyDataRegion(systemDir, storageGroup));
    context = EnvironmentUtils.TEST_QUERY_CONTEXT;
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    EnvironmentUtils.cleanDir(TestConstant.OUTPUT_DATA_DIR);
    File file = new File(filePath);
    File resource = new File(filePath + ".resource");
    try {
      FileUtils.delete(file);
      if (resource.exists()) {
        FileUtils.delete(resource);
      }
    } catch (IOException ignored) {
    }
    config.setTargetChunkPointNum(defaultTargetChunkPointNum);
    config.setTargetChunkSize(defaultTargetChunkSize);
  }

  @Test
  public void testWriteAndFlush()
      throws IOException, WriteProcessException, MetadataException, ExecutionException {
    logger.info("testWriteAndFlush begin..");
    processor =
        new TsFileProcessor(
            storageGroup,
            SystemFileFactory.INSTANCE.getFile(filePath),
            sgInfo,
            this::closeTsFileProcessor,
            (tsFileProcessor, updateMap, systemFlushTime) -> {},
            true);

    TsFileProcessorInfo tsFileProcessorInfo = new TsFileProcessorInfo(sgInfo);
    processor.setTsFileProcessorInfo(tsFileProcessorInfo);
    this.sgInfo.initTsFileProcessorInfo(processor);
    SystemInfo.getInstance().reportStorageGroupStatus(sgInfo, processor);
    List<TsFileResource> tsfileResourcesForQuery = new ArrayList<>();
    NonAlignedFullPath fullPath =
        new NonAlignedFullPath(
            IDeviceID.Factory.DEFAULT_FACTORY.create(deviceId),
            new MeasurementSchema(
                measurementId, dataType, encoding, CompressionType.UNCOMPRESSED, props));
    processor.query(Collections.singletonList(fullPath), context, tsfileResourcesForQuery, null);
    assertTrue(tsfileResourcesForQuery.isEmpty());

    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(deviceId, i);
      record.addTuple(DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i)));
      processor.insert(buildInsertRowNodeByTSRecord(record), new long[5]);
    }

    // query data in memory
    tsfileResourcesForQuery.clear();
    processor.query(Collections.singletonList(fullPath), context, tsfileResourcesForQuery, null);

    TsFileResource tsFileResource = tsfileResourcesForQuery.get(0);
    assertFalse(tsFileResource.getReadOnlyMemChunk(fullPath).isEmpty());
    List<ReadOnlyMemChunk> memChunks = tsFileResource.getReadOnlyMemChunk(fullPath);
    for (ReadOnlyMemChunk chunk : memChunks) {
      IPointReader iterator = chunk.getPointReader();
      for (int num = 1; num <= 100; num++) {
        iterator.hasNextTimeValuePair();
        TimeValuePair timeValuePair = iterator.nextTimeValuePair();
        assertEquals(num, timeValuePair.getTimestamp());
        assertEquals(num, timeValuePair.getValue().getInt());
      }
    }

    // flush synchronously
    processor.syncClose();

    try (TsFileSequenceReader reader = new TsFileSequenceReader(filePath);
        TsFileReader readTsFile = new TsFileReader(reader)) {
      QueryExpression queryExpression =
          QueryExpression.create(
              Collections.singletonList(new Path(deviceId, measurementId, false)), null);
      QueryDataSet queryDataSet = readTsFile.query(queryExpression);
      int num = 1;
      while (queryDataSet.hasNext()) {
        RowRecord rowRecord = queryDataSet.next();
        assertEquals(num, rowRecord.getTimestamp());
        assertEquals(num, rowRecord.getFields().get(0).getIntV());
        num++;
      }
      assertEquals(101, num);
    }
  }

  @Test
  public void testFlushMultiChunks()
      throws IOException, WriteProcessException, MetadataException, ExecutionException {
    config.setTargetChunkPointNum(40);
    processor =
        new TsFileProcessor(
            storageGroup,
            SystemFileFactory.INSTANCE.getFile(filePath),
            sgInfo,
            this::closeTsFileProcessor,
            (tsFileProcessor, updateMap, systemFlushTime) -> {},
            true);

    TsFileProcessorInfo tsFileProcessorInfo = new TsFileProcessorInfo(sgInfo);
    processor.setTsFileProcessorInfo(tsFileProcessorInfo);
    this.sgInfo.initTsFileProcessorInfo(processor);
    SystemInfo.getInstance().reportStorageGroupStatus(sgInfo, processor);
    List<TsFileResource> tsfileResourcesForQuery = new ArrayList<>();
    NonAlignedFullPath fullPath =
        new NonAlignedFullPath(
            IDeviceID.Factory.DEFAULT_FACTORY.create(deviceId),
            new MeasurementSchema(
                measurementId, dataType, encoding, CompressionType.UNCOMPRESSED, props));
    processor.query(Collections.singletonList(fullPath), context, tsfileResourcesForQuery, null);
    assertTrue(tsfileResourcesForQuery.isEmpty());

    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(deviceId, i);
      record.addTuple(DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i)));
      processor.insert(buildInsertRowNodeByTSRecord(record), new long[5]);
    }

    // query data in memory
    tsfileResourcesForQuery.clear();
    processor.query(Collections.singletonList(fullPath), context, tsfileResourcesForQuery, null);

    TsFileResource tsFileResource = tsfileResourcesForQuery.get(0);
    assertFalse(tsFileResource.getReadOnlyMemChunk(fullPath).isEmpty());
    List<ReadOnlyMemChunk> memChunks = tsFileResource.getReadOnlyMemChunk(fullPath);
    for (ReadOnlyMemChunk chunk : memChunks) {
      IPointReader iterator = chunk.getPointReader();
      for (int num = 1; num <= 100; num++) {
        iterator.hasNextTimeValuePair();
        TimeValuePair timeValuePair = iterator.nextTimeValuePair();
        assertEquals(num, timeValuePair.getTimestamp());
        assertEquals(num, timeValuePair.getValue().getInt());
      }
    }

    // flush synchronously
    processor.syncClose();

    try (TsFileSequenceReader reader = new TsFileSequenceReader(filePath);
        TsFileReader readTsFile = new TsFileReader(reader)) {
      QueryExpression queryExpression =
          QueryExpression.create(
              Collections.singletonList(new Path(deviceId, measurementId, false)), null);
      QueryDataSet queryDataSet = readTsFile.query(queryExpression);
      int num = 1;
      while (queryDataSet.hasNext()) {
        RowRecord rowRecord = queryDataSet.next();
        assertEquals(num, rowRecord.getTimestamp());
        assertEquals(num, rowRecord.getFields().get(0).getIntV());
        num++;
      }
      assertEquals(101, num);
    }
  }

  @Test
  public void testFlushMultiBinaryChunks()
      throws IOException, WriteProcessException, MetadataException, ExecutionException {
    config.setTargetChunkSize(1536L);
    processor =
        new TsFileProcessor(
            storageGroup,
            SystemFileFactory.INSTANCE.getFile(filePath),
            sgInfo,
            this::closeTsFileProcessor,
            (tsFileProcessor, updateMap, systemFlushTime) -> {},
            true);

    TsFileProcessorInfo tsFileProcessorInfo = new TsFileProcessorInfo(sgInfo);
    processor.setTsFileProcessorInfo(tsFileProcessorInfo);
    this.sgInfo.initTsFileProcessorInfo(processor);
    SystemInfo.getInstance().reportStorageGroupStatus(sgInfo, processor);
    List<TsFileResource> tsfileResourcesForQuery = new ArrayList<>();
    NonAlignedFullPath fullPath =
        new NonAlignedFullPath(
            IDeviceID.Factory.DEFAULT_FACTORY.create(deviceId),
            new MeasurementSchema(
                measurementId,
                TSDataType.TEXT,
                TSEncoding.PLAIN,
                CompressionType.UNCOMPRESSED,
                props));
    processor.query(Collections.singletonList(fullPath), context, tsfileResourcesForQuery, null);
    assertTrue(tsfileResourcesForQuery.isEmpty());

    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(deviceId, i);
      record.addTuple(DataPoint.getDataPoint(TSDataType.TEXT, measurementId, String.valueOf(i)));
      processor.insert(buildInsertRowNodeByTSRecord(record), new long[5]);
    }

    // query data in memory
    tsfileResourcesForQuery.clear();
    processor.query(Collections.singletonList(fullPath), context, tsfileResourcesForQuery, null);

    TsFileResource tsFileResource = tsfileResourcesForQuery.get(0);
    assertFalse(tsFileResource.getReadOnlyMemChunk(fullPath).isEmpty());
    List<ReadOnlyMemChunk> memChunks = tsFileResource.getReadOnlyMemChunk(fullPath);
    for (ReadOnlyMemChunk chunk : memChunks) {
      IPointReader iterator = chunk.getPointReader();
      for (int num = 1; num <= 100; num++) {
        iterator.hasNextTimeValuePair();
        TimeValuePair timeValuePair = iterator.nextTimeValuePair();
        assertEquals(num, timeValuePair.getTimestamp());
        assertEquals(String.valueOf(num), timeValuePair.getValue().getStringValue());
      }
    }

    // flush synchronously
    processor.syncClose();

    try (TsFileSequenceReader reader = new TsFileSequenceReader(filePath);
        TsFileReader readTsFile = new TsFileReader(reader)) {
      QueryExpression queryExpression =
          QueryExpression.create(
              Collections.singletonList(new Path(deviceId, measurementId, false)), null);
      QueryDataSet queryDataSet = readTsFile.query(queryExpression);
      int num = 1;
      while (queryDataSet.hasNext()) {
        RowRecord rowRecord = queryDataSet.next();
        assertEquals(num, rowRecord.getTimestamp());
        assertEquals(String.valueOf(num), rowRecord.getFields().get(0).getStringValue());
        num++;
      }
      assertEquals(101, num);
    }
  }

  @Test
  public void testFlushMultiAlignedChunks()
      throws IOException, WriteProcessException, MetadataException, ExecutionException {
    config.setTargetChunkPointNum(40);
    processor =
        new TsFileProcessor(
            storageGroup,
            SystemFileFactory.INSTANCE.getFile(filePath),
            sgInfo,
            this::closeTsFileProcessor,
            (tsFileProcessor, updateMap, systemFlushTime) -> {},
            true);

    TsFileProcessorInfo tsFileProcessorInfo = new TsFileProcessorInfo(sgInfo);
    processor.setTsFileProcessorInfo(tsFileProcessorInfo);
    this.sgInfo.initTsFileProcessorInfo(processor);
    SystemInfo.getInstance().reportStorageGroupStatus(sgInfo, processor);
    List<TsFileResource> tsfileResourcesForQuery = new ArrayList<>();
    AlignedFullPath fullPath =
        new AlignedFullPath(
            IDeviceID.Factory.DEFAULT_FACTORY.create(deviceId),
            Collections.singletonList(measurementId),
            Collections.singletonList(
                new MeasurementSchema(
                    measurementId, dataType, encoding, CompressionType.UNCOMPRESSED, props)));
    processor.query(Collections.singletonList(fullPath), context, tsfileResourcesForQuery, null);
    assertTrue(tsfileResourcesForQuery.isEmpty());

    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(deviceId, i);
      record.addTuple(DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i)));
      InsertRowNode rowNode = buildInsertRowNodeByTSRecord(record);
      rowNode.setAligned(true);
      processor.insert(rowNode, new long[5]);
    }

    // add another point time = 1, value = 1
    TSRecord record = new TSRecord(deviceId, 1);
    record.addTuple(DataPoint.getDataPoint(dataType, measurementId, "1"));
    InsertRowNode rowNode = buildInsertRowNodeByTSRecord(record);
    rowNode.setAligned(true);
    processor.insert(rowNode, new long[5]);

    // query data in memory
    tsfileResourcesForQuery.clear();
    processor.query(Collections.singletonList(fullPath), context, tsfileResourcesForQuery, null);

    TsFileResource tsFileResource = tsfileResourcesForQuery.get(0);
    assertFalse(tsFileResource.getReadOnlyMemChunk(fullPath).isEmpty());
    List<ReadOnlyMemChunk> memChunks = tsFileResource.getReadOnlyMemChunk(fullPath);
    for (ReadOnlyMemChunk chunk : memChunks) {
      IPointReader iterator = chunk.getPointReader();
      for (int num = 1; num <= 100; num++) {
        iterator.hasNextTimeValuePair();
        TimeValuePair timeValuePair = iterator.nextTimeValuePair();
        assertEquals(num, timeValuePair.getTimestamp());
        assertEquals(num, timeValuePair.getValue().getVector()[0].getInt());
      }
    }

    // flush synchronously
    processor.syncClose();

    try (TsFileSequenceReader reader = new TsFileSequenceReader(filePath);
        TsFileReader readTsFile = new TsFileReader(reader)) {
      QueryExpression queryExpression =
          QueryExpression.create(
              Collections.singletonList(new Path(deviceId, measurementId, false)), null);
      QueryDataSet queryDataSet = readTsFile.query(queryExpression);
      int num = 1;
      while (queryDataSet.hasNext()) {
        RowRecord rowRecord = queryDataSet.next();
        assertEquals(num, rowRecord.getTimestamp());
        assertEquals(num, rowRecord.getFields().get(0).getIntV());
        num++;
      }
      assertEquals(101, num);
    }
  }

  @Test
  public void testFlushMultiAlignedBinaryChunks()
      throws IOException, WriteProcessException, MetadataException, ExecutionException {
    config.setTargetChunkSize(1536L);
    processor =
        new TsFileProcessor(
            storageGroup,
            SystemFileFactory.INSTANCE.getFile(filePath),
            sgInfo,
            this::closeTsFileProcessor,
            (tsFileProcessor, updateMap, systemFlushTime) -> {},
            true);

    TsFileProcessorInfo tsFileProcessorInfo = new TsFileProcessorInfo(sgInfo);
    processor.setTsFileProcessorInfo(tsFileProcessorInfo);
    this.sgInfo.initTsFileProcessorInfo(processor);
    SystemInfo.getInstance().reportStorageGroupStatus(sgInfo, processor);
    List<TsFileResource> tsfileResourcesForQuery = new ArrayList<>();
    AlignedFullPath fullPath =
        new AlignedFullPath(
            IDeviceID.Factory.DEFAULT_FACTORY.create(deviceId),
            Collections.singletonList(measurementId),
            Collections.singletonList(
                new MeasurementSchema(
                    measurementId,
                    TSDataType.TEXT,
                    encoding,
                    CompressionType.UNCOMPRESSED,
                    props)));
    processor.query(Collections.singletonList(fullPath), context, tsfileResourcesForQuery, null);
    assertTrue(tsfileResourcesForQuery.isEmpty());

    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(deviceId, i);
      record.addTuple(DataPoint.getDataPoint(TSDataType.TEXT, measurementId, String.valueOf(i)));
      InsertRowNode rowNode = buildInsertRowNodeByTSRecord(record);
      rowNode.setAligned(true);
      processor.insert(rowNode, new long[5]);
    }
    // add another point time = 1, value = "1"
    TSRecord record = new TSRecord(deviceId, 1);
    record.addTuple(DataPoint.getDataPoint(TSDataType.TEXT, measurementId, "1"));
    InsertRowNode rowNode = buildInsertRowNodeByTSRecord(record);
    rowNode.setAligned(true);
    processor.insert(rowNode, new long[5]);

    // query data in memory
    tsfileResourcesForQuery.clear();
    processor.query(Collections.singletonList(fullPath), context, tsfileResourcesForQuery, null);

    TsFileResource tsFileResource = tsfileResourcesForQuery.get(0);
    assertFalse(tsFileResource.getReadOnlyMemChunk(fullPath).isEmpty());
    List<ReadOnlyMemChunk> memChunks = tsFileResource.getReadOnlyMemChunk(fullPath);
    for (ReadOnlyMemChunk chunk : memChunks) {
      IPointReader iterator = chunk.getPointReader();
      for (int num = 1; num <= 100; num++) {
        iterator.hasNextTimeValuePair();
        TimeValuePair timeValuePair = iterator.nextTimeValuePair();
        assertEquals(num, timeValuePair.getTimestamp());
        assertEquals(String.valueOf(num), timeValuePair.getValue().getVector()[0].getStringValue());
      }
    }

    // flush synchronously
    processor.syncClose();

    try (TsFileSequenceReader reader = new TsFileSequenceReader(filePath);
        TsFileReader readTsFile = new TsFileReader(reader)) {
      QueryExpression queryExpression =
          QueryExpression.create(
              Collections.singletonList(new Path(deviceId, measurementId, false)), null);
      QueryDataSet queryDataSet = readTsFile.query(queryExpression);
      int num = 1;
      while (queryDataSet.hasNext()) {
        RowRecord rowRecord = queryDataSet.next();
        assertEquals(num, rowRecord.getTimestamp());
        assertEquals(String.valueOf(num), rowRecord.getFields().get(0).getStringValue());
        num++;
      }
      assertEquals(101, num);
    }
  }

  @Test
  public void alignedTvListRamCostTest()
      throws MetadataException, WriteProcessException, IOException {
    processor =
        new TsFileProcessor(
            storageGroup,
            SystemFileFactory.INSTANCE.getFile(filePath),
            sgInfo,
            this::closeTsFileProcessor,
            (tsFileProcessor, updateMap, systemFlushTime) -> {},
            true);
    TsFileProcessorInfo tsFileProcessorInfo = new TsFileProcessorInfo(sgInfo);
    processor.setTsFileProcessorInfo(tsFileProcessorInfo);
    this.sgInfo.initTsFileProcessorInfo(processor);
    SystemInfo.getInstance().reportStorageGroupStatus(sgInfo, processor);
    // Test Tablet
    processor.insertTablet(
        genInsertTableNode(0, true),
        Collections.singletonList(new int[] {0, 10}),
        new TSStatus[10],
        true,
        new long[5]);
    IMemTable memTable = processor.getWorkMemTable();
    Assert.assertEquals(1776552, memTable.getTVListsRamCost());
    processor.insertTablet(
        genInsertTableNode(100, true),
        Collections.singletonList(new int[] {0, 10}),
        new TSStatus[10],
        true,
        new long[5]);
    Assert.assertEquals(1776552, memTable.getTVListsRamCost());
    processor.insertTablet(
        genInsertTableNode(200, true),
        Collections.singletonList(new int[] {0, 10}),
        new TSStatus[10],
        true,
        new long[5]);
    Assert.assertEquals(1776552, memTable.getTVListsRamCost());
    Assert.assertEquals(90000, memTable.getTotalPointsNum());
    Assert.assertEquals(720360, memTable.memSize());
    // Test records
    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(deviceId, i);
      record.addTuple(DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i)));
      processor.insert(buildInsertRowNodeByTSRecord(record), new long[5]);
    }
    Assert.assertEquals(1778168, memTable.getTVListsRamCost());
    Assert.assertEquals(90100, memTable.getTotalPointsNum());
    Assert.assertEquals(721560, memTable.memSize());
  }

  @Test
  public void alignedTabletKeepsFailedStatusesAndCountsWrittenRows()
      throws MetadataException, WriteProcessException, IOException, IllegalPathException {
    final int rowCount = PrimitiveArrayManager.ARRAY_SIZE + 2;
    final List<int[]> rangeList = Collections.singletonList(new int[] {0, rowCount - 1});

    final TsFileProcessor expectedProcessor = newTestProcessor(filePath + ".expected");
    final TSStatus[] expectedResults = new TSStatus[rowCount];
    Arrays.fill(expectedResults, RpcUtils.SUCCESS_STATUS);
    expectedProcessor.insertTablet(
        genSingleMeasurementTablet(rowCount, true), rangeList, expectedResults, false, new long[5]);

    final TsFileProcessor actualProcessor = newTestProcessor(filePath + ".actual");
    final TSStatus[] actualResults = new TSStatus[rowCount];
    Arrays.fill(actualResults, RpcUtils.SUCCESS_STATUS);
    final int failedIndex = rowCount - 2;
    actualResults[failedIndex] = RpcUtils.getStatus(TSStatusCode.OUT_OF_TTL, "failed row");
    actualProcessor.insertTablet(
        genSingleMeasurementTablet(rowCount, true), rangeList, actualResults, false, new long[5]);

    Assert.assertEquals(
        expectedProcessor.getWorkMemTable().getTVListsRamCost(),
        actualProcessor.getWorkMemTable().getTVListsRamCost());
    Assert.assertEquals(
        TSStatusCode.OUT_OF_TTL.getStatusCode(), actualResults[failedIndex].getCode());
  }

  @Test
  public void alignedTvListRamCostTest2()
      throws MetadataException, WriteProcessException, IOException {
    processor =
        new TsFileProcessor(
            storageGroup,
            SystemFileFactory.INSTANCE.getFile(filePath),
            sgInfo,
            this::closeTsFileProcessor,
            (tsFileProcessor, updateMap, systemFlushTime) -> {},
            true);
    TsFileProcessorInfo tsFileProcessorInfo = new TsFileProcessorInfo(sgInfo);
    processor.setTsFileProcessorInfo(tsFileProcessorInfo);
    this.sgInfo.initTsFileProcessorInfo(processor);
    SystemInfo.getInstance().reportStorageGroupStatus(sgInfo, processor);
    // Test Tablet
    processor.insertTablet(
        genInsertTableNode(0, true),
        Collections.singletonList(new int[] {0, 10}),
        new TSStatus[10],
        true,
        new long[5]);
    IMemTable memTable = processor.getWorkMemTable();
    Assert.assertEquals(1776552, memTable.getTVListsRamCost());
    processor.insertTablet(
        genInsertTableNodeFors3000ToS6000(0, true),
        Collections.singletonList(new int[] {0, 10}),
        new TSStatus[10],
        true,
        new long[5]);
    Assert.assertEquals(3552552, memTable.getTVListsRamCost());
    processor.insertTablet(
        genInsertTableNode(100, true),
        Collections.singletonList(new int[] {0, 10}),
        new TSStatus[10],
        true,
        new long[5]);
    Assert.assertEquals(3552552, memTable.getTVListsRamCost());
    processor.insertTablet(
        genInsertTableNodeFors3000ToS6000(100, true),
        Collections.singletonList(new int[] {0, 10}),
        new TSStatus[10],
        true,
        new long[5]);
    Assert.assertEquals(3552552, memTable.getTVListsRamCost());
    processor.insertTablet(
        genInsertTableNode(200, true),
        Collections.singletonList(new int[] {0, 10}),
        new TSStatus[10],
        true,
        new long[5]);
    Assert.assertEquals(3552552, memTable.getTVListsRamCost());
    processor.insertTablet(
        genInsertTableNodeFors3000ToS6000(200, true),
        Collections.singletonList(new int[] {0, 10}),
        new TSStatus[10],
        true,
        new long[5]);
    Assert.assertEquals(3552552, memTable.getTVListsRamCost());
    processor.insertTablet(
        genInsertTableNode(300, true),
        Collections.singletonList(new int[] {0, 10}),
        new TSStatus[10],
        true,
        new long[5]);
    Assert.assertEquals(7105104, memTable.getTVListsRamCost());
    processor.insertTablet(
        genInsertTableNodeFors3000ToS6000(300, true),
        Collections.singletonList(new int[] {0, 10}),
        new TSStatus[10],
        true,
        new long[5]);
    Assert.assertEquals(7105104, memTable.getTVListsRamCost());

    Assert.assertEquals(240000, memTable.getTotalPointsNum());
    Assert.assertEquals(1920960, memTable.memSize());
    // Test records
    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(deviceId, i);
      record.addTuple(DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i)));
      processor.insert(buildInsertRowNodeByTSRecord(record), new long[5]);
    }
    Assert.assertEquals(7106720, memTable.getTVListsRamCost());
    // Test records
    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(deviceId, i);
      record.addTuple(DataPoint.getDataPoint(dataType, "s1", String.valueOf(i)));
      processor.insert(buildInsertRowNodeByTSRecord(record), new long[5]);
    }
    Assert.assertEquals(7108336, memTable.getTVListsRamCost());
    Assert.assertEquals(240200, memTable.getTotalPointsNum());
    Assert.assertEquals(1923360, memTable.memSize());
  }

  @Test
  public void nonAlignedTvListRamCostTest()
      throws MetadataException, WriteProcessException, IOException {
    processor =
        new TsFileProcessor(
            storageGroup,
            SystemFileFactory.INSTANCE.getFile(filePath),
            sgInfo,
            this::closeTsFileProcessor,
            (tsFileProcessor, updateMap, systemFlushTime) -> {},
            true);
    TsFileProcessorInfo tsFileProcessorInfo = new TsFileProcessorInfo(sgInfo);
    processor.setTsFileProcessorInfo(tsFileProcessorInfo);
    this.sgInfo.initTsFileProcessorInfo(processor);
    SystemInfo.getInstance().reportStorageGroupStatus(sgInfo, processor);
    // Test tablet
    processor.insertTablet(
        genInsertTableNode(0, false),
        Collections.singletonList(new int[] {0, 10}),
        new TSStatus[10],
        true,
        new long[5]);
    IMemTable memTable = processor.getWorkMemTable();
    Assert.assertEquals(3192000, memTable.getTVListsRamCost());
    processor.insertTablet(
        genInsertTableNode(100, false),
        Collections.singletonList(new int[] {0, 10}),
        new TSStatus[10],
        true,
        new long[5]);
    Assert.assertEquals(3192000, memTable.getTVListsRamCost());
    processor.insertTablet(
        genInsertTableNode(200, false),
        Collections.singletonList(new int[] {0, 10}),
        new TSStatus[10],
        true,
        new long[5]);
    Assert.assertEquals(3192000, memTable.getTVListsRamCost());
    Assert.assertEquals(90000, memTable.getTotalPointsNum());
    Assert.assertEquals(1440000, memTable.memSize());
    // Test records
    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(deviceId, i);
      record.addTuple(DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i)));
      processor.insert(buildInsertRowNodeByTSRecord(record), new long[5]);
    }
    Assert.assertEquals(3193616, memTable.getTVListsRamCost());
    Assert.assertEquals(90100, memTable.getTotalPointsNum());
    Assert.assertEquals(1441200, memTable.memSize());
  }

  @Test
  public void testRamCostInsertSameNonAlignedDataBy2Ways()
      throws MetadataException, WriteProcessException, IOException {
    TsFileProcessor processor1 =
        new TsFileProcessor(
            storageGroup,
            SystemFileFactory.INSTANCE.getFile(filePath),
            sgInfo,
            this::closeTsFileProcessor,
            (tsFileProcessor, updateMap, systemFlushTime) -> {},
            true);
    TsFileProcessorInfo tsFileProcessorInfo1 = new TsFileProcessorInfo(sgInfo);
    processor1.setTsFileProcessorInfo(tsFileProcessorInfo1);
    this.sgInfo.initTsFileProcessorInfo(processor1);
    SystemInfo.getInstance().reportStorageGroupStatus(sgInfo, processor1);
    // insert 100 rows by insertRow
    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(deviceId, i);
      record.addTuple(DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i)));
      processor1.insert(buildInsertRowNodeByTSRecord(record), new long[5]);
    }
    IMemTable memTable1 = processor1.getWorkMemTable();

    TsFileProcessor processor2 =
        new TsFileProcessor(
            storageGroup,
            SystemFileFactory.INSTANCE.getFile(filePath),
            sgInfo,
            this::closeTsFileProcessor,
            (tsFileProcessor, updateMap, systemFlushTime) -> {},
            true);
    TsFileProcessorInfo tsFileProcessorInfo2 = new TsFileProcessorInfo(sgInfo);
    processor2.setTsFileProcessorInfo(tsFileProcessorInfo2);
    this.sgInfo.initTsFileProcessorInfo(processor2);
    SystemInfo.getInstance().reportStorageGroupStatus(sgInfo, processor2);
    InsertRowsNode insertRowsNode = new InsertRowsNode(new PlanNodeId(""));
    // insert 100 rows by insertRows
    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(deviceId, i);
      record.addTuple(DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i)));
      insertRowsNode.addOneInsertRowNode(buildInsertRowNodeByTSRecord(record), i - 1);
    }
    processor2.insertRows(insertRowsNode, new long[5]);
    IMemTable memTable2 = processor2.getWorkMemTable();

    Assert.assertEquals(memTable1.getTVListsRamCost(), memTable2.getTVListsRamCost());
    Assert.assertEquals(memTable1.getTotalPointsNum(), memTable2.getTotalPointsNum());
    Assert.assertEquals(memTable1.memSize(), memTable2.memSize());

    // insert more rows by insertRow
    TSRecord record = new TSRecord(deviceId, 101);
    record.addTuple(DataPoint.getDataPoint(dataType, measurementId, "1"));
    InsertRowNode insertRowNode1 = buildInsertRowNodeByTSRecord(record);
    processor1.insert(insertRowNode1, new long[5]);
    record = new TSRecord(deviceId, 101);
    record.addTuple(DataPoint.getDataPoint(dataType, "s99", "1"));
    InsertRowNode insertRowNode2 = buildInsertRowNodeByTSRecord(record);
    processor1.insert(insertRowNode2, new long[5]);
    record = new TSRecord(deviceId, 102);
    record.addTuple(DataPoint.getDataPoint(dataType, "s99", "1"));
    InsertRowNode insertRowNode3 = buildInsertRowNodeByTSRecord(record);
    processor1.insert(insertRowNode3, new long[5]);
    record = new TSRecord("root.vehicle.d2", 102);
    record.addTuple(DataPoint.getDataPoint(dataType, measurementId, "1"));
    InsertRowNode insertRowNode4 = buildInsertRowNodeByTSRecord(record);
    processor1.insert(insertRowNode4, new long[5]);

    // insert more rows by insertRows
    insertRowsNode = new InsertRowsNode(new PlanNodeId(""));
    insertRowsNode.addOneInsertRowNode(insertRowNode1, 0);
    insertRowsNode.addOneInsertRowNode(insertRowNode2, 1);
    insertRowsNode.addOneInsertRowNode(insertRowNode3, 2);
    insertRowsNode.addOneInsertRowNode(insertRowNode4, 3);
    processor2.insertRows(insertRowsNode, new long[5]);

    Assert.assertEquals(memTable1.getTVListsRamCost(), memTable2.getTVListsRamCost());
    Assert.assertEquals(memTable1.getTotalPointsNum(), memTable2.getTotalPointsNum());
    Assert.assertEquals(memTable1.memSize(), memTable2.memSize());

    // Insert rows with all column null
    insertRowsNode = new InsertRowsNode(new PlanNodeId(""));
    insertRowNode1.setDataTypes(new TSDataType[1]);
    insertRowNode1.setMeasurements(new String[1]);
    insertRowNode1.setValues(new String[1]);
    insertRowsNode.addOneInsertRowNode(insertRowNode1, 0);
    processor2.insertRows(insertRowsNode, new long[5]);

    processor1.insert(insertRowNode1, new long[5]);
    Assert.assertEquals(memTable1.getTVListsRamCost(), memTable2.getTVListsRamCost());
    Assert.assertEquals(memTable1.getTotalPointsNum(), memTable2.getTotalPointsNum());
    Assert.assertEquals(memTable1.memSize(), memTable2.memSize());
  }

  @Test
  public void testRamCostInsertSameAlignedDataBy2Ways()
      throws MetadataException, WriteProcessException, IOException {
    TsFileProcessor processor1 =
        new TsFileProcessor(
            storageGroup,
            SystemFileFactory.INSTANCE.getFile(filePath),
            sgInfo,
            this::closeTsFileProcessor,
            (tsFileProcessor, updateMap, systemFlushTime) -> {},
            true);
    TsFileProcessorInfo tsFileProcessorInfo1 = new TsFileProcessorInfo(sgInfo);
    processor1.setTsFileProcessorInfo(tsFileProcessorInfo1);
    this.sgInfo.initTsFileProcessorInfo(processor1);
    SystemInfo.getInstance().reportStorageGroupStatus(sgInfo, processor1);
    // insert 100 rows by insertRow
    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(deviceId, i);
      record.addTuple(DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i)));
      InsertRowNode node = buildInsertRowNodeByTSRecord(record);
      node.setAligned(true);
      processor1.insert(node, new long[5]);
    }
    IMemTable memTable1 = processor1.getWorkMemTable();

    TsFileProcessor processor2 =
        new TsFileProcessor(
            storageGroup,
            SystemFileFactory.INSTANCE.getFile(filePath),
            sgInfo,
            this::closeTsFileProcessor,
            (tsFileProcessor, updateMap, systemFlushTime) -> {},
            true);
    TsFileProcessorInfo tsFileProcessorInfo2 = new TsFileProcessorInfo(sgInfo);
    processor2.setTsFileProcessorInfo(tsFileProcessorInfo2);
    this.sgInfo.initTsFileProcessorInfo(processor2);
    SystemInfo.getInstance().reportStorageGroupStatus(sgInfo, processor2);
    InsertRowsNode insertRowsNode = new InsertRowsNode(new PlanNodeId(""));
    insertRowsNode.setAligned(true);
    // insert 100 rows by insertRows
    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(deviceId, i);
      record.addTuple(DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i)));
      InsertRowNode node = buildInsertRowNodeByTSRecord(record);
      node.setAligned(true);
      insertRowsNode.addOneInsertRowNode(node, i - 1);
    }
    processor2.insertRows(insertRowsNode, new long[5]);
    IMemTable memTable2 = processor2.getWorkMemTable();

    Assert.assertEquals(memTable1.getTVListsRamCost(), memTable2.getTVListsRamCost());
    Assert.assertEquals(memTable1.getTotalPointsNum(), memTable2.getTotalPointsNum());
    Assert.assertEquals(memTable1.memSize(), memTable2.memSize());

    // insert more rows by insertRow
    TSRecord record = new TSRecord(deviceId, 101);
    record.addTuple(DataPoint.getDataPoint(dataType, measurementId, "1"));
    InsertRowNode insertRowNode1 = buildInsertRowNodeByTSRecord(record);
    insertRowNode1.setAligned(true);
    processor1.insert(insertRowNode1, new long[5]);
    record = new TSRecord(deviceId, 101);
    record.addTuple(DataPoint.getDataPoint(dataType, "s99", "1"));
    InsertRowNode insertRowNode2 = buildInsertRowNodeByTSRecord(record);
    insertRowNode2.setAligned(true);
    processor1.insert(insertRowNode2, new long[5]);
    record = new TSRecord(deviceId, 102);
    record.addTuple(DataPoint.getDataPoint(dataType, "s99", "1"));
    InsertRowNode insertRowNode3 = buildInsertRowNodeByTSRecord(record);
    insertRowNode3.setAligned(true);
    processor1.insert(insertRowNode3, new long[5]);
    record = new TSRecord("root.vehicle.d2", 102);
    record.addTuple(DataPoint.getDataPoint(dataType, measurementId, "1"));
    InsertRowNode insertRowNode4 = buildInsertRowNodeByTSRecord(record);
    insertRowNode4.setAligned(true);
    processor1.insert(insertRowNode4, new long[5]);

    // insert more rows by insertRows
    insertRowsNode = new InsertRowsNode(new PlanNodeId(""));
    insertRowsNode.setAligned(true);
    insertRowsNode.addOneInsertRowNode(insertRowNode1, 0);
    insertRowsNode.addOneInsertRowNode(insertRowNode2, 1);
    insertRowsNode.addOneInsertRowNode(insertRowNode3, 2);
    insertRowsNode.addOneInsertRowNode(insertRowNode4, 3);
    processor2.insertRows(insertRowsNode, new long[5]);

    Assert.assertEquals(memTable1.getTVListsRamCost(), memTable2.getTVListsRamCost());
    Assert.assertEquals(memTable1.getTotalPointsNum(), memTable2.getTotalPointsNum());
    Assert.assertEquals(memTable1.memSize(), memTable2.memSize());

    // Insert rows with all column null
    insertRowsNode = new InsertRowsNode(new PlanNodeId(""));
    insertRowNode1.setDataTypes(new TSDataType[1]);
    insertRowNode1.setMeasurements(new String[1]);
    insertRowNode1.setValues(new String[1]);
    insertRowsNode.addOneInsertRowNode(insertRowNode1, 0);
    insertRowsNode.setAligned(true);
    processor2.insertRows(insertRowsNode, new long[5]);

    processor1.insert(insertRowNode1, new long[5]);
    Assert.assertEquals(memTable1.getTVListsRamCost(), memTable2.getTVListsRamCost());
    Assert.assertEquals(memTable1.getTotalPointsNum(), memTable2.getTotalPointsNum());
    Assert.assertEquals(memTable1.memSize(), memTable2.memSize());
  }

  @Test
  public void testAlignedRamCostIgnoresRelationalNonFieldAndNullFieldColumns()
      throws IllegalPathException, WriteProcessException, IOException {
    TsFileProcessor relationalProcessor =
        new TsFileProcessor(
            storageGroup,
            SystemFileFactory.INSTANCE.getFile(filePath),
            sgInfo,
            this::closeTsFileProcessor,
            (tsFileProcessor, updateMap, systemFlushTime) -> {},
            true);
    TsFileProcessorInfo relationalInfo = new TsFileProcessorInfo(sgInfo);
    relationalProcessor.setTsFileProcessorInfo(relationalInfo);
    this.sgInfo.initTsFileProcessorInfo(relationalProcessor);
    SystemInfo.getInstance().reportStorageGroupStatus(sgInfo, relationalProcessor);

    RelationalInsertRowNode relationalNode =
        new RelationalInsertRowNode(
            new PlanNodeId("relational"),
            new PartialPath("table1", false),
            true,
            new String[] {"tag1", "attr1", "s1", "s2"},
            new TSDataType[] {TSDataType.TEXT, TSDataType.TEXT, TSDataType.INT32, TSDataType.INT64},
            new MeasurementSchema[] {
              new MeasurementSchema("tag1", TSDataType.TEXT),
              new MeasurementSchema("attr1", TSDataType.TEXT),
              new MeasurementSchema("s1", TSDataType.INT32),
              new MeasurementSchema("s2", TSDataType.INT64)
            },
            1L,
            new Object[] {
              new Binary("tag-value".getBytes(StandardCharsets.UTF_8)),
              new Binary("attr-value".getBytes(StandardCharsets.UTF_8)),
              1,
              null
            },
            false,
            new TsTableColumnCategory[] {
              TsTableColumnCategory.TAG,
              TsTableColumnCategory.ATTRIBUTE,
              TsTableColumnCategory.FIELD,
              TsTableColumnCategory.FIELD
            });
    relationalProcessor.insert(relationalNode, new long[5]);

    TsFileProcessor fieldOnlyProcessor =
        new TsFileProcessor(
            storageGroup,
            SystemFileFactory.INSTANCE.getFile(filePath),
            sgInfo,
            this::closeTsFileProcessor,
            (tsFileProcessor, updateMap, systemFlushTime) -> {},
            true);
    TsFileProcessorInfo fieldOnlyInfo = new TsFileProcessorInfo(sgInfo);
    fieldOnlyProcessor.setTsFileProcessorInfo(fieldOnlyInfo);
    this.sgInfo.initTsFileProcessorInfo(fieldOnlyProcessor);
    SystemInfo.getInstance().reportStorageGroupStatus(sgInfo, fieldOnlyProcessor);

    InsertRowNode fieldOnlyNode =
        new InsertRowNode(
            new PlanNodeId("field-only"),
            new PartialPath(deviceId),
            true,
            new String[] {"s1", "s2"},
            new TSDataType[] {TSDataType.INT32, TSDataType.INT64},
            new MeasurementSchema[] {
              new MeasurementSchema("s1", TSDataType.INT32),
              new MeasurementSchema("s2", TSDataType.INT64)
            },
            1L,
            new Object[] {1, null},
            false);
    fieldOnlyProcessor.insert(fieldOnlyNode, new long[5]);

    IMemTable relationalMemTable = relationalProcessor.getWorkMemTable();
    IMemTable fieldOnlyMemTable = fieldOnlyProcessor.getWorkMemTable();
    Assert.assertEquals(
        fieldOnlyMemTable.getTVListsRamCost(), relationalMemTable.getTVListsRamCost());
    Assert.assertEquals(fieldOnlyInfo.getMemCost(), relationalInfo.getMemCost());
    Assert.assertEquals(fieldOnlyMemTable.memSize(), relationalMemTable.memSize());
    Assert.assertEquals(1, relationalMemTable.getTotalPointsNum());
    Assert.assertEquals(1, relationalMemTable.getSeriesNumber());
  }

  @Test
  public void testNonAlignedRamCostIgnoresRelationalNonFieldAndNullFieldColumns()
      throws IllegalPathException, WriteProcessException, IOException {
    TsFileProcessor relationalProcessor =
        new TsFileProcessor(
            storageGroup,
            SystemFileFactory.INSTANCE.getFile(filePath),
            sgInfo,
            this::closeTsFileProcessor,
            (tsFileProcessor, updateMap, systemFlushTime) -> {},
            true);
    TsFileProcessorInfo relationalInfo = new TsFileProcessorInfo(sgInfo);
    relationalProcessor.setTsFileProcessorInfo(relationalInfo);
    this.sgInfo.initTsFileProcessorInfo(relationalProcessor);
    SystemInfo.getInstance().reportStorageGroupStatus(sgInfo, relationalProcessor);

    RelationalInsertRowNode relationalNode =
        new RelationalInsertRowNode(
            new PlanNodeId("relational"),
            new PartialPath("table1", false),
            false,
            new String[] {"tag1", "attr1", "s1", "s2"},
            new TSDataType[] {TSDataType.TEXT, TSDataType.TEXT, TSDataType.INT32, TSDataType.INT64},
            new MeasurementSchema[] {
              new MeasurementSchema("tag1", TSDataType.TEXT),
              new MeasurementSchema("attr1", TSDataType.TEXT),
              new MeasurementSchema("s1", TSDataType.INT32),
              new MeasurementSchema("s2", TSDataType.INT64)
            },
            1L,
            new Object[] {
              new Binary("tag-value".getBytes(StandardCharsets.UTF_8)),
              new Binary("attr-value".getBytes(StandardCharsets.UTF_8)),
              1,
              null
            },
            false,
            new TsTableColumnCategory[] {
              TsTableColumnCategory.TAG,
              TsTableColumnCategory.ATTRIBUTE,
              TsTableColumnCategory.FIELD,
              TsTableColumnCategory.FIELD
            });
    relationalProcessor.insert(relationalNode, new long[5]);

    TsFileProcessor fieldOnlyProcessor =
        new TsFileProcessor(
            storageGroup,
            SystemFileFactory.INSTANCE.getFile(filePath),
            sgInfo,
            this::closeTsFileProcessor,
            (tsFileProcessor, updateMap, systemFlushTime) -> {},
            true);
    TsFileProcessorInfo fieldOnlyInfo = new TsFileProcessorInfo(sgInfo);
    fieldOnlyProcessor.setTsFileProcessorInfo(fieldOnlyInfo);
    this.sgInfo.initTsFileProcessorInfo(fieldOnlyProcessor);
    SystemInfo.getInstance().reportStorageGroupStatus(sgInfo, fieldOnlyProcessor);

    InsertRowNode fieldOnlyNode =
        new InsertRowNode(
            new PlanNodeId("field-only"),
            new PartialPath(deviceId),
            false,
            new String[] {"s1", "s2"},
            new TSDataType[] {TSDataType.INT32, TSDataType.INT64},
            new MeasurementSchema[] {
              new MeasurementSchema("s1", TSDataType.INT32),
              new MeasurementSchema("s2", TSDataType.INT64)
            },
            1L,
            new Object[] {1, null},
            false);
    fieldOnlyProcessor.insert(fieldOnlyNode, new long[5]);

    IMemTable relationalMemTable = relationalProcessor.getWorkMemTable();
    IMemTable fieldOnlyMemTable = fieldOnlyProcessor.getWorkMemTable();
    Assert.assertEquals(
        fieldOnlyMemTable.getTVListsRamCost(), relationalMemTable.getTVListsRamCost());
    Assert.assertEquals(fieldOnlyInfo.getMemCost(), relationalInfo.getMemCost());
    Assert.assertEquals(fieldOnlyMemTable.memSize(), relationalMemTable.memSize());
    Assert.assertEquals(1, relationalMemTable.getTotalPointsNum());
    Assert.assertEquals(1, relationalMemTable.getSeriesNumber());
  }

  @Test
  public void testRamCostInsertSameDataBy2Ways()
      throws MetadataException, WriteProcessException, IOException {
    TsFileProcessor processor1 =
        new TsFileProcessor(
            storageGroup,
            SystemFileFactory.INSTANCE.getFile(filePath),
            sgInfo,
            this::closeTsFileProcessor,
            (tsFileProcessor, updateMap, systemFlushTime) -> {},
            true);
    TsFileProcessorInfo tsFileProcessorInfo1 = new TsFileProcessorInfo(sgInfo);
    processor1.setTsFileProcessorInfo(tsFileProcessorInfo1);
    this.sgInfo.initTsFileProcessorInfo(processor1);
    SystemInfo.getInstance().reportStorageGroupStatus(sgInfo, processor1);
    // insert 100 rows (50 aligned, 50 non-aligned) by insertRow
    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(i <= 50 ? deviceId : "root.vehicle.d2", i);
      record.addTuple(DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i)));
      InsertRowNode node = buildInsertRowNodeByTSRecord(record);
      if (i <= 50) {
        node.setAligned(true);
      }
      processor1.insert(node, new long[5]);
    }
    IMemTable memTable1 = processor1.getWorkMemTable();

    TsFileProcessor processor2 =
        new TsFileProcessor(
            storageGroup,
            SystemFileFactory.INSTANCE.getFile(filePath),
            sgInfo,
            this::closeTsFileProcessor,
            (tsFileProcessor, updateMap, systemFlushTime) -> {},
            true);
    TsFileProcessorInfo tsFileProcessorInfo2 = new TsFileProcessorInfo(sgInfo);
    processor2.setTsFileProcessorInfo(tsFileProcessorInfo2);
    this.sgInfo.initTsFileProcessorInfo(processor2);
    SystemInfo.getInstance().reportStorageGroupStatus(sgInfo, processor2);
    InsertRowsNode insertRowsNode = new InsertRowsNode(new PlanNodeId(""));
    insertRowsNode.setAligned(true);
    // insert 100 rows (50 aligned, 50 non-aligned) by insertRows
    insertRowsNode.setMixingAlignment(true);
    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(i <= 50 ? deviceId : "root.vehicle.d2", i);
      record.addTuple(DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i)));
      InsertRowNode node = buildInsertRowNodeByTSRecord(record);
      if (i <= 50) {
        node.setAligned(true);
      }
      insertRowsNode.addOneInsertRowNode(node, i - 1);
    }
    processor2.insertRows(insertRowsNode, new long[5]);
    IMemTable memTable2 = processor2.getWorkMemTable();

    Assert.assertEquals(memTable1.getTVListsRamCost(), memTable2.getTVListsRamCost());
    Assert.assertEquals(memTable1.getTotalPointsNum(), memTable2.getTotalPointsNum());
    Assert.assertEquals(memTable1.memSize(), memTable2.memSize());
  }

  @Test
  public void testRamCostInsertSameDataBy2Ways2()
      throws MetadataException, WriteProcessException, IOException {
    TsFileProcessor processor1 =
        new TsFileProcessor(
            storageGroup,
            SystemFileFactory.INSTANCE.getFile(filePath),
            sgInfo,
            this::closeTsFileProcessor,
            (tsFileProcessor, updateMap, systemFlushTime) -> {},
            true);
    TsFileProcessorInfo tsFileProcessorInfo1 = new TsFileProcessorInfo(sgInfo);
    processor1.setTsFileProcessorInfo(tsFileProcessorInfo1);
    this.sgInfo.initTsFileProcessorInfo(processor1);
    SystemInfo.getInstance().reportStorageGroupStatus(sgInfo, processor1);
    // insert 100 rows (50 aligned, 50 non-aligned) by insertRow
    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(i <= 50 ? deviceId : "root.vehicle.d2", i);
      record.addTuple(DataPoint.getDataPoint(dataType, "s" + i, String.valueOf(i)));
      InsertRowNode node = buildInsertRowNodeByTSRecord(record);
      node.setAligned(true);
      if (i <= 50) {
        node.setAligned(true);
      }
      processor1.insert(node, new long[5]);
    }
    IMemTable memTable1 = processor1.getWorkMemTable();

    TsFileProcessor processor2 =
        new TsFileProcessor(
            storageGroup,
            SystemFileFactory.INSTANCE.getFile(filePath),
            sgInfo,
            this::closeTsFileProcessor,
            (tsFileProcessor, updateMap, systemFlushTime) -> {},
            true);
    TsFileProcessorInfo tsFileProcessorInfo2 = new TsFileProcessorInfo(sgInfo);
    processor2.setTsFileProcessorInfo(tsFileProcessorInfo2);
    this.sgInfo.initTsFileProcessorInfo(processor2);
    SystemInfo.getInstance().reportStorageGroupStatus(sgInfo, processor2);
    InsertRowsNode insertRowsNode = new InsertRowsNode(new PlanNodeId(""));
    insertRowsNode.setAligned(true);
    // insert 100 rows (50 aligned, 50 non-aligned) by insertRows
    insertRowsNode.setMixingAlignment(true);
    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(i <= 50 ? deviceId : "root.vehicle.d2", i);
      record.addTuple(DataPoint.getDataPoint(dataType, "s" + i, String.valueOf(i)));
      InsertRowNode node = buildInsertRowNodeByTSRecord(record);
      node.setAligned(true);
      if (i <= 50) {
        node.setAligned(true);
      }
      insertRowsNode.addOneInsertRowNode(node, i - 1);
    }
    processor2.insertRows(insertRowsNode, new long[5]);
    IMemTable memTable2 = processor2.getWorkMemTable();

    Assert.assertEquals(memTable1.getTVListsRamCost(), memTable2.getTVListsRamCost());
    Assert.assertEquals(memTable1.getTotalPointsNum(), memTable2.getTotalPointsNum());
    Assert.assertEquals(memTable1.memSize(), memTable2.memSize());
  }

  @Test
  public void testWriteAndClose()
      throws IOException, WriteProcessException, MetadataException, ExecutionException {
    processor =
        new TsFileProcessor(
            storageGroup,
            SystemFileFactory.INSTANCE.getFile(filePath),
            sgInfo,
            this::closeTsFileProcessor,
            (tsFileProcessor, updateMap, systemFlushTime) -> {},
            true);

    TsFileProcessorInfo tsFileProcessorInfo = new TsFileProcessorInfo(sgInfo);
    processor.setTsFileProcessorInfo(tsFileProcessorInfo);
    this.sgInfo.initTsFileProcessorInfo(processor);
    SystemInfo.getInstance().reportStorageGroupStatus(sgInfo, processor);
    List<TsFileResource> tsfileResourcesForQuery = new ArrayList<>();

    NonAlignedFullPath fullPath =
        new NonAlignedFullPath(
            IDeviceID.Factory.DEFAULT_FACTORY.create(deviceId),
            new MeasurementSchema(
                measurementId, dataType, encoding, CompressionType.UNCOMPRESSED, props));
    processor.query(Collections.singletonList(fullPath), context, tsfileResourcesForQuery, null);
    assertTrue(tsfileResourcesForQuery.isEmpty());

    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(deviceId, i);
      record.addTuple(DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i)));
      processor.insert(buildInsertRowNodeByTSRecord(record), new long[5]);
    }

    // query data in memory
    tsfileResourcesForQuery.clear();
    processor.query(Collections.singletonList(fullPath), context, tsfileResourcesForQuery, null);
    assertFalse(tsfileResourcesForQuery.isEmpty());
    assertFalse(tsfileResourcesForQuery.get(0).getReadOnlyMemChunk(fullPath).isEmpty());
    List<ReadOnlyMemChunk> memChunks = tsfileResourcesForQuery.get(0).getReadOnlyMemChunk(fullPath);
    for (ReadOnlyMemChunk chunk : memChunks) {
      IPointReader iterator = chunk.getPointReader();
      for (int num = 1; num <= 100; num++) {
        iterator.hasNextTimeValuePair();
        TimeValuePair timeValuePair = iterator.nextTimeValuePair();
        assertEquals(num, timeValuePair.getTimestamp());
        assertEquals(num, timeValuePair.getValue().getInt());
      }
    }

    // close synchronously
    processor.syncClose();

    assertTrue(processor.getTsFileResource().isClosed());
  }

  private void closeTsFileProcessor(TsFileProcessor unsealedTsFileProcessor)
      throws TsFileProcessorException {
    TsFileResource resource = unsealedTsFileProcessor.getTsFileResource();
    synchronized (resource) {
      for (IDeviceID deviceId : resource.getDevices()) {
        resource.updateEndTime(deviceId, resource.getStartTime(deviceId).get());
      }
      try {
        resource.close();
      } catch (IOException e) {
        throw new TsFileProcessorException(e);
      }
    }
  }

  private TsFileProcessor newTestProcessor(String path) throws IOException, WriteProcessException {
    TsFileProcessor newProcessor =
        new TsFileProcessor(
            storageGroup,
            SystemFileFactory.INSTANCE.getFile(path),
            sgInfo,
            this::closeTsFileProcessor,
            (tsFileProcessor, updateMap, systemFlushTime) -> {},
            true);
    TsFileProcessorInfo tsFileProcessorInfo = new TsFileProcessorInfo(sgInfo);
    newProcessor.setTsFileProcessorInfo(tsFileProcessorInfo);
    this.sgInfo.initTsFileProcessorInfo(newProcessor);
    SystemInfo.getInstance().reportStorageGroupStatus(sgInfo, newProcessor);
    return newProcessor;
  }

  private InsertTabletNode genSingleMeasurementTablet(int rowCount, boolean isAligned)
      throws IllegalPathException {
    String[] measurements = new String[] {measurementId};
    TSDataType[] dataTypes = new TSDataType[] {dataType};
    MeasurementSchema[] schemas =
        new MeasurementSchema[] {new MeasurementSchema(measurementId, dataType, encoding)};
    long[] times = new long[rowCount];
    Object[] columns = new Object[] {new int[rowCount]};

    for (int i = 0; i < rowCount; i++) {
      times[i] = i;
      ((int[]) columns[0])[i] = i;
    }

    return new InsertTabletNode(
        new QueryId("test_write").genPlanNodeId(),
        new PartialPath(deviceId),
        isAligned,
        measurements,
        dataTypes,
        schemas,
        times,
        null,
        columns,
        rowCount);
  }

  private InsertTabletNode genInsertTableNode(long startTime, boolean isAligned)
      throws IllegalPathException {
    String deviceId = "root.sg.device5";
    String[] measurements = new String[3000];
    TSDataType[] dataTypes = new TSDataType[3000];
    TSEncoding[] encodings = new TSEncoding[3000];
    MeasurementSchema[] schemas = new MeasurementSchema[3000];
    for (int i = 0; i < 3000; i++) {
      measurements[i] = "s" + i;
      dataTypes[i] = TSDataType.INT64;
      encodings[i] = TSEncoding.PLAIN;
      schemas[i] = new MeasurementSchema(measurements[i], dataTypes[i], encodings[i]);
    }

    long[] times = new long[10];
    Object[] columns = new Object[3000];
    for (int i = 0; i < 3000; i++) {
      columns[i] = new long[10];
    }

    for (long r = 0; r < 10; r++) {
      times[(int) r] = r + startTime;
      for (int i = 0; i < 3000; i++) {
        ((long[]) columns[i])[(int) r] = r;
      }
    }

    InsertTabletNode insertTabletNode =
        new InsertTabletNode(
            new QueryId("test_write").genPlanNodeId(),
            new PartialPath(deviceId),
            isAligned,
            measurements,
            dataTypes,
            times,
            null,
            columns,
            times.length);
    insertTabletNode.setMeasurementSchemas(schemas);

    return insertTabletNode;
  }

  private InsertTabletNode genInsertTableNodeFors3000ToS6000(long startTime, boolean isAligned)
      throws IllegalPathException {
    String deviceId = "root.sg.device5";
    String[] measurements = new String[3000];
    TSDataType[] dataTypes = new TSDataType[3000];
    TSEncoding[] encodings = new TSEncoding[3000];
    MeasurementSchema[] schemas = new MeasurementSchema[3000];
    for (int i = 0; i < 3000; i++) {
      measurements[i] = "s" + i + 3000;
      dataTypes[i] = TSDataType.INT64;
      encodings[i] = TSEncoding.PLAIN;
      schemas[i] = new MeasurementSchema(measurements[i], dataTypes[i], encodings[i]);
    }

    long[] times = new long[10];
    Object[] columns = new Object[3000];
    for (int i = 0; i < 3000; i++) {
      columns[i] = new long[10];
    }

    for (long r = 0; r < 10; r++) {
      times[(int) r] = r + startTime;
      for (int i = 0; i < 3000; i++) {
        ((long[]) columns[i])[(int) r] = r;
      }
    }

    InsertTabletNode insertTabletNode =
        new InsertTabletNode(
            new QueryId("test_write").genPlanNodeId(),
            new PartialPath(deviceId),
            isAligned,
            measurements,
            dataTypes,
            times,
            null,
            columns,
            times.length);
    insertTabletNode.setMeasurementSchemas(schemas);

    return insertTabletNode;
  }
}
