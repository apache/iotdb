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
import org.apache.iotdb.commons.path.NonAlignedFullPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.storageengine.dataregion.DataRegionInfo;
import org.apache.iotdb.db.storageengine.dataregion.DataRegionTest;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.reader.IPointReader;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.DataPoint;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.RestorableTsFileIOWriter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertTrue;
import static org.apache.iotdb.db.storageengine.dataregion.DataRegionTest.buildInsertRowNodeByTSRecord;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TsFileProcessorTest {

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
  private static final Logger logger = LoggerFactory.getLogger(TsFileProcessorTest.class);

  public TsFileProcessorTest() {}

  @Before
  public void setUp() throws DataRegionException {
    File file = new File(filePath);
    if (!file.getParentFile().exists()) {
      Assert.assertTrue(file.getParentFile().mkdirs());
    }
    EnvironmentUtils.envSetUp();
    sgInfo = new DataRegionInfo(new DataRegionTest.DummyDataRegion(systemDir, storageGroup));
    context = EnvironmentUtils.TEST_QUERY_CONTEXT;
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    EnvironmentUtils.cleanDir(TestConstant.OUTPUT_DATA_DIR);
  }

  @Test
  public void testWriteAndFlush() throws IOException, WriteProcessException, MetadataException {
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
    processor.query(Collections.singletonList(fullPath), context, tsfileResourcesForQuery);
    assertTrue(tsfileResourcesForQuery.isEmpty());

    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(deviceId, i);
      record.addTuple(DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i)));
      processor.insert(buildInsertRowNodeByTSRecord(record), new long[5]);
    }

    // query data in memory
    tsfileResourcesForQuery.clear();
    processor.query(Collections.singletonList(fullPath), context, tsfileResourcesForQuery);

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
    processor.syncFlush();

    tsfileResourcesForQuery.clear();
    processor.query(Collections.singletonList(fullPath), context, tsfileResourcesForQuery);
    assertTrue(tsfileResourcesForQuery.get(0).getReadOnlyMemChunk(fullPath).isEmpty());
    processor.syncClose();
  }

  @Test
  public void testWriteAndRestoreMetadata()
      throws IOException, WriteProcessException, MetadataException {
    logger.info("testWriteAndRestoreMetadata begin..");
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
    processor.query(Collections.singletonList(fullPath), context, tsfileResourcesForQuery);
    assertTrue(tsfileResourcesForQuery.isEmpty());

    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(deviceId, i);
      record.addTuple(DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i)));
      processor.insert(buildInsertRowNodeByTSRecord(record), new long[5]);
    }

    // query data in memory
    tsfileResourcesForQuery.clear();
    processor.query(Collections.singletonList(fullPath), context, tsfileResourcesForQuery);
    assertFalse(tsfileResourcesForQuery.get(0).getReadOnlyMemChunk(fullPath).isEmpty());
    int num = 1;
    List<ReadOnlyMemChunk> memChunks = tsfileResourcesForQuery.get(0).getReadOnlyMemChunk(fullPath);
    for (ReadOnlyMemChunk chunk : memChunks) {
      IPointReader iterator = chunk.getPointReader();
      for (; num <= 100; num++) {
        iterator.hasNextTimeValuePair();
        TimeValuePair timeValuePair = iterator.nextTimeValuePair();
        assertEquals(num, timeValuePair.getTimestamp());
        assertEquals(num, timeValuePair.getValue().getInt());
      }
    }
    logger.info("syncFlush..");
    // flush synchronously
    processor.syncFlush();

    tsfileResourcesForQuery.clear();
    processor.query(Collections.singletonList(fullPath), context, tsfileResourcesForQuery);
    assertTrue(tsfileResourcesForQuery.get(0).getReadOnlyMemChunk(fullPath).isEmpty());

    RestorableTsFileIOWriter tsFileIOWriter = processor.getWriter();
    Map<IDeviceID, List<ChunkMetadata>> chunkMetaDataListInChunkGroups =
        tsFileIOWriter.getDeviceChunkMetadataMap();
    RestorableTsFileIOWriter restorableTsFileIOWriter =
        new RestorableTsFileIOWriter(SystemFileFactory.INSTANCE.getFile(filePath));
    Map<IDeviceID, List<ChunkMetadata>> restoredChunkMetaDataListInChunkGroups =
        restorableTsFileIOWriter.getDeviceChunkMetadataMap();
    assertEquals(
        chunkMetaDataListInChunkGroups.size(), restoredChunkMetaDataListInChunkGroups.size());
    for (Map.Entry<IDeviceID, List<ChunkMetadata>> entry1 :
        chunkMetaDataListInChunkGroups.entrySet()) {
      for (Map.Entry<IDeviceID, List<ChunkMetadata>> entry2 :
          restoredChunkMetaDataListInChunkGroups.entrySet()) {
        assertEquals(entry1.getKey(), entry2.getKey());
        assertEquals(entry1.getValue().size(), entry2.getValue().size());
        for (int i = 0; i < entry1.getValue().size(); i++) {
          ChunkMetadata chunkMetaData = entry1.getValue().get(i);
          chunkMetaData.setVersion(0);
          ChunkMetadata chunkMetadataRestore = entry2.getValue().get(i);
          chunkMetadataRestore.setVersion(0);
        }
      }
    }
    restorableTsFileIOWriter.close();
    logger.info("syncClose..");
    processor.syncClose();
    // we need to close the tsfile writer first and then reopen it.
  }

  @Test
  public void testMultiFlush() throws IOException, WriteProcessException, MetadataException {
    logger.info("testWriteAndRestoreMetadata begin..");
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
    processor.query(Collections.singletonList(fullPath), context, tsfileResourcesForQuery);
    assertTrue(tsfileResourcesForQuery.isEmpty());

    for (int flushId = 0; flushId < 10; flushId++) {
      for (int i = 1; i <= 10; i++) {
        TSRecord record = new TSRecord(deviceId, i);
        record.addTuple(DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i)));
        processor.insert(buildInsertRowNodeByTSRecord(record), new long[5]);
      }
      processor.asyncFlush();
    }
    processor.syncFlush();

    tsfileResourcesForQuery.clear();
    processor.query(Collections.singletonList(fullPath), context, tsfileResourcesForQuery);
    assertFalse(tsfileResourcesForQuery.isEmpty());
    assertTrue(tsfileResourcesForQuery.get(0).getReadOnlyMemChunk(fullPath).isEmpty());
    processor.syncClose();
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
    Assert.assertEquals(1596808, memTable.getTVListsRamCost());
    processor.insertTablet(
        genInsertTableNode(100, true),
        Collections.singletonList(new int[] {0, 10}),
        new TSStatus[10],
        true,
        new long[5]);
    Assert.assertEquals(1596808, memTable.getTVListsRamCost());
    processor.insertTablet(
        genInsertTableNode(200, true),
        Collections.singletonList(new int[] {0, 10}),
        new TSStatus[10],
        true,
        new long[5]);
    Assert.assertEquals(1596808, memTable.getTVListsRamCost());
    Assert.assertEquals(90000, memTable.getTotalPointsNum());
    Assert.assertEquals(720360, memTable.memSize());
    // Test records
    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(deviceId, i);
      record.addTuple(DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i)));
      processor.insert(buildInsertRowNodeByTSRecord(record), new long[5]);
    }
    Assert.assertEquals(1598424, memTable.getTVListsRamCost());
    Assert.assertEquals(90100, memTable.getTotalPointsNum());
    Assert.assertEquals(721560, memTable.memSize());
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
    Assert.assertEquals(1596808, memTable.getTVListsRamCost());
    processor.insertTablet(
        genInsertTableNodeFors3000ToS6000(0, true),
        Collections.singletonList(new int[] {0, 10}),
        new TSStatus[10],
        true,
        new long[5]);
    Assert.assertEquals(3192808, memTable.getTVListsRamCost());
    processor.insertTablet(
        genInsertTableNode(100, true),
        Collections.singletonList(new int[] {0, 10}),
        new TSStatus[10],
        true,
        new long[5]);
    Assert.assertEquals(3192808, memTable.getTVListsRamCost());
    processor.insertTablet(
        genInsertTableNodeFors3000ToS6000(100, true),
        Collections.singletonList(new int[] {0, 10}),
        new TSStatus[10],
        true,
        new long[5]);
    Assert.assertEquals(3192808, memTable.getTVListsRamCost());
    processor.insertTablet(
        genInsertTableNode(200, true),
        Collections.singletonList(new int[] {0, 10}),
        new TSStatus[10],
        true,
        new long[5]);
    Assert.assertEquals(3192808, memTable.getTVListsRamCost());
    processor.insertTablet(
        genInsertTableNodeFors3000ToS6000(200, true),
        Collections.singletonList(new int[] {0, 10}),
        new TSStatus[10],
        true,
        new long[5]);
    Assert.assertEquals(3192808, memTable.getTVListsRamCost());
    processor.insertTablet(
        genInsertTableNode(300, true),
        Collections.singletonList(new int[] {0, 10}),
        new TSStatus[10],
        true,
        new long[5]);
    Assert.assertEquals(6385616, memTable.getTVListsRamCost());
    processor.insertTablet(
        genInsertTableNodeFors3000ToS6000(300, true),
        Collections.singletonList(new int[] {0, 10}),
        new TSStatus[10],
        true,
        new long[5]);
    Assert.assertEquals(6385616, memTable.getTVListsRamCost());

    Assert.assertEquals(240000, memTable.getTotalPointsNum());
    Assert.assertEquals(1920960, memTable.memSize());
    // Test records
    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(deviceId, i);
      record.addTuple(DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i)));
      processor.insert(buildInsertRowNodeByTSRecord(record), new long[5]);
    }
    Assert.assertEquals(6387232, memTable.getTVListsRamCost());
    // Test records
    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(deviceId, i);
      record.addTuple(DataPoint.getDataPoint(dataType, "s1", String.valueOf(i)));
      processor.insert(buildInsertRowNodeByTSRecord(record), new long[5]);
    }
    Assert.assertEquals(6388848, memTable.getTVListsRamCost());
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
  public void testWriteAndClose() throws IOException, WriteProcessException, MetadataException {
    logger.info("testWriteAndRestoreMetadata begin..");
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
    processor.query(Collections.singletonList(fullPath), context, tsfileResourcesForQuery);
    assertTrue(tsfileResourcesForQuery.isEmpty());

    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(deviceId, i);
      record.addTuple(DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i)));
      processor.insert(buildInsertRowNodeByTSRecord(record), new long[5]);
    }

    // query data in memory
    tsfileResourcesForQuery.clear();
    processor.query(Collections.singletonList(fullPath), context, tsfileResourcesForQuery);
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
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    assertTrue(processor.getTsFileResource().isClosed());
  }

  private void closeTsFileProcessor(TsFileProcessor unsealedTsFileProcessor)
      throws TsFileProcessorException {
    TsFileResource resource = unsealedTsFileProcessor.getTsFileResource();
    synchronized (resource) {
      for (IDeviceID deviceId : resource.getDevices()) {
        resource.updateEndTime(deviceId, resource.getStartTime(deviceId));
      }
      try {
        resource.close();
      } catch (IOException e) {
        throw new TsFileProcessorException(e);
      }
    }
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
