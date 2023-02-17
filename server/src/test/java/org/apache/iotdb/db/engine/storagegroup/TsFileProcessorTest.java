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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.rescon.SystemInfo;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

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
import static org.apache.iotdb.db.engine.storagegroup.DataRegionTest.buildInsertRowNodeByTSRecord;
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
    MeasurementPath fullPath =
        new MeasurementPath(
            deviceId,
            measurementId,
            new MeasurementSchema(
                measurementId, dataType, encoding, CompressionType.UNCOMPRESSED, props));
    processor.query(Collections.singletonList(fullPath), context, tsfileResourcesForQuery);
    assertTrue(tsfileResourcesForQuery.isEmpty());

    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(i, deviceId);
      record.addTuple(DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i)));
      processor.insert(buildInsertRowNodeByTSRecord(record));
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
    MeasurementPath fullPath =
        new MeasurementPath(
            deviceId,
            measurementId,
            new MeasurementSchema(
                measurementId, dataType, encoding, CompressionType.UNCOMPRESSED, props));
    processor.query(Collections.singletonList(fullPath), context, tsfileResourcesForQuery);
    assertTrue(tsfileResourcesForQuery.isEmpty());

    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(i, deviceId);
      record.addTuple(DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i)));
      processor.insert(buildInsertRowNodeByTSRecord(record));
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
    Map<String, List<ChunkMetadata>> chunkMetaDataListInChunkGroups =
        tsFileIOWriter.getDeviceChunkMetadataMap();
    RestorableTsFileIOWriter restorableTsFileIOWriter =
        new RestorableTsFileIOWriter(SystemFileFactory.INSTANCE.getFile(filePath));
    Map<String, List<ChunkMetadata>> restoredChunkMetaDataListInChunkGroups =
        restorableTsFileIOWriter.getDeviceChunkMetadataMap();
    assertEquals(
        chunkMetaDataListInChunkGroups.size(), restoredChunkMetaDataListInChunkGroups.size());
    for (Map.Entry<String, List<ChunkMetadata>> entry1 :
        chunkMetaDataListInChunkGroups.entrySet()) {
      for (Map.Entry<String, List<ChunkMetadata>> entry2 :
          restoredChunkMetaDataListInChunkGroups.entrySet()) {
        assertEquals(entry1.getKey(), entry2.getKey());
        assertEquals(entry1.getValue().size(), entry2.getValue().size());
        for (int i = 0; i < entry1.getValue().size(); i++) {
          ChunkMetadata chunkMetaData = entry1.getValue().get(i);
          chunkMetaData.setFilePath(filePath);

          ChunkMetadata chunkMetadataRestore = entry2.getValue().get(i);
          chunkMetadataRestore.setFilePath(filePath);

          assertEquals(chunkMetaData, chunkMetadataRestore);
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
    MeasurementPath fullPath =
        new MeasurementPath(
            deviceId,
            measurementId,
            new MeasurementSchema(
                measurementId, dataType, encoding, CompressionType.UNCOMPRESSED, props));
    processor.query(Collections.singletonList(fullPath), context, tsfileResourcesForQuery);
    assertTrue(tsfileResourcesForQuery.isEmpty());

    for (int flushId = 0; flushId < 10; flushId++) {
      for (int i = 1; i <= 10; i++) {
        TSRecord record = new TSRecord(i, deviceId);
        record.addTuple(DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i)));
        processor.insert(buildInsertRowNodeByTSRecord(record));
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
    processor.insertTablet(genInsertTableNode(0, true), 0, 10, new TSStatus[10]);
    IMemTable memTable = processor.getWorkMemTable();
    Assert.assertEquals(1596808, memTable.getTVListsRamCost());
    processor.insertTablet(genInsertTableNode(100, true), 0, 10, new TSStatus[10]);
    Assert.assertEquals(1596808, memTable.getTVListsRamCost());
    processor.insertTablet(genInsertTableNode(200, true), 0, 10, new TSStatus[10]);
    Assert.assertEquals(1596808, memTable.getTVListsRamCost());
    Assert.assertEquals(90000, memTable.getTotalPointsNum());
    Assert.assertEquals(720360, memTable.memSize());
    // Test records
    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(i, deviceId);
      record.addTuple(DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i)));
      processor.insert(buildInsertRowNodeByTSRecord(record));
    }
    Assert.assertEquals(1598424, memTable.getTVListsRamCost());
    Assert.assertEquals(90100, memTable.getTotalPointsNum());
    Assert.assertEquals(721560, memTable.memSize());
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
    processor.insertTablet(genInsertTableNode(0, false), 0, 10, new TSStatus[10]);
    IMemTable memTable = processor.getWorkMemTable();
    Assert.assertEquals(3192000, memTable.getTVListsRamCost());
    processor.insertTablet(genInsertTableNode(100, false), 0, 10, new TSStatus[10]);
    Assert.assertEquals(3192000, memTable.getTVListsRamCost());
    processor.insertTablet(genInsertTableNode(200, false), 0, 10, new TSStatus[10]);
    Assert.assertEquals(3192000, memTable.getTVListsRamCost());
    Assert.assertEquals(90000, memTable.getTotalPointsNum());
    Assert.assertEquals(1440000, memTable.memSize());
    // Test records
    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(i, deviceId);
      record.addTuple(DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i)));
      processor.insert(buildInsertRowNodeByTSRecord(record));
    }
    Assert.assertEquals(3193616, memTable.getTVListsRamCost());
    Assert.assertEquals(90100, memTable.getTotalPointsNum());
    Assert.assertEquals(1441200, memTable.memSize());
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

    MeasurementPath fullPath =
        new MeasurementPath(
            deviceId,
            measurementId,
            new MeasurementSchema(
                measurementId, dataType, encoding, CompressionType.UNCOMPRESSED, props));
    processor.query(Collections.singletonList(fullPath), context, tsfileResourcesForQuery);
    assertTrue(tsfileResourcesForQuery.isEmpty());

    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(i, deviceId);
      record.addTuple(DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i)));
      processor.insert(buildInsertRowNodeByTSRecord(record));
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
      for (String deviceId : resource.getDevices()) {
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
}
