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

import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.MetadataManagerHelper;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.rescon.SystemInfo;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TsFileProcessorTest {

  private TsFileProcessor processor;
  private String storageGroup = "root.vehicle";
  private StorageGroupInfo sgInfo = new StorageGroupInfo(null);
  private String filePath = TestConstant.getTestTsFilePath("root.vehicle", 0, 0, 0);
  private String deviceId = "root.vehicle.d0";
  private String measurementId = "s0";
  private TSDataType dataType = TSDataType.INT32;
  private TSEncoding encoding = TSEncoding.RLE;
  private Map<String, String> props = Collections.emptyMap();
  private QueryContext context;
  private static Logger logger = LoggerFactory.getLogger(TsFileProcessorTest.class);

  @Before
  public void setUp() {
    File file = new File(filePath);
    if (!file.getParentFile().exists()) {
      Assert.assertTrue(file.getParentFile().mkdirs());
    }
    EnvironmentUtils.envSetUp();
    MetadataManagerHelper.initMetadata();
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
            (tsFileProcessor) -> true,
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
      processor.insert(new InsertRowPlan(record));
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
            (tsFileProcessor) -> true,
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
      processor.insert(new InsertRowPlan(record));
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
            (tsFileProcessor) -> true,
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
        processor.insert(new InsertRowPlan(record));
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
            (tsFileProcessor) -> true,
            true);
    TsFileProcessorInfo tsFileProcessorInfo = new TsFileProcessorInfo(sgInfo);
    processor.setTsFileProcessorInfo(tsFileProcessorInfo);
    this.sgInfo.initTsFileProcessorInfo(processor);
    SystemInfo.getInstance().reportStorageGroupStatus(sgInfo, processor);
    processor.insertTablet(genInsertTablePlan(0), 0, 100, new TSStatus[100]);
    IMemTable memTable = processor.getWorkMemTable();
    Assert.assertEquals(3008, memTable.getTVListsRamCost());
    processor.insertTablet(genInsertTablePlan(100), 0, 100, new TSStatus[100]);
    Assert.assertEquals(5264, memTable.getTVListsRamCost());
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
            (tsFileProcessor) -> true,
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
      processor.insert(new InsertRowPlan(record));
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

  private InsertTabletPlan genInsertTablePlan(long startTime) throws IllegalPathException {
    String[] measurements = new String[2];
    measurements[0] = "sensor0";
    measurements[1] = "sensor1";

    List<Integer> dataTypesList = new ArrayList<>();
    TSDataType[] dataTypes = new TSDataType[2];
    dataTypesList.add(TSDataType.BOOLEAN.ordinal());
    dataTypesList.add(TSDataType.INT64.ordinal());
    dataTypes[0] = TSDataType.BOOLEAN;
    dataTypes[1] = TSDataType.INT64;

    TSEncoding[] encodings = new TSEncoding[2];
    encodings[0] = TSEncoding.PLAIN;
    encodings[1] = TSEncoding.GORILLA;

    String deviceId = "root.sg.device5";

    IMeasurementMNode[] mNodes = new IMeasurementMNode[2];
    IMeasurementSchema schema0 = new MeasurementSchema(measurements[0], dataTypes[0], encodings[0]);
    IMeasurementSchema schema1 = new MeasurementSchema(measurements[1], dataTypes[1], encodings[1]);
    mNodes[0] = MeasurementMNode.getMeasurementMNode(null, "sensor0", schema0, null);
    mNodes[1] = MeasurementMNode.getMeasurementMNode(null, "sensor1", schema1, null);

    InsertTabletPlan insertTabletPlan =
        new InsertTabletPlan(new PartialPath(deviceId), measurements, dataTypesList);

    long[] times = new long[100];
    Object[] columns = new Object[2];
    columns[0] = new boolean[100];
    columns[1] = new long[100];

    for (long r = 0; r < 100; r++) {
      times[(int) r] = r + startTime;
      ((boolean[]) columns[0])[(int) r] = false;
      ((long[]) columns[1])[(int) r] = r;
    }
    insertTabletPlan.setTimes(times);
    insertTabletPlan.setColumns(columns);
    insertTabletPlan.setRowCount(times.length);
    insertTabletPlan.setMeasurementMNodes(mNodes);
    insertTabletPlan.setStart(0);
    insertTabletPlan.setEnd(100);
    insertTabletPlan.setAligned(true);

    return insertTabletPlan;
  }
}
