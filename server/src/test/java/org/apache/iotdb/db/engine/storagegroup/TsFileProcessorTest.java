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

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.iotdb.db.conf.adapter.ActiveTimeSeriesCounter;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.MetadataManagerHelper;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.version.SysTimeVersionController;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TsFileProcessorTest {

  private TsFileProcessor processor;
  private String storageGroup = "storage_group1";
  private String filePath = TestConstant.OUTPUT_DATA_DIR
      .concat("testUnsealedTsFileProcessor.tsfile");
  private String deviceId = "root.vehicle.d0";
  private String measurementId = "s0";
  private TSDataType dataType = TSDataType.INT32;
  private TSEncoding encoding = TSEncoding.RLE;
  private Map<String, String> props = Collections.emptyMap();
  private QueryContext context;
  private static Logger logger = LoggerFactory.getLogger(TsFileProcessorTest.class);
  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    MetadataManagerHelper.initMetadata();
    ActiveTimeSeriesCounter.getInstance().init(storageGroup);
    context = EnvironmentUtils.TEST_QUERY_CONTEXT;
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    EnvironmentUtils.cleanDir(TestConstant.OUTPUT_DATA_DIR);
  }

  @Test
  public void testWriteAndFlush() throws IOException, QueryProcessException, MetadataException {
    logger.info("testWriteAndFlush begin..");
    processor = new TsFileProcessor(storageGroup, SystemFileFactory.INSTANCE.getFile(filePath),
        SchemaUtils.constructSchema(deviceId), SysTimeVersionController.INSTANCE, this::closeTsFileProcessor,
        (tsFileProcessor) -> true, true);

    Pair<List<ReadOnlyMemChunk>, List<ChunkMetaData>> pair = processor
        .query(deviceId, measurementId, dataType, encoding, props, context);
    List<ReadOnlyMemChunk> left = pair.left;
    List<ChunkMetaData> right = pair.right;
    assertTrue(left.isEmpty());
    assertEquals(0, right.size());

    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(i, deviceId);
      record.addTuple(DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i)));
      processor.insert(new InsertPlan(record));
    }

    // query data in memory
    pair = processor.query(deviceId, measurementId, dataType, encoding, props, context);
    left = pair.left;
    assertFalse(left.isEmpty());
    int num = 1;
    for (; num <= 100; num++) {
      for (ReadOnlyMemChunk chunk : left) {
        IPointReader iterator = chunk.getPointReader();
        iterator.hasNextTimeValuePair();
        TimeValuePair timeValuePair = iterator.nextTimeValuePair();
        assertEquals(num, timeValuePair.getTimestamp());
        assertEquals(num, timeValuePair.getValue().getInt());
      }
    }

    // flush synchronously
    processor.syncFlush();

    pair = processor.query(deviceId, measurementId, dataType, encoding, props, context);
    left = pair.left;
    right = pair.right;
    assertTrue(left.isEmpty());
    assertEquals(1, right.size());
    assertEquals(measurementId, right.get(0).getMeasurementUid());
    assertEquals(dataType, right.get(0).getDataType());
    processor.syncClose();
  }

  @Test
  public void testWriteAndRestoreMetadata()
      throws IOException, QueryProcessException, MetadataException {
    logger.info("testWriteAndRestoreMetadata begin..");
    processor = new TsFileProcessor(storageGroup, SystemFileFactory.INSTANCE.getFile(filePath),
        SchemaUtils.constructSchema(deviceId), SysTimeVersionController.INSTANCE, this::closeTsFileProcessor,
        (tsFileProcessor) -> true, true);

    Pair<List<ReadOnlyMemChunk>, List<ChunkMetaData>> pair = processor
        .query(deviceId, measurementId, dataType, encoding, props, context);
    List<ReadOnlyMemChunk> left = pair.left;
    List<ChunkMetaData> right = pair.right;
    assertTrue(left.isEmpty());
    assertEquals(0, right.size());

    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(i, deviceId);
      record.addTuple(DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i)));
      processor.insert(new InsertPlan(record));
    }

    // query data in memory
    pair = processor.query(deviceId, measurementId, dataType, encoding, props, context);
    left = pair.left;
    assertFalse(left.isEmpty());
    int num = 1;

    for (ReadOnlyMemChunk chunk : left) {
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

    pair = processor.query(deviceId, measurementId, dataType, encoding, props, context);
    left = pair.left;
    right = pair.right;
    assertTrue(left.isEmpty());
    assertEquals(1, right.size());
    assertEquals(measurementId, right.get(0).getMeasurementUid());
    assertEquals(dataType, right.get(0).getDataType());

    RestorableTsFileIOWriter tsFileIOWriter = processor.getWriter();
    List<ChunkGroupMetaData> chunkGroupMetaDataList = tsFileIOWriter.getChunkGroupMetaDatas();
    RestorableTsFileIOWriter restorableTsFileIOWriter = new RestorableTsFileIOWriter(
        SystemFileFactory.INSTANCE.getFile(filePath));
    List<ChunkGroupMetaData> restoredChunkGroupMetaDataList = restorableTsFileIOWriter
        .getChunkGroupMetaDatas();
    assertEquals(chunkGroupMetaDataList.size(), restoredChunkGroupMetaDataList.size());
    for (int i = 0; i < chunkGroupMetaDataList.size(); i++) {
      ChunkGroupMetaData chunkGroupMetaData = chunkGroupMetaDataList.get(i);
      ChunkGroupMetaData chunkGroupMetaDataRestore = restoredChunkGroupMetaDataList.get(i);
      for (int j = 0; j < chunkGroupMetaData.getChunkMetaDataList().size(); j++) {
        ChunkMetaData chunkMetaData = chunkGroupMetaData.getChunkMetaDataList().get(j);
        ChunkMetaData chunkMetaDataRestore = chunkGroupMetaDataRestore.getChunkMetaDataList()
            .get(j);
        assertEquals(chunkMetaData, chunkMetaDataRestore);
      }
    }
    restorableTsFileIOWriter.close();
    logger.info("syncClose..");
    processor.syncClose();
    //we need to close the tsfile writer first and then reopen it.
  }


  @Test
  public void testMultiFlush() throws IOException, QueryProcessException, MetadataException {
    logger.info("testWriteAndRestoreMetadata begin..");
    processor = new TsFileProcessor(storageGroup, SystemFileFactory.INSTANCE.getFile(filePath),
        SchemaUtils.constructSchema(deviceId), SysTimeVersionController.INSTANCE, this::closeTsFileProcessor,
        (tsFileProcessor) -> true, true);

    Pair<List<ReadOnlyMemChunk>, List<ChunkMetaData>> pair = processor
        .query(deviceId, measurementId, dataType, encoding, props, context);
    List<ReadOnlyMemChunk> left = pair.left;
    List<ChunkMetaData> right = pair.right;
    assertTrue(left.isEmpty());
    assertEquals(0, right.size());

    for (int flushId = 0; flushId < 10; flushId++) {
      for (int i = 1; i <= 10; i++) {
        TSRecord record = new TSRecord(i, deviceId);
        record.addTuple(DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i)));
        processor.insert(new InsertPlan(record));
      }
      processor.asyncFlush();
    }
    processor.syncFlush();

    pair = processor.query(deviceId, measurementId, dataType, encoding, props, context);
    left = pair.left;
    right = pair.right;
    assertTrue(left.isEmpty());
    assertEquals(10, right.size());
    assertEquals(measurementId, right.get(0).getMeasurementUid());
    assertEquals(dataType, right.get(0).getDataType());
    processor.syncClose();
  }


  @Test
  public void testWriteAndClose() throws IOException, QueryProcessException, MetadataException {
    logger.info("testWriteAndRestoreMetadata begin..");
    processor = new TsFileProcessor(storageGroup, SystemFileFactory.INSTANCE.getFile(filePath),
        SchemaUtils.constructSchema(deviceId), SysTimeVersionController.INSTANCE,
        this::closeTsFileProcessor, (tsFileProcessor) -> true, true);

    Pair<List<ReadOnlyMemChunk>, List<ChunkMetaData>> pair = processor
        .query(deviceId, measurementId, dataType, encoding, props, context);
    List<ReadOnlyMemChunk> left = pair.left;
    List<ChunkMetaData> right = pair.right;
    assertTrue(left.isEmpty());
    assertEquals(0, right.size());

    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(i, deviceId);
      record.addTuple(DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i)));
      processor.insert(new InsertPlan(record));
    }

    // query data in memory
    pair = processor.query(deviceId, measurementId, dataType, encoding, props, context);
    left = pair.left;
    assertFalse(left.isEmpty());
    int num = 1;

    for (; num <= 100; num++) {
      for (ReadOnlyMemChunk chunk : left) {
        IPointReader iterator = chunk.getPointReader();
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
  private void closeTsFileProcessor(TsFileProcessor unsealedTsFileProcessor) throws TsFileProcessorException {
    TsFileResource resource = unsealedTsFileProcessor.getTsFileResource();
    synchronized (resource) {
      for (Entry<String, Long> startTime : resource.getStartTimeMap().entrySet()) {
        String deviceId = startTime.getKey();
        resource.getEndTimeMap().put(deviceId, resource.getStartTimeMap().get(deviceId));
      }
      try {
        resource.close();
      } catch (IOException e) {
        throw new TsFileProcessorException(e);
      }
    }
  }
}