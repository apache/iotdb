/**
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.iotdb.db.engine.MetadataManagerHelper;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.version.SysTimeVersionController;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.fileSystem.IoTDBFileFactory;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TsFileProcessorTest {

  private TsFileProcessor processor;
  private String storageGroup = "storage_group1";
  private String filePath = "data/testUnsealedTsFileProcessor.tsfile";
  private String deviceId = "root.vehicle.d0";
  private String measurementId = "s0";
  private TSDataType dataType = TSDataType.INT32;
  private Map<String, String> props = Collections.emptyMap();
  private QueryContext context = EnvironmentUtils.TEST_QUERY_CONTEXT;

  @Before
  public void setUp() throws Exception {
    MetadataManagerHelper.initMetadata();
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    EnvironmentUtils.cleanDir("data");
  }

  @Test
  public void testWriteAndFlush()
      throws WriteProcessException, IOException, TsFileProcessorException {
    processor = new TsFileProcessor(storageGroup, IoTDBFileFactory.INSTANCE.getIoTDBFile(filePath),
        SchemaUtils.constructSchema(deviceId), SysTimeVersionController.INSTANCE, x -> {
    },
        () -> true, true);

    Pair<ReadOnlyMemChunk, List<ChunkMetaData>> pair = processor
        .query(deviceId, measurementId, dataType, props, context);
    ReadOnlyMemChunk left = pair.left;
    List<ChunkMetaData> right = pair.right;
    assertTrue(left.isEmpty());
    assertEquals(0, right.size());

    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(i, deviceId);
      record.addTuple(DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i)));
      processor.insert(new InsertPlan(record));
    }

    // query data in memory
    pair = processor.query(deviceId, measurementId, dataType, props, context);
    left = pair.left;
    assertFalse(left.isEmpty());
    int num = 1;
    Iterator<TimeValuePair> iterator = left.getIterator();
    for (; num <= 100; num++) {
      iterator.hasNext();
      TimeValuePair timeValuePair = iterator.next();
      assertEquals(num, timeValuePair.getTimestamp());
      assertEquals(num, timeValuePair.getValue().getInt());
    }

    // flush synchronously
    processor.syncFlush();

    pair = processor.query(deviceId, measurementId, dataType, props, context);
    left = pair.left;
    right = pair.right;
    assertTrue(left.isEmpty());
    assertEquals(1, right.size());
    assertEquals(measurementId, right.get(0).getMeasurementUid());
    assertEquals(dataType, right.get(0).getTsDataType());
    processor.syncClose();
  }

  @Test
  public void testWriteAndRestoreMetadata()
      throws IOException {
    processor = new TsFileProcessor(storageGroup, IoTDBFileFactory.INSTANCE.getIoTDBFile(filePath),
        SchemaUtils.constructSchema(deviceId), SysTimeVersionController.INSTANCE, x -> {
    },
        () -> true, true);

    Pair<ReadOnlyMemChunk, List<ChunkMetaData>> pair = processor
        .query(deviceId, measurementId, dataType, props, context);
    ReadOnlyMemChunk left = pair.left;
    List<ChunkMetaData> right = pair.right;
    assertTrue(left.isEmpty());
    assertEquals(0, right.size());

    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(i, deviceId);
      record.addTuple(DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i)));
      processor.insert(new InsertPlan(record));
    }

    // query data in memory
    pair = processor.query(deviceId, measurementId, dataType, props, context);
    left = pair.left;
    assertFalse(left.isEmpty());
    int num = 1;
    Iterator<TimeValuePair> iterator = left.getIterator();
    for (; num <= 100; num++) {
      iterator.hasNext();
      TimeValuePair timeValuePair = iterator.next();
      assertEquals(num, timeValuePair.getTimestamp());
      assertEquals(num, timeValuePair.getValue().getInt());
    }

    // flush synchronously
    processor.syncFlush();

    pair = processor.query(deviceId, measurementId, dataType, props, context);
    left = pair.left;
    right = pair.right;
    assertTrue(left.isEmpty());
    assertEquals(1, right.size());
    assertEquals(measurementId, right.get(0).getMeasurementUid());
    assertEquals(dataType, right.get(0).getTsDataType());

    RestorableTsFileIOWriter tsFileIOWriter = processor.getWriter();
    List<ChunkGroupMetaData> chunkGroupMetaDataList = tsFileIOWriter.getChunkGroupMetaDatas();
    RestorableTsFileIOWriter restorableTsFileIOWriter = new RestorableTsFileIOWriter(
        IoTDBFileFactory.INSTANCE.getIoTDBFile(filePath));
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
    processor.syncClose();
  }


  @Test
  public void testMultiFlush()
      throws WriteProcessException, IOException, TsFileProcessorException {
    processor = new TsFileProcessor(storageGroup, IoTDBFileFactory.INSTANCE.getIoTDBFile(filePath),
        SchemaUtils.constructSchema(deviceId), SysTimeVersionController.INSTANCE, x -> {
    },
        () -> true, true);

    Pair<ReadOnlyMemChunk, List<ChunkMetaData>> pair = processor
        .query(deviceId, measurementId, dataType, props, context);
    ReadOnlyMemChunk left = pair.left;
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

    pair = processor.query(deviceId, measurementId, dataType, props, context);
    left = pair.left;
    right = pair.right;
    assertTrue(left.isEmpty());
    assertEquals(10, right.size());
    assertEquals(measurementId, right.get(0).getMeasurementUid());
    assertEquals(dataType, right.get(0).getTsDataType());
    processor.syncClose();
  }


  @Test
  public void testWriteAndClose()
      throws WriteProcessException, IOException {
    processor = new TsFileProcessor(storageGroup, IoTDBFileFactory.INSTANCE.getIoTDBFile(filePath),
        SchemaUtils.constructSchema(deviceId), SysTimeVersionController.INSTANCE,
        unsealedTsFileProcessor -> {
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
        }, () -> true, true);

    Pair<ReadOnlyMemChunk, List<ChunkMetaData>> pair = processor
        .query(deviceId, measurementId, dataType, props, context);
    ReadOnlyMemChunk left = pair.left;
    List<ChunkMetaData> right = pair.right;
    assertTrue(left.isEmpty());
    assertEquals(0, right.size());

    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(i, deviceId);
      record.addTuple(DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i)));
      processor.insert(new InsertPlan(record));
    }

    // query data in memory
    pair = processor.query(deviceId, measurementId, dataType, props, context);
    left = pair.left;
    assertFalse(left.isEmpty());
    int num = 1;
    Iterator<TimeValuePair> iterator = left.getIterator();
    for (; num <= 100; num++) {
      iterator.hasNext();
      TimeValuePair timeValuePair = iterator.next();
      assertEquals(num, timeValuePair.getTimestamp());
      assertEquals(num, timeValuePair.getValue().getInt());
    }

    // close synchronously
    processor.syncClose();

    assertTrue(processor.getTsFileResource().isClosed());

  }
}