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
import static org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor.getVmLevel;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.adapter.ActiveTimeSeriesCounter;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.MetadataManagerHelper;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.version.SysTimeVersionController;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TsFileProcessorEnableVmTest {

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
    IoTDBDescriptor.getInstance().getConfig().setEnableVm(true);
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    EnvironmentUtils.cleanDir(TestConstant.OUTPUT_DATA_DIR);
  }

  @Test
  public void testWriteAndFlush() throws IOException, WriteProcessException, IllegalPathException {
    logger.info("testWriteAndFlush begin..");
    processor = new TsFileProcessor(storageGroup, SystemFileFactory.INSTANCE.getFile(filePath),
        new ArrayList<>(),
        SysTimeVersionController.INSTANCE, this::closeTsFileProcessor,
        (tsFileProcessor) -> true, true);

    List<TsFileResource> tsfileResourcesForQuery = new ArrayList<>();

    processor.query(deviceId, measurementId, dataType, encoding, props, context,
        tsfileResourcesForQuery);
    assertTrue(tsfileResourcesForQuery.isEmpty());

    for (int i = 1; i <= 1000000; i++) {
      TSRecord record = new TSRecord(i, deviceId);
      record.addTuple(DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i)));
      processor.insert(new InsertRowPlan(record));
    }

    // query data in memory
    tsfileResourcesForQuery.clear();
    processor.query(deviceId, measurementId, dataType, encoding, props, context,
        tsfileResourcesForQuery);
    assertFalse(tsfileResourcesForQuery.isEmpty());
    Assert.assertEquals(0, tsfileResourcesForQuery.get(0).getChunkMetadataList().size());

    //assertEquals(1, right.get(1).size());
    int num = 1;
    List<ReadOnlyMemChunk> memChunks = tsfileResourcesForQuery.get(0).getReadOnlyMemChunk();
    for (; num <= 100; num++) {
      for (ReadOnlyMemChunk chunk : memChunks) {
        IPointReader iterator = chunk.getPointReader();
        iterator.hasNextTimeValuePair();
        TimeValuePair timeValuePair = iterator.nextTimeValuePair();
        assertEquals(num, timeValuePair.getTimestamp());
        assertEquals(num, timeValuePair.getValue().getInt());
      }
    }

    // flush synchronously
    processor.syncFlush();
    tsfileResourcesForQuery.clear();
    processor.query(deviceId, measurementId, dataType, encoding, props, context,
        tsfileResourcesForQuery);
    List<List<TsFileResource>> tsfileResources = processor.getVmTsFileResources();
    for (List<TsFileResource> levelResources : tsfileResources) {
      for (TsFileResource resource : levelResources) {
        assertEquals(0, getVmLevel(resource.getTsFile()));
      }
    }

    assertEquals(1, tsfileResourcesForQuery.size());
    assertEquals(1, tsfileResourcesForQuery.get(0).getChunkMetadataList().size());
    processor.syncClose();
  }

  @Test
  public void testWriteAndClose() throws IOException, WriteProcessException, IllegalPathException {
    logger.info("testWriteAndRestoreMetadata begin..");
    processor = new TsFileProcessor(storageGroup, SystemFileFactory.INSTANCE.getFile(filePath),
        new ArrayList<>(),
        SysTimeVersionController.INSTANCE, this::closeTsFileProcessor,
        (tsFileProcessor) -> true, true);

    List<TsFileResource> tsfileResourcesForQuery = new ArrayList<>();

    processor.query(deviceId, measurementId, dataType, encoding, props, context,
        tsfileResourcesForQuery);
    assertTrue(tsfileResourcesForQuery.isEmpty());

    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(i, deviceId);
      record.addTuple(DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i)));
      processor.insert(new InsertRowPlan(record));
    }

    // query data in memory
    tsfileResourcesForQuery.clear();
    processor.query(deviceId, measurementId, dataType, encoding, props, context,
        tsfileResourcesForQuery);
    assertFalse(tsfileResourcesForQuery.isEmpty());
    int num = 1;
    List<ReadOnlyMemChunk> memChunks = tsfileResourcesForQuery.get(0).getReadOnlyMemChunk();
    for (; num <= 100; num++) {
      for (ReadOnlyMemChunk chunk : memChunks) {
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

  private void closeTsFileProcessor(TsFileProcessor unsealedTsFileProcessor)
      throws TsFileProcessorException {
    TsFileResource resource = unsealedTsFileProcessor.getTsFileResource();
    synchronized (resource) {
      for (Entry<String, Integer> entry : resource.getDeviceToIndexMap().entrySet()) {
        resource.putEndTime(entry.getKey(), resource.getStartTime(entry.getValue()));
      }
      try {
        resource.close();
      } catch (IOException e) {
        throw new TsFileProcessorException(e);
      }
    }
  }
}