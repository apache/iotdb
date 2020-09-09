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

import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.db.conf.adapter.ActiveTimeSeriesCounter;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.MetadataManagerHelper;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.flush.TsFileFlushPolicy;
import org.apache.iotdb.db.engine.merge.manage.MergeManager;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.StorageGroupProcessorException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.utils.EnvironmentUtils;
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

public class StorageGroupProcessorTest {

  private String storageGroup = "root.vehicle.d0";
  private String systemDir = TestConstant.OUTPUT_DATA_DIR.concat("info");
  private String deviceId = "root.vehicle.d0";
  private String measurementId = "s0";
  private StorageGroupProcessor processor;
  private QueryContext context = EnvironmentUtils.TEST_QUERY_CONTEXT;
  private AtomicLong mergeLock;
  private MNode deviceMNode = null;

  @Before
  public void setUp() throws Exception {
    MetadataManagerHelper.initMetadata();
    EnvironmentUtils.envSetUp();
    ActiveTimeSeriesCounter.getInstance().init(storageGroup);
    processor = new DummySGP(systemDir, storageGroup);
    MergeManager.getINSTANCE().start();
    deviceMNode = new MNode(null, deviceId);
    deviceMNode.addChild(measurementId, new MeasurementMNode(null, null, null, null));
  }

  @After
  public void tearDown() throws Exception {
    processor.syncDeleteDataFiles();
    EnvironmentUtils.cleanEnv();
    EnvironmentUtils.cleanDir(TestConstant.OUTPUT_DATA_DIR);
    MergeManager.getINSTANCE().stop();
    EnvironmentUtils.cleanEnv();
  }

  private void insertToStorageGroupProcessor(TSRecord record)
      throws WriteProcessException, IllegalPathException {
    InsertRowPlan insertRowPlan = new InsertRowPlan(record);
    insertRowPlan.setDeviceMNode(deviceMNode);
    processor.insert(insertRowPlan);
  }

  @Test
  public void testUnseqUnsealedDelete()
      throws WriteProcessException, IOException, IllegalPathException {
    TSRecord record = new TSRecord(10000, deviceId);
    record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(1000)));
    insertToStorageGroupProcessor(record);
    processor.syncCloseAllWorkingTsFileProcessors();

    for (int j = 1; j <= 10; j++) {
      record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      insertToStorageGroupProcessor(record);
    }

    for (TsFileProcessor tsfileProcessor : processor.getWorkUnsequenceTsFileProcessor()) {
      tsfileProcessor.syncFlush();
    }

    for (int j = 11; j <= 20; j++) {
      record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      insertToStorageGroupProcessor(record);
    }

    processor.delete(new PartialPath(deviceId), measurementId, 0, 15L);

    List<TsFileResource> unLockList = new ArrayList<>();
    List<TsFileResource> tsfileResourcesForQuery = new ArrayList<>();
    for (TsFileProcessor tsfileProcessor : processor.getWorkUnsequenceTsFileProcessor()) {
      tsfileProcessor
          .query(deviceId, measurementId, TSDataType.INT32, TSEncoding.RLE, Collections.emptyMap(),
              new QueryContext(), tsfileResourcesForQuery);
      unLockList.add(tsfileProcessor.getTsFileResource());
      for (List<TsFileResource> subTsFileResources : tsfileProcessor.getVmTsFileResources()) {
        unLockList.addAll(subTsFileResources);
      }
      break;
    }

    Assert.assertEquals(1, tsfileResourcesForQuery.size());
    Assert.assertEquals(0, tsfileResourcesForQuery.get(0).getChunkMetadataList().size());
    List<ReadOnlyMemChunk> memChunks = tsfileResourcesForQuery.get(0).getReadOnlyMemChunk();
    long time = 16;
    for (ReadOnlyMemChunk memChunk : memChunks) {
      IPointReader iterator = memChunk.getPointReader();
      while (iterator.hasNextTimeValuePair()) {
        TimeValuePair timeValuePair = iterator.nextTimeValuePair();
        Assert.assertEquals(time++, timeValuePair.getTimestamp());
      }
    }

    unLockList.forEach(TsFileResource::readUnlock);
  }

  @Test
  public void testSequenceSyncClose()
      throws WriteProcessException, QueryProcessException, IllegalPathException {
    for (int j = 1; j <= 10; j++) {
      TSRecord record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      insertToStorageGroupProcessor(record);
      processor.asyncCloseAllWorkingTsFileProcessors();
    }

    processor.syncCloseAllWorkingTsFileProcessors();
    QueryDataSource queryDataSource = processor.query(new PartialPath(deviceId), measurementId, context,
        null, null);

    Assert.assertEquals(10, queryDataSource.getSeqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      Assert.assertTrue(resource.isClosed());
    }
  }

  @Test
  public void testIoTDBTabletWriteAndSyncClose()
      throws WriteProcessException, QueryProcessException, IllegalPathException {

    String[] measurements = new String[2];
    measurements[0] = "s0";
    measurements[1] = "s1";
    List<Integer> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.INT32.ordinal());
    dataTypes.add(TSDataType.INT64.ordinal());

    MeasurementSchema[] schemas = new MeasurementSchema[2];
    schemas[0] = new MeasurementSchema("s0", TSDataType.INT32, TSEncoding.PLAIN);
    schemas[1] = new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN);

    MNode deviceMNode = new MNode(null, deviceId);
    deviceMNode.addChild("s0", new MeasurementMNode(null, null, null, null));
    deviceMNode.addChild("s1", new MeasurementMNode(null, null, null, null));

    InsertTabletPlan insertTabletPlan1 = new InsertTabletPlan(new PartialPath("root.vehicle.d0"), measurements,
        dataTypes);
    insertTabletPlan1.setSchemas(schemas);

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
    insertTabletPlan1.setDeviceMNode(deviceMNode);

    processor.insertTablet(insertTabletPlan1);
    processor.asyncCloseAllWorkingTsFileProcessors();

    InsertTabletPlan insertTabletPlan2 = new InsertTabletPlan(new PartialPath("root.vehicle.d0"), measurements,
        dataTypes);
    insertTabletPlan2.setSchemas(schemas);

    for (int r = 50; r < 149; r++) {
      times[r - 50] = r;
      ((int[]) columns[0])[r - 50] = 1;
      ((long[]) columns[1])[r - 50] = 1;
    }
    insertTabletPlan2.setTimes(times);
    insertTabletPlan2.setColumns(columns);
    insertTabletPlan2.setRowCount(times.length);
    insertTabletPlan2.setDeviceMNode(deviceMNode);

    processor.insertTablet(insertTabletPlan2);
    processor.asyncCloseAllWorkingTsFileProcessors();
    processor.syncCloseAllWorkingTsFileProcessors();

    QueryDataSource queryDataSource = processor.query(new PartialPath(deviceId), measurementId, context,
        null, null);

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

    QueryDataSource queryDataSource = processor.query(new PartialPath(deviceId), measurementId, context,
        null, null);
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
  public void testMerge() throws WriteProcessException, QueryProcessException, IllegalPathException {

    mergeLock = new AtomicLong(0);
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
    processor.merge(true);
    while (mergeLock.get() == 0) {
      // wait
    }

    QueryDataSource queryDataSource = processor.query(new PartialPath(deviceId), measurementId, context,
        null, null);
    Assert.assertEquals(10, queryDataSource.getSeqResources().size());
    Assert.assertEquals(0, queryDataSource.getUnseqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      Assert.assertTrue(resource.isClosed());
    }
    for (TsFileResource resource : queryDataSource.getUnseqResources()) {
      Assert.assertTrue(resource.isClosed());
    }
  }

  class DummySGP extends StorageGroupProcessor {

    DummySGP(String systemInfoDir, String storageGroupName) throws StorageGroupProcessorException {
      super(systemInfoDir, storageGroupName, new TsFileFlushPolicy.DirectFlushPolicy());
    }

    @Override
    protected void mergeEndAction(List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles,
        File mergeLog) {
      super.mergeEndAction(seqFiles, unseqFiles, mergeLog);
      mergeLock.incrementAndGet();
      assertFalse(mergeLog.exists());
    }
  }
}