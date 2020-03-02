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

import org.apache.iotdb.db.conf.adapter.ActiveTimeSeriesCounter;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.MetadataManagerHelper;
import org.apache.iotdb.db.engine.flush.TsFileFlushPolicy;
import org.apache.iotdb.db.engine.merge.manage.MergeManager;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.storageGroup.StorageGroupProcessorException;
import org.apache.iotdb.db.qp.physical.crud.BatchInsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertFalse;

public class StorageGroupProcessorTest {

  private String storageGroup = "root.vehicle.d0";
  private String systemDir = TestConstant.OUTPUT_DATA_DIR.concat("info");
  private String deviceId = "root.vehicle.d0";
  private String measurementId = "s0";
  private StorageGroupProcessor processor;
  private QueryContext context = EnvironmentUtils.TEST_QUERY_CONTEXT;
  private AtomicLong mergeLock;

  @Before
  public void setUp() throws Exception {
    MetadataManagerHelper.initMetadata();
    EnvironmentUtils.envSetUp();
    ActiveTimeSeriesCounter.getInstance().init(storageGroup);
    processor = new DummySGP(systemDir, storageGroup);
    MergeManager.getINSTANCE().start();
  }

  @After
  public void tearDown() throws Exception {
    processor.syncDeleteDataFiles();
    EnvironmentUtils.cleanEnv();
    EnvironmentUtils.cleanDir(TestConstant.OUTPUT_DATA_DIR);
    MergeManager.getINSTANCE().stop();
    EnvironmentUtils.cleanEnv();
  }


  @Test
  public void testUnseqUnsealedDelete() throws QueryProcessException, IOException {
    TSRecord record = new TSRecord(10000, deviceId);
    record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(1000)));
    processor.insert(new InsertPlan(record));
    processor.waitForAllCurrentTsFileProcessorsClosed();

    for (int j = 1; j <= 10; j++) {
      record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      processor.insert(new InsertPlan(record));
    }

    for (TsFileProcessor tsfileProcessor : processor.getWorkUnsequenceTsFileProcessor()) {
      tsfileProcessor.syncFlush();
    }

    for (int j = 11; j <= 20; j++) {
      record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      processor.insert(new InsertPlan(record));
    }

    processor.delete(deviceId, measurementId, 15L);

    Pair<List<ReadOnlyMemChunk>, List<ChunkMetaData>> pair = null;
    for (TsFileProcessor tsfileProcessor : processor.getWorkUnsequenceTsFileProcessor()) {
      pair = tsfileProcessor
          .query(deviceId, measurementId, TSDataType.INT32, TSEncoding.RLE, Collections.emptyMap(),
              new QueryContext());
      break;
    }

    List<ReadOnlyMemChunk> memChunks = pair.left;

    long time = 16;
    for (ReadOnlyMemChunk memChunk : memChunks) {
      IPointReader iterator = memChunk.getPointReader();
      while (iterator.hasNextTimeValuePair()) {
        TimeValuePair timeValuePair = iterator.nextTimeValuePair();
        Assert.assertEquals(time++, timeValuePair.getTimestamp());
      }
    }

    Assert.assertEquals(0, pair.right.size());
  }

  @Test
  public void testSequenceSyncClose() throws QueryProcessException {
    for (int j = 1; j <= 10; j++) {
      TSRecord record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      processor.insert(new InsertPlan(record));
      processor.putAllWorkingTsFileProcessorIntoClosingList();
    }

    processor.waitForAllCurrentTsFileProcessorsClosed();
    QueryDataSource queryDataSource = processor.query(deviceId, measurementId, context,
        null, null);

    Assert.assertEquals(10, queryDataSource.getSeqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      Assert.assertTrue(resource.isClosed());
    }
  }

  @Test
  public void testIoTDBRowBatchWriteAndSyncClose() throws QueryProcessException {

    String[] measurements = new String[2];
    measurements[0] = "s0";
    measurements[1] = "s1";
    List<Integer> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.INT32.ordinal());
    dataTypes.add(TSDataType.INT64.ordinal());

    BatchInsertPlan batchInsertPlan1 = new BatchInsertPlan("root.vehicle.d0", measurements,
        dataTypes);

    long[] times = new long[100];
    Object[] columns = new Object[2];
    columns[0] = new int[100];
    columns[1] = new long[100];

    for (int r = 0; r < 100; r++) {
      times[r] = r;
      ((int[]) columns[0])[r] = 1;
      ((long[]) columns[1])[r] = 1;
    }
    batchInsertPlan1.setTimes(times);
    batchInsertPlan1.setColumns(columns);
    batchInsertPlan1.setRowCount(times.length);

    processor.insertBatch(batchInsertPlan1);
    processor.putAllWorkingTsFileProcessorIntoClosingList();

    BatchInsertPlan batchInsertPlan2 = new BatchInsertPlan("root.vehicle.d0", measurements,
        dataTypes);

    for (int r = 50; r < 149; r++) {
      times[r - 50] = r;
      ((int[]) columns[0])[r - 50] = 1;
      ((long[]) columns[1])[r - 50] = 1;
    }
    batchInsertPlan2.setTimes(times);
    batchInsertPlan2.setColumns(columns);
    batchInsertPlan2.setRowCount(times.length);

    processor.insertBatch(batchInsertPlan2);
    processor.putAllWorkingTsFileProcessorIntoClosingList();
    processor.waitForAllCurrentTsFileProcessorsClosed();

    QueryDataSource queryDataSource = processor.query(deviceId, measurementId, context,
        null, null);

    Assert.assertEquals(2, queryDataSource.getSeqResources().size());
    Assert.assertEquals(1, queryDataSource.getUnseqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      Assert.assertTrue(resource.isClosed());
    }
  }


  @Test
  public void testSeqAndUnSeqSyncClose() throws QueryProcessException {

    for (int j = 21; j <= 30; j++) {
      TSRecord record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      processor.insert(new InsertPlan(record));
      processor.putAllWorkingTsFileProcessorIntoClosingList();
    }
    processor.waitForAllCurrentTsFileProcessorsClosed();

    for (int j = 10; j >= 1; j--) {
      TSRecord record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      processor.insert(new InsertPlan(record));
      processor.putAllWorkingTsFileProcessorIntoClosingList();
    }

    processor.waitForAllCurrentTsFileProcessorsClosed();

    QueryDataSource queryDataSource = processor.query(deviceId, measurementId, context,
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
  public void testMerge() throws QueryProcessException {

    mergeLock = new AtomicLong(0);
    for (int j = 21; j <= 30; j++) {
      TSRecord record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      processor.insert(new InsertPlan(record));
      processor.putAllWorkingTsFileProcessorIntoClosingList();
    }
    processor.waitForAllCurrentTsFileProcessorsClosed();

    for (int j = 10; j >= 1; j--) {
      TSRecord record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      processor.insert(new InsertPlan(record));
      processor.putAllWorkingTsFileProcessorIntoClosingList();
    }

    processor.waitForAllCurrentTsFileProcessorsClosed();
    processor.merge(true);
    while (mergeLock.get() == 0) {
      // wait
    }

    QueryDataSource queryDataSource = processor.query(deviceId, measurementId, context,
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

    DummySGP(String systemInfoDir, String storageGroupName)
        throws StorageGroupProcessorException, MetadataException {
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