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

import org.apache.iotdb.db.engine.MetadataManagerHelper;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageGroupProcessorException;
import org.apache.iotdb.db.qp.physical.crud.BatchInsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.service.rpc.thrift.IoTDBDataType;
import org.apache.iotdb.service.rpc.thrift.TSDataValueList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class StorageGroupProcessorTest {

  private String storageGroup = "storage_group1";
  private String systemDir = "data/info";
  private String deviceId = "root.vehicle.d0";
  private String measurementId = "s0";
  private StorageGroupProcessor processor;
  private QueryContext context = EnvironmentUtils.TEST_QUERY_CONTEXT;

  @Before
  public void setUp() throws Exception {
    MetadataManagerHelper.initMetadata();
    EnvironmentUtils.envSetUp();
    processor = new StorageGroupProcessor(systemDir, storageGroup);
  }

  @After
  public void tearDown() throws Exception {
    processor.syncDeleteDataFiles();
    EnvironmentUtils.cleanEnv();
    EnvironmentUtils.cleanDir("data");
  }


  @Test
  public void testSequenceSyncClose() {
    for (int j = 1; j <= 10; j++) {
      TSRecord record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      processor.insert(new InsertPlan(record));
      processor.putAllWorkingTsFileProcessorIntoClosingList();
    }

    processor.waitForAllCurrentTsFileProcessorsClosed();
    QueryDataSource queryDataSource = processor.query(deviceId, measurementId, context);

    Assert.assertEquals(10, queryDataSource.getSeqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      Assert.assertTrue(resource.isClosed());
    }
  }

  @Test
  public void testIoTDBRowBatchWriteAndSyncClose() {

    String[] measurements = new String[2];
    measurements[0] = "s0";
    measurements[1] = "s1";
    IoTDBDataType[] dataTypes = new IoTDBDataType[2];
    dataTypes[0] = IoTDBDataType.INT32;
    dataTypes[1] = IoTDBDataType.INT64;

    BatchInsertPlan batchInsertPlan1 = new BatchInsertPlan("root.vehicle.d0", measurements, dataTypes);

    Long[] times = new Long[100];
    TSDataValueList[] dataValueLists = new TSDataValueList[2];
    dataValueLists[0] = new TSDataValueList();
    dataValueLists[1] = new TSDataValueList();
    for (long i = 1; i <= 100; i++) {
      times[(int) i-1] = i;
      dataValueLists[0].addToInt_vals((int) i);
      dataValueLists[1].addToLong_vals(i);
    }

    batchInsertPlan1.setTimes(times);
    batchInsertPlan1.setColumns(dataValueLists);
    batchInsertPlan1.setRowCount(times.length);

    processor.insertBatch(batchInsertPlan1);
    processor.putAllWorkingTsFileProcessorIntoClosingList();

    BatchInsertPlan batchInsertPlan2 = new BatchInsertPlan("root.vehicle.d0", measurements, dataTypes);

    times = new Long[100];
    dataValueLists = new TSDataValueList[2];
    dataValueLists[0] = new TSDataValueList();
    dataValueLists[1] = new TSDataValueList();
    for (long i = 50; i <= 149; i++) {
      times[(int) i-50] = i;
      dataValueLists[0].addToInt_vals((int) i);
      dataValueLists[1].addToLong_vals(i);
    }

    batchInsertPlan2.setTimes(times);
    batchInsertPlan2.setColumns(dataValueLists);
    batchInsertPlan2.setRowCount(times.length);

    processor.insertBatch(batchInsertPlan2);
    processor.putAllWorkingTsFileProcessorIntoClosingList();
    processor.waitForAllCurrentTsFileProcessorsClosed();

    QueryDataSource queryDataSource = processor.query(deviceId, measurementId, context);

    Assert.assertEquals(2, queryDataSource.getSeqResources().size());
    Assert.assertEquals(1, queryDataSource.getUnseqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      Assert.assertTrue(resource.isClosed());
    }
  }


  @Test
  public void testSeqAndUnSeqSyncClose() throws StorageGroupProcessorException {

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

    QueryDataSource queryDataSource = processor.query(deviceId, measurementId, context);
    Assert.assertEquals(10, queryDataSource.getSeqResources().size());
    Assert.assertEquals(10, queryDataSource.getUnseqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      Assert.assertTrue(resource.isClosed());
    }
    for (TsFileResource resource : queryDataSource.getUnseqResources()) {
      Assert.assertTrue(resource.isClosed());
    }
  }

}