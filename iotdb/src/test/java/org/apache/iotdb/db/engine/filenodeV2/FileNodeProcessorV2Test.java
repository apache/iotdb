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
package org.apache.iotdb.db.engine.filenodeV2;

import org.apache.iotdb.db.engine.MetadataManagerHelper;
import org.apache.iotdb.db.engine.querycontext.QueryDataSourceV2;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FileNodeProcessorV2Test {

  private String storageGroup = "storage_group1";
  private String systemDir = "data/info";
  private String deviceId = "root.vehicle.d0";
  private String measurementId = "s0";
  private FileNodeProcessorV2 processor;

  @Before
  public void setUp() throws Exception {
    MetadataManagerHelper.initMetadata();
    EnvironmentUtils.envSetUp();
    processor = new FileNodeProcessorV2(systemDir, storageGroup);
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    EnvironmentUtils.cleanDir(systemDir);
  }


  @Test
  public void testSequenceSyncClose() {
    for (int j = 1; j <= 100; j++) {
      TSRecord record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      processor.insert(record);
      processor.asyncForceClose();
    }

    processor.syncCloseFileNode();
    QueryDataSourceV2 queryDataSource = processor.query(deviceId, measurementId);
    Assert.assertEquals(queryDataSource.getSeqDataSource().getQueryTsFiles().size(), 100);
    for (TsFileResourceV2 resource : queryDataSource.getSeqDataSource().getQueryTsFiles()) {
      Assert.assertTrue(resource.isClosed());
    }
  }


  @Test
  public void testSeqAndUnSeqSyncClose() {

    for (int j = 21; j <= 30; j++) {
      TSRecord record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      processor.insert(record);
      processor.asyncForceClose();
    }
    processor.syncCloseFileNode();

    for (int j = 10; j >= 1; j--) {
      TSRecord record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      processor.insert(record);
      processor.asyncForceClose();
    }

    processor.syncCloseFileNode();

    QueryDataSourceV2 queryDataSource = processor.query(deviceId, measurementId);
    Assert.assertEquals(10, queryDataSource.getSeqDataSource().getQueryTsFiles().size());
    Assert.assertEquals(10, queryDataSource.getUnSequenceDataSource().getQueryTsFiles().size());
    for (TsFileResourceV2 resource : queryDataSource.getSeqDataSource().getQueryTsFiles()) {
      Assert.assertTrue(resource.isClosed());
    }
    for (TsFileResourceV2 resource : queryDataSource.getUnSequenceDataSource().getQueryTsFiles()) {
      Assert.assertTrue(resource.isClosed());
    }
  }

}