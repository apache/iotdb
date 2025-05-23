/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.dataregion;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ArrayDeviceTimeIndex;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.DataPoint;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class LastFlushTimeMapTest {

  private DataRegion dataRegion;

  private String storageGroup = "root.vehicle.d0";
  private String measurementId = "s0";
  private String systemDir = TestConstant.OUTPUT_DATA_DIR.concat("info");

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    dataRegion = new DataRegionTest.DummyDataRegion(systemDir, storageGroup);
    StorageEngine.getInstance().setDataRegion(new DataRegionId(0), dataRegion);
    CompactionTaskManager.getInstance().start();
  }

  @After
  public void tearDown() throws Exception {
    if (dataRegion != null) {
      dataRegion.syncDeleteDataFiles();
      StorageEngine.getInstance().deleteDataRegion(new DataRegionId(0));
    }
    EnvironmentUtils.cleanDir(TestConstant.OUTPUT_DATA_DIR);
    CompactionTaskManager.getInstance().stop();
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testDeviceLastFlushTimeMap()
      throws IOException, IllegalPathException, WriteProcessException {
    TSRecord record = new TSRecord("root.vehicle.d0", 10000);
    record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(1000)));
    dataRegion.insert(DataRegionTest.buildInsertRowNodeByTSRecord(record));
    dataRegion.syncCloseAllWorkingTsFileProcessors();

    record = new TSRecord("root.vehicle.d1", 9999);
    record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(1000)));
    dataRegion.insert(DataRegionTest.buildInsertRowNodeByTSRecord(record));
    dataRegion.syncCloseAllWorkingTsFileProcessors();

    for (int j = 1; j <= 10; j++) {
      record = new TSRecord("root.vehicle.d0", j);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      dataRegion.insert(DataRegionTest.buildInsertRowNodeByTSRecord(record));
    }

    dataRegion.syncCloseWorkingTsFileProcessors(false);
    Assert.assertEquals(
        10000,
        dataRegion
            .getLastFlushTimeMap()
            .getFlushedTime(0, IDeviceID.Factory.DEFAULT_FACTORY.create("root.vehicle.d0")));
  }

  @Test
  public void testPartitionLastFlushTimeMap()
      throws IOException, IllegalPathException, WriteProcessException {
    TSRecord record = new TSRecord("root.vehicle.d0", 10000);
    record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(1000)));
    dataRegion.insert(DataRegionTest.buildInsertRowNodeByTSRecord(record));
    dataRegion.syncCloseAllWorkingTsFileProcessors();

    record = new TSRecord("root.vehicle.d1", 9999);
    record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(1000)));
    dataRegion.insert(DataRegionTest.buildInsertRowNodeByTSRecord(record));
    dataRegion.syncCloseAllWorkingTsFileProcessors();

    for (int j = 1; j <= 10; j++) {
      record = new TSRecord("root.vehicle.d0", j);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      dataRegion.insert(DataRegionTest.buildInsertRowNodeByTSRecord(record));
    }
    dataRegion.syncCloseAllWorkingTsFileProcessors();
    Assert.assertEquals(
        10000,
        dataRegion
            .getLastFlushTimeMap()
            .getFlushedTime(0, IDeviceID.Factory.DEFAULT_FACTORY.create("root.vehicle.d0")));
    Assert.assertEquals(
        9999,
        dataRegion
            .getLastFlushTimeMap()
            .getFlushedTime(0, IDeviceID.Factory.DEFAULT_FACTORY.create("root.vehicle.d1")));

    dataRegion.getLastFlushTimeMap().degradeLastFlushTime(0);
    Assert.assertEquals(
        10000,
        dataRegion
            .getLastFlushTimeMap()
            .getFlushedTime(0, IDeviceID.Factory.DEFAULT_FACTORY.create("root.vehicle.d0")));
    Assert.assertEquals(
        10000,
        dataRegion
            .getLastFlushTimeMap()
            .getFlushedTime(0, IDeviceID.Factory.DEFAULT_FACTORY.create("root.vehicle.d1")));
  }

  @Test
  public void testRecoverLastFlushTimeMap()
      throws IOException, IllegalPathException, WriteProcessException, DataRegionException {
    TSRecord record = new TSRecord("root.vehicle.d0", 604_800_000);
    record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(1000)));
    dataRegion.insert(DataRegionTest.buildInsertRowNodeByTSRecord(record));
    dataRegion.syncCloseAllWorkingTsFileProcessors();

    record = new TSRecord("root.vehicle.d0", 604_799_999);
    record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(1000)));
    dataRegion.insert(DataRegionTest.buildInsertRowNodeByTSRecord(record));
    dataRegion.syncCloseAllWorkingTsFileProcessors();

    for (int j = 1; j <= 10; j++) {
      record = new TSRecord("root.vehicle.d0", j);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      dataRegion.insert(DataRegionTest.buildInsertRowNodeByTSRecord(record));
    }
    dataRegion.syncCloseAllWorkingTsFileProcessors();
    Assert.assertEquals(
        604_800_000,
        dataRegion
            .getLastFlushTimeMap()
            .getFlushedTime(1, IDeviceID.Factory.DEFAULT_FACTORY.create("root.vehicle.d0")));
    Assert.assertEquals(
        604_799_999,
        dataRegion
            .getLastFlushTimeMap()
            .getFlushedTime(0, IDeviceID.Factory.DEFAULT_FACTORY.create("root.vehicle.d0")));

    // recover from disk
    dataRegion = new DataRegionTest.DummyDataRegion(systemDir, storageGroup);
    Assert.assertEquals(
        604_800_000,
        dataRegion
            .getLastFlushTimeMap()
            .getFlushedTime(1, IDeviceID.Factory.DEFAULT_FACTORY.create("root.vehicle.d0")));
    Assert.assertEquals(
        604_799_999,
        dataRegion
            .getLastFlushTimeMap()
            .getFlushedTime(0, IDeviceID.Factory.DEFAULT_FACTORY.create("root.vehicle.d0")));
  }

  @Test
  public void testLastFlushedTimeWhenLargestTimestampInUnSeqSpace()
      throws IllegalPathException, WriteProcessException {
    String unseqDirPath =
        TestConstant.BASE_OUTPUT_PATH
            + "data"
            + File.separator
            + "unsequence"
            + File.separator
            + "root.vehicle"
            + File.separator
            + "0"
            + File.separator
            + "0";
    for (int j = 1; j <= 10; j++) {
      TSRecord record = new TSRecord("root.vehicle.d0", j);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      dataRegion.insert(DataRegionTest.buildInsertRowNodeByTSRecord(record));
    }
    dataRegion.syncCloseAllWorkingTsFileProcessors();

    IDeviceID device = IDeviceID.Factory.DEFAULT_FACTORY.create("root.vehicle.d0");
    File unseqResourceFile1 = new File(unseqDirPath + File.separator + "4-4-0-0.tsfile.resource");
    TsFileResource unseqResource1 = new TsFileResource();
    unseqResource1.setTimeIndex(new ArrayDeviceTimeIndex());
    unseqResource1.setFile(unseqResourceFile1);
    unseqResource1.updateStartTime(device, 1);
    unseqResource1.updateEndTime(device, 100);
    dataRegion.updateDeviceLastFlushTime(unseqResource1);

    File unseqResourceFile2 = new File(unseqDirPath + File.separator + "5-5-0-0.tsfile.resource");
    TsFileResource unseqResource2 = new TsFileResource();
    unseqResource2.setTimeIndex(new ArrayDeviceTimeIndex());
    unseqResource2.setFile(unseqResourceFile2);
    unseqResource2.updateStartTime(device, 1);
    unseqResource2.updateEndTime(device, 10);
    dataRegion.updateDeviceLastFlushTime(unseqResource2);

    File unseqResourceFile3 = new File(unseqDirPath + File.separator + "6-6-0-0.tsfile.resource");
    TsFileResource unseqResource3 = new TsFileResource();
    unseqResource3.setTimeIndex(new ArrayDeviceTimeIndex());
    unseqResource3.setFile(unseqResourceFile3);
    unseqResource3.updateStartTime(device, 1);
    unseqResource3.updateEndTime(device, 70);
    dataRegion.updateDeviceLastFlushTime(unseqResource3);

    Assert.assertEquals(100, dataRegion.getLastFlushTimeMap().getFlushedTime(0, device));
    dataRegion.getLastFlushTimeMap().degradeLastFlushTime(0);
    Assert.assertEquals(100, dataRegion.getLastFlushTimeMap().getFlushedTime(0, device));
  }
}
