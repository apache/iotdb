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
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.memtable.TsFileProcessor;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.DeviceTimeIndex;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

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
  public void testRecoverLastFlushTimeMapFromDeviceTimeIndex()
      throws IOException, IllegalPathException, WriteProcessException {
    TSRecord record = new TSRecord(10000, "root.vehicle.d0");
    record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(1000)));
    dataRegion.insert(DataRegionTest.buildInsertRowNodeByTSRecord(record));
    dataRegion.syncCloseAllWorkingTsFileProcessors();

    record = new TSRecord(9999, "root.vehicle.d1");
    record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(1000)));
    dataRegion.insert(DataRegionTest.buildInsertRowNodeByTSRecord(record));
    dataRegion.syncCloseAllWorkingTsFileProcessors();

    for (int j = 1; j <= 10; j++) {
      record = new TSRecord(j, "root.vehicle.d0");
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      dataRegion.insert(DataRegionTest.buildInsertRowNodeByTSRecord(record));
    }

    for (TsFileProcessor tsfileProcessor : dataRegion.getWorkUnsequenceTsFileProcessors()) {
      tsfileProcessor.syncFlush();
    }
    Assert.assertEquals(
        10000, dataRegion.getLastFlushTimeMap().getFlushedTime(0, "root.vehicle.d0"));

    dataRegion.getLastFlushTimeMap().clearFlushedTime();
    dataRegion.getLastFlushTimeMap().checkAndCreateFlushedTimePartition(0);
    Assert.assertEquals(
        10000, dataRegion.getLastFlushTimeMap().getFlushedTime(0, "root.vehicle.d0"));
  }

  @Test
  public void testRecoverLastFlushTimeMapFromFileTimeIndex()
      throws IOException, IllegalPathException, WriteProcessException {
    TSRecord record = new TSRecord(10000, "root.vehicle.d0");
    record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(1000)));
    dataRegion.insert(DataRegionTest.buildInsertRowNodeByTSRecord(record));
    dataRegion.syncCloseAllWorkingTsFileProcessors();

    record = new TSRecord(9999, "root.vehicle.d1");
    record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(1000)));
    dataRegion.insert(DataRegionTest.buildInsertRowNodeByTSRecord(record));
    dataRegion.syncCloseAllWorkingTsFileProcessors();

    for (int j = 1; j <= 10; j++) {
      record = new TSRecord(j, "root.vehicle.d0");
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      dataRegion.insert(DataRegionTest.buildInsertRowNodeByTSRecord(record));
    }

    for (TsFileProcessor tsfileProcessor : dataRegion.getWorkUnsequenceTsFileProcessors()) {
      tsfileProcessor.syncFlush();
    }
    dataRegion.syncCloseAllWorkingTsFileProcessors();
    Assert.assertEquals(
        10000, dataRegion.getLastFlushTimeMap().getFlushedTime(0, "root.vehicle.d0"));

    dataRegion.getLastFlushTimeMap().clearFlushedTime();
    dataRegion.getLastFlushTimeMap().checkAndCreateFlushedTimePartition(0);
    List<TsFileResource> seqs = dataRegion.getSequenceFileList();
    for (TsFileResource res : seqs) {
      res.degradeTimeIndex();
    }
    List<TsFileResource> unseqs = dataRegion.getUnSequenceFileList();
    for (TsFileResource res : unseqs) {
      res.degradeTimeIndex();
    }
    Assert.assertEquals(
        10000, dataRegion.getLastFlushTimeMap().getFlushedTime(0, "root.vehicle.d0"));
  }

  @Test
  public void testRecoverDeviceLastFlushedTimeWhenLargestTimestampInUnSeqSpace() {
    String device = "root.testsg.d1";
    File seqResourceFile1 = new File("1-1-0-0.tsfile.resource");
    TsFileResource seqResource1 = new TsFileResource();
    seqResource1.setFile(seqResourceFile1);
    seqResource1.setTimeIndex(new DeviceTimeIndex());
    seqResource1.updateStartTime(device, 10);
    seqResource1.updateEndTime(device, 20);

    File seqResourceFile2 = new File("2-2-0-0.tsfile.resource");
    TsFileResource seqResource2 = new TsFileResource();
    seqResource2.setTimeIndex(new DeviceTimeIndex());
    seqResource2.setFile(seqResourceFile2);
    seqResource2.updateStartTime(device, 30);
    seqResource2.updateEndTime(device, 40);

    File seqResourceFile3 = new File("3-3-0-0.tsfile.resource");
    TsFileResource seqResource3 = new TsFileResource();
    seqResource3.setTimeIndex(new DeviceTimeIndex());
    seqResource3.setFile(seqResourceFile3);
    seqResource3.updateStartTime(device, 50);
    seqResource3.updateEndTime(device, 60);

    File unseqResourceFile1 = new File("4-4-0-0.tsfile.resource");
    TsFileResource unseqResource1 = new TsFileResource();
    unseqResource1.setTimeIndex(new DeviceTimeIndex());
    unseqResource1.setFile(unseqResourceFile1);
    unseqResource1.updateStartTime(device, 1);
    unseqResource1.updateEndTime(device, 100);

    File unseqResourceFile2 = new File("5-5-0-0.tsfile.resource");
    TsFileResource unseqResource2 = new TsFileResource();
    unseqResource2.setTimeIndex(new DeviceTimeIndex());
    unseqResource2.setFile(unseqResourceFile2);
    unseqResource2.updateStartTime(device, 1);
    unseqResource2.updateEndTime(device, 10);

    File unseqResourceFile3 = new File("6-6-0-0.tsfile.resource");
    TsFileResource unseqResource3 = new TsFileResource();
    unseqResource3.setTimeIndex(new DeviceTimeIndex());
    unseqResource3.setFile(unseqResourceFile3);
    unseqResource3.updateStartTime(device, 1);
    unseqResource3.updateEndTime(device, 70);

    TsFileManager tsFileManager = dataRegion.getTsFileManager();
    tsFileManager.add(seqResource1, true);
    tsFileManager.add(seqResource2, true);
    tsFileManager.add(seqResource3, true);
    tsFileManager.add(unseqResource1, false);
    tsFileManager.add(unseqResource2, false);
    tsFileManager.add(unseqResource3, false);

    dataRegion.getLastFlushTimeMap().checkAndCreateFlushedTimePartition(0);

    Assert.assertEquals(100, dataRegion.getLastFlushTimeMap().getFlushedTime(0, device));
    tsFileManager.clear();
  }
}
