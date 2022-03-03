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

package org.apache.iotdb.db.engine.merge;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.merge.manage.MergeManager;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.task.MergeTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MergeVerificationTest extends MergeTest {

  private int prevMergeChunkThreshold;
  File tempSGDir;

  @Before
  public void setUp() throws MetadataException, IOException, WriteProcessException {
    IoTDB.metaManager.init();
    MergeManager.getINSTANCE().start();
    prevMergeChunkThreshold =
        IoTDBDescriptor.getInstance().getConfig().getMergeChunkPointNumberThreshold();
    IoTDBDescriptor.getInstance().getConfig().setMergeChunkPointNumberThreshold(-1);
    this.measurementNum = 2;
    this.deviceNum = 1;
    prepareSeries();
    this.prepareFiles();
    tempSGDir = new File(TestConstant.OUTPUT_DATA_DIR);
    if (!tempSGDir.exists()) {
      Assert.assertTrue(tempSGDir.mkdirs());
    }
  }

  @After
  public void tearDown() throws StorageEngineException, IOException {
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setMergeChunkPointNumberThreshold(prevMergeChunkThreshold);
    super.tearDown();
  }

  private void prepareFiles() throws IOException, WriteProcessException {
    // first file time range: [0, 100]
    // first measurement: [0, 90]
    // second measurement: [0, 100]
    File firstFile =
        new File(
            TestConstant.OUTPUT_DATA_DIR.concat(
                1
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 1
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile"));
    TsFileResource firstTsFileResource = new TsFileResource(firstFile);
    firstTsFileResource.setClosed(true);
    firstTsFileResource.setMinPlanIndex(1);
    firstTsFileResource.setMaxPlanIndex(1);
    firstTsFileResource.setVersion(1);
    seqResources.add(firstTsFileResource);
    if (!firstFile.getParentFile().exists()) {
      Assert.assertTrue(firstFile.getParentFile().mkdirs());
    }

    TsFileWriter fileWriter = new TsFileWriter(firstFile);
    for (String deviceId : deviceIds) {
      for (MeasurementSchema measurementSchema : measurementSchemas) {
        fileWriter.registerTimeseries(
            new Path(deviceId, measurementSchema.getMeasurementId()), measurementSchema);
      }
    }
    for (long i = 0; i < 100; ++i) {
      for (int j = 0; j < deviceNum; j++) {
        TSRecord record = new TSRecord(i, deviceIds[j]);
        if (i < 90) {
          record.addTuple(
              DataPoint.getDataPoint(
                  measurementSchemas[0].getType(),
                  measurementSchemas[0].getMeasurementId(),
                  String.valueOf(i)));
        }
        record.addTuple(
            DataPoint.getDataPoint(
                measurementSchemas[1].getType(),
                measurementSchemas[1].getMeasurementId(),
                String.valueOf(i)));
        fileWriter.write(record);
        firstTsFileResource.updateStartTime(deviceIds[j], i);
        firstTsFileResource.updateEndTime(deviceIds[j], i);
      }
    }
    for (int j = 0; j < deviceNum; j++) {
      TSRecord record = new TSRecord(100L, deviceIds[j]);
      record.addTuple(
          DataPoint.getDataPoint(
              measurementSchemas[1].getType(),
              measurementSchemas[1].getMeasurementId(),
              String.valueOf(100)));
      fileWriter.write(record);
      firstTsFileResource.updateStartTime(deviceIds[j], 100L);
      firstTsFileResource.updateEndTime(deviceIds[j], 100L);
    }

    fileWriter.flushAllChunkGroups();
    fileWriter.close();

    // second file time range: [101, 200]
    // first measurement: [101, 200]
    // second measurement: [101, 200]
    File secondFile =
        new File(
            TestConstant.OUTPUT_DATA_DIR.concat(
                2
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 2
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile"));
    TsFileResource secondTsFileResource = new TsFileResource(secondFile);
    secondTsFileResource.setClosed(true);
    secondTsFileResource.setMinPlanIndex(2);
    secondTsFileResource.setMaxPlanIndex(2);
    secondTsFileResource.setVersion(2);
    seqResources.add(secondTsFileResource);

    if (!secondFile.getParentFile().exists()) {
      Assert.assertTrue(secondFile.getParentFile().mkdirs());
    }
    fileWriter = new TsFileWriter(secondFile);
    for (String deviceId : deviceIds) {
      for (MeasurementSchema measurementSchema : measurementSchemas) {
        fileWriter.registerTimeseries(
            new Path(deviceId, measurementSchema.getMeasurementId()), measurementSchema);
      }
    }
    for (long i = 101; i < 201; ++i) {
      for (int j = 0; j < deviceNum; j++) {
        TSRecord record = new TSRecord(i, deviceIds[j]);
        for (int k = 0; k < measurementNum; k++) {
          record.addTuple(
              DataPoint.getDataPoint(
                  measurementSchemas[k].getType(),
                  measurementSchemas[k].getMeasurementId(),
                  String.valueOf(i)));
        }
        fileWriter.write(record);
        secondTsFileResource.updateStartTime(deviceIds[j], i);
        secondTsFileResource.updateEndTime(deviceIds[j], i);
      }
    }
    fileWriter.flushAllChunkGroups();
    fileWriter.close();

    // unseq file: [90, 110]
    // first measurement: [90, 110]
    // second measurement: [90, 110]
    File thirdFile =
        new File(
            TestConstant.OUTPUT_DATA_DIR.concat(
                3
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 3
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile"));
    TsFileResource thirdTsFileResource = new TsFileResource(thirdFile);
    thirdTsFileResource.setClosed(true);
    thirdTsFileResource.setMinPlanIndex(2);
    thirdTsFileResource.setMaxPlanIndex(2);
    thirdTsFileResource.setVersion(2);
    unseqResources.add(thirdTsFileResource);

    if (!secondFile.getParentFile().exists()) {
      Assert.assertTrue(thirdFile.getParentFile().mkdirs());
    }
    fileWriter = new TsFileWriter(thirdFile);
    for (String deviceId : deviceIds) {
      for (MeasurementSchema measurementSchema : measurementSchemas) {
        fileWriter.registerTimeseries(
            new Path(deviceId, measurementSchema.getMeasurementId()), measurementSchema);
      }
    }
    for (long i = 90; i < 111; ++i) {
      for (int j = 0; j < deviceNum; j++) {
        TSRecord record = new TSRecord(i, deviceIds[j]);
        for (int k = 0; k < measurementNum; k++) {
          record.addTuple(
              DataPoint.getDataPoint(
                  measurementSchemas[k].getType(),
                  measurementSchemas[k].getMeasurementId(),
                  String.valueOf(i)));
        }
        fileWriter.write(record);
        thirdTsFileResource.updateStartTime(deviceIds[j], i);
        thirdTsFileResource.updateEndTime(deviceIds[j], i);
      }
    }
    fileWriter.flushAllChunkGroups();
    fileWriter.close();
  }

  @Test
  public void testNotOverlapAfterMerge() throws Exception {
    MergeTask mergeTask =
        new MergeTask(
            new MergeResource(seqResources, unseqResources),
            tempSGDir.getPath(),
            (k, v, l) -> {},
            "test",
            true,
            1,
            MERGE_TEST_SG);
    mergeTask.call();
    Map<String, Long> deviceToEndTimeMap = new HashMap<>();
    for (TsFileResource resource : seqResources) {
      for (String deviceId : deviceIds) {
        long currentEndTime = resource.getEndTime(deviceId);
        long currentStartTime = resource.getStartTime(deviceId);
        if (deviceToEndTimeMap.containsKey(deviceId)) {
          Assert.assertTrue(deviceToEndTimeMap.get(deviceId) < currentStartTime);
        }
        deviceToEndTimeMap.put(deviceId, currentEndTime);
      }
    }
  }
}
