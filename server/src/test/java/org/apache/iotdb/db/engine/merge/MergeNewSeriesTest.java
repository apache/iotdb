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

import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.task.MergeTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * In the test, the unseq file has a new series that has never been written to seq files. However,
 * other timeseries of its belonging device have already been written to the seq files. As a result,
 * the unseq file and seq file are device-level overlapped but not series-level overlapped, and the
 * new series in the unseq file should be written into seq files according to their device time
 * range.
 */
public class MergeNewSeriesTest extends MergeTest {

  private MeasurementSchema[] unseqSchemas = new MeasurementSchema[1];

  @Override
  public void setUp() throws IOException, WriteProcessException, MetadataException {
    measurementNum = 3;
    deviceNum = 2;
    seqFileNum = 2;
    // unseq files are manually prepared because they will have new time series
    unseqFileNum = 0;
    super.setUp();
  }

  @Override
  protected void prepareSeries() throws MetadataException {
    super.prepareSeries();
    unseqSchemas[0] = toMeasurementSchema(1);
    createTimeseries(deviceIds, unseqSchemas);
  }

  @Override
  void prepareFiles(int seqFileNum, int unseqFileNum) throws IOException, WriteProcessException {
    // seq file1: root.MergeTest.d0.s0[0,99] root.MergeTest.d1.s0[0,99]
    // seq file2: root.MergeTest.d0.s0[100,199] root.MergeTest.d1.s0[100,199]
    super.prepareFiles(seqFileNum, unseqFileNum);
    // unseq file1: root.MergeTest.d0.s1[0,99]
    // unseq file2: root.MergeTest.d1.s1[100,199]
    // unseq1 should be written into seq1 while unseq2 should be written into seq2
    TsFileResource unseq1 = prepareResource(2);
    unseqResources.add(unseq1);
    TsFileResource unseq2 = prepareResource(3);
    unseqResources.add(unseq2);

    prepareFile(unseq1, 0, ptNum, 0, Arrays.copyOfRange(deviceIds, 0, 1), unseqSchemas);
    prepareFile(unseq2, ptNum, ptNum, 0, Arrays.copyOfRange(deviceIds, 1, 2), unseqSchemas);
  }

  @Test
  public void testFullMerge() throws Exception {
    MergeTask mergeTask =
        new MergeTask(
            new MergeResource(seqResources, unseqResources),
            TestConstant.OUTPUT_DATA_DIR,
            (k, v, l) -> {},
            "test",
            true,
            1,
            MERGE_TEST_SG);
    mergeTask.call();

    // query root.MergeTest.d0.s1 from seq1
    List<TsFileResource> resources = Collections.singletonList(seqResources.get(0));
    queryAndCheck(
        deviceIds[0], unseqSchemas[0], resources, checkResultFunc(0), checkResultCntFunc(ptNum));

    // query root.MergeTest.d1.s1 from seq2
    resources = Collections.singletonList(seqResources.get(1));
    queryAndCheck(
        deviceIds[1], unseqSchemas[0], resources, checkResultFunc(0), checkResultCntFunc(ptNum));

    // query root.MergeTest.d0.s1 from seq2
    resources = Collections.singletonList(seqResources.get(1));
    queryAndCheck(
        deviceIds[0], unseqSchemas[0], resources, checkResultFunc(0), checkResultCntFunc(0));

    // query root.MergeTest.d1.s1 from seq1
    resources = Collections.singletonList(seqResources.get(0));
    queryAndCheck(
        deviceIds[1], unseqSchemas[0], resources, checkResultFunc(0), checkResultCntFunc(0));
  }

  /**
   * seq file1: d0.s0[0,99], d1.s0[0,99]<br>
   * seq file2: d0.s0[100,199], d1.s0[100,199], d1.s2[100,199]<br>
   * unseq file1: d0.s1[0,99]<br>
   * unseq file2: d1.s1[100,199], d1.s2[50,150]<br>
   * target file1: d0.s0[0,99], d0.s1[0,99], d1.s0[0,99], d1.s2[50,99]<br>
   * target file2: d0.s0[100,199], d1.s0[100,199], d1.s1[100,199], d1.s2[100,199]
   *
   * <p>d0.s1 and d1.s1 is in unseq file but not in seq files, and d1.s2 is not in the first seq
   * file.
   */
  @Test
  public void testNewSeriesInUnseqFiles() throws Exception {
    List<TsFileResource> seqTsFileResources = new ArrayList<>();
    List<TsFileResource> unseqTsFileResources = new ArrayList<>();
    TsFileResource seq1 = prepareResource(0);
    seqTsFileResources.add(seq1);
    TsFileResource seq2 = prepareResource(1);
    seqTsFileResources.add(seq2);
    MeasurementSchema[] seqSchemas = new MeasurementSchema[1];
    seqSchemas[0] = toMeasurementSchema(0);
    // prepare seq file1
    prepareFile(seq1, 0, 100, 0, Arrays.copyOfRange(deviceIds, 0, 2), seqSchemas);

    // prepare seq file2
    TsFileWriter fileWriter = new TsFileWriter(seq2.getTsFile());
    for (String deviceId : Arrays.copyOfRange(deviceIds, 0, 2)) {
      for (MeasurementSchema measurementSchema : measurementSchemas) {
        fileWriter.registerTimeseries(
            new Path(deviceId, measurementSchema.getMeasurementId()), measurementSchema);
      }
    }
    for (long i = 100; i < 200; i++) {
      for (int deviceIndex = 0; deviceIndex < deviceIds.length; deviceIndex++) {
        String deviceId = deviceIds[deviceIndex];
        TSRecord record = new TSRecord(i, deviceId);
        if (deviceIndex == 0) {
          record.addTuple(
              DataPoint.getDataPoint(
                  measurementSchemas[0].getType(),
                  measurementSchemas[0].getMeasurementId(),
                  String.valueOf(i)));
        } else {
          record.addTuple(
              DataPoint.getDataPoint(
                  measurementSchemas[0].getType(),
                  measurementSchemas[0].getMeasurementId(),
                  String.valueOf(i)));
          record.addTuple(
              DataPoint.getDataPoint(
                  measurementSchemas[2].getType(),
                  measurementSchemas[2].getMeasurementId(),
                  String.valueOf(i)));
        }
        fileWriter.write(record);
        seq2.updateStartTime(deviceId, i);
        seq2.updateEndTime(deviceId, i);
      }
      if ((i + 1) % flushInterval == 0) {
        fileWriter.flushAllChunkGroups();
      }
    }
    fileWriter.close();

    TsFileResource unseq1 = prepareResource(2);
    unseqTsFileResources.add(unseq1);
    TsFileResource unseq2 = prepareResource(3);
    unseqTsFileResources.add(unseq2);

    // prepare unseq file1
    prepareFile(unseq1, 0, 100, 0, Arrays.copyOfRange(deviceIds, 0, 1), unseqSchemas);

    // prepare unseq file2
    fileWriter = new TsFileWriter(unseq2.getTsFile());
    for (String deviceId : Arrays.copyOfRange(deviceIds, 1, 2)) {
      for (MeasurementSchema measurementSchema : Arrays.copyOfRange(measurementSchemas, 1, 3)) {
        fileWriter.registerTimeseries(
            new Path(deviceId, measurementSchema.getMeasurementId()), measurementSchema);
      }
    }
    for (long i = 100; i < 200; i++) {
      for (String deviceId : Arrays.copyOfRange(deviceIds, 1, 2)) {
        TSRecord record = new TSRecord(i, deviceId);
        record.addTuple(
            DataPoint.getDataPoint(
                measurementSchemas[1].getType(),
                measurementSchemas[1].getMeasurementId(),
                String.valueOf(i)));
        fileWriter.write(record);
        unseq2.updateStartTime(deviceId, i);
        unseq2.updateEndTime(deviceId, i);
      }
      if ((i + 1) % flushInterval == 0) {
        fileWriter.flushAllChunkGroups();
      }
    }
    for (long i = 50; i < 151; i++) {
      for (String deviceId : Arrays.copyOfRange(deviceIds, 1, 2)) {
        TSRecord record = new TSRecord(i, deviceId);
        record.addTuple(
            DataPoint.getDataPoint(
                measurementSchemas[2].getType(),
                measurementSchemas[2].getMeasurementId(),
                String.valueOf(i)));
        fileWriter.write(record);
        unseq2.updateStartTime(deviceId, i);
        unseq2.updateEndTime(deviceId, i);
      }
      if ((i + 1) % flushInterval == 0) {
        fileWriter.flushAllChunkGroups();
      }
    }
    fileWriter.close();

    MergeTask mergeTask =
        new MergeTask(
            new MergeResource(seqTsFileResources, unseqTsFileResources),
            TestConstant.OUTPUT_DATA_DIR,
            (k, v, l) -> {},
            "test",
            true,
            1,
            MERGE_TEST_SG);
    mergeTask.call();

    Assert.assertEquals(0, seqTsFileResources.get(0).getStartTime(deviceIds[0]));
    Assert.assertEquals(99, seqTsFileResources.get(0).getEndTime(deviceIds[0]));
    Assert.assertEquals(0, seqTsFileResources.get(0).getStartTime(deviceIds[1]));
    Assert.assertEquals(99, seqTsFileResources.get(0).getEndTime(deviceIds[1]));
    Assert.assertEquals(100, seqTsFileResources.get(1).getStartTime(deviceIds[0]));
    Assert.assertEquals(199, seqTsFileResources.get(1).getEndTime(deviceIds[0]));
    Assert.assertEquals(100, seqTsFileResources.get(1).getStartTime(deviceIds[1]));
    Assert.assertEquals(199, seqTsFileResources.get(1).getEndTime(deviceIds[1]));

    // query root.MergeTest.d0.s0 from target1
    List<TsFileResource> resources = Collections.singletonList(seqTsFileResources.get(0));
    queryAndCheck(
        deviceIds[0],
        measurementSchemas[0],
        resources,
        checkResultFunc(0),
        checkResultCntFunc(100));

    // query root.MergeTest.d0.s1 from target1
    queryAndCheck(
        deviceIds[0],
        measurementSchemas[1],
        resources,
        checkResultFunc(0),
        checkResultCntFunc(100));

    // query root.MergeTest.d1.s0 from target1
    queryAndCheck(
        deviceIds[1],
        measurementSchemas[0],
        resources,
        checkResultFunc(0),
        checkResultCntFunc(100));

    // query root.MergeTest.d1.s2 from target1
    queryAndCheck(
        deviceIds[1], measurementSchemas[2], resources, checkResultFunc(0), checkResultCntFunc(50));

    // query root.MergeTest.d0.s0 from target2
    resources = Collections.singletonList(seqTsFileResources.get(1));
    queryAndCheck(
        deviceIds[0],
        measurementSchemas[0],
        resources,
        checkResultFunc(0),
        checkResultCntFunc(100));

    // query root.MergeTest.d1.s0 from target2
    queryAndCheck(
        deviceIds[1],
        measurementSchemas[0],
        resources,
        checkResultFunc(0),
        checkResultCntFunc(100));

    // query root.MergeTest.d1.s1 from target2
    queryAndCheck(
        deviceIds[1],
        measurementSchemas[1],
        resources,
        checkResultFunc(0),
        checkResultCntFunc(100));

    // query root.MergeTest.d1.s2 from target2
    queryAndCheck(
        deviceIds[1],
        measurementSchemas[2],
        resources,
        checkResultFunc(0),
        checkResultCntFunc(100));
  }
}
