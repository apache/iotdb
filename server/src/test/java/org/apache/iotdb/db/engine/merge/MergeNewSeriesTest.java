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
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.Test;

import java.io.IOException;
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
    measurementNum = 1;
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
}
