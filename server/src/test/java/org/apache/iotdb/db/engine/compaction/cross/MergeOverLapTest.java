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
 *
 */

package org.apache.iotdb.db.engine.compaction.cross;

import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.compaction.cross.inplace.manage.CrossSpaceMergeResource;
import org.apache.iotdb.db.engine.compaction.cross.inplace.task.CrossSpaceMergeTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class MergeOverLapTest extends MergeTest {

  private File tempSGDir;

  @Override
  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException {
    ptNum = 1000;
    tempSGDir = new File(TestConstant.getTestTsFileDir("root.sg1", 0, 0));
    if (!tempSGDir.exists()) {
      Assert.assertTrue(tempSGDir.mkdirs());
    }
    super.setUp();
  }

  @Override
  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    if (tempSGDir == null) {
      tempSGDir = new File(TestConstant.getTestTsFileDir("root.sg1", 0, 0));
      if (tempSGDir.exists()) {
        FileUtils.deleteDirectory(tempSGDir);
      }
    }
  }

  @Override
  void prepareFiles(int seqFileNum, int unseqFileNum) throws IOException, WriteProcessException {
    for (int i = 0; i < seqFileNum; i++) {
      File file = new File(TestConstant.getTestTsFilePath("root.sg1", 0, 0, i));
      TsFileResource tsFileResource = new TsFileResource(file);
      tsFileResource.setClosed(true);
      tsFileResource.setMinPlanIndex(i);
      tsFileResource.setMaxPlanIndex(i);
      seqResources.add(tsFileResource);
      prepareFile(tsFileResource, i * ptNum, ptNum, 0);
    }
    for (int i = 0; i < unseqFileNum; i++) {
      File file = new File(TestConstant.getTestTsFilePath("root.sg1", 0, 0, i + seqFileNum));
      TsFileResource tsFileResource = new TsFileResource(file);
      tsFileResource.setClosed(true);
      tsFileResource.setMaxPlanIndex(i + seqFileNum);
      tsFileResource.setMinPlanIndex(i + seqFileNum);
      unseqResources.add(tsFileResource);
      prepareUnseqFile(tsFileResource, i * ptNum, ptNum * (i + 1) / unseqFileNum, 10000);
    }
    File file =
        new File(TestConstant.getTestTsFilePath("root.sg1", 0, 0, seqFileNum + unseqFileNum));
    TsFileResource tsFileResource = new TsFileResource(file);
    tsFileResource.setClosed(true);
    tsFileResource.setMinPlanIndex(seqFileNum + unseqFileNum);
    tsFileResource.setMaxPlanIndex(seqFileNum + unseqFileNum);
    unseqResources.add(tsFileResource);
    prepareUnseqFile(tsFileResource, 0, ptNum * unseqFileNum, 20000);
  }

  private void prepareUnseqFile(
      TsFileResource tsFileResource, long timeOffset, long ptNum, long valueOffset)
      throws IOException, WriteProcessException {
    TsFileWriter fileWriter = new TsFileWriter(tsFileResource.getTsFile());
    for (String deviceId : deviceIds) {
      for (UnaryMeasurementSchema measurementSchema : measurementSchemas) {
        fileWriter.registerTimeseries(
            new Path(deviceId, measurementSchema.getMeasurementId()), measurementSchema);
      }
    }
    for (long i = timeOffset; i < timeOffset + ptNum; i++) {
      for (int j = 0; j < deviceNum; j++) {
        TSRecord record = new TSRecord(i, deviceIds[j]);
        for (int k = 0; k < measurementNum; k++) {
          record.addTuple(
              DataPoint.getDataPoint(
                  measurementSchemas[k].getType(),
                  measurementSchemas[k].getMeasurementId(),
                  String.valueOf(i + valueOffset)));
        }
        fileWriter.write(record);
        tsFileResource.updateStartTime(deviceIds[j], i);
        tsFileResource.updateEndTime(deviceIds[j], i);
      }
      // insert overlapping tuples
      if ((i + 1) % 100 == 0) {
        for (int j = 0; j < deviceNum; j++) {
          TSRecord record = new TSRecord(i, deviceIds[j]);
          for (int k = 0; k < measurementNum; k++) {
            record.addTuple(
                DataPoint.getDataPoint(
                    measurementSchemas[k].getType(),
                    measurementSchemas[k].getMeasurementId(),
                    String.valueOf(i + valueOffset)));
          }
          fileWriter.write(record);
          tsFileResource.updateStartTime(deviceIds[j], i);
          tsFileResource.updateEndTime(deviceIds[j], i);
        }
      }
      if ((i + 1) % flushInterval == 0) {
        fileWriter.flushAllChunkGroups();
      }
    }
    fileWriter.close();
  }

  @Test
  public void testFullMerge() throws Exception {
    CrossSpaceMergeTask mergeTask =
        new CrossSpaceMergeTask(
            new CrossSpaceMergeResource(seqResources, unseqResources),
            tempSGDir.getPath(),
            (k, v, l) -> {},
            "test",
            true,
            1,
            MERGE_TEST_SG);
    mergeTask.call();

    PartialPath path =
        new PartialPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    List<TsFileResource> resources = new ArrayList<>();
    resources.add(seqResources.get(0));
    IBatchReader tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[0].getType(),
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            resources,
            new ArrayList<>(),
            null,
            null,
            true);
    int cnt = 0;
    try {
      while (tsFilesReader.hasNextBatch()) {
        BatchData batchData = tsFilesReader.nextBatch();
        for (int i = 0; i < batchData.length(); i++) {
          cnt++;
          assertEquals(batchData.getTimeByIndex(i) + 20000.0, batchData.getDoubleByIndex(i), 0.001);
        }
      }
      assertEquals(1000, cnt);
    } finally {
      tsFilesReader.close();
    }
  }
}
