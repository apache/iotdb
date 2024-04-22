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

package org.apache.iotdb.db.storageengine.dataregion.compaction.cross;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.buffer.ChunkCache;
import org.apache.iotdb.db.storageengine.buffer.TimeSeriesMetadataCache;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionConfigRestorer;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.DataPoint;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_SEPARATOR;

abstract class MergeTest {

  static final String MERGE_TEST_SG = "root.mergeTest";

  int seqFileNum = 5;
  int unseqFileNum = 5;
  int measurementNum = 10;
  int deviceNum = 10;
  long ptNum = 100;
  long flushInterval = 20;
  TSEncoding encoding = TSEncoding.PLAIN;

  IDeviceID[] deviceIds;
  MeasurementSchema[] measurementSchemas;

  List<TsFileResource> seqResources = new ArrayList<>();
  List<TsFileResource> unseqResources = new ArrayList<>();

  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException {
    EnvironmentUtils.envSetUp();
    prepareSeries();
    prepareFiles(seqFileNum, unseqFileNum);
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    new CompactionConfigRestorer().restoreCompactionConfig();
    removeFiles(seqResources, unseqResources);
    seqResources.clear();
    unseqResources.clear();
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    EnvironmentUtils.cleanEnv();
  }

  private void prepareSeries() throws MetadataException {
    measurementSchemas = new MeasurementSchema[measurementNum];
    for (int i = 0; i < measurementNum; i++) {
      measurementSchemas[i] =
          new MeasurementSchema(
              "sensor" + i, TSDataType.DOUBLE, encoding, CompressionType.UNCOMPRESSED);
    }
    deviceIds = new IDeviceID[deviceNum];
    for (int i = 0; i < deviceNum; i++) {
      deviceIds[i] =
          IDeviceID.Factory.DEFAULT_FACTORY.create(MERGE_TEST_SG + PATH_SEPARATOR + "device" + i);
    }
  }

  void prepareFiles(int seqFileNum, int unseqFileNum) throws IOException, WriteProcessException {
    for (int i = 0; i < seqFileNum; i++) {
      File file = new File(TestConstant.getTestTsFilePath("root.sg1", 0, 0, i));
      mkdirs(file);
      TsFileResource tsFileResource = new TsFileResource(file);
      tsFileResource.setStatusForTest(TsFileResourceStatus.NORMAL);
      tsFileResource.setMinPlanIndex(i);
      tsFileResource.setMaxPlanIndex(i);
      tsFileResource.setVersion(i);
      seqResources.add(tsFileResource);
      prepareFile(tsFileResource, i * ptNum, ptNum, 0);
    }
    for (int i = 0; i < unseqFileNum; i++) {
      File file = new File(TestConstant.getTestTsFilePath("root.sg1", 0, 0, i + seqFileNum));
      mkdirs(file);
      TsFileResource tsFileResource = new TsFileResource(file);
      tsFileResource.setStatusForTest(TsFileResourceStatus.NORMAL);
      tsFileResource.setMinPlanIndex(i + seqFileNum);
      tsFileResource.setMaxPlanIndex(i + seqFileNum);
      tsFileResource.setVersion(i + seqFileNum);
      unseqResources.add(tsFileResource);
      prepareFile(tsFileResource, i * ptNum, ptNum * (i + 1) / unseqFileNum, 10000);
    }

    File file =
        new File(TestConstant.getTestTsFilePath("root.sg1", 0, 0, seqFileNum + unseqFileNum));
    mkdirs(file);
    TsFileResource tsFileResource = new TsFileResource(file);
    tsFileResource.setStatusForTest(TsFileResourceStatus.NORMAL);
    tsFileResource.setMinPlanIndex(seqFileNum + unseqFileNum);
    tsFileResource.setMaxPlanIndex(seqFileNum + unseqFileNum);
    tsFileResource.setVersion(seqFileNum + unseqFileNum);
    unseqResources.add(tsFileResource);
    prepareFile(tsFileResource, 0, ptNum * unseqFileNum, 20000);
  }

  void removeFiles(List<TsFileResource> seqResList, List<TsFileResource> unseqResList)
      throws IOException {
    for (TsFileResource tsFileResource : seqResList) {
      tsFileResource.remove();
      tsFileResource.getModFile().remove();
    }
    for (TsFileResource tsFileResource : unseqResList) {
      tsFileResource.remove();
      tsFileResource.getModFile().remove();
    }

    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
  }

  void prepareFile(TsFileResource tsFileResource, long timeOffset, long ptNum, long valueOffset)
      throws IOException, WriteProcessException {
    TsFileWriter fileWriter = new TsFileWriter(tsFileResource.getTsFile());
    for (IDeviceID deviceId : deviceIds) {
      for (IMeasurementSchema measurementSchema : measurementSchemas) {
        fileWriter.registerTimeseries(new Path(deviceId), measurementSchema);
      }
    }
    for (long i = timeOffset; i < timeOffset + ptNum; i++) {
      for (int j = 0; j < deviceNum; j++) {
        TSRecord record = new TSRecord(i, deviceIds[j].toString());
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
      if ((i + 1) % flushInterval == 0) {
        fileWriter.flushAllChunkGroups();
      }
    }
    fileWriter.close();
    tsFileResource.serialize();
  }

  void mkdirs(File file) {
    if (!file.getParentFile().exists()) {
      Assert.assertTrue(file.getParentFile().mkdirs());
    }
  }
}
