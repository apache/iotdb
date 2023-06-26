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

package org.apache.iotdb.db.storageengine.dataregion.compaction.inner.sizetiered;

import org.apache.iotdb.commons.conf.IoTDBConstant;
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
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_SEPARATOR;

public class SizeTieredCompactionTest {
  static final String COMPACTION_TEST_SG = "root.compactionTest";

  protected int seqFileNum = 6;
  int unseqFileNum = 0;
  protected int measurementNum = 10;
  int deviceNum = 10;
  long ptNum = 100;
  long flushInterval = 20;
  TSEncoding encoding = TSEncoding.PLAIN;

  String[] deviceIds;
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
    removeFiles();
    seqResources.clear();
    unseqResources.clear();
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    EnvironmentUtils.cleanEnv();
  }

  void prepareSeries() throws MetadataException {
    measurementSchemas = new MeasurementSchema[measurementNum];
    for (int i = 0; i < measurementNum; i++) {
      measurementSchemas[i] =
          new MeasurementSchema(
              "sensor" + i, TSDataType.DOUBLE, encoding, CompressionType.UNCOMPRESSED);
    }
    deviceIds = new String[deviceNum];
    for (int i = 0; i < deviceNum; i++) {
      deviceIds[i] = COMPACTION_TEST_SG + PATH_SEPARATOR + "device" + i;
    }
  }

  void prepareFiles(int seqFileNum, int unseqFileNum) throws IOException, WriteProcessException {
    for (int i = 0; i < seqFileNum; i++) {
      File file =
          new File(
              TestConstant.getTestTsFileDir("root.compactionTest", 0, 0)
                  .concat(
                      i
                          + IoTDBConstant.FILE_NAME_SEPARATOR
                          + i
                          + IoTDBConstant.FILE_NAME_SEPARATOR
                          + 0
                          + IoTDBConstant.FILE_NAME_SEPARATOR
                          + 0
                          + ".tsfile"));
      TsFileResource tsFileResource = new TsFileResource(file);
      tsFileResource.setStatusForTest(TsFileResourceStatus.NORMAL);
      tsFileResource.updatePlanIndexes((long) i);
      seqResources.add(tsFileResource);
      prepareFile(tsFileResource, i * ptNum, ptNum, 0);
    }
    for (int i = 0; i < unseqFileNum; i++) {
      File file =
          new File(
              TestConstant.getTestTsFileDir("root.compactionTest", 0, 0)
                  .concat(
                      (10000 + i)
                          + IoTDBConstant.FILE_NAME_SEPARATOR
                          + (10000 + i)
                          + IoTDBConstant.FILE_NAME_SEPARATOR
                          + 0
                          + IoTDBConstant.FILE_NAME_SEPARATOR
                          + 0
                          + ".tsfile"));
      TsFileResource tsFileResource = new TsFileResource(file);
      tsFileResource.setStatusForTest(TsFileResourceStatus.NORMAL);
      tsFileResource.updatePlanIndexes(i + seqFileNum);
      unseqResources.add(tsFileResource);
      prepareFile(tsFileResource, i * ptNum, ptNum * (i + 1) / unseqFileNum, 10000);
    }

    File file =
        new File(
            TestConstant.getTestTsFileDir("root.compactionTest", 0, 0)
                .concat(
                    unseqFileNum
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + unseqFileNum
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 0
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 0
                        + ".tsfile"));
    TsFileResource tsFileResource = new TsFileResource(file);
    tsFileResource.setStatusForTest(TsFileResourceStatus.NORMAL);
    tsFileResource.updatePlanIndexes(seqFileNum + unseqFileNum);
    unseqResources.add(tsFileResource);
    prepareFile(tsFileResource, 0, ptNum * unseqFileNum, 20000);
  }

  private void removeFiles() throws IOException {
    for (TsFileResource tsFileResource : seqResources) {
      if (tsFileResource.getTsFile().exists()) {
        tsFileResource.remove();
      }
    }
    for (TsFileResource tsFileResource : unseqResources) {
      if (tsFileResource.getTsFile().exists()) {
        tsFileResource.remove();
      }
    }
    File[] files = FSFactoryProducer.getFSFactory().listFilesBySuffix("target", ".tsfile");
    for (File file : files) {
      file.delete();
    }
    File[] resourceFiles =
        FSFactoryProducer.getFSFactory().listFilesBySuffix("target", ".resource");
    for (File resourceFile : resourceFiles) {
      resourceFile.delete();
    }
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
  }

  void prepareFile(TsFileResource tsFileResource, long timeOffset, long ptNum, long valueOffset)
      throws IOException, WriteProcessException {
    TsFileWriter fileWriter = new TsFileWriter(tsFileResource.getTsFile());
    for (String deviceId : deviceIds) {
      for (MeasurementSchema measurementSchema : measurementSchemas) {
        fileWriter.registerTimeseries(
            new Path(deviceId, measurementSchema.getMeasurementId(), true), measurementSchema);
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
      if ((i + 1) % flushInterval == 0) {
        fileWriter.flushAllChunkGroups();
      }
    }
    fileWriter.close();
  }

  List<TsFileResource> prepareTsFileResources() throws IOException, WriteProcessException {
    List<TsFileResource> ret = new ArrayList<>();
    // prepare file 1
    File file1 =
        new File(
            TestConstant.getTestTsFileDir("root.compactionTest", 0, 0)
                .concat(
                    System.nanoTime()
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 0
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 0
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 0
                        + ".tsfile"));
    TsFileResource tsFileResource1 = new TsFileResource(file1);
    tsFileResource1.setStatusForTest(TsFileResourceStatus.NORMAL);
    tsFileResource1.updatePlanIndexes((long) 0);
    TsFileWriter fileWriter1 = new TsFileWriter(tsFileResource1.getTsFile());
    fileWriter1.registerTimeseries(
        new Path(deviceIds[0], measurementSchemas[0].getMeasurementId(), true),
        measurementSchemas[0]);
    TSRecord record1 = new TSRecord(0, deviceIds[0]);
    record1.addTuple(
        DataPoint.getDataPoint(
            measurementSchemas[0].getType(),
            measurementSchemas[0].getMeasurementId(),
            String.valueOf(0)));
    fileWriter1.write(record1);
    fileWriter1.close();
    // prepare file 2
    File file2 =
        new File(
            TestConstant.getTestTsFileDir("root.compactionTest", 0, 0)
                .concat(
                    System.nanoTime()
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 1
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 0
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 0
                        + ".tsfile"));
    TsFileResource tsFileResource2 = new TsFileResource(file2);
    tsFileResource2.setStatusForTest(TsFileResourceStatus.NORMAL);
    tsFileResource2.updatePlanIndexes((long) 1);
    TsFileWriter fileWriter2 = new TsFileWriter(tsFileResource2.getTsFile());
    fileWriter2.registerTimeseries(
        new Path(deviceIds[0], measurementSchemas[1].getMeasurementId(), true),
        measurementSchemas[1]);
    TSRecord record2 = new TSRecord(0, deviceIds[0]);
    record2.addTuple(
        DataPoint.getDataPoint(
            measurementSchemas[1].getType(),
            measurementSchemas[1].getMeasurementId(),
            String.valueOf(0)));
    fileWriter2.write(record2);
    fileWriter2.close();
    ret.add(tsFileResource1);
    ret.add(tsFileResource2);
    return ret;
  }
}
