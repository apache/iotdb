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
package org.apache.iotdb.db.storageengine.rescon.memory;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.buffer.BloomFilterCache;
import org.apache.iotdb.db.storageengine.buffer.ChunkCache;
import org.apache.iotdb.db.storageengine.buffer.TimeSeriesMetadataCache;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.TimeIndexLevel;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.DataPoint;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_SEPARATOR;
import static org.junit.Assert.assertEquals;

public class ResourceManagerTest {

  static final String RESOURCE_MANAGER_TEST_SG = "root.resourceManagerTest";
  private int seqFileNum = 10;
  private int measurementNum = 10;
  int deviceNum = 10;
  long ptNum = 100;
  long flushInterval = 20;
  TSEncoding encoding = TSEncoding.PLAIN;

  String[] deviceIds;
  MeasurementSchema[] measurementSchemas;

  List<TsFileResource> seqResources = new ArrayList<>();
  List<TsFileResource> unseqResources = new ArrayList<>();

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private final TsFileResourceManager tsFileResourceManager = TsFileResourceManager.getInstance();
  private long prevTimeIndexMemoryThreshold;
  private TimeIndexLevel timeIndexLevel;

  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException {
    prevTimeIndexMemoryThreshold = CONFIG.getAllocateMemoryForTimeIndex();
    timeIndexLevel = CONFIG.getTimeIndexLevel();
    prepareSeries();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    removeFiles();
    seqResources.clear();
    unseqResources.clear();
    CONFIG.setTimeIndexLevel(String.valueOf(timeIndexLevel));
    tsFileResourceManager.setTimeIndexMemoryThreshold(prevTimeIndexMemoryThreshold);
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    BloomFilterCache.getInstance().clear();
    TsFileResourceManager.getInstance().clear();
    EnvironmentUtils.cleanAllDir();
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
      deviceIds[i] = RESOURCE_MANAGER_TEST_SG + PATH_SEPARATOR + "device" + i;
    }
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
      for (IMeasurementSchema measurementSchema : measurementSchemas) {
        fileWriter.registerTimeseries(new Path(deviceId), measurementSchema);
      }
    }
    for (long i = timeOffset; i < timeOffset + ptNum; i++) {
      for (int j = 0; j < deviceNum; j++) {
        TSRecord record = new TSRecord(deviceIds[j], i);
        for (int k = 0; k < measurementNum; k++) {
          record.addTuple(
              DataPoint.getDataPoint(
                  measurementSchemas[k].getType(),
                  measurementSchemas[k].getMeasurementName(),
                  String.valueOf(i + valueOffset)));
        }
        fileWriter.writeRecord(record);
        tsFileResource.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create(deviceIds[j]), i);
        tsFileResource.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create(deviceIds[j]), i);
      }
      if ((i + 1) % flushInterval == 0) {
        fileWriter.flush();
      }
    }
    fileWriter.close();
  }

  @Test
  public void testDegradeMethod() throws IOException, WriteProcessException {
    File file =
        new File(
            TestConstant.BASE_OUTPUT_PATH.concat(
                0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile"));
    TsFileResource tsFileResource = new TsFileResource(file);
    tsFileResource.setStatusForTest(TsFileResourceStatus.NORMAL);
    tsFileResource.updatePlanIndexes((long) 0);
    prepareFile(tsFileResource, 0, ptNum, 0);
    long previousRamSize = tsFileResource.calculateRamSize();
    assertEquals(
        TimeIndexLevel.ARRAY_DEVICE_TIME_INDEX,
        TimeIndexLevel.valueOf(tsFileResource.getTimeIndexType()));
    long reducedMemory = tsFileResource.degradeTimeIndex();
    assertEquals(previousRamSize - tsFileResource.calculateRamSize(), reducedMemory);
    assertEquals(
        TimeIndexLevel.FILE_TIME_INDEX, TimeIndexLevel.valueOf(tsFileResource.getTimeIndexType()));
  }

  @Test
  public void testDegradeToFileTimeIndex() throws IOException, WriteProcessException {
    File file =
        new File(
            TestConstant.BASE_OUTPUT_PATH.concat(
                0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile"));
    TsFileResource tsFileResource = new TsFileResource(file);
    tsFileResource.setStatusForTest(TsFileResourceStatus.NORMAL);
    tsFileResource.updatePlanIndexes((long) 0);
    prepareFile(tsFileResource, 0, ptNum, 0);
    assertEquals(
        TimeIndexLevel.ARRAY_DEVICE_TIME_INDEX,
        TimeIndexLevel.valueOf(tsFileResource.getTimeIndexType()));
    long curTimeIndexMemoryThreshold = 322;
    tsFileResourceManager.setTimeIndexMemoryThreshold(curTimeIndexMemoryThreshold);
    tsFileResourceManager.registerSealedTsFileResource(tsFileResource);
    assertEquals(
        TimeIndexLevel.FILE_TIME_INDEX, TimeIndexLevel.valueOf(tsFileResource.getTimeIndexType()));
  }

  @Test
  public void testNotDegradeToFileTimeIndex() throws IOException, WriteProcessException {
    File file =
        new File(
            TestConstant.BASE_OUTPUT_PATH.concat(
                0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile"));
    TsFileResource tsFileResource = new TsFileResource(file);
    tsFileResource.setStatusForTest(TsFileResourceStatus.NORMAL);
    tsFileResource.updatePlanIndexes((long) 0);
    prepareFile(tsFileResource, 0, ptNum, 0);
    assertEquals(
        TimeIndexLevel.ARRAY_DEVICE_TIME_INDEX,
        TimeIndexLevel.valueOf(tsFileResource.getTimeIndexType()));
    long previousRamSize = tsFileResource.calculateRamSize();
    long curTimeIndexMemoryThreshold = 4291;
    tsFileResourceManager.setTimeIndexMemoryThreshold(curTimeIndexMemoryThreshold);
    tsFileResourceManager.registerSealedTsFileResource(tsFileResource);
    assertEquals(0, previousRamSize - tsFileResource.calculateRamSize());
    assertEquals(
        TimeIndexLevel.ARRAY_DEVICE_TIME_INDEX,
        TimeIndexLevel.valueOf(tsFileResource.getTimeIndexType()));
  }

  @Test
  public void testTwoResourceToDegrade() throws IOException, WriteProcessException {
    File file1 =
        new File(
            TestConstant.BASE_OUTPUT_PATH.concat(
                0
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
    prepareFile(tsFileResource1, 0, ptNum, 0);
    assertEquals(
        TimeIndexLevel.ARRAY_DEVICE_TIME_INDEX,
        TimeIndexLevel.valueOf(tsFileResource1.getTimeIndexType()));
    long curTimeIndexMemoryThreshold = 4291;
    tsFileResourceManager.setTimeIndexMemoryThreshold(curTimeIndexMemoryThreshold);
    tsFileResourceManager.registerSealedTsFileResource(tsFileResource1);
    assertEquals(
        TimeIndexLevel.ARRAY_DEVICE_TIME_INDEX,
        TimeIndexLevel.valueOf(tsFileResource1.getTimeIndexType()));
    File file2 =
        new File(
            TestConstant.BASE_OUTPUT_PATH.concat(
                1
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile"));
    TsFileResource tsFileResource2 = new TsFileResource(file2);
    tsFileResource2.setStatusForTest(TsFileResourceStatus.NORMAL);
    tsFileResource2.updatePlanIndexes((long) 1);
    prepareFile(tsFileResource2, ptNum, ptNum, 0);
    assertEquals(
        TimeIndexLevel.ARRAY_DEVICE_TIME_INDEX,
        TimeIndexLevel.valueOf(tsFileResource2.getTimeIndexType()));
    tsFileResourceManager.registerSealedTsFileResource(tsFileResource2);
    assertEquals(
        TimeIndexLevel.FILE_TIME_INDEX, TimeIndexLevel.valueOf(tsFileResource1.getTimeIndexType()));
    assertEquals(
        TimeIndexLevel.ARRAY_DEVICE_TIME_INDEX,
        TimeIndexLevel.valueOf(tsFileResource2.getTimeIndexType()));
  }

  @Test
  public void testMultiDeviceTimeIndexDegrade() throws IOException, WriteProcessException {
    long curTimeIndexMemoryThreshold = 9663;
    tsFileResourceManager.setTimeIndexMemoryThreshold(curTimeIndexMemoryThreshold);
    for (int i = 0; i < seqFileNum; i++) {
      File file =
          new File(
              TestConstant.BASE_OUTPUT_PATH.concat(
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
      assertEquals(
          TimeIndexLevel.ARRAY_DEVICE_TIME_INDEX,
          TimeIndexLevel.valueOf(tsFileResource.getTimeIndexType()));
      seqResources.add(tsFileResource);
      prepareFile(tsFileResource, i * ptNum, ptNum, 0);
      tsFileResourceManager.registerSealedTsFileResource(tsFileResource);
    }
    assertEquals(10, tsFileResourceManager.getPriorityQueueSize());
    for (int i = 0; i < seqFileNum; i++) {
      // TODO: size of DeviceID may different with string device
      if (i < 8) {
        assertEquals(
            TimeIndexLevel.FILE_TIME_INDEX,
            TimeIndexLevel.valueOf(seqResources.get(i).getTimeIndexType()));
      } else {
        assertEquals(
            TimeIndexLevel.ARRAY_DEVICE_TIME_INDEX,
            TimeIndexLevel.valueOf(seqResources.get(i).getTimeIndexType()));
      }
    }
  }

  @Test
  public void testAllFileTimeIndexDegrade() throws IOException, WriteProcessException {
    long curTimeIndexMemoryThreshold = 322;
    tsFileResourceManager.setTimeIndexMemoryThreshold(curTimeIndexMemoryThreshold);
    for (int i = 0; i < seqFileNum; i++) {
      File file =
          new File(
              TestConstant.BASE_OUTPUT_PATH.concat(
                  i
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + i
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 0
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 0
                      + ".tsfile"));
      TsFileResource tsFileResource = new TsFileResource(file);
      assertEquals(
          TimeIndexLevel.ARRAY_DEVICE_TIME_INDEX,
          TimeIndexLevel.valueOf(tsFileResource.getTimeIndexType()));
      seqResources.add(tsFileResource);
      prepareFile(tsFileResource, i * ptNum, ptNum, 0);
      tsFileResourceManager.registerSealedTsFileResource(tsFileResource);
      assertEquals(
          TimeIndexLevel.FILE_TIME_INDEX,
          TimeIndexLevel.valueOf(tsFileResource.getTimeIndexType()));
    }
  }
}
