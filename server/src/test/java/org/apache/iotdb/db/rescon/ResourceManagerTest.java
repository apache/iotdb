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
package org.apache.iotdb.db.rescon;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.timeindex.TimeIndexLevel;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_SEPARATOR;
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
  UnaryMeasurementSchema[] measurementSchemas;

  List<TsFileResource> seqResources = new ArrayList<>();
  List<TsFileResource> unseqResources = new ArrayList<>();

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private TsFileResourceManager tsFileResourceManager = TsFileResourceManager.getInstance();;
  private double prevTimeIndexMemoryProportion;
  private double prevTimeIndexMemoryThreshold;
  private TimeIndexLevel timeIndexLevel;

  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException {
    IoTDB.metaManager.init();
    prevTimeIndexMemoryProportion = CONFIG.getTimeIndexMemoryProportion();
    timeIndexLevel = CONFIG.getTimeIndexLevel();
    prepareSeries();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    removeFiles();
    seqResources.clear();
    unseqResources.clear();
    CONFIG.setTimeIndexMemoryProportion(prevTimeIndexMemoryProportion);
    CONFIG.setTimeIndexLevel(String.valueOf(timeIndexLevel));
    prevTimeIndexMemoryThreshold =
        prevTimeIndexMemoryProportion * CONFIG.getAllocateMemoryForRead();
    tsFileResourceManager.setTimeIndexMemoryThreshold(prevTimeIndexMemoryThreshold);
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    IoTDB.metaManager.clear();
    TsFileResourceManager.getInstance().clear();
    EnvironmentUtils.cleanAllDir();
  }

  void prepareSeries() throws MetadataException {
    measurementSchemas = new UnaryMeasurementSchema[measurementNum];
    for (int i = 0; i < measurementNum; i++) {
      measurementSchemas[i] =
          new UnaryMeasurementSchema(
              "sensor" + i, TSDataType.DOUBLE, encoding, CompressionType.UNCOMPRESSED);
    }
    deviceIds = new String[deviceNum];
    for (int i = 0; i < deviceNum; i++) {
      deviceIds[i] = RESOURCE_MANAGER_TEST_SG + PATH_SEPARATOR + "device" + i;
    }
    IoTDB.metaManager.setStorageGroup(new PartialPath(RESOURCE_MANAGER_TEST_SG));
    for (String device : deviceIds) {
      for (UnaryMeasurementSchema measurementSchema : measurementSchemas) {
        PartialPath devicePath = new PartialPath(device);
        IoTDB.metaManager.createTimeseries(
            devicePath.concatNode(measurementSchema.getMeasurementId()),
            measurementSchema.getType(),
            measurementSchema.getEncodingType(),
            measurementSchema.getCompressor(),
            Collections.emptyMap());
      }
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
    FileReaderManager.getInstance().stop();
  }

  void prepareFile(TsFileResource tsFileResource, long timeOffset, long ptNum, long valueOffset)
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
      if ((i + 1) % flushInterval == 0) {
        fileWriter.flushAllChunkGroups();
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
    tsFileResource.setClosed(true);
    tsFileResource.updatePlanIndexes((long) 0);
    prepareFile(tsFileResource, 0, ptNum, 0);
    long previousRamSize = tsFileResource.calculateRamSize();
    assertEquals(
        TimeIndexLevel.DEVICE_TIME_INDEX,
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
    tsFileResource.setClosed(true);
    tsFileResource.updatePlanIndexes((long) 0);
    prepareFile(tsFileResource, 0, ptNum, 0);
    assertEquals(
        TimeIndexLevel.DEVICE_TIME_INDEX,
        TimeIndexLevel.valueOf(tsFileResource.getTimeIndexType()));
    double curTimeIndexMemoryThreshold = 322;
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
    tsFileResource.setClosed(true);
    tsFileResource.updatePlanIndexes((long) 0);
    prepareFile(tsFileResource, 0, ptNum, 0);
    assertEquals(
        TimeIndexLevel.DEVICE_TIME_INDEX,
        TimeIndexLevel.valueOf(tsFileResource.getTimeIndexType()));
    long previousRamSize = tsFileResource.calculateRamSize();
    double curTimeIndexMemoryThreshold = 3221;
    tsFileResourceManager.setTimeIndexMemoryThreshold(curTimeIndexMemoryThreshold);
    tsFileResourceManager.registerSealedTsFileResource(tsFileResource);
    assertEquals(0, previousRamSize - tsFileResource.calculateRamSize());
    assertEquals(
        TimeIndexLevel.DEVICE_TIME_INDEX,
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
    tsFileResource1.setClosed(true);
    tsFileResource1.updatePlanIndexes((long) 0);
    prepareFile(tsFileResource1, 0, ptNum, 0);
    assertEquals(
        TimeIndexLevel.DEVICE_TIME_INDEX,
        TimeIndexLevel.valueOf(tsFileResource1.getTimeIndexType()));
    double curTimeIndexMemoryThreshold = 3221;
    tsFileResourceManager.setTimeIndexMemoryThreshold(curTimeIndexMemoryThreshold);
    tsFileResourceManager.registerSealedTsFileResource(tsFileResource1);
    assertEquals(
        TimeIndexLevel.DEVICE_TIME_INDEX,
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
    tsFileResource2.setClosed(true);
    tsFileResource2.updatePlanIndexes((long) 1);
    prepareFile(tsFileResource2, ptNum, ptNum, 0);
    assertEquals(
        TimeIndexLevel.DEVICE_TIME_INDEX,
        TimeIndexLevel.valueOf(tsFileResource2.getTimeIndexType()));
    tsFileResourceManager.registerSealedTsFileResource(tsFileResource2);
    assertEquals(
        TimeIndexLevel.FILE_TIME_INDEX, TimeIndexLevel.valueOf(tsFileResource1.getTimeIndexType()));
    assertEquals(
        TimeIndexLevel.DEVICE_TIME_INDEX,
        TimeIndexLevel.valueOf(tsFileResource2.getTimeIndexType()));
  }

  @Test
  public void testMultiDeviceTimeIndexDegrade() throws IOException, WriteProcessException {
    double curTimeIndexMemoryThreshold = 9663.7;
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
      tsFileResource.setClosed(true);
      tsFileResource.updatePlanIndexes((long) i);
      assertEquals(
          TimeIndexLevel.DEVICE_TIME_INDEX,
          TimeIndexLevel.valueOf(tsFileResource.getTimeIndexType()));
      seqResources.add(tsFileResource);
      prepareFile(tsFileResource, i * ptNum, ptNum, 0);
      tsFileResourceManager.registerSealedTsFileResource(tsFileResource);
    }
    assertEquals(10, tsFileResourceManager.getPriorityQueueSize());
    for (int i = 0; i < seqFileNum; i++) {
      if (i < 7) {
        assertEquals(
            TimeIndexLevel.FILE_TIME_INDEX,
            TimeIndexLevel.valueOf(seqResources.get(i).getTimeIndexType()));
      } else {
        assertEquals(
            TimeIndexLevel.DEVICE_TIME_INDEX,
            TimeIndexLevel.valueOf(seqResources.get(i).getTimeIndexType()));
      }
    }
  }

  @Test(expected = RuntimeException.class)
  public void testAllFileTimeIndexDegrade() throws IOException, WriteProcessException {
    long reducedMemory = 0;
    CONFIG.setTimeIndexLevel(String.valueOf(TimeIndexLevel.FILE_TIME_INDEX));
    double curTimeIndexMemoryThreshold = 322;
    tsFileResourceManager.setTimeIndexMemoryThreshold(curTimeIndexMemoryThreshold);
    try {
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
        tsFileResource.setClosed(true);
        tsFileResource.updatePlanIndexes((long) i);
        seqResources.add(tsFileResource);
        assertEquals(
            TimeIndexLevel.FILE_TIME_INDEX,
            TimeIndexLevel.valueOf(tsFileResource.getTimeIndexType()));
        long previousRamSize = tsFileResource.calculateRamSize();
        prepareFile(tsFileResource, i * ptNum, ptNum, 0);
        tsFileResourceManager.registerSealedTsFileResource(tsFileResource);
        assertEquals(
            TimeIndexLevel.FILE_TIME_INDEX,
            TimeIndexLevel.valueOf(tsFileResource.getTimeIndexType()));
        reducedMemory = previousRamSize - tsFileResource.calculateRamSize();
      }
    } catch (RuntimeException e) {
      assertEquals(0, reducedMemory);
      assertEquals(7, seqResources.size());
      throw e;
    }
  }
}
