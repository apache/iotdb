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

package org.apache.iotdb.db.engine.compaction.inner;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.compaction.inner.sizetiered.SizeTieredCompactionRecoverTest;
import org.apache.iotdb.db.engine.compaction.utils.CompactionConfigRestorer;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceStatus;
import org.apache.iotdb.db.exception.StorageEngineException;
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
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_SEPARATOR;

public abstract class AbstractInnerSpaceCompactionTest {
  protected static final Logger logger =
      LoggerFactory.getLogger(SizeTieredCompactionRecoverTest.class);

  protected File tempSGDir;
  protected static final String COMPACTION_TEST_SG = "root.compactionTest";
  protected TsFileManager tsFileManager;

  protected int seqFileNum = 5;
  protected int unseqFileNum = 0;
  protected int measurementNum = 10;
  protected int deviceNum = 10;
  protected long ptNum = 100;
  protected long flushInterval = 20;
  protected TSEncoding encoding = TSEncoding.PLAIN;

  protected static String SEQ_DIRS =
      TestConstant.BASE_OUTPUT_PATH
          + "data"
          + File.separator
          + "sequence"
          + File.separator
          + "root.compactionTest"
          + File.separator
          + "0"
          + File.separator
          + "0";
  protected static String UNSEQ_DIRS =
      TestConstant.BASE_OUTPUT_PATH
          + "data"
          + File.separator
          + "unsequence"
          + File.separator
          + "root.compactionTest"
          + File.separator
          + "0"
          + File.separator
          + "0";

  protected String[] deviceIds;
  protected MeasurementSchema[] measurementSchemas;

  protected List<TsFileResource> seqResources = new ArrayList<>();
  protected List<TsFileResource> unseqResources = new ArrayList<>();

  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException {
    tempSGDir =
        new File(
            TestConstant.BASE_OUTPUT_PATH
                + "data"
                + File.separator
                + "sequence"
                + File.separator
                + "root.compactionTest"
                + File.separator
                + "0"
                + File.separator
                + "0");
    if (!tempSGDir.exists()) {
      Assert.assertTrue(tempSGDir.mkdirs());
    }
    if (!new File(SEQ_DIRS).exists()) {
      Assert.assertTrue(new File(SEQ_DIRS).mkdirs());
    }
    if (!new File(UNSEQ_DIRS).exists()) {
      Assert.assertTrue(new File(UNSEQ_DIRS).mkdirs());
    }

    EnvironmentUtils.envSetUp();
    IoTDB.configManager.init();
    prepareSeries();
    prepareFiles(seqFileNum, unseqFileNum);
    tsFileManager = new TsFileManager(COMPACTION_TEST_SG, "0", tempSGDir.getAbsolutePath());
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
    IoTDB.schemaProcessor.setStorageGroup(new PartialPath(COMPACTION_TEST_SG));
    for (String device : deviceIds) {
      for (MeasurementSchema measurementSchema : measurementSchemas) {
        PartialPath devicePath = new PartialPath(device);
        IoTDB.schemaProcessor.createTimeseries(
            devicePath.concatNode(measurementSchema.getMeasurementId()),
            measurementSchema.getType(),
            measurementSchema.getEncodingType(),
            measurementSchema.getCompressor(),
            Collections.emptyMap());
      }
    }
  }

  void prepareFiles(int seqFileNum, int unseqFileNum) throws IOException, WriteProcessException {
    for (int i = 0; i < seqFileNum; i++) {
      File file =
          new File(
              SEQ_DIRS
                  + File.separator.concat(
                      i
                          + IoTDBConstant.FILE_NAME_SEPARATOR
                          + i
                          + IoTDBConstant.FILE_NAME_SEPARATOR
                          + 0
                          + IoTDBConstant.FILE_NAME_SEPARATOR
                          + 0
                          + ".tsfile"));
      TsFileResource tsFileResource = new TsFileResource(file);
      tsFileResource.setStatus(TsFileResourceStatus.CLOSED);
      tsFileResource.updatePlanIndexes((long) i);
      seqResources.add(tsFileResource);
      prepareFile(tsFileResource, i * ptNum, ptNum, 0);
    }
    for (int i = 0; i < unseqFileNum; i++) {
      File file =
          new File(
              UNSEQ_DIRS
                  + File.separator.concat(
                      (10000 + i)
                          + IoTDBConstant.FILE_NAME_SEPARATOR
                          + (10000 + i)
                          + IoTDBConstant.FILE_NAME_SEPARATOR
                          + 0
                          + IoTDBConstant.FILE_NAME_SEPARATOR
                          + 0
                          + ".tsfile"));
      TsFileResource tsFileResource = new TsFileResource(file);
      tsFileResource.setStatus(TsFileResourceStatus.CLOSED);
      tsFileResource.updatePlanIndexes(i + seqFileNum);
      unseqResources.add(tsFileResource);
      prepareFile(tsFileResource, i * 2 * ptNum, ptNum * (i + 1) / unseqFileNum, 10000);
    }
  }

  void prepareFile(TsFileResource tsFileResource, long timeOffset, long ptNum, long valueOffset)
      throws IOException, WriteProcessException {
    try (TsFileWriter fileWriter = new TsFileWriter(tsFileResource.getTsFile()); ) {
      for (String deviceId : deviceIds) {
        for (MeasurementSchema measurementSchema : measurementSchemas) {
          fileWriter.registerTimeseries(new Path(deviceId), measurementSchema);
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
    }
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    new CompactionConfigRestorer().restoreCompactionConfig();
    removeFiles();
    seqResources.clear();
    unseqResources.clear();
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    IoTDB.configManager.clear();
    EnvironmentUtils.cleanEnv();
    if (tempSGDir.exists()) {
      FileUtils.deleteDirectory(tempSGDir);
    }
  }

  private void removeFiles() throws IOException {
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
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
  }

  public void setUnseqFileNum(int num) {
    this.unseqFileNum = num;
  }
}
