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

package org.apache.iotdb.db.engine.cache;

import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.DoubleStatistics;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_SEPARATOR;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ChunkCacheTest {
  File tempSGDir;
  static final String TEST_SG = "root.sg1";

  int seqFileNum = 2;
  int unseqFileNum = 2;
  int measurementNum = 2;
  int deviceNum = 2;
  long ptNum = 100;
  long flushInterval = 20;

  String[] deviceIds;
  UnaryMeasurementSchema[] measurementSchemas;

  List<TsFileResource> seqResources = new ArrayList<>();
  List<TsFileResource> unseqResources = new ArrayList<>();

  ChunkCache chunkCache = ChunkCache.getInstance();

  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException {
    tempSGDir = new File(TestConstant.OUTPUT_DATA_DIR);
    if (!tempSGDir.exists()) {
      Assert.assertTrue(tempSGDir.mkdirs());
    }
    IoTDB.metaManager.init();
    prepareSeries();
    prepareFiles(seqFileNum, unseqFileNum);
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    removeFiles();
    seqResources.clear();
    unseqResources.clear();
    chunkCache.clear();
    TimeSeriesMetadataCache.getInstance().clear();
    IoTDB.metaManager.clear();
    EnvironmentUtils.cleanAllDir();
  }

  @Test
  public void testChunkCache() throws IOException {
    TsFileResource tsFileResource = seqResources.get(0);
    TsFileSequenceReader reader = new TsFileSequenceReader(tsFileResource.getTsFilePath());
    List<Path> paths = reader.getAllPaths();

    ChunkMetadata firstChunkMetadata = reader.getChunkMetadataList(paths.get(0)).get(0);
    firstChunkMetadata.setFilePath(tsFileResource.getTsFilePath());

    // add cache
    chunkCache.getAverageSize();
    Chunk chunk1 = chunkCache.get(firstChunkMetadata);
    chunkCache.getAverageSize();

    ChunkMetadata chunkMetadataKey =
        new ChunkMetadata("sensor0", TSDataType.DOUBLE, 25, new DoubleStatistics());
    chunkMetadataKey.setVersion(0);
    chunkMetadataKey.setFilePath(tsFileResource.getTsFilePath());

    Assert.assertEquals(chunkMetadataKey, firstChunkMetadata);

    // get cache
    Chunk chunk2 = chunkCache.get(chunkMetadataKey);
    Assert.assertEquals(chunk1.getHeader(), chunk2.getHeader());
    Assert.assertEquals(chunk1.getData(), chunk2.getData());

    chunkMetadataKey.setFilePath(null);
    try {
      chunkCache.get(chunkMetadataKey);
      fail();
    } catch (NullPointerException e) {
      assertTrue(true);
    }
    reader.close();
  }

  void prepareSeries() throws MetadataException {
    measurementSchemas = new UnaryMeasurementSchema[measurementNum];
    for (int i = 0; i < measurementNum; i++) {
      measurementSchemas[i] =
          new UnaryMeasurementSchema(
              "sensor" + i, TSDataType.DOUBLE, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED);
    }
    deviceIds = new String[deviceNum];
    for (int i = 0; i < deviceNum; i++) {
      deviceIds[i] = TEST_SG + PATH_SEPARATOR + "device" + i;
    }
    IoTDB.metaManager.setStorageGroup(new PartialPath(TEST_SG));
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

  void prepareFiles(int seqFileNum, int unseqFileNum) throws IOException, WriteProcessException {
    for (int i = 0; i < seqFileNum; i++) {
      File file = new File(TestConstant.getTestTsFilePath("root.sg1", 0, 0, i));
      if (!file.getParentFile().exists()) {
        Assert.assertTrue(file.getParentFile().mkdirs());
      }
      TsFileResource tsFileResource = new TsFileResource(file);
      tsFileResource.setClosed(true);
      tsFileResource.updatePlanIndexes(i);
      seqResources.add(tsFileResource);
      prepareFile(tsFileResource, i * ptNum, ptNum, 0);
    }
    for (int i = 0; i < unseqFileNum; i++) {
      File file = new File(TestConstant.getTestTsFilePath("root.sg1", 0, 0, i + seqFileNum));
      if (!file.getParentFile().exists()) {
        Assert.assertTrue(file.getParentFile().mkdirs());
      }
      TsFileResource tsFileResource = new TsFileResource(file);
      tsFileResource.setClosed(true);
      tsFileResource.updatePlanIndexes(i + seqFileNum);
      unseqResources.add(tsFileResource);
      prepareFile(tsFileResource, i * ptNum, ptNum * (i + 1) / unseqFileNum, 10000);
    }
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
}
