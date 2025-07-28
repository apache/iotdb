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

package org.apache.iotdb.db.storageengine.dataregion.compaction.inner;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.IFullPath;
import org.apache.iotdb.commons.path.NonAlignedFullPath;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.reader.IDataBlockReader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.reader.SeriesDataBlockReader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduler;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.commons.io.FileUtils;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.DataPoint;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_SEPARATOR;
import static org.apache.iotdb.db.utils.EnvironmentUtils.TEST_QUERY_FI_CONTEXT;
import static org.junit.Assert.assertEquals;

public class InnerCompactionMoreDataTest extends InnerCompactionTest {

  protected int measurementNum = 3000;
  protected int seqFileNum = 2;

  File tempSGDir;

  @Override
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

  @Override
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
  }

  @Override
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
                  String.valueOf(i + valueOffset + k)));
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

  @Before
  public void setUp() throws Exception {
    tempSGDir = new File(TestConstant.getTestTsFileDir("root.compactionTest", 0, 0));
    if (!tempSGDir.exists()) {
      Assert.assertTrue(tempSGDir.mkdirs());
    }
    super.setUp();
    tempSGDir = new File(TestConstant.BASE_OUTPUT_PATH.concat("tempSG"));
    tempSGDir.mkdirs();
    tsFileManager = new TsFileManager(COMPACTION_TEST_SG, "0", tempSGDir.getAbsolutePath());
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    FileUtils.deleteDirectory(tempSGDir);
  }

  // test file compaction larger than 1024 sensor
  @Test
  public void testSensorWithTwoOrThreeNode()
      throws MetadataException, IOException, InterruptedException {
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);
    CompactionScheduler.scheduleCompaction(tsFileManager, 0);
    try {
      Thread.sleep(500);
    } catch (Exception ignored) {

    }
    CompactionTaskManager.getInstance().waitAllCompactionFinish();

    IFullPath path =
        new NonAlignedFullPath(
            IDeviceID.Factory.DEFAULT_FACTORY.create(deviceIds[0]), measurementSchemas[2688]);
    IDataBlockReader tsFilesReader =
        new SeriesDataBlockReader(
            path,
            TEST_QUERY_FI_CONTEXT,
            tsFileManager.getTsFileList(true),
            new ArrayList<>(),
            true);

    while (tsFilesReader.hasNextBatch()) {
      TsBlock batchData = tsFilesReader.nextBatch();
      for (int i = 0, size = batchData.getPositionCount(); i < size; i++) {
        assertEquals(
            batchData.getTimeByIndex(i) + 2688, batchData.getColumn(0).getDouble(i), 0.001);
      }
    }
  }
}
