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
package org.apache.iotdb.db.queryengine.execution.operator;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.storageengine.buffer.BloomFilterCache;
import org.apache.iotdb.db.storageengine.buffer.ChunkCache;
import org.apache.iotdb.db.storageengine.buffer.TimeSeriesMetadataCache;
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
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_SEPARATOR;

/**
 * This util contains 5 seqFiles and 5 unseqFiles in default.
 *
 * <p>Sequence time range of data: [0, 99], [100, 199], [200, 299], [300, 399], [400, 499]
 *
 * <p>UnSequence time range of data: [0, 19], [100, 139], [200, 259], [300, 379], [400, 499], [0,
 * 199]
 *
 * <p>d0 and d1 are aligned, d2 is nonAligned
 */
public class AlignedSeriesTestUtil {

  public static void setUp(
      List<IMeasurementSchema> measurementSchemas,
      List<TsFileResource> seqResources,
      List<TsFileResource> unseqResources,
      String sgName)
      throws MetadataException, IOException, WriteProcessException {
    prepareSeries(measurementSchemas, sgName);
    prepareFiles(seqResources, unseqResources, measurementSchemas, sgName);
  }

  public static void tearDown(
      List<TsFileResource> seqResources, List<TsFileResource> unseqResources) throws IOException {
    removeFiles(seqResources, unseqResources);
    seqResources.clear();
    unseqResources.clear();
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    BloomFilterCache.getInstance().clear();
    EnvironmentUtils.cleanAllDir();
  }

  private static void prepareFiles(
      List<TsFileResource> seqResources,
      List<TsFileResource> unseqResources,
      List<IMeasurementSchema> measurementSchemas,
      String sgName)
      throws IOException, WriteProcessException {
    int seqFileNum = 5;
    long ptNum = 100;
    for (int i = 0; i < seqFileNum; i++) {
      File file = new File(TestConstant.getTestTsFilePath(sgName, 0, 0, i));
      TsFileResource tsFileResource = new TsFileResource(file);
      tsFileResource.setStatusForTest(TsFileResourceStatus.NORMAL);
      tsFileResource.setMinPlanIndex(i);
      tsFileResource.setMaxPlanIndex(i);
      seqResources.add(tsFileResource);
      prepareFile(sgName, tsFileResource, i * ptNum, ptNum, 0, measurementSchemas);
    }
    int unseqFileNum = 5;
    for (int i = 0; i < unseqFileNum; i++) {
      File file = new File(TestConstant.getTestTsFilePath(sgName, 0, 0, i + seqFileNum));
      TsFileResource tsFileResource = new TsFileResource(file);
      tsFileResource.setStatusForTest(TsFileResourceStatus.NORMAL);
      tsFileResource.setMinPlanIndex(i + seqFileNum);
      tsFileResource.setMaxPlanIndex(i + seqFileNum);
      unseqResources.add(tsFileResource);
      prepareFile(
          sgName,
          tsFileResource,
          i * ptNum,
          ptNum * (i + 1) / unseqFileNum,
          10000,
          measurementSchemas);
    }

    File file = new File(TestConstant.getTestTsFilePath(sgName, 0, 0, seqFileNum + unseqFileNum));
    TsFileResource tsFileResource = new TsFileResource(file);
    tsFileResource.setStatusForTest(TsFileResourceStatus.NORMAL);
    tsFileResource.setMinPlanIndex(seqFileNum + unseqFileNum);
    tsFileResource.setMaxPlanIndex(seqFileNum + unseqFileNum);
    unseqResources.add(tsFileResource);
    prepareFile(sgName, tsFileResource, 0, ptNum * 2, 20000, measurementSchemas);
  }

  private static void prepareFile(
      String sgName,
      TsFileResource tsFileResource,
      long timeOffset,
      long ptNum,
      long valueOffset,
      List<IMeasurementSchema> measurementSchemas)
      throws IOException, WriteProcessException {
    File file = tsFileResource.getTsFile();
    if (!file.getParentFile().exists()) {
      Assert.assertTrue(file.getParentFile().mkdirs());
    }
    TsFileWriter fileWriter = new TsFileWriter(file);

    String device0 = sgName + PATH_SEPARATOR + "device0";
    String device1 = sgName + PATH_SEPARATOR + "device1";
    String device2 = sgName + PATH_SEPARATOR + "device2";

    fileWriter.registerAlignedTimeseries(new Path(device0), measurementSchemas);
    fileWriter.registerAlignedTimeseries(new Path(device1), measurementSchemas);
    fileWriter.registerTimeseries(new Path(device2), measurementSchemas);
    for (long i = timeOffset; i < timeOffset + ptNum; i++) {

      TSRecord record = new TSRecord(device0, i);
      int index = 0;
      for (IMeasurementSchema measurementSchema : measurementSchemas) {
        record.addTuple(
            DataPoint.getDataPoint(
                measurementSchema.getType(),
                measurementSchema.getMeasurementName(),
                index == 0
                    ? String.valueOf((i + valueOffset) % 2 == 0)
                    : String.valueOf((i + valueOffset))));
        index++;
      }
      fileWriter.writeRecord(record);
      tsFileResource.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device0), i);
      tsFileResource.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device0), i);

      record.deviceId = IDeviceID.Factory.DEFAULT_FACTORY.create(device1);
      fileWriter.writeRecord(record);
      tsFileResource.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device1), i);
      tsFileResource.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device1), i);

      record.deviceId = IDeviceID.Factory.DEFAULT_FACTORY.create(device2);
      fileWriter.writeRecord(record);
      tsFileResource.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device2), i);
      tsFileResource.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device2), i);

      long flushInterval = 20;
      if ((i + 1) % flushInterval == 0) {
        fileWriter.flush();
      }
    }
    fileWriter.close();
  }

  private static void prepareSeries(List<IMeasurementSchema> measurementSchemas, String sgName)
      throws MetadataException {

    measurementSchemas.add(
        new MeasurementSchema(
            "sensor0", TSDataType.BOOLEAN, TSEncoding.PLAIN, CompressionType.SNAPPY));
    measurementSchemas.add(
        new MeasurementSchema("sensor1", TSDataType.INT32, TSEncoding.RLE, CompressionType.SNAPPY));
    measurementSchemas.add(
        new MeasurementSchema(
            "sensor2", TSDataType.INT64, TSEncoding.TS_2DIFF, CompressionType.SNAPPY));
    measurementSchemas.add(
        new MeasurementSchema(
            "sensor3", TSDataType.FLOAT, TSEncoding.GORILLA, CompressionType.SNAPPY));
    measurementSchemas.add(
        new MeasurementSchema(
            "sensor4", TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.SNAPPY));
    measurementSchemas.add(
        new MeasurementSchema(
            "sensor5", TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.SNAPPY));
  }

  private static void removeFiles(
      List<TsFileResource> seqResources, List<TsFileResource> unseqResources) throws IOException {
    for (TsFileResource tsFileResource : seqResources) {
      tsFileResource.remove();
    }
    for (TsFileResource tsFileResource : unseqResources) {
      tsFileResource.remove();
    }

    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
  }
}
