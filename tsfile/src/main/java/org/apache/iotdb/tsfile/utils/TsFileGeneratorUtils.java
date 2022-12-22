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
package org.apache.iotdb.tsfile.utils;

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.StringDataPoint;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;

/** This class is only used for generating aligned or nonAligned tsfiles in test. */
public class TsFileGeneratorUtils {
  private static final FSFactory fsFactory = FSFactoryProducer.getFSFactory();
  public static final String testStorageGroup = "root.testsg";
  public static int alignDeviceOffset = 10000;

  public static void writeWithTsRecord(
      TsFileWriter tsFileWriter,
      String deviceId,
      List<MeasurementSchema> schemas,
      long rowSize,
      long startTime,
      long startValue,
      boolean isAligned)
      throws IOException, WriteProcessException {
    for (long time = startTime; time < rowSize + startTime; time++, startValue++) {
      // construct TsRecord
      TSRecord tsRecord = new TSRecord(time, deviceId);
      for (IMeasurementSchema schema : schemas) {
        DataPoint dPoint = new LongDataPoint(schema.getMeasurementId(), startValue);
        tsRecord.addTuple(dPoint);
      }
      // write
      if (isAligned) {
        tsFileWriter.writeAligned(tsRecord);
      } else {
        tsFileWriter.write(tsRecord);
      }
    }
  }

  public static void writeWithTablet(
      TsFileWriter tsFileWriter,
      String deviceId,
      List<MeasurementSchema> schemas,
      long rowNum,
      long startTime,
      long startValue,
      boolean isAligned)
      throws IOException, WriteProcessException {
    Tablet tablet = new Tablet(deviceId, schemas);
    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;
    long sensorNum = schemas.size();

    for (long r = 0; r < rowNum; r++, startValue++) {
      int row = tablet.rowSize++;
      timestamps[row] = startTime++;
      for (int i = 0; i < sensorNum; i++) {
        long[] sensor = (long[]) values[i];
        sensor[row] = startValue;
      }
      // write
      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        if (isAligned) {
          tsFileWriter.writeAligned(tablet);
        } else {
          tsFileWriter.write(tablet);
        }
        tablet.reset();
      }
    }
    // write
    if (tablet.rowSize != 0) {
      if (isAligned) {
        tsFileWriter.writeAligned(tablet);
      } else {
        tsFileWriter.write(tablet);
      }
      tablet.reset();
    }
  }

  // including aligned and nonAligned timeseries
  public static File generateMixTsFile(
      String filePath,
      int deviceNum,
      int measurementNum,
      int pointNum,
      int startTime,
      int startValue,
      int chunkGroupSize,
      int pageSize)
      throws IOException, WriteProcessException {
    File file = fsFactory.getFile(filePath);
    if (file.exists()) {
      file.delete();
    }
    int originGroupSize = TSFileDescriptor.getInstance().getConfig().getGroupSizeInByte();
    int originPageSize = TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();
    try {
      if (chunkGroupSize > 0)
        TSFileDescriptor.getInstance().getConfig().setGroupSizeInByte(chunkGroupSize);
      if (pageSize > 0)
        TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(pageSize);
      try (TsFileWriter tsFileWriter = new TsFileWriter(file)) {
        // register align timeseries
        List<MeasurementSchema> alignedMeasurementSchemas = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          alignedMeasurementSchemas.add(
              new MeasurementSchema("s" + i, TSDataType.INT64, TSEncoding.PLAIN));
        }
        for (int i = alignDeviceOffset; i < alignDeviceOffset + deviceNum; i++) {
          tsFileWriter.registerAlignedTimeseries(
              new Path(testStorageGroup + PATH_SEPARATOR + "d" + i), alignedMeasurementSchemas);
        }

        // write with record
        for (int i = alignDeviceOffset; i < alignDeviceOffset + deviceNum; i++) {
          writeWithTsRecord(
              tsFileWriter,
              testStorageGroup + PATH_SEPARATOR + "d" + i,
              alignedMeasurementSchemas,
              pointNum,
              startTime,
              startValue,
              true);
        }

        // register nonAlign timeseries
        List<MeasurementSchema> measurementSchemas = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          measurementSchemas.add(
              new MeasurementSchema("s" + i, TSDataType.INT64, TSEncoding.PLAIN));
        }
        for (int i = 0; i < deviceNum; i++) {
          tsFileWriter.registerTimeseries(
              new Path(testStorageGroup + PATH_SEPARATOR + "d" + i), measurementSchemas);
        }

        // write with record
        for (int i = 0; i < deviceNum; i++) {
          writeWithTsRecord(
              tsFileWriter,
              testStorageGroup + PATH_SEPARATOR + "d" + i,
              measurementSchemas,
              pointNum,
              startTime,
              startValue,
              false);
        }
      }
      return file;
    } finally {
      TSFileDescriptor.getInstance().getConfig().setGroupSizeInByte(originGroupSize);
      TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(originPageSize);
    }
  }

  public static File generateAlignedTsFile(
      String filePath,
      int deviceNum,
      int measurementNum,
      int pointNum,
      int startTime,
      int startValue,
      int chunkGroupSize,
      int pageSize)
      throws IOException, WriteProcessException {
    File file = fsFactory.getFile(filePath);
    if (file.exists()) {
      file.delete();
    }
    if (chunkGroupSize > 0)
      TSFileDescriptor.getInstance().getConfig().setGroupSizeInByte(chunkGroupSize);
    if (pageSize > 0)
      TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(pageSize);
    try (TsFileWriter tsFileWriter = new TsFileWriter(file)) {
      // register align timeseries
      List<MeasurementSchema> alignedMeasurementSchemas = new ArrayList<>();
      for (int i = 0; i < measurementNum; i++) {
        alignedMeasurementSchemas.add(
            new MeasurementSchema("s" + i, TSDataType.INT64, TSEncoding.PLAIN));
      }
      for (int i = alignDeviceOffset; i < alignDeviceOffset + deviceNum; i++) {
        tsFileWriter.registerAlignedTimeseries(
            new Path(testStorageGroup + PATH_SEPARATOR + "d" + i), alignedMeasurementSchemas);
      }

      // write with record
      for (int i = alignDeviceOffset; i < alignDeviceOffset + deviceNum; i++) {
        writeWithTsRecord(
            tsFileWriter,
            testStorageGroup + PATH_SEPARATOR + "d" + i,
            alignedMeasurementSchemas,
            pointNum,
            startTime,
            startValue,
            true);
      }
    }
    return file;
  }

  public static File generateNonAlignedTsFile(
      String filePath,
      int deviceNum,
      int measurementNum,
      int pointNum,
      int startTime,
      int startValue,
      int chunkGroupSize,
      int pageSize)
      throws IOException, WriteProcessException {
    File file = fsFactory.getFile(filePath);
    if (file.exists()) {
      file.delete();
    }
    if (chunkGroupSize > 0)
      TSFileDescriptor.getInstance().getConfig().setGroupSizeInByte(chunkGroupSize);
    if (pageSize > 0)
      TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(pageSize);
    try (TsFileWriter tsFileWriter = new TsFileWriter(file)) {
      // register nonAlign timeseries
      List<MeasurementSchema> measurementSchemas = new ArrayList<>();
      for (int i = 0; i < measurementNum; i++) {
        measurementSchemas.add(new MeasurementSchema("s" + i, TSDataType.INT64, TSEncoding.PLAIN));
      }
      for (int i = 0; i < deviceNum; i++) {
        tsFileWriter.registerTimeseries(
            new Path(testStorageGroup + PATH_SEPARATOR + "d" + i), measurementSchemas);
      }

      // write with record
      for (int i = 0; i < deviceNum; i++) {
        writeWithTsRecord(
            tsFileWriter,
            testStorageGroup + PATH_SEPARATOR + "d" + i,
            measurementSchemas,
            pointNum,
            startTime,
            startValue,
            false);
      }
      return file;
    }
  }

  public static File generateAlignedTsFileWithTextValues(
      String filePath,
      List<Integer> deviceIndex,
      List<Integer> measurementIndex,
      int pointNum,
      int startTime,
      String value,
      int chunkGroupSize,
      int pageSize)
      throws IOException, WriteProcessException {
    File file = fsFactory.getFile(filePath);
    if (file.exists()) {
      file.delete();
    }
    if (chunkGroupSize > 0)
      TSFileDescriptor.getInstance().getConfig().setGroupSizeInByte(chunkGroupSize);
    if (pageSize > 0)
      TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(pageSize);
    try (TsFileWriter tsFileWriter = new TsFileWriter(file)) {
      // register align timeseries
      List<MeasurementSchema> alignedMeasurementSchemas = new ArrayList<>();
      for (int i = 0; i < measurementIndex.size(); i++) {
        alignedMeasurementSchemas.add(
            new MeasurementSchema(
                "s" + measurementIndex.get(i), TSDataType.TEXT, TSEncoding.PLAIN));
      }
      for (int i = 0; i < deviceIndex.size(); i++) {
        tsFileWriter.registerAlignedTimeseries(
            new Path(
                testStorageGroup + PATH_SEPARATOR + "d" + (deviceIndex.get(i) + alignDeviceOffset)),
            alignedMeasurementSchemas);
      }

      // write with record
      for (int i = 0; i < deviceIndex.size(); i++) {
        for (long time = startTime; time < pointNum + startTime; time++) {
          // construct TsRecord
          TSRecord tsRecord =
              new TSRecord(
                  time,
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + (deviceIndex.get(i) + alignDeviceOffset));
          for (IMeasurementSchema schema : alignedMeasurementSchemas) {
            DataPoint dPoint = new StringDataPoint(schema.getMeasurementId(), new Binary(value));
            tsRecord.addTuple(dPoint);
          }
          // write
          tsFileWriter.writeAligned(tsRecord);
        }
      }
    }
    return file;
  }

  public static File generateNonAlignedTsFileWithTextValues(
      String filePath,
      List<Integer> deviceIndex,
      List<Integer> measurementIndex,
      int pointNum,
      int startTime,
      String value,
      int chunkGroupSize,
      int pageSize)
      throws IOException, WriteProcessException {
    File file = fsFactory.getFile(filePath);
    if (file.exists()) {
      file.delete();
    }
    if (chunkGroupSize > 0)
      TSFileDescriptor.getInstance().getConfig().setGroupSizeInByte(chunkGroupSize);
    if (pageSize > 0)
      TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(pageSize);
    try (TsFileWriter tsFileWriter = new TsFileWriter(file)) {
      // register nonAlign timeseries
      List<MeasurementSchema> measurementSchemas = new ArrayList<>();
      for (int i = 0; i < measurementIndex.size(); i++) {
        measurementSchemas.add(
            new MeasurementSchema(
                "s" + measurementIndex.get(i), TSDataType.TEXT, TSEncoding.PLAIN));
      }
      for (int i = 0; i < deviceIndex.size(); i++) {
        tsFileWriter.registerTimeseries(
            new Path(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex.get(i)),
            measurementSchemas);
      }

      // write with record
      for (int i = 0; i < deviceIndex.size(); i++) {
        for (long time = startTime; time < pointNum + startTime; time++) {
          // construct TsRecord
          TSRecord tsRecord =
              new TSRecord(time, testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex.get(i));
          for (IMeasurementSchema schema : measurementSchemas) {
            DataPoint dPoint = new StringDataPoint(schema.getMeasurementId(), new Binary(value));
            tsRecord.addTuple(dPoint);
          }
          // write
          tsFileWriter.write(tsRecord);
        }
      }
      return file;
    }
  }

  public static String getTsFilePath(String fileParentPath, long tsFileVersion) {
    String fileName =
        System.currentTimeMillis()
            + FilePathUtils.FILE_NAME_SEPARATOR
            + tsFileVersion
            + "-0-0.tsfile";
    return fileParentPath.concat(fileName);
  }

  public static int getAlignDeviceOffset() {
    return alignDeviceOffset;
  }
}
