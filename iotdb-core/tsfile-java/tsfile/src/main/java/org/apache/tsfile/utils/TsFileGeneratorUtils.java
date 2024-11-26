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
package org.apache.tsfile.utils;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.record.datapoint.BooleanDataPoint;
import org.apache.tsfile.write.record.datapoint.DataPoint;
import org.apache.tsfile.write.record.datapoint.DoubleDataPoint;
import org.apache.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.tsfile.write.record.datapoint.StringDataPoint;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;

/** This class is only used for generating aligned or nonAligned tsfiles in test. */
public class TsFileGeneratorUtils {
  private static final FSFactory fsFactory = FSFactoryProducer.getFSFactory();
  public static final String testStorageGroup = "root.testsg";
  public static int alignDeviceOffset = 10000;

  public static boolean useMultiType = false;

  public static void writeWithTsRecord(
      TsFileWriter tsFileWriter,
      String deviceId,
      List<IMeasurementSchema> schemas,
      long rowSize,
      long startTime,
      long startValue,
      boolean isAligned)
      throws IOException, WriteProcessException {
    for (long time = startTime; time < rowSize + startTime; time++, startValue++) {
      // construct TsRecord
      TSRecord tsRecord = new TSRecord(deviceId, time);
      for (IMeasurementSchema schema : schemas) {
        DataPoint dPoint;
        switch (schema.getType()) {
          case INT64:
          case TIMESTAMP:
            dPoint = new LongDataPoint(schema.getMeasurementName(), startValue);
            break;
          case INT32:
          case DATE:
            dPoint = new IntDataPoint(schema.getMeasurementName(), (int) startValue);
            break;
          case DOUBLE:
            dPoint = new DoubleDataPoint(schema.getMeasurementName(), (double) startValue);
            break;
          case FLOAT:
            dPoint = new FloatDataPoint(schema.getMeasurementName(), (float) startValue);
            break;
          case BOOLEAN:
            dPoint = new BooleanDataPoint(schema.getMeasurementName(), true);
            break;
          case TEXT:
          case BLOB:
          case STRING:
          default:
            dPoint =
                new StringDataPoint(
                    schema.getMeasurementName(),
                    new Binary(String.valueOf(startValue), TSFileConfig.STRING_CHARSET));
            break;
        }
        tsRecord.addTuple(dPoint);
      }
      // write
      if (isAligned) {
        tsFileWriter.writeRecord(tsRecord);
      } else {
        tsFileWriter.writeRecord(tsRecord);
      }
    }
  }

  public static void writeWithTablet(
      TsFileWriter tsFileWriter,
      String deviceId,
      List<IMeasurementSchema> schemas,
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
      int row = tablet.getRowSize();
      tablet.addTimestamp(row, startTime++);
      for (int i = 0; i < sensorNum; i++) {
        long[] sensor = (long[]) values[i];
        sensor[row] = startValue;
      }
      // write
      if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
        if (isAligned) {
          tsFileWriter.writeAligned(tablet);
        } else {
          tsFileWriter.writeTree(tablet);
        }
        tablet.reset();
      }
    }
    // write
    if (tablet.getRowSize() != 0) {
      if (isAligned) {
        tsFileWriter.writeAligned(tablet);
      } else {
        tsFileWriter.writeTree(tablet);
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
        List<IMeasurementSchema> alignedMeasurementSchemas = new ArrayList<>();
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
        List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
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
      long startTime,
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
    if (chunkGroupSize > 0) {
      TSFileDescriptor.getInstance().getConfig().setGroupSizeInByte(chunkGroupSize);
    }
    if (pageSize > 0) {
      TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(pageSize);
    }

    try (TsFileWriter tsFileWriter = new TsFileWriter(file)) {
      // register align timeseries
      List<IMeasurementSchema> alignedMeasurementSchemas = new ArrayList<>();
      for (int i = 0; i < measurementNum; i++) {
        alignedMeasurementSchemas.add(
            new MeasurementSchema("s" + i, getDataType(i), TSEncoding.PLAIN));
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
    } finally {
      TSFileDescriptor.getInstance().getConfig().setGroupSizeInByte(originGroupSize);
      TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(originPageSize);
    }
    return file;
  }

  public static File generateNonAlignedTsFile(
      String filePath,
      int deviceNum,
      int measurementNum,
      int pointNum,
      long startTime,
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
    if (chunkGroupSize > 0)
      TSFileDescriptor.getInstance().getConfig().setGroupSizeInByte(chunkGroupSize);
    if (pageSize > 0)
      TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(pageSize);
    try (TsFileWriter tsFileWriter = new TsFileWriter(file)) {
      // register nonAlign timeseries
      List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
      for (int i = 0; i < measurementNum; i++) {
        measurementSchemas.add(new MeasurementSchema("s" + i, getDataType(i), TSEncoding.PLAIN));
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
    } finally {
      TSFileDescriptor.getInstance().getConfig().setGroupSizeInByte(originGroupSize);
      TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(originPageSize);
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

    int originGroupSize = TSFileDescriptor.getInstance().getConfig().getGroupSizeInByte();
    int originPageSize = TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();
    if (chunkGroupSize > 0)
      TSFileDescriptor.getInstance().getConfig().setGroupSizeInByte(chunkGroupSize);
    if (pageSize > 0)
      TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(pageSize);

    try (TsFileWriter tsFileWriter = new TsFileWriter(file)) {
      // register align timeseries
      List<IMeasurementSchema> alignedMeasurementSchemas = new ArrayList<>();
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
                  testStorageGroup
                      + PATH_SEPARATOR
                      + "d"
                      + (deviceIndex.get(i) + alignDeviceOffset),
                  time);
          for (IMeasurementSchema schema : alignedMeasurementSchemas) {
            DataPoint dPoint =
                new StringDataPoint(
                    schema.getMeasurementName(), new Binary(value, TSFileConfig.STRING_CHARSET));
            tsRecord.addTuple(dPoint);
          }
          // write
          tsFileWriter.writeRecord(tsRecord);
        }
      }
    } finally {
      TSFileDescriptor.getInstance().getConfig().setGroupSizeInByte(originGroupSize);
      TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(originPageSize);
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

    int originGroupSize = TSFileDescriptor.getInstance().getConfig().getGroupSizeInByte();
    int originPageSize = TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();
    if (chunkGroupSize > 0)
      TSFileDescriptor.getInstance().getConfig().setGroupSizeInByte(chunkGroupSize);
    if (pageSize > 0)
      TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(pageSize);
    try (TsFileWriter tsFileWriter = new TsFileWriter(file)) {
      // register nonAlign timeseries
      List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
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
              new TSRecord(testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex.get(i), time);
          for (IMeasurementSchema schema : measurementSchemas) {
            DataPoint dPoint =
                new StringDataPoint(
                    schema.getMeasurementName(), new Binary(value, TSFileConfig.STRING_CHARSET));
            tsRecord.addTuple(dPoint);
          }
          // write
          tsFileWriter.writeRecord(tsRecord);
        }
      }
      return file;
    } finally {
      TSFileDescriptor.getInstance().getConfig().setGroupSizeInByte(originGroupSize);
      TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(originPageSize);
    }
  }

  public static TSDataType getDataType(int num) {
    if (!useMultiType) {
      return TSDataType.INT64;
    }
    switch (num % 6) {
      case 0:
        return TSDataType.BOOLEAN;
      case 1:
        return TSDataType.INT32;
      case 2:
        return TSDataType.INT64;
      case 3:
        return TSDataType.FLOAT;
      case 4:
        return TSDataType.DOUBLE;
      case 5:
        return TSDataType.TEXT;
      default:
        throw new IllegalArgumentException("Invalid input: " + num % 6);
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
