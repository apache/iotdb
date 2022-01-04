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
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;

/** This class is only used for generating aligned or nonAligned tsfiles in test. */
public class TsFileGeneratorUtils {
  private static final FSFactory fsFactory = FSFactoryProducer.getFSFactory();
  public static final String testStorageGroup = "root.testsg";
  private static int alignDeviceOffset = 10000;

  public static void writeWithTsRecord(
      TsFileWriter tsFileWriter,
      String deviceId,
      List<UnaryMeasurementSchema> schemas,
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
      List<UnaryMeasurementSchema> schemas,
      long rowNum,
      long startTime,
      long startValue,
      boolean isAligned)
      throws IOException, WriteProcessException {
    List<IMeasurementSchema> measurementSchemas =
        schemas.stream().map(schema -> (IMeasurementSchema) schema).collect(Collectors.toList());
    Tablet tablet = new Tablet(deviceId, measurementSchemas);
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

  // generate aligned timeseries "d1.s1","d1.s2","d1.s3","d1.s4" and nonAligned timeseries
  // "d2.s1","d2.s2","d2.s3"
  public static File generateMixTsFile(
      String filePath,
      int alignDeviceNum,
      int NonAlignDeviceNum,
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
      List<UnaryMeasurementSchema> alignedMeasurementSchemas = new ArrayList<>();
      alignedMeasurementSchemas.add(
          new UnaryMeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN));
      alignedMeasurementSchemas.add(
          new UnaryMeasurementSchema("s2", TSDataType.INT64, TSEncoding.PLAIN));
      alignedMeasurementSchemas.add(
          new UnaryMeasurementSchema("s3", TSDataType.INT64, TSEncoding.PLAIN));
      alignedMeasurementSchemas.add(
          new UnaryMeasurementSchema("s4", TSDataType.INT64, TSEncoding.RLE));
      tsFileWriter.registerAlignedTimeseries(new Path("d1"), alignedMeasurementSchemas);

      // register nonAlign timeseries
      List<UnaryMeasurementSchema> measurementSchemas = new ArrayList<>();
      measurementSchemas.add(new UnaryMeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN));
      measurementSchemas.add(new UnaryMeasurementSchema("s2", TSDataType.INT64, TSEncoding.PLAIN));
      measurementSchemas.add(new UnaryMeasurementSchema("s3", TSDataType.INT64, TSEncoding.PLAIN));
      tsFileWriter.registerTimeseries(new Path("d2"), measurementSchemas);

      TsFileGeneratorUtils.writeWithTsRecord(
          tsFileWriter, "d1", alignedMeasurementSchemas, pointNum, 0, 0, true);
      TsFileGeneratorUtils.writeWithTsRecord(
          tsFileWriter, "d2", measurementSchemas, pointNum, 0, 0, false);
      return file;
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
      List<UnaryMeasurementSchema> alignedMeasurementSchemas = new ArrayList<>();
      for (int i = 0; i < measurementNum; i++) {
        alignedMeasurementSchemas.add(
            new UnaryMeasurementSchema("s" + i, TSDataType.INT64, TSEncoding.PLAIN));
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
      List<UnaryMeasurementSchema> measurementSchemas = new ArrayList<>();
      for (int i = 0; i < measurementNum; i++) {
        measurementSchemas.add(
            new UnaryMeasurementSchema("s" + i, TSDataType.INT64, TSEncoding.PLAIN));
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

  public static String getTsFilePath(
      String systemDir,
      String logicalStorageGroupName,
      long VirtualStorageGroupId,
      long TimePartitionId,
      long tsFileVersion) {
    String filePath =
        String.format(systemDir, logicalStorageGroupName, VirtualStorageGroupId, TimePartitionId);
    String fileName =
        System.currentTimeMillis()
            + FilePathUtils.FILE_NAME_SEPARATOR
            + tsFileVersion
            + "-0-0.tsfile";
    return (filePath + File.separator).concat(fileName);
  }

  public static String getTsFilePath(String fileParentPath, long tsFileVersion) {
    String fileName =
        System.currentTimeMillis()
            + FilePathUtils.FILE_NAME_SEPARATOR
            + tsFileVersion
            + "-0-0.tsfile";
    return (fileParentPath + File.separator).concat(fileName);
  }

  public static int getAlignDeviceOffset() {
    return alignDeviceOffset;
  }
}
