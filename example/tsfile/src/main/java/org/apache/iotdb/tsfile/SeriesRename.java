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

package org.apache.iotdb.tsfile;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkGroupHeader;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.DoubleColumn;
import org.apache.iotdb.tsfile.read.common.block.column.FloatColumn;
import org.apache.iotdb.tsfile.read.common.block.column.IntColumn;
import org.apache.iotdb.tsfile.read.common.block.column.LongColumn;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

public class SeriesRename {

  public static void main(String[] args) {
    long startTime = System.nanoTime();
    File[] tsfileDirs = new File[args.length];
    for (int i = 0, n = args.length; i < n; i++) {
      tsfileDirs[i] = new File(args[i]);
    }
    List<String> tsfileList = new ArrayList<>();
    for (File tsfileDir : tsfileDirs) {
      if (tsfileDir.exists()) {
        if (tsfileDir.isDirectory()) {
          try (Stream<Path> paths = Files.walk(tsfileDir.toPath())) {
            paths
                .map(Path::toString)
                .filter(f -> f.endsWith(TSFILE_SUFFIX))
                .forEach(tsfileList::add);
          } catch (IOException e) {
            System.out.println("Collect tsfile failed!");
            e.printStackTrace();
          }
        } else if (tsfileDir.isFile() && tsfileDir.getName().endsWith(TSFILE_SUFFIX)) {
          tsfileList.add(tsfileDir.getPath());
        } else {
          System.out.println("skip invalid tsfileDir: " + tsfileDir);
        }
      }
    }

    System.out.println("Total tsfile number: " + tsfileList.size() + System.lineSeparator());

    List<String> failedTsFileList = new ArrayList<>();

    int completed = 0;
    int failed = 0;
    for (String tsfileName : tsfileList) {
      boolean succeed = false;
      try (TsFileSequenceReader reader = new TsFileSequenceReader(tsfileName);
          TsFileWriter tsFileWriter = new TsFileWriter(new File(tsfileName + ".rename"))) {
        // Sequential reading of one ChunkGroup now follows this order:
        // first the CHUNK_GROUP_HEADER, then SeriesChunks (headers and data) in one ChunkGroup
        // Because we do not know how many chunks a ChunkGroup may have, we should read one byte
        // (the
        // marker) ahead and judge accordingly.
        reader.position((long) TSFileConfig.MAGIC_STRING.getBytes().length + 1);
        String currentDevice = "";
        byte marker;

        Set<String> seenPath = new HashSet<>();
        while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
          switch (marker) {
            case MetaMarker.CHUNK_HEADER:
            case MetaMarker.TIME_CHUNK_HEADER:
            case MetaMarker.VALUE_CHUNK_HEADER:
            case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
            case MetaMarker.ONLY_ONE_PAGE_TIME_CHUNK_HEADER:
            case MetaMarker.ONLY_ONE_PAGE_VALUE_CHUNK_HEADER:
              ChunkHeader header = reader.readChunkHeader(marker);
              String measurementName = header.getMeasurementID();
              if (header.getDataSize() == 0) {
                // empty value chunk
                break;
              }
              Decoder defaultTimeDecoder =
                  Decoder.getDecoderByType(
                      TSEncoding.valueOf(
                          TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
                      TSDataType.INT64);
              Decoder valueDecoder =
                  Decoder.getDecoderByType(header.getEncodingType(), header.getDataType());
              int dataSize = header.getDataSize();
              if (header.getDataType() == TSDataType.VECTOR) {
                throw new IllegalStateException(
                    "there exist aligned timeseries: " + currentDevice + "." + measurementName);
              }
              Optional<Pair<String, MeasurementSchema>> pair =
                  rename(
                      currentDevice,
                      measurementName,
                      header.getDataType(),
                      header.getEncodingType(),
                      header.getCompressionType());
              if (!pair.isPresent()) {
                System.out.println(
                    "ignore invalid time series: "
                        + currentDevice
                        + "."
                        + measurementName
                        + ", datatype: "
                        + header.getDataType());
              } else {
                if (seenPath.add(pair.get().left + "." + pair.get().right.getMeasurementId())) {
                  tsFileWriter.registerTimeseries(
                      new org.apache.iotdb.tsfile.read.common.Path(pair.get().left),
                      pair.get().right);
                }
              }
              while (dataSize > 0) {
                valueDecoder.reset();
                PageHeader pageHeader =
                    reader.readPageHeader(
                        header.getDataType(),
                        (header.getChunkType() & 0x3F) == MetaMarker.CHUNK_HEADER);
                ByteBuffer pageData = reader.readPage(pageHeader, header.getCompressionType());
                PageReader pageReader =
                    new PageReader(
                        pageHeader,
                        pageData,
                        header.getDataType(),
                        valueDecoder,
                        defaultTimeDecoder,
                        null);
                TsBlock tsBlock = pageReader.getAllSatisfiedData();
                dataSize -= pageHeader.getSerializedPageSize();
                if (pair.isPresent()) {
                  writeToNewTsFile(tsBlock, tsFileWriter, pair.get().left, pair.get().right);
                }
              }
              break;
            case MetaMarker.CHUNK_GROUP_HEADER:
              ChunkGroupHeader chunkGroupHeader = reader.readChunkGroupHeader();
              currentDevice = chunkGroupHeader.getDeviceID();
              break;
            case MetaMarker.OPERATION_INDEX_RANGE:
              reader.readPlanIndex();
              break;
            default:
              MetaMarker.handleUnexpectedMarker(marker);
          }
        }
        succeed = true;
        completed++;
      } catch (Exception e) {
        System.out.println("error happened while renaming " + tsfileName);
        failedTsFileList.add(tsfileName);
        e.printStackTrace();
        failed++;
      }

      if (succeed) {
        System.out.println("successfully rename tsfile: " + tsfileName);
        try {
          Files.deleteIfExists(Paths.get(tsfileName));
        } catch (IOException e) {
          System.out.println("failed to delete: " + tsfileName);
        }
      } else {
        try {
          Files.deleteIfExists(Paths.get(tsfileName + ".rename"));
        } catch (IOException e) {
          System.out.println("failed to delete tmp file: " + tsfileName + ".rename");
        }
      }
      System.out.println(
          "rename completed: "
              + completed
              + ", failed: "
              + failed
              + ", total: "
              + tsfileList.size()
              + ", progress: "
              + (((double) completed + failed) / tsfileList.size())
              + ", elapse time: "
              + (System.nanoTime() - startTime) / 1_000_000_000
              + "s");
    }
  }

  private static Optional<Pair<String, MeasurementSchema>> rename(
      String device,
      String measurement,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressionType) {
    if (device.length() <= 17) {
      return Optional.empty();
    }
    String newDevice;
    MeasurementSchema measurementSchema;
    PartialPath devicePath;
    try {
      devicePath = new PartialPath(device);
    } catch (IllegalPathException e) {
      return Optional.empty();
    }

    String[] deviceNodes = devicePath.getNodes();
    if (deviceNodes == null
        || deviceNodes.length < 4
        || !"root".equals(deviceNodes[0])
        || !"bw".equals(deviceNodes[1])
        || !"baoshan".equals(deviceNodes[2])) {
      return Optional.empty();
    }

    if ("S".equals(deviceNodes[3])) {
      if (dataType != TSDataType.FLOAT
          || "value".equalsIgnoreCase(measurement)
          || deviceNodes.length != 6) {
        return Optional.empty();
      }
      newDevice =
          "root.bw.baoshan." + deviceNodes[4] + "." + deviceNodes[5] + "." + measurement + ".S";
      measurementSchema = new MeasurementSchema("value", dataType, encoding, compressionType);
      return Optional.of(new Pair<>(newDevice, measurementSchema));
    } else if ("I".equals(deviceNodes[3])) {
      if (deviceNodes.length != 7) {
        return Optional.empty();
      }

      if ("cl".equals(measurement)) {
        if (dataType != TSDataType.INT32) {
          return Optional.empty();
        }
      } else if ("hz".equals(measurement)) {
        if (dataType != TSDataType.INT32) {
          return Optional.empty();
        }
      } else if ("end_time".equals(measurement)) {
        if (dataType != TSDataType.INT64) {
          return Optional.empty();
        }
      } else {
        return Optional.empty();
      }
      newDevice =
          "root.bw.baoshan." + deviceNodes[4] + "." + deviceNodes[5] + "." + deviceNodes[6] + ".I";
      measurementSchema = new MeasurementSchema(measurement, dataType, encoding, compressionType);
      return Optional.of(new Pair<>(newDevice, measurementSchema));
    } else if ("F".equals(deviceNodes[3])) {
      if (dataType != TSDataType.DOUBLE
          || "value".equalsIgnoreCase(measurement)
          || deviceNodes.length != 5) {
        return Optional.empty();
      }
      newDevice = "root.bw.baoshan." + deviceNodes[4] + ".`00`" + "." + measurement;
      measurementSchema =
          new MeasurementSchema("value", TSDataType.FLOAT, encoding, compressionType);
      return Optional.of(new Pair<>(newDevice, measurementSchema));
    } else {
      if (dataType != TSDataType.FLOAT
          || "value".equalsIgnoreCase(measurement)
          || deviceNodes.length != 5) {
        return Optional.empty();
      }
      newDevice = "root.bw.baoshan." + deviceNodes[3] + "." + deviceNodes[4] + "." + measurement;
      measurementSchema =
          new MeasurementSchema("value", TSDataType.FLOAT, encoding, compressionType);
      return Optional.of(new Pair<>(newDevice, measurementSchema));
    }
  }

  private static void writeToNewTsFile(
      TsBlock tsBlock,
      TsFileWriter tsFileWriter,
      String newDeviceId,
      MeasurementSchema measurementSchema)
      throws IOException, WriteProcessException {
    Tablet tablet =
        new Tablet(
            newDeviceId, Collections.singletonList(measurementSchema), tsBlock.getPositionCount());
    tablet.rowSize = tsBlock.getPositionCount();
    Column valueColumn = tsBlock.getColumn(0);
    if (valueColumn instanceof FloatColumn) {
      long[] timestamps = tablet.timestamps;
      float[] values = (float[]) tablet.values[0];
      for (int i = 0, n = tsBlock.getPositionCount(); i < n; i++) {
        timestamps[i] = tsBlock.getTimeByIndex(i);
        values[i] = valueColumn.getFloat(i);
      }
    } else if (valueColumn instanceof DoubleColumn) {
      long[] timestamps = tablet.timestamps;
      float[] values = (float[]) tablet.values[0];
      for (int i = 0, n = tsBlock.getPositionCount(); i < n; i++) {
        timestamps[i] = tsBlock.getTimeByIndex(i);
        values[i] = (float) valueColumn.getDouble(i);
      }
    } else if (valueColumn instanceof IntColumn) {
      long[] timestamps = tablet.timestamps;
      int[] values = (int[]) tablet.values[0];
      for (int i = 0, n = tsBlock.getPositionCount(); i < n; i++) {
        timestamps[i] = tsBlock.getTimeByIndex(i);
        values[i] = valueColumn.getInt(i);
      }
    } else if (valueColumn instanceof LongColumn) {
      long[] timestamps = tablet.timestamps;
      long[] values = (long[]) tablet.values[0];
      for (int i = 0, n = tsBlock.getPositionCount(); i < n; i++) {
        timestamps[i] = tsBlock.getTimeByIndex(i);
        values[i] = valueColumn.getLong(i);
      }
    } else {
      throw new IllegalArgumentException(
          "data type should not be: " + valueColumn.getClass().getName());
    }
    sortTablet(tablet);
    tsFileWriter.write(tablet);
  }

  @SuppressWarnings({
    "squid:S3776"
  }) // ignore Cognitive Complexity of methods should not be too high
  public static void sortTablet(Tablet tablet) {
    /*
     * following part of code sort the batch data by time,
     * so we can insert continuous data in value list to get a better performance
     */
    // sort to get index, and use index to sort value list
    Integer[] index = new Integer[tablet.rowSize];
    for (int i = 0; i < tablet.rowSize; i++) {
      index[i] = i;
    }
    Arrays.sort(index, Comparator.comparingLong(o -> tablet.timestamps[o]));
    Arrays.sort(tablet.timestamps, 0, tablet.rowSize);
    int columnIndex = 0;
    for (int i = 0; i < tablet.getSchemas().size(); i++) {
      IMeasurementSchema schema = tablet.getSchemas().get(i);
      if (schema instanceof MeasurementSchema) {
        tablet.values[columnIndex] = sortList(tablet.values[columnIndex], schema.getType(), index);
        if (tablet.bitMaps != null && tablet.bitMaps[columnIndex] != null) {
          tablet.bitMaps[columnIndex] = sortBitMap(tablet.bitMaps[columnIndex], index);
        }
        columnIndex++;
      } else {
        int measurementSize = schema.getSubMeasurementsList().size();
        for (int j = 0; j < measurementSize; j++) {
          tablet.values[columnIndex] =
              sortList(
                  tablet.values[columnIndex],
                  schema.getSubMeasurementsTSDataTypeList().get(j),
                  index);
          if (tablet.bitMaps != null && tablet.bitMaps[columnIndex] != null) {
            tablet.bitMaps[columnIndex] = sortBitMap(tablet.bitMaps[columnIndex], index);
          }
          columnIndex++;
        }
      }
    }
  }

  /**
   * sort value list by index
   *
   * @param valueList value list
   * @param dataType data type
   * @param index index
   * @return sorted list
   */
  private static Object sortList(Object valueList, TSDataType dataType, Integer[] index) {
    switch (dataType) {
      case BOOLEAN:
        boolean[] boolValues = (boolean[]) valueList;
        boolean[] sortedValues = new boolean[boolValues.length];
        for (int i = 0; i < index.length; i++) {
          sortedValues[i] = boolValues[index[i]];
        }
        return sortedValues;
      case INT32:
        int[] intValues = (int[]) valueList;
        int[] sortedIntValues = new int[intValues.length];
        for (int i = 0; i < index.length; i++) {
          sortedIntValues[i] = intValues[index[i]];
        }
        return sortedIntValues;
      case INT64:
        long[] longValues = (long[]) valueList;
        long[] sortedLongValues = new long[longValues.length];
        for (int i = 0; i < index.length; i++) {
          sortedLongValues[i] = longValues[index[i]];
        }
        return sortedLongValues;
      case FLOAT:
        float[] floatValues = (float[]) valueList;
        float[] sortedFloatValues = new float[floatValues.length];
        for (int i = 0; i < index.length; i++) {
          sortedFloatValues[i] = floatValues[index[i]];
        }
        return sortedFloatValues;
      case DOUBLE:
        double[] doubleValues = (double[]) valueList;
        double[] sortedDoubleValues = new double[doubleValues.length];
        for (int i = 0; i < index.length; i++) {
          sortedDoubleValues[i] = doubleValues[index[i]];
        }
        return sortedDoubleValues;
      case TEXT:
        Binary[] binaryValues = (Binary[]) valueList;
        Binary[] sortedBinaryValues = new Binary[binaryValues.length];
        for (int i = 0; i < index.length; i++) {
          sortedBinaryValues[i] = binaryValues[index[i]];
        }
        return sortedBinaryValues;
      default:
        throw new UnSupportedDataTypeException(dataType.toString());
    }
  }

  /**
   * sort BitMap by index
   *
   * @param bitMap BitMap to be sorted
   * @param index index
   * @return sorted bitMap
   */
  private static BitMap sortBitMap(BitMap bitMap, Integer[] index) {
    BitMap sortedBitMap = new BitMap(bitMap.getSize());
    for (int i = 0; i < index.length; i++) {
      if (bitMap.isMarked(index[i])) {
        sortedBitMap.mark(i);
      }
    }
    return sortedBitMap;
  }
}
