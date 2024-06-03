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
package org.apache.iotdb.db.storageengine.dataregion.compaction.utils;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.AlignedFullPath;
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.IFullPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.NonAlignedFullPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.chunk.IChunkWriter;
import org.apache.tsfile.write.chunk.ValueChunkWriter;
import org.apache.tsfile.write.page.PageWriter;
import org.apache.tsfile.write.page.TimePageWriter;
import org.apache.tsfile.write.page.ValuePageWriter;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;
import static org.apache.tsfile.common.constant.TsFileConstant.TIME_COLUMN_ID;
import static org.apache.tsfile.utils.TsFileGeneratorUtils.getDataType;

public class TsFileGeneratorUtils {
  public static final String testStorageGroup = "root.testsg";

  public static List<IChunkWriter> createChunkWriter(
      List<PartialPath> timeseriesPaths,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressionTypes,
      boolean isAligned) {
    List<IChunkWriter> iChunkWriters = new ArrayList<>();
    if (!isAligned) {
      for (int i = 0; i < timeseriesPaths.size(); i++) {
        PartialPath path = timeseriesPaths.get(i);
        MeasurementSchema schema =
            new MeasurementSchema(
                path.getMeasurement(), dataTypes.get(i), encodings.get(i), compressionTypes.get(i));
        iChunkWriters.add(new ChunkWriterImpl(schema));
      }
    } else {
      List<IMeasurementSchema> schemas = new ArrayList<>();
      for (int i = 0; i < timeseriesPaths.size(); i++) {
        AlignedPath alignedPath = (AlignedPath) timeseriesPaths.get(i);
        schemas.add(
            new MeasurementSchema(
                alignedPath.getMeasurementList().get(0),
                dataTypes.get(i),
                encodings.get(i),
                compressionTypes.get(i)));
      }
      // use GZIP to compress time column
      MeasurementSchema timeSchema =
          new MeasurementSchema(
              TIME_COLUMN_ID,
              TSFileDescriptor.getInstance().getConfig().getTimeSeriesDataType(),
              TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
              CompressionType.GZIP);
      iChunkWriters.add(new AlignedChunkWriterImpl(timeSchema, schemas));
    }

    return iChunkWriters;
  }

  public static List<PartialPath> createTimeseries(
      int deviceIndex,
      List<Integer> meausurementIndex,
      List<TSDataType> dataTypes,
      boolean isAligned)
      throws IllegalPathException {
    List<PartialPath> timeseriesPath = new ArrayList<>();
    for (int i : meausurementIndex) {
      if (!isAligned) {
        timeseriesPath.add(
            new MeasurementPath(
                testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex + PATH_SEPARATOR + "s" + i,
                dataTypes.get(i)));
      } else {
        timeseriesPath.add(
            new AlignedPath(
                testStorageGroup + PATH_SEPARATOR + "d" + deviceIndex,
                Collections.singletonList("s" + i),
                Collections.singletonList(new MeasurementSchema("s" + i, dataTypes.get(i)))));
      }
    }
    return timeseriesPath;
  }

  public static List<IFullPath> createTimeseries(
      int deviceNum, int measurementNum, boolean isAligned) throws IllegalPathException {
    List<IFullPath> timeseriesPath = new ArrayList<>();
    for (int d = 0; d < deviceNum; d++) {
      for (int i = 0; i < measurementNum; i++) {
        TSDataType dataType = getDataType(i);
        if (!isAligned) {
          timeseriesPath.add(
              new NonAlignedFullPath(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      testStorageGroup + PATH_SEPARATOR + "d" + d),
                  new MeasurementSchema("s" + i, dataType)));
        } else {
          timeseriesPath.add(
              new AlignedFullPath(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      testStorageGroup + PATH_SEPARATOR + "d" + d),
                  Collections.singletonList("s" + i),
                  Collections.singletonList(new MeasurementSchema("s" + i, dataType))));
        }
      }
    }

    return timeseriesPath;
  }

  public static void writeOneAlignedPage(
      AlignedChunkWriterImpl alignedChunkWriter, List<TimeRange> timeRanges, boolean isSeq) {
    // write time page
    TimePageWriter timePageWriter = alignedChunkWriter.getTimeChunkWriter().getPageWriter();
    for (TimeRange timeRange : timeRanges) {
      for (long timestamp = timeRange.getMin(); timestamp <= timeRange.getMax(); timestamp++) {
        timePageWriter.write(timestamp);
      }
    }

    // write value pages
    for (ValueChunkWriter valueChunkWriter : alignedChunkWriter.getValueChunkWriterList()) {
      ValuePageWriter valuePageWriter = valueChunkWriter.getPageWriter();
      for (TimeRange timeRange : timeRanges) {
        for (long timestamp = timeRange.getMin(); timestamp <= timeRange.getMax(); timestamp++) {
          writeAlignedPoint(valuePageWriter, timestamp, isSeq);
        }
      }
    }

    alignedChunkWriter.sealCurrentPage();
  }

  public static void writeOneNonAlignedPage(
      ChunkWriterImpl chunkWriter, List<TimeRange> timeRanges, boolean isSeq) {
    PageWriter pageWriter = chunkWriter.getPageWriter();
    for (TimeRange timeRange : timeRanges) {
      for (long timestamp = timeRange.getMin(); timestamp <= timeRange.getMax(); timestamp++) {
        writeNonAlignedPoint(pageWriter, timestamp, isSeq);
      }
    }
    chunkWriter.sealCurrentPage();
  }

  public static void writeNonAlignedPoint(PageWriter pageWriter, long timestamp, boolean isSeq) {
    switch (pageWriter.getStatistics().getType()) {
      case TEXT:
      case STRING:
      case BLOB:
        pageWriter.write(
            timestamp, new Binary(isSeq ? "seqText" : "unSeqText", TSFileConfig.STRING_CHARSET));
        break;
      case DOUBLE:
        pageWriter.write(timestamp, isSeq ? timestamp + 0.01 : 100000.01 + timestamp);
        break;
      case BOOLEAN:
        pageWriter.write(timestamp, isSeq);
        break;
      case INT64:
      case TIMESTAMP:
        pageWriter.write(timestamp, isSeq ? timestamp : 100000L + timestamp);
        break;
      case INT32:
      case DATE:
        pageWriter.write(timestamp, isSeq ? (int) timestamp : (int) (100000 + timestamp));
        break;
      case FLOAT:
        pageWriter.write(
            timestamp, isSeq ? (float) (timestamp + 0.1) : (float) (100000.1 + timestamp));
        break;
      default:
        throw new UnsupportedOperationException(
            "Unknown data type " + pageWriter.getStatistics().getType());
    }
  }

  public static void writeNonAlignedChunk(
      ChunkWriterImpl chunkWriter,
      TsFileIOWriter tsFileIOWriter,
      List<TimeRange> pages,
      boolean isSeq)
      throws IOException {
    PageWriter pageWriter = chunkWriter.getPageWriter();
    for (TimeRange page : pages) {
      // write a page
      for (long timestamp = page.getMin(); timestamp <= page.getMax(); timestamp++) {
        writeNonAlignedPoint(pageWriter, timestamp, isSeq);
      }
      // seal the current page
      chunkWriter.sealCurrentPage();
    }
    // seal current chunk
    chunkWriter.writeToFileWriter(tsFileIOWriter);
  }

  public static void writeNullPoint(ValuePageWriter valuePageWriter, long timestamp) {
    valuePageWriter.write(timestamp, null, true);
  }

  public static void writeAlignedPoint(
      ValuePageWriter valuePageWriter, long timestamp, boolean isSeq) {
    switch (valuePageWriter.getStatistics().getType()) {
      case TEXT:
      case STRING:
      case BLOB:
        valuePageWriter.write(
            timestamp,
            new Binary(isSeq ? "seqText" : "unSeqText", TSFileConfig.STRING_CHARSET),
            false);
        break;
      case DOUBLE:
        valuePageWriter.write(timestamp, isSeq ? timestamp + 0.01 : 100000.01 + timestamp, false);
        break;
      case BOOLEAN:
        valuePageWriter.write(timestamp, isSeq, false);
        break;
      case INT64:
      case TIMESTAMP:
        valuePageWriter.write(timestamp, isSeq ? timestamp : 100000L + timestamp, false);
        break;
      case INT32:
      case DATE:
        valuePageWriter.write(
            timestamp, isSeq ? (int) timestamp : (int) (100000 + timestamp), false);
        break;
      case FLOAT:
        valuePageWriter.write(
            timestamp, isSeq ? timestamp + (float) 0.1 : (float) (100000.1 + timestamp), false);
        break;
      default:
        throw new UnsupportedOperationException(
            "Unknown data type " + valuePageWriter.getStatistics().getType());
    }
  }

  public static void writeAlignedChunk(
      AlignedChunkWriterImpl alignedChunkWriter,
      TsFileIOWriter tsFileIOWriter,
      List<TimeRange> pages,
      boolean isSeq)
      throws IOException {
    TimePageWriter timePageWriter = alignedChunkWriter.getTimeChunkWriter().getPageWriter();
    for (TimeRange page : pages) {
      // write time page
      for (long timestamp = page.getMin(); timestamp <= page.getMax(); timestamp++) {
        timePageWriter.write(timestamp);
      }
      // seal time page
      alignedChunkWriter.getTimeChunkWriter().sealCurrentPage();

      // write value page
      for (ValueChunkWriter valueChunkWriter : alignedChunkWriter.getValueChunkWriterList()) {
        ValuePageWriter valuePageWriter = valueChunkWriter.getPageWriter();
        for (long timestamp = page.getMin(); timestamp <= page.getMax(); timestamp++) {
          writeAlignedPoint(valuePageWriter, timestamp, isSeq);
        }
        // seal sub value page
        valueChunkWriter.sealCurrentPage();
      }
    }
    // seal time chunk and value chunks
    alignedChunkWriter.writeToFileWriter(tsFileIOWriter);
  }

  public static List<TSEncoding> createEncodingType(int num) {
    List<TSEncoding> encodings = new ArrayList<>();
    for (int i = 0; i < num; i++) {
      int j = i % 6;
      switch (j) {
        case 0: // boolean
          encodings.add(TSEncoding.RLE);
          break;
        case 5: // TEXT
          encodings.add(TSEncoding.DICTIONARY);
          break;
        default:
          encodings.add(TSEncoding.GORILLA);
          break;
      }
    }
    return encodings;
  }

  public static List<CompressionType> createCompressionType(int num) {
    List<CompressionType> compressionTypes = new ArrayList<>();
    for (int i = 0; i < num; i++) {
      // compressionTypes.add(CompressionType.LZ4);
      switch (i % 6) {
        case 0:
          compressionTypes.add(CompressionType.UNCOMPRESSED);
          break;
        case 1:
          compressionTypes.add(CompressionType.SNAPPY);
          break;
        case 2:
          compressionTypes.add(CompressionType.GZIP);
          break;
        case 3:
          compressionTypes.add(CompressionType.LZ4);
          break;
        default:
          compressionTypes.add(CompressionType.ZSTD);
          break;
      }
    }
    return compressionTypes;
  }

  public static List<TSDataType> createDataType(int num) {
    List<TSDataType> dataTypes = new ArrayList<>();
    for (int i = 0; i < num; i++) {
      switch (i % 6) {
        case 0:
          dataTypes.add(TSDataType.BOOLEAN);
          break;
        case 1:
          dataTypes.add(TSDataType.INT32);
          break;
        case 2:
          dataTypes.add(TSDataType.INT64);
          break;
        case 3:
          dataTypes.add(TSDataType.FLOAT);
          break;
        case 4:
          dataTypes.add(TSDataType.DOUBLE);
          break;
        case 5:
          dataTypes.add(TSDataType.TEXT);
          break;
      }
    }
    return dataTypes;
  }

  public static TsFileResource generateSingleNonAlignedSeriesFile(
      String device,
      String measurement,
      TimeRange[] chunkTimeRanges,
      TSEncoding encoding,
      CompressionType compressionType,
      String filePath)
      throws IOException {
    TsFileResource seqResource1 = new TsFileResource(new File(filePath));

    CompactionTestFileWriter writer1 = new CompactionTestFileWriter(seqResource1);
    writer1.startChunkGroup(device);
    writer1.generateSimpleNonAlignedSeriesToCurrentDevice(
        measurement, chunkTimeRanges, encoding, compressionType);
    writer1.endChunkGroup();
    writer1.endFile();
    writer1.close();
    return seqResource1;
  }

  public static TsFileResource generateSingleNonAlignedSeriesFile(
      String device,
      String measurement,
      TimeRange[][] chunkTimeRanges,
      TSEncoding encoding,
      CompressionType compressionType,
      String filePath)
      throws IOException {
    TsFileResource seqResource1 = new TsFileResource(new File(filePath));

    CompactionTestFileWriter writer1 = new CompactionTestFileWriter(seqResource1);
    writer1.startChunkGroup(device);
    writer1.generateSimpleNonAlignedSeriesToCurrentDevice(
        measurement, chunkTimeRanges, encoding, compressionType);
    writer1.endChunkGroup();
    writer1.endFile();
    writer1.close();
    return seqResource1;
  }

  public static TsFileResource generateSingleNonAlignedSeriesFile(
      String device,
      String measurement,
      TimeRange[][][] chunkTimeRanges,
      TSEncoding encoding,
      CompressionType compressionType,
      boolean isSeq,
      String filePath)
      throws IOException {
    TsFileResource seqResource1 = new TsFileResource(new File(filePath));

    CompactionTestFileWriter writer1 = new CompactionTestFileWriter(seqResource1);
    writer1.startChunkGroup(device);
    writer1.generateSimpleNonAlignedSeriesToCurrentDevice(
        measurement, chunkTimeRanges, encoding, compressionType);
    writer1.endChunkGroup();
    writer1.endFile();
    writer1.close();
    return seqResource1;
  }

  public static TsFileResource generateSingleAlignedSeriesFile(
      String device,
      List<String> measurement,
      TimeRange[] chunkTimeRanges,
      TSEncoding encoding,
      CompressionType compressionType,
      String filePath)
      throws IOException {
    TsFileResource seqResource1 = new TsFileResource(new File(filePath));

    CompactionTestFileWriter writer1 = new CompactionTestFileWriter(seqResource1);
    writer1.startChunkGroup(device);
    writer1.generateSimpleAlignedSeriesToCurrentDevice(
        measurement, chunkTimeRanges, encoding, compressionType);
    writer1.endChunkGroup();
    writer1.endFile();
    writer1.close();
    return seqResource1;
  }

  public static TsFileResource generateSingleAlignedSeriesFile(
      String device,
      List<String> measurement,
      TimeRange[][] chunkTimeRanges,
      TSEncoding encoding,
      CompressionType compressionType,
      String filePath)
      throws IOException {
    TsFileResource seqResource1 = new TsFileResource(new File(filePath));

    CompactionTestFileWriter writer1 = new CompactionTestFileWriter(seqResource1);
    writer1.startChunkGroup(device);
    writer1.generateSimpleAlignedSeriesToCurrentDevice(
        measurement, chunkTimeRanges, encoding, compressionType);
    writer1.endChunkGroup();
    writer1.endFile();
    writer1.close();
    return seqResource1;
  }

  public static TsFileResource generateSingleAlignedSeriesFile(
      String device,
      List<String> measurement,
      TimeRange[][][] chunkTimeRanges,
      TSEncoding encoding,
      CompressionType compressionType,
      String filePath)
      throws IOException {
    TsFileResource seqResource1 = new TsFileResource(new File(filePath));

    CompactionTestFileWriter writer1 = new CompactionTestFileWriter(seqResource1);
    writer1.startChunkGroup(device);
    writer1.generateSimpleAlignedSeriesToCurrentDevice(
        measurement, chunkTimeRanges, encoding, compressionType);
    writer1.endChunkGroup();
    writer1.endFile();
    writer1.close();
    return seqResource1;
  }
}
