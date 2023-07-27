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

import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.page.PageWriter;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class CompactionTestFileWriter {

  private TsFileResource resource;
  private TsFileIOWriter fileWriter;
  private static final String SG_NAME = "root.testsg";
  private String currentDeviceId;
  private long currentDeviceStartTime;
  private long currentDeviceEndTime;

  public CompactionTestFileWriter(TsFileResource emptyFile) throws IOException {
    this.resource = emptyFile;
    fileWriter = new TsFileIOWriter(emptyFile.getTsFile());
  }

  public String startChunkGroup(String deviceName) throws IOException {
    currentDeviceId = SG_NAME + "." + deviceName;
    fileWriter.startChunkGroup(currentDeviceId);
    currentDeviceStartTime = Long.MAX_VALUE;
    currentDeviceEndTime = Long.MIN_VALUE;
    return currentDeviceId;
  }

  public void endChunkGroup() throws IOException {
    resource.updateStartTime(currentDeviceId, currentDeviceStartTime);
    resource.updateEndTime(currentDeviceId, currentDeviceEndTime);
    fileWriter.endChunkGroup();
  }

  public void endFile() throws IOException {
    fileWriter.endFile();
    resource.serialize();
  }

  public void close() throws IOException {
    fileWriter.close();
  }

  public void generateSimpleNonAlignedSeriesToCurrentDevice(
      String measurementName,
      TimeRange[] toGenerateChunkTimeRanges,
      TSEncoding encoding,
      CompressionType compressionType)
      throws IOException {
    MeasurementSchema schema =
        new MeasurementSchema(measurementName, TSDataType.INT32, encoding, compressionType);
    for (TimeRange timeRange : toGenerateChunkTimeRanges) {
      ChunkWriterImpl chunkWriter = new ChunkWriterImpl(schema);
      currentDeviceStartTime = Math.min(timeRange.getMin(), currentDeviceStartTime);
      currentDeviceEndTime = Math.max(timeRange.getMax(), currentDeviceEndTime);
      for (long time = timeRange.getMin(); time <= timeRange.getMax(); time++) {
        chunkWriter.write(time, new Random().nextInt());
      }
      chunkWriter.sealCurrentPage();
      chunkWriter.writeToFileWriter(fileWriter);
    }
  }

  public void generateSimpleNonAlignedSeriesToCurrentDevice(
      String measurementName,
      TimeRange[][] toGenerateChunkPageTimeRanges,
      TSEncoding encoding,
      CompressionType compressionType)
      throws IOException {
    MeasurementSchema schema =
        new MeasurementSchema(measurementName, TSDataType.INT32, encoding, compressionType);
    for (TimeRange[] toGenerateChunk : toGenerateChunkPageTimeRanges) {
      ChunkWriterImpl chunkWriter = new ChunkWriterImpl(schema);
      for (TimeRange toGeneratePage : toGenerateChunk) {
        PageWriter pageWriter = chunkWriter.getPageWriter();
        currentDeviceStartTime = Math.min(toGeneratePage.getMin(), currentDeviceStartTime);
        currentDeviceEndTime = Math.max(toGeneratePage.getMax(), currentDeviceEndTime);
        for (long time = toGeneratePage.getMin(); time <= toGeneratePage.getMax(); time++) {
          pageWriter.write(time, new Random().nextInt());
        }
        chunkWriter.sealCurrentPage();
      }
      chunkWriter.writeToFileWriter(fileWriter);
    }
  }

  public void generateSimpleNonAlignedSeriesToCurrentDevice(
      String measurementName,
      TimeRange[][][] toGenerateChunkPagePointsTimeRanges,
      TSEncoding encoding,
      CompressionType compressionType)
      throws IOException {
    MeasurementSchema schema =
        new MeasurementSchema(measurementName, TSDataType.INT32, encoding, compressionType);
    for (TimeRange[][] toGenerateChunk : toGenerateChunkPagePointsTimeRanges) {
      ChunkWriterImpl chunkWriter = new ChunkWriterImpl(schema);
      for (TimeRange[] toGeneratePage : toGenerateChunk) {
        PageWriter pageWriter = chunkWriter.getPageWriter();
        for (TimeRange pagePointTimeRange : toGeneratePage) {
          currentDeviceStartTime = Math.min(pagePointTimeRange.getMin(), currentDeviceStartTime);
          currentDeviceEndTime = Math.max(pagePointTimeRange.getMax(), currentDeviceEndTime);
          for (long time = pagePointTimeRange.getMin();
              time <= pagePointTimeRange.getMax();
              time++) {
            pageWriter.write(time, new Random().nextInt());
          }
        }
        chunkWriter.sealCurrentPage();
      }
      chunkWriter.writeToFileWriter(fileWriter);
    }
  }

  public void generateSimpleAlignedSeriesToCurrentDevice(
      List<String> measurementNames,
      TimeRange[] toGenerateChunkTimeRanges,
      TSEncoding encoding,
      CompressionType compressionType)
      throws IOException {
    List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
    for (String measurementName : measurementNames) {
      measurementSchemas.add(
          new MeasurementSchema(measurementName, TSDataType.INT32, encoding, compressionType));
    }
    for (TimeRange toGenerateChunk : toGenerateChunkTimeRanges) {
      AlignedChunkWriterImpl alignedChunkWriter = new AlignedChunkWriterImpl(measurementSchemas);
      currentDeviceStartTime = Math.min(toGenerateChunk.getMin(), currentDeviceStartTime);
      currentDeviceEndTime = Math.max(toGenerateChunk.getMax(), currentDeviceEndTime);
      for (long time = toGenerateChunk.getMin(); time <= toGenerateChunk.getMax(); time++) {
        alignedChunkWriter.getTimeChunkWriter().write(time);
        for (int i = 0; i < measurementNames.size(); i++) {
          alignedChunkWriter
              .getValueChunkWriterByIndex(i)
              .write(time, new Random().nextInt(), false);
        }
      }
      alignedChunkWriter.writeToFileWriter(fileWriter);
    }
  }

  public void generateSimpleAlignedSeriesToCurrentDevice(
      List<String> measurementNames,
      TimeRange[][] toGenerateChunkPageTimeRanges,
      TSEncoding encoding,
      CompressionType compressionType)
      throws IOException {
    List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
    for (String measurementName : measurementNames) {
      measurementSchemas.add(
          new MeasurementSchema(measurementName, TSDataType.INT32, encoding, compressionType));
    }
    for (TimeRange[] toGenerateChunk : toGenerateChunkPageTimeRanges) {
      AlignedChunkWriterImpl alignedChunkWriter = new AlignedChunkWriterImpl(measurementSchemas);
      for (TimeRange toGeneratePageTimeRange : toGenerateChunk) {
        currentDeviceStartTime = Math.min(toGeneratePageTimeRange.getMin(), currentDeviceStartTime);
        currentDeviceEndTime = Math.max(toGeneratePageTimeRange.getMax(), currentDeviceEndTime);
        for (long time = toGeneratePageTimeRange.getMin();
            time <= toGeneratePageTimeRange.getMax();
            time++) {
          alignedChunkWriter.write(time);
          for (int i = 0; i < measurementNames.size(); i++) {
            alignedChunkWriter
                .getValueChunkWriterByIndex(i)
                .getPageWriter()
                .write(time, new Random().nextInt(), false);
          }
        }
        alignedChunkWriter.sealCurrentPage();
      }
      alignedChunkWriter.writeToFileWriter(fileWriter);
    }
  }

  public void generateSimpleAlignedSeriesToCurrentDevice(
      List<String> measurementNames,
      TimeRange[][][] toGenerateChunkPageTimeRanges,
      TSEncoding encoding,
      CompressionType compressionType)
      throws IOException {
    List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
    for (String measurementName : measurementNames) {
      measurementSchemas.add(
          new MeasurementSchema(measurementName, TSDataType.INT32, encoding, compressionType));
    }
    for (TimeRange[][] toGenerateChunk : toGenerateChunkPageTimeRanges) {
      AlignedChunkWriterImpl alignedChunkWriter = new AlignedChunkWriterImpl(measurementSchemas);
      for (TimeRange[] toGeneratePageTimeRanges : toGenerateChunk) {
        for (TimeRange pointsTimeRange : toGeneratePageTimeRanges) {
          currentDeviceStartTime = Math.min(pointsTimeRange.getMin(), currentDeviceStartTime);
          currentDeviceEndTime = Math.max(pointsTimeRange.getMax(), currentDeviceEndTime);
          for (long time = pointsTimeRange.getMin(); time <= pointsTimeRange.getMax(); time++) {
            alignedChunkWriter.write(time);
            for (int i = 0; i < measurementNames.size(); i++) {
              alignedChunkWriter
                  .getValueChunkWriterByIndex(i)
                  .getPageWriter()
                  .write(time, new Random().nextInt(), false);
            }
          }
        }
        alignedChunkWriter.sealCurrentPage();
      }
      alignedChunkWriter.writeToFileWriter(fileWriter);
    }
  }
}
