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
package org.apache.iotdb.tsfile.write.chunk;

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.encoding.encoder.TSEncodingBuilder;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class AlignedChunkGroupWriterImpl implements IChunkGroupWriter {
  private static final Logger LOG = LoggerFactory.getLogger(AlignedChunkGroupWriterImpl.class);
  private final int maxNumberOfPointsInChunk;

  private final String deviceId;

  // measurementID -> ValueChunkWriter
  private Map<String, List<ValueChunkWriter>> valueChunkWriterMap = new LinkedHashMap<>();
  private List<TimeChunkWriter> timeChunkWriter = new ArrayList<>();
  private int writingIndex;

  private Set<String> writenMeasurementSet = new HashSet<>();

  private long lastTime = -1;

  public AlignedChunkGroupWriterImpl(String deviceId) {
    this.maxNumberOfPointsInChunk =
        TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInChunk();
    this.writingIndex = 0;
    this.deviceId = deviceId;
    String timeMeasurementId = "";
    CompressionType compressionType = TSFileDescriptor.getInstance().getConfig().getCompressor();
    TSEncoding tsEncoding =
        TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder());
    TSDataType timeType = TSFileDescriptor.getInstance().getConfig().getTimeSeriesDataType();
    Encoder encoder = TSEncodingBuilder.getEncodingBuilder(tsEncoding).getEncoder(timeType);
    timeChunkWriter.add(
        new TimeChunkWriter(timeMeasurementId, compressionType, tsEncoding, encoder));
  }

  @Override
  public void tryToAddSeriesWriter(MeasurementSchema measurementSchema) {
    if (!valueChunkWriterMap.containsKey(measurementSchema.getMeasurementId())) {
      ValueChunkWriter valueChunkWriter =
          new ValueChunkWriter(
              measurementSchema.getMeasurementId(),
              measurementSchema.getCompressor(),
              measurementSchema.getType(),
              measurementSchema.getEncodingType(),
              measurementSchema.getValueEncoder());
      valueChunkWriterMap.putIfAbsent(measurementSchema.getMeasurementId(), new ArrayList<>());
      valueChunkWriterMap.get(measurementSchema.getMeasurementId()).add(valueChunkWriter);
      tryToAddEmptyPageAndData(valueChunkWriter);
    }
  }

  @Override
  public void tryToAddSeriesWriter(List<MeasurementSchema> measurementSchemas) {
    for (MeasurementSchema schema : measurementSchemas) {
      if (!valueChunkWriterMap.containsKey(schema.getMeasurementId())) {
        ValueChunkWriter valueChunkWriter =
            new ValueChunkWriter(
                schema.getMeasurementId(),
                schema.getCompressor(),
                schema.getType(),
                schema.getEncodingType(),
                schema.getValueEncoder());
        valueChunkWriterMap.putIfAbsent(schema.getMeasurementId(), new ArrayList<>());
        valueChunkWriterMap.get(schema.getMeasurementId()).add(valueChunkWriter);
        tryToAddEmptyPageAndData(valueChunkWriter);
      }
    }
  }

  @Override
  public int write(long time, List<DataPoint> data) throws WriteProcessException, IOException {
    checkIsHistoryData("", time);

    for (DataPoint point : data) {
      writenMeasurementSet.add(point.getMeasurementId());
      boolean isNull = point.getValue() == null;
      ValueChunkWriter valueChunkWriter =
          valueChunkWriterMap.get(point.getMeasurementId()).get(writingIndex);
      switch (point.getType()) {
        case BOOLEAN:
          valueChunkWriter.write(time, (boolean) point.getValue(), isNull);
          break;
        case INT32:
          valueChunkWriter.write(time, (int) point.getValue(), isNull);
          break;
        case INT64:
          valueChunkWriter.write(time, (long) point.getValue(), isNull);
          break;
        case FLOAT:
          valueChunkWriter.write(time, (float) point.getValue(), isNull);
          break;
        case DOUBLE:
          valueChunkWriter.write(time, (double) point.getValue(), isNull);
          break;
        case TEXT:
          valueChunkWriter.write(time, (Binary) point.getValue(), isNull);
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Data type %s is not supported.", point.getType()));
      }
    }
    writeEmptyDataInOneRow(time);
    timeChunkWriter.get(writingIndex).write(time);
    lastTime = time;
    if (checkPageSizeAndMayOpenANewPage()) {
      writePageToPageBuffer();
    }
    return 1;
  }

  @Override
  public int write(Tablet tablet) throws WriteProcessException, IOException {
    int pointCount = 0;
    List<MeasurementSchema> measurementSchemas = tablet.getSchemas();
    for (int row = 0; row < tablet.rowSize; row++) {
      long time = tablet.timestamps[row];
      checkIsHistoryData("", time);
      for (int columnIndex = 0; columnIndex < measurementSchemas.size(); columnIndex++) {
        writenMeasurementSet.add(measurementSchemas.get(columnIndex).getMeasurementId());
        boolean isNull = false;
        // check isNull by bitMap in tablet
        if (tablet.bitMaps != null
            && tablet.bitMaps[columnIndex] != null
            && !tablet.bitMaps[columnIndex].isMarked(row)) {
          isNull = true;
        }
        ValueChunkWriter valueChunkWriter =
            valueChunkWriterMap
                .get(measurementSchemas.get(columnIndex).getMeasurementId())
                .get(writingIndex);
        switch (measurementSchemas.get(columnIndex).getType()) {
          case BOOLEAN:
            valueChunkWriter.write(time, ((boolean[]) tablet.values[columnIndex])[row], isNull);
            break;
          case INT32:
            valueChunkWriter.write(time, ((int[]) tablet.values[columnIndex])[row], isNull);
            break;
          case INT64:
            valueChunkWriter.write(time, ((long[]) tablet.values[columnIndex])[row], isNull);
            break;
          case FLOAT:
            valueChunkWriter.write(time, ((float[]) tablet.values[columnIndex])[row], isNull);
            break;
          case DOUBLE:
            valueChunkWriter.write(time, ((double[]) tablet.values[columnIndex])[row], isNull);
            break;
          case TEXT:
            valueChunkWriter.write(time, ((Binary[]) tablet.values[columnIndex])[row], isNull);
            break;
          default:
            throw new UnSupportedDataTypeException(
                String.format(
                    "Data type %s is not supported.",
                    measurementSchemas.get(columnIndex).getType()));
        }
      }
      writeEmptyDataInOneRow(time);
      timeChunkWriter.get(writingIndex).write(time);
      lastTime = time;
      //      System.out.println("\t\t"+time);
      if (checkPageSizeAndMayOpenANewPage()) {
        writePageToPageBuffer();
      }
      if (timeChunkWriter.get(writingIndex).getStatistics().getCount()
          >= maxNumberOfPointsInChunk) {
        sealAllChunks();
        TimeChunkWriter lastTC = timeChunkWriter.get(writingIndex);
        timeChunkWriter.add(
            new TimeChunkWriter(
                lastTC.measurementId,
                lastTC.compressionType,
                lastTC.encodingType,
                lastTC.timeEncoder));

        for (String key : valueChunkWriterMap.keySet()) {
          ValueChunkWriter lastVC = valueChunkWriterMap.get(key).get(writingIndex);
          valueChunkWriterMap
              .get(key)
              .add(
                  new ValueChunkWriter(
                      lastVC.measurementId,
                      lastVC.compressionType,
                      lastVC.dataType,
                      lastVC.encodingType,
                      lastVC.valueEncoder));
        }
        writingIndex++;
      }
      pointCount++;
    }
    return pointCount;
  }

  @Override
  public long flushToFileWriter(TsFileIOWriter tsfileWriter) throws IOException {
    LOG.debug("start flush device id:{}", deviceId);
    // make sure all the pages have been compressed into buffers, so that we can get correct
    // groupWriter.getCurrentChunkGroupSize().
    sealAllChunks();
    long currentChunkGroupSize = getCurrentChunkGroupSize();
    for (int i = 0; i <= writingIndex; i++) {
      LOG.debug(
          "\twriting an AlignedChunk.  TimeChunkCount:{}",
          timeChunkWriter.get(i).getStatistics().getCount());

      timeChunkWriter.get(i).writeToFileWriter(tsfileWriter);
      for (List<ValueChunkWriter> valueChunkWriter : valueChunkWriterMap.values()) {
        valueChunkWriter.get(i).writeToFileWriter(tsfileWriter);
      }
    }
    return currentChunkGroupSize;
  }

  @Override
  public long updateMaxGroupMemSize() { // TODO
    long bufferSize = 0;

    for (int i = 0; i <= writingIndex; i++) {
      bufferSize += timeChunkWriter.get(i).estimateMaxSeriesMemSize();

      for (List<ValueChunkWriter> valueChunkWriter : valueChunkWriterMap.values()) {
        bufferSize += valueChunkWriter.get(i).estimateMaxSeriesMemSize();
      }
    }
    return bufferSize;
  }

  @Override
  public long getCurrentChunkGroupSize() {
    long size = 0;
    for (int i = 0; i <= writingIndex; i++) {
      size += timeChunkWriter.get(i).getCurrentChunkSize();
      for (List<ValueChunkWriter> valueChunkWriter : valueChunkWriterMap.values()) {
        size += valueChunkWriter.get(i).getCurrentChunkSize();
      }
    }
    return size;
  }

  public void tryToAddEmptyPageAndData(ValueChunkWriter valueChunkWriter) {
    // add empty page
    for (int i = 0; i < timeChunkWriter.get(writingIndex).getNumOfPages(); i++) {
      valueChunkWriter.writeEmptyPageToPageBuffer();
    }

    // add empty data of currentPage
    for (long i = 0;
        i < timeChunkWriter.get(writingIndex).getPageWriter().getStatistics().getCount();
        i++) {
      valueChunkWriter.write(0, 0, true);
    }
  }

  private void writeEmptyDataInOneRow(long time) {
    for (Map.Entry<String, List<ValueChunkWriter>> entry : valueChunkWriterMap.entrySet()) {
      if (!writenMeasurementSet.contains(entry.getKey())) {
        entry.getValue().get(writingIndex).write(time, 0, true);
      }
    }
    writenMeasurementSet.clear();
  }

  /**
   * check occupied memory size, if it exceeds the PageSize threshold, construct a page and put it
   * to pageBuffer
   */
  private boolean checkPageSizeAndMayOpenANewPage() {
    if (timeChunkWriter.get(writingIndex).checkPageSizeAndMayOpenANewPage()) {
      return true;
    }
    for (List<ValueChunkWriter> writer : valueChunkWriterMap.values()) {
      if (writer.get(writingIndex).checkPageSizeAndMayOpenANewPage()) {
        return true;
      }
    }
    return false;
  }

  private void writePageToPageBuffer() {
    timeChunkWriter.get(writingIndex).writePageToPageBuffer();
    for (List<ValueChunkWriter> valueChunkWriter : valueChunkWriterMap.values()) {
      valueChunkWriter.get(writingIndex).writePageToPageBuffer();
    }
  }

  private void sealAllChunks() {
    timeChunkWriter.get(writingIndex).sealCurrentPage();
    for (List<ValueChunkWriter> valueChunkWriter : valueChunkWriterMap.values()) {
      valueChunkWriter.get(writingIndex).sealCurrentPage();
    }
  }

  private void checkIsHistoryData(String measurementId, long time) throws WriteProcessException {
    if (time <= lastTime) {
      throw new WriteProcessException(
          "Not allowed to write out-of-order data in timeseries "
              + deviceId
              + TsFileConstant.PATH_SEPARATOR
              + measurementId
              + ", time should later than "
              + lastTime);
    }
  }

  public List<String> getMeasurements() {
    return new ArrayList<>(valueChunkWriterMap.keySet());
  }

  public Long getLastTime() {
    return this.lastTime;
  }

  public void setLastTime(Long lastTime) {
    this.lastTime = lastTime;
  }
}
