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
package org.apache.tsfile.write.chunk;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.encoding.encoder.Encoder;
import org.apache.tsfile.encoding.encoder.TSEncodingBuilder;
import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.encrypt.EncryptUtils;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.record.Tablet.ColumnCategory;
import org.apache.tsfile.write.record.datapoint.DataPoint;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class AlignedChunkGroupWriterImpl implements IChunkGroupWriter {
  private static final Logger LOG = LoggerFactory.getLogger(AlignedChunkGroupWriterImpl.class);

  private final IDeviceID deviceId;

  // measurementID -> ValueChunkWriter
  private final Map<String, ValueChunkWriter> valueChunkWriterMap = new LinkedHashMap<>();

  private final TimeChunkWriter timeChunkWriter;

  private final EncryptParameter encryprParam;

  private long lastTime = Long.MIN_VALUE;
  private boolean isInitLastTime = false;

  public AlignedChunkGroupWriterImpl(IDeviceID deviceId) {
    this.deviceId = deviceId;
    String timeMeasurementId = "";
    CompressionType compressionType = TSFileDescriptor.getInstance().getConfig().getCompressor();
    TSEncoding tsEncoding =
        TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder());
    TSDataType timeType = TSFileDescriptor.getInstance().getConfig().getTimeSeriesDataType();
    Encoder encoder = TSEncodingBuilder.getEncodingBuilder(tsEncoding).getEncoder(timeType);
    this.encryprParam = EncryptUtils.encryptParam;
    timeChunkWriter =
        new TimeChunkWriter(
            timeMeasurementId, compressionType, tsEncoding, encoder, this.encryprParam);
  }

  public AlignedChunkGroupWriterImpl(IDeviceID deviceId, EncryptParameter encryptParam) {
    this.deviceId = deviceId;
    String timeMeasurementId = "";
    CompressionType compressionType = TSFileDescriptor.getInstance().getConfig().getCompressor();
    TSEncoding tsEncoding =
        TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder());
    TSDataType timeType = TSFileDescriptor.getInstance().getConfig().getTimeSeriesDataType();
    Encoder encoder = TSEncodingBuilder.getEncodingBuilder(tsEncoding).getEncoder(timeType);
    this.encryprParam = encryptParam;
    timeChunkWriter =
        new TimeChunkWriter(
            timeMeasurementId, compressionType, tsEncoding, encoder, this.encryprParam);
  }

  @Override
  public void tryToAddSeriesWriter(IMeasurementSchema measurementSchema) throws IOException {
    tryToAddSeriesWriterInternal(measurementSchema);
  }

  public ValueChunkWriter tryToAddSeriesWriterInternal(IMeasurementSchema measurementSchema)
      throws IOException {
    ValueChunkWriter valueChunkWriter =
        valueChunkWriterMap.get(measurementSchema.getMeasurementName());
    if (valueChunkWriter == null) {
      valueChunkWriter =
          new ValueChunkWriter(
              measurementSchema.getMeasurementName(),
              measurementSchema.getCompressor(),
              measurementSchema.getType(),
              measurementSchema.getEncodingType(),
              measurementSchema.getValueEncoder());
      valueChunkWriterMap.put(measurementSchema.getMeasurementName(), valueChunkWriter);
      tryToAddEmptyPageAndData(valueChunkWriter);
    }
    return valueChunkWriter;
  }

  @Override
  public void tryToAddSeriesWriter(List<IMeasurementSchema> measurementSchemas) throws IOException {
    for (IMeasurementSchema schema : measurementSchemas) {
      if (!valueChunkWriterMap.containsKey(schema.getMeasurementName())) {
        ValueChunkWriter valueChunkWriter =
            new ValueChunkWriter(
                schema.getMeasurementName(),
                schema.getCompressor(),
                schema.getType(),
                schema.getEncodingType(),
                schema.getValueEncoder());
        valueChunkWriterMap.put(schema.getMeasurementName(), valueChunkWriter);
        tryToAddEmptyPageAndData(valueChunkWriter);
      }
    }
  }

  @Override
  public int write(long time, List<DataPoint> data) throws WriteProcessException, IOException {
    checkIsHistoryData(time);
    List<ValueChunkWriter> emptyValueChunkWriters = new ArrayList<>();
    Set<String> existingMeasurements =
        data.stream().map(DataPoint::getMeasurementId).collect(Collectors.toSet());
    for (Map.Entry<String, ValueChunkWriter> entry : valueChunkWriterMap.entrySet()) {
      if (!existingMeasurements.contains(entry.getKey())) {
        emptyValueChunkWriters.add(entry.getValue());
      }
    }
    for (DataPoint point : data) {
      boolean isNull = point.getValue() == null;
      ValueChunkWriter valueChunkWriter = valueChunkWriterMap.get(point.getMeasurementId());
      switch (point.getType()) {
        case BOOLEAN:
          valueChunkWriter.write(time, (boolean) point.getValue(), isNull);
          break;
        case INT32:
        case DATE:
          valueChunkWriter.write(time, (int) point.getValue(), isNull);
          break;
        case INT64:
        case TIMESTAMP:
          valueChunkWriter.write(time, (long) point.getValue(), isNull);
          break;
        case FLOAT:
          valueChunkWriter.write(time, (float) point.getValue(), isNull);
          break;
        case DOUBLE:
          valueChunkWriter.write(time, (double) point.getValue(), isNull);
          break;
        case TEXT:
        case BLOB:
        case STRING:
          valueChunkWriter.write(time, (Binary) point.getValue(), isNull);
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Data type %s is not supported.", point.getType()));
      }
    }
    if (!emptyValueChunkWriters.isEmpty()) {
      writeEmptyDataInOneRow(emptyValueChunkWriters);
    }
    timeChunkWriter.write(time);
    lastTime = time;
    isInitLastTime = true;
    if (checkPageSizeAndMayOpenANewPage()) {
      writePageToPageBuffer();
    }
    return 1;
  }

  @Override
  public int write(Tablet tablet) throws IOException, WriteProcessException {
    return write(tablet, 0, tablet.getRowSize());
  }

  @Override
  public int write(Tablet tablet, int startRowIndex, int endRowIndex)
      throws WriteProcessException, IOException {
    int pointCount = 0;
    List<IMeasurementSchema> measurementSchemas = tablet.getSchemas();
    List<ValueChunkWriter> emptyValueChunkWriters = new ArrayList<>();
    // TODO: should we allow duplicated measurements in a Tablet?
    Set<String> existingMeasurements =
        measurementSchemas.stream()
            .map(IMeasurementSchema::getMeasurementName)
            .collect(Collectors.toSet());
    for (Map.Entry<String, ValueChunkWriter> entry : valueChunkWriterMap.entrySet()) {
      if (!existingMeasurements.contains(entry.getKey())) {
        emptyValueChunkWriters.add(entry.getValue());
      }
    }
    // TODO: changing to a column-first style by calculating the remaining page space of each
    // column firsts
    for (int row = startRowIndex; row < endRowIndex; row++) {
      long time = tablet.timestamps[row];
      checkIsHistoryData(time);
      for (int columnIndex = 0; columnIndex < tablet.getSchemas().size(); columnIndex++) {
        if (tablet.getColumnTypes() != null
            && tablet.getColumnTypes().get(columnIndex) != ColumnCategory.MEASUREMENT) {
          continue;
        }

        boolean isNull =
            tablet.bitMaps != null
                && tablet.bitMaps[columnIndex] != null
                && tablet.bitMaps[columnIndex].isMarked(row);
        // check isNull by bitMap in tablet
        ValueChunkWriter valueChunkWriter =
            tryToAddSeriesWriterInternal(measurementSchemas.get(columnIndex));
        switch (measurementSchemas.get(columnIndex).getType()) {
          case BOOLEAN:
            valueChunkWriter.write(time, ((boolean[]) tablet.values[columnIndex])[row], isNull);
            break;
          case INT32:
            valueChunkWriter.write(time, ((int[]) tablet.values[columnIndex])[row], isNull);
            break;
          case DATE:
            valueChunkWriter.write(
                time,
                isNull
                    ? 0
                    : DateUtils.parseDateExpressionToInt(
                        ((LocalDate[]) tablet.values[columnIndex])[row]),
                isNull);
            break;
          case INT64:
          case TIMESTAMP:
            valueChunkWriter.write(time, ((long[]) tablet.values[columnIndex])[row], isNull);
            break;
          case FLOAT:
            valueChunkWriter.write(time, ((float[]) tablet.values[columnIndex])[row], isNull);
            break;
          case DOUBLE:
            valueChunkWriter.write(time, ((double[]) tablet.values[columnIndex])[row], isNull);
            break;
          case TEXT:
          case BLOB:
          case STRING:
            valueChunkWriter.write(time, ((Binary[]) tablet.values[columnIndex])[row], isNull);
            break;
          default:
            throw new UnSupportedDataTypeException(
                String.format(
                    "Data type %s is not supported.",
                    measurementSchemas.get(columnIndex).getType()));
        }
      }
      // TODO: we can write the null columns after whole insertion, according to the point number
      //  in the time chunk before and after, no need to do it in a row-by-row manner
      if (!emptyValueChunkWriters.isEmpty()) {
        writeEmptyDataInOneRow(emptyValueChunkWriters);
      }
      timeChunkWriter.write(time);
      lastTime = time;
      isInitLastTime = true;
      if (checkPageSizeAndMayOpenANewPage()) {
        writePageToPageBuffer();
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
    timeChunkWriter.writeToFileWriter(tsfileWriter);
    for (ValueChunkWriter valueChunkWriter : valueChunkWriterMap.values()) {
      valueChunkWriter.writeToFileWriter(tsfileWriter);
    }
    return currentChunkGroupSize;
  }

  @Override
  public long updateMaxGroupMemSize() {
    long bufferSize = timeChunkWriter.estimateMaxSeriesMemSize();
    for (ValueChunkWriter valueChunkWriter : valueChunkWriterMap.values()) {
      bufferSize += valueChunkWriter.estimateMaxSeriesMemSize();
    }
    return bufferSize;
  }

  @Override
  public long getCurrentChunkGroupSize() {
    long size = timeChunkWriter.getCurrentChunkSize();
    for (ValueChunkWriter valueChunkWriter : valueChunkWriterMap.values()) {
      size += valueChunkWriter.getCurrentChunkSize();
    }
    return size;
  }

  public void tryToAddEmptyPageAndData(ValueChunkWriter valueChunkWriter) throws IOException {
    // add empty page
    for (int i = 0; i < timeChunkWriter.getNumOfPages(); i++) {
      valueChunkWriter.writeEmptyPageToPageBuffer();
    }

    // add empty data of currentPage
    for (long i = 0; i < timeChunkWriter.getPageWriter().getStatistics().getCount(); i++) {
      valueChunkWriter.write(0, 0, true);
    }
  }

  private void writeEmptyDataInOneRow(List<ValueChunkWriter> valueChunkWriterList) {
    for (ValueChunkWriter valueChunkWriter : valueChunkWriterList) {
      TSDataType dataType = valueChunkWriter.getDataType();
      switch (dataType) {
        case BOOLEAN:
          valueChunkWriter.write(-1, false, true);
          break;
        case INT32:
        case DATE:
          valueChunkWriter.write(-1, 0, true);
          break;
        case INT64:
        case TIMESTAMP:
          valueChunkWriter.write(-1, 0L, true);
          break;
        case FLOAT:
          valueChunkWriter.write(-1, 0.0f, true);
          break;
        case DOUBLE:
          valueChunkWriter.write(-1, 0.0d, true);
          break;
        case TEXT:
        case BLOB:
        case STRING:
          valueChunkWriter.write(-1, null, true);
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Data type %s is not supported.", dataType));
      }
    }
  }

  /**
   * check occupied memory size, if it exceeds the PageSize threshold, construct a page and put it
   * to pageBuffer
   */
  private boolean checkPageSizeAndMayOpenANewPage() {
    if (timeChunkWriter.checkPageSizeAndMayOpenANewPage()) {
      return true;
    }
    for (ValueChunkWriter writer : valueChunkWriterMap.values()) {
      if (writer.checkPageSizeAndMayOpenANewPage()) {
        return true;
      }
    }
    return false;
  }

  private void writePageToPageBuffer() {
    timeChunkWriter.writePageToPageBuffer();
    for (ValueChunkWriter valueChunkWriter : valueChunkWriterMap.values()) {
      valueChunkWriter.writePageToPageBuffer();
    }
  }

  private void sealAllChunks() {
    timeChunkWriter.sealCurrentPage();
    for (ValueChunkWriter valueChunkWriter : valueChunkWriterMap.values()) {
      valueChunkWriter.sealCurrentPage();
    }
  }

  private void checkIsHistoryData(long time) throws WriteProcessException {
    if (isInitLastTime && time <= lastTime) {
      throw new WriteProcessException(
          "Not allowed to write out-of-order data in timeseries "
              + deviceId
              + TsFileConstant.PATH_SEPARATOR
              + ""
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
    if (lastTime != null) {
      this.lastTime = lastTime;
      isInitLastTime = true;
    }
  }
}
