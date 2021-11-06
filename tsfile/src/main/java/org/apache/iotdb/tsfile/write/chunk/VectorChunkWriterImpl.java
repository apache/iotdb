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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.encoding.encoder.TSEncodingBuilder;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

public class VectorChunkWriterImpl implements IChunkWriter {

  private final TimeChunkWriter timeChunkWriter;

  // measurementId -> ValueChunkWriter
  private List<ValueChunkWriter> valueChunkWriterList;
  private int valueIndex;

  /** @param measurementSchemas schema of these measurements */
  public VectorChunkWriterImpl(List<IMeasurementSchema> measurementSchemas) {
    String measurementId = "";
    CompressionType compressionType = TSFileDescriptor.getInstance().getConfig().getCompressor();
    TSEncoding tsEncoding =
        TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder());
    TSDataType timeType = TSFileDescriptor.getInstance().getConfig().getTimeSeriesDataType();
    Encoder encoder = TSEncodingBuilder.getEncodingBuilder(tsEncoding).getEncoder(timeType);
    timeChunkWriter = new TimeChunkWriter(measurementId, compressionType, tsEncoding, encoder);

    valueChunkWriterList = new ArrayList<>(measurementSchemas.size());
    for (int i = 0; i < measurementSchemas.size(); i++) {
      IMeasurementSchema measurementSchema = measurementSchemas.get(i);
      valueChunkWriterList.add(
          new ValueChunkWriter(
              measurementSchema.getMeasurementId(),
              measurementSchema.getCompressor(),
              measurementSchema.getType(),
              measurementSchema.getEncodingType(),
              measurementSchema.getValueEncoder()));
    }

    this.valueIndex = 0;
  }

  public VectorChunkWriterImpl() {
    String timeMeasurementId = "";
    CompressionType compressionType = TSFileDescriptor.getInstance().getConfig().getCompressor();
    TSEncoding tsEncoding =
        TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder());
    TSDataType timeType = TSFileDescriptor.getInstance().getConfig().getTimeSeriesDataType();
    Encoder encoder = TSEncodingBuilder.getEncodingBuilder(tsEncoding).getEncoder(timeType);
    timeChunkWriter = new TimeChunkWriter(timeMeasurementId, compressionType, tsEncoding, encoder);
    valueChunkWriterList = new ArrayList<>();
    this.valueIndex = 0;
  }

  @Override
  public void addValueChunkWriter(ValueChunkWriter valueChunkWriter) {
    valueChunkWriterList.add(valueChunkWriter);
    addEmptyData(valueChunkWriter);
  }

  public void addValueChunkWriter(IMeasurementSchema schema) {
    ValueChunkWriter valueChunkWriter =
        new ValueChunkWriter(
            schema.getMeasurementId(),
            schema.getCompressor(),
            schema.getType(),
            schema.getEncodingType(),
            schema.getValueEncoder());
    valueChunkWriterList.add(valueChunkWriter);
    addEmptyData(valueChunkWriter);
  }

  public void addEmptyData(ValueChunkWriter valueChunkWriter) {
    System.out.println("----------Vector page num is " + timeChunkWriter.getNumOfPages());
    // initial empty page
    for (int i = 0; i < timeChunkWriter.getNumOfPages(); i++) {
      valueChunkWriter.writeEmptyPageToPageBuffer();
    }

    // initial emptyData of currentPage
    System.out.println(
        "----------Vector current page point num is "
            + timeChunkWriter.getPageWriter().getStatistics().getCount());
    for (long i = 0; i < timeChunkWriter.getPageWriter().getStatistics().getCount(); i++) {
      valueChunkWriter.write(0, 0, true);
    }
  }

  @Override
  public void write(long time, int value, boolean isNull) {
    valueChunkWriterList.get(valueIndex++).write(time, value, isNull);
  }

  @Override
  public void write(long time, long value, boolean isNull) {
    valueChunkWriterList.get(valueIndex++).write(time, value, isNull);
  }

  @Override
  public void write(long time, boolean value, boolean isNull) {
    valueChunkWriterList.get(valueIndex++).write(time, value, isNull);
  }

  @Override
  public void write(long time, float value, boolean isNull) {
    valueChunkWriterList.get(valueIndex++).write(time, value, isNull);
  }

  @Override
  public void write(long time, double value, boolean isNull) {
    valueChunkWriterList.get(valueIndex++).write(time, value, isNull);
  }

  @Override
  public void write(long time, Binary value, boolean isNull) {
    valueChunkWriterList.get(valueIndex++).write(time, value, isNull);
  }

  @Override
  public void write(long time) {
    valueIndex = 0;
    timeChunkWriter.write(time);
    if (checkPageSizeAndMayOpenANewPage()) {
      writePageToPageBuffer();
    }
  }

  // TODO tsfile write interface
  @Override
  public void write(long[] timestamps, int[] values, int batchSize) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void write(long[] timestamps, long[] values, int batchSize) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void write(long[] timestamps, boolean[] values, int batchSize) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void write(long[] timestamps, float[] values, int batchSize) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void write(long[] timestamps, double[] values, int batchSize) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void write(long[] timestamps, Binary[] values, int batchSize) {
    throw new UnsupportedOperationException();
  }

  /**
   * check occupied memory size, if it exceeds the PageSize threshold, construct a page and put it
   * to pageBuffer
   */
  private boolean checkPageSizeAndMayOpenANewPage() {
    if (timeChunkWriter.checkPageSizeAndMayOpenANewPage()) {
      return true;
    }
    for (ValueChunkWriter writer : valueChunkWriterList) {
      if (writer.checkPageSizeAndMayOpenANewPage()) {
        return true;
      }
    }
    return false;
  }

  private void writePageToPageBuffer() {
    timeChunkWriter.writePageToPageBuffer();
    for (ValueChunkWriter valueChunkWriter : valueChunkWriterList) {
      valueChunkWriter.writePageToPageBuffer();
    }
  }

  @Override
  public void writeToFileWriter(TsFileIOWriter tsfileWriter) throws IOException {
    timeChunkWriter.writeToFileWriter(tsfileWriter);
    for (ValueChunkWriter valueChunkWriter : valueChunkWriterList) {
      valueChunkWriter.writeToFileWriter(tsfileWriter);
    }
  }

  @Override
  public long estimateMaxSeriesMemSize() {
    long estimateMaxSeriesMemSize = timeChunkWriter.estimateMaxSeriesMemSize();
    for (ValueChunkWriter valueChunkWriter : valueChunkWriterList) {
      estimateMaxSeriesMemSize += valueChunkWriter.estimateMaxSeriesMemSize();
    }
    return estimateMaxSeriesMemSize;
  }

  public long getSerializedChunkSize() {
    long currentChunkSize = timeChunkWriter.getCurrentChunkSize();
    for (ValueChunkWriter valueChunkWriter : valueChunkWriterList) {
      currentChunkSize += valueChunkWriter.getCurrentChunkSize();
    }
    return currentChunkSize;
  }

  @Override
  public void sealCurrentPage() {
    timeChunkWriter.sealCurrentPage();
    for (ValueChunkWriter valueChunkWriter : valueChunkWriterList) {
      valueChunkWriter.sealCurrentPage();
    }
  }

  @Override
  public void clearPageWriter() {
    timeChunkWriter.clearPageWriter();
    for (ValueChunkWriter valueChunkWriter : valueChunkWriterList) {
      valueChunkWriter.clearPageWriter();
    }
  }

  @Override
  public int getNumOfPages() {
    return timeChunkWriter.getNumOfPages();
  }

  @Override
  public TSDataType getDataType() {
    return TSDataType.VECTOR;
  }

  public void setValueIndex(int valueIndex) {
    this.valueIndex = valueIndex;
  }
}
