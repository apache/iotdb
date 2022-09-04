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
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.encoding.encoder.TSEncodingBuilder;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AlignedChunkWriterImpl implements IChunkWriter {

  private final TimeChunkWriter timeChunkWriter;
  private final List<ValueChunkWriter> valueChunkWriterList;
  private int valueIndex;

  /**
   * @param schema schema of this measurement
   */
  public AlignedChunkWriterImpl(VectorMeasurementSchema schema) {
    timeChunkWriter =
        new TimeChunkWriter(
            schema.getMeasurementId(),
            schema.getCompressor(),
            schema.getTimeTSEncoding(),
            schema.getTimeEncoder());

    List<String> valueMeasurementIdList = schema.getSubMeasurementsList();
    List<TSDataType> valueTSDataTypeList = schema.getSubMeasurementsTSDataTypeList();
    List<TSEncoding> valueTSEncodingList = schema.getSubMeasurementsTSEncodingList();
    List<Encoder> valueEncoderList = schema.getSubMeasurementsEncoderList();

    valueChunkWriterList = new ArrayList<>(valueMeasurementIdList.size());
    for (int i = 0; i < valueMeasurementIdList.size(); i++) {
      valueChunkWriterList.add(
          new ValueChunkWriter(
              valueMeasurementIdList.get(i),
              schema.getCompressor(),
              valueTSDataTypeList.get(i),
              valueTSEncodingList.get(i),
              valueEncoderList.get(i)));
    }

    this.valueIndex = 0;
  }

  public AlignedChunkWriterImpl(List<IMeasurementSchema> schemaList) {
    TSEncoding timeEncoding =
        TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder());
    TSDataType timeType = TSFileDescriptor.getInstance().getConfig().getTimeSeriesDataType();
    timeChunkWriter =
        new TimeChunkWriter(
            "",
            schemaList.get(0).getCompressor(),
            timeEncoding,
            TSEncodingBuilder.getEncodingBuilder(timeEncoding).getEncoder(timeType));

    valueChunkWriterList = new ArrayList<>(schemaList.size());
    for (int i = 0; i < schemaList.size(); i++) {
      valueChunkWriterList.add(
          new ValueChunkWriter(
              schemaList.get(i).getMeasurementId(),
              schemaList.get(i).getCompressor(),
              schemaList.get(i).getType(),
              schemaList.get(i).getEncodingType(),
              schemaList.get(i).getValueEncoder()));
    }

    this.valueIndex = 0;
  }

  public void write(long time, int value, boolean isNull) {
    valueChunkWriterList.get(valueIndex++).write(time, value, isNull);
  }

  public void write(long time, long value, boolean isNull) {
    valueChunkWriterList.get(valueIndex++).write(time, value, isNull);
  }

  public void write(long time, boolean value, boolean isNull) {
    valueChunkWriterList.get(valueIndex++).write(time, value, isNull);
  }

  public void write(long time, float value, boolean isNull) {
    valueChunkWriterList.get(valueIndex++).write(time, value, isNull);
  }

  public void write(long time, double value, boolean isNull) {
    valueChunkWriterList.get(valueIndex++).write(time, value, isNull);
  }

  public void write(long time, Binary value, boolean isNull) {
    valueChunkWriterList.get(valueIndex++).write(time, value, isNull);
  }

  public void write(long time) {
    valueIndex = 0;
    timeChunkWriter.write(time);
    if (checkPageSizeAndMayOpenANewPage()) {
      writePageToPageBuffer();
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
}
