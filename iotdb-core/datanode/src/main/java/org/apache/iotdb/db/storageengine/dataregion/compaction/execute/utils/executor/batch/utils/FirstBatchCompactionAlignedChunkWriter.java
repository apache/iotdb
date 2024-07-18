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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.batch.utils;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.encoding.encoder.Encoder;
import org.apache.tsfile.encoding.encoder.TSEncodingBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.PageException;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.TimeStatistics;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.chunk.TimeChunkWriter;
import org.apache.tsfile.write.chunk.ValueChunkWriter;
import org.apache.tsfile.write.page.TimePageWriter;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.VectorMeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class FirstBatchCompactionAlignedChunkWriter extends AlignedChunkWriterImpl {

  private ChunkWriterFlushCallback beforeChunkWriterFlushCallback;

  public FirstBatchCompactionAlignedChunkWriter(VectorMeasurementSchema schema) {
    timeChunkWriter =
        new FirstBatchCompactionTimeChunkWriter(
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
    this.remainingPointsNumber = timeChunkWriter.getRemainingPointNumberForCurrentPage();
  }

  public FirstBatchCompactionAlignedChunkWriter(
      IMeasurementSchema timeSchema, List<IMeasurementSchema> valueSchemaList) {
    timeChunkWriter =
        new FirstBatchCompactionTimeChunkWriter(
            timeSchema.getMeasurementId(),
            timeSchema.getCompressor(),
            timeSchema.getEncodingType(),
            timeSchema.getTimeEncoder());

    valueChunkWriterList = new ArrayList<>(valueSchemaList.size());
    for (int i = 0; i < valueSchemaList.size(); i++) {
      valueChunkWriterList.add(
          new ValueChunkWriter(
              valueSchemaList.get(i).getMeasurementId(),
              valueSchemaList.get(i).getCompressor(),
              valueSchemaList.get(i).getType(),
              valueSchemaList.get(i).getEncodingType(),
              valueSchemaList.get(i).getValueEncoder()));
    }

    this.valueIndex = 0;
    this.remainingPointsNumber = timeChunkWriter.getRemainingPointNumberForCurrentPage();
  }

  public FirstBatchCompactionAlignedChunkWriter(List<IMeasurementSchema> schemaList) {
    TSEncoding timeEncoding =
        TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder());
    TSDataType timeType = TSFileDescriptor.getInstance().getConfig().getTimeSeriesDataType();
    CompressionType timeCompression = TSFileDescriptor.getInstance().getConfig().getCompressor();
    timeChunkWriter =
        new FirstBatchCompactionTimeChunkWriter(
            "",
            timeCompression,
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
    this.remainingPointsNumber = timeChunkWriter.getRemainingPointNumberForCurrentPage();
  }

  @Override
  public void writeToFileWriter(TsFileIOWriter tsfileWriter) throws IOException {
    if (!isEmpty() && beforeChunkWriterFlushCallback != null) {
      // make sure all pages are recorded before this call
      sealCurrentPage();
      beforeChunkWriterFlushCallback.call(this);
    }
    super.writeToFileWriter(tsfileWriter);
  }

  public void registerBeforeFlushChunkWriterCallback(
      ChunkWriterFlushCallback flushChunkWriterCallback) {
    this.beforeChunkWriterFlushCallback = flushChunkWriterCallback;
  }

  public CompactChunkPlan getCompactedChunkRecord() {
    return new CompactChunkPlan(
        ((FirstBatchCompactionTimeChunkWriter) this.getTimeChunkWriter()).getPageTimeRanges());
  }

  public static class FirstBatchCompactionTimeChunkWriter extends TimeChunkWriter {

    private List<CompactPagePlan> pageTimeRanges = new ArrayList<>();

    public FirstBatchCompactionTimeChunkWriter(
        String measurementId,
        CompressionType compressionType,
        TSEncoding encodingType,
        Encoder timeEncoder) {
      super(measurementId, compressionType, encodingType, timeEncoder);
    }

    @Override
    public void writePageToPageBuffer() {
      TimePageWriter pageWriter = getPageWriter();
      if (pageWriter != null && pageWriter.getPointNumber() > 0) {
        TimeStatistics statistics = pageWriter.getStatistics();
        pageTimeRanges.add(
            new CompactPagePlan(statistics.getStartTime(), statistics.getEndTime(), false));
        super.writePageToPageBuffer();
      }
    }

    @Override
    public void writeToFileWriter(TsFileIOWriter tsfileWriter) throws IOException {
      super.writeToFileWriter(tsfileWriter);
      this.pageTimeRanges = new ArrayList<>();
    }

    @Override
    public void writePageHeaderAndDataIntoBuff(ByteBuffer data, PageHeader header)
        throws PageException {
      pageTimeRanges.add(new CompactPagePlan(header.getStartTime(), header.getEndTime(), true));
      super.writePageHeaderAndDataIntoBuff(data, header);
    }

    public List<CompactPagePlan> getPageTimeRanges() {
      return this.pageTimeRanges;
    }
  }
}
