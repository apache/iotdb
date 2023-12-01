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

package org.apache.iotdb.db.storageengine.dataregion.compaction.io;

import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.readchunk.loader.LazyPageLoader;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.encoding.encoder.TSEncodingBuilder;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

import java.util.ArrayList;
import java.util.List;

public class LazyAlignedChunkWriterImpl extends AlignedChunkWriterImpl {

  // TestOnly
  public LazyAlignedChunkWriterImpl(VectorMeasurementSchema schema) {
    timeChunkWriter =
        new LazyTimeChunkWriter(
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
          new LazyValueChunkWriter(
              valueMeasurementIdList.get(i),
              schema.getCompressor(),
              valueTSDataTypeList.get(i),
              valueTSEncodingList.get(i),
              valueEncoderList.get(i)));
    }

    this.valueIndex = 0;
    this.remainingPointsNumber = timeChunkWriter.getRemainingPointNumberForCurrentPage();
  }

  /**
   * This is used to rewrite file. The encoding and compression of the time column should be the
   * same as the source file.
   *
   * @param timeSchema time schema
   * @param valueSchemaList value schema list
   */
  public LazyAlignedChunkWriterImpl(
      IMeasurementSchema timeSchema, List<IMeasurementSchema> valueSchemaList) {
    timeChunkWriter =
        new LazyTimeChunkWriter(
            timeSchema.getMeasurementId(),
            timeSchema.getCompressor(),
            timeSchema.getEncodingType(),
            timeSchema.getTimeEncoder());

    valueChunkWriterList = new ArrayList<>(valueSchemaList.size());
    for (int i = 0; i < valueSchemaList.size(); i++) {
      valueChunkWriterList.add(
          new LazyValueChunkWriter(
              valueSchemaList.get(i).getMeasurementId(),
              valueSchemaList.get(i).getCompressor(),
              valueSchemaList.get(i).getType(),
              valueSchemaList.get(i).getEncodingType(),
              valueSchemaList.get(i).getValueEncoder()));
    }

    this.valueIndex = 0;
    this.remainingPointsNumber = timeChunkWriter.getRemainingPointNumberForCurrentPage();
  }

  /**
   * This is used to write 0-level file. The compression of the time column is 'LZ4' in the
   * configuration by default. The encoding of the time column is 'TS_2DIFF' in the configuration by
   * default.
   *
   * @param schemaList value schema list
   */
  public LazyAlignedChunkWriterImpl(List<IMeasurementSchema> schemaList) {
    TSEncoding timeEncoding =
        TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder());
    TSDataType timeType = TSFileDescriptor.getInstance().getConfig().getTimeSeriesDataType();
    CompressionType timeCompression = TSFileDescriptor.getInstance().getConfig().getCompressor();
    timeChunkWriter =
        new LazyTimeChunkWriter(
            "",
            timeCompression,
            timeEncoding,
            TSEncodingBuilder.getEncodingBuilder(timeEncoding).getEncoder(timeType));

    valueChunkWriterList = new ArrayList<>(schemaList.size());
    for (int i = 0; i < schemaList.size(); i++) {
      valueChunkWriterList.add(
          new LazyValueChunkWriter(
              schemaList.get(i).getMeasurementId(),
              schemaList.get(i).getCompressor(),
              schemaList.get(i).getType(),
              schemaList.get(i).getEncodingType(),
              schemaList.get(i).getValueEncoder()));
    }

    this.valueIndex = 0;
    this.remainingPointsNumber = timeChunkWriter.getRemainingPointNumberForCurrentPage();
  }

  public void writePageLoaderIntoTimeBuff(LazyPageLoader lazyPageLoader) throws PageException {
    ((LazyTimeChunkWriter) timeChunkWriter).writeLazyPageLoaderIntoBuff(lazyPageLoader);
  }

  public void writePageLoaderIntoValueBuff(LazyPageLoader lazyPageLoader, int valueIndex)
      throws PageException {
    LazyValueChunkWriter writer = (LazyValueChunkWriter) valueChunkWriterList.get(valueIndex);
    writer.writeLazyPageLoaderIntoBuff(lazyPageLoader);
  }
}
