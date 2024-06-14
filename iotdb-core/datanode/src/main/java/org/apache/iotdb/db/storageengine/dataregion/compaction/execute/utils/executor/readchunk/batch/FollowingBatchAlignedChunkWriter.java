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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.readchunk.batch;

import org.apache.tsfile.encoding.encoder.Encoder;
import org.apache.tsfile.exception.write.PageException;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.chunk.TimeChunkWriter;
import org.apache.tsfile.write.chunk.ValueChunkWriter;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class FollowingBatchAlignedChunkWriter extends AlignedChunkWriterImpl {
  private int currentPage = 0;
  private CompactChunkPlan compactChunkPlan;

  public FollowingBatchAlignedChunkWriter(
      IMeasurementSchema timeSchema,
      List<IMeasurementSchema> valueSchemaList,
      CompactChunkPlan compactChunkPlan) {
    timeChunkWriter =
        new TimeChunkWriter(
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
    this.compactChunkPlan = compactChunkPlan;
  }

  @Override
  protected boolean checkPageSizeAndMayOpenANewPage() {
    long endTime = timeChunkWriter.getPageWriter().getStatistics().getEndTime();
    return endTime == compactChunkPlan.getPageRecords().get(currentPage).getTimeRange().getMax();
  }

  @Override
  protected void writePageToPageBuffer() {
    super.writePageToPageBuffer();
    currentPage++;
  }

  @Override
  public void writePageHeaderAndDataIntoTimeBuff(ByteBuffer data, PageHeader header)
      throws PageException {
    super.writePageHeaderAndDataIntoTimeBuff(data, header);
    currentPage++;
  }

  @Override
  public void writeToFileWriter(TsFileIOWriter tsfileWriter) throws IOException {
    for (ValueChunkWriter valueChunkWriter : valueChunkWriterList) {
      valueChunkWriter.writeToFileWriter(tsfileWriter);
    }
  }

  @Override
  public boolean checkIsChunkSizeOverThreshold(
      long size, long pointNum, boolean returnTrueIfChunkEmpty) {
    return currentPage >= compactChunkPlan.getPageRecords().size();
  }

  public int getCurrentPage() {
    return currentPage;
  }

  public void setCompactChunkPlan(CompactChunkPlan compactChunkPlan) {
    this.compactChunkPlan = compactChunkPlan;
    this.currentPage = 0;
  }

  public static class FollowingBatchTimeChunkWriter extends TimeChunkWriter {
    private long endTime = Long.MIN_VALUE;

    public FollowingBatchTimeChunkWriter(
        String measurementId,
        CompressionType compressionType,
        TSEncoding encodingType,
        Encoder timeEncoder) {
      super(measurementId, compressionType, encodingType, timeEncoder);
    }

    @Override
    public void write(long time) {
      endTime = time;
    }

    @Override
    public void clearPageWriter() {}

    @Override
    public void sealCurrentPage() {}

    @Override
    public boolean checkPageSizeAndMayOpenANewPage() {
      return super.checkPageSizeAndMayOpenANewPage();
    }
  }
}
