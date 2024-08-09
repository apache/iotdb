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

import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.BatchCompactionCannotAlignedException;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.PageException;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.statistics.TimeStatistics;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.chunk.TimeChunkWriter;
import org.apache.tsfile.write.chunk.ValueChunkWriter;
import org.apache.tsfile.write.page.TimePageWriter;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class FollowingBatchCompactionAlignedChunkWriter extends AlignedChunkWriterImpl {
  private int currentPage = 0;
  private CompactChunkPlan compactChunkPlan;
  private ChunkWriterFlushCallback afterChunkWriterFlushCallback;

  public FollowingBatchCompactionAlignedChunkWriter(
      IMeasurementSchema timeSchema,
      List<IMeasurementSchema> valueSchemaList,
      CompactChunkPlan compactChunkPlan) {
    timeChunkWriter = new FollowingBatchCompactionTimeChunkWriter();

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
    this.compactChunkPlan = compactChunkPlan;
  }

  @Override
  protected boolean checkPageSizeAndMayOpenANewPage() {
    long endTime =
        ((FollowingBatchCompactionTimeChunkWriter) timeChunkWriter).chunkStatistics.getEndTime();
    return endTime == compactChunkPlan.getPageRecords().get(currentPage).getTimeRange().getMax();
  }

  @Override
  public void sealCurrentPage() {
    writePageToPageBuffer();
  }

  @Override
  protected void writePageToPageBuffer() {
    FollowingBatchCompactionTimeChunkWriter followingBatchCompactionTimeChunkWriter =
        (FollowingBatchCompactionTimeChunkWriter) timeChunkWriter;
    TimeStatistics pageStatistics = followingBatchCompactionTimeChunkWriter.pageStatistics;
    if (pageStatistics.isEmpty()) {
      return;
    }
    CompactPagePlan alignedTimePage = compactChunkPlan.getPageRecords().get(currentPage);
    if (alignedTimePage.getTimeRange().getMax() != pageStatistics.getEndTime()) {
      throw new BatchCompactionCannotAlignedException(
          pageStatistics, compactChunkPlan, currentPage);
    }
    super.writePageToPageBuffer();
    currentPage++;
  }

  @Override
  public void writePageHeaderAndDataIntoTimeBuff(ByteBuffer data, PageHeader header)
      throws PageException {
    if (currentPage >= compactChunkPlan.getPageRecords().size()
        || header.getStatistics().getStartTime()
            != compactChunkPlan.getPageRecords().get(currentPage).getTimeRange().getMin()) {
      throw new BatchCompactionCannotAlignedException(header, compactChunkPlan, currentPage);
    }
    super.writePageHeaderAndDataIntoTimeBuff(data, header);
    currentPage++;
  }

  @Override
  public void writeToFileWriter(TsFileIOWriter tsfileWriter) throws IOException {
    if (isEmpty()) {
      return;
    }
    for (ValueChunkWriter valueChunkWriter : valueChunkWriterList) {
      valueChunkWriter.writeToFileWriter(tsfileWriter);
    }
    if (afterChunkWriterFlushCallback != null) {
      afterChunkWriterFlushCallback.call(this);
    }
  }

  @Override
  public boolean checkIsChunkSizeOverThreshold(
      long size, long pointNum, boolean returnTrueIfChunkEmpty) {
    if (compactChunkPlan.isCompactedByDirectlyFlush()) {
      return true;
    }
    return currentPage >= compactChunkPlan.getPageRecords().size()
        || ((FollowingBatchCompactionTimeChunkWriter) timeChunkWriter).chunkStatistics.getEndTime()
            == compactChunkPlan.getTimeRange().getMax();
  }

  @Override
  public boolean isEmpty() {
    return timeChunkWriter.getPointNum() == 0;
  }

  @Override
  public boolean checkIsUnsealedPageOverThreshold(
      long size, long pointNum, boolean returnTrueIfPageEmpty) {
    if (currentPage >= compactChunkPlan.getPageRecords().size()) {
      return true;
    }
    CompactPagePlan compactPagePlan = compactChunkPlan.getPageRecords().get(currentPage);
    if (compactPagePlan.isCompactedByDirectlyFlush()) {
      return true;
    }
    long endTime =
        ((FollowingBatchCompactionTimeChunkWriter) timeChunkWriter).pageStatistics.getEndTime();
    return endTime == compactChunkPlan.getPageRecords().get(currentPage).getTimeRange().getMax();
  }

  public int getCurrentPage() {
    return currentPage;
  }

  public void setCompactChunkPlan(CompactChunkPlan compactChunkPlan) {
    this.compactChunkPlan = compactChunkPlan;
    this.currentPage = 0;
    this.timeChunkWriter = new FollowingBatchCompactionTimeChunkWriter();
  }

  public void registerAfterFlushChunkWriterCallback(
      ChunkWriterFlushCallback flushChunkWriterCallback) {
    this.afterChunkWriterFlushCallback = flushChunkWriterCallback;
  }

  public static class FollowingBatchCompactionTimeChunkWriter extends TimeChunkWriter {
    private TimeStatistics chunkStatistics;
    private TimeStatistics pageStatistics;

    public FollowingBatchCompactionTimeChunkWriter() {
      chunkStatistics = new TimeStatistics();
      pageStatistics = new TimeStatistics();
    }

    @Override
    public void write(long time) {
      chunkStatistics.update(time);
      pageStatistics.update(time);
    }

    @Override
    public void write(long[] timestamps, int batchSize, int arrayOffset) {
      throw new RuntimeException("unimplemented");
    }

    @Override
    public boolean checkPageSizeAndMayOpenANewPage() {
      throw new RuntimeException("unimplemented");
    }

    @Override
    public long getRemainingPointNumberForCurrentPage() {
      throw new RuntimeException("unimplemented");
    }

    @Override
    public void writePageToPageBuffer() {
      pageStatistics = new TimeStatistics();
    }

    @Override
    public void writePageHeaderAndDataIntoBuff(ByteBuffer data, PageHeader header)
        throws PageException {
      if (data == null || header.getStatistics() == null || header.getStatistics().isEmpty()) {
        return;
      }
      chunkStatistics.mergeStatistics(header.getStatistics());
    }

    @Override
    public void writeToFileWriter(TsFileIOWriter tsfileWriter) throws IOException {
      chunkStatistics = new TimeStatistics();
      pageStatistics = new TimeStatistics();
    }

    @Override
    public long getCurrentChunkSize() {
      throw new RuntimeException("unimplemented");
    }

    @Override
    public void sealCurrentPage() {
      pageStatistics = new TimeStatistics();
    }

    @Override
    public void clearPageWriter() {
      pageStatistics = new TimeStatistics();
    }

    @Override
    public TSDataType getDataType() {
      return super.getDataType();
    }

    @Override
    public long getPointNum() {
      return chunkStatistics == null ? 0 : chunkStatistics.getCount();
    }

    @Override
    public void writeAllPagesOfChunkToTsFile(TsFileIOWriter writer) throws IOException {
      this.chunkStatistics = new TimeStatistics();
      this.pageStatistics = new TimeStatistics();
    }

    @Override
    public PublicBAOS getPageBuffer() {
      throw new RuntimeException("unimplemented");
    }

    @Override
    public TimePageWriter getPageWriter() {
      throw new RuntimeException("unimplemented");
    }

    @Override
    public boolean checkIsUnsealedPageOverThreshold(long size, long pointNum) {
      throw new RuntimeException("unimplemented");
    }

    public TimeStatistics getChunkStatistics() {
      return chunkStatistics;
    }
  }
}
