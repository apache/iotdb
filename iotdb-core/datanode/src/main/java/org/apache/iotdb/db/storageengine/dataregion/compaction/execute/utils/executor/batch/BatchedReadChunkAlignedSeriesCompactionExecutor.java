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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.batch;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.BatchCompactionCannotAlignedException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.ModifiedStatus;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.batch.utils.AlignedSeriesBatchCompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.batch.utils.BatchCompactionPlan;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.batch.utils.CompactChunkPlan;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.batch.utils.CompactionAlignedPageLazyLoadPointReader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.batch.utils.FirstBatchCompactionAlignedChunkWriter;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.batch.utils.FollowingBatchCompactionAlignedChunkWriter;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.readchunk.ReadChunkAlignedSeriesCompactionExecutor;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.readchunk.loader.ChunkLoader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.readchunk.loader.InstantChunkLoader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.readchunk.loader.PageLoader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.io.CompactionTsFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.exception.write.PageException;
import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.reader.IPointReader;
import org.apache.tsfile.read.reader.page.TimePageReader;
import org.apache.tsfile.read.reader.page.ValuePageReader;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class BatchedReadChunkAlignedSeriesCompactionExecutor
    extends ReadChunkAlignedSeriesCompactionExecutor {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
  private final BatchCompactionPlan batchCompactionPlan = new BatchCompactionPlan();
  private final int batchSize =
      IoTDBDescriptor.getInstance().getConfig().getCompactionMaxAlignedSeriesNumInOneBatch();
  private final AlignedSeriesBatchCompactionUtils.BatchColumnSelection batchColumnSelection;
  private final LinkedList<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>>
      originReaderAndChunkMetadataList;

  public BatchedReadChunkAlignedSeriesCompactionExecutor(
      IDeviceID device,
      TsFileResource targetResource,
      LinkedList<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>> readerAndChunkMetadataList,
      CompactionTsFileWriter writer,
      CompactionTaskSummary summary,
      boolean ignoreAllNullRows)
      throws IOException {
    super(device, targetResource, readerAndChunkMetadataList, writer, summary, ignoreAllNullRows);
    this.originReaderAndChunkMetadataList = readerAndChunkMetadataList;
    this.batchColumnSelection =
        new AlignedSeriesBatchCompactionUtils.BatchColumnSelection(schemaList, batchSize);
  }

  @Override
  public void execute() throws IOException, PageException {
    if (batchSize <= 0 || batchSize >= schemaList.size()) {
      super.execute();
      return;
    }
    AlignedSeriesBatchCompactionUtils.markAlignedChunkHasDeletion(readerAndChunkMetadataList);
    compactFirstBatch();
    if (batchCompactionPlan.isEmpty()) {
      return;
    }
    compactLeftBatches();
  }

  private void compactFirstBatch() throws IOException, PageException {
    List<Integer> selectedColumnIndexList;
    List<IMeasurementSchema> selectedColumnSchemaList;
    if (!batchColumnSelection.hasNext()) {
      if (ignoreAllNullRows) {
        return;
      }
      selectedColumnIndexList = Collections.emptyList();
      selectedColumnSchemaList = Collections.emptyList();
    } else {
      batchColumnSelection.next();
      selectedColumnIndexList = batchColumnSelection.getSelectedColumnIndexList();
      selectedColumnSchemaList = batchColumnSelection.getCurrentSelectedColumnSchemaList();
    }

    LinkedList<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>>
        batchedReaderAndChunkMetadataList =
            filterAlignedChunkMetadataList(readerAndChunkMetadataList, selectedColumnIndexList);

    FirstBatchedReadChunkAlignedSeriesCompactionExecutor executor =
        new FirstBatchedReadChunkAlignedSeriesCompactionExecutor(
            device,
            targetResource,
            batchedReaderAndChunkMetadataList,
            writer,
            summary,
            timeSchema,
            selectedColumnSchemaList,
            ignoreAllNullRows);
    executor.execute();
    LOGGER.debug(
        "[Batch Compaction] current device is {}, first batch compacted time chunk is {}",
        device,
        batchCompactionPlan);
  }

  private void compactLeftBatches() throws PageException, IOException {
    while (batchColumnSelection.hasNext()) {
      batchColumnSelection.next();
      LinkedList<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>>
          groupReaderAndChunkMetadataList =
              filterAlignedChunkMetadataList(
                  readerAndChunkMetadataList, batchColumnSelection.getSelectedColumnIndexList());
      FollowingBatchedReadChunkAlignedSeriesGroupCompactionExecutor executor =
          new FollowingBatchedReadChunkAlignedSeriesGroupCompactionExecutor(
              device,
              targetResource,
              groupReaderAndChunkMetadataList,
              writer,
              summary,
              timeSchema,
              batchColumnSelection.getCurrentSelectedColumnSchemaList(),
              ignoreAllNullRows);
      executor.execute();
    }
  }

  private LinkedList<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>>
      filterAlignedChunkMetadataList(
          List<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>> readerAndChunkMetadataList,
          List<Integer> selectedMeasurementIndexs) {
    LinkedList<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>>
        groupReaderAndChunkMetadataList = new LinkedList<>();
    for (Pair<TsFileSequenceReader, List<AlignedChunkMetadata>> pair : readerAndChunkMetadataList) {
      List<AlignedChunkMetadata> alignedChunkMetadataList = pair.getRight();
      List<AlignedChunkMetadata> selectedColumnAlignedChunkMetadataList = new LinkedList<>();
      for (AlignedChunkMetadata alignedChunkMetadata : alignedChunkMetadataList) {
        selectedColumnAlignedChunkMetadataList.add(
            AlignedSeriesBatchCompactionUtils.filterAlignedChunkMetadataByIndex(
                alignedChunkMetadata, selectedMeasurementIndexs));
      }
      groupReaderAndChunkMetadataList.add(
          new Pair<>(pair.getLeft(), selectedColumnAlignedChunkMetadataList));
    }
    return groupReaderAndChunkMetadataList;
  }

  public class FirstBatchedReadChunkAlignedSeriesCompactionExecutor
      extends ReadChunkAlignedSeriesCompactionExecutor {

    public FirstBatchedReadChunkAlignedSeriesCompactionExecutor(
        IDeviceID device,
        TsFileResource targetResource,
        LinkedList<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>>
            readerAndChunkMetadataList,
        CompactionTsFileWriter writer,
        CompactionTaskSummary summary,
        IMeasurementSchema timeSchema,
        List<IMeasurementSchema> valueSchemaList,
        boolean ignoreAllNullRows) {
      super(
          device,
          targetResource,
          readerAndChunkMetadataList,
          writer,
          summary,
          timeSchema,
          valueSchemaList,
          ignoreAllNullRows);
      int compactionFileLevel =
          Integer.parseInt(this.targetResource.getTsFile().getName().split("-")[2]);
      this.flushController =
          new FirstBatchReadChunkAlignedSeriesCompactionFlushController(compactionFileLevel);
    }

    @Override
    protected AlignedChunkWriterImpl constructAlignedChunkWriter() {
      return new FirstBatchCompactionAlignedChunkWriter(timeSchema, schemaList);
    }

    @Override
    protected void compactAlignedChunkByFlush(ChunkLoader timeChunk, List<ChunkLoader> valueChunks)
        throws IOException {
      ChunkMetadata timeChunkMetadata = timeChunk.getChunkMetadata();
      batchCompactionPlan.recordCompactedChunk(
          new CompactChunkPlan(timeChunkMetadata.getStartTime(), timeChunkMetadata.getEndTime()));
      super.compactAlignedChunkByFlush(timeChunk, valueChunks);
    }

    @Override
    protected boolean isAllValuePageEmpty(PageLoader timePage, List<PageLoader> valuePages) {
      long startTime = timePage.getHeader().getStartTime();
      long endTime = timePage.getHeader().getEndTime();
      String file = timePage.getFile();
      ChunkMetadata timeChunkMetadata = timePage.getChunkMetadata();

      List<AlignedChunkMetadata> alignedChunkMetadataList = Collections.emptyList();
      for (Pair<TsFileSequenceReader, List<AlignedChunkMetadata>> pair :
          originReaderAndChunkMetadataList) {
        TsFileSequenceReader reader = pair.getLeft();
        if (reader.getFileName().equals(file)) {
          alignedChunkMetadataList = pair.getRight();
          break;
        }
      }

      AlignedChunkMetadata originAlignedChunkMetadata = null;
      for (AlignedChunkMetadata alignedChunkMetadata : alignedChunkMetadataList) {
        if (alignedChunkMetadata.getOffsetOfChunkHeader()
            == timeChunkMetadata.getOffsetOfChunkHeader()) {
          originAlignedChunkMetadata = alignedChunkMetadata;
          break;
        }
      }

      ModifiedStatus modifiedStatus =
          AlignedSeriesBatchCompactionUtils.calculateAlignedPageModifiedStatus(
              startTime, endTime, originAlignedChunkMetadata, ignoreAllNullRows);
      batchCompactionPlan.recordPageModifiedStatus(
          file, new TimeRange(startTime, endTime), modifiedStatus);
      return modifiedStatus == ModifiedStatus.ALL_DELETED;
    }

    @Override
    protected ChunkLoader getChunkLoader(TsFileSequenceReader reader, ChunkMetadata chunkMetadata)
        throws IOException {
      ChunkLoader chunkLoader = super.getChunkLoader(reader, chunkMetadata);
      if (!chunkLoader.isEmpty() && AlignedSeriesBatchCompactionUtils.isTimeChunk(chunkMetadata)) {
        batchCompactionPlan.addTimeChunkToCache(
            reader.getFileName(), chunkMetadata.getOffsetOfChunkHeader(), chunkLoader.getChunk());
      }
      return chunkLoader;
    }

    @Override
    protected void flushCurrentChunkWriter() throws IOException {
      chunkWriter.sealCurrentPage();
      if (!chunkWriter.isEmpty()) {
        CompactChunkPlan compactChunkPlan =
            ((FirstBatchCompactionAlignedChunkWriter) chunkWriter).getCompactedChunkRecord();
        batchCompactionPlan.recordCompactedChunk(compactChunkPlan);
      }
      writer.writeChunk(chunkWriter);
    }

    @Override
    protected IPointReader getPointReader(
        TimePageReader timePageReader, List<ValuePageReader> valuePageReaders) throws IOException {
      // we have to set ignoreAllNullRows=false because we can not get the entire row here
      return new CompactionAlignedPageLazyLoadPointReader(timePageReader, valuePageReaders, false);
    }

    private class FirstBatchReadChunkAlignedSeriesCompactionFlushController
        extends ReadChunkAlignedSeriesCompactionFlushController {

      public FirstBatchReadChunkAlignedSeriesCompactionFlushController(int compactionFileLevel) {
        super(compactionFileLevel);
      }

      @Override
      public boolean canCompactCurrentChunkByDirectlyFlush(
          ChunkLoader timeChunk, List<ChunkLoader> valueChunks) throws IOException {
        return !timeChunk.getChunkMetadata().isModified()
            && super.canCompactCurrentChunkByDirectlyFlush(timeChunk, valueChunks);
      }
    }
  }

  public class FollowingBatchedReadChunkAlignedSeriesGroupCompactionExecutor
      extends ReadChunkAlignedSeriesCompactionExecutor {
    private int currentCompactChunk = 0;

    public FollowingBatchedReadChunkAlignedSeriesGroupCompactionExecutor(
        IDeviceID device,
        TsFileResource targetResource,
        LinkedList<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>>
            readerAndChunkMetadataList,
        CompactionTsFileWriter writer,
        CompactionTaskSummary summary,
        IMeasurementSchema timeSchema,
        List<IMeasurementSchema> valueSchemaList,
        boolean ignoreAllNullRows) {
      super(
          device,
          targetResource,
          readerAndChunkMetadataList,
          writer,
          summary,
          timeSchema,
          valueSchemaList,
          ignoreAllNullRows);
      this.flushController = new FollowingBatchReadChunkAlignedSeriesCompactionFlushController();
      this.chunkWriter =
          new FollowingBatchCompactionAlignedChunkWriter(
              timeSchema, schemaList, batchCompactionPlan.getCompactChunkPlan(0));
    }

    @Override
    protected ChunkLoader getChunkLoader(TsFileSequenceReader reader, ChunkMetadata chunkMetadata)
        throws IOException {
      if (chunkMetadata != null && AlignedSeriesBatchCompactionUtils.isTimeChunk(chunkMetadata)) {
        Chunk timeChunk = batchCompactionPlan.getTimeChunkFromCache(reader, chunkMetadata);
        return new InstantChunkLoader(reader.getFileName(), chunkMetadata, timeChunk);
      }
      return super.getChunkLoader(reader, chunkMetadata);
    }

    @Override
    protected void flushCurrentChunkWriter() throws IOException {
      if (chunkWriter.isEmpty() || currentCompactChunk >= batchCompactionPlan.compactedChunkNum()) {
        return;
      }
      super.flushCurrentChunkWriter();
      nextChunk();
    }

    private void nextChunk() {
      currentCompactChunk++;
      if (currentCompactChunk < batchCompactionPlan.compactedChunkNum()) {
        CompactChunkPlan chunkRecord = batchCompactionPlan.getCompactChunkPlan(currentCompactChunk);
        ((FollowingBatchCompactionAlignedChunkWriter) this.chunkWriter)
            .setCompactChunkPlan(chunkRecord);
      }
    }

    @Override
    protected boolean isAllValuePageEmpty(PageLoader timePage, List<PageLoader> valuePages) {
      long startTime = timePage.getHeader().getStartTime();
      long endTime = timePage.getHeader().getEndTime();
      String file = timePage.getFile();
      return batchCompactionPlan.getAlignedPageModifiedStatus(
              file, new TimeRange(startTime, endTime))
          == ModifiedStatus.ALL_DELETED;
    }

    @Override
    protected void compactAlignedChunkByFlush(ChunkLoader timeChunk, List<ChunkLoader> valueChunks)
        throws IOException {
      writer.markStartingWritingAligned();
      checkAndUpdatePreviousTimestamp(timeChunk.getChunkMetadata().getStartTime());
      checkAndUpdatePreviousTimestamp(timeChunk.getChunkMetadata().getEndTime());
      // skip time chunk
      timeChunk.clear();
      int nonEmptyChunkNum = 0;
      for (int i = 0; i < valueChunks.size(); i++) {
        ChunkLoader valueChunk = valueChunks.get(i);
        if (valueChunk.isEmpty()) {
          IMeasurementSchema schema = schemaList.get(i);
          writer.writeEmptyValueChunk(
              schema.getMeasurementName(),
              schema.getCompressor(),
              schema.getType(),
              schema.getEncodingType(),
              Statistics.getStatsByType(schema.getType()));
          continue;
        }
        nonEmptyChunkNum++;
        writer.writeChunk(valueChunk.getChunk(), valueChunk.getChunkMetadata());
        valueChunk.clear();
      }
      summary.increaseDirectlyFlushChunkNum(nonEmptyChunkNum);
      writer.markEndingWritingAligned();

      nextChunk();
    }

    @Override
    protected IPointReader getPointReader(
        TimePageReader timePageReader, List<ValuePageReader> valuePageReaders) throws IOException {
      // we have to set ignoreAllNullRows=false because we can not get the entire row here
      return new CompactionAlignedPageLazyLoadPointReader(timePageReader, valuePageReaders, false);
    }

    private class FollowingBatchReadChunkAlignedSeriesCompactionFlushController
        extends ReadChunkAlignedSeriesCompactionFlushController {

      public FollowingBatchReadChunkAlignedSeriesCompactionFlushController() {
        super(0);
      }

      @Override
      public boolean canCompactCurrentChunkByDirectlyFlush(
          ChunkLoader timeChunk, List<ChunkLoader> valueChunks) throws IOException {
        CompactChunkPlan compactChunkPlan =
            batchCompactionPlan.getCompactChunkPlan(currentCompactChunk);
        boolean isCurrentChunkCompactedByDirectlyFlush =
            compactChunkPlan.isCompactedByDirectlyFlush();
        if (isCurrentChunkCompactedByDirectlyFlush
            && timeChunk.getChunkMetadata().getStartTime()
                != compactChunkPlan.getTimeRange().getMin()) {
          throw new BatchCompactionCannotAlignedException(
              timeChunk.getChunkMetadata(), compactChunkPlan, batchCompactionPlan);
        }
        return isCurrentChunkCompactedByDirectlyFlush;
      }

      @Override
      protected boolean canFlushCurrentChunkWriter() {
        // the parameters are not used in this implementation
        return chunkWriter.checkIsChunkSizeOverThreshold(0, 0, true);
      }

      @Override
      protected boolean canCompactCurrentPageByDirectlyFlush(
          PageLoader timePage, List<PageLoader> valuePages) {
        int currentPage =
            ((FollowingBatchCompactionAlignedChunkWriter) chunkWriter).getCurrentPage();
        for (int i = 0; i < valuePages.size(); i++) {
          PageLoader currentValuePage = valuePages.get(i);
          if (currentValuePage.isEmpty()) {
            continue;
          }
          if (currentValuePage.getCompressionType() != schemaList.get(i).getCompressor()
              || currentValuePage.getEncoding() != schemaList.get(i).getEncodingType()) {
            return false;
          }
          if (currentValuePage.getModifiedStatus() == ModifiedStatus.PARTIAL_DELETED) {
            return false;
          }
        }
        return batchCompactionPlan
            .getCompactChunkPlan(currentCompactChunk)
            .getPageRecords()
            .get(currentPage)
            .isCompactedByDirectlyFlush();
      }
    }
  }
}
