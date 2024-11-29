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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.readchunk;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.CompactionLastTimeCheckFailedException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.ModifiedStatus;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.batch.utils.AlignedSeriesBatchCompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.batch.utils.CompactionAlignedPageLazyLoadPointReader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.readchunk.loader.ChunkLoader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.readchunk.loader.InstantChunkLoader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.readchunk.loader.InstantPageLoader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.readchunk.loader.PageLoader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.io.CompactionTsFileReader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.io.CompactionTsFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.PageException;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.reader.IPointReader;
import org.apache.tsfile.read.reader.page.TimePageReader;
import org.apache.tsfile.read.reader.page.ValuePageReader;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ReadChunkAlignedSeriesCompactionExecutor {

  protected final IDeviceID device;
  protected final LinkedList<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>>
      readerAndChunkMetadataList;
  protected final TsFileResource targetResource;
  protected final CompactionTsFileWriter writer;

  protected AlignedChunkWriterImpl chunkWriter;
  protected IMeasurementSchema timeSchema;
  protected List<IMeasurementSchema> schemaList;
  protected ReadChunkAlignedSeriesCompactionFlushController flushController;
  protected final CompactionTaskSummary summary;
  protected final boolean ignoreAllNullRows;

  private long lastWriteTimestamp = Long.MIN_VALUE;

  public ReadChunkAlignedSeriesCompactionExecutor(
      IDeviceID device,
      TsFileResource targetResource,
      LinkedList<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>> readerAndChunkMetadataList,
      CompactionTsFileWriter writer,
      CompactionTaskSummary summary,
      boolean ignoreAllNullRows)
      throws IOException {
    this.device = device;
    this.readerAndChunkMetadataList = readerAndChunkMetadataList;
    this.writer = writer;
    this.targetResource = targetResource;
    this.summary = summary;
    collectValueColumnSchemaList();
    fillAlignedChunkMetadataToMatchSchemaList();
    int compactionFileLevel =
        Integer.parseInt(this.targetResource.getTsFile().getName().split("-")[2]);
    flushController = new ReadChunkAlignedSeriesCompactionFlushController(compactionFileLevel);
    this.chunkWriter = constructAlignedChunkWriter();
    this.ignoreAllNullRows = ignoreAllNullRows;
  }

  // used for batched column compaction
  public ReadChunkAlignedSeriesCompactionExecutor(
      IDeviceID device,
      TsFileResource targetResource,
      LinkedList<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>> readerAndChunkMetadataList,
      CompactionTsFileWriter writer,
      CompactionTaskSummary summary,
      IMeasurementSchema timeSchema,
      List<IMeasurementSchema> valueSchemaList,
      boolean ignoreAllNullRows) {
    this.device = device;
    this.readerAndChunkMetadataList = readerAndChunkMetadataList;
    this.writer = writer;
    this.targetResource = targetResource;
    this.summary = summary;
    int compactionFileLevel =
        Integer.parseInt(this.targetResource.getTsFile().getName().split("-")[2]);
    flushController = new ReadChunkAlignedSeriesCompactionFlushController(compactionFileLevel);
    this.timeSchema = timeSchema;
    this.schemaList = valueSchemaList;
    this.chunkWriter = constructAlignedChunkWriter();
    this.ignoreAllNullRows = ignoreAllNullRows;
  }

  private void collectValueColumnSchemaList() throws IOException {
    Map<String, IMeasurementSchema> measurementSchemaMap = new HashMap<>();
    for (int i = this.readerAndChunkMetadataList.size() - 1; i >= 0; i--) {
      Pair<TsFileSequenceReader, List<AlignedChunkMetadata>> pair =
          this.readerAndChunkMetadataList.get(i);
      CompactionTsFileReader reader = (CompactionTsFileReader) pair.getLeft();
      List<AlignedChunkMetadata> alignedChunkMetadataList = pair.getRight();
      for (AlignedChunkMetadata alignedChunkMetadata : alignedChunkMetadataList) {
        if (alignedChunkMetadata == null) {
          continue;
        }
        if (timeSchema == null) {
          ChunkMetadata timeChunkMetadata =
              (ChunkMetadata) alignedChunkMetadata.getTimeChunkMetadata();
          ChunkHeader timeChunkHeader =
              reader.readChunkHeader(timeChunkMetadata.getOffsetOfChunkHeader());
          timeSchema =
              new MeasurementSchema(
                  timeChunkHeader.getMeasurementID(),
                  timeChunkHeader.getDataType(),
                  timeChunkHeader.getEncodingType(),
                  timeChunkHeader.getCompressionType());
        }

        for (IChunkMetadata chunkMetadata : alignedChunkMetadata.getValueChunkMetadataList()) {
          if (chunkMetadata == null
              || measurementSchemaMap.containsKey(chunkMetadata.getMeasurementUid())) {
            continue;
          }
          ChunkHeader chunkHeader = reader.readChunkHeader(chunkMetadata.getOffsetOfChunkHeader());
          IMeasurementSchema schema =
              new MeasurementSchema(
                  chunkHeader.getMeasurementID(),
                  chunkHeader.getDataType(),
                  chunkHeader.getEncodingType(),
                  chunkHeader.getCompressionType());
          measurementSchemaMap.put(chunkMetadata.getMeasurementUid(), schema);
        }
      }
    }

    this.schemaList =
        measurementSchemaMap.values().stream()
            .sorted(Comparator.comparing(IMeasurementSchema::getMeasurementName))
            .collect(Collectors.toList());
  }

  private void fillAlignedChunkMetadataToMatchSchemaList() {
    for (Pair<TsFileSequenceReader, List<AlignedChunkMetadata>> pair : readerAndChunkMetadataList) {
      List<AlignedChunkMetadata> alignedChunkMetadataList = pair.getRight();
      for (int i = 0; i < alignedChunkMetadataList.size(); i++) {
        AlignedChunkMetadata alignedChunkMetadata = alignedChunkMetadataList.get(i);
        alignedChunkMetadataList.set(
            i,
            AlignedSeriesBatchCompactionUtils.fillAlignedChunkMetadataBySchemaList(
                alignedChunkMetadata, schemaList));
      }
    }
  }

  protected AlignedChunkWriterImpl constructAlignedChunkWriter() {
    return new AlignedChunkWriterImpl(timeSchema, schemaList);
  }

  public void execute() throws IOException, PageException {
    for (Pair<TsFileSequenceReader, List<AlignedChunkMetadata>> readerListPair :
        readerAndChunkMetadataList) {
      TsFileSequenceReader reader = readerListPair.left;
      List<AlignedChunkMetadata> alignedChunkMetadataList = readerListPair.right;

      if (reader instanceof CompactionTsFileReader) {
        ((CompactionTsFileReader) reader).markStartOfAlignedSeries();
      }
      for (AlignedChunkMetadata alignedChunkMetadata : alignedChunkMetadataList) {
        compactWithAlignedChunk(reader, alignedChunkMetadata);
      }
      if (reader instanceof CompactionTsFileReader) {
        ((CompactionTsFileReader) reader).markEndOfAlignedSeries();
      }
    }

    if (!chunkWriter.isEmpty()) {
      flushCurrentChunkWriter();
    }
  }

  private void compactWithAlignedChunk(
      TsFileSequenceReader reader, AlignedChunkMetadata alignedChunkMetadata)
      throws IOException, PageException {
    ChunkLoader timeChunk =
        getChunkLoader(reader, (ChunkMetadata) alignedChunkMetadata.getTimeChunkMetadata());
    List<ChunkLoader> valueChunks = new ArrayList<>(schemaList.size());
    long pointNum = 0;
    for (int i = 0; i < alignedChunkMetadata.getValueChunkMetadataList().size(); i++) {
      IChunkMetadata chunkMetadata = alignedChunkMetadata.getValueChunkMetadataList().get(i);
      if (chunkMetadata == null
          || !chunkMetadata.getDataType().equals(schemaList.get(i).getType())) {
        valueChunks.add(getChunkLoader(reader, null));
        continue;
      }
      pointNum += chunkMetadata.getStatistics().getCount();
      valueChunks.add(getChunkLoader(reader, (ChunkMetadata) chunkMetadata));
    }
    summary.increaseProcessPointNum(pointNum);
    if (flushController.canFlushCurrentChunkWriter()) {
      flushCurrentChunkWriter();
    }
    if (flushController.canCompactCurrentChunkByDirectlyFlush(timeChunk, valueChunks)) {
      compactAlignedChunkByFlush(timeChunk, valueChunks);
    } else {
      compactAlignedChunkByDeserialize(timeChunk, valueChunks);
    }
  }

  protected ChunkLoader getChunkLoader(TsFileSequenceReader reader, ChunkMetadata chunkMetadata)
      throws IOException {
    if (chunkMetadata == null || chunkMetadata.getStatistics().getCount() == 0) {
      return new InstantChunkLoader();
    }
    Chunk chunk = reader.readMemChunk(chunkMetadata);
    return new InstantChunkLoader(reader.getFileName(), chunkMetadata, chunk);
  }

  protected void flushCurrentChunkWriter() throws IOException {
    chunkWriter.sealCurrentPage();
    writer.writeChunk(chunkWriter);
  }

  protected void compactAlignedChunkByFlush(ChunkLoader timeChunk, List<ChunkLoader> valueChunks)
      throws IOException {
    writer.markStartingWritingAligned();
    checkAndUpdatePreviousTimestamp(timeChunk.getChunkMetadata().getStartTime());
    if (timeChunk.getChunkMetadata().getStartTime() != timeChunk.getChunkMetadata().getEndTime()) {
      checkAndUpdatePreviousTimestamp(timeChunk.getChunkMetadata().getEndTime());
    }
    writer.writeChunk(timeChunk.getChunk(), timeChunk.getChunkMetadata());
    timeChunk.clear();
    int nonEmptyChunkNum = 1;
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
  }

  private void compactAlignedChunkByDeserialize(
      ChunkLoader timeChunk, List<ChunkLoader> valueChunks) throws PageException, IOException {
    List<PageLoader> timeColumnPageList = timeChunk.getPages();
    List<List<PageLoader>> pageListOfAllValueColumns = new ArrayList<>(valueChunks.size());
    int nonEmptyChunkNum = 1;
    for (ChunkLoader valueChunk : valueChunks) {
      if (!valueChunk.isEmpty()) {
        nonEmptyChunkNum++;
      }
      List<PageLoader> valueColumnPageList = valueChunk.getPages();
      pageListOfAllValueColumns.add(valueColumnPageList);
      valueChunk.clear();
    }
    summary.increaseDeserializedChunkNum(nonEmptyChunkNum);

    for (int i = 0; i < timeColumnPageList.size(); i++) {
      PageLoader timePage = timeColumnPageList.get(i);
      List<PageLoader> valuePages = new ArrayList<>(valueChunks.size());
      for (List<PageLoader> pageListOfValueColumn : pageListOfAllValueColumns) {
        valuePages.add(
            pageListOfValueColumn.isEmpty() ? getEmptyPage() : pageListOfValueColumn.get(i));
      }

      // used for tree model
      if (ignoreAllNullRows && isAllValuePageEmpty(timePage, valuePages)) {
        continue;
      }
      // used for table model deletion
      if (!ignoreAllNullRows && timePage.isEmpty()) {
        continue;
      }

      if (flushController.canFlushCurrentChunkWriter()) {
        flushCurrentChunkWriter();
      }
      if (flushController.canCompactCurrentPageByDirectlyFlush(timePage, valuePages)) {
        chunkWriter.sealCurrentPage();
        compactAlignedPageByFlush(timePage, valuePages);
      } else {
        compactAlignedPageByDeserialize(timePage, valuePages);
      }
    }
  }

  protected boolean isAllValuePageEmpty(PageLoader timePage, List<PageLoader> valuePages) {
    for (PageLoader valuePage : valuePages) {
      if (!valuePage.isEmpty()) {
        return false;
      }
    }
    return true;
  }

  private PageLoader getEmptyPage() {
    return new InstantPageLoader();
  }

  protected void compactAlignedPageByFlush(PageLoader timePage, List<PageLoader> valuePageLoaders)
      throws PageException, IOException {
    int nonEmptyPage = 1;
    checkAndUpdatePreviousTimestamp(timePage.getHeader().getStartTime());
    if (timePage.getHeader().getStartTime() != timePage.getHeader().getEndTime()) {
      checkAndUpdatePreviousTimestamp(timePage.getHeader().getEndTime());
    }
    timePage.flushToTimeChunkWriter(chunkWriter);
    for (int i = 0; i < valuePageLoaders.size(); i++) {
      PageLoader valuePage = valuePageLoaders.get(i);
      if (!valuePage.isEmpty()) {
        nonEmptyPage++;
      }
      valuePage.flushToValueChunkWriter(chunkWriter, i);
    }
    summary.increaseDirectlyFlushPageNum(nonEmptyPage);
  }

  private void compactAlignedPageByDeserialize(PageLoader timePage, List<PageLoader> valuePages)
      throws IOException {
    PageHeader timePageHeader = timePage.getHeader();
    ByteBuffer uncompressedTimePageData = timePage.getUnCompressedData();
    Decoder timeDecoder = Decoder.getDecoderByType(timePage.getEncoding(), TSDataType.INT64);
    TimePageReader timePageReader =
        new TimePageReader(timePageHeader, uncompressedTimePageData, timeDecoder);
    timePageReader.setDeleteIntervalList(timePage.getDeleteIntervalList());
    timePage.clear();

    List<ValuePageReader> valuePageReaders = new ArrayList<>(valuePages.size());
    int nonEmptyPageNum = 1;
    for (int i = 0; i < valuePages.size(); i++) {
      PageLoader valuePage = valuePages.get(i);
      ValuePageReader valuePageReader = null;
      if (!valuePage.isEmpty()) {
        valuePageReader =
            new ValuePageReader(
                valuePage.getHeader(),
                valuePage.getUnCompressedData(),
                schemaList.get(i).getType(),
                Decoder.getDecoderByType(
                    schemaList.get(i).getEncodingType(), schemaList.get(i).getType()));
        valuePageReader.setDeleteIntervalList(valuePage.getDeleteIntervalList());
        nonEmptyPageNum++;
      }
      valuePage.clear();
      valuePageReaders.add(valuePageReader);
    }
    summary.increaseDeserializedPageNum(nonEmptyPageNum);

    long processedPointNum = 0;
    IPointReader lazyPointReader = getPointReader(timePageReader, valuePageReaders);
    while (lazyPointReader.hasNextTimeValuePair()) {
      TimeValuePair timeValuePair = lazyPointReader.nextTimeValuePair();
      long currentTime = timeValuePair.getTimestamp();
      chunkWriter.write(currentTime, timeValuePair.getValue().getVector());
      checkAndUpdatePreviousTimestamp(currentTime);
      processedPointNum++;
    }
    processedPointNum *= schemaList.size();
    summary.increaseRewritePointNum(processedPointNum);

    if (flushController.canFlushCurrentChunkWriter()) {
      flushCurrentChunkWriter();
    }
  }

  protected IPointReader getPointReader(
      TimePageReader timePageReader, List<ValuePageReader> valuePageReaders) throws IOException {
    return new CompactionAlignedPageLazyLoadPointReader(
        timePageReader, valuePageReaders, this.ignoreAllNullRows);
  }

  protected void checkAndUpdatePreviousTimestamp(long currentWritingTimestamp) {
    if (currentWritingTimestamp <= lastWriteTimestamp) {
      throw new CompactionLastTimeCheckFailedException(
          device.toString(), currentWritingTimestamp, lastWriteTimestamp);
    } else {
      lastWriteTimestamp = currentWritingTimestamp;
    }
  }

  protected class ReadChunkAlignedSeriesCompactionFlushController {
    private static final int largeFileLevelSeparator = 2;
    private final int compactionTargetFileLevel;
    private final long targetChunkPointNum;
    private final long targetChunkSize;
    private final long targetPagePointNum;
    private final long targetPageSize;

    public ReadChunkAlignedSeriesCompactionFlushController(int compactionFileLevel) {
      this.compactionTargetFileLevel = compactionFileLevel;
      this.targetChunkSize = IoTDBDescriptor.getInstance().getConfig().getTargetChunkSize();
      this.targetChunkPointNum = IoTDBDescriptor.getInstance().getConfig().getTargetChunkPointNum();
      this.targetPageSize = TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
      this.targetPagePointNum =
          TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();
    }

    public boolean canCompactCurrentChunkByDirectlyFlush(
        ChunkLoader timeChunk, List<ChunkLoader> valueChunks) throws IOException {
      return canFlushCurrentChunkWriter() && canFlushChunk(timeChunk, valueChunks);
    }

    protected boolean canFlushCurrentChunkWriter() {
      return chunkWriter.checkIsChunkSizeOverThreshold(targetChunkSize, targetChunkPointNum, true);
    }

    private boolean canFlushChunk(ChunkLoader timeChunk, List<ChunkLoader> valueChunks)
        throws IOException {
      if (timeChunk.getModifiedStatus() == ModifiedStatus.PARTIAL_DELETED) {
        return false;
      }
      boolean largeEnough =
          timeChunk.getHeader().getDataSize() >= targetChunkSize
              || timeChunk.getChunkMetadata().getNumOfPoints() >= targetChunkPointNum;
      if (timeSchema.getEncodingType() != timeChunk.getHeader().getEncodingType()
          || timeSchema.getCompressor() != timeChunk.getHeader().getCompressionType()) {
        return false;
      }
      for (int i = 0; i < valueChunks.size(); i++) {
        ChunkLoader valueChunk = valueChunks.get(i);
        if (valueChunk.isEmpty()) {
          continue;
        }
        IMeasurementSchema schema = schemaList.get(i);
        if (schema.getEncodingType() != valueChunk.getHeader().getEncodingType()
            || schema.getCompressor() != valueChunk.getHeader().getCompressionType()) {
          return false;
        }
        if (valueChunk.getModifiedStatus() == ModifiedStatus.PARTIAL_DELETED) {
          return false;
        }
        if (valueChunk.getHeader().getDataSize() >= targetChunkSize) {
          largeEnough = true;
        }
      }
      return largeEnough;
    }

    protected boolean canCompactCurrentPageByDirectlyFlush(
        PageLoader timePage, List<PageLoader> valuePages) {
      boolean isHighLevelCompaction = compactionTargetFileLevel > largeFileLevelSeparator;
      if (isHighLevelCompaction) {
        return canFlushPage(timePage, valuePages);
      } else {
        return canSealCurrentPageWriter() && canFlushPage(timePage, valuePages);
      }
    }

    private boolean canSealCurrentPageWriter() {
      return chunkWriter.checkIsUnsealedPageOverThreshold(targetPageSize, targetPagePointNum, true);
    }

    private boolean canFlushPage(PageLoader timePage, List<PageLoader> valuePages) {
      if (timePage.getModifiedStatus() == ModifiedStatus.PARTIAL_DELETED) {
        return false;
      }
      long count = timePage.getHeader().getStatistics().getCount();
      boolean largeEnough =
          count >= targetPagePointNum
              || Math.max(
                      estimateMemorySizeAsPageWriter(timePage),
                      timePage.getHeader().getUncompressedSize())
                  >= targetPageSize;
      if (timeSchema.getEncodingType() != timePage.getEncoding()
          || timeSchema.getCompressor() != timePage.getCompressionType()) {
        return false;
      }
      for (int i = 0; i < valuePages.size(); i++) {
        PageLoader valuePage = valuePages.get(i);
        if (valuePage.isEmpty()) {
          continue;
        }
        IMeasurementSchema schema = schemaList.get(i);
        if (schema.getCompressor() != valuePage.getCompressionType()
            || schema.getEncodingType() != valuePage.getEncoding()) {
          return false;
        }
        if (valuePage.getModifiedStatus() == ModifiedStatus.PARTIAL_DELETED) {
          return false;
        }
        if (Math.max(
                valuePage.getHeader().getUncompressedSize(),
                estimateMemorySizeAsPageWriter(valuePage))
            >= targetPageSize) {
          largeEnough = true;
        }
      }
      return largeEnough;
    }

    private long estimateMemorySizeAsPageWriter(PageLoader pageLoader) {
      long count = pageLoader.getHeader().getStatistics().getCount();
      long size;
      switch (pageLoader.getDataType()) {
        case INT32:
        case DATE:
          size = count * Integer.BYTES;
          break;
        case TIMESTAMP:
        case INT64:
        case VECTOR:
          size = count * Long.BYTES;
          break;
        case FLOAT:
          size = count * Float.BYTES;
          break;
        case DOUBLE:
          size = count * Double.BYTES;
          break;
        case BOOLEAN:
          size = count * Byte.BYTES;
          break;
        case TEXT:
        case STRING:
        case BLOB:
          size = pageLoader.getHeader().getUncompressedSize();
          break;
        default:
          throw new IllegalArgumentException(
              "Unsupported data type: " + pageLoader.getDataType().toString());
      }
      // Due to the fact that the page writer in memory includes some other objects
      // and has a special calculation method, the estimated size will actually be
      // larger. So we simply adopt the method of multiplying by 1.05 times. If this
      // is not done, the result here might be close to the target page size but
      // slightly smaller.
      return (long) (size * 1.05);
    }
  }
}
