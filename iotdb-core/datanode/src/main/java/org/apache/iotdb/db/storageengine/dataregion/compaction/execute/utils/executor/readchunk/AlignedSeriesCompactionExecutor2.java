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
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.ModifiedStatus;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.readchunk.loader.ChunkLoader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.readchunk.loader.InstantChunkLoader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.readchunk.loader.InstantPageLoader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.readchunk.loader.LazyChunkLoader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.readchunk.loader.LazyPageLoader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.readchunk.loader.PageLoader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.io.CompactionTsFileReader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.io.CompactionTsFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.compaction.io.LazyAlignedChunkWriterImpl;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.read.reader.page.AlignedPageReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import com.google.common.util.concurrent.RateLimiter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AlignedSeriesCompactionExecutor2 {

  protected final String device;
  protected final LinkedList<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>>
      readerAndChunkMetadataList;
  protected final TsFileResource targetResource;
  protected final CompactionTsFileWriter writer;

  protected final AlignedChunkWriterImpl chunkWriter;
  protected final Map<String, MeasurementSchema> schemaMap;
  protected List<IMeasurementSchema> schemaList;
  protected Map<String, Integer> measurementSchemaListIndexMap;
  protected final CompactionTaskSummary summary;
  protected final RateLimiter rateLimiter;

  protected final long targetChunkPointNum;
  protected final long targetChunkSize;
  protected final long targetPagePointNum;
  protected final long targetPageSize;
  protected final boolean saveMemory;

  public AlignedSeriesCompactionExecutor2(
      String device,
      Map<String, MeasurementSchema> schemaMap,
      TsFileResource targetResource,
      LinkedList<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>> readerAndChunkMetadataList,
      CompactionTsFileWriter writer,
      CompactionTaskSummary summary,
      boolean saveMemory)
      throws IOException {
    this.device = device;
    this.readerAndChunkMetadataList = readerAndChunkMetadataList;
    this.writer = writer;
    this.targetResource = targetResource;
    this.schemaMap = schemaMap;
    this.summary = summary;
    collectValueColumnSchemaList(schemaMap);
    this.saveMemory = saveMemory;
    if (saveMemory) {
      this.chunkWriter = new LazyAlignedChunkWriterImpl(schemaList);
    } else {
      this.chunkWriter = new AlignedChunkWriterImpl(schemaList);
    }
    this.targetChunkSize = IoTDBDescriptor.getInstance().getConfig().getTargetChunkSize();
    this.targetChunkPointNum = IoTDBDescriptor.getInstance().getConfig().getTargetChunkPointNum();
    this.targetPageSize = TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
    this.targetPagePointNum =
        TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();
    this.rateLimiter = CompactionTaskManager.getInstance().getProcessPointRateLimiter();
  }

  private void collectValueColumnSchemaList(Map<String, MeasurementSchema> schemaMap) {
    this.schemaList =
        schemaMap.values().stream()
            .filter(schema -> !"".equals(schema.getMeasurementId()))
            .sorted(Comparator.comparing(IMeasurementSchema::getMeasurementId))
            .collect(Collectors.toList());
    this.measurementSchemaListIndexMap = new HashMap<>();
    for (int i = 0; i < schemaList.size(); i++) {
      this.measurementSchemaListIndexMap.put(schemaList.get(i).getMeasurementId(), i);
    }
  }

  public void execute() throws IOException {
    while (!readerAndChunkMetadataList.isEmpty()) {
      Pair<TsFileSequenceReader, List<AlignedChunkMetadata>> readerListPair =
          readerAndChunkMetadataList.removeFirst();
      TsFileSequenceReader reader = readerListPair.left;
      List<AlignedChunkMetadata> alignedChunkMetadataList = readerListPair.right;

      if (reader instanceof CompactionTsFileReader) {
        ((CompactionTsFileReader) reader).markStartOfAlignedSeries();
      }
      for (AlignedChunkMetadata alignedChunkMetadata : alignedChunkMetadataList) {
        try {
          compactWithAlignedChunk(reader, alignedChunkMetadata);
        } catch (Exception e) {
          throw new RuntimeException("Meet errors when compact aligned series", e);
        }
      }
      if (reader instanceof CompactionTsFileReader) {
        ((CompactionTsFileReader) reader).markEndOfAlignedSeries();
      }
    }

    if (!chunkWriter.isEmpty()) {
      flushCurrentChunkWriter();
    }
    writer.checkMetadataSizeAndMayFlush();
  }

  private void compactWithAlignedChunk(
      TsFileSequenceReader reader, AlignedChunkMetadata alignedChunkMetadata)
      throws IOException, PageException {
    ChunkLoader timeChunk =
        getChunkLoader(reader, (ChunkMetadata) alignedChunkMetadata.getTimeChunkMetadata());
    List<ChunkLoader> valueChunks = Arrays.asList(new ChunkLoader[schemaList.size()]);
    Collections.fill(valueChunks, getChunkLoader(reader, null));
    for (IChunkMetadata chunkMetadata : alignedChunkMetadata.getValueChunkMetadataList()) {
      if (chunkMetadata == null) {
        continue;
      }
      ChunkLoader valueChunk = getChunkLoader(reader, (ChunkMetadata) chunkMetadata);
      int idx = measurementSchemaListIndexMap.get(chunkMetadata.getMeasurementUid());
      valueChunks.set(idx, valueChunk);
    }
    if (canFlushCurrentChunkWriter() && canFlushChunk(timeChunk, valueChunks)) {
      flushCurrentChunkWriter();
      compactAlignedChunkByFlush(timeChunk, valueChunks);
    } else {
      compactAlignedChunkByDeserialize(timeChunk, valueChunks);
    }
  }

  private ChunkLoader getChunkLoader(TsFileSequenceReader reader, ChunkMetadata chunkMetadata)
      throws IOException {
    if (saveMemory) {
      if (chunkMetadata == null || chunkMetadata.getStatistics().getCount() == 0) {
        return new LazyChunkLoader();
      }
      return new LazyChunkLoader((CompactionTsFileReader) reader, chunkMetadata);
    } else {
      if (chunkMetadata == null || chunkMetadata.getStatistics().getCount() == 0) {
        return new InstantChunkLoader();
      }
      Chunk chunk = reader.readMemChunk(chunkMetadata);
      return new InstantChunkLoader(chunkMetadata, chunk);
    }
  }

  private boolean canFlushCurrentChunkWriter() {
    return chunkWriter.checkIsChunkSizeOverThreshold(targetChunkSize, targetChunkPointNum, true);
  }

  private void flushCurrentChunkWriter() throws IOException {
    chunkWriter.sealCurrentPage();
    writer.writeChunk(chunkWriter);
  }

  private boolean canFlushChunk(ChunkLoader timeChunk, List<ChunkLoader> valueChunks)
      throws IOException {
    boolean largeEnough =
        timeChunk.getHeader().getDataSize() > targetChunkSize
            || timeChunk.getChunkMetadata().getNumOfPoints() > targetChunkPointNum;
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
      if (valueChunk.getHeader().getDataSize() > targetChunkSize) {
        largeEnough = true;
      }
    }
    return largeEnough;
  }

  private void compactAlignedChunkByFlush(ChunkLoader timeChunk, List<ChunkLoader> valueChunks)
      throws IOException {
    writer.markStartingWritingAligned();
    writer.writeChunk(timeChunk.getChunk(), timeChunk.getChunkMetadata());
    int nonEmptyChunkNum = 1;
    for (int i = 0; i < valueChunks.size(); i++) {
      ChunkLoader valueChunk = valueChunks.get(i);
      if (valueChunk.isEmpty()) {
        IMeasurementSchema schema = schemaList.get(i + 1);
        writer.writeEmptyValueChunk(
            schema.getMeasurementId(),
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

      if (canSealCurrentPageWriter() && canFlushPage(timePage, valuePages)) {
        chunkWriter.sealCurrentPage();
        compactAlignedPageByFlush(timePage, valuePages);
      } else {
        compactAlignedPageByDeserialize(timePage, valuePages);
      }
    }
  }

  private PageLoader getEmptyPage() {
    return saveMemory ? new LazyPageLoader() : new InstantPageLoader();
  }

  private boolean canSealCurrentPageWriter() {
    return chunkWriter.checkIsUnsealedPageOverThreshold(targetPageSize, targetPagePointNum, true);
  }

  private boolean canFlushPage(PageLoader timePage, List<PageLoader> valuePages) {
    boolean largeEnough =
        timePage.getHeader().getUncompressedSize() > targetPageSize
            || timePage.getHeader().getStatistics().getCount() > targetPagePointNum;

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
      if (valuePage.getHeader().getUncompressedSize() > targetPageSize) {
        largeEnough = true;
      }
    }
    return largeEnough;
  }

  private void compactAlignedPageByFlush(PageLoader timePage, List<PageLoader> valuePageLoaders)
      throws PageException, IOException {
    int nonEmptyPage = 1;
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
    timePage.clear();

    List<PageHeader> valuePageHeaders = new ArrayList<>(valuePages.size());
    List<ByteBuffer> uncompressedValuePageDatas = new ArrayList<>(valuePages.size());
    List<TSDataType> valueDataTypes = new ArrayList<>(valuePages.size());
    List<Decoder> valueDecoders = new ArrayList<>(valuePages.size());
    List<List<TimeRange>> deleteIntervalLists = new ArrayList<>(valuePages.size());
    int nonEmptyPageNum = 1;
    for (int i = 0; i < valuePages.size(); i++) {
      PageLoader valuePage = valuePages.get(i);
      if (valuePage.isEmpty()) {
        valuePageHeaders.add(null);
        uncompressedValuePageDatas.add(null);
        valueDataTypes.add(schemaList.get(i).getType());
        valueDecoders.add(
            Decoder.getDecoderByType(
                schemaList.get(i).getEncodingType(), schemaList.get(i).getType()));
        deleteIntervalLists.add(null);
      } else {
        valuePageHeaders.add(valuePage.getHeader());
        uncompressedValuePageDatas.add(valuePage.getUnCompressedData());
        valueDataTypes.add(valuePage.getDataType());
        valueDecoders.add(
            Decoder.getDecoderByType(valuePage.getEncoding(), valuePage.getDataType()));
        deleteIntervalLists.add(valuePage.getDeleteIntervalList());
        valuePage.clear();
        nonEmptyPageNum++;
      }
    }
    summary.increaseDeserializedPageNum(nonEmptyPageNum);

    AlignedPageReader alignedPageReader =
        new AlignedPageReader(
            timePageHeader,
            uncompressedTimePageData,
            timeDecoder,
            valuePageHeaders,
            uncompressedValuePageDatas,
            valueDataTypes,
            valueDecoders,
            null,
            true);
    alignedPageReader.setDeleteIntervalList(deleteIntervalLists);
    int processedPointNum = 0;
    if (true) {
      IPointReader lazyPointReader = alignedPageReader.getLazyPointReader();
      while (lazyPointReader.hasNextTimeValuePair()) {
        TimeValuePair timeValuePair = lazyPointReader.nextTimeValuePair();
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getVector());
        processedPointNum++;
      }
    } else {
      BatchData batchData = alignedPageReader.getAllSatisfiedPageData();
      while (batchData.hasCurrent()) {
        chunkWriter.write(batchData.currentTime(), (TsPrimitiveType[]) batchData.currentValue());
        batchData.next();
      }
      processedPointNum = batchData.length();
    }
    summary.increaseProcessPointNum(processedPointNum);
    rateLimiter.acquire(processedPointNum);
  }
}
