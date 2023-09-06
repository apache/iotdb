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

package org.apache.iotdb.db.queryengine.execution.load;

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.db.storageengine.dataregion.modification.Deletion;
import org.apache.iotdb.db.storageengine.dataregion.modification.Modification;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.utils.TimePartitionUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.exception.TsFileRuntimeException;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkGroupHeader;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.read.reader.page.TimePageReader;
import org.apache.iotdb.tsfile.read.reader.page.ValuePageReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class MergedTsFileSplitter {

  private static final Logger logger = LoggerFactory.getLogger(TsFileSplitter.class);

  private final List<File> tsFiles;
  private final Function<TsFileData, Boolean> consumer;
  private final PriorityQueue<SplitTask> taskPriorityQueue;
  private ExecutorService asyncExecutor;
  private long timePartitionInterval;
  private AtomicInteger splitIdGenerator = new AtomicInteger();

  public MergedTsFileSplitter(
      List<File> tsFiles,
      Function<TsFileData, Boolean> consumer,
      ExecutorService asyncExecutor,
      long timePartitionInterval) {
    this.tsFiles = tsFiles;
    this.consumer = consumer;
    this.asyncExecutor = asyncExecutor;
    this.timePartitionInterval = timePartitionInterval;
    taskPriorityQueue = new PriorityQueue<>();
  }

  public void splitTsFileByDataPartition() throws IOException, IllegalStateException {
    for (File tsFile : tsFiles) {
      SplitTask splitTask = new SplitTask(tsFile, asyncExecutor);
      if (splitTask.hasNext()) {
        taskPriorityQueue.add(splitTask);
      }
    }

    List<SplitTask> equalTasks = new ArrayList<>();
    while (!taskPriorityQueue.isEmpty()) {
      SplitTask task = taskPriorityQueue.poll();
      equalTasks.add(task);
      // find chunks of the same series in other files
      while (!taskPriorityQueue.isEmpty()) {
        if (taskPriorityQueue.peek().compareTo(task) == 0) {
          equalTasks.add(taskPriorityQueue.poll());
        } else {
          break;
        }
      }

      for (SplitTask equalTask : equalTasks) {
        TsFileData tsFileData = equalTask.removeNext();
        tsFileData.setSplitId(splitIdGenerator.incrementAndGet());
        consumer.apply(tsFileData);
        if (equalTask.hasNext()) {
          taskPriorityQueue.add(equalTask);
        }
      }
      equalTasks.clear();
    }
  }

  public void close() throws IOException {
    for (SplitTask task : taskPriorityQueue) {
      task.close();
    }
    taskPriorityQueue.clear();
  }

  private class SplitTask implements Comparable<SplitTask> {

    private final File tsFile;
    private TsFileSequenceReader reader;
    private final TreeMap<Long, List<Deletion>> offset2Deletions;

    private String curDevice;
    private boolean isTimeChunkNeedDecode;
    private Map<Integer, List<AlignedChunkData>> pageIndex2ChunkData;
    private Map<Integer, long[]> pageIndex2Times;
    private Map<Long, IChunkMetadata> offset2ChunkMetadata;

    private BlockingQueue<TsFileData> nextSplits;
    private TsFileData nextSplit;
    private byte marker = -1;

    private ExecutorService asyncExecutor;
    private Future<Void> asyncTask;

    public SplitTask(File tsFile, ExecutorService asyncExecutor) throws IOException {
      this.tsFile = tsFile;
      this.asyncExecutor = asyncExecutor;
      offset2Deletions = new TreeMap<>();
      init();
    }

    @Override
    public int compareTo(SplitTask o) {
      try {
        TsFileData thisNext = showNext();
        TsFileData thatNext = o.showNext();
        // out put modification first
        if (thisNext.isModification() && thatNext.isModification()) {
          return 0;
        }
        if (thisNext.isModification()) {
          return 1;
        }
        if (thatNext.isModification()) {
          return -1;
        }

        ChunkData thisChunk = (ChunkData) thisNext;
        ChunkData thatChunk = ((ChunkData) thatNext);
        Comparator<ChunkData> chunkDataComparator =
            Comparator.comparing(ChunkData::getDevice, String::compareTo)
                .thenComparing(ChunkData::firstMeasurement, String::compareTo);
        return chunkDataComparator.compare(thisChunk, thatChunk);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    private void init() throws IOException {
      this.reader = new TsFileSequenceReader(tsFile.getAbsolutePath());
      getAllModification(offset2Deletions);

      if (!checkMagic(reader)) {
        throw new TsFileRuntimeException(
            String.format("Magic String check error when parsing TsFile %s.", tsFile.getPath()));
      }
      reader.position((long) TSFileConfig.MAGIC_STRING.getBytes().length + 1);

      curDevice = null;
      isTimeChunkNeedDecode = true;
      pageIndex2ChunkData = new HashMap<>();
      pageIndex2Times = null;
      offset2ChunkMetadata = new HashMap<>();
      getChunkMetadata(reader, offset2ChunkMetadata);

      nextSplits = new LinkedBlockingDeque<>(64);
      if (asyncExecutor != null) {
        asyncTask =
            asyncExecutor.submit(
                () -> {
                  try {
                    asyncComputeNext();
                  } catch (Throwable e) {
                    logger.info("Exception during splitting", e);
                    throw e;
                  }
                  return null;
                });
      }
    }

    private void asyncComputeNext() throws IOException {
      while (reader != null && !Thread.interrupted()) {
        computeNext();
      }
      logger.info("{} end splitting", tsFile);
    }

    public boolean hasNext() throws IOException {
      if (nextSplit != null && !(nextSplit instanceof EmptyTsFileData)) {
        return true;
      }
      if (reader == null && nextSplits.isEmpty()) {
        return false;
      }

      if (asyncExecutor == null) {
        computeNext();
        if (!nextSplits.isEmpty()) {
          nextSplit = nextSplits.poll();
          return true;
        } else {
          return false;
        }
      } else {
        try {
          nextSplit = nextSplits.take();
        } catch (InterruptedException e) {
          return false;
        }
        return !(nextSplit instanceof EmptyTsFileData);
      }
    }

    public TsFileData removeNext() throws IOException {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      TsFileData split = nextSplit;
      nextSplit = null;
      return split;
    }

    public TsFileData showNext() throws IOException {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return nextSplit;
    }

    public void close() throws IOException {
      if (asyncTask != null) {
        asyncTask.cancel(true);
        try {
          asyncTask.get();
        } catch (CancellationException ignored) {

        } catch (InterruptedException | ExecutionException e) {
          throw new IOException(e);
        }
      }
      if (reader != null) {
        reader.close();
        reader = null;
      }
    }

    private byte nextMarker() throws IOException {
      if (marker != -1) {
        // inherit the marker from the previous breakpoint
        // e.g. the marker after processing a chunk
        return marker;
      }
      return marker = reader.readMarker();
    }

    private void insertNewChunk(ChunkData chunkData) throws IOException {
      if (asyncExecutor == null) {
        nextSplits.add(chunkData);
      } else {
        try {
          nextSplits.put(chunkData);
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      }
    }

    @SuppressWarnings({"squid:S3776", "squid:S6541"})
    private void computeNext() throws IOException, IllegalStateException {
      if (reader == null) {
        return;
      }

      while (nextMarker() != MetaMarker.SEPARATOR) {
        switch (marker) {
          case MetaMarker.CHUNK_HEADER:
          case MetaMarker.TIME_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_TIME_CHUNK_HEADER:
            long chunkOffset = reader.position();
            boolean chunkDataGenerated =
                consumeAllAlignedChunkData(chunkOffset, pageIndex2ChunkData);
            handleModification(offset2Deletions, chunkOffset);
            if (chunkDataGenerated) {
              return;
            }

            ChunkHeader header = reader.readChunkHeader(marker);
            String measurementId = header.getMeasurementID();
            if (header.getDataSize() == 0) {
              throw new TsFileRuntimeException(
                  String.format(
                      "Empty Nonaligned Chunk or Time Chunk with offset %d in TsFile %s.",
                      chunkOffset, tsFile.getPath()));
            }

            boolean isAligned =
                ((header.getChunkType() & TsFileConstant.TIME_COLUMN_MASK)
                    == TsFileConstant.TIME_COLUMN_MASK);
            IChunkMetadata chunkMetadata = offset2ChunkMetadata.get(chunkOffset - Byte.BYTES);
            TTimePartitionSlot timePartitionSlot =
                TimePartitionUtils.getTimePartition(
                    chunkMetadata.getStartTime(), timePartitionInterval);
            ChunkData chunkData =
                ChunkData.createChunkData(isAligned, curDevice, header, timePartitionSlot);

            if (!needDecodeChunk(chunkMetadata)) {
              chunkData.setNotDecode();
              chunkData.writeEntireChunk(reader.readChunk(-1, header.getDataSize()), chunkMetadata);
              if (isAligned) {
                isTimeChunkNeedDecode = false;
                pageIndex2ChunkData
                    .computeIfAbsent(1, o -> new ArrayList<>())
                    .add((AlignedChunkData) chunkData);
              } else {
                insertNewChunk(chunkData);
              }
              break;
            }

            Decoder defaultTimeDecoder =
                Decoder.getDecoderByType(
                    TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
                    TSDataType.INT64);
            Decoder valueDecoder =
                Decoder.getDecoderByType(header.getEncodingType(), header.getDataType());
            int dataSize = header.getDataSize();
            int pageIndex = 0;
            if (isAligned) {
              isTimeChunkNeedDecode = true;
              pageIndex2Times = new HashMap<>();
            }

            while (dataSize > 0) {
              PageHeader pageHeader =
                  reader.readPageHeader(
                      header.getDataType(),
                      (header.getChunkType() & 0x3F) == MetaMarker.CHUNK_HEADER);
              long pageDataSize = pageHeader.getSerializedPageSize();
              if (!needDecodePage(pageHeader, chunkMetadata)) { // an entire page
                long startTime =
                    pageHeader.getStatistics() == null
                        ? chunkMetadata.getStartTime()
                        : pageHeader.getStartTime();
                TTimePartitionSlot pageTimePartitionSlot =
                    TimePartitionUtils.getTimePartition(startTime, timePartitionInterval);
                if (!timePartitionSlot.equals(pageTimePartitionSlot)) {
                  if (!isAligned) {
                    insertNewChunk(chunkData);
                  }
                  timePartitionSlot = pageTimePartitionSlot;
                  chunkData =
                      ChunkData.createChunkData(isAligned, curDevice, header, timePartitionSlot);
                }
                if (isAligned) {
                  pageIndex2ChunkData
                      .computeIfAbsent(pageIndex, o -> new ArrayList<>())
                      .add((AlignedChunkData) chunkData);
                }
                chunkData.writeEntirePage(pageHeader, reader.readCompressedPage(pageHeader));
              } else { // split page
                ByteBuffer pageData = reader.readPage(pageHeader, header.getCompressionType());
                Pair<long[], Object[]> tvArray =
                    decodePage(
                        isAligned, pageData, pageHeader, defaultTimeDecoder, valueDecoder, header);
                long[] times = tvArray.left;
                Object[] values = tvArray.right;
                if (isAligned) {
                  pageIndex2Times.put(pageIndex, times);
                }

                int start = 0;
                long endTime = timePartitionSlot.getStartTime() + timePartitionInterval;
                for (int i = 0; i < times.length; i++) {
                  if (times[i] >= endTime) {
                    chunkData.writeDecodePage(times, values, start, i);
                    if (isAligned) {
                      pageIndex2ChunkData
                          .computeIfAbsent(pageIndex, o -> new ArrayList<>())
                          .add((AlignedChunkData) chunkData);
                    } else {
                      insertNewChunk(chunkData);
                    }

                    timePartitionSlot =
                        TimePartitionUtils.getTimePartition(times[i], timePartitionInterval);
                    endTime = timePartitionSlot.getStartTime() + timePartitionInterval;
                    chunkData =
                        ChunkData.createChunkData(isAligned, curDevice, header, timePartitionSlot);
                    start = i;
                  }
                }
                chunkData.writeDecodePage(times, values, start, times.length);
                if (isAligned) {
                  pageIndex2ChunkData
                      .computeIfAbsent(pageIndex, o -> new ArrayList<>())
                      .add((AlignedChunkData) chunkData);
                }
              }

              pageIndex += 1;
              dataSize -= pageDataSize;
            }

            if (!isAligned) {
              insertNewChunk(chunkData);
            }
            break;
          case MetaMarker.VALUE_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_VALUE_CHUNK_HEADER:
            chunkOffset = reader.position();
            chunkMetadata = offset2ChunkMetadata.get(chunkOffset - Byte.BYTES);
            header = reader.readChunkHeader(marker);
            if (header.getDataSize() == 0) {
              handleEmptyValueChunk(
                  header, pageIndex2ChunkData, chunkMetadata, isTimeChunkNeedDecode);
              break;
            }

            if (!isTimeChunkNeedDecode) {
              AlignedChunkData alignedChunkData = pageIndex2ChunkData.get(1).get(0);
              alignedChunkData.addValueChunk(header);
              alignedChunkData.writeEntireChunk(
                  reader.readChunk(-1, header.getDataSize()), chunkMetadata);
              break;
            }

            Set<ChunkData> allChunkData = new HashSet<>();
            dataSize = header.getDataSize();
            pageIndex = 0;
            valueDecoder = Decoder.getDecoderByType(header.getEncodingType(), header.getDataType());

            while (dataSize > 0) {
              PageHeader pageHeader =
                  reader.readPageHeader(
                      header.getDataType(),
                      (header.getChunkType() & 0x3F) == MetaMarker.CHUNK_HEADER);
              List<AlignedChunkData> alignedChunkDataList = pageIndex2ChunkData.get(pageIndex);
              for (AlignedChunkData alignedChunkData : alignedChunkDataList) {
                if (!allChunkData.contains(alignedChunkData)) {
                  alignedChunkData.addValueChunk(header);
                  allChunkData.add(alignedChunkData);
                }
              }
              if (alignedChunkDataList.size() == 1) { // write entire page
                // write the entire page if it's not an empty page.
                alignedChunkDataList
                    .get(0)
                    .writeEntirePage(pageHeader, reader.readCompressedPage(pageHeader));
              } else { // decode page
                long[] times = pageIndex2Times.get(pageIndex);
                TsPrimitiveType[] values =
                    decodeValuePage(reader, header, pageHeader, times, valueDecoder);
                for (AlignedChunkData alignedChunkData : alignedChunkDataList) {
                  alignedChunkData.writeDecodeValuePage(times, values, header.getDataType());
                }
              }
              long pageDataSize = pageHeader.getSerializedPageSize();
              pageIndex += 1;
              dataSize -= pageDataSize;
            }
            break;
          case MetaMarker.CHUNK_GROUP_HEADER:
            ChunkGroupHeader chunkGroupHeader = reader.readChunkGroupHeader();
            curDevice = chunkGroupHeader.getDeviceID();
            break;
          case MetaMarker.OPERATION_INDEX_RANGE:
            reader.readPlanIndex();
            break;
          default:
            MetaMarker.handleUnexpectedMarker(marker);
        }
        marker = -1;
        if (!nextSplits.isEmpty()) {
          return;
        }
      }

      consumeAllAlignedChunkData(reader.position(), pageIndex2ChunkData);
      handleModification(offset2Deletions, Long.MAX_VALUE);
      close();
      if (asyncExecutor != null) {
        nextSplits.add(new EmptyTsFileData());
      }
    }

    private class EmptyTsFileData implements TsFileData {

      @Override
      public long getDataSize() {
        return 0;
      }

      @Override
      public void writeToFileWriter(TsFileIOWriter writer) throws IOException {}

      @Override
      public boolean isModification() {
        return false;
      }

      @Override
      public void serialize(DataOutputStream stream) throws IOException {}

      @Override
      public int getSplitId() {
        return 0;
      }

      @Override
      public void setSplitId(int sid) {}
    }

    private void getAllModification(Map<Long, List<Deletion>> offset2Deletions) throws IOException {
      try (ModificationFile modificationFile =
          new ModificationFile(tsFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX)) {
        for (Modification modification : modificationFile.getModifications()) {
          offset2Deletions
              .computeIfAbsent(modification.getFileOffset(), o -> new ArrayList<>())
              .add((Deletion) modification);
        }
      }
    }

    private boolean checkMagic(TsFileSequenceReader reader) throws IOException {
      String magic = reader.readHeadMagic();
      if (!magic.equals(TSFileConfig.MAGIC_STRING)) {
        logger.error("the file's MAGIC STRING is incorrect, file path: {}", reader.getFileName());
        return false;
      }

      byte versionNumber = reader.readVersionNumber();
      if (versionNumber != TSFileConfig.VERSION_NUMBER) {
        logger.error("the file's Version Number is incorrect, file path: {}", reader.getFileName());
        return false;
      }

      if (!reader.readTailMagic().equals(TSFileConfig.MAGIC_STRING)) {
        logger.error("the file is not closed correctly, file path: {}", reader.getFileName());
        return false;
      }
      return true;
    }

    private void getChunkMetadata(
        TsFileSequenceReader reader, Map<Long, IChunkMetadata> offset2ChunkMetadata)
        throws IOException {
      Map<String, List<TimeseriesMetadata>> device2Metadata = reader.getAllTimeseriesMetadata(true);
      for (Map.Entry<String, List<TimeseriesMetadata>> entry : device2Metadata.entrySet()) {
        for (TimeseriesMetadata timeseriesMetadata : entry.getValue()) {
          for (IChunkMetadata chunkMetadata : timeseriesMetadata.getChunkMetadataList()) {
            offset2ChunkMetadata.put(chunkMetadata.getOffsetOfChunkHeader(), chunkMetadata);
          }
        }
      }
    }

    private void handleModification(
        TreeMap<Long, List<Deletion>> offset2Deletions, long chunkOffset) {
      while (!offset2Deletions.isEmpty() && offset2Deletions.firstEntry().getKey() <= chunkOffset) {
        offset2Deletions
            .pollFirstEntry()
            .getValue()
            .forEach(o -> nextSplits.add(new DeletionData(o)));
      }
    }

    private boolean consumeAllAlignedChunkData(
        long offset, Map<Integer, List<AlignedChunkData>> pageIndex2ChunkData) {
      if (pageIndex2ChunkData.isEmpty()) {
        return false;
      }

      Set<ChunkData> allChunkData = new HashSet<>();
      for (Map.Entry<Integer, List<AlignedChunkData>> entry : pageIndex2ChunkData.entrySet()) {
        allChunkData.addAll(entry.getValue());
      }
      nextSplits.addAll(allChunkData);
      pageIndex2ChunkData.clear();
      return true;
    }

    private boolean needDecodeChunk(IChunkMetadata chunkMetadata) {
      return !TimePartitionUtils.getTimePartition(
              chunkMetadata.getStartTime(), timePartitionInterval)
          .equals(
              TimePartitionUtils.getTimePartition(
                  chunkMetadata.getEndTime(), timePartitionInterval));
    }

    private boolean needDecodePage(PageHeader pageHeader, IChunkMetadata chunkMetadata) {
      if (pageHeader.getStatistics() == null) {
        return !TimePartitionUtils.getTimePartition(
                chunkMetadata.getStartTime(), timePartitionInterval)
            .equals(
                TimePartitionUtils.getTimePartition(
                    chunkMetadata.getEndTime(), timePartitionInterval));
      }
      return !TimePartitionUtils.getTimePartition(pageHeader.getStartTime(), timePartitionInterval)
          .equals(
              TimePartitionUtils.getTimePartition(pageHeader.getEndTime(), timePartitionInterval));
    }

    private Pair<long[], Object[]> decodePage(
        boolean isAligned,
        ByteBuffer pageData,
        PageHeader pageHeader,
        Decoder timeDecoder,
        Decoder valueDecoder,
        ChunkHeader chunkHeader)
        throws IOException {
      if (isAligned) {
        TimePageReader timePageReader = new TimePageReader(pageHeader, pageData, timeDecoder);
        long[] times = timePageReader.getNextTimeBatch();
        return new Pair<>(times, new Object[times.length]);
      }

      valueDecoder.reset();
      PageReader pageReader =
          new PageReader(pageData, chunkHeader.getDataType(), valueDecoder, timeDecoder, null);
      BatchData batchData = pageReader.getAllSatisfiedPageData();
      long[] times = new long[batchData.length()];
      Object[] values = new Object[batchData.length()];
      int index = 0;
      while (batchData.hasCurrent()) {
        times[index] = batchData.currentTime();
        values[index++] = batchData.currentValue();
        batchData.next();
      }
      return new Pair<>(times, values);
    }

    private void handleEmptyValueChunk(
        ChunkHeader header,
        Map<Integer, List<AlignedChunkData>> pageIndex2ChunkData,
        IChunkMetadata chunkMetadata,
        boolean isTimeChunkNeedDecode)
        throws IOException {
      Set<ChunkData> allChunkData = new HashSet<>();
      for (Map.Entry<Integer, List<AlignedChunkData>> entry : pageIndex2ChunkData.entrySet()) {
        for (AlignedChunkData alignedChunkData : entry.getValue()) {
          if (!allChunkData.contains(alignedChunkData)) {
            alignedChunkData.addValueChunk(header);
            if (!isTimeChunkNeedDecode) {
              alignedChunkData.writeEntireChunk(ByteBuffer.allocate(0), chunkMetadata);
            }
            allChunkData.add(alignedChunkData);
          }
        }
      }
    }

    /**
     * handle empty page in aligned chunk, if uncompressedSize and compressedSize are both 0, and
     * the statistics is null, then the page is empty.
     *
     * @param pageHeader page header
     * @return true if the page is empty
     */
    private boolean isEmptyPage(PageHeader pageHeader) {
      return pageHeader.getUncompressedSize() == 0
          && pageHeader.getCompressedSize() == 0
          && pageHeader.getStatistics() == null;
    }

    private TsPrimitiveType[] decodeValuePage(
        TsFileSequenceReader reader,
        ChunkHeader chunkHeader,
        PageHeader pageHeader,
        long[] times,
        Decoder valueDecoder)
        throws IOException {
      if (pageHeader.getSerializedPageSize() == 0) {
        return new TsPrimitiveType[times.length];
      }

      valueDecoder.reset();
      ByteBuffer pageData = reader.readPage(pageHeader, chunkHeader.getCompressionType());
      ValuePageReader valuePageReader =
          new ValuePageReader(pageHeader, pageData, chunkHeader.getDataType(), valueDecoder);
      return valuePageReader.nextValueBatch(
          times); // should be origin time, so recording satisfied length is necessary
    }
  }
}
