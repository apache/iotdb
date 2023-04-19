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
package org.apache.iotdb.db.engine.compaction.execute.utils.executor.fast;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.compaction.execute.task.subtask.FastCompactionTaskSummary;
import org.apache.iotdb.db.engine.compaction.execute.utils.executor.fast.element.ChunkMetadataElement;
import org.apache.iotdb.db.engine.compaction.execute.utils.executor.fast.element.FileElement;
import org.apache.iotdb.db.engine.compaction.execute.utils.executor.fast.element.PageElement;
import org.apache.iotdb.db.engine.compaction.execute.utils.reader.PointPriorityReader;
import org.apache.iotdb.db.engine.compaction.execute.utils.writer.AbstractCompactionWriter;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.TimeRange;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

public abstract class SeriesCompactionExecutor {
  protected enum ModifiedStatus {
    ALL_DELETED,
    PARTIAL_DELETED,
    NONE_DELETED;
  }

  @FunctionalInterface
  public interface RemovePage {
    void call(PageElement pageElement)
        throws WriteProcessException, IOException, IllegalPathException;
  }

  private final FastCompactionTaskSummary summary;

  // source files which are sorted by the start time of current device from old to new. Notice: If
  // the type of timeIndex is FileTimeIndex, it may contain resources in which the current device
  // does not exist.
  protected List<FileElement> fileList = new ArrayList<>();;

  protected final PriorityQueue<ChunkMetadataElement> chunkMetadataQueue;

  protected final PriorityQueue<PageElement> pageQueue;

  protected AbstractCompactionWriter compactionWriter;

  protected int subTaskId;

  protected Map<TsFileResource, TsFileSequenceReader> readerCacheMap;

  private final Map<TsFileResource, List<Modification>> modificationCacheMap;

  private final PointPriorityReader pointPriorityReader;

  protected String deviceId;

  // Pages in this list will be sequentially judged whether there is a real overlap to choose
  // whether to put them in the point priority reader to deserialize or directly flush to chunk
  // writer. During the process of compacting overlapped page, there may be new overlapped pages
  // added into this list.
  private final List<PageElement> candidateOverlappedPages = new ArrayList<>();
  private long nextChunkStartTime = Long.MAX_VALUE;

  private long nextPageStartTime = Long.MAX_VALUE;

  protected boolean isAligned;

  protected SeriesCompactionExecutor(
      AbstractCompactionWriter compactionWriter,
      Map<TsFileResource, TsFileSequenceReader> readerCacheMap,
      Map<TsFileResource, List<Modification>> modificationCacheMap,
      String deviceId,
      boolean isAligned,
      int subTaskId,
      FastCompactionTaskSummary summary) {
    this.compactionWriter = compactionWriter;
    this.subTaskId = subTaskId;
    this.deviceId = deviceId;
    this.readerCacheMap = readerCacheMap;
    this.modificationCacheMap = modificationCacheMap;
    this.summary = summary;
    pointPriorityReader = new PointPriorityReader(this::checkShouldRemoveFile, isAligned);
    this.isAligned = isAligned;

    chunkMetadataQueue =
        new PriorityQueue<>(
            (o1, o2) -> {
              int timeCompare = Long.compare(o1.startTime, o2.startTime);
              return timeCompare != 0 ? timeCompare : Long.compare(o2.priority, o1.priority);
            });

    pageQueue =
        new PriorityQueue<>(
            (o1, o2) -> {
              int timeCompare = Long.compare(o1.startTime, o2.startTime);
              return timeCompare != 0 ? timeCompare : Long.compare(o2.priority, o1.priority);
            });
  }

  public abstract void execute()
      throws PageException, IllegalPathException, IOException, WriteProcessException;

  protected abstract void compactFiles()
      throws PageException, IOException, WriteProcessException, IllegalPathException;

  /** Compact chunks in chunk metadata queue. */
  protected void compactChunks()
      throws IOException, PageException, WriteProcessException, IllegalPathException {
    while (!chunkMetadataQueue.isEmpty()) {
      ChunkMetadataElement firstChunkMetadataElement = chunkMetadataQueue.poll();
      nextChunkStartTime =
          chunkMetadataQueue.isEmpty() ? Long.MAX_VALUE : chunkMetadataQueue.peek().startTime;
      boolean isChunkOverlap =
          firstChunkMetadataElement.chunkMetadata.getEndTime() >= nextChunkStartTime;
      boolean isModified = firstChunkMetadataElement.chunkMetadata.isModified();

      if (isChunkOverlap || isModified) {
        // has overlap or modified chunk, then deserialize it
        summary.CHUNK_OVERLAP_OR_MODIFIED++;
        compactWithOverlapChunks(firstChunkMetadataElement);
      } else {
        // has none overlap or modified chunk, flush it to file writer directly
        summary.CHUNK_NONE_OVERLAP += 1;
        compactWithNonOverlapChunk(firstChunkMetadataElement);
      }
    }
  }

  /**
   * Deserialize chunks and start compacting pages. Compact a series of chunks that overlap with
   * each other. Eg: The parameters are chunk 1 and chunk 2, that is, chunk 1 only overlaps with
   * chunk 2, while chunk 2 overlap with chunk 3, chunk 3 overlap with chunk 4,and so on, there are
   * 10 chunks in total. This method will merge all 10 chunks.
   */
  private void compactWithOverlapChunks(ChunkMetadataElement overlappedChunkMetadata)
      throws IOException, PageException, WriteProcessException, IllegalPathException {
    readChunk(overlappedChunkMetadata);
    deserializeChunkIntoPageQueue(overlappedChunkMetadata);

    compactPages();
  }

  /**
   * Flush chunk to target file directly. If the end time of chunk exceeds the end time of file or
   * the unsealed chunk is too small, then deserialize it.
   */
  private void compactWithNonOverlapChunk(ChunkMetadataElement chunkMetadataElement)
      throws IOException, PageException, WriteProcessException, IllegalPathException {
    readChunk(chunkMetadataElement);
    boolean success;
    if (isAligned) {
      success =
          compactionWriter.flushAlignedChunk(
              chunkMetadataElement.chunk,
              ((AlignedChunkMetadata) chunkMetadataElement.chunkMetadata).getTimeChunkMetadata(),
              chunkMetadataElement.valueChunks,
              ((AlignedChunkMetadata) chunkMetadataElement.chunkMetadata)
                  .getValueChunkMetadataList(),
              subTaskId);
    } else {
      success =
          compactionWriter.flushNonAlignedChunk(
              chunkMetadataElement.chunk,
              (ChunkMetadata) chunkMetadataElement.chunkMetadata,
              subTaskId);
    }
    if (success) {
      // flush chunk successfully, then remove this chunk
      updateSummary(chunkMetadataElement, ChunkStatus.DIRECTORY_FLUSH);
      checkShouldRemoveFile(chunkMetadataElement);
    } else {
      // unsealed chunk is not large enough or chunk.endTime > file.endTime, then deserialize chunk
      summary.CHUNK_NONE_OVERLAP_BUT_DESERIALIZE += 1;
      deserializeChunkIntoPageQueue(chunkMetadataElement);
      compactPages();
    }
  }

  abstract void deserializeChunkIntoPageQueue(ChunkMetadataElement chunkMetadataElement)
      throws IOException;

  abstract void readChunk(ChunkMetadataElement chunkMetadataElement) throws IOException;

  /** Deserialize files into chunk metadatas and put them into the chunk metadata queue. */
  abstract void deserializeFileIntoChunkMetadataQueue(List<FileElement> fileElements)
      throws IOException, IllegalPathException;

  /** Compact pages in page queue. */
  private void compactPages()
      throws IOException, PageException, WriteProcessException, IllegalPathException {
    while (!pageQueue.isEmpty()) {
      PageElement firstPageElement = getPageFromPageQueue(pageQueue.peek().startTime);
      ModifiedStatus modifiedStatus = isPageModified(firstPageElement);

      if (modifiedStatus == ModifiedStatus.ALL_DELETED) {
        // all data on this page has been deleted, remove it
        checkShouldRemoveFile(firstPageElement);
        continue;
      }

      boolean isPageOverlap =
          firstPageElement.pageHeader.getEndTime() >= nextPageStartTime
              || firstPageElement.pageHeader.getEndTime() >= nextChunkStartTime;

      if (isPageOverlap || modifiedStatus == ModifiedStatus.PARTIAL_DELETED) {
        // has overlap or modified pages, then deserialize it
        summary.PAGE_OVERLAP_OR_MODIFIED += 1;
        pointPriorityReader.addNewPage(firstPageElement);
        compactWithOverlapPages();
      } else {
        // has none overlap or modified pages, flush it to chunk writer directly
        summary.PAGE_NONE_OVERLAP += 1;
        compactWithNonOverlapPage(firstPageElement);
      }
    }
  }

  private void compactWithNonOverlapPage(PageElement pageElement)
      throws PageException, IOException, WriteProcessException, IllegalPathException {
    boolean success;
    if (isAligned) {
      success =
          compactionWriter.flushAlignedPage(
              pageElement.pageData,
              pageElement.pageHeader,
              pageElement.valuePageDatas,
              pageElement.valuePageHeaders,
              subTaskId);
    } else {
      success =
          compactionWriter.flushNonAlignedPage(
              pageElement.pageData, pageElement.pageHeader, subTaskId);
    }
    if (success) {
      // flush the page successfully, then remove this page
      checkShouldRemoveFile(pageElement);
    } else {
      // unsealed page is not large enough or page.endTime > file.endTime, then deserialze it
      summary.PAGE_NONE_OVERLAP_BUT_DESERIALIZE += 1;
      pointPriorityReader.addNewPage(pageElement);

      // write data points of the current page into chunk writer
      TimeValuePair point;
      while (pointPriorityReader.hasNext()) {
        point = pointPriorityReader.currentPoint();
        if (point.getTimestamp() > pageElement.pageHeader.getEndTime()) {
          // finish writing this page
          break;
        }
        compactionWriter.write(point, subTaskId);
        pointPriorityReader.next();
      }
    }
  }

  /**
   * Compact a series of pages that overlap with each other. Eg: The parameters are page 1 and page
   * 2, that is, page 1 only overlaps with page 2, while page 2 overlap with page 3, page 3 overlap
   * with page 4,and so on, there are 10 pages in total. This method will compact all 10 pages.
   * Pages in the candidate overlapped pages list will be sequentially judged whether there is a
   * real overlap, if so, it will be put into the point priority reader and deserialized; if not, it
   * means that the page is located in a gap inside another pages, and it can be directly flushed to
   * chunk writer. There will be new overlapped pages added into the list during the process of
   * compacting overlapped pages. Notice: for a real overlap page, it will be removed from candidate
   * list after it has been adding into point priority reader and deserializing. For a fake overlap
   * page, it will be removed from candidate list after it has been flushing to chunk writer
   * completely.
   */
  private void compactWithOverlapPages()
      throws IOException, PageException, WriteProcessException, IllegalPathException {
    while (pointPriorityReader.hasNext()) {
      TimeValuePair currentPoint = pointPriorityReader.currentPoint();
      long currentTime = currentPoint.getTimestamp();

      while (currentTime >= nextChunkStartTime || currentTime >= nextPageStartTime) {
        // current point overlaps with next chunk or next page, then deserialize next chunk if
        // necessary and get next page
        PageElement nextPageElement = getPageFromPageQueue(currentTime);
        // check whether next page is fake overlap or not
        checkAndCompactOverlapPage(nextPageElement, currentPoint);

        // get new current point
        currentPoint = pointPriorityReader.currentPoint();
        currentTime = currentPoint.getTimestamp();
      }

      // write data point into chunk writer
      compactionWriter.write(currentPoint, subTaskId);
      pointPriorityReader.next();
    }
  }

  /**
   * Check whether the page is true overlap or fake overlap. If a page is located in the gap of
   * another page, then this page is fake overlap, which can be flushed to chunk writer directly.
   * Otherwise, deserialize this page into point priority reader.
   */
  private void checkAndCompactOverlapPage(PageElement nextPageElement, TimeValuePair currentPoint)
      throws IOException, IllegalPathException, PageException, WriteProcessException {
    ModifiedStatus nextPageModifiedStatus = isPageModified(nextPageElement);

    if (nextPageModifiedStatus == ModifiedStatus.ALL_DELETED) {
      // all data on next page has been deleted, remove it
      checkShouldRemoveFile(nextPageElement);
    } else {
      // check is next page fake overlap (locates in the gap) or not
      boolean isNextPageOverlap =
          currentPoint.getTimestamp() <= nextPageElement.pageHeader.getEndTime()
              || nextPageElement.pageHeader.getEndTime() >= nextPageStartTime
              || nextPageElement.pageHeader.getEndTime() >= nextChunkStartTime;
      if (isNextPageOverlap || nextPageModifiedStatus == ModifiedStatus.PARTIAL_DELETED) {
        // next page is overlapped or modified, then deserialize it
        summary.PAGE_OVERLAP_OR_MODIFIED++;
        pointPriorityReader.addNewPage(nextPageElement);
      } else {
        // has none overlap or modified pages, flush it to chunk writer directly
        summary.PAGE_FAKE_OVERLAP += 1;
        compactWithNonOverlapPage(nextPageElement);
      }
    }
  }

  /**
   * Find overlapped files which have not been selected. Notice: We must ensure that the returned
   * list is ordered according to the startTime of the current device in the file from small to
   * large, so that each file can be compacted in order.
   */
  protected List<FileElement> findOverlapFiles(FileElement file) {
    List<FileElement> overlappedFiles = new ArrayList<>();
    long endTime = file.resource.getEndTime(deviceId);
    for (FileElement fileElement : fileList) {
      if (fileElement.resource.getStartTime(deviceId) <= endTime) {
        if (!fileElement.isSelected) {
          overlappedFiles.add(fileElement);
          fileElement.isSelected = true;
        }
      } else {
        break;
      }
    }
    return overlappedFiles;
  }

  /**
   * NONE_DELETED means that no data on this page has been deleted. <br>
   * PARTIAL_DELETED means that there is data on this page been deleted. <br>
   * ALL_DELETED means that all data on this page has been deleted.
   *
   * <p>Notice: If is aligned page, return ALL_DELETED if and only if all value pages are deleted.
   * Return NONE_DELETED if and only if no data exists on all value pages is deleted
   */
  protected abstract ModifiedStatus isPageModified(PageElement pageElement);

  protected ModifiedStatus checkIsModified(
      long startTime, long endTime, Collection<TimeRange> deletions) {
    ModifiedStatus status = ModifiedStatus.NONE_DELETED;
    if (deletions != null) {
      for (TimeRange range : deletions) {
        if (range.contains(startTime, endTime)) {
          // all data on this page or chunk has been deleted
          return ModifiedStatus.ALL_DELETED;
        }
        if (range.overlaps(new TimeRange(startTime, endTime))) {
          // exist data on this page or chunk been deleted
          status = ModifiedStatus.PARTIAL_DELETED;
        }
      }
    }
    return status;
  }

  /**
   * Check whether current page is overlap with next chunk which has not been read into memory yet
   * before getting one page. If it is, then read next chunk into memory and deserialize it into
   * pages.
   */
  private PageElement getPageFromPageQueue(long curTime) throws IOException {
    if (curTime >= nextChunkStartTime) {
      // overlap with next chunk, then read it into memory and deserialize it into page queue
      summary.CHUNK_OVERLAP_OR_MODIFIED++;
      ChunkMetadataElement chunkMetadataElement = chunkMetadataQueue.poll();
      nextChunkStartTime =
          chunkMetadataQueue.isEmpty() ? Long.MAX_VALUE : chunkMetadataQueue.peek().startTime;
      readChunk(chunkMetadataElement);
      deserializeChunkIntoPageQueue(chunkMetadataElement);
    }
    PageElement page = pageQueue.poll();
    nextPageStartTime = pageQueue.isEmpty() ? Long.MAX_VALUE : pageQueue.peek().startTime;
    return page;
  }

  /**
   * Check should remove file or not. If it is the last page in the chunk and the last chunk in the
   * file, then it means the file has been finished compacting and needs to be removed.
   */
  private void checkShouldRemoveFile(PageElement pageElement)
      throws IOException, IllegalPathException {
    if (pageElement.isLastPage && pageElement.chunkMetadataElement.isLastChunk) {
      // finish compacting the file, remove it from list
      removeFile(pageElement.chunkMetadataElement.fileElement);
    }
  }

  /**
   * Check if it is the last chunk in the file. If it is, it means the file has been finished
   * compacting and needs to be removed.
   */
  private void checkShouldRemoveFile(ChunkMetadataElement chunkMetadataElement)
      throws IOException, IllegalPathException {
    if (chunkMetadataElement.isLastChunk) {
      // finish compacting the file, remove it from list
      removeFile(chunkMetadataElement.fileElement);
    }
  }

  /**
   * Remove file from sorted list. If the file to be removed is the first file, we should re-find
   * new overlapped files with the first file in the current file list, deserialize them into chunk
   * metadatas and put them into chunk metadata queue.
   */
  protected void removeFile(FileElement fileElement) throws IllegalPathException, IOException {
    boolean isFirstFile = fileList.get(0).equals(fileElement);
    fileList.remove(fileElement);
    if (isFirstFile && !fileList.isEmpty()) {
      // find new overlapped files and deserialize them into chunk metadata queue
      List<FileElement> newOverlappedFiles = findOverlapFiles(fileList.get(0));
      deserializeFileIntoChunkMetadataQueue(newOverlappedFiles);
      nextChunkStartTime =
          chunkMetadataQueue.isEmpty() ? Long.MAX_VALUE : chunkMetadataQueue.peek().startTime;
    }
  }

  /**
   * Get the modifications of a timeseries in the ModificationFile of a TsFile.
   *
   * @param path name of the time series
   */
  protected List<Modification> getModificationsFromCache(
      TsFileResource tsFileResource, PartialPath path) {
    // copy from TsFileResource so queries are not affected
    List<Modification> modifications =
        modificationCacheMap.computeIfAbsent(
            tsFileResource, resource -> new ArrayList<>(resource.getModFile().getModifications()));
    List<Modification> pathModifications = new ArrayList<>();
    Iterator<Modification> modificationIterator = modifications.iterator();
    while (modificationIterator.hasNext()) {
      Modification modification = modificationIterator.next();
      if (modification.getPath().matchFullPath(path)) {
        pathModifications.add(modification);
      }
    }
    return pathModifications;
  }

  protected void updateSummary(ChunkMetadataElement chunkMetadataElement, ChunkStatus status) {
    switch (status) {
      case READ_IN:
        summary.increaseProcessChunkNum(
            isAligned
                ? ((AlignedChunkMetadata) chunkMetadataElement.chunkMetadata)
                        .getValueChunkMetadataList()
                        .size()
                    + 1
                : 1);
        if (isAligned) {
          for (IChunkMetadata valueChunkMetadata :
              ((AlignedChunkMetadata) chunkMetadataElement.chunkMetadata)
                  .getValueChunkMetadataList()) {
            if (valueChunkMetadata == null) {
              continue;
            }
            summary.increaseProcessPointNum(valueChunkMetadata.getStatistics().getCount());
          }
        } else {
          summary.increaseProcessPointNum(
              chunkMetadataElement.chunkMetadata.getStatistics().getCount());
        }
        break;
      case DIRECTORY_FLUSH:
        if (isAligned) {
          summary.increaseDirectlyFlushChunkNum(
              ((AlignedChunkMetadata) (chunkMetadataElement.chunkMetadata))
                      .getValueChunkMetadataList()
                      .size()
                  + 1);
        } else {
          summary.increaseDirectlyFlushChunkNum(1);
        }
        break;
      case DESERIALIZE_CHUNK:
        if (isAligned) {
          summary.increaseDeserializedChunkNum(
              ((AlignedChunkMetadata) (chunkMetadataElement.chunkMetadata))
                      .getValueChunkMetadataList()
                      .size()
                  + 1);
        } else {
          summary.increaseDeserializedChunkNum(1);
        }
        break;
    }
  }

  enum ChunkStatus {
    READ_IN,
    DIRECTORY_FLUSH,
    DESERIALIZE_CHUNK
  }
}
