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
package org.apache.iotdb.db.engine.compaction.cross.utils;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.compaction.reader.PointPriorityReader;
import org.apache.iotdb.db.engine.compaction.task.SubCompactionTaskSummary;
import org.apache.iotdb.db.engine.compaction.writer.AbstractCompactionWriter;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.reader.chunk.AlignedChunkReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
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

  private final SubCompactionTaskSummary summary;

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

  private final PointPriorityReader pointPriorityReader = new PointPriorityReader(this::removePage);

  protected String deviceId;

  // Pages in this list will be sequentially judged whether there is a real overlap to choose
  // whether to put them in the point priority reader to deserialize or directly flush to chunk
  // writer. During the process of compacting overlapped page, there may be new overlapped pages
  // added into this list.
  private final List<PageElement> candidateOverlappedPages = new ArrayList<>();

  public SeriesCompactionExecutor(
      AbstractCompactionWriter compactionWriter,
      Map<TsFileResource, TsFileSequenceReader> readerCacheMap,
      Map<TsFileResource, List<Modification>> modificationCacheMap,
      String deviceId,
      int subTaskId,
      SubCompactionTaskSummary summary) {
    this.compactionWriter = compactionWriter;
    this.subTaskId = subTaskId;
    this.deviceId = deviceId;
    this.readerCacheMap = readerCacheMap;
    this.modificationCacheMap = modificationCacheMap;
    this.summary = summary;

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

  public abstract void excute()
      throws PageException, IllegalPathException, IOException, WriteProcessException;

  protected abstract void compactFiles()
      throws PageException, IOException, WriteProcessException, IllegalPathException;

  /** Compact chunks in chunk metadata queue. */
  protected void compactChunks()
      throws IOException, PageException, WriteProcessException, IllegalPathException {
    while (!chunkMetadataQueue.isEmpty()) {
      ChunkMetadataElement firstChunkMetadataElement = chunkMetadataQueue.peek();
      List<ChunkMetadataElement> overlappedChunkMetadatas =
          findOverlapChunkMetadatas(firstChunkMetadataElement);
      boolean isChunkOverlap = overlappedChunkMetadatas.size() > 1;
      boolean isModified = isChunkModified(firstChunkMetadataElement);

      if (isChunkOverlap || isModified) {
        // has overlap or modified chunk, then deserialize it
        summary.CHUNK_OVERLAP_OR_MODIFIED += overlappedChunkMetadatas.size();
        compactWithOverlapChunks(overlappedChunkMetadatas);
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
  private void compactWithOverlapChunks(List<ChunkMetadataElement> overlappedChunkMetadatas)
      throws IOException, PageException, WriteProcessException, IllegalPathException {
    for (ChunkMetadataElement overlappedChunkMetadata : overlappedChunkMetadatas) {
      readChunk(overlappedChunkMetadata);
      deserializeChunkIntoQueue(overlappedChunkMetadata);
    }
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
    if (chunkMetadataElement.chunkMetadata instanceof AlignedChunkMetadata) {
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
      removeChunk(chunkMetadataQueue.peek());
    } else {
      // unsealed chunk is not large enough or chunk.endTime > file.endTime, then deserialize chunk
      summary.CHUNK_NONE_OVERLAP_BUT_DESERIALIZE += 1;
      deserializeChunkIntoQueue(chunkMetadataElement);
      compactPages();
    }
  }

  abstract void deserializeChunkIntoQueue(ChunkMetadataElement chunkMetadataElement)
      throws IOException;

  abstract void readChunk(ChunkMetadataElement chunkMetadataElement) throws IOException;

  /** Deserialize files into chunk metadatas and put them into the chunk metadata queue. */
  abstract void deserializeFileIntoQueue(List<FileElement> fileElements)
      throws IOException, IllegalPathException;

  /** Compact pages in page queue. */
  private void compactPages()
      throws IOException, PageException, WriteProcessException, IllegalPathException {
    while (!pageQueue.isEmpty()) {
      PageElement firstPageElement = pageQueue.peek();
      ModifiedStatus modifiedStatus = isPageModified(firstPageElement);

      if (modifiedStatus == ModifiedStatus.ALL_DELETED) {
        // all data on this page has been deleted, remove it
        removePage(firstPageElement);
        continue;
      }

      List<PageElement> overlapPages = findOverlapPages(firstPageElement);
      boolean isPageOverlap = overlapPages.size() > 1;

      if (isPageOverlap || modifiedStatus == ModifiedStatus.PARTIAL_DELETED) {
        // has overlap or modified pages, then deserialize it
        summary.PAGE_OVERLAP_OR_MODIFIED += 1;
        pointPriorityReader.addNewPage(overlapPages.remove(0));
        addOverlappedPagesIntoList(overlapPages);
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
    if (pageElement.iChunkReader instanceof AlignedChunkReader) {
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
      removePage(pageElement);
    } else {
      // unsealed page is not large enough or page.endTime > file.endTime, then deserialze it
      summary.PAGE_NONE_OVERLAP_BUT_DESERIALIZE += 1;
      pointPriorityReader.addNewPage(pageElement);

      // write data points of the current page into chunk writer
      while (pointPriorityReader.hasNext()
          && pointPriorityReader.currentPoint().getTimestamp()
              <= pageElement.pageHeader.getEndTime()) {
        compactionWriter.write(pointPriorityReader.currentPoint(), subTaskId);
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
    checkAndCompactOverlappePages();

    // write remaining data points, of which point.time >= the last overlapped page.startTime
    while (pointPriorityReader.hasNext()) {
      // write data point to chunk writer

      compactionWriter.write(pointPriorityReader.currentPoint(), subTaskId);
      pointPriorityReader.next();
      if (candidateOverlappedPages.size() > 0) {
        // finish compacting the first page or there are new chunks being deserialized and find
        // the new overlapped pages, then start compacting them
        checkAndCompactOverlappePages();
      }
    }
  }

  /**
   * Check whether the page is true overlap or fake overlap. If a page is located in the gap of
   * another page, then this page is fake overlap, which can be flushed to chunk writer directly.
   * Otherwise, deserialize this page into point priority reader.
   */
  private void checkAndCompactOverlappePages()
      throws IllegalPathException, IOException, WriteProcessException, PageException {
    // write point.time < the last overlapped page.startTime
    while (candidateOverlappedPages.size() > 0) {
      PageElement nextPageElement = candidateOverlappedPages.get(0);

      int oldSize = candidateOverlappedPages.size();
      // write currentPage.point.time < nextPage.startTime to chunk writer
      while (pointPriorityReader.hasNext()
          && pointPriorityReader.currentPoint().getTimestamp() < nextPageElement.startTime) {
        // write data point to chunk writer
        compactionWriter.write(pointPriorityReader.currentPoint(), subTaskId);
        pointPriorityReader.next();
        if (candidateOverlappedPages.size() > oldSize) {
          // during the process of writing overlapped points, if the first page is compacted
          // completely or a new chunk is deserialized, there may be new pages overlapped with the
          // first page in page queue which are added into the list. If so, the next overlapped
          // page in the list may be changed, so we should re-get next overlap page here.
          oldSize = candidateOverlappedPages.size();
          nextPageElement = candidateOverlappedPages.get(0);
        }
      }

      ModifiedStatus nextPageModifiedStatus = isPageModified(nextPageElement);

      if (nextPageModifiedStatus == ModifiedStatus.ALL_DELETED) {
        // all data on next page has been deleted, remove it
        removePage(nextPageElement);
      } else {
        boolean isNextPageOverlap =
            (pointPriorityReader.hasNext()
                    && pointPriorityReader.currentPoint().getTimestamp()
                        <= nextPageElement.pageHeader.getEndTime())
                || isPageOverlap(nextPageElement);

        if (isNextPageOverlap || nextPageModifiedStatus == ModifiedStatus.PARTIAL_DELETED) {
          // has overlap or modified pages, then deserialize it
          pointPriorityReader.addNewPage(nextPageElement);
        } else {
          // has none overlap or modified pages, flush it to chunk writer directly
          summary.PAGE_FAKE_OVERLAP += 1;
          compactWithNonOverlapPage(nextPageElement);
        }
      }
      candidateOverlappedPages.remove(0);
    }
  }

  /**
   * Add the new overlapped pages into the global list and sort it according to the startTime of the
   * page from small to large, so that each page can be compacted in order. If the page has been
   * deleted completely, we remove it.
   */
  private void addOverlappedPagesIntoList(List<PageElement> newOverlappedPages) {
    summary.PAGE_OVERLAP_OR_MODIFIED += newOverlappedPages.size();
    int oldSize = candidateOverlappedPages.size();
    candidateOverlappedPages.addAll(newOverlappedPages);
    if (oldSize != 0 && candidateOverlappedPages.size() > oldSize) {
      // if there is no pages in the overlappedPages, then we don't need to sort it after adding the
      // new overlapped pages, because newOverlappedPages is already sorted. When there is pages in
      // list before and there is new pages added into list, then we need to sort it again.
      // we should ensure that the list is ordered according to the startTime of the page from small
      // to large, so that each page can be compacted in order
      candidateOverlappedPages.sort(Comparator.comparingLong(o -> o.startTime));
    }
  }

  /**
   * Find overlaped pages which have not been selected. Notice: We must ensure that the returned
   * list is ordered according to the startTime of the page from small to large, so that each page
   * can be compacted in order.
   */
  private List<PageElement> findOverlapPages(PageElement page) {
    List<PageElement> elements = new ArrayList<>();
    long endTime = page.pageHeader.getEndTime();
    for (PageElement element : pageQueue) {
      if (element.startTime <= endTime) {
        if (!element.isOverlaped) {
          elements.add(element);
          element.isOverlaped = true;
        }
      }
    }
    elements.sort(Comparator.comparingLong(o -> o.startTime));
    return elements;
  }

  /**
   * Find overlapped chunks which have not been selected. Notice: We must ensure that the returned
   * list is ordered according to the startTime of the chunk from small to large, so that each chunk
   * can be compacted in order.
   */
  private List<ChunkMetadataElement> findOverlapChunkMetadatas(
      ChunkMetadataElement chunkMetadataElement) {
    List<ChunkMetadataElement> elements = new ArrayList<>();
    long endTime = chunkMetadataElement.chunkMetadata.getEndTime();
    for (ChunkMetadataElement element : chunkMetadataQueue) {
      if (element.chunkMetadata.getStartTime() <= endTime) {
        if (!element.isOverlaped) {
          elements.add(element);
          element.isOverlaped = true;
        }
      }
    }
    elements.sort(Comparator.comparingLong(o -> o.startTime));
    return elements;
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
        if (!fileElement.isOverlap) {
          overlappedFiles.add(fileElement);
          fileElement.isOverlap = true;
        }
      } else {
        break;
      }
    }
    return overlappedFiles;
  }

  /** Check is the page overlap with other pages later then the specific page in queue or not. */
  private boolean isPageOverlap(PageElement pageElement) {
    long endTime = pageElement.pageHeader.getEndTime();
    long startTime = pageElement.startTime;
    for (PageElement element : pageQueue) {
      if (element.equals(pageElement)) {
        continue;
      }
      // only check pages later than the specific page
      if (element.startTime >= startTime && element.startTime <= endTime) {
        return true;
      }
    }
    return false;
  }

  /**
   * Check whether the chunk is modified.
   *
   * <p>Notice: if is aligned chunk, return true if any of value chunk has data been deleted. Return
   * false if and only if all value chunks has no data been deleted.
   */
  protected boolean isChunkModified(ChunkMetadataElement chunkMetadataElement) {
    return chunkMetadataElement.chunkMetadata.isModified();
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
   * Remove the page from page queue. If the page to be removed is the last page of chunk, it means
   * this chunk has been compacted completely, we should remove this chunk. When removing chunks,
   * there may be new overlapped chunks being deserialized and their pages are put into pageQueue.
   * Therefore, when the removed page is the first page or when new chunks are deserialized and
   * their pages are put into the queue, it is necessary to re-find new pages that overlap with the
   * first page in the current queue, and put them put into list.
   */
  private void removePage(PageElement pageElement) throws IOException, IllegalPathException {
    boolean isFirstPage = pageQueue.peek().equals(pageElement);
    boolean hasNewOverlappedChunks = false;
    pageQueue.remove(pageElement);
    if (pageElement.isLastPage) {
      // finish compacting the chunk, remove it from queue
      hasNewOverlappedChunks = removeChunk(pageElement.chunkMetadataElement);
    }

    if ((isFirstPage || hasNewOverlappedChunks)
        && (pointPriorityReader.hasNext() || candidateOverlappedPages.size() > 0)
        && pageQueue.size() != 0) {
      // During the procession of compacting overlapped pages, when deserializing new chunks into
      // page queue or first page is removed from page queue, we should find new overlapped pages
      // and put them into list.
      // If (candidateOverlappedPages.size() > 0 || pointPriorityReader.hasNext()) is true, it
      // indicates that it is still in the process of compacting overlapped pages.
      // pointPriorityReader.hasNext() indicates that the new first page in page queue has not been
      // finished compacting yet,  so there may be other pages overlap with it.
      // Due to modifications of this page, the new first page in page queue may not been
      // deserialized into pointPriorityReader yet, but it is still in the candidateOverlappedPages
      // list, so there may be other pages overlap with it.
      addOverlappedPagesIntoList(findOverlapPages(pageQueue.peek()));
    }
  }

  /**
   * Remove chunk metadata from chunk metadata queue. If the chunk metadata to be removed is the
   * last chunk of file, it means this file has been compacted completely, we should remove this
   * file. When removing file, there may be new overlapped files being deserialized and their chunk
   * metadatas are put into chunk metadata queue. Therefore, when the removed chunk is the first
   * chunk or when new files are deserialized and their chunk metadatas are put into the queue, it
   * is necessary to re-find new chunk metadatas that overlap with the first chunk metadata in the
   * current queue, deserialize them into pages and put them into page queue.
   *
   * @return has new overlapped chunks or not
   */
  private boolean removeChunk(ChunkMetadataElement chunkMetadataElement)
      throws IOException, IllegalPathException {
    boolean hasNewOverlappedChunks = false;
    boolean isFirstChunk = chunkMetadataQueue.peek().equals(chunkMetadataElement);
    boolean hasNewOverlappedFiles = false;
    chunkMetadataQueue.remove(chunkMetadataElement);
    if (chunkMetadataElement.isLastChunk) {
      // finish compacting the file, remove it from list
      hasNewOverlappedFiles = removeFile(chunkMetadataElement.fileElement);
    }

    if ((isFirstChunk || hasNewOverlappedFiles)
        && pageQueue.size() != 0
        && chunkMetadataQueue.size() != 0) {
      // pageQueue.size > 0 indicates that the new first chunk in chunk metadata queue has not been
      // finished compacting yet, so there may be other chunks overlap with it.
      // when deserializing new files into chunk metadata queue or first chunk is removed from chunk
      // metadata queue, we should find new overlapped chunks and deserialize them into page queue
      for (ChunkMetadataElement newOverlappedChunkMetadata :
          findOverlapChunkMetadatas(chunkMetadataQueue.peek())) {
        summary.CHUNK_OVERLAP_OR_MODIFIED++;
        readChunk(newOverlappedChunkMetadata);
        deserializeChunkIntoQueue(newOverlappedChunkMetadata);
        hasNewOverlappedChunks = true;
      }
    }
    return hasNewOverlappedChunks;
  }

  /**
   * Remove file from sorted list. If the file to be removed is the first file, we should re-find
   * new overlapped files with the first file in the current file list, deserialize them into chunk
   * metadatas and put them into chunk metadata queue.
   *
   * @return has new overlapped files or not
   */
  protected boolean removeFile(FileElement fileElement) throws IllegalPathException, IOException {
    boolean hasNewOverlappedFiles = false;
    boolean isFirstFile = fileList.get(0).equals(fileElement);
    fileList.remove(fileElement);
    if (isFirstFile && !fileList.isEmpty()) {
      // find new overlapped files and deserialize them into chunk metadata queue
      List<FileElement> newOverlappedFiles = findOverlapFiles(fileList.get(0));
      deserializeFileIntoQueue(newOverlappedFiles);
      hasNewOverlappedFiles = newOverlappedFiles.size() > 0;
    }
    return hasNewOverlappedFiles;
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
}
