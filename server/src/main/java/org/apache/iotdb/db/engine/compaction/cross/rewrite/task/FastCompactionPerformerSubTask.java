package org.apache.iotdb.db.engine.compaction.cross.rewrite.task;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.compaction.cross.utils.ChunkMetadataElement;
import org.apache.iotdb.db.engine.compaction.cross.utils.FileElement;
import org.apache.iotdb.db.engine.compaction.cross.utils.PageElement;
import org.apache.iotdb.db.engine.compaction.reader.PointPriorityReader;
import org.apache.iotdb.db.engine.compaction.writer.FastCrossCompactionWriter;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.reader.chunk.AlignedChunkReader;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

public abstract class FastCompactionPerformerSubTask implements Callable<Void> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);

  @FunctionalInterface
  public interface RemovePage {
    void call(PageElement pageElement)
        throws WriteProcessException, IOException, IllegalPathException;
  }

  // sorted source files by the start time of device
  protected List<FileElement> fileList;

  protected final PriorityQueue<ChunkMetadataElement> chunkMetadataQueue;

  protected final PriorityQueue<PageElement> pageQueue;

  protected FastCrossCompactionWriter compactionWriter;

  protected int subTaskId;

  // measurement -> tsfile resource -> timeseries metadata <startOffset, endOffset>
  // used to get the chunk metadatas from tsfile directly according to timeseries metadata offset.
  protected Map<String, Map<TsFileResource, Pair<Long, Long>>> timeseriesMetadataOffsetMap;

  protected Map<TsFileResource, TsFileSequenceReader> readerCacheMap;

  private final Map<TsFileResource, List<Modification>> modificationCacheMap;

  // source files which are sorted by the start time of current device from old to new. Notice: If
  // the type of timeIndex is FileTimeIndex, it may contain resources in which the current device
  // does not exist.
  protected List<TsFileResource> sortedSourceFiles;

  private final PointPriorityReader pointPriorityReader = new PointPriorityReader(this::removePage);

  private final boolean isAligned;

  protected String deviceId;

  public FastCompactionPerformerSubTask(
      FastCrossCompactionWriter compactionWriter,
      Map<String, Map<TsFileResource, Pair<Long, Long>>> timeseriesMetadataOffsetMap,
      Map<TsFileResource, TsFileSequenceReader> readerCacheMap,
      Map<TsFileResource, List<Modification>> modificationCacheMap,
      List<TsFileResource> sortedSourceFiles,
      String deviceId,
      boolean isAligned,
      int subTaskId) {
    this.compactionWriter = compactionWriter;
    this.subTaskId = subTaskId;
    this.timeseriesMetadataOffsetMap = timeseriesMetadataOffsetMap;
    this.isAligned = isAligned;
    this.deviceId = deviceId;
    this.readerCacheMap = readerCacheMap;
    this.modificationCacheMap = modificationCacheMap;
    this.sortedSourceFiles = sortedSourceFiles;

    this.fileList = new ArrayList<>();
    chunkMetadataQueue =
        new PriorityQueue<>(
            (o1, o2) -> {
              int timeCompare = Long.compare(o1.startTime, o2.startTime);
              return timeCompare != 0 ? timeCompare : Integer.compare(o2.priority, o1.priority);
            });

    pageQueue =
        new PriorityQueue<>(
            (o1, o2) -> {
              int timeCompare = Long.compare(o1.startTime, o2.startTime);
              return timeCompare != 0 ? timeCompare : Integer.compare(o2.priority, o1.priority);
            });
  }

  protected void compactFiles()
      throws PageException, IOException, WriteProcessException, IllegalPathException {
    while (!fileList.isEmpty()) {
      List<FileElement> overlappedFiles = findOverlapFiles(fileList.get(0));

      // read chunk metadatas from files and put them into chunk metadata queue
      deserializeFileIntoQueue(overlappedFiles);

      if (!isAligned) {
        // for nonAligned sensors, only after getting chunkMetadatas can we create schema to start
        // measurement; for aligned sensors, we get all schemas of value sensors and
        // startMeasurement() in the previous process, because we need to get all chunk metadatas of
        // sensors and their schemas under the current device, but since the compaction process is
        // to read a batch of overlapped files each time, which may not contain all the sensors.
        startMeasurement();
      }

      compactChunks();
    }
  }

  protected abstract void startMeasurement() throws IOException;

  /**
   * Compact chunks in chunk metadata queue.
   *
   * @throws IOException
   * @throws PageException
   */
  private void compactChunks()
      throws IOException, PageException, WriteProcessException, IllegalPathException {
    while (!chunkMetadataQueue.isEmpty()) {
      ChunkMetadataElement firstChunkMetadataElement = chunkMetadataQueue.peek();
      List<ChunkMetadataElement> overlappedChunkMetadatas =
          findOverlapChunkMetadatas(firstChunkMetadataElement);
      boolean isChunkOverlap = overlappedChunkMetadatas.size() > 1;
      boolean isModified = isChunkModified(firstChunkMetadataElement);

      if (isChunkOverlap || isModified) {
        // has overlap or modified chunk, then deserialize it
        compactWithOverlapChunks(overlappedChunkMetadatas);
      } else {
        // has none overlap or modified chunk, flush it to file writer directly
        compactWithNonOverlapChunks(firstChunkMetadataElement);
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
      deserializeChunkIntoQueue(overlappedChunkMetadata);
    }
    compactPages();
  }

  /**
   * Flush chunk to target file directly. If the end time of chunk exceeds the end time of file,
   * then deserialize it.
   */
  private void compactWithNonOverlapChunks(ChunkMetadataElement chunkMetadataElement)
      throws IOException, PageException, WriteProcessException, IllegalPathException {
    if (compactionWriter.flushChunkToFileWriter(chunkMetadataElement.chunkMetadata, subTaskId)) {
      // flush chunk successfully
      removeChunk(chunkMetadataQueue.peek());
    } else {
      // unsealed chunk is not large enough or chunk.endTime > file.endTime, then deserialize chunk
      deserializeChunkIntoQueue(chunkMetadataElement);
      compactPages();
    }
  }

  abstract void deserializeChunkIntoQueue(ChunkMetadataElement chunkMetadataElement)
      throws IOException;

  abstract void deserializeFileIntoQueue(List<FileElement> fileElements)
      throws IOException, IllegalPathException;

  /** Compact pages in page queue. */
  private void compactPages()
      throws IOException, PageException, WriteProcessException, IllegalPathException {
    while (!pageQueue.isEmpty()) {
      PageElement firstPageElement = pageQueue.peek();
      int modifiedStatus = isPageModified(firstPageElement);

      if (modifiedStatus == 1) {
        // all data on this page has been deleted, remove it
        removePage(firstPageElement);
        continue;
      }

      List<PageElement> overlappedPages = findOverlapPages(firstPageElement);
      boolean isPageOverlap = overlappedPages.size() > 1;

      if (isPageOverlap || modifiedStatus == 0) {
        // has overlap or modified pages, then deserialize it
        compactWithOverlapPages(overlappedPages);
      } else {
        // has none overlap or modified pages, flush it to chunk writer directly
        compactWithNonOverlapPage(firstPageElement);
      }
    }
  }

  private void compactWithNonOverlapPage(PageElement pageElement)
      throws PageException, IOException, WriteProcessException, IllegalPathException {
    boolean success;
    if (pageElement.iChunkReader instanceof AlignedChunkReader) {
      success =
          compactionWriter.flushAlignedPageToChunkWriter(
              pageElement.pageData,
              pageElement.pageHeader,
              pageElement.valuePageDatas,
              pageElement.valuePageHeaders,
              subTaskId);
    } else {
      success =
          compactionWriter.flushPageToChunkWriter(
              pageElement.pageData, pageElement.pageHeader, subTaskId);
    }
    if (success) {
      // flush the page successfully, then remove this page
      removePage(pageElement);
    } else {
      // unsealed page is not large enough or page.endTime > file.endTime, then deserialze it
      pointPriorityReader.addNewPage(pageElement);

      // write data points of the current page into chunk writer
      while (pointPriorityReader.hasNext()
          && pointPriorityReader.currentPoint().left <= pageElement.pageHeader.getEndTime()) {
        compactionWriter.write(
            pointPriorityReader.currentPoint().left,
            pointPriorityReader.currentPoint().right,
            subTaskId);
        pointPriorityReader.next();
      }
    }
  }

  /**
   * Compact a series of pages that overlap with each other. Eg: The parameters are page 1 and page
   * 2, that is, page 1 only overlaps with page 2, while page 2 overlap with page 3, page 3 overlap
   * with page 4,and so on, there are 10 pages in total. This method will merge all 10 pages.
   */
  private void compactWithOverlapPages(List<PageElement> overlappedPages)
      throws IOException, PageException, WriteProcessException, IllegalPathException {
    pointPriorityReader.addNewPage(overlappedPages.remove(0));
    pointPriorityReader.updateNewOverlappedPages(overlappedPages);
    while (pointPriorityReader.hasNext()) {
      // write point.time < the last overlapped page.startTime
      while (overlappedPages.size() > 0) {
        PageElement nextPageElement = overlappedPages.get(0);

        int oldSize = overlappedPages.size();
        // write currentPage.point.time < nextPage.startTime to chunk writer
        while (pointPriorityReader.currentPoint().left < nextPageElement.startTime) {
          // write data point to chunk writer
          compactionWriter.write(
              pointPriorityReader.currentPoint().left,
              pointPriorityReader.currentPoint().right,
              subTaskId);
          pointPriorityReader.next();
          if (overlappedPages.size() > oldSize) {
            // find the new overlapped pages, next page may be changed
            oldSize = overlappedPages.size();
            nextPageElement = overlappedPages.get(0);
          }
        }

        int nextPageModifiedStatus = isPageModified(nextPageElement);

        if (nextPageModifiedStatus == 1) {
          // all data on next page has been deleted, remove it
          removePage(nextPageElement);
        } else {
          boolean isNextPageOverlap =
              pointPriorityReader.currentPoint().left <= nextPageElement.pageHeader.getEndTime()
                  || isPageOverlap(nextPageElement);

          if (isNextPageOverlap || nextPageModifiedStatus == 0) {
            // has overlap or modified pages, then deserialize it
            pointPriorityReader.addNewPage(nextPageElement);
          } else {
            // has none overlap or modified pages, flush it to chunk writer directly
            compactWithNonOverlapPage(nextPageElement);
          }
        }
        overlappedPages.remove(0);
      }

      // write remaining data points, of which point.time >= the last overlapped page.startTime
      while (pointPriorityReader.hasNext()) {
        // write data point to chunk writer
        compactionWriter.write(
            pointPriorityReader.currentPoint().left,
            pointPriorityReader.currentPoint().right,
            subTaskId);
        pointPriorityReader.next();
        if (overlappedPages.size() > 0) {
          // finish compacting the first page and find the new overlapped pages, then start
          // compacting them with new first page
          break;
        }
      }
    }
  }

  /**
   * Find overlaped pages which has not been selected with the specific page. Notice: We must ensure
   * that the returned list is ordered according to the startTime of the page from small to large,
   * so that each page can be compacted in order.
   */
  private List<PageElement> findOverlapPages(PageElement page) {
    List<PageElement> elements = new ArrayList<>();
    long endTime = page.pageHeader.getEndTime();
    //    Iterator<PageElement> iterator = pageQueue.iterator();
    //    while (iterator.hasNext()) {
    //      PageElement element = iterator.next();
    //      if (element.startTime <= endTime) {
    //        if (!element.isOverlaped) {
    //          elements.add(element);
    //          element.isOverlaped = true;
    //        }
    //      }
    //    }

    Stream<PageElement> pages =
        pageQueue.stream().sorted(Comparator.comparingLong(o -> o.startTime));
    Iterator<PageElement> it = pages.iterator();
    while (it.hasNext()) {
      PageElement element = it.next();
      if (element.startTime <= endTime) {
        if (!element.isOverlaped) {
          elements.add(element);
          element.isOverlaped = true;
        }
      } else {
        break;
      }
    }
    return elements;
  }

  /**
   * Find overlapped chunks which has not been selected with the specific chunk. Notice: We must
   * ensure that the returned list is ordered according to the startTime of the chunk from small to
   * large, so that each chunk can be compacted in order.
   */
  private List<ChunkMetadataElement> findOverlapChunkMetadatas(ChunkMetadataElement chunkMetadata) {
    List<ChunkMetadataElement> elements = new ArrayList<>();
    long endTime = chunkMetadata.chunkMetadata.getEndTime();
    //    Iterator<ChunkMetadataElement> iterator = chunkMetadataQueue.iterator();
    //    while (iterator.hasNext()) {
    //      ChunkMetadataElement element = iterator.next();
    //      if (element.chunkMetadata.getStartTime() <= endTime) {
    //        if (!element.isOverlaped) {
    //          elements.add(element);
    //          element.isOverlaped = true;
    //        }
    //      }
    //    }

    Stream<ChunkMetadataElement> chunks =
        chunkMetadataQueue.stream().sorted(Comparator.comparingLong(o -> o.startTime));
    Iterator<ChunkMetadataElement> it = chunks.iterator();
    while (it.hasNext()) {
      ChunkMetadataElement element = it.next();
      if (element.chunkMetadata.getStartTime() <= endTime) {
        if (!element.isOverlaped) {
          elements.add(element);
          element.isOverlaped = true;
        }
      } else {
        break;
      }
    }
    return elements;
  }

  private List<FileElement> findOverlapFiles(FileElement file) {
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

  private boolean isChunkOverlap(ChunkMetadataElement chunkMetadataElement) {
    return false;
  }

  /** Check is the page overlap with other pages later then the specific page in queue or not. */
  private boolean isPageOverlap(PageElement pageElement) {
    long endTime = pageElement.pageHeader.getEndTime();
    Stream<PageElement> pages =
        pageQueue.stream().sorted(Comparator.comparingLong(o -> o.startTime));
    Iterator<PageElement> it = pages.iterator();
    while (it.hasNext()) {
      PageElement element = it.next();
      // only check pages later then the specific page
      if (element.equals(pageElement) || element.startTime < pageElement.startTime) {
        continue;
      }
      return element.startTime <= endTime;
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
   * -1 means that no data on this page has been deleted. <br>
   * 0 means that there is data on this page been deleted. <br>
   * 1 means that all data on this page has been deleted.
   *
   * <p>Notice: If is aligned page, return 1 if and only if all value pages are deleted. Return -1
   * if and only if no data exists on all value pages is deleted
   */
  protected abstract int isPageModified(PageElement pageElement);

  protected int checkIsModified(long startTime, long endTime, Collection<TimeRange> deletions) {
    int status = -1;
    if (deletions != null) {
      for (TimeRange range : deletions) {
        if (range.contains(startTime, endTime)) {
          // all data on this page or chunk has been deleted
          return 1;
        }
        if (range.overlaps(new TimeRange(startTime, endTime))) {
          // exist data on this page or chunk been deleted
          status = 0;
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
   * their pages are put into the queue, it is necessary to re-find all pages that overlap with the
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
        && pointPriorityReader.hasNext()
        && pageQueue.size() != 0) {
      // pointPriorityReader.hasNext() indicates that the new first page in page queue has not been
      // finished compacting yet, so there may be other pages overlap with it.
      // when deserializing new chunks into page queue or first page is removed from page queue, we
      // should find new overlapped pages and put them into list}
      pointPriorityReader.getNewOverlappedPages().addAll(findOverlapPages(pageQueue.peek()));
      // we should ensure that the list is ordered according to the startTime of the page from small
      // to large, so that each page can be compacted in order
      pointPriorityReader.getNewOverlappedPages().sort(Comparator.comparingLong(o -> o.startTime));
    }
  }

  private void insertList(List<PageElement> oldSortedList, List<PageElement> newSortedList) {
    int startIndex = 0;
    int endIndex = oldSortedList.size() - 1;
    for (PageElement pageElement : newSortedList) {
      while (startIndex + 1 != endIndex) {
        int pos = (startIndex + endIndex) / 2;
        if (oldSortedList.get(pos).startTime <= pageElement.startTime) {
          startIndex = pos;
        } else {
          endIndex = pos;
        }
      }
      // find the position to insert
      oldSortedList.add(startIndex + 1, pageElement);
      startIndex++;
      endIndex = oldSortedList.size();
    }
  }

  /**
   * Remove chunk metadata from chunk metadata queue. If the chunk metadata to be removed is the
   * last chunk of file, it means this file has been compacted completely, we should remove this
   * file. When removing file, there may be new overlapped files being deserialized and their chunk
   * metadatas are put into chunk metadata queue. Therefore, when the removed chunk is the first
   * chunk or when new files are deserialized and their chunk metadatas are put into the queue, it
   * is necessary to re-find all chunk metadatas that overlap with the first chunk metadata in the
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
