package org.apache.iotdb.db.engine.compaction.cross.rewrite.task;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.engine.compaction.cross.utils.ChunkMetadataElement;
import org.apache.iotdb.db.engine.compaction.cross.utils.FileElement;
import org.apache.iotdb.db.engine.compaction.cross.utils.PageElement;
import org.apache.iotdb.db.engine.compaction.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.reader.PointPriorityReader;
import org.apache.iotdb.db.engine.compaction.writer.NewFastCrossCompactionWriter;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.reader.chunk.AlignedChunkReader;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
    void call(PageElement pageElement, List<PageElement> newOverlappedPages)
        throws WriteProcessException, IOException, IllegalPathException;
  }

  // sorted source files by the start time of device
  protected List<FileElement> fileList;

  protected final PriorityQueue<ChunkMetadataElement> chunkMetadataQueue;

  protected final PriorityQueue<PageElement> pageQueue;

  protected NewFastCrossCompactionWriter compactionWriter;

  protected FastCompactionPerformer fastCompactionPerformer;

  protected int subTaskId;

  // measurement -> tsfile resource -> timeseries metadata <startOffset, endOffset>
  protected Map<String, Map<TsFileResource, Pair<Long, Long>>> timeseriesMetadataOffsetMap;

  private final PointPriorityReader pointPriorityReader = new PointPriorityReader(this::removePage);

  private boolean isAligned;

  protected String deviceId;

  public FastCompactionPerformerSubTask(
      NewFastCrossCompactionWriter compactionWriter,
      FastCompactionPerformer fastCompactionPerformer,
      Map<String, Map<TsFileResource, Pair<Long, Long>>> timeseriesMetadataOffsetMap,
      String deviceId,
      boolean isAligned,
      int subTaskId) {
    this.compactionWriter = compactionWriter;
    this.fastCompactionPerformer = fastCompactionPerformer;
    this.subTaskId = subTaskId;
    this.timeseriesMetadataOffsetMap = timeseriesMetadataOffsetMap;
    this.isAligned = isAligned;
    this.deviceId = deviceId;

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

  /** Deserialize chunks and start compacting pages. */
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
      // chunk.endTime > file.endTime, then deserialize chunk
      compactWithOverlapChunks(Collections.singletonList(chunkMetadataElement));
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
      List<PageElement> overlappedPages = findOverlapPages(firstPageElement);
      boolean isPageOverlap = overlappedPages.size() > 1;
      int modifiedStatus = isPageModified(firstPageElement);

      switch (modifiedStatus) {
        case -1:
          // no data on this page has been deleted
          if (isPageOverlap) {
            // has overlap pages, deserialize it
            compactWithOverlapPages(overlappedPages);
          } else {
            // has none overlap pages,  flush it to chunk writer directly
            compactWithNonOverlapPage(firstPageElement);
          }
          break;
        case 0:
          // there is data on this page been deleted, deserialize it
          compactWithOverlapPages(overlappedPages);
          break;
        case 1:
          // all data on this page has been deleted, remove it
          removePage(firstPageElement, null);
          break;
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
      // flush the page successfully
      removePage(pageElement, null);
    } else {
      // page.endTime > file.endTime, then deserialze it
      List<PageElement> pageElements = new ArrayList<>();
      pageElements.add(pageElement);
      compactWithOverlapPages(pageElements);
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
    pointPriorityReader.setNewOverlappedPages(overlappedPages);
    while (pointPriorityReader.hasNext()) {
      for (int pageIndex = 0; pageIndex < overlappedPages.size(); pageIndex++) {
        PageElement nextPageElement = overlappedPages.get(pageIndex);

        // write currentPage.point.time < nextPage.startTime to chunk writer
        while (pointPriorityReader.currentPoint().left < nextPageElement.startTime) {
          // write data point to chunk writer
          compactionWriter.write(
              pointPriorityReader.currentPoint().left,
              pointPriorityReader.currentPoint().right,
              subTaskId);
          pointPriorityReader.next();
        }

        boolean isNextPageOverlap =
            !(pointPriorityReader.currentPoint().left > nextPageElement.pageHeader.getEndTime()
                && !isPageOverlap(nextPageElement));

        switch (isPageModified(nextPageElement)) {
          case -1:
            // no data on this page has been deleted
            if (isNextPageOverlap) {
              // has overlap with other pages, deserialize it
              pointPriorityReader.addNewPage(nextPageElement);
            } else {
              // has none overlap, flush it to chunk writer directly
              compactWithNonOverlapPage(nextPageElement);
            }
            break;
          case 0:
            // there is data on this page been deleted, deserialize it
            pointPriorityReader.addNewPage(nextPageElement);
            break;
          case 1:
            // all data on this page has been deleted, remove it
            removePage(nextPageElement, null);
            break;
        }
      }

      overlappedPages.clear();

      // write remaining data points
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

  /** Find overlaped pages which is not been selected with the specific page. */
  private List<PageElement> findOverlapPages(PageElement page) {
    List<PageElement> elements = new ArrayList<>();
    long endTime = page.pageHeader.getEndTime();
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

  /** Find overlaped chunks which is not been selected with the specific chunk. */
  private List<ChunkMetadataElement> findOverlapChunkMetadatas(ChunkMetadataElement chunkMetadata) {
    List<ChunkMetadataElement> elements = new ArrayList<>();
    long endTime = chunkMetadata.chunkMetadata.getEndTime();
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

  private void removePage(PageElement pageElement, List<PageElement> newOverlappedPages)
      throws IOException, IllegalPathException {
    // check is first page or not
    boolean isFirstPage = pageQueue.peek().equals(pageElement);
    pageQueue.remove(pageElement);
    if (pageElement.isLastPage) {
      // finish compacting the chunk, remove it from queue
      removeChunk(pageElement.chunkMetadataElement);
    }
    if (newOverlappedPages == null) {
      // uncessary to find new overlap pages with current top page, just return
      return;
    }
    if (pointPriorityReader.hasNext() && pageQueue.size() != 0) {
      // pointPriorityReader.hasNext() indicates that the new first page in page queue has not been
      // finished compacting yet, so there may be other pages overlap with it.
      // when deserializing new chunks into page queue or first page is removed from page queue, we
      // should find new overlapped pages and put them into list
      newOverlappedPages.addAll(findOverlapPages(pageQueue.peek()));
    }
  }

  private void removeChunk(ChunkMetadataElement chunkMetadataElement)
      throws IOException, IllegalPathException {

    chunkMetadataQueue.remove(chunkMetadataElement);
    if (chunkMetadataElement.isLastChunk) {
      // finish compacting the file, remove it from list
      removeFile(chunkMetadataElement.fileElement);
    }

    if (pageQueue.size() != 0 && chunkMetadataQueue.size() != 0) {
      // pageQueue.size > 0 indicates that the new first chunk in chunk metadata queue has not been
      // finished compacting yet, so there may be other chunks overlap with it.
      // when deserializing new files into chunk metadata queue or first chunk is removed from chunk
      // metadata queue, we should find new overlapped chunks and deserialize them into page queue
      for (ChunkMetadataElement newOverlappedChunkMetadata :
          findOverlapChunkMetadatas(chunkMetadataQueue.peek())) {
        deserializeChunkIntoQueue(newOverlappedChunkMetadata);
      }
    }
  }

  protected void removeFile(FileElement fileElement) throws IllegalPathException, IOException {
    boolean isFirstFile = fileList.get(0).equals(fileElement);
    fileList.remove(fileElement);
    if (isFirstFile && !fileList.isEmpty()) {
      // find new overlapped files and deserialize them into chunk metadata queue
      deserializeFileIntoQueue(findOverlapFiles(fileList.get(0)));
    }
  }
}
