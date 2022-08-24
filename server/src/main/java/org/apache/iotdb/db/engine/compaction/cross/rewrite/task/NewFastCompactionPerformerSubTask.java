package org.apache.iotdb.db.engine.compaction.cross.rewrite.task;

import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.compaction.cross.utils.ChunkMetadataElement;
import org.apache.iotdb.db.engine.compaction.cross.utils.PageElement;
import org.apache.iotdb.db.engine.compaction.cross.utils.PointDataElement;
import org.apache.iotdb.db.engine.compaction.writer.NewFastCrossCompactionWriter;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;

public class NewFastCompactionPerformerSubTask implements Callable<Void> {

  @FunctionalInterface
  public interface RemovePage {
    void call(PageElement pageElement) throws WriteProcessException;
  }

  private final PriorityQueue<ChunkMetadataElement> chunkMetadataQueue;

  private final PriorityQueue<PageElement> pageQueue;

  private final PriorityQueue<PointDataElement> pointDataQueue;

  private boolean isFirstPageEnd = false;

  private final NewFastCrossCompactionWriter compactionWriter;

  private final int subTaskId;

  // all measurements id of this device
  private final List<String> allMeasurements;

  // the indexs of the timseries to be compacted to which the current sub thread is assigned
  private final List<Integer> pathsIndex;

  // chunk metadata list of all timeseries of this device in this seq file
  private final List<List<ChunkMetadata>> allSensorMetadatas;

  private final List<MeasurementSchema> measurementSchemas;

  private int pagePriority = 0;

  public NewFastCompactionPerformerSubTask(
      List<String> allMeasurements,
      List<Integer> pathsIndex,
      List<List<ChunkMetadata>> allSensorMetadatas,
      List<MeasurementSchema> measurementSchemas,
      NewFastCrossCompactionWriter compactionWriter,
      int subTaskId) {
    this.compactionWriter = compactionWriter;
    this.subTaskId = subTaskId;
    this.allMeasurements = allMeasurements;
    this.measurementSchemas = measurementSchemas;
    this.pathsIndex = pathsIndex;
    this.allSensorMetadatas = allSensorMetadatas;
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

    pointDataQueue =
        new PriorityQueue<>(
            (o1, o2) -> {
              int timeCompare = Long.compare(o1.curTimestamp, o2.curTimestamp);
              return timeCompare != 0 ? timeCompare : Integer.compare(o2.priority, o1.priority);
            });
  }

  @Override
  public Void call() throws IOException, PageException {
    for (Integer index : pathsIndex) {
      if (allSensorMetadatas.get(index).isEmpty()) {
        continue;
      }
      int chunkMetadataPriority = 0;
      for (ChunkMetadata chunkMetadata : allSensorMetadatas.get(index)) {
        chunkMetadataQueue.add(new ChunkMetadataElement(chunkMetadata, chunkMetadataPriority++));
      }
      chunkMetadataQueue.peek().isFirstChunk = true;

      compactionWriter.startMeasurement(
          Collections.singletonList(measurementSchemas.get(index)), subTaskId);

      while (!chunkMetadataQueue.isEmpty()) {
        ChunkMetadataElement firstChunkMetadataElement = chunkMetadataQueue.peek();
        List<ChunkMetadataElement> overlapedChunkMetadatas =
            findOverlapChunkMetadatas(firstChunkMetadataElement);
        if (overlapedChunkMetadatas.isEmpty()) {
          // has none overlap chunks, flush it directly to tsfile writer
          compactionWriter.flushChunkToFileWriter(
              firstChunkMetadataElement.chunkMetadata, subTaskId);
          chunkMetadataQueue.poll();
        } else {
          // deserialize chunks
          deserializeChunkIntoQueue(firstChunkMetadataElement);
          for (ChunkMetadataElement overlapChunkMetadata : overlapedChunkMetadatas) {
            deserializeChunkIntoQueue(overlapChunkMetadata);
          }
          compactWithPages();
        }
      }
      compactionWriter.endMeasurement(subTaskId);
    }
    return null;
  }

  //  private void compactWithOverlapChunks(
  //      List<PageElement> pageElementsToAddIntoQueue,
  //      List<ChunkMetadataElement> overlappedChunks,
  //      int pagePriority)
  //      throws IOException, PageException {
  //    while (!overlappedChunks.isEmpty()) {
  //      ChunkMetadataElement nextChunkMetadataElement = overlappedChunks.remove(0);
  //      // put page.endTime < next chunk.startTime into page queue
  //      while (pageElementsToAddIntoQueue.get(0).pageHeader.getEndTime()
  //          < nextChunkMetadataElement.startTime) {
  //        pageQueue.add(pageElementsToAddIntoQueue.remove(0));
  //      }
  //
  //      if (pageElementsToAddIntoQueue.get(0).startTime
  //          > nextChunkMetadataElement.chunkMetadata.getEndTime()) {
  //        // has none overlap data, flush chunk to tsfile writer directly
  //        compactWithPages();
  //        compactionWriter.flushChunkToFileWriter(nextChunkMetadataElement.chunkMetadata,
  // subTaskId);
  //      } else {
  //        // has overlap data, then deserialize next chunk and put page elements to queue
  //        pageElementsToAddIntoQueue.addAll(
  //            deserializeChunk(nextChunkMetadataElement));
  //      }
  //    }
  //  }

  private void deserializeChunkIntoQueue(ChunkMetadataElement chunkMetadataElement)
      throws IOException {
    Chunk chunk = ChunkCache.getInstance().get(chunkMetadataElement.chunkMetadata);
    ChunkReader chunkReader = new ChunkReader(chunk);
    ByteBuffer chunkDataBuffer = chunk.getData();
    ChunkHeader chunkHeader = chunk.getHeader();
    while (chunkDataBuffer.remaining() > 0) {
      // deserialize a PageHeader from chunkDataBuffer
      PageHeader pageHeader;
      if (((byte) (chunkHeader.getChunkType() & 0x3F)) == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER) {
        pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunk.getChunkStatistic());
      } else {
        pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunkHeader.getDataType());
      }
      ByteBuffer compressedPageData = chunkReader.readPageDataWithoutUncompressing(pageHeader);

      boolean isLastPage = chunkDataBuffer.remaining() <= 0;
      pageQueue.add(
          new PageElement(
              pageHeader,
              compressedPageData,
              chunkReader,
              chunkMetadataElement,
              isLastPage,
              pagePriority++));
    }
  }

  private void compactWithPages() throws IOException, PageException {
    while (!pageQueue.isEmpty()) {
      PageElement firstPageElement = pageQueue.peek();
      List<PageElement> overlappedPages = findOverlapPages(firstPageElement);
      if (overlappedPages.isEmpty()) {
        // has none overlap pages, flush it directly to chunk writer
        compactionWriter.flushPageToChunkWriter(
            firstPageElement.pageData, firstPageElement.pageHeader, subTaskId);
        pageQueue.poll();
        checkIsFirstChunkEnd(firstPageElement);
      } else {
        // deserialize first page into data point queue
        PointDataElement firstPageDataPointElement =
            new PointDataElement(
                firstPageElement.chunkReader.readPageData(
                    firstPageElement.pageHeader, firstPageElement.pageData),
                firstPageElement,
                firstPageElement.priority);
        firstPageDataPointElement.isFirstPage = true;
        pointDataQueue.add(firstPageDataPointElement);

        // compact the overlapped pages
        while (!pointDataQueue.isEmpty()) {
          compactWithOverlapPages(overlappedPages);
          // write remaining data points of these overlapped pages and get new overlapped pages if
          // finish compacting the first page
          overlappedPages = writeRemainingPoints();
        }
      }
    }
  }

  /**
   * Compact a series of pages that overlap with each other. Eg: The parameters are page 1 and page
   * 2, that is, page 1 only overlaps with page 2, while page 2 overlap with page 3, page 3 overlap
   * with page 4,and so on, there are 10 pages in total. This method will merge all 10 pages.
   */
  private void compactWithOverlapPages(List<PageElement> overlappedPages)
      throws IOException, PageException {
    while (!overlappedPages.isEmpty()) {
      PageElement nextPageElement = overlappedPages.remove(0);
      // write current page point.time < next page point.time
      while (currentDataPoint().getTimestamp() < nextPageElement.startTime) {
        // write data point to chunk writer
        TimeValuePair timeValuePair = nextDataPoint();
        compactionWriter.writeTimeValue(timeValuePair, subTaskId);
      }

      if (currentDataPoint().getTimestamp() > nextPageElement.pageHeader.getEndTime()
          && !isPageOverlap(nextPageElement)) {
        // has none overlap data, flush next page to chunk writer directly
        compactionWriter.flushPageToChunkWriter(
            nextPageElement.pageData, nextPageElement.pageHeader, subTaskId);
      } else {
        // has overlap data, then deserialize next page to dataPointQueue
        BatchData batchData =
            nextPageElement.chunkReader.readPageData(
                nextPageElement.pageHeader, nextPageElement.pageData);
        pointDataQueue.add(
            new PointDataElement(batchData, nextPageElement, nextPageElement.priority));
      }
    }
  }

  private List<PageElement> writeRemainingPoints() throws IOException {
    while (!pointDataQueue.isEmpty()) {
      // write data point to chunk writer
      TimeValuePair timeValuePair = nextDataPoint();
      compactionWriter.writeTimeValue(timeValuePair, subTaskId);

      // if finishing writing the first page, then start compacting the next page with its
      // overlapped pages
      if (isFirstPageEnd && !pageQueue.isEmpty()) {
        List<PageElement> newOverlappedPages = findOverlapPages(pageQueue.peek());
        isFirstPageEnd = false;
        if (!newOverlappedPages.isEmpty()) {
          // If there are new pages that overlap with the current firstPage, then exit the loop
          return newOverlappedPages;
        }
      }
    }
    return Collections.emptyList();
  }

  private TimeValuePair currentDataPoint() {
    return pointDataQueue.peek().curTimeValuePair;
  }

  /**
   * Read next data point from pointDataQueue.
   *
   * @return
   * @throws IOException
   */
  private TimeValuePair nextDataPoint() throws IOException {
    PointDataElement element = pointDataQueue.peek();
    TimeValuePair timeValuePair = element.curTimeValuePair;
    if (element.hasNext()) {
      element.next();
    } else {
      // has no data left in the page
      // remove it from point queue
      element = pointDataQueue.poll();
      checkIsFirstPageEnd(element);
      // remove it from page queue
      pageQueue.remove(element.pageElement);
      checkIsFirstChunkEnd(element.pageElement);
    }
    long lastTime = timeValuePair.getTimestamp();

    // remove data points with the same timestamp as the last point
    while (!pointDataQueue.isEmpty() && pointDataQueue.peek().curTimestamp <= lastTime) {
      element = pointDataQueue.peek();
      if (element.hasNext()) {
        element.next();
      } else {
        // has no data left in the page
        // remove it from point queue
        element = pointDataQueue.poll();
        checkIsFirstPageEnd(element);
        // remove it from page queue
        pageQueue.remove(element.pageElement);
        checkIsFirstChunkEnd(element.pageElement);
      }
    }
    return timeValuePair;
  }

  /**
   * Find overlaped pages which is not been selected with the specific page.
   *
   * @param page
   * @return
   */
  private List<PageElement> findOverlapPages(PageElement page) {
    List<PageElement> elements = new ArrayList<>();
    long endTime = page.pageHeader.getEndTime();
    for (PageElement element : pageQueue) {
      if (element.equals(page)) {
        continue;
      }
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

  private List<ChunkMetadataElement> findOverlapChunkMetadatas(ChunkMetadataElement chunkMetadata) {
    List<ChunkMetadataElement> elements = new ArrayList<>();
    long endTime = chunkMetadata.chunkMetadata.getEndTime();
    for (ChunkMetadataElement element : chunkMetadataQueue) {
      if (element.equals(chunkMetadata)) {
        continue;
      }
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

  private void checkIsFirstPageEnd(PointDataElement pointDataElement) {
    if (pointDataElement.isFirstPage) {
      if (!pointDataQueue.isEmpty()) {
        pointDataQueue.peek().isFirstPage = true;
      }
      isFirstPageEnd = true;
    }
  }

  /**
   * This method is called after removing page from page queue.
   *
   * @param pageElement
   * @throws IOException
   */
  private void checkIsFirstChunkEnd(PageElement pageElement) throws IOException {
    if (pageElement.isLastPage && pageElement.chunkMetadataElement.isFirstChunk) {
      // finish compacting the first chunk
      chunkMetadataQueue.poll();
      if (!pageQueue.isEmpty()) {
        pageQueue.peek().chunkMetadataElement.isFirstChunk = true;
      }
      if (!chunkMetadataQueue.isEmpty()) {
        for (ChunkMetadataElement newOverlappedChunkMetadata :
            findOverlapChunkMetadatas(chunkMetadataQueue.peek())) {
          deserializeChunkIntoQueue(newOverlappedChunkMetadata);
        }
      }
    }
  }

  private boolean isChunkOverlap(ChunkMetadataElement chunkMetadataElement) {
    return false;
  }

  private boolean isPageOverlap(PageElement pageElement) {
    long endTime = pageElement.pageHeader.getEndTime();
    for (PageElement element : pageQueue) {
      if (element.equals(pageElement)) {
        continue;
      }
      if (element.startTime <= endTime) {
        return true;
      } else {
        return false;
      }
    }
    return false;
  }

  private void removePage(PageElement pageElement) {
    if (pageQueue.peek().equals(pageElement)) {
      // first page end

    }
    pageQueue.remove(pageElement);
  }
}
