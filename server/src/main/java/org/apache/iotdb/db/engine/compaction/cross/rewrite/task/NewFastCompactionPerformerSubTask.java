package org.apache.iotdb.db.engine.compaction.cross.rewrite.task;

import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.compaction.cross.utils.ChunkMetadataElement;
import org.apache.iotdb.db.engine.compaction.cross.utils.PageElement;
import org.apache.iotdb.db.engine.compaction.cross.utils.PointDataElement;
import org.apache.iotdb.db.engine.compaction.reader.PriorityCompactionReader;
import org.apache.iotdb.db.engine.compaction.writer.NewFastCrossCompactionWriter;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

public class NewFastCompactionPerformerSubTask implements Callable<Void> {

  @FunctionalInterface
  public interface RemovePage {
    void call(PageElement pageElement) throws WriteProcessException, IOException;
  }

  private final PriorityQueue<ChunkMetadataElement> chunkMetadataQueue;

  private final PriorityQueue<PageElement> pageQueue;

  private final PriorityQueue<PointDataElement> pointDataQueue;

  private boolean isFirstPageEnd = false;

  private final NewFastCrossCompactionWriter compactionWriter;

  private final int subTaskId;

  // all measurements id of this device
  private final List<String> allMeasurements;

  String sensor = "";

  // the indexs of the timseries to be compacted to which the current sub thread is assigned
  private final List<Integer> pathsIndex;

  // chunk metadata list of all timeseries of this device in this seq file
  private final List<List<ChunkMetadata>> allSensorMetadatas;

  private final List<MeasurementSchema> measurementSchemas;

  private final PriorityCompactionReader priorityCompactionReader =
      new PriorityCompactionReader(this::removePage);

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
  public Void call() throws IOException, PageException, WriteProcessException {
    for (Integer index : pathsIndex) {
      if (allSensorMetadatas.get(index).isEmpty()) {
        continue;
      }
      sensor = allMeasurements.get(index);
      int chunkMetadataPriority = 0;
      for (ChunkMetadata chunkMetadata : allSensorMetadatas.get(index)) {
        chunkMetadataQueue.add(new ChunkMetadataElement(chunkMetadata, chunkMetadataPriority++));
      }

      compactionWriter.startMeasurement(
          Collections.singletonList(measurementSchemas.get(index)), subTaskId);
      compactWithChunks();
      compactionWriter.endMeasurement(subTaskId);
    }
    return null;
  }

  /**
   * Compact chunks in chunk metadata queue.
   *
   * @throws IOException
   * @throws PageException
   */
  private void compactWithChunks() throws IOException, PageException, WriteProcessException {
    while (!chunkMetadataQueue.isEmpty()) {
      chunkMetadataQueue.peek().isFirstChunk = true;
      ChunkMetadataElement firstChunkMetadataElement = chunkMetadataQueue.peek();
      List<ChunkMetadataElement> overlappedChunkMetadatas =
          findOverlapChunkMetadatas(firstChunkMetadataElement);
      if (overlappedChunkMetadatas.isEmpty()) {
        // has none overlap chunks, flush it directly to tsfile writer
        compactionWriter.flushChunkToFileWriter(firstChunkMetadataElement.chunkMetadata, subTaskId);
        chunkMetadataQueue.poll();
      } else {
        // deserialize chunks
        deserializeChunkIntoQueue(firstChunkMetadataElement);
        for (ChunkMetadataElement overlappedChunkMetadata : overlappedChunkMetadatas) {
          deserializeChunkIntoQueue(overlappedChunkMetadata);
        }
        compactWithPages();
      }
    }
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
              chunkMetadataElement.priority));
    }
  }

  /**
   * Compact pages in page queue.
   *
   * @throws IOException
   * @throws PageException
   */
  private void compactWithPages() throws IOException, PageException, WriteProcessException {
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
        // deserialize pages
        priorityCompactionReader.addNewPages(Collections.singletonList(firstPageElement));
        compactWithOverlapPages(overlappedPages);
      }
    }
  }

  /**
   * Compact a series of pages that overlap with each other. Eg: The parameters are page 1 and page
   * 2, that is, page 1 only overlaps with page 2, while page 2 overlap with page 3, page 3 overlap
   * with page 4,and so on, there are 10 pages in total. This method will merge all 10 pages.
   */
  private void compactWithOverlapPages(List<PageElement> overlappedPages)
      throws IOException, PageException, WriteProcessException {
    int pageIndex = 0;
    while (priorityCompactionReader.hasNext()) {
      for (; pageIndex < overlappedPages.size(); pageIndex++) {
        PageElement nextPageElement = overlappedPages.get(pageIndex);
        // write current page point.time < next page point.time
        while (priorityCompactionReader.currentPoint().getTimestamp() < nextPageElement.startTime) {
          // write data point to chunk writer
          compactionWriter.writeTimeValue(priorityCompactionReader.currentPoint(), subTaskId);
          priorityCompactionReader.next();
        }

        if (priorityCompactionReader.currentPoint().getTimestamp()
                > nextPageElement.pageHeader.getEndTime()
            && !isPageOverlap(nextPageElement)) {
          // has none overlap data, flush next page to chunk writer directly
          compactionWriter.flushPageToChunkWriter(
              nextPageElement.pageData, nextPageElement.pageHeader, subTaskId);
        } else {
          // has overlap data, then deserialize next page to dataPointQueue
          priorityCompactionReader.addNewPages(Collections.singletonList(nextPageElement));
        }
      }

      // write remaining data points
      while (priorityCompactionReader.hasNext()) {
        // write data point to chunk writer
        compactionWriter.writeTimeValue(priorityCompactionReader.currentPoint(), subTaskId);
        priorityCompactionReader.next();

        // if finishing writing the first page, then start compacting the next page with its
        // overlapped pages
        if (isFirstPageEnd && !pageQueue.isEmpty()) {
          isFirstPageEnd = false;
          if (!overlappedPages.contains(pageQueue.peek())) {
            // all overlapped pages has been compacted, return
            return;
          }
          // get the new overlapped pages of the top page
          overlappedPages.addAll(findOverlapPages(pageQueue.peek()));
          if (!overlappedPages.isEmpty()) {
            // If there are new pages that overlap with the current first page, then exit the loop
            break;
          }
        }
      }
    }
  }

  private List<PageElement> writeRemainingPoints() throws IOException, WriteProcessException {
    while (priorityCompactionReader.hasNext()) {
      // write data point to chunk writer
      compactionWriter.writeTimeValue(priorityCompactionReader.currentPoint(), subTaskId);
      priorityCompactionReader.next();

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
    Stream<PageElement> pages =
        pageQueue.stream().sorted(Comparator.comparingLong(o -> o.startTime));
    Iterator<PageElement> it = pages.iterator();
    while (it.hasNext()) {
      PageElement element = it.next();
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
    Stream<ChunkMetadataElement> chunks =
        chunkMetadataQueue.stream().sorted(Comparator.comparingLong(o -> o.startTime));
    Iterator<ChunkMetadataElement> it = chunks.iterator();
    while (it.hasNext()) {
      ChunkMetadataElement element = it.next();
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
      if (!chunkMetadataQueue.isEmpty()) {
        chunkMetadataQueue.peek().isFirstChunk = true;
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
    Stream<PageElement> pages =
        pageQueue.stream().sorted(Comparator.comparingLong(o -> o.startTime));
    Iterator<PageElement> it = pages.iterator();
    while (it.hasNext()) {
      PageElement element = it.next();
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

  private void removePage(PageElement pageElement) throws IOException {
    isFirstPageEnd = pageQueue.peek().equals(pageElement);
    pageQueue.remove(pageElement);
    checkIsFirstChunkEnd(pageElement);
  }
}
