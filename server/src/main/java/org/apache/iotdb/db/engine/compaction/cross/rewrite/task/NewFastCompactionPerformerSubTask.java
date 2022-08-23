package org.apache.iotdb.db.engine.compaction.cross.rewrite.task;

import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.compaction.cross.utils.ChunkMetadataElement;
import org.apache.iotdb.db.engine.compaction.cross.utils.PageElement;
import org.apache.iotdb.db.engine.compaction.cross.utils.PointDataElement;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

public class NewFastCompactionPerformerSubTask {
  List<ChunkMetadata> seqChunkMetadataList;
  List<ChunkMetadata> unseqChunkMetadataList;

  private final PriorityQueue<ChunkMetadataElement> chunkMetadataQueue;

  private final PriorityQueue<PageElement> pageQueue;

  private final PriorityQueue<PointDataElement> pointDataQueue;

  private boolean isFirstPageEnd = false;

  public NewFastCompactionPerformerSubTask() {
    chunkMetadataQueue =
        new PriorityQueue<>(
            (o1, o2) -> {
              int timeCompare = Long.compare(o1.startTime, o2.startTime);
              return timeCompare != 0 ? timeCompare : Integer.compare(o1.priority, o2.priority);
            });

    pageQueue =
        new PriorityQueue<>(
            (o1, o2) -> {
              int timeCompare = Long.compare(o1.startTime, o2.startTime);
              return timeCompare != 0 ? timeCompare : Integer.compare(o1.priority, o2.priority);
            });

    pointDataQueue =
        new PriorityQueue<>(
            (o1, o2) -> {
              int timeCompare = Long.compare(o1.curTimestamp, o2.curTimestamp);
              return timeCompare != 0 ? timeCompare : Integer.compare(o1.priority, o2.priority);
            });
  }

  public void compact() throws IOException {
    int chunkMetadataPriority = 0;
    for (ChunkMetadata chunkMetadata : seqChunkMetadataList) {
      chunkMetadataQueue.add(new ChunkMetadataElement(chunkMetadata, chunkMetadataPriority++));
    }
    for (ChunkMetadata chunkMetadata : unseqChunkMetadataList) {
      chunkMetadataQueue.add(new ChunkMetadataElement(chunkMetadata, chunkMetadataPriority++));
    }

    while (!chunkMetadataQueue.isEmpty()) {
      ChunkMetadataElement firstChunkMetadataElement = chunkMetadataQueue.peek();
      List<ChunkMetadataElement> overlapedChunkMetadatas =
          findOverlapChunkMetadatas(firstChunkMetadataElement);
      if (overlapedChunkMetadatas.isEmpty()) {
        // has none overlap chunks, flush it directly to tsfile writer

      } else {
        // deserialize chunks
        overlapedChunkMetadatas.add(0, firstChunkMetadataElement);
        int pagePriority = 0;
        for (ChunkMetadataElement chunkMetadataElement : overlapedChunkMetadatas) {
          Chunk chunk = ChunkCache.getInstance().get(chunkMetadataElement.chunkMetadata);
          ChunkReader chunkReader = new ChunkReader(chunk);
          ByteBuffer chunkDataBuffer = chunk.getData();
          ChunkHeader chunkHeader = chunk.getHeader();
          while (chunkDataBuffer.remaining() > 0) {
            // deserialize a PageHeader from chunkDataBuffer
            PageHeader pageHeader;
            if (((byte) (chunkHeader.getChunkType() & 0x3F))
                == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER) {
              pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunk.getChunkStatistic());
            } else {
              pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunkHeader.getDataType());
            }
            ByteBuffer compressedPageData =
                chunkReader.readPageDataWithoutUncompressing(pageHeader);
            pageQueue.add(
                new PageElement(pageHeader, compressedPageData, chunkReader, pagePriority++));
          }
        }
        compactWithPages();
      }
    }
  }

  private void compactWithPages() throws IOException {
    while (!pageQueue.isEmpty()) {
      PageElement firstPageElement = pageQueue.peek();
      List<PageElement> overlappedPages = findOverlapPages(firstPageElement);
      if (overlappedPages.isEmpty()) {
        // has none overlap pages, flush it directly to chunk writer
        compactWithNonOverlapPages(firstPageElement);
        pageQueue.poll();
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
        compactWithOverlapPages(overlappedPages);
      }
    }
  }

  /**
   * Compact a series of pages that overlap with each other. Eg: The parameters are page 1 and page
   * 2, that is, page 1 only overlaps with page 2, while page 2 overlap with page 3, page 3 overlap
   * with page 4,and so on, there are 10 pages in total. This method will merge all 10 pages.
   */
  private void compactWithOverlapPages(List<PageElement> overlappedPages) throws IOException {
    while (!overlappedPages.isEmpty()) {
      PageElement nextPageElement = overlappedPages.remove(0);
      // write current page point.time < next page point.time
      while (currentDataPoint().getTimestamp() < nextPageElement.startTime) {
        // write data point to chunk writer
        TimeValuePair timeValuePair = nextDataPoint();
      }

      if (currentDataPoint().getTimestamp() > nextPageElement.pageHeader.getEndTime()
          && !isPageOverlap(nextPageElement)) {
        // has none overlap pages, flush next page to chunk writer directly

      } else {
        // has overlap pages, then deserialize next page to dataPointQueue
        BatchData batchData =
            nextPageElement.chunkReader.readPageData(
                nextPageElement.pageHeader, nextPageElement.pageData);
        pointDataQueue.add(
            new PointDataElement(batchData, nextPageElement, nextPageElement.priority));
      }
    }

    // write remaining data points of these overlapped pages
    while (!pointDataQueue.isEmpty()) {
      // write data point to chunk writer
      TimeValuePair timeValuePair = nextDataPoint();

      // if finishing writing the first page, then start compacting the next page with its
      // overlapped pages
      if (isFirstPageEnd) {
        overlappedPages.addAll(findOverlapPages(pageQueue.peek()));
        isFirstPageEnd = false;
        if (!overlappedPages.isEmpty()) {
          // If there are new pages that overlap with the current firstPage, then exit the loop
          break;
        }
      }
    }
  }

  private void compactWithNonOverlapPages(PageElement pageElement) {}

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
      // has no data left in the page, remove it from queue
      element = pointDataQueue.poll();
      pageQueue.remove(element.pageElement);
      if (element.isFirstPage) {
        pointDataQueue.peek().isFirstPage = true;
        isFirstPageEnd = true;
      }
    }
    long lastTime = timeValuePair.getTimestamp();

    // remove data points with the same timestamp as the last point
    while (pointDataQueue.peek().curTimestamp == lastTime) {
      element = pointDataQueue.peek();
      if (element.hasNext()) {
        element.next();
      } else {
        // has no data left in the page, remove it from queue
        element = pointDataQueue.poll();
        pageQueue.remove(element.pageElement);
        if (element.isFirstPage) {
          pointDataQueue.peek().isFirstPage = true;
          isFirstPageEnd = true;
        }
      }
    }
    return timeValuePair;
  }

  private void checkNextPoint(PointDataElement element) {}

  /**
   * Add batch data to dataPointQueue
   *
   * @param pageElement
   */
  private void addOverlapedPagesToQueue(PageElement pageElement) throws IOException {
    for (PageElement element : findOverlapPages(pageElement)) {
      BatchData batchData = element.chunkReader.readPageData(element.pageHeader, element.pageData);
      pointDataQueue.add(new PointDataElement(batchData, element, element.priority));
    }
  }

  private ByteBuffer readPageData(PageHeader pageHeader, ByteBuffer chunkDataBuffer)
      throws IOException {
    int compressedPageBodyLength = pageHeader.getCompressedSize();
    byte[] compressedPageBody = new byte[compressedPageBodyLength];

    // doesn't has a complete page body
    if (compressedPageBodyLength > chunkDataBuffer.remaining()) {
      throw new IOException(
          "do not has a complete page body. Expected:"
              + compressedPageBodyLength
              + ". Actual:"
              + chunkDataBuffer.remaining());
    }

    chunkDataBuffer.get(compressedPageBody);
    return ByteBuffer.wrap(compressedPageBody);
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
}
