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

public class OptimizeCompactionPerformerSubTask {
  List<ChunkMetadata> seqChunkMetadataList;
  List<ChunkMetadata> unseqChunkMetadataList;

  private long lastTime;

  private PriorityQueue<ChunkMetadataElement> chunkMetadataQueue;

  private PriorityQueue<PageElement> pageQueue;

  private PriorityQueue<PointDataElement> pointDataQueue;

  public OptimizeCompactionPerformerSubTask() {
    chunkMetadataQueue =
        new PriorityQueue<>(
            (o1, o2) -> {
              int timeCompare = Long.compare(o1.getStartTime(), o2.getStartTime());
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

    ChunkMetadata firstChunkMetadata = chunkMetadataQueue.peek().getChunkMetadata();
    List<ChunkMetadata> overlapedChunkMetadatas = findOverlapChunkMetadatas(firstChunkMetadata);
    if (overlapedChunkMetadatas.isEmpty()) {
      // has none overlap chunks, flush it directly to tsfile writer

    } else {
      // deserialize chunks
      overlapedChunkMetadatas.add(0, firstChunkMetadata);
      int pagePriority = 0;
      for (ChunkMetadata metadata : overlapedChunkMetadatas) {
        Chunk chunk = ChunkCache.getInstance().get(metadata);
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
          ByteBuffer pageData = readPageData(pageHeader, chunkDataBuffer);
          pageQueue.add(new PageElement(pageHeader, pageData, chunkReader, pagePriority++));
        }
      }
    }
  }

  private void compactWithPages() throws IOException {
    while (!pageQueue.isEmpty()) {
      PageElement firstPageElement = pageQueue.peek();
      List<PageElement> overlapedPages = findOverlapPages(firstPageElement);
      if (overlapedPages.isEmpty()) {
        // has none overlap pages, flush it directly to chunk writer

      } else {
        // deserialize pages
        PointDataElement firstPageDataPointElement =
            new PointDataElement(
                firstPageElement.chunkReader.readPageData(
                    firstPageElement.pageHeader, firstPageElement.pageData),
                firstPageElement.priority);
        firstPageDataPointElement.isFirstPage = true;
        pointDataQueue.add(firstPageDataPointElement);
        for (PageElement element : overlapedPages) {
          BatchData batchData =
              element.chunkReader.readPageData(element.pageHeader, element.pageData);
          pointDataQueue.add(new PointDataElement(batchData, element.priority));
        }

        while (!pointDataQueue.isEmpty()) {
          // write data point to chunk writer
          TimeValuePair timeValuePair = nextDataPoint();
        }
      }
    }
  }

  private TimeValuePair nextDataPoint() throws IOException {
    PointDataElement element = pointDataQueue.peek();
    TimeValuePair timeValuePair = element.curTimeValuePair;
    if (element.hasNext()) {
      element.next();
    } else {
      // has no data left in the page, remove it
      element = pointDataQueue.poll();
      if (element.isFirstPage) {
        pointDataQueue.peek().isFirstPage = true;
        addOverlapedPagesToQueue(pageQueue.poll());
      }
    }
    lastTime = timeValuePair.getTimestamp();

    // remove data points with the same timestamp as the last point
    while (pointDataQueue.peek().curTimestamp == lastTime) {
      element = pointDataQueue.peek();
      if (element.hasNext()) {
        element.next();
      } else {
        // has no data left in the page, remove it
        element = pointDataQueue.poll();
        if (element.isFirstPage) {
          pointDataQueue.peek().isFirstPage = true;
          addOverlapedPagesToQueue(pageQueue.poll());
        }
      }
    }
    return timeValuePair;
  }

  /**
   * Add batch data to dataPointQueue
   *
   * @param pageElement
   */
  private void addOverlapedPagesToQueue(PageElement pageElement) throws IOException {
    for (PageElement element : findOverlapPages(pageElement)) {
      BatchData batchData = element.chunkReader.readPageData(element.pageHeader, element.pageData);
      pointDataQueue.add(new PointDataElement(batchData, element.priority));
    }
  }

  private void compactWithOverlapedPages() {}

  private ByteBuffer readPageData(PageHeader pageHeader, ByteBuffer chunkDataBuffer) {}

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
      if (!element.isOverlaped && element.startTime <= endTime) {
        elements.add(element);
        element.isOverlaped = true;
      } else {
        break;
      }
    }
    return elements;
  }

  private List<ChunkMetadata> findOverlapChunkMetadatas(ChunkMetadataElement element) {
    List<ChunkMetadata> elements = new ArrayList<>();
    long endTime = chunkMetadata.getEndTime();
    for (ChunkMetadataElement element : chunkMetadataQueue) {
      ChunkMetadata metadata = element.getChunkMetadata();
      if (metadata.getStartTime() <= endTime) {
        elements.add(metadata);
      } else {
        break;
      }
    }
    return elements;
  }
}
