package org.apache.iotdb.db.engine.compaction.cross.rewrite.task;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.compaction.cross.utils.ChunkMetadataElement;
import org.apache.iotdb.db.engine.compaction.cross.utils.PageElement;
import org.apache.iotdb.db.engine.compaction.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.reader.PointPriorityReader;
import org.apache.iotdb.db.engine.compaction.writer.NewFastCrossCompactionWriter;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.reader.chunk.AlignedChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

public class NewFastCompactionPerformerSubTask implements Callable<Void> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);

  @FunctionalInterface
  public interface RemovePage {
    void call(PageElement pageElement, List<PageElement> newOverlappedPages)
        throws WriteProcessException, IOException;
  }

  private final PriorityQueue<ChunkMetadataElement> chunkMetadataQueue;

  private final PriorityQueue<PageElement> pageQueue;

  private final NewFastCrossCompactionWriter compactionWriter;

  private final int subTaskId;

  // all measurements id of this device
  private final List<String> allMeasurements;

  // the indexs of the timseries to be compacted to which the current sub thread is assigned
  private final List<Integer> pathsIndex;

  // chunk metadata list of all timeseries of this device in this seq file
  private final List<List<ChunkMetadata>> allSensorMetadatas;

  private final List<MeasurementSchema> measurementSchemas;

  private final PointPriorityReader pointPriorityReader = new PointPriorityReader(this::removePage);

  // sorted source files by the start time of device
  private final List<TsFileResource> sortedSourceFiles = new ArrayList<>();

  private FastCompactionPerformer fastCompactionPerformer;

  private boolean isAligned;

  private String deviceId;

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
  }

  @Override
  public Void call() throws IOException, PageException, WriteProcessException {
    for (Integer index : pathsIndex) {
      if (allSensorMetadatas.get(index).isEmpty()) {
        continue;
      }
      int chunkMetadataPriority = 0;
      if (isAligned) {
        for (int i = 0; i < allSensorMetadatas.get(0).size(); i++) {
          List<IChunkMetadata> alignedChunkMetadatas = new ArrayList<>();
          int idx = i;
          allSensorMetadatas.forEach(x -> alignedChunkMetadatas.add(x.get(idx)));
          chunkMetadataQueue.add(
              new ChunkMetadataElement(
                  new AlignedChunkMetadata(alignedChunkMetadatas.remove(0), alignedChunkMetadatas),
                  chunkMetadataPriority++));
        }
      } else {
        for (ChunkMetadata chunkMetadata : allSensorMetadatas.get(index)) {
          chunkMetadataQueue.add(new ChunkMetadataElement(chunkMetadata, chunkMetadataPriority++));
        }
      }

      compactionWriter.startMeasurement(
          Collections.singletonList(measurementSchemas.get(index)), subTaskId);
      compactChunks();
      compactionWriter.endMeasurement(subTaskId);
    }
    return null;
  }

  private void compactFiles() throws PageException, IOException, WriteProcessException {
    while (!sortedSourceFiles.isEmpty()) {
      // =sortedSourceFiles.remove(0);
      List<TsFileResource> overlappedFiles = findOverlapFiles(sortedSourceFiles.get(0));

      // read chunk metadatas and put them into chunk metadata queue

      compactChunks();
    }
  }

  /**
   * Compact chunks in chunk metadata queue.
   *
   * @throws IOException
   * @throws PageException
   */
  private void compactChunks() throws IOException, PageException, WriteProcessException {
    while (!chunkMetadataQueue.isEmpty()) {
      chunkMetadataQueue.peek().isFirstChunk = true;
      ChunkMetadataElement firstChunkMetadataElement = chunkMetadataQueue.peek();
      List<ChunkMetadataElement> overlappedChunkMetadatas =
          findOverlapChunkMetadatas(firstChunkMetadataElement);
      boolean isChunkOverlap = overlappedChunkMetadatas.size() > 1;
      int modifiedStatus =
          isAligned
              ? (getAlignedChunkDeletions(
                              (AlignedChunkMetadata) firstChunkMetadataElement.chunkMetadata)
                          .size()
                      > 0
                  ? 0
                  : -1)
              : isPageOrChunkModified(
                  firstChunkMetadataElement.startTime,
                  firstChunkMetadataElement.chunkMetadata.getEndTime(),
                  firstChunkMetadataElement.chunkMetadata.getDeleteIntervalList());

      switch (isPageOrChunkModified(
          firstChunkMetadataElement.startTime,
          firstChunkMetadataElement.chunkMetadata.getEndTime(),
          firstChunkMetadataElement.chunkMetadata.getDeleteIntervalList())) {
        case -1:
          // no data on this chunk has been deleted
          if (isChunkOverlap) {
            // has overlap chunks, deserialize it
            compactWithOverlapChunks(overlappedChunkMetadatas);
          } else {
            // has none overlap chunk, flush it to file writer directly
            compactWithNonOverlapChunks(firstChunkMetadataElement);
          }
          break;
        case 0:
          // there is data on this chunk been deleted, deserialize it
          compactWithOverlapChunks(overlappedChunkMetadatas);
          break;
        case 1:
          // all data on this chunk has been deleted, remove it
          removeChunk(firstChunkMetadataElement);
          break;
      }
    }
  }

  /** Deserialize chunks and start compacting pages. */
  private void compactWithOverlapChunks(List<ChunkMetadataElement> overlappedChunkMetadatas)
      throws IOException, PageException, WriteProcessException {
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
      throws IOException, PageException, WriteProcessException {
    if (compactionWriter.flushChunkToFileWriter(chunkMetadataElement.chunkMetadata, subTaskId)) {
      // flush chunk successfully
      removeChunk(chunkMetadataQueue.peek());
    } else {
      // chunk.endTime > file.endTime, then deserialize chunk
      compactWithOverlapChunks(Collections.singletonList(chunkMetadataElement));
    }
  }

  private void deserializeFileIntoQueue(List<TsFileResource> resources)
      throws IOException, IllegalPathException {
    for (TsFileResource resource : resources) {
      for (Map.Entry<String, List<ChunkMetadata>> entry :
          fastCompactionPerformer
              .getReaderFromCache(resource)
              .readChunkMetadataInDevice(deviceId)
              .entrySet()) {
        String sensor = entry.getKey();
        List<ChunkMetadata> chunkMetadataList = entry.getValue();
        if (!chunkMetadataList.isEmpty()) {
          // set file path
          chunkMetadataList.forEach(x -> x.setFilePath(resource.getTsFilePath()));

          // modify chunk metadatas
          QueryUtils.modifyChunkMetaData(
              chunkMetadataList,
              fastCompactionPerformer.getModifications(
                  resource, new PartialPath(deviceId, sensor)));
        }
      }
    }
  }

  /** Deserialize chunk into pages without uncompressing and put them into the page queue. */
  private void deserializeChunkIntoQueue(ChunkMetadataElement chunkMetadataElement)
      throws IOException {
    Chunk chunk = ChunkCache.getInstance().get((ChunkMetadata) chunkMetadataElement.chunkMetadata);
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
  private void compactPages() throws IOException, PageException, WriteProcessException {
    while (!pageQueue.isEmpty()) {
      PageElement firstPageElement = pageQueue.peek();
      List<PageElement> overlappedPages = findOverlapPages(firstPageElement);
      boolean isPageOverlap = overlappedPages.size() > 1;
      switch (isPageOrChunkModified(
          firstPageElement.startTime,
          firstPageElement.pageHeader.getEndTime(),
          firstPageElement.chunkMetadataElement.chunkMetadata.getDeleteIntervalList())) {
        case -1:
          // no data on this page has been deleted, flush it to chunk writer directly
          if (isPageOverlap) {
            compactWithOverlapPages(overlappedPages);
          } else {
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
      throws PageException, IOException, WriteProcessException {
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
      throws IOException, PageException, WriteProcessException {
    pointPriorityReader.addNewPages(Collections.singletonList(overlappedPages.remove(0)));
    pointPriorityReader.setNewOverlappedPages(overlappedPages);
    while (pointPriorityReader.hasNext()) {
      for (int pageIndex = 0; pageIndex < overlappedPages.size(); pageIndex++) {
        PageElement nextPageElement = overlappedPages.get(pageIndex);

        // write currentPage.point.time < nextPage.startTime to chunk writer
        while (pointPriorityReader.currentPoint().getTimestamp() < nextPageElement.startTime) {
          // write data point to chunk writer
          compactionWriter.writeTimeValue(pointPriorityReader.currentPoint(), subTaskId);
          pointPriorityReader.next();
        }

        boolean isNextPageOverlap =
            !(pointPriorityReader.currentPoint().getTimestamp()
                    > nextPageElement.pageHeader.getEndTime()
                && !isPageOverlap(nextPageElement));

        switch (isPageOrChunkModified(
            nextPageElement.startTime,
            nextPageElement.pageHeader.getEndTime(),
            nextPageElement.chunkMetadataElement.chunkMetadata.getDeleteIntervalList())) {
          case -1:
            // no data on this page has been deleted
            if (isNextPageOverlap) {
              // has overlap with other pages, deserialize it
              pointPriorityReader.addNewPages(Collections.singletonList(nextPageElement));
            } else {
              // has none overlap, flush it to chunk writer directly
              compactWithNonOverlapPage(nextPageElement);
            }
            break;
          case 0:
            // there is data on this page been deleted, deserialize it
            pointPriorityReader.addNewPages(Collections.singletonList(nextPageElement));
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
        compactionWriter.writeTimeValue(pointPriorityReader.currentPoint(), subTaskId);
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

  private List<TsFileResource> findOverlapFiles(TsFileResource resource) {
    List<TsFileResource> overlappedFiles = new ArrayList<>();
    long endTime = resource.getEndTime(deviceId);
    for (TsFileResource sourceFile : sortedSourceFiles) {
      if (sourceFile.getStartTime(deviceId) <= endTime) {
        overlappedFiles.add(sourceFile);
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
   * -1 means that no data on this page or chunk has been deleted. <br>
   * 0 means that there is data on this page or chunk been deleted. <br>
   * 1 means that all data on this page or chunk has been deleted.
   */
  private int isPageOrChunkModified(
      long startTime, long endTime, List<TimeRange> deleteIntervalList) {
    int status = -1;
    if (deleteIntervalList != null) {
      for (TimeRange range : deleteIntervalList) {
        if (range.contains(startTime, endTime)) {
          // all data on this page has been deleted
          return 1;
        }
        if (range.overlaps(new TimeRange(startTime, endTime))) {
          // exist data on this page been deleted
          // pageHeader.setModified(true);
          status = 0;
        }
      }
    }
    return status;
  }

  private void removePage(PageElement pageElement, List<PageElement> newOverlappedPages)
      throws IOException {
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
      // when deserializing new chunks into page queue or first page is removed from page queue, we
      // should find new overlapped pages and put them into list
      newOverlappedPages.addAll(findOverlapPages(pageQueue.peek()));
    }
  }

  private void removeChunk(ChunkMetadataElement chunkMetadataElement) throws IOException {
    // check is first chunk or not
    boolean isFirstChunk = chunkMetadataQueue.peek().equals(chunkMetadataElement);

    chunkMetadataQueue.remove(chunkMetadataElement);
    if (isFirstChunk && !chunkMetadataQueue.isEmpty()) {
      chunkMetadataQueue.peek().isFirstChunk = true;
      if (!pageQueue.isEmpty()) {
        // find new overlapped chunks and deserialize them into page queue
        for (ChunkMetadataElement newOverlappedChunkMetadata :
            findOverlapChunkMetadatas(chunkMetadataQueue.peek())) {
          deserializeChunkIntoQueue(newOverlappedChunkMetadata);
        }
      }
    }
  }

  private List<TimeRange> getAlignedChunkDeletions(AlignedChunkMetadata alignedChunkMetadata) {
    List<TimeRange> deletions = new ArrayList<>();
    deletions.addAll(alignedChunkMetadata.getTimeChunkMetadata().getDeleteIntervalList());
    for (IChunkMetadata valueChunkMetadata : alignedChunkMetadata.getValueChunkMetadataList()) {
      deletions.addAll(valueChunkMetadata.getDeleteIntervalList());
    }
    return deletions;
  }
}
