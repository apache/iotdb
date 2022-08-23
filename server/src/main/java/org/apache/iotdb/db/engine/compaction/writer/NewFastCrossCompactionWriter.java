package org.apache.iotdb.db.engine.compaction.writer;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.cross.utils.ChunkMetadataElement;
import org.apache.iotdb.db.engine.compaction.cross.utils.PageElement;
import org.apache.iotdb.db.engine.compaction.cross.utils.PointDataElement;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

public class NewFastCrossCompactionWriter {
  private static final int subTaskNum =
      IoTDBDescriptor.getInstance().getConfig().getSubCompactionTaskNum();

  List<TsFileIOWriter> targetFileWriters = new ArrayList<>();

  // Each sub task has its own chunk writer.
  // The index of the array corresponds to subTaskId.
  private final IChunkWriter[] chunkWriters = new IChunkWriter[subTaskNum];

  // whether each target file has device data or not
  private final boolean[] isDeviceExistedInTargetFiles;

  private String deviceId;

    private boolean isAlign;


  private final boolean[] isFirstPageEnd=new boolean[subTaskNum];

  private final PriorityQueue<ChunkMetadataElement>[] chunkMetadataQueue=new PriorityQueue<>[subTaskNum];

  private final PriorityQueue<PageElement>[] pageQueue=new PriorityQueue<>[subTaskNum];

  private final PriorityQueue<PointDataElement>[] pointDataQueue=new PriorityQueue<>[subTaskNum];

  // Each sub task has point count in current measurment, which is used to check size.
  // The index of the array corresponds to subTaskId.
  private final long[] measurementPointCountArray = new long[subTaskNum];

  public NewFastCrossCompactionWriter(List<TsFileResource> targetResources) throws IOException {
    for (TsFileResource resource : targetResources) {
      this.targetFileWriters.add(new TsFileIOWriter(resource.getTsFile()));
    }
    isDeviceExistedInTargetFiles = new boolean[targetResources.size()];
  }

  public void startChunkGroup(String deviceId,boolean isAlign) throws IOException {
      this.deviceId = deviceId;
      this.isAlign = isAlign;
      for(TsFileIOWriter writer:targetFileWriters){
          writer.startChunkGroup(deviceId);
      }
  }

  public void startMeasurement(List<IMeasurementSchema> measurementSchemaList, int subTaskId){
      if (isAlign) {
          chunkWriters[subTaskId] = new AlignedChunkWriterImpl(measurementSchemaList);
      } else {
          chunkWriters[subTaskId] = new ChunkWriterImpl(measurementSchemaList.get(0), true);
      }
  }

  public void endMeasurement(int subTaskId){

  }


    private void flushChunkToFileWriter(Chunk seqChunk, ChunkMetadata seqChunkMetadata, int subTaskId)
            throws IOException {
        TsFileIOWriter tsFileIOWriter = targetFileWriters.get(targetFileIndex);
        // seal last chunk to file writer
        // Todo: may cause small chunk
        chunkWriters[subTaskId].writeToFileWriter(tsFileIOWriter);

        synchronized (tsFileIOWriter) {
            tsFileIOWriter.writeChunk(seqChunk, seqChunkMetadata);
        }
    }

    private void flushPageToChunkWriter(
            ByteBuffer chunkDataBuffer, PageHeader pageHeader, int subTaskId)
            throws IOException, PageException {
        int compressedPageBodyLength = pageHeader.getCompressedSize();
        byte[] compressedPageBody = new byte[compressedPageBodyLength];
        // not a complete page body
        if (compressedPageBodyLength > chunkDataBuffer.remaining()) {
            throw new IOException(
                    "Do not have a complete page body. Expected:"
                            + compressedPageBodyLength
                            + ". Actual:"
                            + chunkDataBuffer.remaining());
        }
        chunkDataBuffer.get(compressedPageBody);
        ChunkWriterImpl chunkWriter = (ChunkWriterImpl) chunkWriters[subTaskId];
        // seal current page
        chunkWriter.sealCurrentPage();
        // flush new page to chunk writer directly
        // Todo: may cause small page
        chunkWriter.writePageHeaderAndDataIntoBuff(ByteBuffer.wrap(compressedPageBody), pageHeader);

        // check chunk size and may open a new chunk
        CompactionWriterUtils.checkChunkSizeAndMayOpenANewChunk(
                targetFileWriters.get(targetFileIndex), chunkWriter, true);
    }



    /**
     * Compact a series of pages that overlap with each other. Eg: The parameters are page 1 and page
     * 2, that is, page 1 only overlaps with page 2, while page 2 overlap with page 3, page 3 overlap
     * with page 4,and so on, there are 10 pages in total. This method will merge all 10 pages.
     */
    private void compactWithOverlapPages(List<PageElement> overlappedPages,int subTaskId) throws IOException {
        while (!overlappedPages.isEmpty()) {
            PageElement nextPageElement = overlappedPages.remove(0);
            // write current page point.time < next page point.time
            while (currentDataPoint(subTaskId).getTimestamp() < nextPageElement.startTime) {
                // write data point to chunk writer
                TimeValuePair timeValuePair = nextDataPoint(subTaskId);
            }

            if (currentDataPoint(subTaskId).getTimestamp() > nextPageElement.pageHeader.getEndTime()
                    && !isPageOverlap(nextPageElement,subTaskId)) {
                // has none overlap pages, flush next page to chunk writer directly

            } else {
                // has overlap pages, then deserialize next page to dataPointQueue
                BatchData batchData =
                        nextPageElement.chunkReader.readPageData(
                                nextPageElement.pageHeader, nextPageElement.pageData);
                pointDataQueue[subTaskId].add(
                        new PointDataElement(batchData, nextPageElement, nextPageElement.priority));
            }
        }

        // write remaining data points of these overlapped pages
        while (!pointDataQueue[subTaskId].isEmpty()) {
            // write data point to chunk writer
            TimeValuePair timeValuePair = nextDataPoint(subTaskId);

            // if finishing writing the first page, then start compacting the next page with its
            // overlapped pages
            if (isFirstPageEnd[subTaskId]) {
                overlappedPages.addAll(findOverlapPages(pageQueue[subTaskId].peek(),subTaskId));
                isFirstPageEnd[subTaskId] = false;
                if (!overlappedPages.isEmpty()) {
                    // If there are new pages that overlap with the current firstPage, then exit the loop
                    break;
                }
            }
        }
    }

    private TimeValuePair currentDataPoint(int subTaskId) {
        return pointDataQueue[subTaskId].peek().curTimeValuePair;
    }

    /**
     * Read next data point from pointDataQueue.
     *
     * @return
     * @throws IOException
     */
    private TimeValuePair nextDataPoint(int subTaskId) throws IOException {
        PriorityQueue<PointDataElement> pointQueue=pointDataQueue[subTaskId];
        PointDataElement element = pointQueue.peek();
        TimeValuePair timeValuePair = element.curTimeValuePair;
        if (element.hasNext()) {
            element.next();
        } else {
            // has no data left in the page, remove it from queue
            element = pointQueue.poll();
            pageQueue[subTaskId].remove(element.pageElement);
            if (element.isFirstPage) {
                pointQueue.peek().isFirstPage = true;
                isFirstPageEnd[subTaskId] = true;
            }
        }
        long lastTime = timeValuePair.getTimestamp();

        // remove data points with the same timestamp as the last point
        while (pointQueue.peek().curTimestamp == lastTime) {
            element = pointQueue.peek();
            if (element.hasNext()) {
                element.next();
            } else {
                // has no data left in the page, remove it from queue
                element = pointQueue.poll();
                pageQueue[subTaskId].remove(element.pageElement);
                if (element.isFirstPage) {
                    pointQueue.peek().isFirstPage = true;
                    isFirstPageEnd[subTaskId] = true;
                }
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
    private List<PageElement> findOverlapPages(PageElement page,int subTaskId) {
        List<PageElement> elements = new ArrayList<>();
        long endTime = page.pageHeader.getEndTime();
        for (PageElement element : pageQueue[subTaskId]) {
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

    private List<ChunkMetadataElement> findOverlapChunkMetadatas(ChunkMetadataElement chunkMetadata,int subTaskId) {
        List<ChunkMetadataElement> elements = new ArrayList<>();
        long endTime = chunkMetadata.chunkMetadata.getEndTime();
        for (ChunkMetadataElement element : chunkMetadataQueue[subTaskId]) {
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

    private boolean isPageOverlap(PageElement pageElement,int subTaskId) {
        long endTime = pageElement.pageHeader.getEndTime();
        for (PageElement element : pageQueue[subTaskId]) {
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
