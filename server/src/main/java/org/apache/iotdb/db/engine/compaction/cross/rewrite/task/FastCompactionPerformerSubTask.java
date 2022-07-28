package org.apache.iotdb.db.engine.compaction.cross.rewrite.task;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.compaction.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class FastCompactionPerformerSubTask implements Callable<Void> {

  private TsFileResource seqFile;

  private List<String> sensorPaths = new ArrayList<>();

  private Map<Integer, IPointReader> unseqReaders;

  private List<List<ChunkMetadata>> sensorMetadatas;

  List<Integer> pathsIndex;

  String deviceID;

  @Override
  public Void call() throws Exception {
    for (Integer index : pathsIndex) {
      IPointReader unseqReader =
          unseqReaders.computeIfAbsent(
              index,
              integer -> {
                try {
                  return FastCompactionPerformer.getUnseqReaders(
                      new PartialPath(sensorPaths.get(index)));
                } catch (IOException | IllegalPathException e) {
                  throw new RuntimeException(e);
                }
              });

      // iterate seq chunk
      List<ChunkMetadata> curSensorMetadataList = sensorMetadatas.get(index);
      for (int i = 0; i < curSensorMetadataList.size(); i++) {
        ChunkMetadata curChunkMetadata = curSensorMetadataList.get(i);
        boolean isLastChunk = i == curSensorMetadataList.size() - 1;
        boolean isChunkOverlap =
            isChunkOverlap(
                unseqReader.currentTimeValuePair(),
                curChunkMetadata,
                isLastChunk,
                seqFile.getEndTime(deviceID));
        boolean isChunkModified =
            (curChunkMetadata.getDeleteIntervalList() != null
                && !curChunkMetadata.getDeleteIntervalList().isEmpty());
        Chunk chunk = ChunkCache.getInstance().get(curChunkMetadata);
      }
    }
  }

  private void compactWithNonOverlapSeqChunk(
      Chunk seqChunk, boolean isChunkModified, ChunkWriterImpl chunkWriter)
      throws IOException, PageException {
    if (!isChunkModified) {
      // flush chunk to tsfileIOWriter directly

    } else {
      ChunkReader chunkReader = new ChunkReader(seqChunk);
      ByteBuffer chunkDataBuffer = seqChunk.getData();
      ChunkHeader chunkHeader = seqChunk.getHeader();
      while (chunkDataBuffer.remaining() > 0) {
        // deserialize a PageHeader from chunkDataBuffer
        PageHeader pageHeader;
        if (((byte) (chunkHeader.getChunkType() & 0x3F)) == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER) {
          pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, seqChunk.getChunkStatistic());
        } else {
          pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunkHeader.getDataType());
        }

        switch (isPageModified(pageHeader, seqChunk.getDeleteIntervalList())) {
          case 1:
            // all data on this page has been deleted, skip this page
            chunkReader.skipBytesInStreamByLength(pageHeader.getCompressedSize());
            break;
          case 0:
            // exist data on this page been deleted, deserialize this page
            PageReader pageReader = chunkReader.constructPageReaderForNextPage(pageHeader);
            BatchData batchData = pageReader.getAllSatisfiedPageData();
            while (batchData.hasCurrent()) {
              chunkWriter.write(batchData.currentTime(), batchData.currentValue());
              batchData.next();
            }
            break;
          case -1:
            // no data on this page has been deleted, flush this page to chunkWriter
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
            chunkWriter.writePageHeaderAndDataIntoBuff(
                ByteBuffer.wrap(compressedPageBody), pageHeader);
            break;
        }
      }
    }
  }

  private void compactWithOverlapSeqChunk(
      Chunk seqChunk, IPointReader unseqReader, ChunkWriterImpl chunkWriter) throws IOException {
    ChunkReader chunkReader = new ChunkReader(seqChunk);
    ChunkHeader chunkHeader = seqChunk.getHeader();
    ByteBuffer chunkDataBuffer = seqChunk.getData();
    while (chunkDataBuffer.hasRemaining()) {
      // deserialize a PageHeader from chunkDataBuffer
      PageHeader pageHeader;
      if (((byte) (chunkHeader.getChunkType() & 0x3F)) == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER) {
        pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, seqChunk.getChunkStatistic());
      } else {
        pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunkHeader.getDataType());
      }

      // compact with unseq data points
      // 1. write unseq point.time < seq page.startTime
      while (unseqReader.hasNextTimeValuePair()
          && unseqReader.currentTimeValuePair().getTimestamp() < pageHeader.getStartTime()) {
        TimeValuePair timeValuePair = unseqReader.nextTimeValuePair();
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue());
      }

      // 2. compact with this seq page, write point.time < seq page.endTime
      if (unseqReader.currentTimeValuePair().getTimestamp() <= pageHeader.getEndTime()) {
        // overlap with this page, then deserialize it and compact with unseq data point
        PageReader pageReader = chunkReader.constructPageReaderForNextPage(pageHeader);
        BatchData batchData = pageReader.getAllSatisfiedPageData();
        boolean hasOverWritenSeqPoint = false;
        while (batchData.hasCurrent()) {
          long seqTimestamp = batchData.currentTime();
          while (unseqReader.hasNextTimeValuePair()
              && unseqReader.currentTimeValuePair().getTimestamp() <= seqTimestamp) {
            // write unseq point
            TimeValuePair timeValuePair = unseqReader.nextTimeValuePair();
            long unseqTimestamp = timeValuePair.getTimestamp();
            if (unseqTimestamp == seqTimestamp) {
              hasOverWritenSeqPoint = true;
            }
            chunkWriter.write(unseqTimestamp, timeValuePair.getValue());
          }
          // write seq point
          if (!hasOverWritenSeqPoint) {
            chunkWriter.write(seqTimestamp, batchData.currentValue());
          }
          batchData.next();
        }
      } else {
        // do not overlap with this page, then flush it to chunkWriter directly

      }
    }
  }

  private boolean isChunkOverlap(
      TimeValuePair timeValuePair,
      ChunkMetadata metaData,
      boolean isLastChunk,
      long currentResourceEndTime) {
    return timeValuePair != null
        && (timeValuePair.getTimestamp() <= metaData.getEndTime()
            || (isLastChunk && timeValuePair.getTimestamp() <= currentResourceEndTime));
  }

  /**
   * -1 means that no data on this page has been deleted. <br>
   * 0 means that there is data on this page been deleted. <br>
   * 1 means that all data on this page has been deleted.
   */
  private int isPageModified(PageHeader pageHeader, List<TimeRange> deleteIntervalList) {
    if (deleteIntervalList != null) {
      for (TimeRange range : deleteIntervalList) {
        if (range.contains(pageHeader.getStartTime(), pageHeader.getEndTime())) {
          // all data on this page has been deleted
          return 1;
        }
        if (range.overlaps(new TimeRange(pageHeader.getStartTime(), pageHeader.getEndTime()))) {
          // exist data on this page been deleted
          pageHeader.setModified(true);
          return 0;
        }
      }
      return -1;
    }
  }
}
