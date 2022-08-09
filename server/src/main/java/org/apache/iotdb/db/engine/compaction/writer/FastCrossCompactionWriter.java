package org.apache.iotdb.db.engine.compaction.writer;

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
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class FastCrossCompactionWriter implements AutoCloseable {
  List<TsFileIOWriter> targetFileWriters = new ArrayList<>();

  // Each sub task has its own chunk writer.
  // The index of the array corresponds to subTaskId.
  protected IChunkWriter[] chunkWriters = new IChunkWriter[4];

  private boolean isAlign;

  // whether each target file has device data or not
  private final boolean[] isDeviceExistedInTargetFiles;

  public FastCrossCompactionWriter(List<TsFileResource> targetResources) throws IOException {
    for (int i = 0; i < targetResources.size(); i++) {
      this.targetFileWriters.add(new TsFileIOWriter(targetResources.get(i).getTsFile()));
    }
    isDeviceExistedInTargetFiles = new boolean[targetResources.size()];
  }

  public void startChunkGroup(int targetFileIndex, String deviceId) throws IOException {
    targetFileWriters.get(targetFileIndex).startChunkGroup(deviceId);
  }

  public void endChunkGroup(int targetFileIndex) throws IOException {
    targetFileWriters.get(targetFileIndex).endChunkGroup();
  }

  public void startMeasurement(
      List<IMeasurementSchema> measurementSchemaList, boolean isAlign, int subTaskId) {
    this.isAlign = isAlign;
    if (isAlign) {
      chunkWriters[subTaskId] = new AlignedChunkWriterImpl(measurementSchemaList);
    } else {
      chunkWriters[subTaskId] = new ChunkWriterImpl(measurementSchemaList.get(0), true);
    }
  }

  public void endMeasurment(int targetFileIndex, int subTaskId) throws IOException {
    flushChunkToFileWriter(targetFileWriters.get(targetFileIndex), subTaskId);
    chunkWriters[subTaskId] = null;
  }

  public void endFile() throws IOException {
    for (int i = 0; i < targetFileWriters.size(); i++) {
      targetFileWriters.get(i).endFile();
    }
  }

  public List<TsFileIOWriter> getFileIOWriter() {
    return targetFileWriters;
  }

  protected void flushChunkToFileWriter(TsFileIOWriter targetWriter, int subTaskId)
      throws IOException {
    // writeRateLimit(chunkWriters[subTaskId].estimateMaxSeriesMemSize());
    synchronized (targetWriter) {
      chunkWriters[subTaskId].writeToFileWriter(targetWriter);
    }
  }

  public void compactWithNonOverlapSeqChunk(
      Chunk seqChunk,
      boolean isChunkModified,
      ChunkMetadata seqChunkMetadata,
      int targetFileIndex,
      int subTaskId)
      throws IOException, PageException {
    if (!isChunkModified) {
      // flush chunk to tsfileIOWriter directly
      TsFileIOWriter tsFileIOWriter = targetFileWriters.get(targetFileIndex);
      synchronized (tsFileIOWriter) {
        tsFileIOWriter.writeChunk(seqChunk, seqChunkMetadata);
      }
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
              CompactionWriterUtils.writeDataPoint(
                  batchData.currentTime(),
                  batchData.currentValue(),
                  isAlign,
                  chunkWriters[subTaskId]);
              batchData.next();
            }
            break;
          case -1:
            // no data on this page has been deleted, flush this page to chunkWriter directly
            flushPageToChunkWriter(chunkDataBuffer, pageHeader, subTaskId);
            break;
        }
      }
    }
    isDeviceExistedInTargetFiles[targetFileIndex] = true;
  }

  public void compactWithOverlapSeqChunk(Chunk seqChunk, IPointReader unseqReader, int subTaskId)
      throws IOException, PageException {
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
        CompactionWriterUtils.writeTVPair(timeValuePair, chunkWriters[subTaskId]);
      }

      // 2. compact with this seq page, write point.time < seq page.endTime
      if (unseqReader.currentTimeValuePair().getTimestamp() <= pageHeader.getEndTime()) {
        // overlap with this page, then deserialize it and compact with unseq data point
        PageReader pageReader = chunkReader.constructPageReaderForNextPage(pageHeader);
        BatchData batchData = pageReader.getAllSatisfiedPageData();
        while (batchData.hasCurrent()) {
          boolean hasOverWritenSeqPoint = false;
          long seqTimestamp = batchData.currentTime();
          while (unseqReader.hasNextTimeValuePair()
              && unseqReader.currentTimeValuePair().getTimestamp() <= seqTimestamp) {
            // write unseq point
            TimeValuePair timeValuePair = unseqReader.nextTimeValuePair();
            long unseqTimestamp = timeValuePair.getTimestamp();
            if (unseqTimestamp == seqTimestamp) {
              hasOverWritenSeqPoint = true;
            }
            CompactionWriterUtils.writeTVPair(timeValuePair, chunkWriters[subTaskId]);
          }
          // write seq point
          if (!hasOverWritenSeqPoint) {
            CompactionWriterUtils.writeDataPoint(
                seqTimestamp, batchData.currentValue(), isAlign, chunkWriters[subTaskId]);
          }
          batchData.next();
        }
      } else {
        // do not overlap with this seq page, then flush it to chunkWriter directly
        flushPageToChunkWriter(chunkDataBuffer, pageHeader, subTaskId);
      }
    }
  }

  public void writeRemainingUnseqPoints(IPointReader unseqReader, long timeLimit, int subTaskId)
      throws IOException {
    while (unseqReader.hasNextTimeValuePair()
        && unseqReader.currentTimeValuePair().getTimestamp() <= timeLimit) {
      TimeValuePair timeValuePair = unseqReader.nextTimeValuePair();
      CompactionWriterUtils.writeTVPair(timeValuePair, chunkWriters[subTaskId]);
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
    ((ChunkWriterImpl) chunkWriters[subTaskId])
        .writePageHeaderAndDataIntoBuff(ByteBuffer.wrap(compressedPageBody), pageHeader);
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
    }
    return -1;
  }

  @Override
  public void close() throws Exception {
    for (TsFileIOWriter targetWriter : targetFileWriters) {
      if (targetWriter != null && targetWriter.canWrite()) {
        targetWriter.close();
      }
    }
    targetFileWriters = null;
  }
}
