package org.apache.iotdb.db.engine.compaction.writer;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TimeValuePair;
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

public class NewFastCrossCompactionWriter implements AutoCloseable {
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

  // Each sub task has point count in current measurment, which is used to check size.
  // The index of the array corresponds to subTaskId.
  private final long[] measurementPointCountArray = new long[subTaskNum];

  // Each sub task has its corresponding seq file index.
  // The index of the array corresponds to subTaskId.
  private int[] seqFileIndexArray = new int[subTaskNum];

  // current chunk group header size
  private int chunkGroupHeaderSize;

  public NewFastCrossCompactionWriter(List<TsFileResource> targetResources) throws IOException {
    for (TsFileResource resource : targetResources) {
      this.targetFileWriters.add(new TsFileIOWriter(resource.getTsFile()));
    }
    isDeviceExistedInTargetFiles = new boolean[targetResources.size()];
  }

  public void startChunkGroup(String deviceId, boolean isAlign) throws IOException {
    this.deviceId = deviceId;
    this.isAlign = isAlign;
    for (TsFileIOWriter writer : targetFileWriters) {
      chunkGroupHeaderSize = writer.startChunkGroup(deviceId);
    }
  }

  public void endChunkGroup() throws IOException {
    for (int i = 0; i < targetFileWriters.size(); i++) {
      TsFileIOWriter targetFileWriter = targetFileWriters.get(i);
      if (isDeviceExistedInTargetFiles[i]) {
        targetFileWriter.endChunkGroup();
      } else {
        targetFileWriter.truncate(targetFileWriter.getPos() - chunkGroupHeaderSize);
      }
      isDeviceExistedInTargetFiles[i] = false;
    }
    deviceId = null;
  }

  public void startMeasurement(List<IMeasurementSchema> measurementSchemaList, int subTaskId) {
    if (isAlign) {
      chunkWriters[subTaskId] = new AlignedChunkWriterImpl(measurementSchemaList);
    } else {
      chunkWriters[subTaskId] = new ChunkWriterImpl(measurementSchemaList.get(0), true);
    }
  }

  public void endMeasurement(int subTaskId) throws IOException {
    CompactionWriterUtils.flushChunkToFileWriter(
        targetFileWriters.get(seqFileIndexArray[subTaskId]), chunkWriters[subTaskId]);
    seqFileIndexArray[subTaskId] = 0;
  }

  public void writeTimeValue(TimeValuePair timeValuePair, int subTaskId) throws IOException {
    CompactionWriterUtils.writeTVPair(timeValuePair, chunkWriters[subTaskId], null, true);
  }

  public void flushChunkToFileWriter(ChunkMetadata chunkMetadata, int subTaskId)
      throws IOException {
    TsFileIOWriter tsFileIOWriter = targetFileWriters.get(seqFileIndexArray[subTaskId]);
    // seal last chunk to file writer
    // Todo: may cause small chunk
    chunkWriters[subTaskId].writeToFileWriter(tsFileIOWriter);

    Chunk chunk = ChunkCache.getInstance().get(chunkMetadata);
    synchronized (tsFileIOWriter) {
      tsFileIOWriter.writeChunk(chunk, chunkMetadata);
    }
  }

  public void flushPageToChunkWriter(
      ByteBuffer compressedPageData, PageHeader pageHeader, int subTaskId)
      throws IOException, PageException {
    ChunkWriterImpl chunkWriter = (ChunkWriterImpl) chunkWriters[subTaskId];
    // seal current page
    chunkWriter.sealCurrentPage();
    // flush new page to chunk writer directly
    // Todo: may cause small page
    chunkWriter.writePageHeaderAndDataIntoBuff(compressedPageData, pageHeader);

    // check chunk size and may open a new chunk
    CompactionWriterUtils.checkChunkSizeAndMayOpenANewChunk(
        targetFileWriters.get(seqFileIndexArray[subTaskId]), chunkWriter, true);
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

  public List<TsFileIOWriter> getFileIOWriter() {
    return targetFileWriters;
  }
}
