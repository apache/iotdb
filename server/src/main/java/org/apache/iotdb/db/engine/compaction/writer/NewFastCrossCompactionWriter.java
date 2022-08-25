package org.apache.iotdb.db.engine.compaction.writer;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class NewFastCrossCompactionWriter extends AbstractCrossCompactionWriter {
  private static final int subTaskNum =
      IoTDBDescriptor.getInstance().getConfig().getSubCompactionTaskNum();

  List<TsFileIOWriter> targetFileWriters = new ArrayList<>();

  // Each sub task has its own chunk writer.
  // The index of the array corresponds to subTaskId.
  private final IChunkWriter[] chunkWriters = new IChunkWriter[subTaskNum];

  // whether each target file has device data or not
  private final boolean[] isDeviceExistedInTargetFiles;

  // device end time in each source seq file
  private final long[] currentDeviceEndTime;

  // whether each target file is empty or not
  private final boolean[] isEmptyFile;

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
    currentDeviceEndTime = new long[targetResources.size()];
    isDeviceExistedInTargetFiles = new boolean[targetResources.size()];
    isEmptyFile = new boolean[targetResources.size()];
    for (int i = 0; i < targetResources.size(); i++) {
      this.targetFileWriters.add(new TsFileIOWriter(targetResources.get(i).getTsFile()));
      isEmptyFile[i] = true;
    }
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

  @Override
  public void write(long timestamp, Object value, int subTaskId) throws IOException {}

  @Override
  public void write(TimeColumn timestamps, Column[] columns, int subTaskId, int batchSize)
      throws IOException {}

  public void writeTimeValue(TimeValuePair timeValuePair, int subTaskId) throws IOException {
    CompactionWriterUtils.writeTVPair(timeValuePair, chunkWriters[subTaskId], null, true);
    isDeviceExistedInTargetFiles[seqFileIndexArray[subTaskId]] = true;
    isEmptyFile[seqFileIndexArray[subTaskId]] = false;
  }

  public void flushChunkToFileWriter(ChunkMetadata chunkMetadata, int subTaskId)
      throws IOException {
    int fileIndex = seqFileIndexArray[subTaskId];
    TsFileIOWriter tsFileIOWriter = targetFileWriters.get(fileIndex);

    synchronized (tsFileIOWriter) {
      // seal last chunk to file writer
      // Todo: may cause small chunk
      chunkWriters[subTaskId].writeToFileWriter(tsFileIOWriter);
      Chunk chunk = ChunkCache.getInstance().get(chunkMetadata);
      tsFileIOWriter.writeChunk(chunk, chunkMetadata);
    }
    isDeviceExistedInTargetFiles[fileIndex] = true;
    isEmptyFile[fileIndex] = false;
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

    int fileIndex = seqFileIndexArray[subTaskId];
    // check chunk size and may open a new chunk
    CompactionWriterUtils.checkChunkSizeAndMayOpenANewChunk(
        targetFileWriters.get(fileIndex), chunkWriter, true);
    isDeviceExistedInTargetFiles[fileIndex] = true;
    isEmptyFile[fileIndex] = false;
  }

  private void checkTimeAndMayFlushChunkToCurrentFile(long timestamp, int subTaskId)
      throws IOException {
    int fileIndex = seqFileIndexArray[subTaskId];
    // if timestamp is later than the current source seq tsfile, than flush chunk writer
    while (timestamp > currentDeviceEndTime[fileIndex]) {
      if (fileIndex != seqTsFileResources.size() - 1) {
        CompactionWriterUtils.flushChunkToFileWriter(
            fileWriterList.get(fileIndex), chunkWriters[subTaskId]);
        seqFileIndexArray[subTaskId] = ++fileIndex;
      } else {
        // If the seq file is deleted for various reasons, the following two situations may occur
        // when selecting the source files: (1) unseq files may have some devices or measurements
        // which are not exist in seq files. (2) timestamp of one timeseries in unseq files may
        // later than any seq files. Then write these data into the last target file.
        return;
      }
    }
  }

  private void checkIsDeviceExistAndGetDeviceEndTime() throws IOException {
    int fileIndex = 0;
    while (fileIndex < seqTsFileResources.size()) {
      if (seqTsFileResources.get(fileIndex).getTimeIndexType() == 1) {
        // the timeIndexType of resource is deviceTimeIndex
        currentDeviceEndTime[fileIndex] = seqTsFileResources.get(fileIndex).getEndTime(deviceId);
      } else {
        long endTime = Long.MIN_VALUE;
        Map<String, TimeseriesMetadata> deviceMetadataMap =
            FileReaderManager.getInstance()
                .get(seqTsFileResources.get(fileIndex).getTsFilePath(), true)
                .readDeviceMetadata(deviceId);
        for (Map.Entry<String, TimeseriesMetadata> entry : deviceMetadataMap.entrySet()) {
          long tmpStartTime = entry.getValue().getStatistics().getStartTime();
          long tmpEndTime = entry.getValue().getStatistics().getEndTime();
          if (tmpEndTime >= tmpStartTime && endTime < tmpEndTime) {
            endTime = tmpEndTime;
          }
        }
        currentDeviceEndTime[fileIndex] = endTime;
      }

      fileIndex++;
    }
  }

  public void endFile() throws IOException {
    for (int i = 0; i < targetFileWriters.size(); i++) {
      targetFileWriters.get(i).endFile();
      // delete empty target file
      //      if (isEmptyFile[i]) {
      //        targetFileWriters.get(i).getFile().delete();
      //      }
    }
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
