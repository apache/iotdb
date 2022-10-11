package org.apache.iotdb.db.engine.compaction.writer;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class AbstractCrossCompactionWriter extends AbstractCompactionWriter {
  // Each sub task has point count in current measurment, which is used to check size.
  // The index of the array corresponds to subTaskId.
  protected int[] measurementPointCountArray = new int[subTaskNum];

  // target fileIOWriters
  protected List<TsFileIOWriter> targetFileWriters = new ArrayList<>();

  // source tsfiles
  private List<TsFileResource> seqTsFileResources;

  // Each sub task has its corresponding seq file index.
  // The index of the array corresponds to subTaskId.
  protected int[] seqFileIndexArray = new int[subTaskNum];

  // device end time in each source seq file
  protected final long[] currentDeviceEndTime;

  protected boolean isAlign;

  protected String deviceId;

  // whether each target file is empty or not
  protected final boolean[] isEmptyFile;

  // whether each target file has device data or not
  protected final boolean[] isDeviceExistedInTargetFiles;

  // current chunk group header size
  private int chunkGroupHeaderSize;

  protected List<TsFileResource> targetResources;

  private long lastCheckIndex = 0;

  public AbstractCrossCompactionWriter(
      List<TsFileResource> targetResources, List<TsFileResource> seqFileResources)
      throws IOException {
    currentDeviceEndTime = new long[seqFileResources.size()];
    isEmptyFile = new boolean[seqFileResources.size()];
    isDeviceExistedInTargetFiles = new boolean[targetResources.size()];
    for (int i = 0; i < targetResources.size(); i++) {
      this.targetFileWriters.add(new TsFileIOWriter(targetResources.get(i).getTsFile()));
      isEmptyFile[i] = true;
    }
    this.seqTsFileResources = seqFileResources;
    this.targetResources = targetResources;
  }

  @Override
  public void startChunkGroup(String deviceId, boolean isAlign) throws IOException {
    this.deviceId = deviceId;
    this.isAlign = isAlign;
    this.seqFileIndexArray = new int[subTaskNum];
    checkIsDeviceExistAndGetDeviceEndTime();
    for (int i = 0; i < targetFileWriters.size(); i++) {
      chunkGroupHeaderSize = targetFileWriters.get(i).startChunkGroup(deviceId);
    }
  }

  @Override
  public void endChunkGroup() throws IOException {
    for (int i = 0; i < seqTsFileResources.size(); i++) {
      TsFileIOWriter targetFileWriter = targetFileWriters.get(i);
      if (isDeviceExistedInTargetFiles[i]) {
        targetFileWriter.endChunkGroup();
      } else {
        targetFileWriter.truncate(targetFileWriter.getPos() - chunkGroupHeaderSize);
      }
      isDeviceExistedInTargetFiles[i] = false;
    }
    deviceId = null;
    seqFileIndexArray = null;
  }

  @Override
  public void startMeasurement(List<IMeasurementSchema> measurementSchemaList, int subTaskId) {
    measurementPointCountArray[subTaskId] = 0;
    if (isAlign) {
      chunkWriters[subTaskId] = new AlignedChunkWriterImpl(measurementSchemaList);
    } else {
      chunkWriters[subTaskId] = new ChunkWriterImpl(measurementSchemaList.get(0), true);
    }
  }

  @Override
  public void endMeasurement(int subTaskId) throws IOException {
    flushChunkToFileWriter(
        targetFileWriters.get(seqFileIndexArray[subTaskId]), chunkWriters[subTaskId]);
    seqFileIndexArray[subTaskId] = 0;
  }

  @Override
  public void write(long timestamp, Object value, int subTaskId) throws IOException {
    int fileIndex = seqFileIndexArray[subTaskId];
    checkTimeAndMayFlushChunkToCurrentFile(timestamp, subTaskId);
    long curCheckIndex = ++measurementPointCountArray[subTaskId] / checkPoint;
    writeDataPoint(
        timestamp,
        value,
        chunkWriters[subTaskId],
        curCheckIndex > lastCheckIndex ? targetFileWriters.get(fileIndex) : null,
        true);
    isDeviceExistedInTargetFiles[fileIndex] = true;
    isEmptyFile[fileIndex] = false;
    lastCheckIndex = curCheckIndex;
  }

  @Override
  public abstract void write(TimeColumn timestamps, Column[] columns, int subTaskId, int batchSize)
      throws IOException;

  @Override
  public void endFile() throws IOException {
    for (int i = 0; i < isEmptyFile.length; i++) {
      targetFileWriters.get(i).endFile();
      // delete empty target file
      if (isEmptyFile[i]) {
        targetFileWriters.get(i).getFile().delete();
      }
    }
  }

  @Override
  public void close() throws IOException {
    for (TsFileIOWriter targetWriter : targetFileWriters) {
      if (targetWriter != null && targetWriter.canWrite()) {
        targetWriter.close();
      }
    }
    targetFileWriters = null;
    seqTsFileResources = null;
  }

  /**
   * Find the index of the target file to be inserted according to the data time. Notice: unsealed
   * chunk should be flushed to current file before moving target file index.<br>
   * If the seq file is deleted for various reasons, the following two situations may occur when
   * selecting the source files: (1) unseq files may have some devices or measurements which are not
   * exist in seq files. (2) timestamp of one timeseries in unseq files may later than any seq
   * files. Then write these data into the last target file.
   */
  protected void checkTimeAndMayFlushChunkToCurrentFile(long timestamp, int subTaskId)
      throws IOException {
    int fileIndex = seqFileIndexArray[subTaskId];
    boolean hasFlushedCurrentChunk = false;
    // if timestamp is later than the current source seq tsfile, then flush chunk writer and move to
    // next file
    while (timestamp > currentDeviceEndTime[fileIndex]
        && fileIndex != seqTsFileResources.size() - 1) {
      if (!hasFlushedCurrentChunk) {
        // flush chunk to current file before moving target file index
        flushChunkToFileWriter(targetFileWriters.get(fileIndex), chunkWriters[subTaskId]);
        hasFlushedCurrentChunk = true;
      }
      seqFileIndexArray[subTaskId] = ++fileIndex;
    }
  }

  @Override
  public List<TsFileIOWriter> getFileIOWriter() {
    return targetFileWriters;
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

  @Override
  public void updateStartTimeAndEndTime(String device, long time, int subTaskId) {
    synchronized (this) {
      int fileIndex = seqFileIndexArray[subTaskId];
      TsFileResource resource = targetResources.get(fileIndex);
      // we need to synchronized here to avoid multi-thread competition in sub-task
      resource.updateStartTime(device, time);
      resource.updateEndTime(device, time);
    }
  }
}
