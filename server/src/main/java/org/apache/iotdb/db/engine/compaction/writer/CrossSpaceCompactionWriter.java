package org.apache.iotdb.db.engine.compaction.writer;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CrossSpaceCompactionWriter extends AbstractCompactionWriter {
  // target fileIOWriter
  private List<TsFileIOWriter> fileWriterList = new ArrayList<>();
  // old source tsfile
  private List<TsFileResource> seqTsFileResources;

  private int seqFileIndex;

  public CrossSpaceCompactionWriter(
      List<TsFileResource> targetResources, List<TsFileResource> seqFileResources)
      throws IOException {
    for (TsFileResource resource : targetResources) {
      this.fileWriterList.add(new RestorableTsFileIOWriter(resource.getTsFile()));
    }
    this.seqTsFileResources = seqFileResources;
    seqFileIndex = 0;
  }

  @Override
  public void startChunkGroup(String deviceId, boolean isAlign) throws IOException {
    this.deviceId = deviceId;
    this.isAlign = isAlign;
    // Todo:若某个文件里的该设备的数据都被删光了，则可能出现空ChunkGroup
    for (TsFileResource resource : seqTsFileResources) {
      if (resource.isDeviceIdExist(deviceId)) {
        fileWriterList.get(seqFileIndex).startChunkGroup(deviceId);
      }
      seqFileIndex++;
    }
    seqFileIndex = 0;
  }

  @Override
  public void endChunkGroup() throws IOException {
    for (TsFileResource resource : seqTsFileResources) {
      if (resource.isDeviceIdExist(deviceId)) {
        fileWriterList.get(seqFileIndex).endChunkGroup();
      }
      seqFileIndex++;
    }
    seqFileIndex = 0;
    deviceId = null;
  }

  @Override
  public void endMeasurement() throws IOException {
    writeRateLimit(chunkWriter.estimateMaxSeriesMemSize());
    chunkWriter.writeToFileWriter(fileWriterList.get(seqFileIndex));
    chunkWriter = null;
    seqFileIndex = 0;
  }

  @Override
  public void write(long timestamp, Object value) throws IOException {
    // if timestamp is later than the current source seq tsfile, than flush chunk writer
    while (timestamp > seqTsFileResources.get(seqFileIndex).getEndTime(deviceId)) {
      writeRateLimit(chunkWriter.estimateMaxSeriesMemSize());
      chunkWriter.writeToFileWriter(fileWriterList.get(seqFileIndex++));
    }
    if (checkChunkSizeAndMayOpenANewChunk()) {
      writeRateLimit(chunkWriter.estimateMaxSeriesMemSize());
      chunkWriter.writeToFileWriter(fileWriterList.get(seqFileIndex));
      fileWriterList.get(seqFileIndex).endChunkGroup();
      fileWriterList.get(seqFileIndex).startChunkGroup(deviceId);
    }
    writeDataPoint(timestamp, value);
  }

  @Override
  public void write(long[] timestamps, Object values) {}

  @Override
  public void endFile() throws IOException {
    for (TsFileIOWriter targetWriter : fileWriterList) {
      targetWriter.endFile();
    }
  }

  @Override
  public void close() throws IOException {
    for (TsFileIOWriter targetWriter : fileWriterList) {
      if (targetWriter != null && targetWriter.canWrite()) {
        targetWriter.close();
      }
    }
    fileWriterList = null;
    seqTsFileResources = null;
    chunkWriter = null;
  }

  public List<TsFileIOWriter> getFileWriters() {
    return this.fileWriterList;
  }
}
