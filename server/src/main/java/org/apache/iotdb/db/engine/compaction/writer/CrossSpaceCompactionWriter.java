package org.apache.iotdb.db.engine.compaction.writer;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
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

  // Todo:现在跨空间合并可能存在目标文件里某个对齐序列全部都是空valueChunk，即该序列的TimeseriesMetadata里的statistics里标记数据为空，因此查询的时候会出现死循环，即firstTimeseriesMetadata不为null，可是在解它到ChunkMetadataList却发现起始结束时间不对
  @Override
  public void write(long timestamp, Object value) throws IOException {
    // if timestamp is later than the current source seq tsfile, than flush chunk writer
    // 若该文件不存在该设备，则会返回Long.Min。因此此处要判断若该文件不存在该device则将seqFileIndex++即可。
    if (IoTDBDescriptor.getInstance().getConfig().getTimeIndexLevel().ordinal() == 1) {
      // device time index
      while (timestamp > seqTsFileResources.get(seqFileIndex).getEndTime(deviceId)) {
        writeRateLimit(chunkWriter.estimateMaxSeriesMemSize());
        chunkWriter.writeToFileWriter(fileWriterList.get(seqFileIndex++));
      }
    } else {
      // Todo
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
