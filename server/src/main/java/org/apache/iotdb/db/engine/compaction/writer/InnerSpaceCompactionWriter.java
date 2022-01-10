package org.apache.iotdb.db.engine.compaction.writer;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;

public class InnerSpaceCompactionWriter extends AbstractCompactionWriter {
  private TsFileIOWriter fileWriter;

  public InnerSpaceCompactionWriter(TsFileResource targetFileResource) throws IOException {
    fileWriter = new RestorableTsFileIOWriter(targetFileResource.getTsFile());
  }

  @Override
  public void startChunkGroup(String deviceId, boolean isAlign) throws IOException {
    fileWriter.startChunkGroup(deviceId);
    this.isAlign = isAlign;
    this.deviceId = deviceId;
  }

  @Override
  public void endChunkGroup() throws IOException {
    fileWriter.endChunkGroup();
  }

  @Override
  public void endMeasurement() throws IOException {
    writeRateLimit(chunkWriter.estimateMaxSeriesMemSize());
    chunkWriter.writeToFileWriter(fileWriter);
    chunkWriter = null;
  }

  @Override
  public void write(long timestamp, Object value) throws IOException {
    if (checkChunkSizeAndMayOpenANewChunk()) {
      writeRateLimit(chunkWriter.estimateMaxSeriesMemSize());
      chunkWriter.writeToFileWriter(fileWriter);
      fileWriter.endChunkGroup();
      fileWriter.startChunkGroup(deviceId);
    }
    writeDataPoint(timestamp, value);
  }

  @Override
  public void write(long[] timestamps, Object values) {}

  @Override
  public void endFile() throws IOException {
    fileWriter.endFile();
  }

  @Override
  public void close() throws IOException {
    if (fileWriter != null && fileWriter.canWrite()) {
      fileWriter.close();
    }
    chunkWriter = null;
    fileWriter = null;
  }

  public TsFileIOWriter getFileWriter() {
    return this.fileWriter;
  }
}
