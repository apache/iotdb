package org.apache.iotdb.db.engine.compaction.writer;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.rescon.SystemInfo;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;
import java.util.List;

public abstract class AbstractInnerCompactionWriter extends AbstractCompactionWriter {
  protected TsFileIOWriter fileWriter;

  protected boolean isEmptyFile;

  protected TsFileResource targetResource;

  public AbstractInnerCompactionWriter(TsFileResource targetFileResource) throws IOException {
    long sizeForFileWriter =
        (long)
            (SystemInfo.getInstance().getMemorySizeForCompaction()
                / IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread()
                * IoTDBDescriptor.getInstance()
                    .getConfig()
                    .getChunkMetadataSizeProportionInCompaction());
    this.fileWriter = new TsFileIOWriter(targetFileResource.getTsFile(), true, sizeForFileWriter);
    this.targetResource = targetFileResource;
    isEmptyFile = true;
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
  public void endMeasurement(int subTaskId) throws IOException {
    flushChunkToFileWriter(fileWriter, chunkWriters[subTaskId]);
  }

  @Override
  public abstract void write(long timestamp, Object value, int subTaskId) throws IOException;

  @Override
  public abstract void write(TimeColumn timestamps, Column[] columns, int subTaskId, int batchSize)
      throws IOException;

  @Override
  public void endFile() throws IOException {
    fileWriter.endFile();
    if (isEmptyFile) {
      fileWriter.getFile().delete();
    }
  }

  @Override
  public void close() throws Exception {
    if (fileWriter != null && fileWriter.canWrite()) {
      fileWriter.close();
    }
    fileWriter = null;
  }

  @Override
  public void checkAndMayFlushChunkMetadata() throws IOException {
    // Before flushing chunk metadatas, we use chunk metadatas in tsfile io writer to update start
    // time and end time in resource.
    List<TimeseriesMetadata> timeseriesMetadatasOfCurrentDevice =
        fileWriter.getDeviceTimeseriesMetadataMap().get(deviceId);
    if (timeseriesMetadatasOfCurrentDevice != null) {
      // this target file contains current device
      for (TimeseriesMetadata timeseriesMetadata : timeseriesMetadatasOfCurrentDevice) {
        targetResource.updateStartTime(deviceId, timeseriesMetadata.getStatistics().getStartTime());
        targetResource.updateEndTime(deviceId, timeseriesMetadata.getStatistics().getEndTime());
      }
    }
    fileWriter.checkMetadataSizeAndMayFlush();
  }
}
