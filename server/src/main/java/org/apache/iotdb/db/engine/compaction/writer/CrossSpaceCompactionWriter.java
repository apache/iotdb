package org.apache.iotdb.db.engine.compaction.writer;

import org.apache.iotdb.db.engine.compaction.cross.inplace.manage.MergeManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CrossSpaceCompactionWriter implements ICompactionWriter {
  // target file path -> file writer
  private List<TsFileIOWriter> fileWriterList = new ArrayList<>();
  // old source tsfile
  private List<TsFileResource> seqTsFileResources;

  private IChunkWriter chunkWriter;

  private int seqFileIndex;

  private String deviceId;

  private boolean isAlign;

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
    for (TsFileResource resource : seqTsFileResources) {
      if (resource.isDeviceIdExist(deviceId)) {
        fileWriterList.get(seqFileIndex).startChunkGroup(deviceId);
      }
    }
  }

  @Override
  public void endChunkGroup() throws IOException {
    for (TsFileResource resource : seqTsFileResources) {
      if (resource.isDeviceIdExist(deviceId)) {
        fileWriterList.get(seqFileIndex).endChunkGroup();
      }
    }
    deviceId = null;
  }

  @Override
  public void startMeasurement(List<IMeasurementSchema> measurementSchemaList) {
    if (isAlign) {
      chunkWriter = new AlignedChunkWriterImpl(measurementSchemaList);
    } else {
      chunkWriter = new ChunkWriterImpl(measurementSchemaList.get(0), true);
    }
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
    // if timestamp is later than current source seq tsfile, than flush chunk writer
    while (timestamp > seqTsFileResources.get(seqFileIndex).getEndTime(deviceId)) {
      writeRateLimit(chunkWriter.estimateMaxSeriesMemSize());
      chunkWriter.writeToFileWriter(fileWriterList.get(seqFileIndex));
      seqFileIndex++;
    }
    if (!isAlign) {
      ChunkWriterImpl chunkWriter = (ChunkWriterImpl) this.chunkWriter;
      switch (chunkWriter.getDataType()) {
        case TEXT:
          chunkWriter.write(timestamp, (Binary) value);
          break;
        case DOUBLE:
          chunkWriter.write(timestamp, (Double) value);
          break;
        case BOOLEAN:
          chunkWriter.write(timestamp, (Boolean) value);
          break;
        case INT64:
          chunkWriter.write(timestamp, (Long) value);
          break;
        case INT32:
          chunkWriter.write(timestamp, (Integer) value);
          break;
        case FLOAT:
          chunkWriter.write(timestamp, (Float) value);
          break;
        default:
          throw new UnsupportedOperationException("Unknown data type " + chunkWriter.getDataType());
      }
    } else {
      value = ((TsPrimitiveType[]) value)[0].getValue();
      TSDataType tsDataType = ((TsPrimitiveType[]) value)[0].getDataType();
      boolean isNull = value == null;
      AlignedChunkWriterImpl chunkWriter = (AlignedChunkWriterImpl) this.chunkWriter;
      switch (tsDataType) {
        case TEXT:
          chunkWriter.write(timestamp, (Binary) value, isNull);
          break;
        case DOUBLE:
          chunkWriter.write(timestamp, (Double) value, isNull);
          break;
        case BOOLEAN:
          chunkWriter.write(timestamp, (Boolean) value, isNull);
          break;
        case INT64:
          chunkWriter.write(timestamp, (Long) value, isNull);
          break;
        case INT32:
          chunkWriter.write(timestamp, (Integer) value, isNull);
          break;
        case FLOAT:
          chunkWriter.write(timestamp, (Float) value, isNull);
          break;
        default:
          throw new UnsupportedOperationException("Unknown data type " + tsDataType);
      }
    }
    if (chunkWriter.estimateMaxSeriesMemSize() > 2 * 1024 * 1024) { // Todo:
      writeRateLimit(chunkWriter.estimateMaxSeriesMemSize());
      chunkWriter.writeToFileWriter(fileWriterList.get(seqFileIndex));
    }
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

  private static void writeRateLimit(long bytesLength) {
    MergeManager.mergeRateLimiterAcquire(
        MergeManager.getINSTANCE().getMergeWriteRateLimiter(), bytesLength);
  }

  public List<TsFileIOWriter> getFileWriters() {
    return this.fileWriterList;
  }
}
