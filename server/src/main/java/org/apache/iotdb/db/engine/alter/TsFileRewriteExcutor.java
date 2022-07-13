package org.apache.iotdb.db.engine.alter;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.TsFileDeviceIterator;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/** This class is used to rewrite one tsfile with current encoding & compressionType. */
public class TsFileRewriteExcutor {

  private static final Logger logger = LoggerFactory.getLogger(TsFileRewriteExcutor.class);

  private TsFileResource tsFileResource;
  private TsFileResource targetTsFileResource;
  private PartialPath fullPath;
  private TSEncoding curEncoding;
  private CompressionType curCompressionType;
  private long timePartition;
  private boolean sequence;
  // record the min time and max time to update the target resource
  private long minStartTimestamp = Long.MAX_VALUE;
  private long maxEndTimestamp = Long.MIN_VALUE;

  public TsFileRewriteExcutor(
      TsFileResource tsFileResource,
      TsFileResource targetTsFileResource,
      PartialPath fullPath,
      TSEncoding curEncoding,
      CompressionType curCompressionType,
      long timePartition,
      boolean sequence) {
    this.tsFileResource = tsFileResource;
    this.targetTsFileResource = targetTsFileResource;
    this.fullPath = fullPath;
    this.curEncoding = curEncoding;
    this.curCompressionType = curCompressionType;
    this.timePartition = timePartition;
    this.sequence = sequence;
  }

  /** This function execute the rewrite task */
  public void execute() throws IOException {

    tsFileResource.tryReadLock();
    try (TsFileSequenceReader reader =
            new TsFileSequenceReader(tsFileResource.getTsFilePath());
        TsFileIOWriter writer = new TsFileIOWriter(targetTsFileResource.getTsFile())) {
      // read devices
      TsFileDeviceIterator deviceIterator = reader.getAllDevicesIteratorWithIsAligned();
      while (deviceIterator.hasNext()) {
        minStartTimestamp = Long.MAX_VALUE;
        maxEndTimestamp = Long.MIN_VALUE;
        Pair<String, Boolean> deviceInfo = deviceIterator.next();
        String device = deviceInfo.left;
        boolean aligned = deviceInfo.right;
        // write chunkGroup header
        writer.startChunkGroup(device);
        // write chunk & page data
        if (aligned) {
          // TODO
          List<AlignedChunkMetadata> alignedChunkMetadata = reader.getAlignedChunkMetadata(device);
        } else {
          rewriteNotAligned(device, reader, writer);
        }
        // chunkGroup end
        writer.endChunkGroup();

        targetTsFileResource.updateStartTime(device, minStartTimestamp);
        targetTsFileResource.updateEndTime(device, maxEndTimestamp);
      }

      targetTsFileResource.updatePlanIndexes(tsFileResource);
      // write index,bloom,footer, end file
      writer.endFile();
      targetTsFileResource.close();
    } finally {
      tsFileResource.readUnlock();
    }
  }

  private void rewriteNotAligned(String device, TsFileSequenceReader reader, TsFileIOWriter writer)
      throws IOException {
    Map<String, List<ChunkMetadata>> measurementMap = reader.readChunkMetadataInDevice(device);
    if (measurementMap == null) {
      logger.warn("[alter timeseries] device({}) measurementMap is null", device);
      return;
    }
    measurementMap.forEach(
        (measurementId, chunkMetadatas) -> {
          if (chunkMetadatas == null || chunkMetadatas.isEmpty()) {
            logger.warn("[alter timeseries] empty measurement({})", measurementId);
            return;
          }
          // target chunk writer
          ChunkWriterImpl chunkWriter =
              new ChunkWriterImpl(
                  new MeasurementSchema(
                      measurementId,
                      chunkMetadatas.get(0).getDataType(),
                      curEncoding,
                      curCompressionType));
          chunkMetadatas.forEach(
              chunkMetadata -> {
                Chunk currentChunk = null;
                try {
                  // old mem chunk
                  currentChunk = reader.readMemChunk(chunkMetadata);
                  IChunkReader chunkReader = new ChunkReader(currentChunk, null);
                  while (chunkReader.hasNextSatisfiedPage()) {
                    IPointReader batchIterator = chunkReader.nextPageData().getBatchDataIterator();
                    while (batchIterator.hasNextTimeValuePair()) {
                      TimeValuePair timeValuePair = batchIterator.nextTimeValuePair();
                      writeTimeAndValueToChunkWriter(chunkWriter, timeValuePair);
                      if (timeValuePair.getTimestamp() > maxEndTimestamp) {
                        maxEndTimestamp = timeValuePair.getTimestamp();
                      }
                      if (timeValuePair.getTimestamp() < minStartTimestamp) {
                        minStartTimestamp = timeValuePair.getTimestamp();
                      }
                    }
                  }
                  // flush
                  chunkWriter.writeToFileWriter(writer);
                } catch (IOException e) {
                  // TODO
                  logger.warn("[alter timeseries] chunk write faild", e);
                }
              });
        });
  }

  private void writeTimeAndValueToChunkWriter(
      ChunkWriterImpl chunkWriter, TimeValuePair timeValuePair) {
    switch (chunkWriter.getDataType()) {
      case TEXT:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getBinary());
        break;
      case FLOAT:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getFloat());
        break;
      case DOUBLE:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getDouble());
        break;
      case BOOLEAN:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getBoolean());
        break;
      case INT64:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getLong());
        break;
      case INT32:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getInt());
        break;
      default:
        throw new UnsupportedOperationException("Unknown data type " + chunkWriter.getDataType());
    }
  }
}
