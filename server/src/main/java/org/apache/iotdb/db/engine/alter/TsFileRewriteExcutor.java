package org.apache.iotdb.db.engine.alter;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.TsFileAlignedSeriesReaderIterator;
import org.apache.iotdb.tsfile.read.TsFileDeviceIterator;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.read.reader.chunk.AlignedChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** This class is used to rewrite one tsfile with current encoding & compressionType. */
public class TsFileRewriteExcutor {

  private static final Logger logger = LoggerFactory.getLogger(TsFileRewriteExcutor.class);

  private final TsFileResource tsFileResource;
  private final TsFileResource targetTsFileResource;
  private final PartialPath fullPath;
  private final TSEncoding curEncoding;
  private final CompressionType curCompressionType;
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
    try (TsFileSequenceReader reader = new TsFileSequenceReader(tsFileResource.getTsFilePath());
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
        boolean isTargetDevice = fullPath.getDevice().equals(device);
        String targetMeasurement = fullPath.getMeasurement();
        // write chunk & page data
        if (aligned) {
          rewriteAlgined(reader, writer, device, isTargetDevice, targetMeasurement);
        } else {
          rewriteNotAligned(device, reader, writer, targetMeasurement, isTargetDevice);
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

  private void rewriteAlgined(
      TsFileSequenceReader reader,
      TsFileIOWriter writer,
      String device,
      boolean isTargetDevice,
      String targetMeasurement)
      throws IOException {
    List<AlignedChunkMetadata> alignedChunkMetadatas = reader.getAlignedChunkMetadata(device);
    if (alignedChunkMetadatas == null || alignedChunkMetadatas.isEmpty()) {
      logger.warn("[alter timeseries] device({}) alignedChunkMetadatas is null", device);
      return;
    }
    // TODO To be optimized: Non-target modification measurements are directly written to data
    List<IMeasurementSchema> schemaList =
        collectSchemaList(alignedChunkMetadatas, reader, targetMeasurement, isTargetDevice);
    List<IMeasurementSchema> schemaOldList =
        collectSchemaList(alignedChunkMetadatas, reader, targetMeasurement, false);
    AlignedChunkWriterImpl chunkWriter = new AlignedChunkWriterImpl(schemaList);
    TsFileAlignedSeriesReaderIterator readerIterator =
        new TsFileAlignedSeriesReaderIterator(reader, alignedChunkMetadatas, schemaOldList);

    while (readerIterator.hasNext()) {
      Pair<AlignedChunkReader, Long> chunkReaderAndChunkSize = readerIterator.nextReader();
      AlignedChunkReader chunkReader = chunkReaderAndChunkSize.left;
      while (chunkReader.hasNextSatisfiedPage()) {
        IBatchDataIterator batchDataIterator = chunkReader.nextPageData().getBatchDataIterator();
        while (batchDataIterator.hasNext()) {
          TsPrimitiveType[] pointsData = (TsPrimitiveType[]) batchDataIterator.currentValue();
          long time = batchDataIterator.currentTime();
          chunkWriter.write(time, pointsData);
          targetTsFileResource.updateStartTime(device, time);
          targetTsFileResource.updateEndTime(device, time);
          batchDataIterator.next();
        }
      }
    }
    chunkWriter.writeToFileWriter(writer);
  }

  protected List<IMeasurementSchema> collectSchemaList(
      List<AlignedChunkMetadata> alignedChunkMetadatas,
      TsFileSequenceReader reader,
      String targetMeasurement,
      boolean isTargetDevice)
      throws IOException {

    Set<MeasurementSchema> schemaSet = new HashSet<>();
    Set<String> measurementSet = new HashSet<>();
    for (AlignedChunkMetadata alignedChunkMetadata : alignedChunkMetadatas) {
      List<IChunkMetadata> valueChunkMetadataList =
          alignedChunkMetadata.getValueChunkMetadataList();
      for (IChunkMetadata chunkMetadata : valueChunkMetadataList) {
        if (chunkMetadata == null) {
          continue;
        }
        if (measurementSet.contains(chunkMetadata.getMeasurementUid())) {
          continue;
        }
        measurementSet.add(chunkMetadata.getMeasurementUid());
        Chunk chunk = reader.readMemChunk((ChunkMetadata) chunkMetadata);
        ChunkHeader header = chunk.getHeader();
        boolean findTarget =
            isTargetDevice
                && CommonUtils.equal(chunkMetadata.getMeasurementUid(), targetMeasurement);
        MeasurementSchema measurementSchema =
            new MeasurementSchema(
                header.getMeasurementID(),
                header.getDataType(),
                findTarget ? this.curEncoding : header.getEncodingType(),
                findTarget ? this.curCompressionType : header.getCompressionType());

        schemaSet.add(measurementSchema);
      }
    }
    List<IMeasurementSchema> schemaList = new ArrayList<>(schemaSet);
    schemaList.sort(Comparator.comparing(IMeasurementSchema::getMeasurementId));
    return schemaList;
  }

  private void rewriteNotAligned(
      String device,
      TsFileSequenceReader reader,
      TsFileIOWriter writer,
      String targetMeasurement,
      boolean isTargetDevice)
      throws IOException {
    Map<String, List<ChunkMetadata>> measurementMap = reader.readChunkMetadataInDevice(device);
    if (measurementMap == null) {
      logger.warn("[alter timeseries] device({}) measurementMap is null", device);
      return;
    }
    for (Map.Entry<String, List<ChunkMetadata>> next : measurementMap.entrySet()) {
      String measurementId = next.getKey();
      List<ChunkMetadata> chunkMetadatas = next.getValue();
      if (chunkMetadatas == null || chunkMetadatas.isEmpty()) {
        logger.warn("[alter timeseries] empty measurement({})", measurementId);
        return;
      }
      boolean findTarget = isTargetDevice && CommonUtils.equal(measurementId, targetMeasurement);
      // target chunk writer
      ChunkWriterImpl chunkWriter =
          new ChunkWriterImpl(
              new MeasurementSchema(
                  measurementId,
                  chunkMetadatas.get(0).getDataType(),
                  curEncoding,
                  curCompressionType));
      for (ChunkMetadata chunkMetadata : chunkMetadatas) {
        // old mem chunk
        Chunk currentChunk = reader.readMemChunk(chunkMetadata);
        if (!findTarget) {
          // skip
          writer.writeChunk(currentChunk, chunkMetadata);
          continue;
        }
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
      }
    }
  }

  /** TODO Copy it from the compaction code and extract it later into the utility class */
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
