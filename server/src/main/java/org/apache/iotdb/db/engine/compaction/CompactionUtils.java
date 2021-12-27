package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.db.engine.compaction.cross.inplace.manage.MergeManager;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.mnode.EntityMNode;
import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CompactionUtils {

  public static void executeCompaction(
      List<TsFileResource> seqFileResources,
      List<TsFileResource> unseqFileResources,
      String storageGroup)
      throws IOException, MetadataException, StorageEngineException {
    long queryId =
        QueryResourceManager.getInstance()
            .assignCompactionQueryId(1); // Todo:Thread.currentThread().getName()
    QueryContext queryContext = new QueryContext(queryId);
    QueryDataSource queryDataSource = new QueryDataSource(seqFileResources, unseqFileResources);
    QueryResourceManager.getInstance()
        .getQueryFileManager()
        .addUsedFilesForQuery(queryId, queryDataSource);

    TsFileIOWriter targetFileWriter = null;
    try {
      targetFileWriter = getFileIOWriter(seqFileResources, unseqFileResources);
      Set<String> tsFileDevicesSet = getTsFileDevicesSet(seqFileResources, unseqFileResources);

      for (String device : tsFileDevicesSet) {
        targetFileWriter.startChunkGroup(device);
        boolean isAligned =
            ((EntityMNode) MManager.getInstance().getDeviceNode(new PartialPath(device)))
                .isAligned();
        Map<String, TimeseriesMetadata> deviceMeasurementsMap =
            getDeviceMeasurementsMap(device, seqFileResources, unseqFileResources);
        PartialPath seriesPath;
        if (isAligned) {
          //  aligned
          List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
          deviceMeasurementsMap.forEach(
              (measurementId, timeseriesMetadata) -> {
                measurementSchemas.add(
                    new UnaryMeasurementSchema(measurementId, timeseriesMetadata.getTSDataType()));
              });
          seriesPath =
              new AlignedPath(
                  device, new ArrayList<>(deviceMeasurementsMap.keySet()), measurementSchemas);
          SeriesRawDataBatchReader dataBatchReader =
                  new SeriesRawDataBatchReader(
                          seriesPath,
                          deviceMeasurementsMap.keySet(),
                          TSDataType.VECTOR,
                          queryContext,
                          queryDataSource,
                          null,
                          null,
                          null,
                          true);
          IChunkWriter chunkWriter=new AlignedChunkWriterImpl(measurementSchemas);
          while (dataBatchReader.hasNextBatch()) {
            BatchData batchData = dataBatchReader.nextBatch();
            while (batchData.hasCurrent()) {
              writeDataPoint(batchData.currentTime(), batchData.currentValue(), chunkWriter);
              batchData.next();
              if (chunkWriter.getSerializedChunkSize() > 2 * 1024 * 1024) { // Todo:
                writeRateLimit(chunkWriter.estimateMaxSeriesMemSize());
                chunkWriter.writeToFileWriter(targetFileWriter);
                targetFileWriter.endChunkGroup();
                if (batchData.hasCurrent() || dataBatchReader.hasNextBatch()) {
                  targetFileWriter.startChunkGroup(device);
                }
              }
            }
          }
          writeRateLimit(chunkWriter.estimateMaxSeriesMemSize());
          chunkWriter.writeToFileWriter(targetFileWriter);

        } else {
          //  nonAligned
          for (Map.Entry<String, TimeseriesMetadata> entry : deviceMeasurementsMap.entrySet()) {
            seriesPath =
                new MeasurementPath(
                    device,
                    entry.getKey(),
                    new UnaryMeasurementSchema(entry.getKey(), entry.getValue().getTSDataType()));
            SeriesRawDataBatchReader dataBatchReader =
                new SeriesRawDataBatchReader(
                    seriesPath,
                    deviceMeasurementsMap.keySet(),
                    entry.getValue().getTSDataType(),
                    queryContext,
                    queryDataSource,
                    null,
                    null,
                    null,
                    true);
            ChunkWriterImpl chunkWriter =
                new ChunkWriterImpl(
                    IoTDB.metaManager.getSeriesSchema(new PartialPath(device, entry.getKey())),
                    true);
            while (dataBatchReader.hasNextBatch()) {
              BatchData batchData = dataBatchReader.nextBatch();
              while (batchData.hasCurrent()) {
                writeDataPoint(batchData.currentTime(), batchData.currentValue(), chunkWriter);
                batchData.next();
                if (chunkWriter.getSerializedChunkSize() > 2 * 1024 * 1024) { // Todo:
                  writeRateLimit(chunkWriter.estimateMaxSeriesMemSize());
                  chunkWriter.writeToFileWriter(targetFileWriter);
                  targetFileWriter.endChunkGroup();
                  if (batchData.hasCurrent() || dataBatchReader.hasNextBatch()) {
                    targetFileWriter.startChunkGroup(device);
                  }
                }
              }
            }
            writeRateLimit(chunkWriter.estimateMaxSeriesMemSize());
            chunkWriter.writeToFileWriter(targetFileWriter);
          }
        }
        targetFileWriter.endChunkGroup();
      }
      targetFileWriter.endFile();
    } finally {
      QueryResourceManager.getInstance().endQuery(queryId);
      if (targetFileWriter != null && targetFileWriter.canWrite()) {
        targetFileWriter.close();
      }
    }
  }

  private static void writeWithReader(
      PartialPath path,
      Set<String> allSensors,
      TSDataType dataType,
      QueryContext queryContext,
      QueryDataSource queryDataSource) {}

  private static Set<String> getTsFileDevicesSet(
      List<TsFileResource> seqResources, List<TsFileResource> unseqResources) throws IOException {
    Set<String> deviceSet = new HashSet<>();
    for (TsFileResource seqResource : seqResources) {
      deviceSet.addAll(
          FileReaderManager.getInstance().get(seqResource.getTsFilePath(), true).getAllDevices());
    }
    for (TsFileResource unseqResource : unseqResources) {
      deviceSet.addAll(
          FileReaderManager.getInstance().get(unseqResource.getTsFilePath(), true).getAllDevices());
    }
    return deviceSet;
  }

  private static void writeRateLimit(long bytesLength) {
    MergeManager.mergeRateLimiterAcquire(
        MergeManager.getINSTANCE().getMergeWriteRateLimiter(), bytesLength);
  }

  private static TsFileIOWriter getFileIOWriter(
      List<TsFileResource> seqFileResources, List<TsFileResource> unseqFileResources)
      throws IOException {
    String dataDirectory = seqFileResources.get(0).getTsFile().getParent();
    if (!seqFileResources.isEmpty() && !unseqFileResources.isEmpty()) {
      // cross space
      // Todo: get resource of each target file
      return null;
    } else {
      String targetFileName;
      if (!seqFileResources.isEmpty()) {
        // seq inner space
        targetFileName =
            TsFileNameGenerator.getInnerCompactionFileName(seqFileResources, true).getName();
      } else {
        // unseq inner space
        targetFileName =
            TsFileNameGenerator.getInnerCompactionFileName(unseqFileResources, false).getName();
      }
      TsFileResource targetTsFileResource =
          new TsFileResource(new File(dataDirectory + File.separator + targetFileName));
      return new RestorableTsFileIOWriter(targetTsFileResource.getTsFile());
    }
  }

  private static Map<String, TimeseriesMetadata> getDeviceMeasurementsMap(
      String device, List<TsFileResource> seqFileResources, List<TsFileResource> unseqFileResources)
      throws IOException {
    Map<String, TimeseriesMetadata> deviceMeasurementsMap = new HashMap<>();
    for (TsFileResource seqResource : seqFileResources) {
      deviceMeasurementsMap.putAll(
          FileReaderManager.getInstance()
              .get(seqResource.getTsFilePath(), true)
              .readDeviceMetadata(device));
    }
    for (TsFileResource unseqResource : unseqFileResources) {
      deviceMeasurementsMap.putAll(
          FileReaderManager.getInstance()
              .get(unseqResource.getTsFilePath(), true)
              .readDeviceMetadata(device));
    }
    return deviceMeasurementsMap;
  }

  public static void writeDataPoint(Long time, Object value, ChunkWriterImpl chunkWriter) {
    switch (chunkWriter.getDataType()) {
      case TEXT:
        chunkWriter.write(time, (Binary) value);
        break;
      case DOUBLE:
        chunkWriter.write(time, (Double) value);
        break;
      case BOOLEAN:
        chunkWriter.write(time, (Boolean) value);
        break;
      case INT64:
        chunkWriter.write(time, (Long) value);
        break;
      case INT32:
        chunkWriter.write(time, (Integer) value);
        break;
      case FLOAT:
        chunkWriter.write(time, (Float) value);
        break;
      default:
        throw new UnsupportedOperationException("Unknown data type " + chunkWriter.getDataType());
    }
  }

  public static void writeAlignedDataPoint(Long time, Object value, AlignedChunkWriterImpl chunkWriter) {
    switch (chunkWriter.getDataType()) {
      case TEXT:
        chunkWriter.write(time, (Binary) value);
        break;
      case DOUBLE:
        chunkWriter.write(time, (Double) value);
        break;
      case BOOLEAN:
        chunkWriter.write(time, (Boolean) value);
        break;
      case INT64:
        chunkWriter.write(time, (Long) value);
        break;
      case INT32:
        chunkWriter.write(time, (Integer) value);
        break;
      case FLOAT:
        chunkWriter.write(time, (Float) value);
        break;
      default:
        throw new UnsupportedOperationException("Unknown data type " + chunkWriter.getDataType());
    }
  }
}
