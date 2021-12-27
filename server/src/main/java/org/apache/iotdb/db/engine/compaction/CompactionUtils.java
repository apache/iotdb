package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.db.engine.compaction.writer.ICompactionWriter;
import org.apache.iotdb.db.engine.compaction.writer.InnerSpaceCompactionWriter;
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
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

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

    ICompactionWriter compactionWriter = null;
    try {
      compactionWriter = getCompactionWriter(seqFileResources, unseqFileResources);
      Set<String> tsFileDevicesSet = getTsFileDevicesSet(seqFileResources, unseqFileResources);

      for (String device : tsFileDevicesSet) {
        boolean isAligned =
            ((EntityMNode) MManager.getInstance().getDeviceNode(new PartialPath(device)))
                .isAligned();
        compactionWriter.startChunkGroup(device, isAligned);
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
          compactionWriter.startMeasurement(measurementSchemas);

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
          while (dataBatchReader.hasNextBatch()) {
            BatchData batchData = dataBatchReader.nextBatch();
            while (batchData.hasCurrent()) {
              compactionWriter.write(batchData.currentTime(), batchData.currentValue());
              batchData.next();
            }
          }
          compactionWriter.endMeasurement();
        } else {
          //  nonAligned
          for (Map.Entry<String, TimeseriesMetadata> entry : deviceMeasurementsMap.entrySet()) {
            List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
            measurementSchemas.add(
                IoTDB.metaManager.getSeriesSchema(new PartialPath(device, entry.getKey())));
            compactionWriter.startMeasurement(measurementSchemas);

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
            while (dataBatchReader.hasNextBatch()) {
              BatchData batchData = dataBatchReader.nextBatch();
              while (batchData.hasCurrent()) {
                compactionWriter.write(batchData.currentTime(), batchData.currentValue());
                batchData.next();
              }
            }
            compactionWriter.endMeasurement();
          }
        }
        compactionWriter.endChunkGroup();
      }
      compactionWriter.endFile();
    } finally {
      QueryResourceManager.getInstance().endQuery(queryId);
      compactionWriter.close();
    }
  }

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

  private static ICompactionWriter getCompactionWriter(
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
      return new InnerSpaceCompactionWriter(targetTsFileResource.getTsFile());
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
}
