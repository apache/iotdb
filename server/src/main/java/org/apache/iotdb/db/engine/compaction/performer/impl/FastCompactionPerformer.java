package org.apache.iotdb.db.engine.compaction.performer.impl;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.CompactionUtils;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.task.AlignedFastCompactionPerformerSubTask;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.task.NonAlignedFastCompactionPerformerSubTask;
import org.apache.iotdb.db.engine.compaction.inner.utils.MultiTsFileDeviceIterator;
import org.apache.iotdb.db.engine.compaction.performer.ICrossCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.task.CompactionTaskSummary;
import org.apache.iotdb.db.engine.compaction.writer.NewFastCrossCompactionWriter;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.MetadataIndexNode;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class FastCompactionPerformer implements ICrossCompactionPerformer {
  private final Logger LOGGER = LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
  private List<TsFileResource> seqFiles;

  private List<TsFileResource> unseqFiles;

  private List<TsFileResource> sortedSourceFiles;

  private static final int subTaskNum =
      IoTDBDescriptor.getInstance().getConfig().getSubCompactionTaskNum();

  public Map<TsFileResource, TsFileSequenceReader> readerCacheMap = new ConcurrentHashMap<>();

  private CompactionTaskSummary summary;

  private List<TsFileResource> targetFiles;

  public Map<TsFileResource, List<Modification>> modificationCache = new ConcurrentHashMap<>();

  private MultiTsFileDeviceIterator deviceIterator;

  private final Map<TsFileSequenceReader, Iterator<Map<String, List<ChunkMetadata>>>>
      measurementChunkMetadataListMapIteratorCache =
          new TreeMap<>(
              (o1, o2) ->
                  TsFileManager.compareFileName(
                      new File(o1.getFileName()), new File(o2.getFileName())));

  public FastCompactionPerformer(
      List<TsFileResource> seqFiles,
      List<TsFileResource> unseqFiles,
      List<TsFileResource> targetFiles) {
    this.seqFiles = seqFiles;
    this.unseqFiles = unseqFiles;
    this.targetFiles = targetFiles;
  }

  public FastCompactionPerformer() {}

  @Override
  public void perform()
      throws IOException, MetadataException, StorageEngineException, InterruptedException {
    // Todo: use one tsfileSequenceReader，instead of opening a new reader in device iterator.
    try (NewFastCrossCompactionWriter compactionWriter =
        new NewFastCrossCompactionWriter(targetFiles, seqFiles); ) {
      deviceIterator = new MultiTsFileDeviceIterator(seqFiles, unseqFiles);
      // iterate each device
      // Todo: to decrease memory, get device in batch on one node instead of getting all at one
      // time.
      // Todo: use tsfile resource to get device in memory instead of getting them with I/O
      // readings.
      while (deviceIterator.hasNextDevice()) {
        Pair<String, Boolean> deviceInfo = deviceIterator.nextDevice();
        String device = deviceInfo.left;
        boolean isAligned = deviceInfo.right;

        sortedSourceFiles = new ArrayList<>(seqFiles);
        sortedSourceFiles.addAll(unseqFiles);
        sortedSourceFiles.removeIf(x -> !x.mayContainsDevice(device));
        sortedSourceFiles.sort(Comparator.comparingLong(x -> x.getStartTime(device)));
        compactionWriter.startChunkGroup(device, isAligned);

        if (isAligned) {
          compactAlignedSeries(device, deviceIterator, compactionWriter);
        } else {
          compactNonAlignedSeries(device, deviceIterator, compactionWriter);
        }

        compactionWriter.endChunkGroup();
      }
      compactionWriter.endFile();
      CompactionUtils.updateDeviceStartTimeAndEndTime(
          targetFiles, compactionWriter.getFileIOWriter());
      CompactionUtils.updatePlanIndexes(targetFiles, seqFiles, unseqFiles);
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      // close all readers of source files
      for (TsFileSequenceReader reader : readerCacheMap.values()) {
        reader.close();
      }
      deviceIterator.close();
      // clean cache
      readerCacheMap = null;
      modificationCache = null;
    }
  }

  private Iterator<Map<String, List<ChunkMetadata>>> getAllChunkMetadataByDevice(
      TsFileResource resource, String deviceID) {
    return measurementChunkMetadataListMapIteratorCache.computeIfAbsent(
        readerCacheMap.computeIfAbsent(
            resource,
            tsFileResource -> {
              try {
                return new TsFileSequenceReader(tsFileResource.getTsFilePath());
              } catch (IOException e) {
                throw new RuntimeException(
                    String.format("Failed to construct sequence reader for %s", resource));
              }
            }),
        (tsFileSequenceReader -> {
          try {
            return tsFileSequenceReader.getMeasurementChunkMetadataListMapIterator(deviceID);
          } catch (IOException e) {
            throw new RuntimeException(
                "GetMeasurementChunkMetadataListMapIterator meets error. iterator create failed.");
          }
        }));
  }

  private void compactAlignedSeries(
      String deviceId,
      MultiTsFileDeviceIterator deviceIterator,
      NewFastCrossCompactionWriter newFastCrossCompactionWriter)
      throws PageException, IOException, WriteProcessException, IllegalPathException {
    Map<String, Map<TsFileResource, Pair<Long, Long>>> timeseriesMetadataOffsetMap =
        new HashMap<>();
    List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
    for (Map.Entry<String, Pair<MeasurementSchema, Map<TsFileResource, Pair<Long, Long>>>> entry :
        deviceIterator.getTimeseriesSchemaAndMetadataOffsetOfCurrentDevice().entrySet()) {
      if (!entry.getKey().equals("")) {
        measurementSchemas.add(entry.getValue().left);
      }
      timeseriesMetadataOffsetMap.put(entry.getKey(), entry.getValue().right);
    }

    new AlignedFastCompactionPerformerSubTask(
            newFastCrossCompactionWriter,
            this,
            timeseriesMetadataOffsetMap,
            measurementSchemas,
            deviceId,
            0)
        .call();
  }

  private void compactNonAlignedSeries(
      String deviceID,
      MultiTsFileDeviceIterator deviceIterator,
      NewFastCrossCompactionWriter newFastCrossCompactionWriter)
      throws IOException, InterruptedException, IllegalPathException {
    //    Map<String, Pair<MeasurementSchema, List<IChunkMetadata>>> measurementMap =
    //        deviceIterator.getAllNonAlignedSchemasAndMetadatasOfCurrentDevice();

    Map<String, Map<TsFileResource, Pair<Long, Long>>> timeseriesMetadataOffsetMap =
        deviceIterator.getTimeseriesMetadataOffsetOfCurrentDevice();

    List<String> allMeasurements = new ArrayList<>(timeseriesMetadataOffsetMap.keySet());
    //    List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
    //    List<List<IChunkMetadata>> measurementChunkMetadatas = new ArrayList<>();
    //    measurementMap
    //        .values()
    //        .forEach(
    //            x -> {
    //              measurementSchemas.add(x.left);
    //              measurementChunkMetadatas.add(x.right);
    //            });

    int subTaskNums = Math.min(allMeasurements.size(), subTaskNum);

    // assign all measurements to different sub tasks
    List<Integer>[] measurementsForEachSubTask = new ArrayList[subTaskNums];
    for (int idx = 0; idx < allMeasurements.size(); idx++) {
      if (measurementsForEachSubTask[idx % subTaskNums] == null) {
        measurementsForEachSubTask[idx % subTaskNums] = new ArrayList<>();
      }
      measurementsForEachSubTask[idx % subTaskNums].add(idx);
    }

    // construct sub tasks and start compacting measurements in parallel
    List<Future<Void>> futures = new ArrayList<>();
    for (int i = 0; i < subTaskNums; i++) {
      futures.add(
          CompactionTaskManager.getInstance()
              .submitSubTask(
                  new NonAlignedFastCompactionPerformerSubTask(
                      newFastCrossCompactionWriter,
                      deviceID,
                      measurementsForEachSubTask[i],
                      allMeasurements,
                      this,
                      timeseriesMetadataOffsetMap,
                      i)));
    }

    // wait for all sub tasks finish
    for (int i = 0; i < subTaskNums; i++) {
      try {
        futures.get(i).get();
      } catch (ExecutionException e) {
        LOGGER.error("[Compaction] SubCompactionTask meet errors ", e);
        throw new IOException(e);
      }
    }
  }

  private List<AlignedChunkMetadata> getModifiedAlignedChunkMetadatasByDevice(String device)
      throws IOException, IllegalPathException {
    List<AlignedChunkMetadata> alignedChunkMetadatas = new ArrayList<>();
    List<TsFileResource> allResources = new ArrayList<>(seqFiles);
    allResources.addAll(unseqFiles);
    for (TsFileResource resource : allResources) {
      List<AlignedChunkMetadata> alignedChunkMetadataList =
          getReaderFromCache(resource).getAlignedChunkMetadata(device);
      List<List<Modification>> valueModifications = new ArrayList<>();
      for (IChunkMetadata valueChunkMetadata :
          alignedChunkMetadataList.get(0).getValueChunkMetadataList()) {
        valueModifications.add(
            getModifications(
                resource, new PartialPath(device, valueChunkMetadata.getMeasurementUid())));
      }
      QueryUtils.modifyAlignedChunkMetaData(alignedChunkMetadataList, valueModifications);
      alignedChunkMetadatas.addAll(alignedChunkMetadataList);
    }
    return alignedChunkMetadatas;
  }

  private Map<String, List<ChunkMetadata>> getModifiedChunkMetadatasByDevice(String device)
      throws IOException, IllegalPathException {
    Map<String, List<ChunkMetadata>> chunkMetadataMap = new HashMap<>();
    List<TsFileResource> allResources = new ArrayList<>(seqFiles);
    allResources.addAll(unseqFiles);
    for (TsFileResource resource : allResources) {
      for (Map.Entry<String, List<ChunkMetadata>> entry :
          getReaderFromCache(resource).readChunkMetadataInDevice(device).entrySet()) {
        String sensor = entry.getKey();
        List<ChunkMetadata> chunkMetadataList = entry.getValue();
        if (!chunkMetadataList.isEmpty()) {
          // set file path
          chunkMetadataList.forEach(x -> x.setFilePath(resource.getTsFilePath()));

          // modify chunk metadatas
          if (!sensor.equals("")) {
            QueryUtils.modifyChunkMetaData(
                chunkMetadataList, getModifications(resource, new PartialPath(device, sensor)));
          }

          if (!chunkMetadataMap.containsKey(sensor)) {
            chunkMetadataMap.put(sensor, chunkMetadataList);
          } else {
            chunkMetadataMap.get(sensor).addAll(chunkMetadataList);
          }
        }
      }
    }
    return chunkMetadataMap;
  }

  @Override
  public void setTargetFiles(List<TsFileResource> targetFiles) {
    this.targetFiles = targetFiles;
  }

  @Override
  public void setSummary(CompactionTaskSummary summary) {
    this.summary = summary;
  }

  @Override
  public void setSourceFiles(List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles) {
    this.seqFiles = seqFiles;
    this.unseqFiles = unseqFiles;
  }

  public List<TsFileResource> getUnseqFiles() {
    return unseqFiles;
  }

  public List<TsFileResource> getSeqFiles() {
    return seqFiles;
  }

  /**
   * Get the modifications of a timeseries in the ModificationFile of a TsFile.
   *
   * @param path name of the time series
   */
  public List<Modification> getModifications(TsFileResource tsFileResource, PartialPath path) {
    // copy from TsFileResource so queries are not affected
    List<Modification> modifications =
        modificationCache.computeIfAbsent(
            tsFileResource, resource -> new ArrayList<>(resource.getModFile().getModifications()));
    List<Modification> pathModifications = new ArrayList<>();
    Iterator<Modification> modificationIterator = modifications.iterator();
    while (modificationIterator.hasNext()) {
      Modification modification = modificationIterator.next();
      if (modification.getPath().matchFullPath(path)) {
        pathModifications.add(modification);
      }
    }
    return pathModifications;
  }

  public List<TsFileResource> getSortedSourceFiles() {
    return sortedSourceFiles;
  }

  public List<Pair<TsFileResource, MetadataIndexNode>>
      getSortedSourceFilesAndFirstMeasurementNodeOfCurrentDevice() {
    List<Pair<TsFileResource, MetadataIndexNode>> pairList = new ArrayList<>();
    for (TsFileResource resource : sortedSourceFiles) {
      pairList.add(
          new Pair<>(
              resource, deviceIterator.getFirstMeasurementNodeOfCurrentDeviceByResource(resource)));
    }
    return pairList;
  }

  public TsFileSequenceReader getReaderFromCache(TsFileResource resource) {
    return readerCacheMap.computeIfAbsent(
        resource,
        tsFileResource -> {
          try {
            return new TsFileSequenceReader(tsFileResource.getTsFilePath());
          } catch (IOException e) {
            throw new RuntimeException(
                String.format("Failed to construct sequence reader for %s", resource));
          }
        });
  }
}
