package org.apache.iotdb.db.engine.compaction.performer.impl;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.CompactionUtils;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.task.NewFastCompactionPerformerSubTask;
import org.apache.iotdb.db.engine.compaction.inner.utils.MultiTsFileDeviceIterator;
import org.apache.iotdb.db.engine.compaction.performer.ICrossCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.task.CompactionTaskSummary;
import org.apache.iotdb.db.engine.compaction.writer.NewFastCrossCompactionWriter;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class NewFastCompactionPerformer implements ICrossCompactionPerformer {
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

  private final Map<TsFileSequenceReader, Iterator<Map<String, List<ChunkMetadata>>>>
      measurementChunkMetadataListMapIteratorCache =
          new TreeMap<>(
              (o1, o2) ->
                  TsFileManager.compareFileName(
                      new File(o1.getFileName()), new File(o2.getFileName())));

  public NewFastCompactionPerformer(
      List<TsFileResource> seqFiles,
      List<TsFileResource> unseqFiles,
      List<TsFileResource> targetFiles) {
    this.seqFiles = seqFiles;
    this.unseqFiles = unseqFiles;
    this.targetFiles = targetFiles;
  }

  public NewFastCompactionPerformer() {}

  @Override
  public void perform()
      throws IOException, MetadataException, StorageEngineException, InterruptedException {
    sortedSourceFiles = CompactionUtils.sortSourceFiles(seqFiles, unseqFiles);
    try (NewFastCrossCompactionWriter compactionWriter =
        new NewFastCrossCompactionWriter(targetFiles)) {
      MultiTsFileDeviceIterator deviceIterator =
          new MultiTsFileDeviceIterator(seqFiles, unseqFiles);

      // iterate each device
      // Todo: to decrease memory, get device in batch on one node instead of getting all at one
      // time.
      // Todo: use tsfile resource to get device in memory instead of getting them with I/O
      // readings.
      while (deviceIterator.hasNextDevice()) {
        Pair<String, Boolean> deviceInfo = deviceIterator.nextDevice();
        String device = deviceInfo.left;
        boolean isAligned = deviceInfo.right;

        compactionWriter.startChunkGroup(device, isAligned);

        Map<String, List<ChunkMetadata>> measurementChunkMetadatas =
            getAllModifiedChunkMetadatasByDevice(device);

        Map<String, MeasurementSchema> measurementSchemaMap =
            CompactionUtils.getMeasurementSchema(
                device, measurementChunkMetadatas.keySet(), sortedSourceFiles, readerCacheMap);

        if (isAligned) {

        } else {
          compactNonAlignedSeries(
              new ArrayList<>(measurementChunkMetadatas.keySet()),
              new ArrayList<>(measurementChunkMetadatas.values()),
              new ArrayList<>(measurementSchemaMap.values()),
              compactionWriter);
        }

        compactionWriter.endChunkGroup();
      }
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

  private void compactAlignedSeries() {}

  private void compactNonAlignedSeries(
      List<String> allMeasurements,
      List<List<ChunkMetadata>> allChunkMetadataList,
      List<MeasurementSchema> measurementSchemas,
      NewFastCrossCompactionWriter newFastCrossCompactionWriter)
      throws IOException, InterruptedException {
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
                  new NewFastCompactionPerformerSubTask(
                      allMeasurements,
                      measurementsForEachSubTask[i],
                      allChunkMetadataList,
                      measurementSchemas,
                      newFastCrossCompactionWriter,
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

  private Set<String> getAllUnseqMeasurementsByDevice(String device) throws IOException {
    Set<String> allUnseqMeasurements = new HashSet<>();
    for (TsFileResource unseqResource : unseqFiles) {
      allUnseqMeasurements.addAll(
          getReaderFromCache(unseqResource).readDeviceMetadata(device).keySet());
    }
    return allUnseqMeasurements;
  }

  private Map<String, List<ChunkMetadata>> getAllModifiedChunkMetadatasByDevice(String device)
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
          QueryUtils.modifyChunkMetaData(
              chunkMetadataList, getModifications(resource, new PartialPath(device, sensor)));

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
