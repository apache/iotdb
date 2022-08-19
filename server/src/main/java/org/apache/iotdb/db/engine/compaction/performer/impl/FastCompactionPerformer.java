package org.apache.iotdb.db.engine.compaction.performer.impl;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.CompactionUtils;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.task.FastCompactionPerformerSubTask;
import org.apache.iotdb.db.engine.compaction.inner.utils.MultiTsFileDeviceIterator;
import org.apache.iotdb.db.engine.compaction.performer.ICrossCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.task.CompactionTaskSummary;
import org.apache.iotdb.db.engine.compaction.writer.FastCrossCompactionWriter;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

  // measurementID -> schema, schemas of measurements under one device
  public Map<String, MeasurementSchema> schemaMapCache = new ConcurrentHashMap<>();

  private CompactionTaskSummary summary;

  private List<TsFileResource> targetFiles;

  public Map<TsFileResource, List<Modification>> modificationCache = new ConcurrentHashMap<>();

  // measurementID -> unseq reader, unseq reader of sensors in one device
  public Map<String, IPointReader> unseqReaders = new ConcurrentHashMap<>();

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
    sortedSourceFiles = CompactionUtils.sortSourceFiles(seqFiles, unseqFiles);
    try (FastCrossCompactionWriter compactionWriter = new FastCrossCompactionWriter(targetFiles)) {
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

        // get all unseq measurements of this device
        Set<String> unseqMeasurements = getAllUnseqMeasurementsByDevice(device);

        // iterate each seq file
        for (int i = 0; i < seqFiles.size(); i++) {
          boolean isLastFile = i == seqFiles.size() - 1;
          if (!seqFiles.get(i).mayContainsDevice(device) && !isLastFile) {
            continue;
          } else {
            compactionWriter.startChunkGroup(i, device, isAligned);

            TsFileResource seqFile = seqFiles.get(i);

            // get chunk metadata list of all sensors under this device in this seq file
            // Todo: to decrease memory, use iterator to get measurement metadata in batch instead
            // of getting all at one time.
            Map<String, List<ChunkMetadata>> measurmentMetadataList =
                readerCacheMap
                    .computeIfAbsent(
                        seqFile,
                        resource -> {
                          try {
                            return new TsFileSequenceReader(resource.getTsFilePath());
                          } catch (IOException e) {
                            throw new RuntimeException(
                                String.format(
                                    "Failed to construct sequence reader for %s", resource));
                          }
                        })
                    .readChunkMetadataInDevice(device);

            // modify seq chunk metadatas
            for (Map.Entry<String, List<ChunkMetadata>> entry : measurmentMetadataList.entrySet()) {
              QueryUtils.modifyChunkMetaData(
                  entry.getValue(),
                  getModifications(seqFile, new PartialPath(device, entry.getKey())));
            }

            // put all unseq measurements into map
            unseqMeasurements.forEach(
                measurement -> {
                  measurmentMetadataList.putIfAbsent(measurement, null);
                });

            // get schema of measurements under this device
            Set<String> measurentsUnknownSchema = new HashSet<>();
            for (String measurementID : measurmentMetadataList.keySet()) {
              if (!schemaMapCache.containsKey(measurementID)) {
                measurentsUnknownSchema.add(measurementID);
              }
            }

            schemaMapCache.putAll(
                // Todo：speed up, avoid sorting all source files on every seq file traversal
                CompactionUtils.getMeasurementSchema(
                    device, measurentsUnknownSchema, sortedSourceFiles, readerCacheMap));

            if (isAligned) {
              compactAlignedSeries();
            } else {
              compactNonAlignedSeries(
                  i,
                  device,
                  new ArrayList<>(measurmentMetadataList.keySet()),
                  new ArrayList<>(measurmentMetadataList.values()),
                  compactionWriter);
            }
            compactionWriter.endChunkGroup();
          }
        }
        schemaMapCache.clear();
        unseqReaders.clear();
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
      // clean cache
      schemaMapCache = null;
      readerCacheMap = null;
      modificationCache = null;
      unseqReaders = null;
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
      int seqFileIndex,
      String deviceID,
      List<String> allMeasurements,
      List<List<ChunkMetadata>> allChunkMetadataList,
      FastCrossCompactionWriter fastCrossCompactionWriter)
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
                  new FastCompactionPerformerSubTask(
                      seqFileIndex,
                      new ArrayList<>(allMeasurements),
                      measurementsForEachSubTask[i],
                      allChunkMetadataList,
                      fastCrossCompactionWriter,
                      this,
                      deviceID,
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
