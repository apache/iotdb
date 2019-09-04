/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.storagegroup;

import static org.apache.iotdb.db.engine.merge.task.MergeTask.MERGE_SUFFIX;
import static org.apache.iotdb.db.engine.storagegroup.TsFileResource.TEMP_SUFFIX;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.merge.manage.MergeManager;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.selector.IMergeFileSelector;
import org.apache.iotdb.db.engine.merge.selector.MaxFileMergeFileSelector;
import org.apache.iotdb.db.engine.merge.selector.MaxSeriesMergeFileSelector;
import org.apache.iotdb.db.engine.merge.selector.MergeFileStrategy;
import org.apache.iotdb.db.engine.merge.task.MergeTask;
import org.apache.iotdb.db.engine.merge.task.RecoverMergeTask;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.version.SimpleFileVersionController;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.exception.MergeException;
import org.apache.iotdb.db.exception.MetadataErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.exception.StorageGroupProcessorException;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.physical.crud.BatchInsertPlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.JobFileManager;
import org.apache.iotdb.db.utils.CopyOnReadLinkedList;
import org.apache.iotdb.db.writelog.recover.TsFileRecoverPerformer;
import org.apache.iotdb.rpc.TSStatusType;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * For sequence data, a StorageGroupProcessor has some TsFileProcessors, in which there is only one
 * TsFileProcessor in the working status. <br/>
 *
 * There are two situations to set the working TsFileProcessor to closing status:<br/>
 *
 * (1) when inserting data into the TsFileProcessor, and the TsFileProcessor shouldFlush() (or
 * shouldClose())<br/>
 *
 * (2) someone calls waitForAllCurrentTsFileProcessorsClosed(). (up to now, only flush command from
 * cli will call this method)<br/>
 *
 * UnSequence data has the similar process as above.
 *
 * When a sequence TsFileProcessor is submitted to be flushed, the updateLatestFlushTimeCallback()
 * method will be called as a callback.<br/>
 *
 * When a TsFileProcessor is closed, the closeUnsealedTsFileProcessor() method will be called as a
 * callback.
 */
public class StorageGroupProcessor {

  private static final String MERGING_MODIFICAITON_FILE_NAME = "merge.mods";
  private static final Logger logger = LoggerFactory.getLogger(StorageGroupProcessor.class);
  /**
   * a read write lock for guaranteeing concurrent safety when accessing all fields in this class
   * (i.e., schema, (un)sequenceFileList, work(un)SequenceTsFileProcessor,
   * closing(Un)SequenceTsFileProcessor, latestTimeForEachDevice, and
   * latestFlushedTimeForEachDevice)
   */
  private final ReadWriteLock insertLock = new ReentrantReadWriteLock();
  /**
   *
   */
  private final Object closeStorageGroupCondition = new Object();
  /**
   * avoid some tsfileResource is changed (e.g., from unsealed to sealed) when a query is executed.
   */
  private final ReadWriteLock closeQueryLock = new ReentrantReadWriteLock();
  /**
   * the schema of time series that belong this storage group
   */
  private Schema schema;
  // includes sealed and unsealed sequence TsFiles
  private List<TsFileResource> sequenceFileList = new ArrayList<>();
  private TsFileProcessor workSequenceTsFileProcessor = null;
  private CopyOnReadLinkedList<TsFileProcessor> closingSequenceTsFileProcessor = new CopyOnReadLinkedList<>();
  // includes sealed and unsealed unSequence TsFiles
  private List<TsFileResource> unSequenceFileList = new ArrayList<>();
  private TsFileProcessor workUnSequenceTsFileProcessor = null;
  private CopyOnReadLinkedList<TsFileProcessor> closingUnSequenceTsFileProcessor = new CopyOnReadLinkedList<>();
  /**
   * device -> global latest timestamp of each device latestTimeForEachDevice caches non-flushed
   * changes upon timestamps of each device, and is used to update latestFlushedTimeForEachDevice
   * when a flush is issued.
   */
  private Map<String, Long> latestTimeForEachDevice = new HashMap<>();
  /**
   * device -> largest timestamp of the latest memtable to be submitted to asyncTryToFlush
   * latestFlushedTimeForEachDevice determines whether a data point should be put into a sequential
   * file or an unsequential file. Data of some device with timestamp less than or equals to the
   * device's latestFlushedTime should go into an unsequential file.
   */
  private Map<String, Long> latestFlushedTimeForEachDevice = new HashMap<>();
  private String storageGroupName;
  private File storageGroupSysDir;

  /**
   * versionController assigns a version for each MemTable and deletion/update such that after they
   * are persisted, the order of insertions, deletions and updates can be re-determined.
   */
  private VersionController versionController;

  /**
   * mergeLock is to be used in the merge process. Concurrent queries, deletions and merges may
   * result in losing some deletion in the merged new file, so a lock is necessary.
   */
  private ReentrantReadWriteLock mergeLock = new ReentrantReadWriteLock();

  /**
   * This is the modification file of the result of the current merge. Because the merged file may
   * be invisible at this moment, without this, deletion/update during merge could be lost.
   */
  private ModificationFile mergingModification;

  private volatile boolean isMerging = false;
  private long mergeStartTime;

  /**
   * This linked list records the access order of measurements used by query.
   */
  private LinkedList<String> lruForSensorUsedInQuery = new LinkedList<>();
  private static final int MAX_CACHE_SENSORS = 5000;


  public StorageGroupProcessor(String systemInfoDir, String storageGroupName)
      throws ProcessorException {
    this.storageGroupName = storageGroupName;

    // construct the file schema
    this.schema = constructSchema(storageGroupName);

    try {
      storageGroupSysDir = new File(systemInfoDir, storageGroupName);
      if (storageGroupSysDir.mkdirs()) {
        logger.info("Storage Group system Directory {} doesn't exist, create it",
            storageGroupSysDir.getPath());
      } else if (!storageGroupSysDir.exists()) {
        logger.error("create Storage Group system Directory {} failed",
            storageGroupSysDir.getPath());
      }

      versionController = new SimpleFileVersionController(storageGroupSysDir.getPath());
    } catch (IOException e) {
      throw new StorageGroupProcessorException(e);
    }

    recover();
  }

  private void recover() throws ProcessorException {
    logger.info("recover Storage Group  {}", storageGroupName);

    try {
      // collect TsFiles from sequential and unsequential data directory
      List<TsFileResource> seqTsFiles = getAllFiles(
          DirectoryManager.getInstance().getAllSequenceFileFolders());
      List<TsFileResource> unseqTsFiles =
          getAllFiles(DirectoryManager.getInstance().getAllUnSequenceFileFolders());

      recoverSeqFiles(seqTsFiles);
      recoverUnseqFiles(unseqTsFiles);

      String taskName = storageGroupName + "-" + System.currentTimeMillis();
      File mergingMods = new File(storageGroupSysDir, MERGING_MODIFICAITON_FILE_NAME);
      if (mergingMods.exists()) {
        mergingModification = new ModificationFile(mergingMods.getPath());
      }
      RecoverMergeTask recoverMergeTask = new RecoverMergeTask(seqTsFiles, unseqTsFiles,
          storageGroupSysDir.getPath(), this::mergeEndAction, taskName,
          IoTDBDescriptor.getInstance().getConfig().isForceFullMerge(), storageGroupName);
      logger.info("{} a RecoverMergeTask {} starts...", storageGroupName, taskName);
      recoverMergeTask
          .recoverMerge(IoTDBDescriptor.getInstance().getConfig().isContinueMergeAfterReboot());
      if (!IoTDBDescriptor.getInstance().getConfig().isContinueMergeAfterReboot()) {
        mergingMods.delete();
      }
    } catch (IOException | MetadataErrorException e) {
      throw new ProcessorException(e);
    }

    for (TsFileResource resource : sequenceFileList) {
      latestTimeForEachDevice.putAll(resource.getEndTimeMap());
      latestFlushedTimeForEachDevice.putAll(resource.getEndTimeMap());
    }
  }

  private List<TsFileResource> getAllFiles(List<String> folders) throws IOException {
    List<File> tsFiles = new ArrayList<>();
    for (String baseDir : folders) {
      File fileFolder = new File(baseDir, storageGroupName);
      if (!fileFolder.exists()) {
        continue;
      }
      // some TsFileResource may be being persisted when the system crashed, try recovering such
      // resources
      continueFailedRenames(fileFolder, TEMP_SUFFIX);

      // some TsFiles were going to be replaced by the merged files when the system crashed and
      // the process was interrupted before the merged files could be named
      continueFailedRenames(fileFolder, MERGE_SUFFIX);

      Collections
          .addAll(tsFiles, fileFolder.listFiles(file -> file.getName().endsWith(TSFILE_SUFFIX)));
    }
    tsFiles.sort(this::compareFileName);
    List<TsFileResource> ret = new ArrayList<>();
    tsFiles.forEach(f -> ret.add(new TsFileResource(f)));
    return ret;
  }

  private void continueFailedRenames(File fileFolder, String suffix) {
    File[] files = fileFolder.listFiles(file -> file.getName().endsWith(suffix));
    if (files != null) {
      for (File tempResource : files) {
        File originResource = new File(tempResource.getPath().replace(suffix, ""));
        if (originResource.exists()) {
          tempResource.delete();
        } else {
          tempResource.renameTo(originResource);
        }
      }
    }
  }

  private void recoverSeqFiles(List<TsFileResource> tsFiles) throws ProcessorException {

    for (TsFileResource tsFileResource : tsFiles) {
      sequenceFileList.add(tsFileResource);
      TsFileRecoverPerformer recoverPerformer = new TsFileRecoverPerformer(storageGroupName + "-"
          , schema, versionController, tsFileResource, false);
      recoverPerformer.recover();
      tsFileResource.setClosed(true);
    }
  }

  private void recoverUnseqFiles(List<TsFileResource> tsFiles) throws ProcessorException {
    for (TsFileResource tsFileResource : tsFiles) {
      unSequenceFileList.add(tsFileResource);
      TsFileRecoverPerformer recoverPerformer = new TsFileRecoverPerformer(storageGroupName + "-",
          schema,
          versionController, tsFileResource, true);
      recoverPerformer.recover();
      tsFileResource.setClosed(true);
    }
  }

  // TsFileNameComparator compares TsFiles by the version number in its name
  // ({systemTime}-{versionNum}-{mergeNum}.tsfile)
  private int compareFileName(File o1, File o2) {
    String[] items1 = o1.getName().replace(TSFILE_SUFFIX, "")
        .split(IoTDBConstant.FILE_NAME_SEPARATOR);
    String[] items2 = o2.getName().replace(TSFILE_SUFFIX, "")
        .split(IoTDBConstant.FILE_NAME_SEPARATOR);
    if (Long.valueOf(items1[0]) - Long.valueOf(items2[0]) == 0) {
      return Long.compare(Long.valueOf(items1[1]), Long.valueOf(items2[1]));
    } else {
      return Long.compare(Long.valueOf(items1[0]), Long.valueOf(items2[0]));
    }
  }

  private Schema constructSchema(String storageGroupName) {
    List<MeasurementSchema> columnSchemaList;
    columnSchemaList = MManager.getInstance().getSchemaForStorageGroup(storageGroupName);

    Schema schema = new Schema();
    for (MeasurementSchema measurementSchema : columnSchemaList) {
      schema.registerMeasurement(measurementSchema);
    }
    return schema;
  }


  /**
   * add a measurement into the schema.
   */
  public void addMeasurement(String measurementId, TSDataType dataType, TSEncoding encoding,
      CompressionType compressor, Map<String, String> props) {
    writeLock();
    try {
      schema.registerMeasurement(new MeasurementSchema(measurementId, dataType, encoding,
          compressor, props));
    } finally {
      writeUnlock();
    }
  }

  public boolean insert(InsertPlan insertPlan) {
    writeLock();
    try {
      // init map
      latestTimeForEachDevice.putIfAbsent(insertPlan.getDeviceId(), Long.MIN_VALUE);
      latestFlushedTimeForEachDevice.putIfAbsent(insertPlan.getDeviceId(), Long.MIN_VALUE);

      // insert to sequence or unSequence file
      return insertToTsFileProcessor(insertPlan,
          insertPlan.getTime() > latestFlushedTimeForEachDevice.get(insertPlan.getDeviceId()));
    } finally {
      writeUnlock();
    }
  }

  public Integer[] insertBatch(BatchInsertPlan batchInsertPlan) {
    writeLock();
    try {
      // init map
      latestTimeForEachDevice.putIfAbsent(batchInsertPlan.getDeviceId(), Long.MIN_VALUE);
      latestFlushedTimeForEachDevice.putIfAbsent(batchInsertPlan.getDeviceId(), Long.MIN_VALUE);

      Integer[] results = new Integer[batchInsertPlan.getRowCount()];
      List<Integer> sequenceIndexes = new ArrayList<>();
      List<Integer> unsequenceIndexes = new ArrayList<>();

      for (int i = 0; i < batchInsertPlan.getRowCount(); i++) {
        results[i] = TSStatusType.SUCCESS_STATUS.getStatusCode();
        if (batchInsertPlan.getTimes()[i] > latestFlushedTimeForEachDevice
            .get(batchInsertPlan.getDeviceId())) {
          sequenceIndexes.add(i);
        } else {
          unsequenceIndexes.add(i);
        }
      }

      if (!sequenceIndexes.isEmpty()) {
        insertBatchToTsFileProcessor(batchInsertPlan, sequenceIndexes, true, results);
      }

      if (!unsequenceIndexes.isEmpty()) {
        insertBatchToTsFileProcessor(batchInsertPlan, unsequenceIndexes, false, results);
      }
      return results;
    } finally {
      writeUnlock();
    }
  }

  private void insertBatchToTsFileProcessor(BatchInsertPlan batchInsertPlan,
      List<Integer> indexes, boolean sequence, Integer[] results) {

    TsFileProcessor tsFileProcessor = getOrCreateTsFileProcessor(sequence);
    if (tsFileProcessor == null) {
      for (int index : indexes) {
        results[index] = TSStatusType.INTERNAL_SERVER_ERROR.getStatusCode();
      }
      return;
    }

    boolean result = tsFileProcessor.insertBatch(batchInsertPlan, indexes, results);

    // try to update the latest time of the device of this tsRecord
    if (result && latestTimeForEachDevice.get(batchInsertPlan.getDeviceId()) < batchInsertPlan
        .getMaxTime()) {
      latestTimeForEachDevice.put(batchInsertPlan.getDeviceId(), batchInsertPlan.getMaxTime());
    }

    // check memtable size and may asyncTryToFlush the work memtable
    if (tsFileProcessor.shouldFlush()) {
      logger.info("The memtable size {} reaches the threshold, async flush it to tsfile: {}",
          tsFileProcessor.getWorkMemTableMemory(),
          tsFileProcessor.getTsFileResource().getFile().getAbsolutePath());

      if (tsFileProcessor.shouldClose()) {
        moveOneWorkProcessorToClosingList(sequence);
      } else {
        tsFileProcessor.asyncFlush();
      }
    }
  }

  private boolean insertToTsFileProcessor(InsertPlan insertPlan, boolean sequence) {
    TsFileProcessor tsFileProcessor;
    boolean result;

    tsFileProcessor = getOrCreateTsFileProcessor(sequence);

    if (tsFileProcessor == null) {
      return false;
    }

    // insert TsFileProcessor
    result = tsFileProcessor.insert(insertPlan);

    // try to update the latest time of the device of this tsRecord
    if (result && latestTimeForEachDevice.get(insertPlan.getDeviceId()) < insertPlan.getTime()) {
      latestTimeForEachDevice.put(insertPlan.getDeviceId(), insertPlan.getTime());
    }

    // check memtable size and may asyncTryToFlush the work memtable
    if (tsFileProcessor.shouldFlush()) {
      logger.info("The memtable size {} reaches the threshold, async flush it to tsfile: {}",
          tsFileProcessor.getWorkMemTableMemory(),
          tsFileProcessor.getTsFileResource().getFile().getAbsolutePath());

      if (tsFileProcessor.shouldClose()) {
        moveOneWorkProcessorToClosingList(sequence);
      } else {
        tsFileProcessor.asyncFlush();
      }
    }
    return result;
  }

  private TsFileProcessor getOrCreateTsFileProcessor(boolean sequence) {
    TsFileProcessor tsFileProcessor = null;
    try {
      if (sequence) {
        if (workSequenceTsFileProcessor == null) {
          // create a new TsfileProcessor
          workSequenceTsFileProcessor = createTsFileProcessor(true);
          sequenceFileList.add(workSequenceTsFileProcessor.getTsFileResource());
        }
        tsFileProcessor = workSequenceTsFileProcessor;
      } else {
        if (workUnSequenceTsFileProcessor == null) {
          // create a new TsfileProcessor
          workUnSequenceTsFileProcessor = createTsFileProcessor(false);
          unSequenceFileList.add(workUnSequenceTsFileProcessor.getTsFileResource());
        }
        tsFileProcessor = workUnSequenceTsFileProcessor;
      }
    } catch (DiskSpaceInsufficientException e) {
      logger.error(
          "disk space is insufficient when creating TsFile processor, change system mode to read-only",
          e);
      IoTDBDescriptor.getInstance().getConfig().setReadOnly(true);
    } catch (IOException e) {
      logger
          .error("meet IOException when creating TsFileProcessor, change system mode to read-only",
              e);
      IoTDBDescriptor.getInstance().getConfig().setReadOnly(true);
    }
    return tsFileProcessor;
  }

  private TsFileProcessor createTsFileProcessor(boolean sequence)
      throws IOException, DiskSpaceInsufficientException {
    String baseDir;
    if (sequence) {
      baseDir = DirectoryManager.getInstance().getNextFolderForSequenceFile();
    } else {
      baseDir = DirectoryManager.getInstance().getNextFolderForUnSequenceFile();
    }
    new File(baseDir, storageGroupName).mkdirs();

    String filePath = Paths.get(baseDir, storageGroupName,
        System.currentTimeMillis() + IoTDBConstant.FILE_NAME_SEPARATOR + versionController
            .nextVersion()).toString() + IoTDBConstant.FILE_NAME_SEPARATOR + "0" + TSFILE_SUFFIX;

    if (sequence) {
      return new TsFileProcessor(storageGroupName, new File(filePath),
          schema, versionController, this::closeUnsealedTsFileProcessor,
          this::updateLatestFlushTimeCallback, sequence);
    } else {
      return new TsFileProcessor(storageGroupName, new File(filePath),
          schema, versionController, this::closeUnsealedTsFileProcessor,
          () -> true, sequence);
    }
  }


  /**
   * only called by insert(), thread-safety should be ensured by caller
   */
  private void moveOneWorkProcessorToClosingList(boolean sequence) {
    //for sequence tsfile, we update the endTimeMap only when the file is prepared to be closed.
    //for unsequence tsfile, we have maintained the endTimeMap when an insertion comes.
    if (sequence) {
      closingSequenceTsFileProcessor.add(workSequenceTsFileProcessor);
      updateEndTimeMap(workSequenceTsFileProcessor);
      workSequenceTsFileProcessor.asyncClose();
      workSequenceTsFileProcessor = null;
    } else {
      closingUnSequenceTsFileProcessor.add(workUnSequenceTsFileProcessor);
      workUnSequenceTsFileProcessor.asyncClose();
      workUnSequenceTsFileProcessor = null;
    }
  }

  public void syncDeleteDataFiles() {
    waitForAllCurrentTsFileProcessorsClosed();
    writeLock();
    try {
      for (TsFileResource tsFileResource : unSequenceFileList) {
        tsFileResource.close();
      }
      for (TsFileResource tsFileResource : sequenceFileList) {
        tsFileResource.close();
      }
      List<String> folder = DirectoryManager.getInstance().getAllSequenceFileFolders();
      folder.addAll(DirectoryManager.getInstance().getAllUnSequenceFileFolders());
      for (String tsfilePath : folder) {
        File storageGroupFolder = new File(tsfilePath, storageGroupName);
        if (storageGroupFolder.exists()) {
          try {
            FileUtils.deleteDirectory(storageGroupFolder);
          } catch (IOException e) {
            logger.error("Delete tsfiles failed", e);
          }
        }
      }
      this.workSequenceTsFileProcessor = null;
      this.workUnSequenceTsFileProcessor = null;
      this.sequenceFileList.clear();
      this.unSequenceFileList.clear();
      this.latestFlushedTimeForEachDevice.clear();
      this.latestTimeForEachDevice.clear();
    } catch (IOException e) {
      logger.error("Cannot delete files in storage group {}", storageGroupName, e);
    } finally {
      writeUnlock();
    }
  }

  /**
   * This method will be blocked until all tsfile processors are closed.
   */
  public void waitForAllCurrentTsFileProcessorsClosed() {
    synchronized (closeStorageGroupCondition) {
      try {
        putAllWorkingTsFileProcessorIntoClosingList();
        while (!closingSequenceTsFileProcessor.isEmpty() || !closingUnSequenceTsFileProcessor
            .isEmpty()) {
          closeStorageGroupCondition.wait();
        }
      } catch (InterruptedException e) {
        logger.error("CloseFileNodeCondition error occurs while waiting for closing the storage "
            + "group {}", storageGroupName, e);
        Thread.currentThread().interrupt();
      }
    }
  }

  public void putAllWorkingTsFileProcessorIntoClosingList() {
    writeLock();
    try {
      logger.info("async force close all files in storage group: {}", storageGroupName);
      if (workSequenceTsFileProcessor != null) {
        moveOneWorkProcessorToClosingList(true);
      }
      if (workUnSequenceTsFileProcessor != null) {
        moveOneWorkProcessorToClosingList(false);
      }
    } finally {
      writeUnlock();
    }
  }

  // TODO need a read lock, please consider the concurrency with flush manager threads.
  public QueryDataSource query(String deviceId, String measurementId, QueryContext context,
      JobFileManager filePathsManager) {
    insertLock.readLock().lock();
    mergeLock.readLock().lock();
    synchronized (lruForSensorUsedInQuery) {
      if (lruForSensorUsedInQuery.size() >= MAX_CACHE_SENSORS) {
        lruForSensorUsedInQuery.removeFirst();
      }
      lruForSensorUsedInQuery.add(measurementId);
    }
    try {
      List<TsFileResource> seqResources = getFileReSourceListForQuery(sequenceFileList,
          deviceId, measurementId, context);
      List<TsFileResource> unseqResources = getFileReSourceListForQuery(unSequenceFileList,
          deviceId, measurementId, context);
      QueryDataSource dataSource = new QueryDataSource(new Path(deviceId, measurementId),
          seqResources, unseqResources);
      // used files should be added before mergeLock is unlocked, or they may be deleted by
      // running merge
      // is null only in tests
      if (filePathsManager != null) {
        filePathsManager.addUsedFilesForGivenJob(context.getJobId(), dataSource);
      }
      return dataSource;
    } finally {
      insertLock.readLock().unlock();
      mergeLock.readLock().unlock();
    }
  }

  /**
   * returns the top k% measurements which are recently used in queries.
   */
  public Set calTopKMeasurement(String sensorId, double k) {
    int num = (int) (lruForSensorUsedInQuery.size() * k);
    Set<String> sensorSet = new HashSet<>(num + 1);
    synchronized (lruForSensorUsedInQuery) {
      Iterator<String> iterator = lruForSensorUsedInQuery.descendingIterator();
      while (iterator.hasNext() && sensorSet.size() < num) {
        String sensor = iterator.next();
        if (sensorSet.contains(sensor)) {
          iterator.remove();
        } else {
          sensorSet.add(sensor);
        }
      }
    }
    sensorSet.add(sensorId);
    return sensorSet;
  }

  private void writeLock() {
    insertLock.writeLock().lock();
  }

  private void writeUnlock() {
    insertLock.writeLock().unlock();
  }


  /**
   * @param tsFileResources includes sealed and unsealed tsfile resources
   * @return fill unsealed tsfile resources with memory data and ChunkMetadataList of data in disk
   */
  private List<TsFileResource> getFileReSourceListForQuery(List<TsFileResource> tsFileResources,
      String deviceId, String measurementId, QueryContext context) {

    MeasurementSchema mSchema = schema.getMeasurementSchema(measurementId);
    TSDataType dataType = mSchema.getType();

    List<TsFileResource> tsfileResourcesForQuery = new ArrayList<>();
    for (TsFileResource tsFileResource : tsFileResources) {
      // TODO: try filtering files if the query contains time filter
      if (!tsFileResource.containsDevice(deviceId)) {
        continue;
      }
      if (!tsFileResource.getStartTimeMap().isEmpty()) {
        closeQueryLock.readLock().lock();
        try {
          if (tsFileResource.isClosed()) {
            tsfileResourcesForQuery.add(tsFileResource);
          } else {
            // left: in-memory data, right: meta of disk data
            Pair<ReadOnlyMemChunk, List<ChunkMetaData>> pair;
            pair = tsFileResource
                .getUnsealedFileProcessor()
                .query(deviceId, measurementId, dataType, mSchema.getProps(), context);
            tsfileResourcesForQuery
                .add(new TsFileResource(tsFileResource.getFile(),
                    tsFileResource.getStartTimeMap(),
                    tsFileResource.getEndTimeMap(), pair.left, pair.right));
          }
        } finally {
          closeQueryLock.readLock().unlock();
        }
      }
    }
    return tsfileResourcesForQuery;
  }


  /**
   * Delete data whose timestamp <= 'timestamp' and belongs to the timeseries
   * deviceId.measurementId.
   *
   * @param deviceId the deviceId of the timeseries to be deleted.
   * @param measurementId the measurementId of the timeseries to be deleted.
   * @param timestamp the delete range is (0, timestamp].
   */
  public void delete(String deviceId, String measurementId, long timestamp) throws IOException {
    // TODO: how to avoid partial deletion?
    writeLock();
    mergeLock.writeLock().lock();

    // record files which are updated so that we can roll back them in case of exception
    List<ModificationFile> updatedModFiles = new ArrayList<>();

    try {
      Long lastUpdateTime = latestTimeForEachDevice.get(deviceId);
      // no tsfile data, the delete operation is invalid
      if (lastUpdateTime == null) {
        logger.debug("No device {} in SG {}, deletion invalid", deviceId, storageGroupName);
        return;
      }

      // write log
      if (IoTDBDescriptor.getInstance().getConfig().isEnableWal()) {
        if (workSequenceTsFileProcessor != null) {
          workSequenceTsFileProcessor.getLogNode()
              .write(new DeletePlan(timestamp, new Path(deviceId, measurementId)));
        }
        if (workUnSequenceTsFileProcessor != null) {
          workUnSequenceTsFileProcessor.getLogNode()
              .write(new DeletePlan(timestamp, new Path(deviceId, measurementId)));
        }
      }

      Path fullPath = new Path(deviceId, measurementId);
      Deletion deletion = new Deletion(fullPath, versionController.nextVersion(), timestamp);
      if (mergingModification != null) {
        mergingModification.write(deletion);
        updatedModFiles.add(mergingModification);
      }

      deleteDataInFiles(sequenceFileList, deletion, updatedModFiles);
      deleteDataInFiles(unSequenceFileList, deletion, updatedModFiles);

    } catch (Exception e) {
      // roll back
      for (ModificationFile modFile : updatedModFiles) {
        modFile.abort();
      }
      throw new IOException(e);
    } finally {
      writeUnlock();
      mergeLock.writeLock().unlock();
    }
  }


  private void deleteDataInFiles(List<TsFileResource> tsFileResourceList, Deletion deletion,
      List<ModificationFile> updatedModFiles)
      throws IOException {
    String deviceId = deletion.getDevice();
    for (TsFileResource tsFileResource : tsFileResourceList) {
      if (!tsFileResource.containsDevice(deviceId) ||
          deletion.getTimestamp() < tsFileResource.getStartTimeMap().get(deviceId)) {
        continue;
      }

      // write deletion into modification file
      tsFileResource.getModFile().write(deletion);

      // delete data in memory of unsealed file
      if (!tsFileResource.isClosed()) {
        TsFileProcessor tsfileProcessor = tsFileResource.getUnsealedFileProcessor();
        tsfileProcessor.deleteDataInMemory(deletion);
      }

      // add a record in case of rollback
      updatedModFiles.add(tsFileResource.getModFile());
    }
  }

  /**
   * when close an TsFileProcessor, update its EndTimeMap immediately
   *
   * @param tsFileProcessor processor to be closed
   */
  private void updateEndTimeMap(TsFileProcessor tsFileProcessor) {
    TsFileResource resource = tsFileProcessor.getTsFileResource();
    for (Entry<String, Long> startTime : resource.getStartTimeMap().entrySet()) {
      String deviceId = startTime.getKey();
      resource.forceUpdateEndTime(deviceId, latestTimeForEachDevice.get(deviceId));
    }
  }


  private boolean updateLatestFlushTimeCallback() {
    // update the largest timestamp in the last flushing memtable
    for (Entry<String, Long> entry : latestTimeForEachDevice.entrySet()) {
      latestFlushedTimeForEachDevice.put(entry.getKey(), entry.getValue());
    }
    return true;
  }

  /**
   * put the memtable back to the MemTablePool and make the metadata in writer visible
   */
  // TODO please consider concurrency with query and insert method.
  public void closeUnsealedTsFileProcessor(
      TsFileProcessor tsFileProcessor) throws TsFileProcessorException {
    closeQueryLock.writeLock().lock();
    try {
      tsFileProcessor.close();
    } finally {
      closeQueryLock.writeLock().unlock();
    }
    //closingSequenceTsFileProcessor is a thread safety class.
    if (closingSequenceTsFileProcessor.contains(tsFileProcessor)) {
      closingSequenceTsFileProcessor.remove(tsFileProcessor);
    } else {
      closingUnSequenceTsFileProcessor.remove(tsFileProcessor);
    }
    logger.info("signal closing storage group condition in {}", storageGroupName);
    synchronized (closeStorageGroupCondition) {
      closeStorageGroupCondition.notifyAll();
    }
  }

  public void merge(boolean fullMerge) {
    writeLock();
    try {
      if (isMerging) {
        if (logger.isInfoEnabled()) {
          logger.info("{} Last merge is ongoing, currently consumed time: {}ms", storageGroupName,
              (System.currentTimeMillis() - mergeStartTime));
        }
        return;
      }
      if (unSequenceFileList.isEmpty() || sequenceFileList.isEmpty()) {
        logger.info("{} no files to be merged", storageGroupName);
        return;
      }

      long budget = IoTDBDescriptor.getInstance().getConfig().getMergeMemoryBudget();
      MergeResource mergeResource = new MergeResource(sequenceFileList, unSequenceFileList);
      IMergeFileSelector fileSelector = getMergeFileSelector(budget, mergeResource);
      try {
        List[] mergeFiles = fileSelector.select();
        if (mergeFiles.length == 0) {
          logger.info("{} cannot select merge candidates under the budget {}", storageGroupName,
              budget);
          return;
        }
        // avoid pending tasks holds the metadata and streams
        mergeResource.clear();
        String taskName = storageGroupName + "-" + System.currentTimeMillis();
        // do not cache metadata until true candidates are chosen, or too much metadata will be
        // cached during selection
        mergeResource.setCacheDeviceMeta(true);

        MergeTask mergeTask = new MergeTask(mergeResource, storageGroupSysDir.getPath(),
            this::mergeEndAction, taskName, fullMerge, fileSelector.getConcurrentMergeNum(),
            storageGroupName);
        mergingModification = new ModificationFile(
            storageGroupSysDir + File.separator + MERGING_MODIFICAITON_FILE_NAME);
        MergeManager.getINSTANCE().submitMainTask(mergeTask);
        if (logger.isInfoEnabled()) {
          logger.info("{} submits a merge task {}, merging {} seqFiles, {} unseqFiles",
              storageGroupName, taskName, mergeFiles[0].size(), mergeFiles[1].size());
        }
        isMerging = true;
        mergeStartTime = System.currentTimeMillis();

      } catch (MergeException | IOException e) {
        logger.error("{} cannot select file for merge", storageGroupName, e);
      }
    } finally {
      writeUnlock();
    }
  }

  private IMergeFileSelector getMergeFileSelector(long budget, MergeResource resource) {
    MergeFileStrategy strategy = IoTDBDescriptor.getInstance().getConfig().getMergeFileStrategy();
    switch (strategy) {
      case MAX_FILE_NUM:
        return new MaxFileMergeFileSelector(resource, budget);
      case MAX_SERIES_NUM:
        return new MaxSeriesMergeFileSelector(resource, budget);
      default:
        throw new UnsupportedOperationException("Unknown MergeFileStrategy " + strategy);
    }
  }

  protected void mergeEndAction(List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles,
      File mergeLog) {
    logger.info("{} a merge task is ending...", storageGroupName);

    if (unseqFiles.isEmpty()) {
      // merge runtime exception arose, just end this merge
      isMerging = false;
      logger.info("{} a merge task abnormally ends", storageGroupName);
      return;
    }

    mergeLock.writeLock().lock();
    try {
      unSequenceFileList.removeAll(unseqFiles);
    } finally {
      mergeLock.writeLock().unlock();
    }

    for (TsFileResource unseqFile : unseqFiles) {
      unseqFile.getMergeQueryLock().writeLock().lock();
      try {
        unseqFile.remove();
      } finally {
        unseqFile.getMergeQueryLock().writeLock().unlock();
      }
    }

    for (int i = 0; i < seqFiles.size(); i++) {
      TsFileResource seqFile = seqFiles.get(i);
      seqFile.getMergeQueryLock().writeLock().lock();
      mergeLock.writeLock().lock();
      try {
        logger.debug("{} is updating the {} merged file's modification file", storageGroupName, i);
        try {
          // remove old modifications and write modifications generated during merge
          seqFile.removeModFile();
          if (mergingModification != null) {
            for (Modification modification : mergingModification.getModifications()) {
              seqFile.getModFile().write(modification);
            }
          }
        } catch (IOException e) {
          logger.error("{} cannot clean the ModificationFile of {} after merge", storageGroupName,
              seqFile.getFile(), e);
        }
        if (i == seqFiles.size() - 1) {
          try {
            if (mergingModification != null) {
              mergingModification.remove();
              mergingModification = null;
            }
          } catch (IOException e) {
            logger.error("{} cannot remove merging modification ", storageGroupName, e);
          }
          isMerging = false;
        }
      } finally {
        mergeLog.delete();
        seqFile.getMergeQueryLock().writeLock().unlock();
        mergeLock.writeLock().unlock();
      }
    }
    logger.info("{} a merge task ends", storageGroupName);
  }

  /**
   * Load a new tsfile to storage group processor
   *
   * Firstly, determine the loading type of the file, whether it needs to be loaded in sequence list
   * or unsequence list.
   *
   * Secondly, execute the loading process by the type.
   *
   * Finally, update the latestTimeForEachDevice and latestFlushedTimeForEachDevice.
   *
   * @param newTsFile new tsfile
   * @param newTsFileResource tsfile resource
   * @UsedBy sync module.
   */
  public void loadNewTsFile(File newTsFile, TsFileResource newTsFileResource)
      throws TsFileProcessorException {
    writeLock();
    mergeLock.writeLock().lock();
    try {
      boolean isOverlap = false;
      int preIndex = -1, subsequentIndex = sequenceFileList.size();
      // check new tsfile
      outer:
      for (int i = 0; i < sequenceFileList.size(); i++) {
        if (sequenceFileList.get(i).getFile().getName().equals(newTsFile.getName())) {
          return;
        }
        if (i == sequenceFileList.size() - 1 && sequenceFileList.get(i).getEndTimeMap().isEmpty()) {
          continue;
        }
        int preCnt = 0, subsequenceCnt = 0;
        for (String device : newTsFileResource.getStartTimeMap().keySet()) {
          if (sequenceFileList.get(i).getStartTimeMap().containsKey(device)) {
            long startTime1 = sequenceFileList.get(i).getStartTimeMap().get(device);
            long endTime1 = sequenceFileList.get(i).getEndTimeMap().get(device);
            long startTime2 = newTsFileResource.getStartTimeMap().get(device);
            long endTime2 = newTsFileResource.getEndTimeMap().get(device);
            if (startTime1 > endTime2) {
              subsequenceCnt++;
            } else if (startTime2 > endTime1) {
              preCnt++;
            } else {
              isOverlap = true;
              break outer;
            }
          }
        }
        if (preCnt != 0 && subsequenceCnt != 0) {
          isOverlap = true;
          break;
        }
        if (preCnt == 0 && subsequenceCnt != 0) {
          subsequentIndex = i;
          break;
        }
        if (preCnt != 0 && subsequenceCnt == 0) {
          preIndex = i;
        }
      }

      // loading tsfile by type
      if (isOverlap) {
        loadTsFileByType(-1, newTsFile, newTsFileResource, unSequenceFileList.size());
      } else {
        if (subsequentIndex != sequenceFileList.size()) {
          loadTsFileByType(1, newTsFile, newTsFileResource, subsequentIndex);
        } else {
          if (preIndex != -1) {
            loadTsFileByType(1, newTsFile, newTsFileResource, preIndex + 1);
          } else {
            loadTsFileByType(1, newTsFile, newTsFileResource, sequenceFileList.size());
          }
        }
      }

      // update latest time map
      updateLatestTimeMap(newTsFileResource);
    } catch (TsFileProcessorException | DiskSpaceInsufficientException e) {
      logger.error("Failed to append the tsfile {} to storage group processor {}.",
          newTsFile.getAbsolutePath(), newTsFile.getParentFile().getName());
      throw new TsFileProcessorException(e);
    } finally {
      mergeLock.writeLock().unlock();
      writeUnlock();
    }
  }

  /**
   * Update latest time in latestTimeForEachDevice and latestFlushedTimeForEachDevice.
   *
   * @UsedBy sync module
   */
  private void updateLatestTimeMap(TsFileResource newTsFileResource) {
    for (Entry<String, Long> entry : newTsFileResource.getEndTimeMap().entrySet()) {
      String device = entry.getKey();
      long endTime = newTsFileResource.getEndTimeMap().get(device);
      if (!latestTimeForEachDevice.containsKey(device)
          || latestTimeForEachDevice.get(device) < endTime) {
        latestTimeForEachDevice.put(device, endTime);
      }
      if (!latestFlushedTimeForEachDevice.containsKey(device)
          || latestFlushedTimeForEachDevice.get(device) < endTime) {
        latestFlushedTimeForEachDevice.put(device, endTime);
      }
    }
  }

  /**
   * Execute the loading process by the type.
   *
   * @param type load type: 1 sequence tsfile ; 2 unsequence tsfile
   * @param tsFile tsfile to be loaded
   * @param tsFileResource tsfile resource to be loaded
   * @param index the index in sequenceFileList/unSequenceFileList
   * @UsedBy sync module
   */
  private void loadTsFileByType(int type, File tsFile, TsFileResource tsFileResource, int index)
      throws TsFileProcessorException, DiskSpaceInsufficientException {
    File targetFile;
    if (type == -1) {
      targetFile =
          new File(DirectoryManager.getInstance().getNextFolderForUnSequenceFile(),
              tsFile.getParentFile().getName() + File.separatorChar + tsFile.getName());
      tsFileResource.setFile(targetFile);
      unSequenceFileList.add(index, tsFileResource);
      logger
          .info("Load tsfile in unsequence list, move file from {} to {}", tsFile.getAbsolutePath(),
              targetFile.getAbsolutePath());
    } else {
      targetFile =
          new File(DirectoryManager.getInstance().getNextFolderForSequenceFile(),
              tsFile.getParentFile().getName() + File.separatorChar + getFileNameForLoadingFile(
                  tsFile.getName(), index));
      tsFileResource.setFile(targetFile);
      sequenceFileList.add(index, tsFileResource);
      logger.info("Load tsfile in sequence list, move file from {} to {}", tsFile.getAbsolutePath(),
          targetFile.getAbsolutePath());
    }

    // move file from sync dir to data dir
    if (!targetFile.getParentFile().exists()) {
      targetFile.getParentFile().mkdirs();
    }
    if (tsFile.exists() && !targetFile.exists()) {
      try {
        FileUtils.moveFile(tsFile, targetFile);
      } catch (IOException e) {
        throw new TsFileProcessorException(String.format(
            "File renaming failed when loading tsfile. Origin: %s, Target: %s",
            tsFile.getAbsolutePath(), targetFile.getAbsolutePath()));
      }
    }
    if (new File(tsFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).exists() && !new File(
        targetFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).exists()) {
      try {
        FileUtils.moveFile(new File(tsFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX),
            new File(targetFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX));
      } catch (IOException e) {
        throw new TsFileProcessorException(String.format(
            "File renaming failed when loading .resource file. Origin: %s, Target: %s",
            new File(tsFile, TsFileResource.RESOURCE_SUFFIX).getAbsolutePath(),
            new File(targetFile, TsFileResource.RESOURCE_SUFFIX).getAbsolutePath()));
      }
    }
  }

  /**
   * Get an appropriate filename to ensure the order between files
   *
   * @param tsfileName origin tsfile name
   * @param index the index to be inserted
   * @return appropriate filename
   */
  private String getFileNameForLoadingFile(String tsfileName, int index) {
    long currentTsFileTime = Long.parseLong(tsfileName.split(IoTDBConstant.FILE_NAME_SEPARATOR)[0]);
    long preTime;
    if (index == 0) {
      preTime = 0L;
    } else {
      String preName = sequenceFileList.get(index - 1).getFile().getName();
      preTime = Long.parseLong(preName.split(IoTDBConstant.FILE_NAME_SEPARATOR)[0]);
    }
    if (index == sequenceFileList.size()) {
      return preTime < currentTsFileTime ? tsfileName
          : System.currentTimeMillis() + IoTDBConstant.FILE_NAME_SEPARATOR + versionController
              .nextVersion() + IoTDBConstant.FILE_NAME_SEPARATOR + "0" + TSFILE_SUFFIX;
    } else {
      String subsequenceName = sequenceFileList.get(index).getFile().getName();
      long subsequenceTime = Long
          .parseLong(subsequenceName.split(IoTDBConstant.FILE_NAME_SEPARATOR)[0]);
      long subsequenceVersion = Long
          .parseLong(subsequenceName.split(IoTDBConstant.FILE_NAME_SEPARATOR)[1]);
      if (preTime < currentTsFileTime && currentTsFileTime < subsequenceTime) {
        return tsfileName;
      } else {
        return (preTime + ((subsequenceTime - preTime) >> 1)) + IoTDBConstant.FILE_NAME_SEPARATOR
            + subsequenceVersion + IoTDBConstant.FILE_NAME_SEPARATOR + "0" + TSFILE_SUFFIX;
      }
    }
  }

  /**
   * Delete tsfile if it exists.
   *
   * Firstly, remove the TsFileResource from sequenceFileList/unSequenceFileList.
   *
   * Secondly, delete the tsfile and .resource file.
   *
   * @param deletedTsfile tsfile to be deleted
   * @UsedBy sync module.
   */
  public void deleteTsfile(File deletedTsfile) {
    writeLock();
    mergeLock.writeLock().lock();
    TsFileResource deletedTsFileResource = null;
    try {
      Iterator<TsFileResource> sequenceIterator = sequenceFileList.iterator();
      while (sequenceIterator.hasNext()) {
        TsFileResource sequenceResource = sequenceIterator.next();
        if (sequenceResource.getFile().getName().equals(deletedTsfile.getName())) {
          deletedTsFileResource = sequenceResource;
          sequenceIterator.remove();
          break;
        }
      }
      if (deletedTsFileResource == null) {
        Iterator<TsFileResource> unsequenceIterator = unSequenceFileList.iterator();
        while (unsequenceIterator.hasNext()) {
          TsFileResource unsequenceResource = unsequenceIterator.next();
          if (unsequenceResource.getFile().getName().equals(deletedTsfile.getName())) {
            deletedTsFileResource = unsequenceResource;
            unsequenceIterator.remove();
            break;
          }
        }
      }
    } finally {
      mergeLock.writeLock().unlock();
      writeUnlock();
    }
    if (deletedTsFileResource == null) {
      return;
    }
    deletedTsFileResource.getMergeQueryLock().writeLock().lock();
    try {
      logger.info("Delete tsfile {} in sync loading process.", deletedTsFileResource.getFile());
      deletedTsFileResource.getFile().delete();
      new File(deletedTsFileResource.getFile().getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX)
          .delete();
    } finally {
      deletedTsFileResource.getMergeQueryLock().writeLock().unlock();
    }
  }

  public TsFileProcessor getWorkSequenceTsFileProcessor() {
    return workSequenceTsFileProcessor;
  }

  public List<TsFileResource> getSequenceFileList() {
    return sequenceFileList;
  }

  public List<TsFileResource> getUnSequenceFileList() {
    return unSequenceFileList;
  }

  @FunctionalInterface
  public interface CloseTsFileCallBack {

    void call(TsFileProcessor caller) throws TsFileProcessorException, IOException;
  }

}
