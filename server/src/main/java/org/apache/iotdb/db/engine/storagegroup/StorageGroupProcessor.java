/*
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
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
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.OutOfTTLException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.storageGroup.StorageGroupProcessorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.physical.crud.BatchInsertPlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.JobFileManager;
import org.apache.iotdb.db.utils.CopyOnReadLinkedList;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.db.utils.UpgradeUtils;
import org.apache.iotdb.db.writelog.recover.TsFileRecoverPerformer;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
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

  private static final String MERGING_MODIFICATION_FILE_NAME = "merge.mods";
  private static final Logger logger = LoggerFactory.getLogger(StorageGroupProcessor.class);
  private static final int MAX_CACHE_SENSORS = 5000;
  /**
   * a read write lock for guaranteeing concurrent safety when accessing all fields in this class
   * (i.e., schema, (un)sequenceFileList, work(un)SequenceTsFileProcessor,
   * closing(Un)SequenceTsFileProcessor, latestTimeForEachDevice, and
   * latestFlushedTimeForEachDevice)
   */
  private final ReadWriteLock insertLock = new ReentrantReadWriteLock();
  /**
   * closeStorageGroupCondition is used to wait for all currently closing TsFiles to be done.
   */
  private final Object closeStorageGroupCondition = new Object();
  /**
   * avoid some tsfileResource is changed (e.g., from unsealed to sealed) when a query is executed.
   */
  private final ReadWriteLock closeQueryLock = new ReentrantReadWriteLock();
  private final HashMap<Long, TsFileProcessor> workSequenceTsFileProcessor = new HashMap<>();
  private final HashMap<Long, Long> sequenceTsfileProcessorLastUseTime = new HashMap<>();
  private final HashMap<Long, TsFileProcessor> workUnsequenceTsFileProcessor = new HashMap<>();
  private final HashMap<Long, Long> unsequenceTsfileProcessorLastUseTime = new HashMap<>();
  // Time range for divide storage group, unit is second
  private final long timeRangeForStorageGroup = IoTDBDescriptor.getInstance().getConfig()
      .getTimeRangeForStorageGroup();
  /**
   * the schema of time series that belong this storage group
   */
  private Schema schema;
  // includes sealed and unsealed sequence TsFiles
  private TreeSet<TsFileResource> sequenceFileList = new TreeSet<>(
      new Comparator<TsFileResource>() {
        @Override
        public int compare(TsFileResource o1, TsFileResource o2) {
          if (o1.getProcessor() == null || o2.getProcessor() == null) {
            return compareFileName(o1.getFile(), o2.getFile());
          }
          int rangeCompare = Long
              .compare(o1.getProcessor().getTimeRange(), o2.getProcessor().getTimeRange());
          return rangeCompare == 0 ? compareFileName(o1.getFile(), o2.getFile()) : rangeCompare;
        }
      });
  private CopyOnReadLinkedList<TsFileProcessor> closingSequenceTsFileProcessor = new CopyOnReadLinkedList<>();
  // includes sealed and unsealed unSequence TsFiles
  private List<TsFileResource> unSequenceFileList = new ArrayList<>();
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
  /**
   * when the data in a storage group is older than dataTTL, it is considered invalid and will be
   * eventually removed.
   */
  private long dataTTL = Long.MAX_VALUE;
  private FSFactory fsFactory = FSFactoryProducer.getFSFactory();

  public StorageGroupProcessor(String systemInfoDir, String storageGroupName)
      throws StorageGroupProcessorException {
    this.storageGroupName = storageGroupName;

    // construct the file schema
    this.schema = constructSchema(storageGroupName);

    try {
      storageGroupSysDir = SystemFileFactory.INSTANCE.getFile(systemInfoDir, storageGroupName);
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

  private void recover() throws StorageGroupProcessorException {
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
      File mergingMods = SystemFileFactory.INSTANCE.getFile(storageGroupSysDir,
          MERGING_MODIFICATION_FILE_NAME);
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
    } catch (IOException | MetadataException e) {
      throw new StorageGroupProcessorException(e);
    }

    for (TsFileResource resource : sequenceFileList) {
      latestTimeForEachDevice.putAll(resource.getEndTimeMap());
      latestFlushedTimeForEachDevice.putAll(resource.getEndTimeMap());
    }
  }

  private List<TsFileResource> getAllFiles(List<String> folders) {
    List<File> tsFiles = new ArrayList<>();
    for (String baseDir : folders) {
      File fileFolder = fsFactory.getFile(baseDir, storageGroupName);
      if (!fileFolder.exists()) {
        continue;
      }

      for (File timeRangeFileFolder : fileFolder.listFiles()) {
        // some TsFileResource may be being persisted when the system crashed, try recovering such
        // resources
        continueFailedRenames(timeRangeFileFolder, TEMP_SUFFIX);

        // some TsFiles were going to be replaced by the merged files when the system crashed and
        // the process was interrupted before the merged files could be named
        continueFailedRenames(timeRangeFileFolder, MERGE_SUFFIX);

        Collections.addAll(tsFiles,
            fsFactory.listFilesBySuffix(timeRangeFileFolder.getAbsolutePath(), TSFILE_SUFFIX));
      }

    }
    tsFiles.sort(this::compareFileName);
    List<TsFileResource> ret = new ArrayList<>();
    tsFiles.forEach(f -> ret.add(new TsFileResource(f)));
    return ret;
  }

  private void continueFailedRenames(File fileFolder, String suffix) {
    File[] files = fsFactory.listFilesBySuffix(fileFolder.getAbsolutePath(), suffix);
    if (files != null) {
      for (File tempResource : files) {
        File originResource = fsFactory.getFile(tempResource.getPath().replace(suffix, ""));
        if (originResource.exists()) {
          tempResource.delete();
        } else {
          tempResource.renameTo(originResource);
        }
      }
    }
  }

  private void recoverSeqFiles(List<TsFileResource> tsFiles) throws StorageGroupProcessorException {
    for (TsFileResource tsFileResource : tsFiles) {
      sequenceFileList.add(tsFileResource);
      TsFileRecoverPerformer recoverPerformer = new TsFileRecoverPerformer(storageGroupName + "-"
          , schema, versionController, tsFileResource, false);
      recoverPerformer.recover();
      tsFileResource.setClosed(true);
    }
  }

  private void recoverUnseqFiles(List<TsFileResource> tsFiles)
      throws StorageGroupProcessorException {
    for (TsFileResource tsFileResource : tsFiles) {
      unSequenceFileList.add(tsFileResource);
      TsFileRecoverPerformer recoverPerformer = new TsFileRecoverPerformer(storageGroupName + "-",
          schema,
          versionController, tsFileResource, true);
      recoverPerformer.recover();
      tsFileResource.setClosed(true);
    }
  }

  // ({systemTime}-{versionNum}-{mergeNum}.tsfile)
  private int compareFileName(File o1, File o2) {
    String[] items1 = o1.getName().replace(TSFILE_SUFFIX, "")
        .split(IoTDBConstant.TSFILE_NAME_SEPARATOR);
    String[] items2 = o2.getName().replace(TSFILE_SUFFIX, "")
        .split(IoTDBConstant.TSFILE_NAME_SEPARATOR);
    long ver1 = Long.parseLong(items1[0]);
    long ver2 = Long.parseLong(items2[0]);
    int cmp = Long.compare(ver1, ver2);
    if (cmp == 0) {
      return Long.compare(Long.parseLong(items1[1]), Long.parseLong(items2[1]));
    } else {
      return cmp;
    }
  }

  private Schema constructSchema(String storageGroupName) {
    List<MeasurementSchema> columnSchemaList;
    columnSchemaList = MManager.getInstance().getSchemaForStorageGroup(storageGroupName);

    Schema newSchema = new Schema();
    for (MeasurementSchema measurementSchema : columnSchemaList) {
      newSchema.registerMeasurement(measurementSchema);
    }
    return newSchema;
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

  public void insert(InsertPlan insertPlan) throws QueryProcessException {
    // reject insertions that are out of ttl
    if (!checkTTL(insertPlan.getTime())) {
      throw new OutOfTTLException(insertPlan.getTime(), (System.currentTimeMillis() - dataTTL));
    }
    writeLock();
    try {
      // init map
      latestTimeForEachDevice.putIfAbsent(insertPlan.getDeviceId(), Long.MIN_VALUE);
      latestFlushedTimeForEachDevice.putIfAbsent(insertPlan.getDeviceId(), Long.MIN_VALUE);

      // insert to sequence or unSequence file
      insertToTsFileProcessor(insertPlan,
          insertPlan.getTime() > latestFlushedTimeForEachDevice.get(insertPlan.getDeviceId()));
    } finally {
      writeUnlock();
    }
  }

  public Integer[] insertBatch(BatchInsertPlan batchInsertPlan) throws QueryProcessException {
    writeLock();
    try {
      // init map
      latestTimeForEachDevice.putIfAbsent(batchInsertPlan.getDeviceId(), Long.MIN_VALUE);
      latestFlushedTimeForEachDevice.putIfAbsent(batchInsertPlan.getDeviceId(), Long.MIN_VALUE);

      Integer[] results = new Integer[batchInsertPlan.getRowCount()];
      long lastFlushTime = latestFlushedTimeForEachDevice.get(batchInsertPlan.getDeviceId());

      HashMap<Long, List<Integer>> sequenceTimeRangeIndexes = new HashMap<>();
      HashMap<Long, List<Integer>> unsequenceTimeRangeIndexes = new HashMap<>();
      for (int i = 0; i < batchInsertPlan.getRowCount(); i++) {
        long currTime = batchInsertPlan.getTimes()[i];
        // skip points that do not satisfy TTL
        if (!checkTTL(currTime)) {
          results[i] = TSStatusCode.OUT_OF_TTL_ERROR.getStatusCode();
          continue;
        }
        results[i] = TSStatusCode.SUCCESS_STATUS.getStatusCode();
        long timeRange = fromTimeToTimeRange(currTime);
        if (currTime > lastFlushTime) {
          // sequence
          List<Integer> curIndex = sequenceTimeRangeIndexes.get(timeRange);
          if (curIndex == null) {
            // if map not contains this time range
            curIndex = new ArrayList<>();
            sequenceTimeRangeIndexes.put(timeRange, curIndex);
          }

          curIndex.add(i);
        } else {
          // unsequence
          List<Integer> curIndex = unsequenceTimeRangeIndexes.get(timeRange);
          if (curIndex == null) {
            // if map not contains this time range
            curIndex = new ArrayList<>();
            unsequenceTimeRangeIndexes.put(timeRange, curIndex);
          }

          curIndex.add(i);
        }
      }

      for (Map.Entry<Long, List<Integer>> entry : sequenceTimeRangeIndexes.entrySet()) {
        insertBatchToTsFileProcessor(batchInsertPlan, entry.getValue(), true, results,
            entry.getKey());
      }

      for (Map.Entry<Long, List<Integer>> entry : unsequenceTimeRangeIndexes.entrySet()) {
        insertBatchToTsFileProcessor(batchInsertPlan, entry.getValue(), false, results,
            entry.getKey());
      }

      return results;
    } finally {
      writeUnlock();
    }
  }

  /**
   * @return whether the given time falls in ttl
   */
  private boolean checkTTL(long time) {
    return dataTTL == Long.MAX_VALUE || (System.currentTimeMillis() - time) <= dataTTL;
  }

  private void insertBatchToTsFileProcessor(BatchInsertPlan batchInsertPlan,
      List<Integer> indexes, boolean sequence, Integer[] results, long timeRange)
      throws QueryProcessException {

    TsFileProcessor tsFileProcessor = getOrCreateTsFileProcessor(fromTimeRangeToTime(timeRange),
        sequence);
    if (tsFileProcessor == null) {
      for (int index : indexes) {
        results[index] = TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode();
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
        moveOneWorkProcessorToClosingList(sequence, tsFileProcessor);
      } else {
        tsFileProcessor.asyncFlush();
      }
    }
  }

  private void insertToTsFileProcessor(InsertPlan insertPlan, boolean sequence)
      throws QueryProcessException {
    TsFileProcessor tsFileProcessor;
    boolean result;

    tsFileProcessor = getOrCreateTsFileProcessor(insertPlan.getTime(),
        sequence);

    if (tsFileProcessor == null) {
      return;
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
        moveOneWorkProcessorToClosingList(sequence, tsFileProcessor);
      } else {
        tsFileProcessor.asyncFlush();
      }
    }
  }

  private TsFileProcessor getOrCreateTsFileProcessor(long time, boolean sequence) {
    TsFileProcessor tsFileProcessor = null;
    try {
      if (sequence) {
        tsFileProcessor = getOrCreateTsFileProcessorIntern(time, workSequenceTsFileProcessor,
            sequenceTsfileProcessorLastUseTime, sequenceFileList, true);
      } else {
        tsFileProcessor = getOrCreateTsFileProcessorIntern(time, workUnsequenceTsFileProcessor,
            unsequenceTsfileProcessorLastUseTime, unSequenceFileList, false);
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

  /**
   * get processor from hashmap, flush oldest processor is necessary
   *
   * @param time insert time
   * @param tsFileProcessorHashMap tsFileProcessorHashMap
   * @param tsfileProcessorLastUseTime last use time of this processor map
   * @param fileList file list to add new processor
   * @param sequence whether is sequence or not
   */
  private TsFileProcessor getOrCreateTsFileProcessorIntern(long time,
      HashMap<Long, TsFileProcessor> tsFileProcessorHashMap,
      HashMap<Long, Long> tsfileProcessorLastUseTime,
      Collection<TsFileResource> fileList,
      boolean sequence)
      throws IOException, DiskSpaceInsufficientException {
    // time partition range
    long timeRange = fromTimeToTimeRange(time);
    TsFileProcessor res = null;
    // we have to ensure only one thread can change workSequenceTsFileProcessor
    writeLock();
    try {
      if (!tsFileProcessorHashMap.containsKey(timeRange)) {
        // we have to remove oldest processor to control the num of the memtables
        if (tsFileProcessorHashMap.size()
            >= IoTDBConstant.MEMTABLE_NUM_IN_EACH_STORAGE_GROUP / 2) {
          long oldestTimeRange = -1;
          long oldestUseTime = Long.MAX_VALUE;
          for (long curTimeRange : tsFileProcessorHashMap.keySet()) {
            long curUseTime = tsfileProcessorLastUseTime.get(curTimeRange);
            if (oldestUseTime > curUseTime) {
              oldestUseTime = curUseTime;
              oldestTimeRange = curTimeRange;
            }
          }
          if (oldestTimeRange == -1) {
            throw new IllegalStateException("MEMTABLE_NUM_IN_EACH_STORAGE_GROUP size is too small: "
                + IoTDBConstant.MEMTABLE_NUM_IN_EACH_STORAGE_GROUP);
          }

          moveOneWorkProcessorToClosingList(sequence, tsFileProcessorHashMap.get(oldestTimeRange));
        }

        // build new processor
        TsFileProcessor newProcessor = createTsFileProcessor(sequence, timeRange);
        tsFileProcessorHashMap.put(timeRange, newProcessor);
        fileList.add(newProcessor.getTsFileResource());
        res = newProcessor;
      } else {
        res = tsFileProcessorHashMap.get(timeRange);
      }

      tsfileProcessorLastUseTime.put(timeRange, System.currentTimeMillis());
    } finally {
      // unlock in finally
      writeUnlock();
    }

    return res;
  }

  private long fromTimeRangeToTime(long timeRange) {
    return timeRange * timeRangeForStorageGroup;
  }

  private long fromTimeToTimeRange(long time) {
    return time / timeRangeForStorageGroup;
  }

  private TsFileProcessor createTsFileProcessor(boolean sequence, long timeRange)
      throws IOException, DiskSpaceInsufficientException {
    String baseDir;
    if (sequence) {
      baseDir = DirectoryManager.getInstance().getNextFolderForSequenceFile();
    } else {
      baseDir = DirectoryManager.getInstance().getNextFolderForUnSequenceFile();
    }
    fsFactory.getFile(baseDir, storageGroupName).mkdirs();

    String filePath =
        baseDir + File.separator + storageGroupName + File.separator + timeRange + File.separator
            + System.currentTimeMillis() + IoTDBConstant.TSFILE_NAME_SEPARATOR + versionController
            .nextVersion() + IoTDBConstant.TSFILE_NAME_SEPARATOR + "0" + TSFILE_SUFFIX;

    TsFileProcessor tsFileProcessor = new TsFileProcessor(storageGroupName,
        fsFactory.getFileWithParent(filePath),
        schema, versionController, this::closeUnsealedTsFileProcessor,
        this::updateLatestFlushTimeCallback, sequence);
    tsFileProcessor.setTimeRange(timeRange);
    return tsFileProcessor;
  }


  /**
   * only called by insert(), thread-safety should be ensured by caller
   */
  private void moveOneWorkProcessorToClosingList(boolean sequence,
      TsFileProcessor tsFileProcessor) {
    //for sequence tsfile, we update the endTimeMap only when the file is prepared to be closed.
    //for unsequence tsfile, we have maintained the endTimeMap when an insertion comes.
    if (sequence) {
      closingSequenceTsFileProcessor.add(tsFileProcessor);
      updateEndTimeMap(tsFileProcessor);
      tsFileProcessor.asyncClose();

      workSequenceTsFileProcessor.remove(tsFileProcessor.getTimeRange());
      sequenceTsfileProcessorLastUseTime.remove(tsFileProcessor.getTimeRange());
      logger.info("close a sequence tsfile processor {}", storageGroupName);
    } else {
      closingUnSequenceTsFileProcessor.add(tsFileProcessor);
      tsFileProcessor.asyncClose();

      workUnsequenceTsFileProcessor.remove(tsFileProcessor.getTimeRange());
      unsequenceTsfileProcessorLastUseTime.remove(tsFileProcessor.getTimeRange());
      logger.info("close an unsequence tsfile processor {}", storageGroupName);
    }
  }

  /**
   * delete the storageGroup's own folder in folder data/system/storage_groups
   */
  public void deleteFolder(String systemDir) {
    waitForAllCurrentTsFileProcessorsClosed();
    writeLock();
    try {
      File storageGroupFolder = SystemFileFactory.INSTANCE.getFile(systemDir, storageGroupName);
      if (storageGroupFolder.exists()) {
        FileUtils.deleteDirectory(storageGroupFolder);
      }
    } catch (IOException e) {
      logger.error("Cannot delete the folder in storage group {}, because", storageGroupName, e);
    } finally {
      writeUnlock();
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
      deleteAllSGFolders(folder);

      this.workSequenceTsFileProcessor.clear();
      this.workUnsequenceTsFileProcessor.clear();
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

  private void deleteAllSGFolders(List<String> folder) {
    for (String tsfilePath : folder) {
      File storageGroupFolder = fsFactory.getFile(tsfilePath, storageGroupName);
      if (storageGroupFolder.exists()) {
        try {
          FileUtils.deleteDirectory(storageGroupFolder);
        } catch (IOException e) {
          logger.error("Delete TsFiles failed", e);
        }
      }
    }
  }

  /**
   * Iterate each TsFile and try to lock and remove those out of TTL.
   */
  public synchronized void checkFilesTTL() {
    if (dataTTL == Long.MAX_VALUE) {
      logger.debug("{}: TTL not set, ignore the check", storageGroupName);
      return;
    }
    long timeLowerBound = System.currentTimeMillis() - dataTTL;
    if (logger.isDebugEnabled()) {
      logger.debug("{}: TTL removing files before {}", storageGroupName, new Date(timeLowerBound));
    }
    // copy to avoid concurrent modification of deletion
    List<TsFileResource> seqFiles = new ArrayList<>(sequenceFileList);
    List<TsFileResource> unseqFiles = new ArrayList<>(unSequenceFileList);

    for (TsFileResource tsFileResource : seqFiles) {
      checkFileTTL(tsFileResource, timeLowerBound, true);
    }
    for (TsFileResource tsFileResource : unseqFiles) {
      checkFileTTL(tsFileResource, timeLowerBound, false);
    }
  }

  private void checkFileTTL(TsFileResource resource, long timeLowerBound, boolean isSeq) {
    if (resource.isMerging() || !resource.isClosed()
        || !resource.isDeleted() && resource.stillLives(timeLowerBound)) {
      return;
    }

    writeLock();
    try {
      // prevent new merges and queries from choosing this file
      resource.setDeleted(true);
      // the file may be chosen for merge after the last check and before writeLock()
      // double check to ensure the file is not used by a merge
      if (resource.isMerging()) {
        return;
      }
      // ensure that the file is not used by any queries
      if (resource.getWriteQueryLock().writeLock().tryLock()) {
        try {
          // physical removal
          resource.remove();
          if (logger.isInfoEnabled()) {
            logger.info("Removed a file {} before {} by ttl ({}ms)", resource.getFile().getPath(),
                new Date(timeLowerBound), dataTTL);
          }
          if (isSeq) {
            sequenceFileList.remove(resource);
          } else {
            unSequenceFileList.remove(resource);
          }
        } finally {
          resource.getWriteQueryLock().writeLock().unlock();
        }
      }
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
        // to avoid concurrent modification problem, we need a new array list
        for (TsFileProcessor tsFileProcessor : new ArrayList<>(
            workSequenceTsFileProcessor.values())) {
          moveOneWorkProcessorToClosingList(true, tsFileProcessor);
        }
      }
      if (workUnsequenceTsFileProcessor != null) {
        // to avoid concurrent modification problem, we need a new array list
        for (TsFileProcessor tsFileProcessor : new ArrayList<>(
            workUnsequenceTsFileProcessor.values())) {
          moveOneWorkProcessorToClosingList(false, tsFileProcessor);
        }
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
      dataSource.setDataTTL(dataTTL);
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
  private List<TsFileResource> getFileReSourceListForQuery(
      Collection<TsFileResource> tsFileResources,
      String deviceId, String measurementId, QueryContext context) {

    MeasurementSchema mSchema = schema.getMeasurementSchema(measurementId);
    TSDataType dataType = mSchema.getType();

    List<TsFileResource> tsfileResourcesForQuery = new ArrayList<>();
    long timeLowerBound = dataTTL != Long.MAX_VALUE ? System.currentTimeMillis() - dataTTL : Long
        .MIN_VALUE;
    context.setQueryTimeLowerBound(timeLowerBound);

    for (TsFileResource tsFileResource : tsFileResources) {
      // TODO: try filtering files if the query contains time filter
      if (!testResourceDevice(tsFileResource, deviceId)) {
        continue;
      }
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
    return tsfileResourcesForQuery;
  }

  /**
   * @return true if the device is contained in the TsFile and it lives beyond TTL
   */
  private boolean testResourceDevice(TsFileResource tsFileResource, String deviceId) {
    if (!tsFileResource.containsDevice(deviceId)) {
      return false;
    }
    if (dataTTL != Long.MAX_VALUE) {
      Long deviceEndTime = tsFileResource.getEndTimeMap().get(deviceId);
      return deviceEndTime == null || checkTTL(deviceEndTime);
    }
    return true;
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
        for (TsFileProcessor tsFileProcessor : workSequenceTsFileProcessor.values()) {
          tsFileProcessor.getLogNode()
              .write(new DeletePlan(timestamp, new Path(deviceId, measurementId)));
        }

        for (TsFileProcessor tsFileProcessor : workUnsequenceTsFileProcessor.values()) {
          tsFileProcessor.getLogNode()
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


  private void deleteDataInFiles(Collection<TsFileResource> tsFileResourceList, Deletion deletion,
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
  private void closeUnsealedTsFileProcessor(
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

  /**
   * count all Tsfiles in the storage group which need to be upgraded
   *
   * @return total num of the tsfiles which need to be upgraded in the storage group
   */
  public int countUpgradeFiles() {
    int cntUpgradeFileNum = 0;
    for (TsFileResource seqTsFileResource : sequenceFileList) {
      if (UpgradeUtils.isNeedUpgrade(seqTsFileResource)) {
        cntUpgradeFileNum += 1;
      }
    }
    for (TsFileResource unseqTsFileResource : unSequenceFileList) {
      if (UpgradeUtils.isNeedUpgrade(unseqTsFileResource)) {
        cntUpgradeFileNum += 1;
      }
    }
    return cntUpgradeFileNum;
  }

  public void upgrade() {
    for (TsFileResource seqTsFileResource : sequenceFileList) {
      seqTsFileResource.doUpgrade();
    }
    for (TsFileResource unseqTsFileResource : unSequenceFileList) {
      unseqTsFileResource.doUpgrade();
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
      long timeLowerBound = System.currentTimeMillis() - dataTTL;
      MergeResource mergeResource = new MergeResource(sequenceFileList, unSequenceFileList,
          timeLowerBound);

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

        for (TsFileResource tsFileResource : mergeResource.getSeqFiles()) {
          tsFileResource.setMerging(true);
        }
        for (TsFileResource tsFileResource : mergeResource.getUnseqFiles()) {
          tsFileResource.setMerging(true);
        }

        MergeTask mergeTask = new MergeTask(mergeResource, storageGroupSysDir.getPath(),
            this::mergeEndAction, taskName, fullMerge, fileSelector.getConcurrentMergeNum(),
            storageGroupName);
        mergingModification = new ModificationFile(
            storageGroupSysDir + File.separator + MERGING_MODIFICATION_FILE_NAME);
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

  private void removeUnseqFiles(List<TsFileResource> unseqFiles) {
    mergeLock.writeLock().lock();
    try {
      unSequenceFileList.removeAll(unseqFiles);
    } finally {
      mergeLock.writeLock().unlock();
    }

    for (TsFileResource unseqFile : unseqFiles) {
      unseqFile.getWriteQueryLock().writeLock().lock();
      try {
        unseqFile.remove();
      } finally {
        unseqFile.getWriteQueryLock().writeLock().unlock();
      }
    }
  }

  private void updateMergeModification(TsFileResource seqFile) {
    seqFile.getWriteQueryLock().writeLock().lock();
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
    } finally {
      seqFile.getWriteQueryLock().writeLock().unlock();
    }
  }

  private void removeMergingModification() {
    try {
      if (mergingModification != null) {
        mergingModification.remove();
        mergingModification = null;
      }
    } catch (IOException e) {
      logger.error("{} cannot remove merging modification ", storageGroupName, e);
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

    removeUnseqFiles(unseqFiles);

    for (int i = 0; i < seqFiles.size(); i++) {
      TsFileResource seqFile = seqFiles.get(i);
      mergeLock.writeLock().lock();
      try {
        updateMergeModification(seqFile);
        if (i == seqFiles.size() - 1) {
          removeMergingModification();
          isMerging = false;
          mergeLog.delete();
        }
      } finally {
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
   * @param newTsFileResource tsfile resource
   * @UsedBy sync module.
   */
  public void loadNewTsFile(TsFileResource newTsFileResource)
      throws TsFileProcessorException {
    File tsfileToBeInserted = newTsFileResource.getFile();
    writeLock();
    mergeLock.writeLock().lock();
    try {
      loadTsFileByType(LoadTsFileType.LOAD_SEQUENCE, tsfileToBeInserted, newTsFileResource,
          getBinarySearchIndex(newTsFileResource));
      updateLatestTimeMap(newTsFileResource);
    } catch (TsFileProcessorException | DiskSpaceInsufficientException e) {
      logger.error("Failed to append the tsfile {} to storage group processor {}.",
          tsfileToBeInserted.getAbsolutePath(), tsfileToBeInserted.getParentFile().getName());
      IoTDBDescriptor.getInstance().getConfig().setReadOnly(true);
      throw new TsFileProcessorException(e);
    } finally {
      mergeLock.writeLock().unlock();
      writeUnlock();
    }
  }

  /**
   * Get binary search index in @code{sequenceFileList}
   *
   * @return right index to insert
   */
  private int getBinarySearchIndex(TsFileResource tsFileResource) {
    if (sequenceFileList.isEmpty()) {
      return 0;
    }
    long targetTsFileTime = Long.parseLong(
        tsFileResource.getFile().getName().split(IoTDBConstant.TSFILE_NAME_SEPARATOR)[0]);
    int s = 0;
    int e = sequenceFileList.size() - 1;
    List<TsFileResource> l = new ArrayList<>(sequenceFileList);
    while (s <= e) {
      int m = s + ((e - s) >> 1);
      long currentTsFileTime = Long.parseLong(l.get(m).getFile().getName()
          .split(IoTDBConstant.TSFILE_NAME_SEPARATOR)[0]);
      if (currentTsFileTime >= targetTsFileTime) {
        e = m - 1;
      } else {
        s = m + 1;
      }
    }
    return s;
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
   * @param type load type
   * @param tsFileResource tsfile resource to be loaded
   * @param index the index in sequenceFileList/unSequenceFileList
   * @UsedBy sync module
   */
  private void loadTsFileByType(LoadTsFileType type, File syncedTsFile,
      TsFileResource tsFileResource, int index)
      throws TsFileProcessorException, DiskSpaceInsufficientException {
    File targetFile;
    switch (type) {
      case LOAD_UNSEQUENCE:
        targetFile =
            new File(DirectoryManager.getInstance().getNextFolderForUnSequenceFile(),
                syncedTsFile.getParentFile().getName() + File.separatorChar + syncedTsFile
                    .getName());
        tsFileResource.setFile(targetFile);
        unSequenceFileList.add(index, tsFileResource);
        logger
            .info("Load tsfile in unsequence list, move file from {} to {}",
                syncedTsFile.getAbsolutePath(),
                targetFile.getAbsolutePath());
        break;
      case LOAD_SEQUENCE:
        targetFile =
            new File(DirectoryManager.getInstance().getNextFolderForSequenceFile(),
                syncedTsFile.getParentFile().getName() + File.separatorChar + syncedTsFile
                    .getName());
        tsFileResource.setFile(targetFile);
        sequenceFileList.add(tsFileResource);
        logger
            .info("Load tsfile in sequence list, move file from {} to {}",
                syncedTsFile.getAbsolutePath(),
                targetFile.getAbsolutePath());
        break;
      default:
        throw new TsFileProcessorException(
            String.format("Unsupported type of loading tsfile : %s", type));
    }

    // move file from sync dir to data dir
    if (!targetFile.getParentFile().exists()) {
      targetFile.getParentFile().mkdirs();
    }
    if (syncedTsFile.exists() && !targetFile.exists()) {
      try {
        FileUtils.moveFile(syncedTsFile, targetFile);
      } catch (IOException e) {
        throw new TsFileProcessorException(String.format(
            "File renaming failed when loading tsfile. Origin: %s, Target: %s, because %s",
            syncedTsFile.getAbsolutePath(), targetFile.getAbsolutePath(), e.getMessage()));
      }
    }
    try {
      FileUtils.moveFile(new File(syncedTsFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX),
          new File(targetFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX));
    } catch (IOException e) {
      throw new TsFileProcessorException(String.format(
          "File renaming failed when loading .resource file. Origin: %s, Target: %s, because %s",
          new File(syncedTsFile + TsFileResource.RESOURCE_SUFFIX).getAbsolutePath(),
          new File(targetFile + TsFileResource.RESOURCE_SUFFIX).getAbsolutePath(), e.getMessage()));
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
    deletedTsFileResource.getWriteQueryLock().writeLock().lock();
    try {
      logger.info("Delete tsfile {} in sync loading process.", deletedTsFileResource.getFile());
      deletedTsFileResource.remove();
    } finally {
      deletedTsFileResource.getWriteQueryLock().writeLock().unlock();
    }
  }


  public Collection<TsFileProcessor> getWorkSequenceTsFileProcessor() {
    return workSequenceTsFileProcessor.values();
  }

  public void setDataTTL(long dataTTL) {
    this.dataTTL = dataTTL;
    checkFilesTTL();
  }

  @TestOnly
  public List<TsFileResource> getSequenceFileList() {
    return new ArrayList<>(sequenceFileList);
  }

  @TestOnly
  public List<TsFileResource> getUnSequenceFileList() {
    return unSequenceFileList;
  }

  private enum LoadTsFileType {
    LOAD_SEQUENCE, LOAD_UNSEQUENCE
  }

  @FunctionalInterface
  public interface CloseTsFileCallBack {

    void call(TsFileProcessor caller) throws TsFileProcessorException, IOException;
  }

}
