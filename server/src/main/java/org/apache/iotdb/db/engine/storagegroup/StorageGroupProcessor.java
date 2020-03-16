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

import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.flush.TsFileFlushPolicy;
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
import org.apache.iotdb.db.exception.*;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.OutOfTTLException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.mnode.LeafMNode;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.qp.physical.crud.BatchInsertPlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryFileManager;
import org.apache.iotdb.db.utils.CopyOnReadLinkedList;
import org.apache.iotdb.db.utils.UpgradeUtils;
import org.apache.iotdb.db.writelog.recover.TsFileRecoverPerformer;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.iotdb.db.engine.merge.task.MergeTask.MERGE_SUFFIX;
import static org.apache.iotdb.db.engine.storagegroup.TsFileResource.TEMP_SUFFIX;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;


/**
 * For sequence data, a StorageGroupProcessor has some TsFileProcessors, in which there is only one
 * TsFileProcessor in the working status. <br/>
 * <p>
 * There are two situations to set the working TsFileProcessor to closing status:<br/>
 * <p>
 * (1) when inserting data into the TsFileProcessor, and the TsFileProcessor shouldFlush() (or
 * shouldClose())<br/>
 * <p>
 * (2) someone calls waitForAllCurrentTsFileProcessorsClosed(). (up to now, only flush command from
 * cli will call this method)<br/>
 * <p>
 * UnSequence data has the similar process as above.
 * <p>
 * When a sequence TsFileProcessor is submitted to be flushed, the updateLatestFlushTimeCallback()
 * method will be called as a callback.<br/>
 * <p>
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
   * partitionLatestFlushedTimeForEachDevice)
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
  /**
   * time partition id in the storage group -> tsFileProcessor for this time partition
   */
  private final TreeMap<Long, TsFileProcessor> workSequenceTsFileProcessors = new TreeMap<>();
  /**
   * time partition id in the storage group -> tsFileProcessor for this time partition
   */
  private final TreeMap<Long, TsFileProcessor> workUnsequenceTsFileProcessors = new TreeMap<>();

  /**
   * the schema of time series that belong this storage group
   */
  private Schema schema;
  // includes sealed and unsealed sequence TsFiles
  private TreeSet<TsFileResource> sequenceFileTreeSet = new TreeSet<>(
      (o1, o2) -> {
        int rangeCompare = Long.compare(Long.parseLong(o1.getFile().getParentFile().getName()),
            Long.parseLong(o2.getFile().getParentFile().getName()));
        return rangeCompare == 0 ? compareFileName(o1.getFile(), o2.getFile()) : rangeCompare;
      });

  private CopyOnReadLinkedList<TsFileProcessor> closingSequenceTsFileProcessor = new CopyOnReadLinkedList<>();
  // includes sealed and unsealed unSequence TsFiles
  private List<TsFileResource> unSequenceFileList = new ArrayList<>();
  private CopyOnReadLinkedList<TsFileProcessor> closingUnSequenceTsFileProcessor = new CopyOnReadLinkedList<>();
  /*
   * time partition id -> map, which contains
   * device -> global latest timestamp of each device latestTimeForEachDevice caches non-flushed
   * changes upon timestamps of each device, and is used to update partitionLatestFlushedTimeForEachDevice
   * when a flush is issued.
   */
  private Map<Long, Map<String, Long>> latestTimeForEachDevice = new HashMap<>();
  /**
   * time partition id -> map, which contains device -> largest timestamp of the latest memtable to
   * be submitted to asyncTryToFlush partitionLatestFlushedTimeForEachDevice determines whether a data point
   * should be put into a sequential file or an unsequential file. Data of some device with
   * timestamp less than or equals to the device's latestFlushedTime should go into an unsequential
   * file.
   */
  private Map<Long, Map<String, Long>> partitionLatestFlushedTimeForEachDevice = new HashMap<>();
  /**
   * global mapping of device -> largest timestamp of the latest memtable to * be submitted to
   * asyncTryToFlush, globalLatestFlushedTimeForEachDevice is utilized to maintain global
   * latestFlushedTime of devices and will be updated along with partitionLatestFlushedTimeForEachDevice
   */
  private Map<String, Long> globalLatestFlushedTimeForEachDevice = new HashMap<>();
  private String storageGroupName;
  private File storageGroupSysDir;
  /**
   * time partition id -> version controller which assigns a version for each MemTable and
   * deletion/update such that after they are persisted, the order of insertions, deletions and
   * updates can be re-determined.
   */
  private HashMap<Long, VersionController> timePartitionIdVersionControllerMap = new HashMap<>();
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
  private TsFileFlushPolicy fileFlushPolicy;

  // allDirectFileVersions records the versions of the direct TsFiles (generated by flush), not
  // including the files generated by merge
  private Set<Long> allDirectFileVersions = new HashSet<>();

  public StorageGroupProcessor(String systemInfoDir, String storageGroupName,
      TsFileFlushPolicy fileFlushPolicy)
      throws StorageGroupProcessorException, MetadataException {
    this.storageGroupName = storageGroupName;
    this.fileFlushPolicy = fileFlushPolicy;

    // construct the file schema
    this.schema = constructSchema(storageGroupName);

    storageGroupSysDir = SystemFileFactory.INSTANCE.getFile(systemInfoDir, storageGroupName);
    if (storageGroupSysDir.mkdirs()) {
      logger.info("Storage Group system Directory {} doesn't exist, create it",
          storageGroupSysDir.getPath());
    } else if (!storageGroupSysDir.exists()) {
      logger.error("create Storage Group system Directory {} failed",
          storageGroupSysDir.getPath());
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

      for (TsFileResource resource : seqTsFiles) {
        //After recover, case the TsFile's length is equal to 0, delete both the TsFileResource and the file itself
        if (resource.getFile().length() == 0) {
          deleteTsfile(resource.getFile());
        }
        allDirectFileVersions.addAll(resource.getHistoricalVersions());
      }
      for (TsFileResource resource : unseqTsFiles) {
        //After recover, case the TsFile's length is equal to 0, delete both the TsFileResource and the file itself
        if (resource.getFile().length() == 0) {
          deleteTsfile(resource.getFile());
        }
        allDirectFileVersions.addAll(resource.getHistoricalVersions());
      }

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

    for (TsFileResource resource : sequenceFileTreeSet) {
      long timePartitionId = getTimePartitionFromTsFileResource(resource);
      if (timePartitionId != -1) {
        latestTimeForEachDevice.computeIfAbsent(timePartitionId, l -> new HashMap<>())
            .putAll(resource.getEndTimeMap());
        partitionLatestFlushedTimeForEachDevice.computeIfAbsent(timePartitionId, id -> new HashMap<>())
            .putAll(resource.getEndTimeMap());

        for (Map.Entry<String, Long> mapEntry : resource.getEndTimeMap().entrySet()) {
          if (!globalLatestFlushedTimeForEachDevice.containsKey(mapEntry.getKey())
              || globalLatestFlushedTimeForEachDevice.get(mapEntry.getKey())
                  < mapEntry.getValue()) {
            globalLatestFlushedTimeForEachDevice.put(mapEntry.getKey(), mapEntry.getValue());
          }
        }
      }
    }
  }

  private long getTimePartitionFromTsFileResource(TsFileResource resource) {
    // device id -> start map
    // if start time map is empty, tsfile resource is empty, return -1;
    Map<String, Long> startTimeMap = resource.getStartTimeMap();
    // just find any time of device
    Iterator<Long> iterator = startTimeMap.values().iterator();
    if (iterator.hasNext()) {
      return StorageEngine.fromTimeToTimePartition(iterator.next());
    }

    return -1;
  }

  /**
   * get version controller by time partition Id Thread-safety should be ensure by caller
   *
   * @param timePartitionId time partition Id
   * @return version controller
   */
  private VersionController getVersionControllerByTimePartitionId(long timePartitionId) {
    VersionController res = timePartitionIdVersionControllerMap.get(timePartitionId);
    if (res == null) {
      try {
        res = new SimpleFileVersionController(storageGroupSysDir.getPath(), timePartitionId);
        timePartitionIdVersionControllerMap.put(timePartitionId, res);
      } catch (IOException e) {
        logger.error("can't build a version controller for time partition" + timePartitionId);
      }
    }

    return res;
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
    for (int i = 0; i < tsFiles.size(); i++) {
      TsFileResource tsFileResource = tsFiles.get(i);
      sequenceFileTreeSet.add(tsFileResource);
      long timePartitionId = getTimePartitionFromTsFileResource(tsFileResource);

      TsFileRecoverPerformer recoverPerformer = new TsFileRecoverPerformer(storageGroupName + "-",
          schema, getVersionControllerByTimePartitionId(timePartitionId), tsFileResource, false,
          i == tsFiles.size() - 1);
      RestorableTsFileIOWriter writer = recoverPerformer.recover();
      if (i != tsFiles.size() - 1 || !writer.canWrite()) {
        // not the last file or cannot write, just close it
        tsFileResource.setClosed(true);
      } else if (writer.canWrite()) {
        // the last file is not closed, continue writing to in
        TsFileProcessor tsFileProcessor = new TsFileProcessor(storageGroupName, tsFileResource,
            schema, getVersionControllerByTimePartitionId(timePartitionId),
            this::closeUnsealedTsFileProcessor,
            this::updateLatestFlushTimeCallback, true, writer);
        workUnsequenceTsFileProcessors
            .put(timePartitionId, tsFileProcessor);
        tsFileResource.setProcessor(tsFileProcessor);
        tsFileProcessor.setTimeRangeId(timePartitionId);
        writer.makeMetadataVisible();
      }
    }
  }

  private void recoverUnseqFiles(List<TsFileResource> tsFiles)
      throws StorageGroupProcessorException {
    for (int i = 0; i < tsFiles.size(); i++) {
      TsFileResource tsFileResource = tsFiles.get(i);
      unSequenceFileList.add(tsFileResource);
      long timePartitionId = getTimePartitionFromTsFileResource(tsFileResource);

      TsFileRecoverPerformer recoverPerformer = new TsFileRecoverPerformer(storageGroupName + "-",
          schema, getVersionControllerByTimePartitionId(timePartitionId), tsFileResource, true,
          i == tsFiles.size() - 1);
      RestorableTsFileIOWriter writer = recoverPerformer.recover();
      if (i != tsFiles.size() - 1 || !writer.canWrite()) {
        // not the last file or cannot write, just close it
        tsFileResource.setClosed(true);
      } else if (writer.canWrite()) {
        // the last file is not closed, continue writing to in
        TsFileProcessor tsFileProcessor = new TsFileProcessor(storageGroupName, tsFileResource,
            schema, getVersionControllerByTimePartitionId(timePartitionId),
            this::closeUnsealedTsFileProcessor,
            this::unsequenceFlushCallback, false, writer);
        tsFileResource.setProcessor(tsFileProcessor);
        tsFileProcessor.setTimeRangeId(timePartitionId);
        writer.makeMetadataVisible();
      }
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

  private Schema constructSchema(String storageGroupName) throws MetadataException {
    List<MeasurementSchema> columnSchemaList =
        MManager.getInstance().getStorageGroupSchema(storageGroupName);

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
      long timePartitionId = StorageEngine.fromTimeToTimePartition(insertPlan.getTime());
      latestTimeForEachDevice.computeIfAbsent(timePartitionId, l -> new HashMap<>())
          .putIfAbsent(insertPlan.getDeviceId(), Long.MIN_VALUE);
      partitionLatestFlushedTimeForEachDevice.computeIfAbsent(timePartitionId, id -> new HashMap<>())
          .putIfAbsent(insertPlan.getDeviceId(), Long.MIN_VALUE);

      // insert to sequence or unSequence file
      insertToTsFileProcessor(insertPlan,
          insertPlan.getTime() > partitionLatestFlushedTimeForEachDevice.get(timePartitionId)
              .get(insertPlan.getDeviceId()));
    } finally {
      writeUnlock();
    }
  }

  public TSStatus[] insertBatch(BatchInsertPlan batchInsertPlan) throws WriteProcessException {
    writeLock();
    try {
      TSStatus[] results = new TSStatus[batchInsertPlan.getRowCount()];

      /*
       * assume that batch has been sorted by client
       */
      int loc = 0;
      while (loc < batchInsertPlan.getRowCount()) {
        long currTime = batchInsertPlan.getTimes()[loc];
        // skip points that do not satisfy TTL
        if (!checkTTL(currTime)) {
          results[loc] = RpcUtils.getStatus(TSStatusCode.OUT_OF_TTL_ERROR,
              "time " + currTime + " in current line is out of TTL: " + dataTTL);
          loc++;
        } else {
          break;
        }
      }
      // loc pointing at first legal position
      if (loc == batchInsertPlan.getRowCount()) {
        return results;
      }
      // before is first start point
      int before = loc;
      // before time partition
      long beforeTimePartition = StorageEngine.fromTimeToTimePartition(batchInsertPlan.getTimes()[before]);
      // init map
      long lastFlushTime = partitionLatestFlushedTimeForEachDevice.
          computeIfAbsent(beforeTimePartition, id -> new HashMap<>()).
          computeIfAbsent(batchInsertPlan.getDeviceId(), id -> Long.MIN_VALUE);
      // if is sequence
      boolean isSequence = false;
      while (loc < batchInsertPlan.getRowCount()) {
        long time = batchInsertPlan.getTimes()[loc];
        long curTimePartition = StorageEngine.fromTimeToTimePartition(time);
        results[loc] = RpcUtils.SUCCESS_STATUS;
        // start next partition
        if (curTimePartition != beforeTimePartition) {
          // insert last time partition
          insertBatchToTsFileProcessor(batchInsertPlan, before, loc, isSequence, results,
              beforeTimePartition);
          // re initialize
          before = loc;
          beforeTimePartition = curTimePartition;
          lastFlushTime = partitionLatestFlushedTimeForEachDevice.
              computeIfAbsent(beforeTimePartition, id -> new HashMap<>()).
              computeIfAbsent(batchInsertPlan.getDeviceId(), id -> Long.MIN_VALUE);
          isSequence = false;
        }
        // still in this partition
        else {
          // judge if we should insert sequence
          if (!isSequence && time > lastFlushTime) {
            // insert into unsequence and then start sequence
            insertBatchToTsFileProcessor(batchInsertPlan, before, loc, false, results,
                beforeTimePartition);
            before = loc;
            isSequence = true;
          }
          loc++;
        }
      }

      // do not forget last part
      if (before < loc) {
        insertBatchToTsFileProcessor(batchInsertPlan, before, loc, isSequence, results,
            beforeTimePartition);
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

  /**
   * insert batch to tsfile processor thread-safety that the caller need to guarantee
   *
   * @param batchInsertPlan batch insert plan
   * @param sequence whether is sequence
   * @param results result array
   * @param timePartitionId time partition id
   */
  private void insertBatchToTsFileProcessor(BatchInsertPlan batchInsertPlan,
      int start, int end, boolean sequence, TSStatus[] results, long timePartitionId)
      throws WriteProcessException {
    // return when start <= end
    if (start >= end) {
      return;
    }

    TsFileProcessor tsFileProcessor = getOrCreateTsFileProcessor(timePartitionId, sequence);
    if (tsFileProcessor == null) {
      for (int i = start; i < end; i++) {
        results[i] = RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR,
            "can not create TsFileProcessor, timePartitionId: " + timePartitionId);
      }
      return;
    }

    tsFileProcessor.insertBatch(batchInsertPlan, start, end, results);

    latestTimeForEachDevice.computeIfAbsent(timePartitionId, t -> new HashMap<>())
        .putIfAbsent(batchInsertPlan.getDeviceId(), Long.MIN_VALUE);
    // try to update the latest time of the device of this tsRecord
    if (sequence && latestTimeForEachDevice.get(timePartitionId).get(batchInsertPlan.getDeviceId())
        < batchInsertPlan.getTimes()[end - 1]) {
      latestTimeForEachDevice.get(timePartitionId)
          .put(batchInsertPlan.getDeviceId(), batchInsertPlan.getTimes()[end - 1]);
    }
    long globalLatestFlushedTime =
        globalLatestFlushedTimeForEachDevice.computeIfAbsent(
            batchInsertPlan.getDeviceId(), k -> Long.MIN_VALUE);
    tryToUpdateBatchInsertLastCache(batchInsertPlan, globalLatestFlushedTime);
    if (globalLatestFlushedTime < batchInsertPlan.getMaxTime())
      globalLatestFlushedTimeForEachDevice.put(
          batchInsertPlan.getDeviceId(), batchInsertPlan.getMaxTime());

    // check memtable size and may async try to flush the work memtable
    if (tsFileProcessor.shouldFlush()) {
      fileFlushPolicy.apply(this, tsFileProcessor, sequence);
    }
  }

  public void tryToUpdateBatchInsertLastCache(BatchInsertPlan plan, Long latestFlushedTime)
      throws WriteProcessException {
    try {
      MNode node =
          MManager.getInstance().getDeviceNodeWithAutoCreateStorageGroup(plan.getDeviceId());
      String[] measurementList = plan.getMeasurements();
      for (int i = 0; i < measurementList.length; i++) {
        // Update cached last value with high priority
        MNode measurementNode = node.getChild(measurementList[i]);
        ((LeafMNode) measurementNode)
            .updateCachedLast(plan.composeLastTimeValuePair(i), true, latestFlushedTime);
      }
    } catch (MetadataException e) {
      throw new WriteProcessException(e);
    }
  }

  private void insertToTsFileProcessor(InsertPlan insertPlan, boolean sequence)
      throws QueryProcessException {
    TsFileProcessor tsFileProcessor;
    boolean result;
    long timePartitionId = StorageEngine.fromTimeToTimePartition(insertPlan.getTime());

    tsFileProcessor = getOrCreateTsFileProcessor(timePartitionId, sequence);

    if (tsFileProcessor == null) {
      return;
    }

    // insert TsFileProcessor
    result = tsFileProcessor.insert(insertPlan);

    // try to update the latest time of the device of this tsRecord
    if (result
        && latestTimeForEachDevice.get(timePartitionId).get(insertPlan.getDeviceId()) < insertPlan
        .getTime()) {
      latestTimeForEachDevice.get(timePartitionId)
          .put(insertPlan.getDeviceId(), insertPlan.getTime());
    }
    long globalLatestFlushTime =
        globalLatestFlushedTimeForEachDevice.computeIfAbsent(
            insertPlan.getDeviceId(), k -> Long.MIN_VALUE);
    tryToUpdateInsertLastCache(insertPlan, globalLatestFlushTime);
    if (result && globalLatestFlushTime < insertPlan.getTime()) {
      globalLatestFlushedTimeForEachDevice.put(insertPlan.getDeviceId(), insertPlan.getTime());
    }

    // check memtable size and may asyncTryToFlush the work memtable
    if (tsFileProcessor.shouldFlush()) {
      fileFlushPolicy.apply(this, tsFileProcessor, sequence);
    }
  }

  public void tryToUpdateInsertLastCache(InsertPlan plan, Long latestFlushedTime)
      throws QueryProcessException {
    try {
      MNode node =
          MManager.getInstance().getDeviceNodeWithAutoCreateStorageGroup(plan.getDeviceId());
      String[] measurementList = plan.getMeasurements();
      for (int i = 0; i < measurementList.length; i++) {
        // Update cached last value with high priority
        MNode measurementNode = node.getChild(measurementList[i]);
        ((LeafMNode) measurementNode)
            .updateCachedLast(plan.composeTimeValuePair(i), true, latestFlushedTime);
      }
    } catch (MetadataException e) {
      throw new QueryProcessException(e);
    }
  }

  private TsFileProcessor getOrCreateTsFileProcessor(long timeRangeId, boolean sequence) {
    TsFileProcessor tsFileProcessor = null;
    try {
      if (sequence) {
        tsFileProcessor = getOrCreateTsFileProcessorIntern(timeRangeId,
            workSequenceTsFileProcessors, sequenceFileTreeSet, true);
      } else {
        tsFileProcessor = getOrCreateTsFileProcessorIntern(timeRangeId,
            workUnsequenceTsFileProcessors, unSequenceFileList, false);
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
   * get processor from hashmap, flush oldest processor if necessary
   *
   * @param timeRangeId time partition range
   * @param tsFileProcessorTreeMap tsFileProcessorTreeMap
   * @param fileList file list to add new processor
   * @param sequence whether is sequence or not
   */
  private TsFileProcessor getOrCreateTsFileProcessorIntern(long timeRangeId,
      TreeMap<Long, TsFileProcessor> tsFileProcessorTreeMap,
      Collection<TsFileResource> fileList,
      boolean sequence)
      throws IOException, DiskSpaceInsufficientException {

    TsFileProcessor res;
    // we have to ensure only one thread can change workSequenceTsFileProcessors
    writeLock();
    try {
      if (!tsFileProcessorTreeMap.containsKey(timeRangeId)) {
        // we have to remove oldest processor to control the num of the memtables
        // TODO: use a method to control the number of memtables
        if (tsFileProcessorTreeMap.size()
            >= IoTDBDescriptor.getInstance().getConfig().getMemtableNumInEachStorageGroup() / 2) {
          Map.Entry<Long, TsFileProcessor> processorEntry = tsFileProcessorTreeMap.firstEntry();
          logger.info(
              "will close a TsFile because too many memtables ({} > {}) in the storage group {},",
              tsFileProcessorTreeMap.size(),
              IoTDBDescriptor.getInstance().getConfig().getMemtableNumInEachStorageGroup() / 2,
              storageGroupName);
          moveOneWorkProcessorToClosingList(sequence, processorEntry.getValue());
        }

        // build new processor
        TsFileProcessor newProcessor = createTsFileProcessor(sequence, timeRangeId);
        tsFileProcessorTreeMap.put(timeRangeId, newProcessor);
        fileList.add(newProcessor.getTsFileResource());
        res = newProcessor;
      } else {
        res = tsFileProcessorTreeMap.get(timeRangeId);
      }

    } finally {
      // unlock in finally
      writeUnlock();
    }

    return res;
  }



  private TsFileProcessor createTsFileProcessor(boolean sequence, long timePartitionId)
      throws IOException, DiskSpaceInsufficientException {
    String baseDir;
    if (sequence) {
      baseDir = DirectoryManager.getInstance().getNextFolderForSequenceFile();
    } else {
      baseDir = DirectoryManager.getInstance().getNextFolderForUnSequenceFile();
    }
    fsFactory.getFile(baseDir, storageGroupName).mkdirs();

    String filePath =
        baseDir + File.separator + storageGroupName + File.separator + timePartitionId
            + File.separator
            + getNewTsFileName(timePartitionId);

    TsFileProcessor tsFileProcessor;
    VersionController versionController = getVersionControllerByTimePartitionId(timePartitionId);
    if (sequence) {
      tsFileProcessor = new TsFileProcessor(storageGroupName,
          fsFactory.getFileWithParent(filePath),
          schema, versionController, this::closeUnsealedTsFileProcessor,
          this::updateLatestFlushTimeCallback, true);
    } else {
      tsFileProcessor = new TsFileProcessor(storageGroupName,
          fsFactory.getFileWithParent(filePath),
          schema, versionController, this::closeUnsealedTsFileProcessor,
          this::unsequenceFlushCallback, false);
    }

    tsFileProcessor.setTimeRangeId(timePartitionId);
    return tsFileProcessor;
  }

  /**
   * Create a new tsfile name
   *
   * @return file name
   */
  private String getNewTsFileName(long timePartitionId) {
    return getNewTsFileName(System.currentTimeMillis(),
        getVersionControllerByTimePartitionId(timePartitionId).nextVersion(), 0);
  }

  private String getNewTsFileName(long time, long version, int mergeCnt) {
    allDirectFileVersions.add(version);
    return time + IoTDBConstant.TSFILE_NAME_SEPARATOR + version
        + IoTDBConstant.TSFILE_NAME_SEPARATOR + mergeCnt + TSFILE_SUFFIX;
  }


  /**
   * thread-safety should be ensured by caller
   */
  public void moveOneWorkProcessorToClosingList(boolean sequence,
      TsFileProcessor tsFileProcessor) {
    //for sequence tsfile, we update the endTimeMap only when the file is prepared to be closed.
    //for unsequence tsfile, we have maintained the endTimeMap when an insertion comes.
    if (sequence) {
      closingSequenceTsFileProcessor.add(tsFileProcessor);
      updateEndTimeMap(tsFileProcessor);
      tsFileProcessor.asyncClose();

      workSequenceTsFileProcessors.remove(tsFileProcessor.getTimeRangeId());
      // if unsequence files don't contain this time range id, we should remove it's version controller
      if (!workUnsequenceTsFileProcessors.containsKey(tsFileProcessor.getTimeRangeId())) {
        timePartitionIdVersionControllerMap.remove(tsFileProcessor.getTimeRangeId());
      }
      logger.info("close a sequence tsfile processor {}", storageGroupName);
    } else {
      closingUnSequenceTsFileProcessor.add(tsFileProcessor);
      tsFileProcessor.asyncClose();

      workUnsequenceTsFileProcessors.remove(tsFileProcessor.getTimeRangeId());
      // if sequence files don't contain this time range id, we should remove it's version controller
      if (!workSequenceTsFileProcessors.containsKey(tsFileProcessor.getTimeRangeId())) {
        timePartitionIdVersionControllerMap.remove(tsFileProcessor.getTimeRangeId());
      }
    }
  }

  /**
   * delete the storageGroup's own folder in folder data/system/storage_groups
   */
  public void deleteFolder(String systemDir) {
    logger.info("{} will close all files for deleting data folder {}", storageGroupName, systemDir);
    waitForAllCurrentTsFileProcessorsClosed();
    writeLock();
    try {
      File storageGroupFolder = SystemFileFactory.INSTANCE.getFile(systemDir, storageGroupName);
      if (storageGroupFolder.exists()) {
        org.apache.iotdb.db.utils.FileUtils.deleteDirectory(storageGroupFolder);
      }
    } catch (IOException e) {
      logger.error("Cannot delete the folder in storage group {}, because", storageGroupName, e);
    } finally {
      writeUnlock();
    }
  }

  public void closeAllResources() {
    for (TsFileResource tsFileResource : unSequenceFileList) {
      try {
        tsFileResource.close();
      } catch (IOException e) {
        logger.error("Cannot close a TsFileResource {}", tsFileResource, e);
      }
    }
    for (TsFileResource tsFileResource : sequenceFileTreeSet) {
      try {
        tsFileResource.close();
      } catch (IOException e) {
        logger.error("Cannot close a TsFileResource {}", tsFileResource, e);
      }
    }
  }

  public void syncDeleteDataFiles() {
    logger.info("{} will close all files for deleting data files", storageGroupName);
    waitForAllCurrentTsFileProcessorsClosed();
    //normally, mergingModification is just need to be closed by after a merge task is finished.
    //we close it here just for IT test.
    if (this.mergingModification != null) {
      try {
        mergingModification.close();
      } catch (IOException e) {
        logger.error("Cannot close the mergingMod file {}", mergingModification.getFilePath(), e);
      }

    }
    writeLock();
    try {
      closeAllResources();
      List<String> folder = DirectoryManager.getInstance().getAllSequenceFileFolders();
      folder.addAll(DirectoryManager.getInstance().getAllUnSequenceFileFolders());
      deleteAllSGFolders(folder);

      this.workSequenceTsFileProcessors.clear();
      this.workUnsequenceTsFileProcessors.clear();
      this.sequenceFileTreeSet.clear();
      this.unSequenceFileList.clear();
      this.partitionLatestFlushedTimeForEachDevice.clear();
      this.globalLatestFlushedTimeForEachDevice.clear();
      this.latestTimeForEachDevice.clear();
    } finally {
      writeUnlock();
    }
  }

  private void deleteAllSGFolders(List<String> folder) {
    for (String tsfilePath : folder) {
      File storageGroupFolder = fsFactory.getFile(tsfilePath, storageGroupName);
      if (storageGroupFolder.exists()) {
        try {
          org.apache.iotdb.db.utils.FileUtils.deleteDirectory(storageGroupFolder);
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
    List<TsFileResource> seqFiles = new ArrayList<>(sequenceFileTreeSet);
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
            sequenceFileTreeSet.remove(resource);
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
        long startTime = System.currentTimeMillis();
        while (!closingSequenceTsFileProcessor.isEmpty() || !closingUnSequenceTsFileProcessor
            .isEmpty()) {
          closeStorageGroupCondition.wait(60_000);
          if (System.currentTimeMillis() - startTime > 60_000) {
            logger.warn("{} has spent {}s to wait for closing all TsFiles.", this.storageGroupName,
                (System.currentTimeMillis() - startTime)/1000);
          }
        }
      } catch (InterruptedException e) {
        logger.error("CloseFileNodeCondition error occurs while waiting for closing the storage "
            + "group {}", storageGroupName, e);
      }
    }
  }

  public void putAllWorkingTsFileProcessorIntoClosingList() {
    writeLock();
    try {
      logger.info("async force close all files in storage group: {}", storageGroupName);
      // to avoid concurrent modification problem, we need a new array list
      for (TsFileProcessor tsFileProcessor : new ArrayList<>(
          workSequenceTsFileProcessors.values())) {
        moveOneWorkProcessorToClosingList(true, tsFileProcessor);
      }
      // to avoid concurrent modification problem, we need a new array list
      for (TsFileProcessor tsFileProcessor : new ArrayList<>(
          workUnsequenceTsFileProcessors.values())) {
        moveOneWorkProcessorToClosingList(false, tsFileProcessor);
      }
    } finally {
      writeUnlock();
    }
  }

  // TODO need a read lock, please consider the concurrency with flush manager threads.
  public QueryDataSource query(String deviceId, String measurementId, QueryContext context,
      QueryFileManager filePathsManager, Filter timeFilter) {
    insertLock.readLock().lock();
    mergeLock.readLock().lock();
    synchronized (lruForSensorUsedInQuery) {
      if (lruForSensorUsedInQuery.size() >= MAX_CACHE_SENSORS) {
        lruForSensorUsedInQuery.removeFirst();
      }
      lruForSensorUsedInQuery.add(measurementId);
    }
    try {
      List<TsFileResource> seqResources = getFileResourceListForQuery(sequenceFileTreeSet,
          deviceId, measurementId, context, timeFilter);
      List<TsFileResource> unseqResources = getFileResourceListForQuery(unSequenceFileList,
          deviceId, measurementId, context, timeFilter);
      QueryDataSource dataSource = new QueryDataSource(new Path(deviceId, measurementId),
          seqResources, unseqResources);
      // used files should be added before mergeLock is unlocked, or they may be deleted by
      // running merge
      // is null only in tests
      if (filePathsManager != null) {
        filePathsManager.addUsedFilesForQuery(context.getQueryId(), dataSource);
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

  public void writeLock() {
    insertLock.writeLock().lock();
  }

  public void writeUnlock() {
    insertLock.writeLock().unlock();
  }


  /**
   * @param tsFileResources includes sealed and unsealed tsfile resources
   * @return fill unsealed tsfile resources with memory data and ChunkMetadataList of data in disk
   */
  private List<TsFileResource> getFileResourceListForQuery(
      Collection<TsFileResource> tsFileResources,
      String deviceId, String measurementId, QueryContext context, Filter timeFilter) {

    MeasurementSchema mSchema = schema.getMeasurementSchema(measurementId);

    List<TsFileResource> tsfileResourcesForQuery = new ArrayList<>();
    long timeLowerBound = dataTTL != Long.MAX_VALUE ? System.currentTimeMillis() - dataTTL : Long
        .MIN_VALUE;
    context.setQueryTimeLowerBound(timeLowerBound);

    for (TsFileResource tsFileResource : tsFileResources) {
      if (!isTsFileResourceSatisfied(tsFileResource, deviceId, timeFilter)) {
        continue;
      }
      closeQueryLock.readLock().lock();

      try {
        if (tsFileResource.isClosed()) {
          tsfileResourcesForQuery.add(tsFileResource);
        } else {
          // left: in-memory data, right: meta of disk data
          Pair<List<ReadOnlyMemChunk>, List<ChunkMetaData>> pair = tsFileResource
              .getUnsealedFileProcessor()
              .query(deviceId, measurementId, mSchema.getType(), mSchema.getEncodingType(),
                  mSchema.getProps(), context);

          tsfileResourcesForQuery.add(new TsFileResource(tsFileResource.getFile(),
              tsFileResource.getStartTimeMap(), tsFileResource.getEndTimeMap(), pair.left,
              pair.right));
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
  private boolean isTsFileResourceSatisfied(TsFileResource tsFileResource, String deviceId,
      Filter timeFilter) {
    if (!tsFileResource.containsDevice(deviceId)) {
      return false;
    }
    if (dataTTL != Long.MAX_VALUE) {
      Long deviceEndTime = tsFileResource.getEndTimeMap().get(deviceId);
      return deviceEndTime == null || checkTTL(deviceEndTime);
    }

    if (timeFilter != null) {
      long startTime = tsFileResource.getStartTimeMap().get(deviceId);
      long endTime = tsFileResource.getEndTimeMap().getOrDefault(deviceId, Long.MAX_VALUE);
      return timeFilter.satisfyStartEndTime(startTime, endTime);
    }
    return true;
  }


  /**
   * Delete data whose timestamp <= 'timestamp' and belongs to the time series
   * deviceId.measurementId.
   *
   * @param deviceId the deviceId of the timeseries to be deleted.
   * @param measurementId the measurementId of the timeseries to be deleted.
   * @param timestamp the delete range is (0, timestamp].
   */
  public void delete(String deviceId, String measurementId, long timestamp) throws IOException {
    // TODO: how to avoid partial deletion?
    //FIXME: notice that if we may remove a SGProcessor out of memory, we need to close all opened
    //mod files in mergingModification, sequenceFileList, and unsequenceFileList
    writeLock();
    mergeLock.writeLock().lock();

    // record files which are updated so that we can roll back them in case of exception
    List<ModificationFile> updatedModFiles = new ArrayList<>();

    try {
      Long lastUpdateTime = null;
      for (Map<String, Long> latestTimeMap : latestTimeForEachDevice.values()) {
        Long curTime = latestTimeMap.get(deviceId);
        if (curTime != null && (lastUpdateTime == null || lastUpdateTime < curTime)) {
          lastUpdateTime = curTime;
        }
      }

      // There is no tsfile data, the delete operation is invalid
      if (lastUpdateTime == null) {
        logger.debug("No device {} in SG {}, deletion invalid", deviceId, storageGroupName);
        return;
      }

      // time partition to divide storage group
      long timePartitionId = StorageEngine.fromTimeToTimePartition(timestamp);
      // write log
      if (IoTDBDescriptor.getInstance().getConfig().isEnableWal()) {
        DeletePlan deletionPlan = new DeletePlan(timestamp, new Path(deviceId, measurementId));
        for (Map.Entry<Long, TsFileProcessor> entry : workSequenceTsFileProcessors.entrySet()) {
          if (entry.getKey() <= timePartitionId) {
            entry.getValue().getLogNode()
                .write(deletionPlan);
          }
        }

        for (Map.Entry<Long, TsFileProcessor> entry : workUnsequenceTsFileProcessors.entrySet()) {
          if (entry.getKey() <= timePartitionId) {
            entry.getValue().getLogNode()
                .write(deletionPlan);
          }
        }
      }

      Path fullPath = new Path(deviceId, measurementId);
      Deletion deletion = new Deletion(fullPath,
          getVersionControllerByTimePartitionId(timePartitionId).nextVersion(), timestamp);
      if (mergingModification != null) {
        mergingModification.write(deletion);
        updatedModFiles.add(mergingModification);
      }

      deleteDataInFiles(sequenceFileTreeSet, deletion, updatedModFiles);
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
      // remember to close mod file
      tsFileResource.getModFile().close();

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
      resource.forceUpdateEndTime(deviceId,
          latestTimeForEachDevice.get(tsFileProcessor.getTimeRangeId()).get(deviceId));
    }
  }

  private boolean unsequenceFlushCallback(TsFileProcessor processor) {
    return true;
  }

  private boolean updateLatestFlushTimeCallback(TsFileProcessor processor) {
    // update the largest timestamp in the last flushing memtable
    Map<String, Long> curPartitionDeviceLatestTime = latestTimeForEachDevice
        .get(processor.getTimeRangeId());

    if (curPartitionDeviceLatestTime == null) {
      logger.error("Partition: " + processor.getTimeRangeId() +
          " does't have latest time for each device record. Flushing tsfile is: "
          + processor.getTsFileResource().getFile());
      return false;
    }

    for (Entry<String, Long> entry : curPartitionDeviceLatestTime.entrySet()) {
      partitionLatestFlushedTimeForEachDevice
          .computeIfAbsent(processor.getTimeRangeId(), id -> new HashMap<>())
          .put(entry.getKey(), entry.getValue());
      if (!globalLatestFlushedTimeForEachDevice.containsKey(entry.getKey())
          || globalLatestFlushedTimeForEachDevice.get(entry.getKey()) < entry.getValue()) {
        globalLatestFlushedTimeForEachDevice.put(entry.getKey(), entry.getValue());
      }
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
    for (TsFileResource seqTsFileResource : sequenceFileTreeSet) {
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
    for (TsFileResource seqTsFileResource : sequenceFileTreeSet) {
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
      logger.info("{} will close all files for starting a merge (fullmerge = {})", storageGroupName,
          fullMerge);
      waitForAllCurrentTsFileProcessorsClosed();
      if (unSequenceFileList.isEmpty() || sequenceFileTreeSet.isEmpty()) {
        logger.info("{} no files to be merged", storageGroupName);
        return;
      }

      long budget = IoTDBDescriptor.getInstance().getConfig().getMergeMemoryBudget();
      long timeLowerBound = System.currentTimeMillis() - dataTTL;
      MergeResource mergeResource = new MergeResource(sequenceFileTreeSet, unSequenceFileList,
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

  @SuppressWarnings("squid:S1141")
  private void updateMergeModification(TsFileResource seqFile) {
    seqFile.getWriteQueryLock().writeLock().lock();
    try {
      // remove old modifications and write modifications generated during merge
      seqFile.removeModFile();
      if (mergingModification != null) {
        for (Modification modification : mergingModification.getModifications()) {
          seqFile.getModFile().write(modification);
        }
        try {
          seqFile.getModFile().close();
        } catch (IOException e) {
          logger
              .error("Cannot close the ModificationFile {}", seqFile.getModFile().getFilePath(), e);
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
          //FIXME if there is an exception, the the modification file will be not closed.
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
   * Load a new tsfile to storage group processor. Tne file may have overlap with other files.
   * <p>
   * or unsequence list.
   * <p>
   * Secondly, execute the loading process by the type.
   * <p>
   * Finally, update the latestTimeForEachDevice and partitionLatestFlushedTimeForEachDevice.
   * @param newTsFileResource tsfile resource
   * @UsedBy sync module.
   */
  public void loadNewTsFileForSync(TsFileResource newTsFileResource)
      throws TsFileProcessorException {
    File tsfileToBeInserted = newTsFileResource.getFile();
    writeLock();
    mergeLock.writeLock().lock();
    try {
      loadTsFileByType(LoadTsFileType.LOAD_SEQUENCE, tsfileToBeInserted, newTsFileResource);
      updateLatestTimeMap(newTsFileResource);
    } catch (DiskSpaceInsufficientException e) {
      logger.error(
          "Failed to append the tsfile {} to storage group processor {} because the disk space is insufficient.",
          tsfileToBeInserted.getAbsolutePath(), tsfileToBeInserted.getParentFile().getName());
      IoTDBDescriptor.getInstance().getConfig().setReadOnly(true);
      throw new TsFileProcessorException(e);
    } finally {
      mergeLock.writeLock().unlock();
      writeUnlock();
    }
  }

  /**
   * Load a new tsfile to storage group processor. Tne file may have overlap with other files. <p>
   * that there has no file which is overlapping with the new file.
   * <p>
   * Firstly, determine the loading type of the file, whether it needs to be loaded in sequence list
   * or unsequence list.
   * <p>
   * Secondly, execute the loading process by the type.
   * <p>
   * Finally, update the latestTimeForEachDevice and partitionLatestFlushedTimeForEachDevice.
   *
   * @param newTsFileResource tsfile resource
   * @UsedBy load external tsfile module
   */
  public void loadNewTsFile(TsFileResource newTsFileResource)
      throws TsFileProcessorException {
    File tsfileToBeInserted = newTsFileResource.getFile();
    writeLock();
    mergeLock.writeLock().lock();
    try {
      boolean isOverlap = false;
      int preIndex = -1, subsequentIndex = sequenceFileTreeSet.size();

      List<TsFileResource> sequenceList = new ArrayList<>(sequenceFileTreeSet);
      // check new tsfile
      outer:
      for (int i = 0; i < sequenceList.size(); i++) {
        if (sequenceList.get(i).getFile().getName().equals(tsfileToBeInserted.getName())) {
          return;
        }
        if (i == sequenceList.size() - 1 && sequenceList.get(i).getEndTimeMap().isEmpty()) {
          continue;
        }
        boolean hasPre = false, hasSubsequence = false;
        for (String device : newTsFileResource.getStartTimeMap().keySet()) {
          if (sequenceList.get(i).getStartTimeMap().containsKey(device)) {
            long startTime1 = sequenceList.get(i).getStartTimeMap().get(device);
            long endTime1 = sequenceList.get(i).getEndTimeMap().get(device);
            long startTime2 = newTsFileResource.getStartTimeMap().get(device);
            long endTime2 = newTsFileResource.getEndTimeMap().get(device);
            if (startTime1 > endTime2) {
              hasSubsequence = true;
            } else if (startTime2 > endTime1) {
              hasPre = true;
            } else {
              isOverlap = true;
              break outer;
            }
          }
        }
        if (hasPre && hasSubsequence) {
          isOverlap = true;
          break;
        }
        if (!hasPre && hasSubsequence) {
          subsequentIndex = i;
          break;
        }
        if (hasPre) {
          preIndex = i;
        }
      }

      // loading tsfile by type
      if (isOverlap) {
        loadTsFileByType(LoadTsFileType.LOAD_UNSEQUENCE, tsfileToBeInserted, newTsFileResource);
      } else {

        // check whether the file name needs to be renamed.
        if (subsequentIndex != sequenceFileTreeSet.size() || preIndex != -1) {
          String newFileName = getFileNameForLoadingFile(tsfileToBeInserted.getName(), preIndex,
              subsequentIndex, getTimePartitionFromTsFileResource(newTsFileResource));
          if (!newFileName.equals(tsfileToBeInserted.getName())) {
            logger.info("Tsfile {} must be renamed to {} for loading into the sequence list.",
                tsfileToBeInserted.getName(), newFileName);
            newTsFileResource.setFile(new File(tsfileToBeInserted.getParentFile(), newFileName));
          }
        }
        loadTsFileByType(LoadTsFileType.LOAD_SEQUENCE, tsfileToBeInserted, newTsFileResource);
      }

      // update latest time map
      updateLatestTimeMap(newTsFileResource);
      allDirectFileVersions.addAll(newTsFileResource.getHistoricalVersions());
    } catch (DiskSpaceInsufficientException e) {
      logger.error(
          "Failed to append the tsfile {} to storage group processor {} because the disk space is insufficient.",
          tsfileToBeInserted.getAbsolutePath(), tsfileToBeInserted.getParentFile().getName());
      IoTDBDescriptor.getInstance().getConfig().setReadOnly(true);
      throw new TsFileProcessorException(e);
    } finally {
      mergeLock.writeLock().unlock();
      writeUnlock();
    }
  }

  /**
   * If the historical versions of a file is a sub-set of the given file's, remove it to reduce
   * unnecessary merge. Only used when the file sender and the receiver share the same file
   * close policy.
   * @param resource
   */
  public void removeFullyOverlapFiles(TsFileResource resource) {
    writeLock();
    closeQueryLock.writeLock().lock();
    try {
      Iterator<TsFileResource> iterator = sequenceFileTreeSet.iterator();
      removeFullyOverlapFiles(resource, iterator);

      iterator = unSequenceFileList.iterator();
      removeFullyOverlapFiles(resource, iterator);
    } finally {
      closeQueryLock.writeLock().unlock();
      writeUnlock();
    }
  }

  private void removeFullyOverlapFiles(TsFileResource resource, Iterator<TsFileResource> iterator) {
    while (iterator.hasNext()) {
      TsFileResource seqFile = iterator.next();
      if (resource.getHistoricalVersions().containsAll(seqFile.getHistoricalVersions())
          && !resource.getHistoricalVersions().equals(seqFile.getHistoricalVersions())
          && seqFile.getWriteQueryLock().writeLock().tryLock()) {
        try {
          iterator.remove();
          seqFile.remove();
        } catch (Exception e) {
          logger.error("Something gets wrong while removing FullyOverlapFiles ", e);
          throw e;
        } finally {
          seqFile.getWriteQueryLock().writeLock().unlock();
        }
      }
    }
  }

  /**
   * Get an appropriate filename to ensure the order between files. The tsfile is named after
   * ({systemTime}-{versionNum}-{mergeNum}.tsfile).
   * <p>
   * The sorting rules for tsfile names @see {@link this#compareFileName}, we can restore the list
   * based on the file name and ensure the correctness of the order, so there are three cases.
   * <p>
   * 1. The tsfile is to be inserted in the first place of the list. If the timestamp in the file
   * name is less than the timestamp in the file name of the first tsfile  in the list, then the
   * file name is legal and the file name is returned directly. Otherwise, its timestamp can be set
   * to half of the timestamp value in the file name of the first tsfile in the list , and the
   * version number is the version number in the file name of the first tsfile in the list.
   * <p>
   * 2. The tsfile is to be inserted in the last place of the list. If the timestamp in the file
   * name is lager than the timestamp in the file name of the last tsfile  in the list, then the
   * file name is legal and the file name is returned directly. Otherwise, the file name is
   * generated by the system according to the naming rules and returned.
   * <p>
   * 3. This file is inserted between two files. If the timestamp in the name of the file satisfies
   * the timestamp between the timestamps in the name of the two files, then it is a legal name and
   * returns directly; otherwise, the time stamp is the mean of the timestamps of the two files, the
   * version number is the version number in the tsfile with a larger timestamp.
   *
   * @param tsfileName origin tsfile name
   * @return appropriate filename
   */
  private String getFileNameForLoadingFile(String tsfileName, int preIndex, int subsequentIndex,
      long timePartitionId) {
    long currentTsFileTime = Long
        .parseLong(tsfileName.split(IoTDBConstant.TSFILE_NAME_SEPARATOR)[0]);
    long preTime;
    List<TsFileResource> sequenceList = new ArrayList<>(sequenceFileTreeSet);
    if (preIndex == -1) {
      preTime = 0L;
    } else {
      String preName = sequenceList.get(preIndex).getFile().getName();
      preTime = Long.parseLong(preName.split(IoTDBConstant.TSFILE_NAME_SEPARATOR)[0]);
    }
    if (subsequentIndex == sequenceFileTreeSet.size()) {
      return preTime < currentTsFileTime ? tsfileName : getNewTsFileName(timePartitionId);
    } else {
      String subsequenceName = sequenceList.get(subsequentIndex).getFile().getName();
      long subsequenceTime = Long
          .parseLong(subsequenceName.split(IoTDBConstant.TSFILE_NAME_SEPARATOR)[0]);
      long subsequenceVersion = Long
          .parseLong(subsequenceName.split(IoTDBConstant.TSFILE_NAME_SEPARATOR)[1]);
      if (preTime < currentTsFileTime && currentTsFileTime < subsequenceTime) {
        return tsfileName;
      } else {
        return getNewTsFileName(preTime + ((subsequenceTime - preTime) >> 1), subsequenceVersion,
            0);
      }
    }
  }

  /**
   * Update latest time in latestTimeForEachDevice and partitionLatestFlushedTimeForEachDevice.
   *
   * @UsedBy sync module, load external tsfile module.
   */
  private void updateLatestTimeMap(TsFileResource newTsFileResource) {
    for (Entry<String, Long> entry : newTsFileResource.getEndTimeMap().entrySet()) {
      String device = entry.getKey();
      long endTime = newTsFileResource.getEndTimeMap().get(device);
      long timePartitionId = StorageEngine.fromTimeToTimePartition(endTime);
      if (!latestTimeForEachDevice.computeIfAbsent(timePartitionId, id -> new HashMap<>())
          .containsKey(device)
          || latestTimeForEachDevice.get(timePartitionId).get(device) < endTime) {
        latestTimeForEachDevice.get(timePartitionId).put(device, endTime);
      }

      Map<String, Long> latestFlushTimeForPartition = partitionLatestFlushedTimeForEachDevice
          .getOrDefault(timePartitionId, new HashMap<>());

      if (!latestFlushTimeForPartition.containsKey(device)
          || latestFlushTimeForPartition.get(device) < endTime) {
        partitionLatestFlushedTimeForEachDevice
            .computeIfAbsent(timePartitionId, id -> new HashMap<>())
            .put(device, endTime);
      }
      if (!globalLatestFlushedTimeForEachDevice.containsKey(device)
          || globalLatestFlushedTimeForEachDevice.get(device) < endTime) {
        globalLatestFlushedTimeForEachDevice.put(device, endTime);
      }
    }
  }

  /**
   * Execute the loading process by the type.
   *
   * @param type load type
   * @param tsFileResource tsfile resource to be loaded
   * @UsedBy sync module, load external tsfile module.
   */
  private void loadTsFileByType(LoadTsFileType type, File syncedTsFile,
      TsFileResource tsFileResource)
      throws TsFileProcessorException, DiskSpaceInsufficientException {
    File targetFile;
    long timeRangeId = StorageEngine.fromTimeToTimePartition(
        tsFileResource.getStartTimeMap().entrySet().iterator().next().getValue());
    switch (type) {
      case LOAD_UNSEQUENCE:
        targetFile = new File(DirectoryManager.getInstance().getNextFolderForUnSequenceFile(),
            storageGroupName + File.separatorChar + timeRangeId + File.separator + tsFileResource
                .getFile().getName());
        tsFileResource.setFile(targetFile);
        unSequenceFileList.add(tsFileResource);
        logger.info("Load tsfile in unsequence list, move file from {} to {}",
            syncedTsFile.getAbsolutePath(), targetFile.getAbsolutePath());
        break;
      case LOAD_SEQUENCE:
        targetFile =
            new File(DirectoryManager.getInstance().getNextFolderForSequenceFile(),
                storageGroupName + File.separatorChar + timeRangeId + File.separator
                    + tsFileResource.getFile().getName());
        tsFileResource.setFile(targetFile);
        sequenceFileTreeSet.add(tsFileResource);
        logger.info("Load tsfile in sequence list, move file from {} to {}",
            syncedTsFile.getAbsolutePath(), targetFile.getAbsolutePath());
        break;
      default:
        throw new TsFileProcessorException(
            String.format("Unsupported type of loading tsfile : %s", type));
    }

    // move file from sync dir to data dir
    if (!targetFile.getParentFile().exists()) {
      targetFile.getParentFile().mkdirs();
    }
    try {
      FileUtils.moveFile(syncedTsFile, targetFile);
    } catch (IOException e) {
      logger.error("File renaming failed when loading tsfile. Origin: {}, Target: {}",
          syncedTsFile.getAbsolutePath(), targetFile.getAbsolutePath(), e);
      throw new TsFileProcessorException(String.format(
          "File renaming failed when loading tsfile. Origin: %s, Target: %s, because %s",
          syncedTsFile.getAbsolutePath(), targetFile.getAbsolutePath(), e.getMessage()));
    }

    File syncedResourceFile = new File(
        syncedTsFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX);
    File targetResourceFile = new File(
        targetFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX);
    try {
      FileUtils.moveFile(syncedResourceFile, targetResourceFile);
    } catch (IOException e) {
      logger.error("File renaming failed when loading .resource file. Origin: {}, Target: {}",
          syncedResourceFile.getAbsolutePath(), targetResourceFile.getAbsolutePath(), e);
      throw new TsFileProcessorException(String.format(
          "File renaming failed when loading .resource file. Origin: %s, Target: %s, because %s",
          syncedResourceFile.getAbsolutePath(), targetResourceFile.getAbsolutePath(),
          e.getMessage()));
    }
  }

  /**
   * Delete tsfile if it exists.
   * <p>
   * Firstly, remove the TsFileResource from sequenceFileList/unSequenceFileList.
   * <p>
   * Secondly, delete the tsfile and .resource file.
   *
   * @param tsfieToBeDeleted tsfile to be deleted
   * @return whether the file to be deleted exists.
   * @UsedBy sync module, load external tsfile module.
   */
  public boolean deleteTsfile(File tsfieToBeDeleted) {
    writeLock();
    mergeLock.writeLock().lock();
    TsFileResource tsFileResourceToBeDeleted = null;
    try {
      Iterator<TsFileResource> sequenceIterator = sequenceFileTreeSet.iterator();
      while (sequenceIterator.hasNext()) {
        TsFileResource sequenceResource = sequenceIterator.next();
        if (sequenceResource.getFile().getName().equals(tsfieToBeDeleted.getName())) {
          tsFileResourceToBeDeleted = sequenceResource;
          sequenceIterator.remove();
          break;
        }
      }
      if (tsFileResourceToBeDeleted == null) {
        Iterator<TsFileResource> unsequenceIterator = unSequenceFileList.iterator();
        while (unsequenceIterator.hasNext()) {
          TsFileResource unsequenceResource = unsequenceIterator.next();
          if (unsequenceResource.getFile().getName().equals(tsfieToBeDeleted.getName())) {
            tsFileResourceToBeDeleted = unsequenceResource;
            unsequenceIterator.remove();
            break;
          }
        }
      }
    } finally {
      mergeLock.writeLock().unlock();
      writeUnlock();
    }
    if (tsFileResourceToBeDeleted == null) {
      return false;
    }
    tsFileResourceToBeDeleted.getWriteQueryLock().writeLock().lock();
    try {
      tsFileResourceToBeDeleted.remove();
      logger.info("Delete tsfile {} successfully.", tsFileResourceToBeDeleted.getFile());
    } finally {
      tsFileResourceToBeDeleted.getWriteQueryLock().writeLock().unlock();
    }
    return true;
  }


  public Collection<TsFileProcessor> getWorkSequenceTsFileProcessors() {
    return workSequenceTsFileProcessors.values();
  }

  /**
   * Move tsfile to the target directory if it exists.
   * <p>
   * Firstly, remove the TsFileResource from sequenceFileList/unSequenceFileList.
   * <p>
   * Secondly, move the tsfile and .resource file to the target directory.
   *
   * @param fileToBeMoved tsfile to be moved
   * @return whether the file to be moved exists.
   * @UsedBy load external tsfile module.
   */
  public boolean moveTsfile(File fileToBeMoved, File targetDir) throws IOException {
    writeLock();
    mergeLock.writeLock().lock();
    TsFileResource tsFileResourceToBeMoved = null;
    try {
      Iterator<TsFileResource> sequenceIterator = sequenceFileTreeSet.iterator();
      while (sequenceIterator.hasNext()) {
        TsFileResource sequenceResource = sequenceIterator.next();
        if (sequenceResource.getFile().getName().equals(fileToBeMoved.getName())) {
          tsFileResourceToBeMoved = sequenceResource;
          sequenceIterator.remove();
          break;
        }
      }
      if (tsFileResourceToBeMoved == null) {
        Iterator<TsFileResource> unsequenceIterator = unSequenceFileList.iterator();
        while (unsequenceIterator.hasNext()) {
          TsFileResource unsequenceResource = unsequenceIterator.next();
          if (unsequenceResource.getFile().getName().equals(fileToBeMoved.getName())) {
            tsFileResourceToBeMoved = unsequenceResource;
            unsequenceIterator.remove();
            break;
          }
        }
      }
    } finally {
      mergeLock.writeLock().unlock();
      writeUnlock();
    }
    if (tsFileResourceToBeMoved == null) {
      return false;
    }
    tsFileResourceToBeMoved.getWriteQueryLock().writeLock().lock();
    try {
      tsFileResourceToBeMoved.moveTo(targetDir);
      logger
          .info("Move tsfile {} to target dir {} successfully.", tsFileResourceToBeMoved.getFile(),
              targetDir.getPath());
    } finally {
      tsFileResourceToBeMoved.getWriteQueryLock().writeLock().unlock();
    }
    return true;
  }


  public Collection<TsFileProcessor> getWorkUnsequenceTsFileProcessor() {
    return workUnsequenceTsFileProcessors.values();
  }

  public void setDataTTL(long dataTTL) {
    this.dataTTL = dataTTL;
    checkFilesTTL();
  }

  public List<TsFileResource> getSequenceFileTreeSet() {
    return new ArrayList<>(sequenceFileTreeSet);
  }

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

  public String getStorageGroupName() {
    return storageGroupName;
  }

  public boolean isFileAlreadyExist(TsFileResource tsFileResource) {
    return allDirectFileVersions.containsAll(tsFileResource.getHistoricalVersions());
  }

  @FunctionalInterface
  public interface UpdateEndTimeCallBack {

    boolean call(TsFileProcessor caller);
  }
}
