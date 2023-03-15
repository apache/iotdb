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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.TsFileMetricManager;
import org.apache.iotdb.db.engine.cache.BloomFilterCache;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.compaction.execute.recover.CompactionRecoverManager;
import org.apache.iotdb.db.engine.compaction.execute.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.compaction.schedule.CompactionScheduler;
import org.apache.iotdb.db.engine.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.engine.flush.CloseFileListener;
import org.apache.iotdb.db.engine.flush.FlushListener;
import org.apache.iotdb.db.engine.flush.FlushStatus;
import org.apache.iotdb.db.engine.flush.TsFileFlushPolicy;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.upgrade.UpgradeCheckStatus;
import org.apache.iotdb.db.engine.upgrade.UpgradeLog;
import org.apache.iotdb.db.engine.version.SimpleFileVersionController;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.exception.BatchProcessException;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.exception.LoadFileException;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.WriteProcessRejectException;
import org.apache.iotdb.db.exception.query.OutOfTTLException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.cache.DataNodeSchemaCache;
import org.apache.iotdb.db.metadata.idtable.IDTable;
import org.apache.iotdb.db.metadata.idtable.IDTableManager;
import org.apache.iotdb.db.mpp.metric.PerformanceOverviewMetricsManager;
import org.apache.iotdb.db.mpp.metric.QueryMetricsManager;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowsOfOneDeviceNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.rescon.TsFileResourceManager;
import org.apache.iotdb.db.service.SettleService;
import org.apache.iotdb.db.sync.SyncService;
import org.apache.iotdb.db.sync.sender.manager.ISyncManager;
import org.apache.iotdb.db.tools.settle.TsFileAndModSettleTool;
import org.apache.iotdb.db.utils.CopyOnReadLinkedList;
import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.db.utils.UpgradeUtils;
import org.apache.iotdb.db.wal.WALManager;
import org.apache.iotdb.db.wal.node.IWALNode;
import org.apache.iotdb.db.wal.recover.WALRecoverManager;
import org.apache.iotdb.db.wal.recover.file.SealedTsFileRecoverPerformer;
import org.apache.iotdb.db.wal.recover.file.UnsealedTsFileRecoverPerformer;
import org.apache.iotdb.db.wal.utils.WALMode;
import org.apache.iotdb.db.wal.utils.listener.WALFlushListener;
import org.apache.iotdb.db.wal.utils.listener.WALRecoverListener;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.TreeMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.iotdb.commons.conf.IoTDBConstant.FILE_NAME_SEPARATOR;
import static org.apache.iotdb.db.engine.storagegroup.TsFileResource.TEMP_SUFFIX;
import static org.apache.iotdb.db.mpp.metric.QueryResourceMetricSet.SEQUENCE_TSFILE;
import static org.apache.iotdb.db.mpp.metric.QueryResourceMetricSet.UNSEQUENCE_TSFILE;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

/**
 * For sequence data, a DataRegion has some TsFileProcessors, in which there is only one
 * TsFileProcessor in the working status. <br>
 *
 * <p>There are two situations to set the working TsFileProcessor to closing status:<br>
 *
 * <p>(1) when inserting data into the TsFileProcessor, and the TsFileProcessor shouldFlush() (or
 * shouldClose())<br>
 *
 * <p>(2) someone calls syncCloseAllWorkingTsFileProcessors(). (up to now, only flush command from
 * cli will call this method)<br>
 *
 * <p>UnSequence data has the similar process as above.
 *
 * <p>When a sequence TsFileProcessor is submitted to be flushed, the
 * updateLatestFlushTimeCallback() method will be called as a callback.<br>
 *
 * <p>When a TsFileProcessor is closed, the closeUnsealedTsFileProcessorCallBack() method will be
 * called as a callback.
 */
public class DataRegion implements IDataRegionForQuery {

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final Logger DEBUG_LOGGER = LoggerFactory.getLogger("QUERY_DEBUG");

  /**
   * All newly generated chunks after merge have version number 0, so we set merged Modification
   * file version to 1 to take effect.
   */
  private static final int MERGE_MOD_START_VERSION_NUM = 1;

  private static final Logger logger = LoggerFactory.getLogger(DataRegion.class);

  /** indicating the file to be loaded overlap with some files. */
  private static final int POS_OVERLAP = -3;

  private final boolean enableMemControl = config.isEnableMemControl();
  /**
   * a read write lock for guaranteeing concurrent safety when accessing all fields in this class
   * (i.e., schema, (un)sequenceFileList, work(un)SequenceTsFileProcessor,
   * closing(Un)SequenceTsFileProcessor, latestTimeForEachDevice, and
   * partitionLatestFlushedTimeForEachDevice)
   */
  private final ReadWriteLock insertLock = new ReentrantReadWriteLock();
  /** condition to safely delete data region. */
  private final Condition deletedCondition = insertLock.writeLock().newCondition();
  /** data region has been deleted or not. */
  private volatile boolean deleted = false;
  /** closeStorageGroupCondition is used to wait for all currently closing TsFiles to be done. */
  private final Object closeStorageGroupCondition = new Object();
  /**
   * avoid some tsfileResource is changed (e.g., from unsealed to sealed) when a query is executed.
   */
  private final ReadWriteLock closeQueryLock = new ReentrantReadWriteLock();
  /** time partition id in the database -> tsFileProcessor for this time partition. */
  private final TreeMap<Long, TsFileProcessor> workSequenceTsFileProcessors = new TreeMap<>();
  /** time partition id in the database -> tsFileProcessor for this time partition. */
  private final TreeMap<Long, TsFileProcessor> workUnsequenceTsFileProcessors = new TreeMap<>();

  // upgrading sequence TsFile resource list
  private final List<TsFileResource> upgradeSeqFileList = new LinkedList<>();
  /** sequence tsfile processors which are closing. */
  private final CopyOnReadLinkedList<TsFileProcessor> closingSequenceTsFileProcessor =
      new CopyOnReadLinkedList<>();
  /** upgrading unsequence TsFile resource list. */
  private final List<TsFileResource> upgradeUnseqFileList = new LinkedList<>();

  /** unsequence tsfile processors which are closing. */
  private final CopyOnReadLinkedList<TsFileProcessor> closingUnSequenceTsFileProcessor =
      new CopyOnReadLinkedList<>();

  private final AtomicInteger upgradeFileCount = new AtomicInteger();

  private final AtomicBoolean isSettling = new AtomicBoolean();

  /** data region id. */
  private final String dataRegionId;
  /** database name. */
  private final String databaseName;
  /** database system directory. */
  private File storageGroupSysDir;
  /** manage seqFileList and unSeqFileList. */
  private final TsFileManager tsFileManager;

  /** manage tsFileResource degrade. */
  private final TsFileResourceManager tsFileResourceManager = TsFileResourceManager.getInstance();

  /**
   * time partition id -> version controller which assigns a version for each MemTable and
   * deletion/update such that after they are persisted, the order of insertions, deletions and
   * updates can be re-determined. Will be empty if there are not MemTables in memory.
   */
  private final HashMap<Long, VersionController> timePartitionIdVersionControllerMap =
      new HashMap<>();
  /**
   * when the data in a database is older than dataTTL, it is considered invalid and will be
   * eventually removed.
   */
  private long dataTTL = Long.MAX_VALUE;
  /** file system factory (local or hdfs). */
  private final FSFactory fsFactory = FSFactoryProducer.getFSFactory();
  /** file flush policy. */
  private TsFileFlushPolicy fileFlushPolicy;
  /**
   * The max file versions in each partition. By recording this, if several IoTDB instances have the
   * same policy of closing file and their ingestion is identical, then files of the same version in
   * different IoTDB instance will have identical data, providing convenience for data comparison
   * across different instances. partition number -> max version number
   */
  private Map<Long, Long> partitionMaxFileVersions = new HashMap<>();
  /** database info for mem control. */
  private final DataRegionInfo dataRegionInfo = new DataRegionInfo(this);
  /** whether it's ready from recovery. */
  private boolean isReady = false;
  /** close file listeners. */
  private List<CloseFileListener> customCloseFileListeners = Collections.emptyList();
  /** flush listeners. */
  private List<FlushListener> customFlushListeners = Collections.emptyList();

  private ILastFlushTimeMap lastFlushTimeMap;

  /**
   * record the insertWriteLock in SG is being hold by which method, it will be empty string if on
   * one holds the insertWriteLock.
   */
  private String insertWriteLockHolder = "";

  private ScheduledExecutorService timedCompactionScheduleTask;

  public static final long COMPACTION_TASK_SUBMIT_DELAY = 20L * 1000L;

  private IDTable idTable;

  private final QueryMetricsManager queryMetricsManager = QueryMetricsManager.getInstance();

  /**
   * construct a database processor.
   *
   * @param systemDir system dir path
   * @param dataRegionId data region id e.g. 1
   * @param fileFlushPolicy file flush policy
   * @param databaseName database name e.g. root.sg1
   */
  public DataRegion(
      String systemDir, String dataRegionId, TsFileFlushPolicy fileFlushPolicy, String databaseName)
      throws DataRegionException {
    this.dataRegionId = dataRegionId;
    this.databaseName = databaseName;
    this.fileFlushPolicy = fileFlushPolicy;

    storageGroupSysDir = SystemFileFactory.INSTANCE.getFile(systemDir, dataRegionId);
    this.tsFileManager =
        new TsFileManager(databaseName, dataRegionId, storageGroupSysDir.getPath());
    if (storageGroupSysDir.mkdirs()) {
      logger.info(
          "Database system Directory {} doesn't exist, create it", storageGroupSysDir.getPath());
    } else if (!storageGroupSysDir.exists()) {
      logger.error("create database system Directory {} failed", storageGroupSysDir.getPath());
    }

    // if you use id table, we use id table flush time manager
    if (config.isEnableIDTable()) {
      idTable = IDTableManager.getInstance().getIDTableDirectly(databaseName);
      lastFlushTimeMap = new IDTableLastFlushTimeMap(idTable, tsFileManager);
    } else {
      lastFlushTimeMap = new HashLastFlushTimeMap(tsFileManager);
    }

    // recover tsfiles unless consensus protocol is ratis and storage engine is not ready
    if (config.isClusterMode()
        && config.getDataRegionConsensusProtocolClass().equals(ConsensusFactory.RATIS_CONSENSUS)
        && !StorageEngine.getInstance().isAllSgReady()) {
      logger.debug(
          "Skip recovering data region {}[{}] when consensus protocol is ratis and storage engine is not ready.",
          databaseName,
          dataRegionId);
      for (String fileFolder : DirectoryManager.getInstance().getAllFilesFolders()) {
        File dataRegionFolder =
            fsFactory.getFile(fileFolder, databaseName + File.separator + dataRegionId);
        if (dataRegionFolder.exists()) {
          File[] timePartitions = dataRegionFolder.listFiles();
          if (timePartitions != null) {
            for (File timePartition : timePartitions) {
              try {
                FileUtils.forceDelete(timePartition);
              } catch (IOException e) {
                logger.error(
                    "Exception occurs when deleting time partition directory {} for {}-{}",
                    timePartitions,
                    databaseName,
                    dataRegionId,
                    e);
              }
            }
          }
        }
      }
    } else {
      recover();
    }

    MetricService.getInstance().addMetricSet(new DataRegionMetrics(this));
  }

  @TestOnly
  public DataRegion(String databaseName, String id) {
    this.databaseName = databaseName;
    this.dataRegionId = id;
    this.tsFileManager = new TsFileManager(databaseName, id, "");
    this.partitionMaxFileVersions = new HashMap<>();
    partitionMaxFileVersions.put(0L, 0L);
  }

  @Override
  public String getDatabaseName() {
    return databaseName;
  }

  public boolean isReady() {
    return isReady;
  }

  public void setReady(boolean ready) {
    isReady = ready;
  }

  private Map<Long, List<TsFileResource>> splitResourcesByPartition(
      List<TsFileResource> resources) {
    Map<Long, List<TsFileResource>> ret = new TreeMap<>();
    for (TsFileResource resource : resources) {
      ret.computeIfAbsent(resource.getTimePartition(), l -> new ArrayList<>()).add(resource);
    }
    return ret;
  }

  public AtomicBoolean getIsSettling() {
    return isSettling;
  }

  public void setSettling(boolean isSettling) {
    this.isSettling.set(isSettling);
  }

  /** this class is used to store recovering context. */
  private class DataRegionRecoveryContext {
    /** number of files to be recovered. */
    private final long numOfFilesToRecover;
    /** when the change of recoveredFilesNum exceeds this, log check will be triggered. */
    private final long filesNumLogCheckTrigger;
    /** number of already recovered files. */
    private long recoveredFilesNum;
    /** last recovery log time. */
    private long lastLogTime;
    /** last recovery log files num. */
    private long lastLogCheckFilesNum;
    /** recover performers of unsealed TsFiles. */
    private final List<UnsealedTsFileRecoverPerformer> recoverPerformers = new ArrayList<>();

    public DataRegionRecoveryContext(long numOfFilesToRecover) {
      this.numOfFilesToRecover = numOfFilesToRecover;
      this.recoveredFilesNum = 0;
      this.filesNumLogCheckTrigger = this.numOfFilesToRecover / 100;
      this.lastLogTime = System.currentTimeMillis();
      this.lastLogCheckFilesNum = 0;
    }

    public void incrementRecoveredFilesNum() {
      recoveredFilesNum++;
      // check log only when 1% more files have been recovered
      if (lastLogCheckFilesNum + filesNumLogCheckTrigger < recoveredFilesNum) {
        lastLogCheckFilesNum = recoveredFilesNum;
        // log only when log interval exceeds recovery log interval
        if (lastLogTime + config.getRecoveryLogIntervalInMs() < System.currentTimeMillis()) {
          logger.info(
              "The data region {}[{}] has recovered {}%, please wait a moment.",
              databaseName, dataRegionId, recoveredFilesNum * 1.0 / numOfFilesToRecover);
          lastLogTime = System.currentTimeMillis();
        }
      }
    }
  }

  /** recover from file */
  private void recover() throws DataRegionException {
    try {
      recoverCompaction();
    } catch (Exception e) {
      throw new DataRegionException(e);
    }

    try {
      // collect candidate TsFiles from sequential and unsequential data directory
      Pair<List<TsFileResource>, List<TsFileResource>> seqTsFilesPair =
          getAllFiles(DirectoryManager.getInstance().getAllSequenceFileFolders());
      List<TsFileResource> tmpSeqTsFiles = seqTsFilesPair.left;
      List<TsFileResource> oldSeqTsFiles = seqTsFilesPair.right;
      upgradeSeqFileList.addAll(oldSeqTsFiles);
      Pair<List<TsFileResource>, List<TsFileResource>> unseqTsFilesPair =
          getAllFiles(DirectoryManager.getInstance().getAllUnSequenceFileFolders());
      List<TsFileResource> tmpUnseqTsFiles = unseqTsFilesPair.left;
      List<TsFileResource> oldUnseqTsFiles = unseqTsFilesPair.right;
      upgradeUnseqFileList.addAll(oldUnseqTsFiles);

      if (upgradeSeqFileList.size() + upgradeUnseqFileList.size() != 0) {
        upgradeFileCount.set(upgradeSeqFileList.size() + upgradeUnseqFileList.size());
      }

      // split by partition so that we can find the last file of each partition and decide to
      // close it or not
      DataRegionRecoveryContext dataRegionRecoveryContext =
          new DataRegionRecoveryContext(tmpSeqTsFiles.size() + tmpUnseqTsFiles.size());
      Map<Long, List<TsFileResource>> partitionTmpSeqTsFiles =
          splitResourcesByPartition(tmpSeqTsFiles);
      Map<Long, List<TsFileResource>> partitionTmpUnseqTsFiles =
          splitResourcesByPartition(tmpUnseqTsFiles);
      // submit unsealed TsFiles to recover
      List<WALRecoverListener> recoverListeners = new ArrayList<>();
      for (List<TsFileResource> value : partitionTmpSeqTsFiles.values()) {
        // tsFiles without resource file are unsealed
        for (TsFileResource resource : value) {
          if (resource.resourceFileExists()) {
            TsFileMetricManager.getInstance().addFile(resource.getTsFile().length(), true);
            if (resource.getModFile().exists()) {
              TsFileMetricManager.getInstance().increaseModFileNum(1);
              TsFileMetricManager.getInstance()
                  .increaseModFileSize(resource.getModFile().getSize());
            }
          }
        }
        while (!value.isEmpty()) {
          TsFileResource tsFileResource = value.get(value.size() - 1);
          if (tsFileResource.resourceFileExists()) {
            break;
          } else {
            value.remove(value.size() - 1);
            WALRecoverListener recoverListener =
                recoverUnsealedTsFile(tsFileResource, dataRegionRecoveryContext, true);
            if (recoverListener != null) {
              recoverListeners.add(recoverListener);
            }
          }
        }
      }
      for (List<TsFileResource> value : partitionTmpUnseqTsFiles.values()) {
        // tsFiles without resource file are unsealed
        for (TsFileResource resource : value) {
          if (resource.resourceFileExists()) {
            TsFileMetricManager.getInstance().addFile(resource.getTsFile().length(), false);
          }
          if (resource.getModFile().exists()) {
            TsFileMetricManager.getInstance().increaseModFileNum(1);
            TsFileMetricManager.getInstance().increaseModFileSize(resource.getModFile().getSize());
          }
        }
        while (!value.isEmpty()) {
          TsFileResource tsFileResource = value.get(value.size() - 1);
          if (tsFileResource.resourceFileExists()) {
            break;
          } else {
            value.remove(value.size() - 1);
            WALRecoverListener recoverListener =
                recoverUnsealedTsFile(tsFileResource, dataRegionRecoveryContext, false);
            if (recoverListener != null) {
              recoverListeners.add(recoverListener);
            }
          }
        }
      }
      // signal wal recover manager to recover this region's files
      WALRecoverManager.getInstance().getAllDataRegionScannedLatch().countDown();
      // recover sealed TsFiles
      if (!partitionTmpSeqTsFiles.isEmpty()) {
        long latestPartitionId =
            ((TreeMap<Long, List<TsFileResource>>) partitionTmpSeqTsFiles).lastKey();
        for (Entry<Long, List<TsFileResource>> partitionFiles : partitionTmpSeqTsFiles.entrySet()) {
          recoverFilesInPartition(
              partitionFiles.getKey(),
              dataRegionRecoveryContext,
              partitionFiles.getValue(),
              true,
              partitionFiles.getKey() == latestPartitionId);
        }
      }
      for (Entry<Long, List<TsFileResource>> partitionFiles : partitionTmpUnseqTsFiles.entrySet()) {
        recoverFilesInPartition(
            partitionFiles.getKey(),
            dataRegionRecoveryContext,
            partitionFiles.getValue(),
            false,
            false);
      }
      // wait until all unsealed TsFiles have been recovered
      for (WALRecoverListener recoverListener : recoverListeners) {
        if (recoverListener.waitForResult() == WALRecoverListener.Status.FAILURE) {
          logger.error(
              "Fail to recover unsealed TsFile {}, skip it.",
              recoverListener.getFilePath(),
              recoverListener.getCause());
        }
        // update VSGRecoveryContext
        dataRegionRecoveryContext.incrementRecoveredFilesNum();
      }
      // recover unsealed TsFiles, sort make sure last flush time not be replaced by early files
      dataRegionRecoveryContext.recoverPerformers.sort(
          (p1, p2) ->
              compareFileName(
                  p1.getTsFileResource().getTsFile(), p2.getTsFileResource().getTsFile()));
      for (UnsealedTsFileRecoverPerformer recoverPerformer :
          dataRegionRecoveryContext.recoverPerformers) {
        recoverUnsealedTsFileCallBack(recoverPerformer);
      }
      for (TsFileResource resource : tsFileManager.getTsFileList(true)) {
        long partitionNum = resource.getTimePartition();
        updatePartitionFileVersion(partitionNum, resource.getVersion());
      }
      for (TsFileResource resource : tsFileManager.getTsFileList(false)) {
        long partitionNum = resource.getTimePartition();
        updatePartitionFileVersion(partitionNum, resource.getVersion());
      }
      for (TsFileResource resource : upgradeSeqFileList) {
        long partitionNum = resource.getTimePartition();
        updatePartitionFileVersion(partitionNum, resource.getVersion());
      }
      for (TsFileResource resource : upgradeUnseqFileList) {
        long partitionNum = resource.getTimePartition();
        updatePartitionFileVersion(partitionNum, resource.getVersion());
      }
      updateLatestFlushedTime();
    } catch (IOException e) {
      throw new DataRegionException(e);
    }

    // recover and start timed compaction thread
    initCompaction();

    if (StorageEngine.getInstance().isAllSgReady()) {
      logger.info("The data region {}[{}] is created successfully", databaseName, dataRegionId);
    } else {
      logger.info("The data region {}[{}] is recovered successfully", databaseName, dataRegionId);
    }
  }

  private void updateLastFlushTime(TsFileResource resource, boolean isSeq) {
    //  only update flush time when it is a seq file
    if (isSeq) {
      long timePartitionId = resource.getTimePartition();
      Map<String, Long> endTimeMap = new HashMap<>();
      for (String deviceId : resource.getDevices()) {
        long endTime = resource.getEndTime(deviceId);
        endTimeMap.put(deviceId.intern(), endTime);
      }
      lastFlushTimeMap.setMultiDeviceFlushedTime(timePartitionId, endTimeMap);
      lastFlushTimeMap.setMultiDeviceGlobalFlushedTime(endTimeMap);
    }
  }

  private void initCompaction() {
    if (!config.isEnableSeqSpaceCompaction()
        && !config.isEnableUnseqSpaceCompaction()
        && !config.isEnableCrossSpaceCompaction()) {
      return;
    }
    timedCompactionScheduleTask =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
            ThreadName.COMPACTION_SCHEDULE.getName() + "-" + databaseName + "-" + dataRegionId);
    ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
        timedCompactionScheduleTask,
        this::executeCompaction,
        COMPACTION_TASK_SUBMIT_DELAY,
        IoTDBDescriptor.getInstance().getConfig().getCompactionScheduleIntervalInMs(),
        TimeUnit.MILLISECONDS);
  }

  private void recoverCompaction() {
    CompactionRecoverManager compactionRecoverManager =
        new CompactionRecoverManager(tsFileManager, databaseName, dataRegionId);
    compactionRecoverManager.recoverInnerSpaceCompaction(true);
    compactionRecoverManager.recoverInnerSpaceCompaction(false);
    compactionRecoverManager.recoverCrossSpaceCompaction();
  }

  private void updatePartitionFileVersion(long partitionNum, long fileVersion) {
    long oldVersion = partitionMaxFileVersions.getOrDefault(partitionNum, 0L);
    if (fileVersion > oldVersion) {
      partitionMaxFileVersions.put(partitionNum, fileVersion);
    }
  }

  /**
   * use old seq file to update latestTimeForEachDevice, globalLatestFlushedTimeForEachDevice,
   * partitionLatestFlushedTimeForEachDevice and timePartitionIdVersionControllerMap.
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private void updateLatestFlushedTime() throws IOException {

    VersionController versionController =
        new SimpleFileVersionController(storageGroupSysDir.getPath());
    long currentVersion = versionController.currVersion();
    for (TsFileResource resource : upgradeSeqFileList) {
      for (String deviceId : resource.getDevices()) {
        long endTime = resource.getEndTime(deviceId);
        long endTimePartitionId = StorageEngine.getTimePartition(endTime);
        lastFlushTimeMap.setOneDeviceGlobalFlushedTime(deviceId, endTime);

        // set all the covered partition's LatestFlushedTime
        long partitionId = StorageEngine.getTimePartition(resource.getStartTime(deviceId));
        while (partitionId <= endTimePartitionId) {
          lastFlushTimeMap.setOneDeviceFlushedTime(partitionId, deviceId, endTime);
          if (!timePartitionIdVersionControllerMap.containsKey(partitionId)) {
            File directory =
                SystemFileFactory.INSTANCE.getFile(storageGroupSysDir, String.valueOf(partitionId));
            if (!directory.exists()) {
              directory.mkdirs();
            }
            File versionFile =
                SystemFileFactory.INSTANCE.getFile(
                    directory, SimpleFileVersionController.FILE_PREFIX + currentVersion);
            if (!versionFile.createNewFile()) {
              logger.warn("Version file {} has already been created ", versionFile);
            }
            timePartitionIdVersionControllerMap.put(
                partitionId,
                new SimpleFileVersionController(storageGroupSysDir.getPath(), partitionId));
          }
          partitionId++;
        }
      }
    }
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private Pair<List<TsFileResource>, List<TsFileResource>> getAllFiles(List<String> folders)
      throws IOException, DataRegionException {
    List<File> tsFiles = new ArrayList<>();
    List<File> upgradeFiles = new ArrayList<>();
    for (String baseDir : folders) {
      File fileFolder = fsFactory.getFile(baseDir + File.separator + databaseName, dataRegionId);
      if (!fileFolder.exists()) {
        continue;
      }

      // old version
      // some TsFileResource may be being persisted when the system crashed, try recovering such
      // resources
      continueFailedRenames(fileFolder, TEMP_SUFFIX);

      File[] subFiles = fileFolder.listFiles();
      if (subFiles != null) {
        for (File partitionFolder : subFiles) {
          if (!partitionFolder.isDirectory()) {
            logger.warn("{} is not a directory.", partitionFolder.getAbsolutePath());
          } else if (!partitionFolder.getName().equals(IoTDBConstant.UPGRADE_FOLDER_NAME)) {
            // some TsFileResource may be being persisted when the system crashed, try recovering
            // such
            // resources
            continueFailedRenames(partitionFolder, TEMP_SUFFIX);

            Collections.addAll(
                tsFiles,
                fsFactory.listFilesBySuffix(partitionFolder.getAbsolutePath(), TSFILE_SUFFIX));
          } else {
            // collect old TsFiles for upgrading
            Collections.addAll(
                upgradeFiles,
                fsFactory.listFilesBySuffix(partitionFolder.getAbsolutePath(), TSFILE_SUFFIX));
          }
        }
      }
    }

    tsFiles.sort(this::compareFileName);
    if (!tsFiles.isEmpty()) {
      checkTsFileTime(tsFiles.get(tsFiles.size() - 1));
    }
    List<TsFileResource> ret = new ArrayList<>();
    tsFiles.forEach(f -> ret.add(new TsFileResource(f)));

    upgradeFiles.sort(this::compareFileName);
    if (!upgradeFiles.isEmpty()) {
      checkTsFileTime(upgradeFiles.get(upgradeFiles.size() - 1));
    }
    List<TsFileResource> upgradeRet = new ArrayList<>();
    for (File f : upgradeFiles) {
      TsFileResource fileResource = new TsFileResource(f);
      fileResource.setStatus(TsFileResourceStatus.CLOSED);
      // make sure the flush command is called before IoTDB is down.
      fileResource.deserializeFromOldFile();
      upgradeRet.add(fileResource);
    }
    return new Pair<>(ret, upgradeRet);
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

  /** check if the tsfile's time is smaller than system current time. */
  private void checkTsFileTime(File tsFile) throws DataRegionException {
    String[] items = tsFile.getName().replace(TSFILE_SUFFIX, "").split(FILE_NAME_SEPARATOR);
    long fileTime = Long.parseLong(items[0]);
    long currentTime = System.currentTimeMillis();
    if (fileTime > currentTime) {
      throw new DataRegionException(
          String.format(
              "data region %s[%s] is down, because the time of tsfile %s is larger than system current time, "
                  + "file time is %d while system current time is %d, please check it.",
              databaseName, dataRegionId, tsFile.getAbsolutePath(), fileTime, currentTime));
    }
  }

  /** submit unsealed TsFile to WALRecoverManager. */
  private WALRecoverListener recoverUnsealedTsFile(
      TsFileResource unsealedTsFile, DataRegionRecoveryContext context, boolean isSeq) {
    UnsealedTsFileRecoverPerformer recoverPerformer =
        new UnsealedTsFileRecoverPerformer(
            unsealedTsFile, isSeq, idTable, context.recoverPerformers::add);
    // remember to close UnsealedTsFileRecoverPerformer
    return WALRecoverManager.getInstance().addRecoverPerformer(recoverPerformer);
  }

  private void recoverUnsealedTsFileCallBack(UnsealedTsFileRecoverPerformer recoverPerformer) {
    try {
      TsFileResource tsFileResource = recoverPerformer.getTsFileResource();
      boolean isSeq = recoverPerformer.isSequence();
      if (!recoverPerformer.canWrite()) {
        // cannot write, just close it
        for (ISyncManager syncManager :
            SyncService.getInstance().getOrCreateSyncManager(dataRegionId)) {
          syncManager.syncRealTimeTsFile(tsFileResource.getTsFile());
        }
        try {
          tsFileResource.close();
        } catch (IOException e) {
          logger.error("Fail to close TsFile {} when recovering", tsFileResource.getTsFile(), e);
        }
        updateLastFlushTime(tsFileResource, isSeq);
        tsFileResourceManager.registerSealedTsFileResource(tsFileResource);
        TsFileMetricManager.getInstance()
            .addFile(tsFileResource.getTsFile().length(), recoverPerformer.isSequence());
      } else {
        // the last file is not closed, continue writing to it
        RestorableTsFileIOWriter writer = recoverPerformer.getWriter();
        long timePartitionId = tsFileResource.getTimePartition();
        TimePartitionManager.getInstance()
            .updateAfterOpeningTsFileProcessor(
                new DataRegionId(Integer.parseInt(dataRegionId)), timePartitionId);
        TsFileProcessor tsFileProcessor =
            new TsFileProcessor(
                dataRegionId,
                dataRegionInfo,
                tsFileResource,
                this::closeUnsealedTsFileProcessorCallBack,
                isSeq ? this::sequenceFlushCallback : this::unsequenceFlushCallback,
                isSeq,
                writer);
        if (isSeq) {
          workSequenceTsFileProcessors.put(timePartitionId, tsFileProcessor);
        } else {
          workUnsequenceTsFileProcessors.put(timePartitionId, tsFileProcessor);
        }
        tsFileResource.setProcessor(tsFileProcessor);
        tsFileResource.removeResourceFile();
        tsFileProcessor.setTimeRangeId(timePartitionId);
        writer.makeMetadataVisible();
        if (enableMemControl) {
          TsFileProcessorInfo tsFileProcessorInfo = new TsFileProcessorInfo(dataRegionInfo);
          tsFileProcessor.setTsFileProcessorInfo(tsFileProcessorInfo);
          this.dataRegionInfo.initTsFileProcessorInfo(tsFileProcessor);
          // get chunkMetadata size
          long chunkMetadataSize = 0;
          for (Map<String, List<ChunkMetadata>> metaMap : writer.getMetadatasForQuery().values()) {
            for (List<ChunkMetadata> metadatas : metaMap.values()) {
              for (ChunkMetadata chunkMetadata : metadatas) {
                chunkMetadataSize += chunkMetadata.calculateRamSize();
              }
            }
          }
          tsFileProcessorInfo.addTSPMemCost(chunkMetadataSize);
        }
      }
      tsFileManager.add(tsFileResource, recoverPerformer.isSequence());
    } catch (Throwable e) {
      logger.error(
          "Fail to recover unsealed TsFile {}, skip it.",
          recoverPerformer.getTsFileAbsolutePath(),
          e);
    }
  }

  /** recover sealed TsFile. */
  private void recoverSealedTsFiles(
      TsFileResource sealedTsFile, DataRegionRecoveryContext context, boolean isSeq) {
    try (SealedTsFileRecoverPerformer recoverPerformer =
        new SealedTsFileRecoverPerformer(sealedTsFile)) {
      recoverPerformer.recover();
      // pick up crashed compaction target files
      if (recoverPerformer.hasCrashed()) {
        if (TsFileResource.getInnerCompactionCount(sealedTsFile.getTsFile().getName()) > 0) {
          tsFileManager.addForRecover(sealedTsFile, isSeq);
          return;
        } else {
          logger.warn(
              "Sealed TsFile {} has crashed at zero level, truncate and recover it.",
              sealedTsFile.getTsFilePath());
        }
      }
      sealedTsFile.close();
      tsFileManager.add(sealedTsFile, isSeq);
      tsFileResourceManager.registerSealedTsFileResource(sealedTsFile);
    } catch (Throwable e) {
      logger.error("Fail to recover sealed TsFile {}, skip it.", sealedTsFile.getTsFilePath(), e);
    } finally {
      // update recovery context
      context.incrementRecoveredFilesNum();
    }
  }

  private void recoverFilesInPartition(
      long partitionId,
      DataRegionRecoveryContext context,
      List<TsFileResource> resourceList,
      boolean isSeq,
      boolean isLatestPartition) {
    for (TsFileResource tsFileResource : resourceList) {
      recoverSealedTsFiles(tsFileResource, context, isSeq);
    }
    if (isLatestPartition && isSeq) {
      lastFlushTimeMap.checkAndCreateFlushedTimePartition(partitionId);
      for (TsFileResource tsFileResource : resourceList) {
        updateLastFlushTime(tsFileResource, true);
      }
      TimePartitionManager.getInstance()
          .registerTimePartitionInfo(
              new TimePartitionInfo(
                  new DataRegionId(Integer.parseInt(dataRegionId)),
                  partitionId,
                  false,
                  Long.MAX_VALUE,
                  lastFlushTimeMap.getMemSize(partitionId),
                  true));
    }
  }

  // ({systemTime}-{versionNum}-{mergeNum}.tsfile)
  private int compareFileName(File o1, File o2) {
    String[] items1 = o1.getName().replace(TSFILE_SUFFIX, "").split(FILE_NAME_SEPARATOR);
    String[] items2 = o2.getName().replace(TSFILE_SUFFIX, "").split(FILE_NAME_SEPARATOR);
    long ver1 = Long.parseLong(items1[0]);
    long ver2 = Long.parseLong(items2[0]);
    int cmp = Long.compare(ver1, ver2);
    if (cmp == 0) {
      return Long.compare(Long.parseLong(items1[1]), Long.parseLong(items2[1]));
    } else {
      return cmp;
    }
  }

  /**
   * insert one row of data.
   *
   * @param insertRowNode one row of data
   */
  public void insert(InsertRowNode insertRowNode) throws WriteProcessException {
    // reject insertions that are out of ttl
    if (!isAlive(insertRowNode.getTime())) {
      throw new OutOfTTLException(insertRowNode.getTime(), (DateTimeUtils.currentTime() - dataTTL));
    }
    if (enableMemControl) {
      StorageEngine.blockInsertionIfReject(null);
    }
    long startTime = System.nanoTime();
    writeLock("InsertRow");
    PerformanceOverviewMetricsManager.recordScheduleLockCost(System.nanoTime() - startTime);
    try {
      if (deleted) {
        return;
      }
      // init map
      long timePartitionId = StorageEngine.getTimePartition(insertRowNode.getTime());

      if (!lastFlushTimeMap.checkAndCreateFlushedTimePartition(timePartitionId)) {
        TimePartitionManager.getInstance()
            .registerTimePartitionInfo(
                new TimePartitionInfo(
                    new DataRegionId(Integer.parseInt(dataRegionId)),
                    timePartitionId,
                    true,
                    Long.MAX_VALUE,
                    0,
                    tsFileManager.isLatestTimePartition(timePartitionId)));
      }

      boolean isSequence =
          insertRowNode.getTime()
              > lastFlushTimeMap.getFlushedTime(
                  timePartitionId, insertRowNode.getDevicePath().getFullPath());

      // is unsequence and user set config to discard out of order data
      if (!isSequence
          && IoTDBDescriptor.getInstance().getConfig().isEnableDiscardOutOfOrderData()) {
        return;
      }

      // insert to sequence or unSequence file
      insertToTsFileProcessor(insertRowNode, isSequence, timePartitionId);
    } finally {
      writeUnlock();
    }
  }

  /**
   * Insert a tablet (rows belonging to the same devices) into this database.
   *
   * @throws BatchProcessException if some of the rows failed to be inserted
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void insertTablet(InsertTabletNode insertTabletNode)
      throws BatchProcessException, WriteProcessException {
    if (enableMemControl) {
      StorageEngine.blockInsertionIfReject(null);
    }
    long startTime = System.nanoTime();
    writeLock("insertTablet");
    PerformanceOverviewMetricsManager.recordScheduleLockCost(System.nanoTime() - startTime);
    try {
      if (deleted) {
        return;
      }
      TSStatus[] results = new TSStatus[insertTabletNode.getRowCount()];
      Arrays.fill(results, RpcUtils.SUCCESS_STATUS);
      boolean noFailure = true;

      /*
       * assume that batch has been sorted by client
       */
      int loc = 0;
      while (loc < insertTabletNode.getRowCount()) {
        long currTime = insertTabletNode.getTimes()[loc];
        // skip points that do not satisfy TTL
        if (!isAlive(currTime)) {
          results[loc] =
              RpcUtils.getStatus(
                  TSStatusCode.OUT_OF_TTL,
                  String.format(
                      "Insertion time [%s] is less than ttl time bound [%s]",
                      DateTimeUtils.convertMillsecondToZonedDateTime(currTime),
                      DateTimeUtils.convertMillsecondToZonedDateTime(
                          DateTimeUtils.currentTime() - dataTTL)));
          loc++;
          noFailure = false;
        } else {
          break;
        }
      }
      // loc pointing at first legal position
      if (loc == insertTabletNode.getRowCount()) {
        throw new OutOfTTLException(
            insertTabletNode.getTimes()[insertTabletNode.getTimes().length - 1],
            (DateTimeUtils.currentTime() - dataTTL));
      }
      // before is first start point
      int before = loc;
      // before time partition
      long beforeTimePartition =
          StorageEngine.getTimePartition(insertTabletNode.getTimes()[before]);
      // init map

      if (!lastFlushTimeMap.checkAndCreateFlushedTimePartition(beforeTimePartition)) {
        TimePartitionManager.getInstance()
            .registerTimePartitionInfo(
                new TimePartitionInfo(
                    new DataRegionId(Integer.parseInt(dataRegionId)),
                    beforeTimePartition,
                    true,
                    Long.MAX_VALUE,
                    0,
                    tsFileManager.isLatestTimePartition(beforeTimePartition)));
      }

      long lastFlushTime =
          lastFlushTimeMap.getFlushedTime(
              beforeTimePartition, insertTabletNode.getDevicePath().getFullPath());

      // if is sequence
      boolean isSequence = false;
      while (loc < insertTabletNode.getRowCount()) {
        long time = insertTabletNode.getTimes()[loc];
        // always in some time partition
        // judge if we should insert sequence
        if (!isSequence && time > lastFlushTime) {
          // insert into unsequence and then start sequence
          if (!IoTDBDescriptor.getInstance().getConfig().isEnableDiscardOutOfOrderData()) {
            noFailure =
                insertTabletToTsFileProcessor(
                        insertTabletNode, before, loc, false, results, beforeTimePartition)
                    && noFailure;
          }
          before = loc;
          isSequence = true;
        }
        loc++;
      }

      // do not forget last part
      if (before < loc
          && (isSequence
              || !IoTDBDescriptor.getInstance().getConfig().isEnableDiscardOutOfOrderData())) {
        noFailure =
            insertTabletToTsFileProcessor(
                    insertTabletNode, before, loc, isSequence, results, beforeTimePartition)
                && noFailure;
      }
      long globalLatestFlushedTime =
          lastFlushTimeMap.getGlobalFlushedTime(insertTabletNode.getDevicePath().getFullPath());
      startTime = System.nanoTime();
      tryToUpdateBatchInsertLastCache(insertTabletNode, globalLatestFlushedTime);
      PerformanceOverviewMetricsManager.recordScheduleUpdateLastCacheCost(
          System.nanoTime() - startTime);

      if (!noFailure) {
        throw new BatchProcessException(results);
      }
    } finally {
      writeUnlock();
    }
  }

  /**
   * Check whether the time falls in TTL.
   *
   * @return whether the given time falls in ttl
   */
  private boolean isAlive(long time) {
    return dataTTL == Long.MAX_VALUE || (DateTimeUtils.currentTime() - time) <= dataTTL;
  }

  /**
   * insert batch to tsfile processor thread-safety that the caller need to guarantee The rows to be
   * inserted are in the range [start, end) Null value in each column values will be replaced by the
   * subsequent non-null value, e.g., {1, null, 3, null, 5} will be {1, 3, 5, null, 5}
   *
   * @param insertTabletNode insert a tablet of a device
   * @param sequence whether is sequence
   * @param start start index of rows to be inserted in insertTabletPlan
   * @param end end index of rows to be inserted in insertTabletPlan
   * @param results result array
   * @param timePartitionId time partition id
   * @return false if any failure occurs when inserting the tablet, true otherwise
   */
  private boolean insertTabletToTsFileProcessor(
      InsertTabletNode insertTabletNode,
      int start,
      int end,
      boolean sequence,
      TSStatus[] results,
      long timePartitionId) {
    // return when start >= end
    if (start >= end) {
      return true;
    }

    TsFileProcessor tsFileProcessor = getOrCreateTsFileProcessor(timePartitionId, sequence);
    if (tsFileProcessor == null) {
      for (int i = start; i < end; i++) {
        results[i] =
            RpcUtils.getStatus(
                TSStatusCode.INTERNAL_SERVER_ERROR,
                "can not create TsFileProcessor, timePartitionId: " + timePartitionId);
      }
      return false;
    }

    try {
      tsFileProcessor.insertTablet(insertTabletNode, start, end, results);
    } catch (WriteProcessRejectException e) {
      logger.warn("insert to TsFileProcessor rejected, {}", e.getMessage());
      return false;
    } catch (WriteProcessException e) {
      logger.error("insert to TsFileProcessor error ", e);
      return false;
    }

    // check memtable size and may async try to flush the work memtable
    if (tsFileProcessor.shouldFlush()) {
      fileFlushPolicy.apply(this, tsFileProcessor, sequence);
    }
    return true;
  }

  private void tryToUpdateBatchInsertLastCache(InsertTabletNode node, long latestFlushedTime) {
    if (!IoTDBDescriptor.getInstance().getConfig().isLastCacheEnabled()) {
      return;
    }
    for (int i = 0; i < node.getColumns().length; i++) {
      if (node.getColumns()[i] == null) {
        continue;
      }
      // Update cached last value with high priority
      DataNodeSchemaCache.getInstance()
          .updateLastCache(
              node.getDevicePath().concatNode(node.getMeasurements()[i]),
              node.composeLastTimeValuePair(i),
              true,
              latestFlushedTime);
    }
  }

  private void insertToTsFileProcessor(
      InsertRowNode insertRowNode, boolean sequence, long timePartitionId)
      throws WriteProcessException {
    TsFileProcessor tsFileProcessor = getOrCreateTsFileProcessor(timePartitionId, sequence);
    if (tsFileProcessor == null) {
      return;
    }

    tsFileProcessor.insert(insertRowNode);

    long globalLatestFlushTime =
        lastFlushTimeMap.getGlobalFlushedTime(insertRowNode.getDevicePath().getFullPath());

    long startTime = System.nanoTime();
    tryToUpdateInsertLastCache(insertRowNode, globalLatestFlushTime);
    PerformanceOverviewMetricsManager.recordScheduleUpdateLastCacheCost(
        System.nanoTime() - startTime);

    // check memtable size and may asyncTryToFlush the work memtable
    if (tsFileProcessor.shouldFlush()) {
      fileFlushPolicy.apply(this, tsFileProcessor, sequence);
    }
  }

  private void tryToUpdateInsertLastCache(InsertRowNode node, long latestFlushedTime) {
    if (!IoTDBDescriptor.getInstance().getConfig().isLastCacheEnabled()) {
      return;
    }
    for (int i = 0; i < node.getValues().length; i++) {
      if (node.getValues()[i] == null) {
        continue;
      }
      // Update cached last value with high priority
      DataNodeSchemaCache.getInstance()
          .updateLastCache(
              node.getDevicePath().concatNode(node.getMeasurements()[i]),
              node.composeTimeValuePair(i),
              true,
              latestFlushedTime);
    }
  }

  /**
   * WAL module uses this method to flush memTable
   *
   * @return True if flush task is submitted successfully
   */
  public boolean submitAFlushTask(long timeRangeId, boolean sequence, IMemTable memTable) {
    writeLock("submitAFlushTask");
    try {
      if (memTable.getFlushStatus() != FlushStatus.WORKING) {
        return false;
      }

      TsFileProcessor tsFileProcessor;
      if (sequence) {
        tsFileProcessor = workSequenceTsFileProcessors.get(timeRangeId);
      } else {
        tsFileProcessor = workUnsequenceTsFileProcessors.get(timeRangeId);
      }
      // only submit when tsFileProcessor exists and memTables are same
      boolean shouldSubmit =
          tsFileProcessor != null && tsFileProcessor.getWorkMemTable() == memTable;
      if (shouldSubmit) {
        fileFlushPolicy.apply(this, tsFileProcessor, tsFileProcessor.isSequence());
      }
      return shouldSubmit;
    } finally {
      writeUnlock();
    }
  }

  /**
   * mem control module uses this method to flush memTable
   *
   * @param tsFileProcessor tsfile processor in which memTable to be flushed
   */
  public void submitAFlushTaskWhenShouldFlush(TsFileProcessor tsFileProcessor) {
    writeLock("submitAFlushTaskWhenShouldFlush");
    try {
      // check memtable size and may asyncTryToFlush the work memtable
      if (tsFileProcessor.shouldFlush()) {
        fileFlushPolicy.apply(this, tsFileProcessor, tsFileProcessor.isSequence());
      }
    } finally {
      writeUnlock();
    }
  }

  private TsFileProcessor getOrCreateTsFileProcessor(long timeRangeId, boolean sequence) {
    TsFileProcessor tsFileProcessor = null;
    int retryCnt = 0;
    do {
      try {
        if (sequence) {
          tsFileProcessor =
              getOrCreateTsFileProcessorIntern(timeRangeId, workSequenceTsFileProcessors, true);
        } else {
          tsFileProcessor =
              getOrCreateTsFileProcessorIntern(timeRangeId, workUnsequenceTsFileProcessors, false);
        }
      } catch (DiskSpaceInsufficientException e) {
        logger.error(
            "disk space is insufficient when creating TsFile processor, change system mode to read-only",
            e);
        CommonDescriptor.getInstance().getConfig().setNodeStatus(NodeStatus.ReadOnly);
        break;
      } catch (IOException e) {
        if (retryCnt < 3) {
          logger.warn("meet IOException when creating TsFileProcessor, retry it again", e);
          retryCnt++;
        } else {
          logger.error(
              "meet IOException when creating TsFileProcessor, change system mode to error", e);
          CommonDescriptor.getInstance().getConfig().handleUnrecoverableError();
          break;
        }
      }
    } while (tsFileProcessor == null);
    return tsFileProcessor;
  }

  /**
   * get processor from hashmap, flush oldest processor if necessary
   *
   * @param timeRangeId time partition range
   * @param tsFileProcessorTreeMap tsFileProcessorTreeMap
   * @param sequence whether is sequence or not
   */
  private TsFileProcessor getOrCreateTsFileProcessorIntern(
      long timeRangeId, TreeMap<Long, TsFileProcessor> tsFileProcessorTreeMap, boolean sequence)
      throws IOException, DiskSpaceInsufficientException {

    TsFileProcessor res = tsFileProcessorTreeMap.get(timeRangeId);

    if (null == res) {
      // build new processor, memory control module will control the number of memtables
      TimePartitionManager.getInstance()
          .updateAfterOpeningTsFileProcessor(
              new DataRegionId(Integer.valueOf(dataRegionId)), timeRangeId);
      res = newTsFileProcessor(sequence, timeRangeId);
      tsFileProcessorTreeMap.put(timeRangeId, res);
      tsFileManager.add(res.getTsFileResource(), sequence);
    }

    return res;
  }

  private TsFileProcessor newTsFileProcessor(boolean sequence, long timePartitionId)
      throws IOException, DiskSpaceInsufficientException {

    long version = partitionMaxFileVersions.getOrDefault(timePartitionId, 0L) + 1;
    partitionMaxFileVersions.put(timePartitionId, version);
    String filePath =
        TsFileNameGenerator.generateNewTsFilePathWithMkdir(
            sequence,
            databaseName,
            dataRegionId,
            timePartitionId,
            System.currentTimeMillis(),
            version,
            0,
            0);

    return getTsFileProcessor(sequence, filePath, timePartitionId);
  }

  private TsFileProcessor getTsFileProcessor(
      boolean sequence, String filePath, long timePartitionId) throws IOException {
    TsFileProcessor tsFileProcessor;
    if (sequence) {
      tsFileProcessor =
          new TsFileProcessor(
              databaseName + FILE_NAME_SEPARATOR + dataRegionId,
              fsFactory.getFileWithParent(filePath),
              dataRegionInfo,
              this::closeUnsealedTsFileProcessorCallBack,
              this::sequenceFlushCallback,
              true);
    } else {
      tsFileProcessor =
          new TsFileProcessor(
              databaseName + FILE_NAME_SEPARATOR + dataRegionId,
              fsFactory.getFileWithParent(filePath),
              dataRegionInfo,
              this::closeUnsealedTsFileProcessorCallBack,
              this::unsequenceFlushCallback,
              false);
    }

    if (enableMemControl) {
      TsFileProcessorInfo tsFileProcessorInfo = new TsFileProcessorInfo(dataRegionInfo);
      tsFileProcessor.setTsFileProcessorInfo(tsFileProcessorInfo);
      this.dataRegionInfo.initTsFileProcessorInfo(tsFileProcessor);
    }

    tsFileProcessor.addCloseFileListeners(customCloseFileListeners);
    tsFileProcessor.addFlushListeners(customFlushListeners);
    tsFileProcessor.setTimeRangeId(timePartitionId);

    return tsFileProcessor;
  }

  /**
   * Create a new tsfile name
   *
   * @return file name
   */
  private String getNewTsFileName(long timePartitionId) {
    long version = partitionMaxFileVersions.getOrDefault(timePartitionId, 0L) + 1;
    partitionMaxFileVersions.put(timePartitionId, version);
    return getNewTsFileName(System.currentTimeMillis(), version, 0, 0);
  }

  private String getNewTsFileName(long time, long version, int mergeCnt, int unseqCompactionCnt) {
    return TsFileNameGenerator.generateNewTsFileName(time, version, mergeCnt, unseqCompactionCnt);
  }

  /**
   * close one tsfile processor
   *
   * @param sequence whether this tsfile processor is sequence or not
   * @param tsFileProcessor tsfile processor
   */
  public void syncCloseOneTsFileProcessor(boolean sequence, TsFileProcessor tsFileProcessor) {
    synchronized (closeStorageGroupCondition) {
      try {
        asyncCloseOneTsFileProcessor(sequence, tsFileProcessor);
        long startTime = System.currentTimeMillis();
        while (closingSequenceTsFileProcessor.contains(tsFileProcessor)
            || closingUnSequenceTsFileProcessor.contains(tsFileProcessor)) {
          closeStorageGroupCondition.wait(60_000);
          if (System.currentTimeMillis() - startTime > 60_000) {
            logger.warn(
                "{} has spent {}s to wait for closing one tsfile.",
                databaseName + "-" + this.dataRegionId,
                (System.currentTimeMillis() - startTime) / 1000);
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error(
            "syncCloseOneTsFileProcessor error occurs while waiting for closing the storage "
                + "group {}",
            databaseName + "-" + dataRegionId,
            e);
      }
    }
  }

  /**
   * close one tsfile processor, thread-safety should be ensured by caller
   *
   * @param sequence whether this tsfile processor is sequence or not
   * @param tsFileProcessor tsfile processor
   */
  public void asyncCloseOneTsFileProcessor(boolean sequence, TsFileProcessor tsFileProcessor) {
    // for sequence tsfile, we update the endTimeMap only when the file is prepared to be closed.
    // for unsequence tsfile, we have maintained the endTimeMap when an insertion comes.
    if (closingSequenceTsFileProcessor.contains(tsFileProcessor)
        || closingUnSequenceTsFileProcessor.contains(tsFileProcessor)
        || tsFileProcessor.alreadyMarkedClosing()) {
      return;
    }
    logger.info(
        "Async close tsfile: {}",
        tsFileProcessor.getTsFileResource().getTsFile().getAbsolutePath());

    if (sequence) {
      closingSequenceTsFileProcessor.add(tsFileProcessor);
      tsFileProcessor.asyncClose();

      workSequenceTsFileProcessors.remove(tsFileProcessor.getTimeRangeId());
      // if unsequence files don't contain this time range id, we should remove it's version
      // controller
      if (!workUnsequenceTsFileProcessors.containsKey(tsFileProcessor.getTimeRangeId())) {
        timePartitionIdVersionControllerMap.remove(tsFileProcessor.getTimeRangeId());
      }
      logger.info("close a sequence tsfile processor {}", databaseName + "-" + dataRegionId);
    } else {
      closingUnSequenceTsFileProcessor.add(tsFileProcessor);
      tsFileProcessor.asyncClose();

      workUnsequenceTsFileProcessors.remove(tsFileProcessor.getTimeRangeId());
      // if sequence files don't contain this time range id, we should remove it's version
      // controller
      if (!workSequenceTsFileProcessors.containsKey(tsFileProcessor.getTimeRangeId())) {
        timePartitionIdVersionControllerMap.remove(tsFileProcessor.getTimeRangeId());
      }
    }
  }

  /**
   * delete the database's own folder in folder data/system/databases
   *
   * @param systemDir system dir
   */
  public void deleteFolder(String systemDir) {
    logger.info(
        "{} will close all files for deleting data folder {}",
        databaseName + "-" + dataRegionId,
        systemDir);
    writeLock("deleteFolder");
    try {
      File dataRegionSystemFolder =
          SystemFileFactory.INSTANCE.getFile(
              systemDir + File.separator + databaseName, dataRegionId);
      org.apache.iotdb.commons.utils.FileUtils.deleteDirectoryAndEmptyParent(
          dataRegionSystemFolder);
    } finally {
      writeUnlock();
    }
  }

  /** close all tsfile resource */
  public void closeAllResources() {
    for (TsFileResource tsFileResource : tsFileManager.getTsFileList(false)) {
      try {
        tsFileResource.close();
      } catch (IOException e) {
        logger.error("Cannot close a TsFileResource {}", tsFileResource, e);
      }
    }
    for (TsFileResource tsFileResource : tsFileManager.getTsFileList(true)) {
      try {
        tsFileResource.close();
      } catch (IOException e) {
        logger.error("Cannot close a TsFileResource {}", tsFileResource, e);
      }
    }
  }

  /** delete tsfile */
  public void syncDeleteDataFiles() {
    logger.info(
        "{} will close all files for deleting data files", databaseName + "-" + dataRegionId);
    writeLock("syncDeleteDataFiles");
    try {

      syncCloseAllWorkingTsFileProcessors();
      // normally, mergingModification is just need to be closed by after a merge task is finished.
      // we close it here just for IT test.
      closeAllResources();
      List<TsFileResource> tsFileResourceList = tsFileManager.getTsFileList(true);
      tsFileResourceList.addAll(tsFileManager.getTsFileList(false));
      tsFileResourceList.forEach(
          x -> {
            if (x.getModFile().exists()) {
              TsFileMetricManager.getInstance().decreaseModFileNum(1);
              TsFileMetricManager.getInstance().decreaseModFileSize(x.getModFile().getSize());
            }
          });
      deleteAllSGFolders(DirectoryManager.getInstance().getAllFilesFolders());

      this.workSequenceTsFileProcessors.clear();
      this.workUnsequenceTsFileProcessors.clear();
      this.tsFileManager.clear();
      lastFlushTimeMap.clearFlushedTime();
      lastFlushTimeMap.clearGlobalFlushedTime();
    } finally {
      writeUnlock();
    }
  }

  private void deleteAllSGFolders(List<String> folder) {
    for (String tsfilePath : folder) {
      File dataRegionDataFolder =
          fsFactory.getFile(tsfilePath, databaseName + File.separator + dataRegionId);
      if (dataRegionDataFolder.exists()) {
        org.apache.iotdb.commons.utils.FileUtils.deleteDirectoryAndEmptyParent(
            dataRegionDataFolder);
      }
    }
  }

  /** Iterate each TsFile and try to lock and remove those out of TTL. */
  public synchronized void checkFilesTTL() {
    if (dataTTL == Long.MAX_VALUE) {
      logger.debug("{}: TTL not set, ignore the check", databaseName + "-" + dataRegionId);
      return;
    }
    long ttlLowerBound = DateTimeUtils.currentTime() - dataTTL;
    logger.debug(
        "{}: TTL removing files before {}",
        databaseName + "-" + dataRegionId,
        new Date(ttlLowerBound));

    // copy to avoid concurrent modification of deletion
    List<TsFileResource> seqFiles = new ArrayList<>(tsFileManager.getTsFileList(true));
    List<TsFileResource> unseqFiles = new ArrayList<>(tsFileManager.getTsFileList(false));

    for (TsFileResource tsFileResource : seqFiles) {
      checkFileTTL(tsFileResource, ttlLowerBound, true);
    }
    for (TsFileResource tsFileResource : unseqFiles) {
      checkFileTTL(tsFileResource, ttlLowerBound, false);
    }
  }

  private void checkFileTTL(TsFileResource resource, long ttlLowerBound, boolean isSeq) {
    if (!resource.isClosed() || !resource.isDeleted() && resource.stillLives(ttlLowerBound)) {
      return;
    }

    // ensure that the file is not used by any queries
    if (resource.tryWriteLock()) {
      try {
        // try to delete physical data file
        resource.remove();
        tsFileManager.remove(resource, isSeq);
        logger.info(
            "Removed a file {} before {} by ttl ({} {})",
            resource.getTsFilePath(),
            new Date(ttlLowerBound),
            dataTTL,
            config.getTimestampPrecision());
      } finally {
        resource.writeUnlock();
      }
    }
  }

  public void timedFlushSeqMemTable() {
    writeLock("timedFlushSeqMemTable");
    try {
      // only check sequence tsfiles' memtables
      List<TsFileProcessor> tsFileProcessors =
          new ArrayList<>(workSequenceTsFileProcessors.values());
      long timeLowerBound = System.currentTimeMillis() - config.getSeqMemtableFlushInterval();

      for (TsFileProcessor tsFileProcessor : tsFileProcessors) {
        if (tsFileProcessor.getWorkMemTableCreatedTime() < timeLowerBound) {
          logger.info(
              "Exceed sequence memtable flush interval, so flush working memtable of time partition {} in database {}[{}]",
              tsFileProcessor.getTimeRangeId(),
              databaseName,
              dataRegionId);
          fileFlushPolicy.apply(this, tsFileProcessor, tsFileProcessor.isSequence());
        }
      }
    } finally {
      writeUnlock();
    }
  }

  public void timedFlushUnseqMemTable() {
    writeLock("timedFlushUnseqMemTable");
    try {
      // only check unsequence tsfiles' memtables
      List<TsFileProcessor> tsFileProcessors =
          new ArrayList<>(workUnsequenceTsFileProcessors.values());
      long timeLowerBound = System.currentTimeMillis() - config.getUnseqMemtableFlushInterval();

      for (TsFileProcessor tsFileProcessor : tsFileProcessors) {
        if (tsFileProcessor.getWorkMemTableCreatedTime() < timeLowerBound) {
          logger.info(
              "Exceed unsequence memtable flush interval, so flush working memtable of time partition {} in database {}[{}]",
              tsFileProcessor.getTimeRangeId(),
              databaseName,
              dataRegionId);
          fileFlushPolicy.apply(this, tsFileProcessor, tsFileProcessor.isSequence());
        }
      }
    } finally {
      writeUnlock();
    }
  }

  /** This method will be blocked until all tsfile processors are closed. */
  public void syncCloseAllWorkingTsFileProcessors() {
    synchronized (closeStorageGroupCondition) {
      try {
        asyncCloseAllWorkingTsFileProcessors();
        long startTime = System.currentTimeMillis();
        while (!closingSequenceTsFileProcessor.isEmpty()
            || !closingUnSequenceTsFileProcessor.isEmpty()) {
          closeStorageGroupCondition.wait(60_000);
          if (System.currentTimeMillis() - startTime > 60_000) {
            logger.warn(
                "{} has spent {}s to wait for closing all TsFiles.",
                databaseName + "-" + this.dataRegionId,
                (System.currentTimeMillis() - startTime) / 1000);
          }
        }
      } catch (InterruptedException e) {
        logger.error(
            "CloseFileNodeCondition error occurs while waiting for closing the storage "
                + "group {}",
            databaseName + "-" + dataRegionId,
            e);
        Thread.currentThread().interrupt();
      }
    }
  }

  /** close all working tsfile processors */
  public void asyncCloseAllWorkingTsFileProcessors() {
    writeLock("asyncCloseAllWorkingTsFileProcessors");
    try {
      logger.info("async force close all files in database: {}", databaseName + "-" + dataRegionId);
      // to avoid concurrent modification problem, we need a new array list
      for (TsFileProcessor tsFileProcessor :
          new ArrayList<>(workSequenceTsFileProcessors.values())) {
        asyncCloseOneTsFileProcessor(true, tsFileProcessor);
      }
      // to avoid concurrent modification problem, we need a new array list
      for (TsFileProcessor tsFileProcessor :
          new ArrayList<>(workUnsequenceTsFileProcessors.values())) {
        asyncCloseOneTsFileProcessor(false, tsFileProcessor);
      }
    } finally {
      writeUnlock();
    }
  }

  /** force close all working tsfile processors */
  public void forceCloseAllWorkingTsFileProcessors() throws TsFileProcessorException {
    writeLock("forceCloseAllWorkingTsFileProcessors");
    try {
      logger.info("force close all processors in database: {}", databaseName + "-" + dataRegionId);
      // to avoid concurrent modification problem, we need a new array list
      for (TsFileProcessor tsFileProcessor :
          new ArrayList<>(workSequenceTsFileProcessors.values())) {
        tsFileProcessor.putMemTableBackAndClose();
      }
      // to avoid concurrent modification problem, we need a new array list
      for (TsFileProcessor tsFileProcessor :
          new ArrayList<>(workUnsequenceTsFileProcessors.values())) {
        tsFileProcessor.putMemTableBackAndClose();
      }
    } finally {
      writeUnlock();
    }
  }

  /** used for mpp */
  @Override
  public QueryDataSource query(
      List<PartialPath> pathList, String singleDeviceId, QueryContext context, Filter timeFilter)
      throws QueryProcessException {
    try {
      List<TsFileResource> seqResources =
          getFileResourceListForQuery(
              tsFileManager.getTsFileList(true),
              upgradeSeqFileList,
              pathList,
              singleDeviceId,
              context,
              timeFilter,
              true);
      List<TsFileResource> unseqResources =
          getFileResourceListForQuery(
              tsFileManager.getTsFileList(false),
              upgradeUnseqFileList,
              pathList,
              singleDeviceId,
              context,
              timeFilter,
              false);

      queryMetricsManager.recordQueryResourceNum(SEQUENCE_TSFILE, seqResources.size());
      queryMetricsManager.recordQueryResourceNum(UNSEQUENCE_TSFILE, unseqResources.size());

      QueryDataSource dataSource = new QueryDataSource(seqResources, unseqResources);
      dataSource.setDataTTL(dataTTL);
      return dataSource;
    } catch (MetadataException e) {
      throw new QueryProcessException(e);
    }
  }

  /** lock the read lock of the insert lock */
  @Override
  public void readLock() {
    // apply read lock for SG insert lock to prevent inconsistent with concurrently writing memtable
    insertLock.readLock().lock();
    // apply read lock for TsFileResource list
    tsFileManager.readLock();
  }

  /** unlock the read lock of insert lock */
  @Override
  public void readUnlock() {
    tsFileManager.readUnlock();
    insertLock.readLock().unlock();
  }

  /** lock the write lock of the insert lock */
  public void writeLock(String holder) {
    insertLock.writeLock().lock();
    insertWriteLockHolder = holder;
  }

  /** unlock the write lock of the insert lock */
  public void writeUnlock() {
    insertWriteLockHolder = "";
    insertLock.writeLock().unlock();
  }

  /**
   * @param tsFileResources includes sealed and unsealed tsfile resources
   * @return fill unsealed tsfile resources with memory data and ChunkMetadataList of data in disk
   */
  private List<TsFileResource> getFileResourceListForQuery(
      Collection<TsFileResource> tsFileResources,
      List<TsFileResource> upgradeTsFileResources,
      List<PartialPath> pathList,
      String singleDeviceId,
      QueryContext context,
      Filter timeFilter,
      boolean isSeq)
      throws MetadataException {

    if (context.isDebug()) {
      DEBUG_LOGGER.info(
          "Path: {}, get tsfile list: {} isSeq: {} timefilter: {}",
          pathList,
          tsFileResources,
          isSeq,
          (timeFilter == null ? "null" : timeFilter));
    }

    List<TsFileResource> tsfileResourcesForQuery = new ArrayList<>();

    long timeLowerBound =
        dataTTL != Long.MAX_VALUE ? DateTimeUtils.currentTime() - dataTTL : Long.MIN_VALUE;
    context.setQueryTimeLowerBound(timeLowerBound);

    // for upgrade files and old files must be closed
    for (TsFileResource tsFileResource : upgradeTsFileResources) {
      if (!tsFileResource.isSatisfied(
          singleDeviceId, timeFilter, isSeq, dataTTL, context.isDebug())) {
        continue;
      }
      closeQueryLock.readLock().lock();
      try {
        tsfileResourcesForQuery.add(tsFileResource);
      } finally {
        closeQueryLock.readLock().unlock();
      }
    }

    for (TsFileResource tsFileResource : tsFileResources) {
      if (!tsFileResource.isSatisfied(
          singleDeviceId, timeFilter, isSeq, dataTTL, context.isDebug())) {
        continue;
      }
      closeQueryLock.readLock().lock();
      try {
        if (tsFileResource.isClosed()) {
          tsfileResourcesForQuery.add(tsFileResource);
        } else {
          tsFileResource.getProcessor().query(pathList, context, tsfileResourcesForQuery);
        }
      } catch (IOException e) {
        throw new MetadataException(e);
      } finally {
        closeQueryLock.readLock().unlock();
      }
    }
    return tsfileResourcesForQuery;
  }

  /** Seperate tsfiles in TsFileManager to sealedList and unsealedList. */
  private void separateTsFile(
      List<TsFileResource> sealedResource, List<TsFileResource> unsealedResource) {
    tsFileManager
        .getTsFileList(true)
        .forEach(
            tsFileResource -> {
              if (tsFileResource.isClosed()) {
                sealedResource.add(tsFileResource);
              } else {
                unsealedResource.add(tsFileResource);
              }
            });
    tsFileManager
        .getTsFileList(false)
        .forEach(
            tsFileResource -> {
              if (tsFileResource.isClosed()) {
                sealedResource.add(tsFileResource);
              } else {
                unsealedResource.add(tsFileResource);
              }
            });
  }

  /**
   * @param pattern Must be a pattern start with a precise device path
   * @param startTime
   * @param endTime
   * @param searchIndex
   * @param timePartitionFilter
   * @throws IOException
   */
  public void deleteByDevice(
      PartialPath pattern,
      long startTime,
      long endTime,
      long searchIndex,
      TimePartitionFilter timePartitionFilter)
      throws IOException {
    // If there are still some old version tsfiles, the delete won't succeeded.
    if (upgradeFileCount.get() != 0) {
      throw new IOException(
          "Delete failed. " + "Please do not delete until the old files upgraded.");
    }
    if (SettleService.getINSTANCE().getFilesToBeSettledCount().get() != 0) {
      throw new IOException(
          "Delete failed. " + "Please do not delete until the old files settled.");
    }
    // TODO: how to avoid partial deletion?
    // FIXME: notice that if we may remove a SGProcessor out of memory, we need to close all opened
    // mod files in mergingModification, sequenceFileList, and unsequenceFileList
    writeLock("delete");

    // record files which are updated so that we can roll back them in case of exception
    List<ModificationFile> updatedModFiles = new ArrayList<>();
    boolean hasReleasedLock = false;

    try {

      Set<PartialPath> devicePaths = new HashSet<>(pattern.getDevicePathPattern());

      // delete Last cache record if necessary
      // todo implement more precise process
      DataNodeSchemaCache.getInstance().invalidateAll();

      // write log to impacted working TsFileProcessors
      List<WALFlushListener> walListeners =
          logDeletionInWAL(startTime, endTime, searchIndex, pattern, timePartitionFilter);

      for (WALFlushListener walFlushListener : walListeners) {
        if (walFlushListener.waitForResult() == WALFlushListener.Status.FAILURE) {
          logger.error("Fail to log delete to wal.", walFlushListener.getCause());
          throw walFlushListener.getCause();
        }
      }

      Deletion deletion = new Deletion(pattern, MERGE_MOD_START_VERSION_NUM, startTime, endTime);

      List<TsFileResource> sealedTsFileResource = new ArrayList<>();
      List<TsFileResource> unsealedTsFileResource = new ArrayList<>();
      separateTsFile(sealedTsFileResource, unsealedTsFileResource);

      deleteDataInFiles(
          unsealedTsFileResource, deletion, devicePaths, updatedModFiles, timePartitionFilter);
      writeUnlock();
      hasReleasedLock = true;

      deleteDataInFiles(
          sealedTsFileResource, deletion, devicePaths, updatedModFiles, timePartitionFilter);

    } catch (Exception e) {
      // roll back
      for (ModificationFile modFile : updatedModFiles) {
        modFile.abort();
        // remember to close mod file
        modFile.close();
      }
      throw new IOException(e);
    } finally {
      if (!hasReleasedLock) {
        writeUnlock();
      }
    }
  }

  private List<WALFlushListener> logDeletionInWAL(
      long startTime,
      long endTime,
      long searchIndex,
      PartialPath path,
      TimePartitionFilter timePartitionFilter) {
    long timePartitionStartId = StorageEngine.getTimePartition(startTime);
    long timePartitionEndId = StorageEngine.getTimePartition(endTime);
    List<WALFlushListener> walFlushListeners = new ArrayList<>();
    if (config.getWalMode() == WALMode.DISABLE) {
      return walFlushListeners;
    }
    DeleteDataNode deleteDataNode =
        new DeleteDataNode(new PlanNodeId(""), Collections.singletonList(path), startTime, endTime);
    deleteDataNode.setSearchIndex(searchIndex);
    for (Map.Entry<Long, TsFileProcessor> entry : workSequenceTsFileProcessors.entrySet()) {
      if (timePartitionStartId <= entry.getKey()
          && entry.getKey() <= timePartitionEndId
          && (timePartitionFilter == null
              || timePartitionFilter.satisfy(databaseName, entry.getKey()))) {
        WALFlushListener walFlushListener = entry.getValue().logDeleteDataNodeInWAL(deleteDataNode);
        walFlushListeners.add(walFlushListener);
      }
    }
    for (Map.Entry<Long, TsFileProcessor> entry : workUnsequenceTsFileProcessors.entrySet()) {
      if (timePartitionStartId <= entry.getKey()
          && entry.getKey() <= timePartitionEndId
          && (timePartitionFilter == null
              || timePartitionFilter.satisfy(databaseName, entry.getKey()))) {
        WALFlushListener walFlushListener = entry.getValue().logDeleteDataNodeInWAL(deleteDataNode);
        walFlushListeners.add(walFlushListener);
      }
    }
    return walFlushListeners;
  }

  private boolean canSkipDelete(
      TsFileResource tsFileResource,
      Set<PartialPath> devicePaths,
      long deleteStart,
      long deleteEnd,
      TimePartitionFilter timePartitionFilter) {
    if (timePartitionFilter != null
        && !timePartitionFilter.satisfy(databaseName, tsFileResource.getTimePartition())) {
      return true;
    }

    for (PartialPath device : devicePaths) {
      long deviceStartTime, deviceEndTime;
      if (device.hasWildcard()) {
        Pair<Long, Long> startAndEndTime = tsFileResource.getPossibleStartTimeAndEndTime(device);
        if (startAndEndTime == null) {
          continue;
        }
        deviceStartTime = startAndEndTime.getLeft();
        deviceEndTime = startAndEndTime.getRight();
      } else {
        String deviceId = device.getFullPath();
        if (!tsFileResource.mayContainsDevice(deviceId)) {
          // resource does not contain this device
          continue;
        }
        deviceStartTime = tsFileResource.getStartTime(deviceId);
        deviceEndTime = tsFileResource.getEndTime(deviceId);
      }

      if (!tsFileResource.isClosed() && deviceEndTime == Long.MIN_VALUE) {
        // unsealed seq file
        if (deleteEnd >= deviceStartTime) {
          return false;
        }
      } else {
        // sealed file or unsealed unseq file
        if (deleteEnd >= deviceStartTime && deleteStart <= deviceEndTime) {
          // time range of device has overlap with the deletion
          return false;
        }
      }
    }
    return true;
  }

  private void deleteDataInFiles(
      Collection<TsFileResource> tsFileResourceList,
      Deletion deletion,
      Set<PartialPath> devicePaths,
      List<ModificationFile> updatedModFiles,
      TimePartitionFilter timePartitionFilter)
      throws IOException {
    for (TsFileResource tsFileResource : tsFileResourceList) {
      if (canSkipDelete(
          tsFileResource,
          devicePaths,
          deletion.getStartTime(),
          deletion.getEndTime(),
          timePartitionFilter)) {
        continue;
      }

      if (tsFileResource.isClosed()) {
        // delete data in sealed file
        if (tsFileResource.isCompacting()) {
          // we have to set modification offset to MAX_VALUE, as the offset of source chunk may
          // change after compaction
          deletion.setFileOffset(Long.MAX_VALUE);
          // write deletion into compaction modification file
          tsFileResource.getCompactionModFile().write(deletion);
          // write deletion into modification file to enable query during compaction
          tsFileResource.getModFile().write(deletion);
          // remember to close mod file
          tsFileResource.getCompactionModFile().close();
          tsFileResource.getModFile().close();
        } else {
          deletion.setFileOffset(tsFileResource.getTsFileSize());
          // write deletion into modification file
          boolean modFileExists = tsFileResource.getModFile().exists();
          long originSize = tsFileResource.getModFile().getSize();
          tsFileResource.getModFile().write(deletion);
          // remember to close mod file
          tsFileResource.getModFile().close();
          if (!modFileExists) {
            TsFileMetricManager.getInstance().increaseModFileNum(1);
          }
          TsFileMetricManager.getInstance()
              .increaseModFileSize(tsFileResource.getModFile().getSize() - originSize);
        }
        logger.info(
            "[Deletion] Deletion with path:{}, time:{}-{} written into mods file:{}.",
            deletion.getPath(),
            deletion.getStartTime(),
            deletion.getEndTime(),
            tsFileResource.getModFile().getFilePath());
      } else {
        // delete data in memory of unsealed file
        tsFileResource.getProcessor().deleteDataInMemory(deletion, devicePaths);
      }

      for (ISyncManager syncManager :
          SyncService.getInstance().getOrCreateSyncManager(dataRegionId)) {
        syncManager.syncRealTimeDeletion(deletion);
      }

      // add a record in case of rollback
      updatedModFiles.add(tsFileResource.getModFile());
    }
  }

  private void unsequenceFlushCallback(
      TsFileProcessor processor, Map<String, Long> updateMap, long systemFlushTime) {
    TimePartitionManager.getInstance()
        .updateAfterFlushing(
            new DataRegionId(Integer.valueOf(dataRegionId)),
            processor.getTimeRangeId(),
            systemFlushTime,
            lastFlushTimeMap.getMemSize(processor.getTimeRangeId()),
            workSequenceTsFileProcessors.get(processor.getTimeRangeId()) != null);
  }

  private void sequenceFlushCallback(
      TsFileProcessor processor, Map<String, Long> updateMap, long systemFlushTime) {
    lastFlushTimeMap.updateLatestFlushTime(processor.getTimeRangeId(), updateMap);
    TimePartitionManager.getInstance()
        .updateAfterFlushing(
            new DataRegionId(Integer.valueOf(dataRegionId)),
            processor.getTimeRangeId(),
            systemFlushTime,
            lastFlushTimeMap.getMemSize(processor.getTimeRangeId()),
            workUnsequenceTsFileProcessors.get(processor.getTimeRangeId()) != null);
  }

  /** used for upgrading */
  public void updateNewlyFlushedPartitionLatestFlushedTimeForEachDevice(
      long partitionId, String deviceId, long time) {
    lastFlushTimeMap.updateNewlyFlushedPartitionLatestFlushedTimeForEachDevice(
        partitionId, deviceId, time);
  }

  /** put the memtable back to the MemTablePool and make the metadata in writer visible */
  // TODO please consider concurrency with query and insert method.
  private void closeUnsealedTsFileProcessorCallBack(TsFileProcessor tsFileProcessor)
      throws TsFileProcessorException {
    closeQueryLock.writeLock().lock();
    try {
      tsFileProcessor.close();
      if (tsFileProcessor.isEmpty()) {
        try {
          fsFactory.deleteIfExists(tsFileProcessor.getTsFileResource().getTsFile());
          tsFileManager.remove(tsFileProcessor.getTsFileResource(), tsFileProcessor.isSequence());
        } catch (IOException e) {
          logger.error(
              "Remove empty file {} error",
              tsFileProcessor.getTsFileResource().getTsFile().getAbsolutePath());
        }
      } else {
        tsFileResourceManager.registerSealedTsFileResource(tsFileProcessor.getTsFileResource());
      }
    } finally {
      closeQueryLock.writeLock().unlock();
    }
    // closingSequenceTsFileProcessor is a thread safety class.
    if (closingSequenceTsFileProcessor.contains(tsFileProcessor)) {
      closingSequenceTsFileProcessor.remove(tsFileProcessor);
    } else {
      closingUnSequenceTsFileProcessor.remove(tsFileProcessor);
    }
    synchronized (closeStorageGroupCondition) {
      closeStorageGroupCondition.notifyAll();
    }
    TsFileMetricManager.getInstance()
        .addFile(tsFileProcessor.getTsFileResource().getTsFileSize(), tsFileProcessor.isSequence());
    logger.info("signal closing database condition in {}", databaseName + "-" + dataRegionId);
  }

  protected void executeCompaction() {
    try {
      List<Long> timePartitions = new ArrayList<>(tsFileManager.getTimePartitions());
      // sort the time partition from largest to smallest
      timePartitions.sort(Comparator.reverseOrder());
      for (long timePartition : timePartitions) {
        CompactionScheduler.scheduleCompaction(tsFileManager, timePartition);
      }
    } catch (Throwable e) {
      logger.error("Meet error in compaction schedule.", e);
    }
  }

  /**
   * count all TsFiles in the database which need to be upgraded
   *
   * @return total num of the tsfiles which need to be upgraded in the database
   */
  public int countUpgradeFiles() {
    return upgradeFileCount.get();
  }

  /** upgrade all files belongs to this database */
  public void upgrade() {
    for (TsFileResource seqTsFileResource : upgradeSeqFileList) {
      seqTsFileResource.setUpgradeTsFileResourceCallBack(this::upgradeTsFileResourceCallBack);
      seqTsFileResource.doUpgrade();
    }
    for (TsFileResource unseqTsFileResource : upgradeUnseqFileList) {
      unseqTsFileResource.setUpgradeTsFileResourceCallBack(this::upgradeTsFileResourceCallBack);
      unseqTsFileResource.doUpgrade();
    }
  }

  private void upgradeTsFileResourceCallBack(TsFileResource tsFileResource) {
    List<TsFileResource> upgradedResources = tsFileResource.getUpgradedResources();
    for (TsFileResource resource : upgradedResources) {
      long partitionId = resource.getTimePartition();
      resource
          .getDevices()
          .forEach(
              device ->
                  updateNewlyFlushedPartitionLatestFlushedTimeForEachDevice(
                      partitionId, device, resource.getEndTime(device)));
    }
    upgradeFileCount.getAndAdd(-1);
    // load all upgraded resources in this sg to tsFileResourceManager
    if (upgradeFileCount.get() == 0) {
      writeLock("upgradeTsFileResourceCallBack");
      try {
        loadUpgradedResources(upgradeSeqFileList, true);
        loadUpgradedResources(upgradeUnseqFileList, false);
      } finally {
        writeUnlock();
      }
      // after upgrade complete, update partitionLatestFlushedTimeForEachDevice
      lastFlushTimeMap.applyNewlyFlushedTimeToFlushedTime();
    }
  }

  /**
   * After finishing settling tsfile, we need to do 2 things : (1) move the new tsfile to the
   * correct folder, including deleting its old mods file (2) update the relevant data of this old
   * tsFile in memory ,eg: FileSequenceReader, tsFileManager, cache, etc.
   */
  private void settleTsFileCallBack(
      TsFileResource oldTsFileResource, List<TsFileResource> newTsFileResources)
      throws WriteProcessException {
    oldTsFileResource.readUnlock();
    oldTsFileResource.writeLock();
    try {
      TsFileAndModSettleTool.moveNewTsFile(oldTsFileResource, newTsFileResources);
      if (TsFileAndModSettleTool.getInstance().recoverSettleFileMap.size() != 0) {
        TsFileAndModSettleTool.getInstance()
            .recoverSettleFileMap
            .remove(oldTsFileResource.getTsFile().getAbsolutePath());
      }
      // clear Cache , including chunk cache, timeseriesMetadata cache and bloom filter cache
      operateClearCache();

      // if old tsfile is being deleted in the process due to its all data's being deleted.
      if (!oldTsFileResource.getTsFile().exists()) {
        tsFileManager.remove(oldTsFileResource, oldTsFileResource.isSeq());
      }
      FileReaderManager.getInstance().closeFileAndRemoveReader(oldTsFileResource.getTsFilePath());
      oldTsFileResource.setSettleTsFileCallBack(null);
      SettleService.getINSTANCE().getFilesToBeSettledCount().addAndGet(-1);
    } catch (IOException e) {
      logger.error("Exception to move new tsfile in settling", e);
      throw new WriteProcessException(
          "Meet error when settling file: " + oldTsFileResource.getTsFile().getAbsolutePath(), e);
    } finally {
      oldTsFileResource.writeUnlock();
    }
  }

  public static void operateClearCache() {
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    BloomFilterCache.getInstance().clear();
  }

  private void loadUpgradedResources(List<TsFileResource> resources, boolean isseq) {
    if (resources.isEmpty()) {
      return;
    }
    for (TsFileResource resource : resources) {
      resource.writeLock();
      try {
        UpgradeUtils.moveUpgradedFiles(resource);
        tsFileManager.addAll(resource.getUpgradedResources(), isseq);
        // delete old TsFile and resource
        resource.delete();
        Files.deleteIfExists(
            fsFactory
                .getFile(resource.getTsFile().toPath() + ModificationFile.FILE_SUFFIX)
                .toPath());
        UpgradeLog.writeUpgradeLogFile(
            resource.getTsFile().getAbsolutePath() + "," + UpgradeCheckStatus.UPGRADE_SUCCESS);
      } catch (IOException e) {
        logger.error("Unable to load {}, caused by ", resource, e);
      } finally {
        resource.writeUnlock();
      }
    }
    // delete upgrade folder when it is empty
    if (resources.get(0).getTsFile().getParentFile().isDirectory()
        && resources.get(0).getTsFile().getParentFile().listFiles().length == 0) {
      try {
        Files.delete(resources.get(0).getTsFile().getParentFile().toPath());
      } catch (IOException e) {
        logger.error(
            "Delete upgrade folder {} failed, caused by ",
            resources.get(0).getTsFile().getParentFile(),
            e);
      }
    }
    resources.clear();
  }

  /** merge file under this database processor */
  public void compact() {
    writeLock("merge");
    try {
      executeCompaction();
    } finally {
      writeUnlock();
    }
  }

  private void resetLastCacheWhenLoadingTsFile() throws IllegalPathException {
    if (!IoTDBDescriptor.getInstance().getConfig().isLastCacheEnabled()) {
      return;
    }
    DataNodeSchemaCache.getInstance().invalidateAll();
  }

  /**
   * Load a new tsfile to database processor. Tne file may have overlap with other files.
   *
   * <p>that there has no file which is overlapping with the new file.
   *
   * <p>Firstly, determine the loading type of the file, whether it needs to be loaded in sequence
   * list or unsequence list.
   *
   * <p>Secondly, execute the loading process by the type.
   *
   * <p>Finally, update the latestTimeForEachDevice and partitionLatestFlushedTimeForEachDevice.
   *
   * @param newTsFileResource tsfile resource @UsedBy load external tsfile module
   * @param deleteOriginFile whether to delete origin tsfile
   */
  public void loadNewTsFile(TsFileResource newTsFileResource, boolean deleteOriginFile)
      throws LoadFileException {
    File tsfileToBeInserted = newTsFileResource.getTsFile();
    long newFilePartitionId = newTsFileResource.getTimePartitionWithCheck();
    writeLock("loadNewTsFile");
    try {
      List<TsFileResource> sequenceList =
          tsFileManager.getOrCreateSequenceListByTimePartition(newFilePartitionId);

      int insertPos = findInsertionPosition(newTsFileResource, sequenceList);
      LoadTsFileType tsFileType = getLoadingTsFileType(insertPos, sequenceList);
      String renameInfo =
          (tsFileType == LoadTsFileType.LOAD_SEQUENCE)
              ? IoTDBConstant.SEQUENCE_FLODER_NAME
              : IoTDBConstant.UNSEQUENCE_FLODER_NAME;
      newTsFileResource.setSeq(tsFileType == LoadTsFileType.LOAD_SEQUENCE);
      String newFileName =
          getLoadingTsFileName(tsFileType, insertPos, newTsFileResource, sequenceList);

      if (!newFileName.equals(tsfileToBeInserted.getName())) {
        logger.info(
            "TsFile {} must be renamed to {} for loading into the " + renameInfo + " list.",
            tsfileToBeInserted.getName(),
            newFileName);
        newTsFileResource.setFile(
            fsFactory.getFile(tsfileToBeInserted.getParentFile(), newFileName));
      }
      loadTsFileByType(
          tsFileType,
          tsfileToBeInserted,
          newTsFileResource,
          newFilePartitionId,
          insertPos,
          deleteOriginFile);
      TsFileMetricManager.getInstance()
          .addFile(
              newTsFileResource.getTsFile().length(), tsFileType == LoadTsFileType.LOAD_SEQUENCE);

      resetLastCacheWhenLoadingTsFile(); // update last cache
      updateLastFlushTime(newTsFileResource); // update last flush time
      long partitionNum = newTsFileResource.getTimePartition();
      updatePartitionFileVersion(partitionNum, newTsFileResource.getVersion());
      logger.info("TsFile {} is successfully loaded in {} list.", newFileName, renameInfo);
    } catch (DiskSpaceInsufficientException e) {
      logger.error(
          "Failed to append the tsfile {} to database processor {} because the disk space is insufficient.",
          tsfileToBeInserted.getAbsolutePath(),
          tsfileToBeInserted.getParentFile().getName());
      throw new LoadFileException(e);
    } catch (IllegalPathException e) {
      logger.error(
          "Failed to reset last cache when loading file {}", newTsFileResource.getTsFilePath());
      throw new LoadFileException(e);
    } finally {
      writeUnlock();
    }
  }

  /**
   * Set the version in "partition" to "version" if "version" is larger than the current version.
   */
  public void setPartitionFileVersionToMax(long partition, long version) {
    partitionMaxFileVersions.compute(
        partition, (prt, oldVer) -> computeMaxVersion(oldVer, version));
  }

  private long computeMaxVersion(Long oldVersion, Long newVersion) {
    if (oldVersion == null) {
      return newVersion;
    }
    return Math.max(oldVersion, newVersion);
  }

  private Long getTsFileResourceEstablishTime(TsFileResource tsFileResource) {
    String tsFileName = tsFileResource.getTsFile().getName();
    return Long.parseLong(tsFileName.split(FILE_NAME_SEPARATOR)[0]);
  }

  private LoadTsFileType getLoadingTsFileType(int insertPos, List<TsFileResource> sequenceList) {
    if (insertPos == POS_OVERLAP) {
      return LoadTsFileType.LOAD_UNSEQUENCE;
    }
    if (insertPos == sequenceList.size() - 1) {
      return LoadTsFileType.LOAD_SEQUENCE;
    }
    long preTime =
        (insertPos == -1) ? 0 : getTsFileResourceEstablishTime(sequenceList.get(insertPos));
    long subsequenceTime = getTsFileResourceEstablishTime(sequenceList.get(insertPos + 1));
    return preTime == subsequenceTime
        ? LoadTsFileType.LOAD_UNSEQUENCE
        : LoadTsFileType.LOAD_SEQUENCE;
  }

  /**
   * Find the position of "newTsFileResource" in the sequence files if it can be inserted into them.
   *
   * @return POS_ALREADY_EXIST(- 2) if some file has the same name as the one to be inserted
   *     POS_OVERLAP(-3) if some file overlaps the new file an insertion position i >= -1 if the new
   *     file can be inserted between [i, i+1]
   */
  private int findInsertionPosition(
      TsFileResource newTsFileResource, List<TsFileResource> sequenceList) {

    int insertPos = -1;

    // find the position where the new file should be inserted
    for (int i = 0; i < sequenceList.size(); i++) {
      TsFileResource localFile = sequenceList.get(i);

      if (!localFile.isClosed() && localFile.getProcessor() != null) {
        // we cannot compare two files by TsFileResource unless they are both closed
        syncCloseOneTsFileProcessor(true, localFile.getProcessor());
      }
      int fileComparison = compareTsFileDevices(newTsFileResource, localFile);
      switch (fileComparison) {
        case 0:
          // some devices are newer but some devices are older, the two files overlap in general
          return POS_OVERLAP;
        case -1:
          // all devices in localFile are newer than the new file, the new file can be
          // inserted before localFile
          return i - 1;
        default:
          // all devices in the local file are older than the new file, proceed to the next file
          insertPos = i;
      }
    }
    return insertPos;
  }

  /**
   * Compare each device in the two files to find the time relation of them.
   *
   * @return -1 if fileA is totally older than fileB (A < B) 0 if fileA is partially older than
   *     fileB and partially newer than fileB (A X B) 1 if fileA is totally newer than fileB (B < A)
   */
  private int compareTsFileDevices(TsFileResource fileA, TsFileResource fileB) {
    boolean hasPre = false, hasSubsequence = false;
    Set<String> fileADevices = fileA.getDevices();
    Set<String> fileBDevices = fileB.getDevices();
    for (String device : fileADevices) {
      if (!fileBDevices.contains(device)) {
        continue;
      }
      long startTimeA = fileA.getStartTime(device);
      long endTimeA = fileA.getEndTime(device);
      long startTimeB = fileB.getStartTime(device);
      long endTimeB = fileB.getEndTime(device);
      if (startTimeA > endTimeB) {
        // A's data of the device is later than to the B's data
        hasPre = true;
      } else if (startTimeB > endTimeA) {
        // A's data of the device is previous to the B's data
        hasSubsequence = true;
      } else {
        // the two files overlap in the device
        return 0;
      }
    }
    if (hasPre && hasSubsequence) {
      // some devices are newer but some devices are older, the two files overlap in general
      return 0;
    }
    if (!hasPre && hasSubsequence) {
      // all devices in B are newer than those in A
      return -1;
    }
    // all devices in B are older than those in A
    return 1;
  }

  /**
   * If the historical versions of a file is a sub-set of the given file's, (close and) remove it to
   * reduce unnecessary merge. Only used when the file sender and the receiver share the same file
   * close policy. Warning: DO NOT REMOVE
   */
  @SuppressWarnings("unused")
  public void removeFullyOverlapFiles(TsFileResource resource) {
    writeLock("removeFullyOverlapFiles");
    try {
      Iterator<TsFileResource> iterator = tsFileManager.getIterator(true);
      removeFullyOverlapFiles(resource, iterator, true);

      iterator = tsFileManager.getIterator(false);
      removeFullyOverlapFiles(resource, iterator, false);
    } finally {
      writeUnlock();
    }
  }

  private void removeFullyOverlapFiles(
      TsFileResource newTsFile, Iterator<TsFileResource> iterator, boolean isSeq) {
    while (iterator.hasNext()) {
      TsFileResource existingTsFile = iterator.next();
      if (newTsFile.isPlanRangeCovers(existingTsFile)
          && !newTsFile.getTsFile().equals(existingTsFile.getTsFile())
          && existingTsFile.tryWriteLock()) {
        logger.info(
            "{} is covered by {}: [{}, {}], [{}, {}], remove it",
            existingTsFile,
            newTsFile,
            existingTsFile.minPlanIndex,
            existingTsFile.maxPlanIndex,
            newTsFile.minPlanIndex,
            newTsFile.maxPlanIndex);
        // if we fail to lock the file, it means it is being queried or merged and we will not
        // wait until it is free, we will just leave it to the next merge
        try {
          removeFullyOverlapFile(existingTsFile, iterator, isSeq);
        } catch (Exception e) {
          logger.error(
              "Something gets wrong while removing FullyOverlapFiles: {}",
              existingTsFile.getTsFile().getAbsolutePath(),
              e);
        } finally {
          existingTsFile.writeUnlock();
        }
      }
    }
  }

  /**
   * remove the given tsFileResource. If the corresponding tsFileProcessor is in the working status,
   * close it before remove the related resource files. maybe time-consuming for closing a tsfile.
   */
  private void removeFullyOverlapFile(
      TsFileResource tsFileResource, Iterator<TsFileResource> iterator, boolean isSeq) {
    logger.info(
        "Removing a covered file {}, closed: {}", tsFileResource, tsFileResource.isClosed());
    if (!tsFileResource.isClosed()) {
      try {
        // also remove the TsFileProcessor if the overlapped file is not closed
        long timePartition = tsFileResource.getTimePartition();
        Map<Long, TsFileProcessor> fileProcessorMap =
            isSeq ? workSequenceTsFileProcessors : workUnsequenceTsFileProcessors;
        TsFileProcessor tsFileProcessor = fileProcessorMap.get(timePartition);
        if (tsFileProcessor != null && tsFileProcessor.getTsFileResource() == tsFileResource) {
          // have to take some time to close the tsFileProcessor
          tsFileProcessor.syncClose();
          fileProcessorMap.remove(timePartition);
        }
      } catch (Exception e) {
        logger.error("Cannot close {}", tsFileResource, e);
      }
    }
    tsFileManager.remove(tsFileResource, isSeq);
    iterator.remove();
    tsFileResource.remove();
  }

  /**
   * Get an appropriate filename to ensure the order between files. The tsfile is named after
   * ({systemTime}-{versionNum}-{in_space_compaction_num}-{cross_space_compaction_num}.tsfile).
   *
   * <p>The sorting rules for tsfile names @see {@link this#compareFileName}, we can restore the
   * list based on the file name and ensure the correctness of the order, so there are three cases.
   *
   * <p>1. The tsfile is to be inserted in the first place of the list. Timestamp can be set to half
   * of the timestamp value in the file name of the first tsfile in the list , and the version
   * number will be updated to the largest number in this time partition.
   *
   * <p>2. The tsfile is to be inserted in the last place of the list. The file name is generated by
   * the system according to the naming rules and returned.
   *
   * <p>3. This file is inserted between two files. The time stamp is the mean of the timestamps of
   * the two files, the version number will be updated to the largest number in this time partition.
   *
   * @param insertIndex the new file will be inserted between the files [insertIndex, insertIndex +
   *     1]
   * @return appropriate filename
   */
  private String getLoadingTsFileName(
      LoadTsFileType tsFileType,
      int insertIndex,
      TsFileResource newTsFileResource,
      List<TsFileResource> sequenceList) {
    long timePartitionId = newTsFileResource.getTimePartition();
    if (tsFileType == LoadTsFileType.LOAD_UNSEQUENCE || insertIndex == sequenceList.size() - 1) {
      return getNewTsFileName(
          System.currentTimeMillis(),
          getAndSetNewVersion(timePartitionId, newTsFileResource),
          0,
          0);
    }

    long preTime =
        (insertIndex == -1) ? 0 : getTsFileResourceEstablishTime(sequenceList.get(insertIndex));
    long subsequenceTime = getTsFileResourceEstablishTime(sequenceList.get(insertIndex + 1));
    long meanTime = preTime + ((subsequenceTime - preTime) >> 1);

    return getNewTsFileName(
        meanTime, getAndSetNewVersion(timePartitionId, newTsFileResource), 0, 0);
  }

  private long getAndSetNewVersion(long timePartitionId, TsFileResource tsFileResource) {
    long version = partitionMaxFileVersions.getOrDefault(timePartitionId, 0L) + 1;
    partitionMaxFileVersions.put(timePartitionId, version);
    tsFileResource.setVersion(version);
    return version;
  }

  /**
   * Update latest time in latestTimeForEachDevice and
   * partitionLatestFlushedTimeForEachDevice. @UsedBy sync module, load external tsfile module.
   */
  private void updateLastFlushTime(TsFileResource newTsFileResource) {
    for (String device : newTsFileResource.getDevices()) {
      long endTime = newTsFileResource.getEndTime(device);
      long timePartitionId = StorageEngine.getTimePartition(endTime);
      lastFlushTimeMap.updateFlushedTime(timePartitionId, device, endTime);
      lastFlushTimeMap.updateGlobalFlushedTime(device, endTime);
    }
  }

  /**
   * Execute the loading process by the type.
   *
   * @param type load type
   * @param tsFileResource tsfile resource to be loaded
   * @param filePartitionId the partition id of the new file
   * @param deleteOriginFile whether to delete the original file
   * @return load the file successfully @UsedBy sync module, load external tsfile module.
   */
  private boolean loadTsFileByType(
      LoadTsFileType type,
      File tsFileToLoad,
      TsFileResource tsFileResource,
      long filePartitionId,
      int insertPos,
      boolean deleteOriginFile)
      throws LoadFileException, DiskSpaceInsufficientException {
    File targetFile;
    switch (type) {
      case LOAD_UNSEQUENCE:
        targetFile =
            fsFactory.getFile(
                DirectoryManager.getInstance().getNextFolderForUnSequenceFile(),
                databaseName
                    + File.separatorChar
                    + dataRegionId
                    + File.separatorChar
                    + filePartitionId
                    + File.separator
                    + tsFileResource.getTsFile().getName());
        tsFileResource.setFile(targetFile);
        if (tsFileManager.contains(tsFileResource, false)) {
          logger.error("The file {} has already been loaded in unsequence list", tsFileResource);
          return false;
        }
        tsFileManager.add(tsFileResource, false);
        logger.info(
            "Load tsfile in unsequence list, move file from {} to {}",
            tsFileToLoad.getAbsolutePath(),
            targetFile.getAbsolutePath());
        break;
      case LOAD_SEQUENCE:
        targetFile =
            fsFactory.getFile(
                DirectoryManager.getInstance().getNextFolderForSequenceFile(),
                databaseName
                    + File.separatorChar
                    + dataRegionId
                    + File.separatorChar
                    + filePartitionId
                    + File.separator
                    + tsFileResource.getTsFile().getName());
        tsFileResource.setFile(targetFile);
        if (tsFileManager.contains(tsFileResource, true)) {
          logger.error("The file {} has already been loaded in sequence list", tsFileResource);
          return false;
        }
        if (insertPos == -1) {
          tsFileManager.insertToPartitionFileList(tsFileResource, filePartitionId, true, 0);
        } else {
          tsFileManager.insertToPartitionFileList(
              tsFileResource, filePartitionId, true, insertPos + 1);
        }
        logger.info(
            "Load tsfile in sequence list, move file from {} to {}",
            tsFileToLoad.getAbsolutePath(),
            targetFile.getAbsolutePath());
        break;
      default:
        throw new LoadFileException(String.format("Unsupported type of loading tsfile : %s", type));
    }

    // move file from sync dir to data dir
    if (!targetFile.getParentFile().exists()) {
      targetFile.getParentFile().mkdirs();
    }
    try {
      if (deleteOriginFile) {
        FileUtils.moveFile(tsFileToLoad, targetFile);
      } else {
        Files.copy(tsFileToLoad.toPath(), targetFile.toPath());
      }
    } catch (IOException e) {
      logger.error(
          "File renaming failed when loading tsfile. Origin: {}, Target: {}",
          tsFileToLoad.getAbsolutePath(),
          targetFile.getAbsolutePath(),
          e);
      throw new LoadFileException(
          String.format(
              "File renaming failed when loading tsfile. Origin: %s, Target: %s, because %s",
              tsFileToLoad.getAbsolutePath(), targetFile.getAbsolutePath(), e.getMessage()));
    }

    File resourceFileToLoad =
        fsFactory.getFile(tsFileToLoad.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX);
    File targetResourceFile =
        fsFactory.getFile(targetFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX);
    try {
      if (deleteOriginFile) {
        FileUtils.moveFile(resourceFileToLoad, targetResourceFile);
      } else {
        Files.copy(resourceFileToLoad.toPath(), targetResourceFile.toPath());
      }
    } catch (IOException e) {
      logger.error(
          "File renaming failed when loading .resource file. Origin: {}, Target: {}",
          resourceFileToLoad.getAbsolutePath(),
          targetResourceFile.getAbsolutePath(),
          e);
      throw new LoadFileException(
          String.format(
              "File renaming failed when loading .resource file. Origin: %s, Target: %s, because %s",
              resourceFileToLoad.getAbsolutePath(),
              targetResourceFile.getAbsolutePath(),
              e.getMessage()));
    }

    File modFileToLoad =
        fsFactory.getFile(tsFileToLoad.getAbsolutePath() + ModificationFile.FILE_SUFFIX);
    if (modFileToLoad.exists()) {
      // when successfully loaded, the filepath of the resource will be changed to the IoTDB data
      // dir, so we can add a suffix to find the old modification file.
      File targetModFile =
          fsFactory.getFile(targetFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX);
      try {
        Files.deleteIfExists(targetModFile.toPath());
      } catch (IOException e) {
        logger.warn("Cannot delete localModFile {}", targetModFile, e);
      }
      try {
        if (deleteOriginFile) {
          FileUtils.moveFile(modFileToLoad, targetModFile);
        } else {
          Files.copy(modFileToLoad.toPath(), targetModFile.toPath());
        }
      } catch (IOException e) {
        logger.error(
            "File renaming failed when loading .mod file. Origin: {}, Target: {}",
            modFileToLoad.getAbsolutePath(),
            targetModFile.getAbsolutePath(),
            e);
        throw new LoadFileException(
            String.format(
                "File renaming failed when loading .mod file. Origin: %s, Target: %s, because %s",
                modFileToLoad.getAbsolutePath(), targetModFile.getAbsolutePath(), e.getMessage()));
      } finally {
        // ModFile will be updated during the next call to `getModFile`
        tsFileResource.setModFile(null);
      }
    }

    updatePartitionFileVersion(filePartitionId, tsFileResource.getVersion());
    return true;
  }

  /**
   * Delete tsfile if it exists.
   *
   * <p>Firstly, remove the TsFileResource from sequenceFileList/unSequenceFileList.
   *
   * <p>Secondly, delete the tsfile and .resource file.
   *
   * @param tsfieToBeDeleted tsfile to be deleted
   * @return whether the file to be deleted exists. @UsedBy sync module, load external tsfile
   *     module.
   */
  public boolean deleteTsfile(File tsfieToBeDeleted) {
    writeLock("deleteTsfile");
    TsFileResource tsFileResourceToBeDeleted = null;
    try {
      Iterator<TsFileResource> sequenceIterator = tsFileManager.getIterator(true);
      while (sequenceIterator.hasNext()) {
        TsFileResource sequenceResource = sequenceIterator.next();
        if (sequenceResource.getTsFile().getName().equals(tsfieToBeDeleted.getName())) {
          tsFileResourceToBeDeleted = sequenceResource;
          tsFileManager.remove(tsFileResourceToBeDeleted, true);
          break;
        }
      }
      if (tsFileResourceToBeDeleted == null) {
        Iterator<TsFileResource> unsequenceIterator = tsFileManager.getIterator(false);
        while (unsequenceIterator.hasNext()) {
          TsFileResource unsequenceResource = unsequenceIterator.next();
          if (unsequenceResource.getTsFile().getName().equals(tsfieToBeDeleted.getName())) {
            tsFileResourceToBeDeleted = unsequenceResource;
            tsFileManager.remove(tsFileResourceToBeDeleted, false);
            break;
          }
        }
      }
    } finally {
      writeUnlock();
    }
    if (tsFileResourceToBeDeleted == null) {
      return false;
    }
    tsFileResourceToBeDeleted.writeLock();
    try {
      tsFileResourceToBeDeleted.remove();
      logger.info("Delete tsfile {} successfully.", tsFileResourceToBeDeleted.getTsFile());
    } finally {
      tsFileResourceToBeDeleted.writeUnlock();
    }
    return true;
  }

  /**
   * get all working sequence tsfile processors
   *
   * @return all working sequence tsfile processors
   */
  public Collection<TsFileProcessor> getWorkSequenceTsFileProcessors() {
    return workSequenceTsFileProcessors.values();
  }

  /**
   * Unload tsfile and move it to the target directory if it exists.
   *
   * <p>Firstly, unload the TsFileResource from sequenceFileList/unSequenceFileList.
   *
   * <p>Secondly, move the tsfile and .resource file to the target directory.
   *
   * @param fileToBeUnloaded tsfile to be unloaded
   * @return whether the file to be unloaded exists. @UsedBy load external tsfile module.
   */
  public boolean unloadTsfile(File fileToBeUnloaded, File targetDir) {
    writeLock("unloadTsfile");
    TsFileResource tsFileResourceToBeMoved = null;
    try {
      Iterator<TsFileResource> sequenceIterator = tsFileManager.getIterator(true);
      while (sequenceIterator.hasNext()) {
        TsFileResource sequenceResource = sequenceIterator.next();
        if (sequenceResource.getTsFile().getName().equals(fileToBeUnloaded.getName())) {
          tsFileResourceToBeMoved = sequenceResource;
          tsFileManager.remove(tsFileResourceToBeMoved, true);
          break;
        }
      }
      if (tsFileResourceToBeMoved == null) {
        Iterator<TsFileResource> unsequenceIterator = tsFileManager.getIterator(false);
        while (unsequenceIterator.hasNext()) {
          TsFileResource unsequenceResource = unsequenceIterator.next();
          if (unsequenceResource.getTsFile().getName().equals(fileToBeUnloaded.getName())) {
            tsFileResourceToBeMoved = unsequenceResource;
            tsFileManager.remove(tsFileResourceToBeMoved, false);
            break;
          }
        }
      }
    } finally {
      writeUnlock();
    }
    if (tsFileResourceToBeMoved == null) {
      return false;
    }
    tsFileResourceToBeMoved.writeLock();
    try {
      tsFileResourceToBeMoved.moveTo(targetDir);
      logger.info(
          "Move tsfile {} to target dir {} successfully.",
          tsFileResourceToBeMoved.getTsFile(),
          targetDir.getPath());
    } finally {
      tsFileResourceToBeMoved.writeUnlock();
    }
    return true;
  }

  /**
   * get all working unsequence tsfile processors
   *
   * @return all working unsequence tsfile processors
   */
  public Collection<TsFileProcessor> getWorkUnsequenceTsFileProcessors() {
    return workUnsequenceTsFileProcessors.values();
  }

  public void setDataTTLWithTimePrecisionCheck(long dataTTL) {
    if (dataTTL != Long.MAX_VALUE) {
      dataTTL =
          DateTimeUtils.convertMilliTimeWithPrecision(
              dataTTL, IoTDBDescriptor.getInstance().getConfig().getTimestampPrecision());
    }
    this.dataTTL = dataTTL;
  }

  public void setDataTTL(long dataTTL) {
    this.dataTTL = dataTTL;
  }

  public List<TsFileResource> getSequenceFileList() {
    return tsFileManager.getTsFileList(true);
  }

  public List<TsFileResource> getUnSequenceFileList() {
    return tsFileManager.getTsFileList(false);
  }

  public String getDataRegionId() {
    return dataRegionId;
  }

  /**
   * Get the storageGroupPath with dataRegionId.
   *
   * @return data region path, like root.sg1/0
   */
  public String getStorageGroupPath() {
    return databaseName + File.separator + dataRegionId;
  }

  /**
   * Check if the data of "tsFileResource" all exist locally by comparing planIndexes in the
   * partition of "partitionNumber". This is available only when the IoTDB instances which generated
   * "tsFileResource" have the same plan indexes as the local one.
   *
   * @return true if any file contains plans with indexes no less than the max plan index of
   *     "tsFileResource", otherwise false.
   */
  public boolean isFileAlreadyExist(TsFileResource tsFileResource, long partitionNum) {
    // examine working processor first as they have the largest plan index
    return isFileAlreadyExistInWorking(
            tsFileResource, partitionNum, getWorkSequenceTsFileProcessors())
        || isFileAlreadyExistInWorking(
            tsFileResource, partitionNum, getWorkUnsequenceTsFileProcessors())
        || isFileAlreadyExistInClosed(tsFileResource, partitionNum, getSequenceFileList())
        || isFileAlreadyExistInClosed(tsFileResource, partitionNum, getUnSequenceFileList());
  }

  private boolean isFileAlreadyExistInClosed(
      TsFileResource tsFileResource, long partitionNum, Collection<TsFileResource> existingFiles) {
    for (TsFileResource resource : existingFiles) {
      if (resource.getTimePartition() == partitionNum
          && resource.getMaxPlanIndex() > tsFileResource.getMaxPlanIndex()) {
        logger.info(
            "{} is covered by a closed file {}: [{}, {}] [{}, {}]",
            tsFileResource,
            resource,
            tsFileResource.minPlanIndex,
            tsFileResource.maxPlanIndex,
            resource.minPlanIndex,
            resource.maxPlanIndex);
        return true;
      }
    }
    return false;
  }

  private boolean isFileAlreadyExistInWorking(
      TsFileResource tsFileResource,
      long partitionNum,
      Collection<TsFileProcessor> workingProcessors) {
    for (TsFileProcessor workingProcesssor : workingProcessors) {
      if (workingProcesssor.getTimeRangeId() == partitionNum) {
        TsFileResource workResource = workingProcesssor.getTsFileResource();
        boolean isCovered = workResource.getMaxPlanIndex() > tsFileResource.getMaxPlanIndex();
        if (isCovered) {
          logger.info(
              "{} is covered by a working file {}: [{}, {}] [{}, {}]",
              tsFileResource,
              workResource,
              tsFileResource.minPlanIndex,
              tsFileResource.maxPlanIndex,
              workResource.minPlanIndex,
              workResource.maxPlanIndex);
        }
        return isCovered;
      }
    }
    return false;
  }

  /** remove all partitions that satisfy a filter. */
  public void removePartitions(TimePartitionFilter filter) {
    // this requires blocking all other activities
    writeLock("removePartitions");
    try {
      // abort ongoing compaction
      abortCompaction();
      try {
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        // Wait two seconds for the compaction thread to terminate
      }
      // close all working files that should be removed
      removePartitions(filter, workSequenceTsFileProcessors.entrySet(), true);
      removePartitions(filter, workUnsequenceTsFileProcessors.entrySet(), false);

      // remove data files
      removePartitions(filter, tsFileManager.getIterator(true), true);
      removePartitions(filter, tsFileManager.getIterator(false), false);

    } finally {
      writeUnlock();
    }
  }

  public void abortCompaction() {
    tsFileManager.setAllowCompaction(false);
    List<AbstractCompactionTask> runningTasks =
        CompactionTaskManager.getInstance().abortCompaction(databaseName + "-" + dataRegionId);
    while (CompactionTaskManager.getInstance().isAnyTaskInListStillRunning(runningTasks)) {
      try {
        TimeUnit.MILLISECONDS.sleep(10);
      } catch (InterruptedException e) {
        logger.error("Thread get interrupted when waiting compaction to finish", e);
        break;
      }
    }
  }

  // may remove the processorEntrys
  private void removePartitions(
      TimePartitionFilter filter,
      Set<Entry<Long, TsFileProcessor>> processorEntrys,
      boolean sequence) {
    for (Iterator<Entry<Long, TsFileProcessor>> iterator = processorEntrys.iterator();
        iterator.hasNext(); ) {
      Entry<Long, TsFileProcessor> longTsFileProcessorEntry = iterator.next();
      long partitionId = longTsFileProcessorEntry.getKey();
      lastFlushTimeMap.removePartition(partitionId);
      TimePartitionManager.getInstance()
          .removePartition(new DataRegionId(Integer.valueOf(dataRegionId)), partitionId);
      TsFileProcessor processor = longTsFileProcessorEntry.getValue();
      if (filter.satisfy(databaseName, partitionId)) {
        processor.syncClose();
        iterator.remove();
        processor.getTsFileResource().remove();
        tsFileManager.remove(processor.getTsFileResource(), sequence);
        logger.debug(
            "{} is removed during deleting partitions",
            processor.getTsFileResource().getTsFilePath());
      }
    }
  }

  // may remove the iterator's data
  private void removePartitions(
      TimePartitionFilter filter, Iterator<TsFileResource> iterator, boolean sequence) {
    while (iterator.hasNext()) {
      TsFileResource tsFileResource = iterator.next();
      if (filter.satisfy(databaseName, tsFileResource.getTimePartition())) {
        tsFileResource.remove();
        tsFileManager.remove(tsFileResource, sequence);
        lastFlushTimeMap.removePartition(tsFileResource.getTimePartition());
        TimePartitionManager.getInstance()
            .removePartition(
                new DataRegionId(Integer.valueOf(dataRegionId)), tsFileResource.getTimePartition());
        logger.debug("{} is removed during deleting partitions", tsFileResource.getTsFilePath());
      }
    }
  }

  public TsFileManager getTsFileResourceManager() {
    return tsFileManager;
  }

  /**
   * insert batch of rows belongs to one device
   *
   * @param insertRowsOfOneDeviceNode batch of rows belongs to one device
   */
  public void insert(InsertRowsOfOneDeviceNode insertRowsOfOneDeviceNode)
      throws WriteProcessException, BatchProcessException {
    if (enableMemControl) {
      StorageEngine.blockInsertionIfReject(null);
    }
    long startTime = System.nanoTime();
    writeLock("InsertRowsOfOneDevice");
    PerformanceOverviewMetricsManager.recordScheduleLockCost(System.nanoTime() - startTime);
    try {
      if (deleted) {
        return;
      }
      boolean isSequence = false;
      for (int i = 0; i < insertRowsOfOneDeviceNode.getInsertRowNodeList().size(); i++) {
        InsertRowNode insertRowNode = insertRowsOfOneDeviceNode.getInsertRowNodeList().get(i);
        if (!isAlive(insertRowNode.getTime())) {
          // we do not need to write these part of data, as they can not be queried
          // or the sub-plan has already been executed, we are retrying other sub-plans
          insertRowsOfOneDeviceNode
              .getResults()
              .put(
                  i,
                  RpcUtils.getStatus(
                      TSStatusCode.OUT_OF_TTL.getStatusCode(),
                      String.format(
                          "Insertion time [%s] is less than ttl time bound [%s]",
                          DateTimeUtils.convertMillsecondToZonedDateTime(insertRowNode.getTime()),
                          DateTimeUtils.convertMillsecondToZonedDateTime(
                              DateTimeUtils.currentTime() - dataTTL))));
          continue;
        }
        // init map
        long timePartitionId = StorageEngine.getTimePartition(insertRowNode.getTime());

        if (!lastFlushTimeMap.checkAndCreateFlushedTimePartition(timePartitionId)) {
          TimePartitionManager.getInstance()
              .registerTimePartitionInfo(
                  new TimePartitionInfo(
                      new DataRegionId(Integer.valueOf(dataRegionId)),
                      timePartitionId,
                      true,
                      Long.MAX_VALUE,
                      0,
                      tsFileManager.isLatestTimePartition(timePartitionId)));
        }

        // as the plans have been ordered, and we have get the write lock,
        // So, if a plan is sequenced, then all the rest plans are sequenced.
        //
        if (!isSequence) {
          isSequence =
              insertRowNode.getTime()
                  > lastFlushTimeMap.getFlushedTime(
                      timePartitionId, insertRowNode.getDevicePath().getFullPath());
        }
        // is unsequence and user set config to discard out of order data
        if (!isSequence
            && IoTDBDescriptor.getInstance().getConfig().isEnableDiscardOutOfOrderData()) {
          return;
        }

        // insert to sequence or unSequence file
        try {
          insertToTsFileProcessor(insertRowNode, isSequence, timePartitionId);
        } catch (WriteProcessException e) {
          insertRowsOfOneDeviceNode
              .getResults()
              .put(i, RpcUtils.getStatus(e.getErrorCode(), e.getMessage()));
        }
      }
    } finally {
      writeUnlock();
    }
    if (!insertRowsOfOneDeviceNode.getResults().isEmpty()) {
      throw new BatchProcessException("Partial failed inserting rows of one device");
    }
  }

  /**
   * insert batch of rows belongs to multiple devices
   *
   * @param insertRowsNode batch of rows belongs to multiple devices
   */
  public void insert(InsertRowsNode insertRowsNode) throws BatchProcessException {
    for (int i = 0; i < insertRowsNode.getInsertRowNodeList().size(); i++) {
      InsertRowNode insertRowNode = insertRowsNode.getInsertRowNodeList().get(i);
      try {
        insert(insertRowNode);
      } catch (WriteProcessException e) {
        insertRowsNode.getResults().put(i, RpcUtils.getStatus(e.getErrorCode(), e.getMessage()));
      }
    }

    if (!insertRowsNode.getResults().isEmpty()) {
      throw new BatchProcessException("Partial failed inserting rows");
    }
  }

  /**
   * insert batch of tablets belongs to multiple devices
   *
   * @param insertMultiTabletsNode batch of tablets belongs to multiple devices
   */
  public void insertTablets(InsertMultiTabletsNode insertMultiTabletsNode)
      throws BatchProcessException {
    for (int i = 0; i < insertMultiTabletsNode.getInsertTabletNodeList().size(); i++) {
      InsertTabletNode insertTabletNode = insertMultiTabletsNode.getInsertTabletNodeList().get(i);
      try {
        insertTablet(insertTabletNode);
      } catch (WriteProcessException | BatchProcessException e) {
        insertMultiTabletsNode
            .getResults()
            .put(i, RpcUtils.getStatus(e.getErrorCode(), e.getMessage()));
      }
    }

    if (!insertMultiTabletsNode.getResults().isEmpty()) {
      throw new BatchProcessException("Partial failed inserting multi tablets");
    }
  }

  @TestOnly
  public long getPartitionMaxFileVersions(long partitionId) {
    return partitionMaxFileVersions.getOrDefault(partitionId, 0L);
  }

  public void addSettleFilesToList(
      List<TsFileResource> seqResourcesToBeSettled,
      List<TsFileResource> unseqResourcesToBeSettled,
      List<String> tsFilePaths) {
    if (tsFilePaths.isEmpty()) {
      for (TsFileResource resource : tsFileManager.getTsFileList(true)) {
        if (!resource.isClosed()) {
          continue;
        }
        resource.setSettleTsFileCallBack(this::settleTsFileCallBack);
        seqResourcesToBeSettled.add(resource);
      }
      for (TsFileResource resource : tsFileManager.getTsFileList(false)) {
        if (!resource.isClosed()) {
          continue;
        }
        resource.setSettleTsFileCallBack(this::settleTsFileCallBack);
        unseqResourcesToBeSettled.add(resource);
      }
    } else {
      for (String tsFilePath : tsFilePaths) {
        File fileToBeSettled = new File(tsFilePath);
        if ("sequence"
            .equals(
                fileToBeSettled
                    .getParentFile()
                    .getParentFile()
                    .getParentFile()
                    .getParentFile()
                    .getName())) {
          for (TsFileResource resource : tsFileManager.getTsFileList(true)) {
            if (resource.getTsFile().getAbsolutePath().equals(tsFilePath)) {
              resource.setSettleTsFileCallBack(this::settleTsFileCallBack);
              seqResourcesToBeSettled.add(resource);
              break;
            }
          }
        } else {
          for (TsFileResource resource : tsFileManager.getTsFileList(false)) {
            if (resource.getTsFile().getAbsolutePath().equals(tsFilePath)) {
              unseqResourcesToBeSettled.add(resource);
              break;
            }
          }
        }
      }
    }
  }

  /**
   * Used to collect history TsFiles(i.e. the tsfile whose memtable == null).
   *
   * @param syncManager ISyncManager which invokes to collect history TsFile
   * @param dataStartTime only collect history TsFiles which contains the data after the
   *     dataStartTime
   * @return A list, which contains TsFile path
   */
  public List<File> collectHistoryTsFileForSync(ISyncManager syncManager, long dataStartTime) {
    writeLock("Collect data for sync");
    try {
      return tsFileManager.collectHistoryTsFileForSync(syncManager, dataStartTime);
    } finally {
      writeUnlock();
    }
  }

  public void setCustomCloseFileListeners(List<CloseFileListener> customCloseFileListeners) {
    this.customCloseFileListeners = customCloseFileListeners;
  }

  public void setCustomFlushListeners(List<FlushListener> customFlushListeners) {
    this.customFlushListeners = customFlushListeners;
  }

  public void setAllowCompaction(boolean allowCompaction) {
    this.tsFileManager.setAllowCompaction(allowCompaction);
  }

  private enum LoadTsFileType {
    LOAD_SEQUENCE,
    LOAD_UNSEQUENCE
  }

  @FunctionalInterface
  public interface CloseTsFileCallBack {

    void call(TsFileProcessor caller) throws TsFileProcessorException, IOException;
  }

  @FunctionalInterface
  public interface UpdateEndTimeCallBack {

    void call(TsFileProcessor caller, Map<String, Long> updateMap, long systemFlushTime);
  }

  @FunctionalInterface
  public interface UpgradeTsFileResourceCallBack {

    void call(TsFileResource caller);
  }

  @FunctionalInterface
  public interface CompactionRecoverCallBack {
    void call();
  }

  @FunctionalInterface
  public interface TimePartitionFilter {

    boolean satisfy(String storageGroupName, long timePartitionId);
  }

  @FunctionalInterface
  public interface SettleTsFileCallBack {

    void call(TsFileResource oldTsFileResource, List<TsFileResource> newTsFileResources)
        throws WriteProcessException;
  }

  public List<Long> getTimePartitions() {
    return new ArrayList<>(partitionMaxFileVersions.keySet());
  }

  public Long getLatestTimePartition() {
    return partitionMaxFileVersions.keySet().stream().max(Long::compareTo).orElse(0L);
  }

  public String getInsertWriteLockHolder() {
    return insertWriteLockHolder;
  }

  public ScheduledExecutorService getTimedCompactionScheduleTask() {
    return timedCompactionScheduleTask;
  }

  public IDTable getIdTable() {
    return idTable;
  }

  /** This method could only be used in iot consensus */
  public IWALNode getWALNode() {
    if (!config.getDataRegionConsensusProtocolClass().equals(ConsensusFactory.IOT_CONSENSUS)) {
      throw new UnsupportedOperationException();
    }
    // identifier should be same with getTsFileProcessor method
    return WALManager.getInstance()
        .applyForWALNode(databaseName + FILE_NAME_SEPARATOR + dataRegionId);
  }

  /** Wait for this data region successfully deleted */
  public void waitForDeleted() {
    writeLock("waitForDeleted");
    try {
      if (!deleted) {
        deletedCondition.await();
      }
    } catch (InterruptedException e) {
      logger.error("Interrupted When waiting for data region deleted.");
      Thread.currentThread().interrupt();
    } finally {
      writeUnlock();
    }
  }

  /** Release all threads waiting for this data region successfully deleted */
  public void markDeleted() {
    writeLock("markDeleted");
    try {
      deleted = true;
      deletedCondition.signalAll();
    } finally {
      writeUnlock();
    }
  }

  public void releaseFlushTimeMap(long timePartitionId) {
    lastFlushTimeMap.removePartition(timePartitionId);
  }

  public long getMemCost() {
    return dataRegionInfo.getMemCost();
  }

  @Override
  public long getDataTTL() {
    return dataTTL;
  }

  @TestOnly
  public ILastFlushTimeMap getLastFlushTimeMap() {
    return lastFlushTimeMap;
  }

  @TestOnly
  public TsFileManager getTsFileManager() {
    return tsFileManager;
  }
}
