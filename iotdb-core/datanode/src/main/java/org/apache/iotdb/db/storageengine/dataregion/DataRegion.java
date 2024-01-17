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

package org.apache.iotdb.db.storageengine.dataregion;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.PerformanceOverviewMetrics;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.BatchProcessException;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.exception.LoadFileException;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.WriteProcessRejectException;
import org.apache.iotdb.db.exception.query.OutOfTTLException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.quota.ExceedQuotaException;
import org.apache.iotdb.db.pipe.extractor.realtime.listener.PipeInsertionDataNodeListener;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.queryengine.metric.QueryResourceMetricSet;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeSchemaCache;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsOfOneDeviceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.service.SettleService;
import org.apache.iotdb.db.service.metrics.CompactionMetrics;
import org.apache.iotdb.db.service.metrics.FileMetrics;
import org.apache.iotdb.db.service.metrics.WritingMetrics;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.buffer.BloomFilterCache;
import org.apache.iotdb.db.storageengine.buffer.ChunkCache;
import org.apache.iotdb.db.storageengine.buffer.TimeSeriesMetadataCache;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.recover.CompactionRecoverManager;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.AbstractCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduleSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduler;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.flush.CloseFileListener;
import org.apache.iotdb.db.storageengine.dataregion.flush.FlushListener;
import org.apache.iotdb.db.storageengine.dataregion.flush.FlushStatus;
import org.apache.iotdb.db.storageengine.dataregion.flush.TsFileFlushPolicy;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IMemTable;
import org.apache.iotdb.db.storageengine.dataregion.memtable.TsFileProcessor;
import org.apache.iotdb.db.storageengine.dataregion.memtable.TsFileProcessorInfo;
import org.apache.iotdb.db.storageengine.dataregion.modification.Deletion;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.VersionController;
import org.apache.iotdb.db.storageengine.dataregion.utils.validate.TsFileValidator;
import org.apache.iotdb.db.storageengine.dataregion.wal.WALManager;
import org.apache.iotdb.db.storageengine.dataregion.wal.node.IWALNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.recover.WALRecoverManager;
import org.apache.iotdb.db.storageengine.dataregion.wal.recover.file.SealedTsFileRecoverPerformer;
import org.apache.iotdb.db.storageengine.dataregion.wal.recover.file.UnsealedTsFileRecoverPerformer;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALMode;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.listener.WALFlushListener;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.listener.WALRecoverListener;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;
import org.apache.iotdb.db.storageengine.rescon.memory.TimePartitionInfo;
import org.apache.iotdb.db.storageengine.rescon.memory.TimePartitionManager;
import org.apache.iotdb.db.storageengine.rescon.memory.TsFileResourceManager;
import org.apache.iotdb.db.storageengine.rescon.quotas.DataNodeSpaceQuotaManager;
import org.apache.iotdb.db.tools.settle.TsFileAndModSettleTool;
import org.apache.iotdb.db.utils.CopyOnReadLinkedList;
import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.FSType;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.FSUtils;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.iotdb.commons.conf.IoTDBConstant.FILE_NAME_SEPARATOR;
import static org.apache.iotdb.db.queryengine.metric.QueryResourceMetricSet.SEQUENCE_TSFILE;
import static org.apache.iotdb.db.queryengine.metric.QueryResourceMetricSet.UNSEQUENCE_TSFILE;
import static org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource.BROKEN_SUFFIX;
import static org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource.RESOURCE_SUFFIX;
import static org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource.TEMP_SUFFIX;
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
   * avoid some tsfileResource is changed (e.g., from unsealed to sealed) when a read is executed.
   */
  private final ReadWriteLock closeQueryLock = new ReentrantReadWriteLock();
  /** time partition id in the database -> tsFileProcessor for this time partition. */
  private final TreeMap<Long, TsFileProcessor> workSequenceTsFileProcessors = new TreeMap<>();
  /** time partition id in the database -> tsFileProcessor for this time partition. */
  private final TreeMap<Long, TsFileProcessor> workUnsequenceTsFileProcessors = new TreeMap<>();

  /** sequence tsfile processors which are closing. */
  private final CopyOnReadLinkedList<TsFileProcessor> closingSequenceTsFileProcessor =
      new CopyOnReadLinkedList<>();

  /** unsequence tsfile processors which are closing. */
  private final CopyOnReadLinkedList<TsFileProcessor> closingUnSequenceTsFileProcessor =
      new CopyOnReadLinkedList<>();

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
   * record the insertWriteLock in SG is being hold by which method, it will be empty string if no
   * one holds the insertWriteLock.
   */
  private String insertWriteLockHolder = "";

  private ScheduledExecutorService timedCompactionScheduleTask;

  public static final long COMPACTION_TASK_SUBMIT_DELAY = 20L * 1000L;

  private static final QueryResourceMetricSet QUERY_RESOURCE_METRIC_SET =
      QueryResourceMetricSet.getInstance();

  private static final PerformanceOverviewMetrics PERFORMANCE_OVERVIEW_METRICS =
      PerformanceOverviewMetrics.getInstance();

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

    lastFlushTimeMap = new HashLastFlushTimeMap(tsFileManager);

    // recover tsfiles unless consensus protocol is ratis and storage storageengine is not ready
    if (config.isClusterMode()
        && config.getDataRegionConsensusProtocolClass().equals(ConsensusFactory.RATIS_CONSENSUS)
        && !StorageEngine.getInstance().isAllSgReady()) {
      logger.debug(
          "Skip recovering data region {}[{}] when consensus protocol is ratis and storage storageengine is not ready.",
          databaseName,
          dataRegionId);
      for (String fileFolder : TierManager.getInstance().getAllFilesFolders()) {
        File dataRegionFolder =
            fsFactory.getFile(fileFolder, databaseName + File.separator + dataRegionId);
        try {
          fsFactory.deleteDirectory(dataRegionFolder.getPath());
        } catch (IOException e) {
          logger.error(
              "Exception occurs when deleting data region folder for {}-{}",
              databaseName,
              dataRegionId,
              e);
        }
        if (FSUtils.getFSType(dataRegionFolder) == FSType.LOCAL) {
          dataRegionFolder.mkdirs();
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
  @SuppressWarnings({"squid:S3776", "squid:S6541"}) // Suppress high Cognitive Complexity warning
  private void recover() throws DataRegionException {
    try {
      recoverCompaction();
    } catch (Exception e) {
      // signal wal recover manager to recover this region's files
      WALRecoverManager.getInstance()
          .getAllDataRegionScannedLatch()
          .countDownWithException(e.getMessage());
      throw new DataRegionException(e);
    }

    try {
      // collect candidate TsFiles from sequential and unsequential data directory
      List<TsFileResource> tmpSeqTsFiles =
          getAllFiles(TierManager.getInstance().getAllLocalSequenceFileFolders());
      List<TsFileResource> tmpUnseqTsFiles =
          getAllFiles(TierManager.getInstance().getAllLocalUnSequenceFileFolders());

      // split by partition so that we can find the last file of each partition and decide to
      // close it or not
      DataRegionRecoveryContext dataRegionRecoveryContext =
          new DataRegionRecoveryContext((long) tmpSeqTsFiles.size() + tmpUnseqTsFiles.size());
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
            FileMetrics.getInstance()
                .addTsFile(
                    resource.getDatabaseName(),
                    resource.getDataRegionId(),
                    resource.getTsFile().length(),
                    true,
                    resource.getTsFile().getName());
            if (resource.getModFile().exists()) {
              FileMetrics.getInstance().increaseModFileNum(1);
              FileMetrics.getInstance().increaseModFileSize(resource.getModFile().getSize());
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
            FileMetrics.getInstance()
                .addTsFile(
                    resource.getDatabaseName(),
                    resource.getDataRegionId(),
                    resource.getTsFile().length(),
                    false,
                    resource.getTsFile().getName());
          }
          if (resource.getModFile().exists()) {
            FileMetrics.getInstance().increaseModFileNum(1);
            FileMetrics.getInstance().increaseModFileSize(resource.getModFile().getSize());
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
      if (!partitionTmpSeqTsFiles.isEmpty() || !partitionTmpUnseqTsFiles.isEmpty()) {
        long latestPartitionId = Long.MIN_VALUE;
        if (!partitionTmpSeqTsFiles.isEmpty()) {
          latestPartitionId =
              ((TreeMap<Long, List<TsFileResource>>) partitionTmpSeqTsFiles).lastKey();
        }
        if (!partitionTmpUnseqTsFiles.isEmpty()) {
          latestPartitionId =
              Math.max(
                  latestPartitionId,
                  ((TreeMap<Long, List<TsFileResource>>) partitionTmpUnseqTsFiles).lastKey());
        }
        for (Entry<Long, List<TsFileResource>> partitionFiles : partitionTmpSeqTsFiles.entrySet()) {
          recoverFilesInPartition(
              partitionFiles.getKey(),
              dataRegionRecoveryContext,
              partitionFiles.getValue(),
              true,
              partitionFiles.getKey() == latestPartitionId);
        }
        for (Entry<Long, List<TsFileResource>> partitionFiles :
            partitionTmpUnseqTsFiles.entrySet()) {
          recoverFilesInPartition(
              partitionFiles.getKey(),
              dataRegionRecoveryContext,
              partitionFiles.getValue(),
              false,
              partitionFiles.getKey() == latestPartitionId);
        }
        if (config.isEnableSeparateData()) {
          TimePartitionManager.getInstance()
              .registerTimePartitionInfo(
                  new TimePartitionInfo(
                      new DataRegionId(Integer.parseInt(dataRegionId)),
                      latestPartitionId,
                      false,
                      Long.MAX_VALUE,
                      lastFlushTimeMap.getMemSize(latestPartitionId),
                      true));
        }
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
    } catch (IOException e) {
      // signal wal recover manager to recover this region's files
      WALRecoverManager.getInstance()
          .getAllDataRegionScannedLatch()
          .countDownWithException(e.getMessage());
      throw new DataRegionException(e);
    }

    initCompactionSchedule();

    if (StorageEngine.getInstance().isAllSgReady()) {
      logger.info("The data region {}[{}] is created successfully", databaseName, dataRegionId);
    } else {
      logger.info("The data region {}[{}] is recovered successfully", databaseName, dataRegionId);
    }
  }

  private void updateLastFlushTime(TsFileResource resource, boolean isSeq) {
    long timePartitionId = resource.getTimePartition();
    Map<String, Long> endTimeMap = new HashMap<>();
    for (String deviceId : resource.getDevices()) {
      long endTime = resource.getEndTime(deviceId);
      endTimeMap.put(deviceId.intern(), endTime);
    }
    if (config.isEnableSeparateData()) {
      lastFlushTimeMap.updateMultiDeviceFlushedTime(timePartitionId, endTimeMap);
    }
    if (CommonDescriptor.getInstance().getConfig().isLastCacheEnable()) {
      lastFlushTimeMap.updateMultiDeviceGlobalFlushedTime(endTimeMap);
    }
  }

  public void initCompactionSchedule() {
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

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private List<TsFileResource> getAllFiles(List<String> folders)
      throws IOException, DataRegionException {
    // "{partition id}/{tsfile name}" -> tsfile file, remove duplicate files in one time partition
    Map<String, File> tsFilePartitionPath2File = new HashMap<>();
    for (String baseDir : folders) {
      File fileFolder = fsFactory.getFile(baseDir + File.separator + databaseName, dataRegionId);
      if (!fileFolder.exists()) {
        continue;
      }
      // some TsFileResource may be being persisted when the system crashed, try recovering such
      // resources
      continueFailedRenames(fileFolder, TEMP_SUFFIX);

      File[] subFiles = fileFolder.listFiles();
      if (subFiles != null) {
        for (File partitionFolder : subFiles) {
          if (!partitionFolder.isDirectory()) {
            logger.warn("{} is not a directory.", partitionFolder.getAbsolutePath());
          } else {
            // some TsFileResource may be being persisted when the system crashed, try recovering
            // such resources
            continueFailedRenames(partitionFolder, TEMP_SUFFIX);
            String partitionName = partitionFolder.getName();
            File[] tsFilesInThisFolder =
                fsFactory.listFilesBySuffix(partitionFolder.getAbsolutePath(), TSFILE_SUFFIX);
            for (File f : tsFilesInThisFolder) {
              String tsFilePartitionPath = partitionName + File.separator + f.getName();
              tsFilePartitionPath2File.put(tsFilePartitionPath, f);
            }
          }
        }
      }
    }

    List<File> sortedFiles = new ArrayList<>(tsFilePartitionPath2File.values());
    sortedFiles.sort(this::compareFileName);

    long currentTime = System.currentTimeMillis();
    List<TsFileResource> ret = new ArrayList<>();
    for (File f : sortedFiles) {
      checkTsFileTime(f, currentTime);
      ret.add(new TsFileResource(f));
    }
    return ret;
  }

  private void continueFailedRenames(File fileFolder, String suffix) throws IOException {
    File[] files = fsFactory.listFilesBySuffix(fileFolder.getAbsolutePath(), suffix);
    if (files != null) {
      for (File tempResource : files) {
        File originResource = fsFactory.getFile(tempResource.getPath().replace(suffix, ""));
        if (originResource.exists()) {
          Files.delete(tempResource.toPath());
        } else {
          Files.move(tempResource.toPath(), originResource.toPath());
        }
      }
    }
  }

  /** check if the tsfile's time is smaller than system current time. */
  private void checkTsFileTime(File tsFile, long currentTime) throws DataRegionException {
    String[] items = tsFile.getName().replace(TSFILE_SUFFIX, "").split(FILE_NAME_SEPARATOR);
    long fileTime = Long.parseLong(items[0]);
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
        new UnsealedTsFileRecoverPerformer(unsealedTsFile, isSeq, context.recoverPerformers::add);
    // remember to close UnsealedTsFileRecoverPerformer
    return WALRecoverManager.getInstance().addRecoverPerformer(recoverPerformer);
  }

  private void recoverUnsealedTsFileCallBack(UnsealedTsFileRecoverPerformer recoverPerformer) {
    try {
      TsFileResource tsFileResource = recoverPerformer.getTsFileResource();
      boolean isSeq = recoverPerformer.isSequence();
      if (!recoverPerformer.canWrite()) {
        // cannot write, just close it
        try {
          tsFileResource.close();
        } catch (IOException e) {
          logger.error("Fail to close TsFile {} when recovering", tsFileResource.getTsFile(), e);
        }
        if (!TsFileValidator.getInstance().validateTsFile(tsFileResource)) {
          tsFileResource.remove();
          return;
        }
        updateLastFlushTime(tsFileResource, isSeq);
        tsFileResourceManager.registerSealedTsFileResource(tsFileResource);
        FileMetrics.getInstance()
            .addTsFile(
                tsFileResource.getDatabaseName(),
                tsFileResource.getDataRegionId(),
                tsFileResource.getTsFile().length(),
                recoverPerformer.isSequence(),
                tsFileResource.getTsFile().getName());
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
        if (workSequenceTsFileProcessors.get(tsFileProcessor.getTimeRangeId()) == null
            && workUnsequenceTsFileProcessors.get(tsFileProcessor.getTimeRangeId()) == null) {
          WritingMetrics.getInstance().recordActiveTimePartitionCount(1);
        }
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
                chunkMetadataSize += chunkMetadata.getRetainedSizeInBytes();
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
    if (isLatestPartition && config.isEnableSeparateData()) {
      lastFlushTimeMap.checkAndCreateFlushedTimePartition(partitionId);
      for (TsFileResource tsFileResource : resourceList) {
        updateLastFlushTime(tsFileResource, isSeq);
      }
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
      throw new OutOfTTLException(
          insertRowNode.getTime(), (CommonDateTimeUtils.currentTime() - dataTTL));
    }
    if (enableMemControl) {
      StorageEngine.blockInsertionIfReject(null);
    }
    long startTime = System.nanoTime();
    writeLock("InsertRow");
    PERFORMANCE_OVERVIEW_METRICS.recordScheduleLockCost(System.nanoTime() - startTime);
    try {
      if (deleted) {
        return;
      }
      // init map
      long timePartitionId = TimePartitionUtils.getTimePartitionId(insertRowNode.getTime());

      if (config.isEnableSeparateData()
          && !lastFlushTimeMap.checkAndCreateFlushedTimePartition(timePartitionId)) {
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
          config.isEnableSeparateData()
              && insertRowNode.getTime()
                  > lastFlushTimeMap.getFlushedTime(
                      timePartitionId, insertRowNode.getDevicePath().getFullPath());

      Map<TsFileProcessor, Boolean> tsFileProcessorMapForFlushing = new HashMap<>();

      // insert to sequence or unSequence file
      insertToTsFileProcessor(
          insertRowNode, isSequence, timePartitionId, tsFileProcessorMapForFlushing);

      // check memtable size and may asyncTryToFlush the work memtable
      for (Map.Entry<TsFileProcessor, Boolean> entry : tsFileProcessorMapForFlushing.entrySet()) {
        if (entry.getKey().shouldFlush()) {
          fileFlushPolicy.apply(this, entry.getKey(), entry.getValue());
        }
      }
    } finally {
      writeUnlock();
    }
  }

  /**
   * Insert a tablet (rows belonging to the same devices) into this database.
   *
   * @throws BatchProcessException if some of the rows failed to be inserted
   */
  @SuppressWarnings({"squid:S3776", "squid:S6541"}) // Suppress high Cognitive Complexity warning
  public void insertTablet(InsertTabletNode insertTabletNode)
      throws BatchProcessException, WriteProcessException {
    if (enableMemControl) {
      StorageEngine.blockInsertionIfReject(null);
    }
    long startTime = System.nanoTime();
    writeLock("insertTablet");
    PERFORMANCE_OVERVIEW_METRICS.recordScheduleLockCost(System.nanoTime() - startTime);
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
                      DateTimeUtils.convertLongToDate(currTime),
                      DateTimeUtils.convertLongToDate(
                          CommonDateTimeUtils.currentTime() - dataTTL)));
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
            (CommonDateTimeUtils.currentTime() - dataTTL));
      }
      // before is first start point
      int before = loc;
      // before time partition
      long beforeTimePartition =
          TimePartitionUtils.getTimePartitionId(insertTabletNode.getTimes()[before]);
      // init map

      if (config.isEnableSeparateData()
          && !lastFlushTimeMap.checkAndCreateFlushedTimePartition(beforeTimePartition)) {
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
          config.isEnableSeparateData()
              ? lastFlushTimeMap.getFlushedTime(
                  beforeTimePartition, insertTabletNode.getDevicePath().getFullPath())
              : Long.MAX_VALUE;

      // if is sequence
      boolean isSequence = false;
      while (loc < insertTabletNode.getRowCount()) {
        long time = insertTabletNode.getTimes()[loc];
        // always in some time partition
        // judge if we should insert sequence
        if (!isSequence && time > lastFlushTime) {
          // insert into unsequence and then start sequence
          noFailure =
              insertTabletToTsFileProcessor(
                      insertTabletNode, before, loc, false, results, beforeTimePartition)
                  && noFailure;
          before = loc;
          isSequence = true;
        }
        loc++;
      }

      // do not forget last part
      if (before < loc) {
        noFailure =
            insertTabletToTsFileProcessor(
                    insertTabletNode, before, loc, isSequence, results, beforeTimePartition)
                && noFailure;
      }
      startTime = System.nanoTime();
      tryToUpdateInsertTabletLastCache(insertTabletNode);
      PERFORMANCE_OVERVIEW_METRICS.recordScheduleUpdateLastCacheCost(System.nanoTime() - startTime);

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
    return dataTTL == Long.MAX_VALUE || (CommonDateTimeUtils.currentTime() - time) <= dataTTL;
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
    // return when start >= end or all measurement failed
    if (start >= end || insertTabletNode.allMeasurementFailed()) {
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

  private void tryToUpdateInsertTabletLastCache(InsertTabletNode node) {
    if (!CommonDescriptor.getInstance().getConfig().isLastCacheEnable()
        || (config.getDataRegionConsensusProtocolClass().equals(ConsensusFactory.IOT_CONSENSUS)
            && node.isSyncFromLeaderWhenUsingIoTConsensus())) {
      // disable updating last cache on follower
      return;
    }
    long latestFlushedTime =
        lastFlushTimeMap.getGlobalFlushedTime(node.getDevicePath().getFullPath());
    String[] measurements = node.getMeasurements();
    MeasurementSchema[] measurementSchemas = node.getMeasurementSchemas();
    String[] rawMeasurements = new String[measurements.length];
    for (int i = 0; i < measurements.length; i++) {
      if (measurementSchemas[i] != null) {
        // get raw measurement rather than alias
        rawMeasurements[i] = measurementSchemas[i].getMeasurementId();
      } else {
        rawMeasurements[i] = measurements[i];
      }
    }
    DataNodeSchemaCache.getInstance()
        .updateLastCache(
            getDatabaseName(),
            node.getDevicePath(),
            rawMeasurements,
            node.getMeasurementSchemas(),
            node.isAligned(),
            node::composeLastTimeValuePair,
            index -> node.getColumns()[index] != null,
            true,
            latestFlushedTime);
  }

  private void insertToTsFileProcessor(
      InsertRowNode insertRowNode,
      boolean sequence,
      long timePartitionId,
      Map<TsFileProcessor, Boolean> tsFileProcessorMapForFlushing)
      throws WriteProcessException {
    if (insertRowNode.allMeasurementFailed()) {
      return;
    }
    TsFileProcessor tsFileProcessor = getOrCreateTsFileProcessor(timePartitionId, sequence);
    if (tsFileProcessor == null) {
      return;
    }
    long[] costsForMetrics = new long[4];
    tsFileProcessor.insert(insertRowNode, costsForMetrics);
    PERFORMANCE_OVERVIEW_METRICS.recordCreateMemtableBlockCost(costsForMetrics[0]);
    PERFORMANCE_OVERVIEW_METRICS.recordScheduleMemoryBlockCost(costsForMetrics[1]);
    PERFORMANCE_OVERVIEW_METRICS.recordScheduleWalCost(costsForMetrics[2]);
    PERFORMANCE_OVERVIEW_METRICS.recordScheduleMemTableCost(costsForMetrics[3]);

    tsFileProcessorMapForFlushing.put(tsFileProcessor, sequence);

    if (CommonDescriptor.getInstance().getConfig().isLastCacheEnable()) {
      if ((config.getDataRegionConsensusProtocolClass().equals(ConsensusFactory.IOT_CONSENSUS)
          && insertRowNode.isSyncFromLeaderWhenUsingIoTConsensus())) {
        return;
      }
      // disable updating last cache on follower
      long startTime = System.nanoTime();
      tryToUpdateInsertRowLastCache(insertRowNode);
      PERFORMANCE_OVERVIEW_METRICS.recordScheduleUpdateLastCacheCost(System.nanoTime() - startTime);
    }
  }

  private void tryToUpdateInsertRowLastCache(InsertRowNode node) {
    long latestFlushedTime =
        lastFlushTimeMap.getGlobalFlushedTime(node.getDevicePath().getFullPath());
    String[] measurements = node.getMeasurements();
    MeasurementSchema[] measurementSchemas = node.getMeasurementSchemas();
    String[] rawMeasurements = new String[measurements.length];
    for (int i = 0; i < measurements.length; i++) {
      if (measurementSchemas[i] != null) {
        // get raw measurement rather than alias
        rawMeasurements[i] = measurementSchemas[i].getMeasurementId();
      } else {
        rawMeasurements[i] = measurements[i];
      }
    }
    DataNodeSchemaCache.getInstance()
        .updateLastCache(
            getDatabaseName(),
            node.getDevicePath(),
            rawMeasurements,
            node.getMeasurementSchemas(),
            node.isAligned(),
            node::composeTimeValuePair,
            index -> node.getValues()[index] != null,
            true,
            latestFlushedTime);
  }

  private void insertToTsFileProcessors(
      InsertRowsNode insertRowsNode, boolean[] areSequence, long[] timePartitionIds) {
    List<InsertRowNode> executedInsertRowNodeList = new ArrayList<>();
    long[] costsForMetrics = new long[4];
    Map<TsFileProcessor, Boolean> tsFileProcessorMapForFlushing = new HashMap<>();
    for (int i = 0; i < areSequence.length; i++) {
      InsertRowNode insertRowNode = insertRowsNode.getInsertRowNodeList().get(i);
      if (insertRowNode.allMeasurementFailed()) {
        continue;
      }
      TsFileProcessor tsFileProcessor =
          getOrCreateTsFileProcessor(timePartitionIds[i], areSequence[i]);
      if (tsFileProcessor == null) {
        continue;
      }
      tsFileProcessorMapForFlushing.put(tsFileProcessor, areSequence[i]);
      try {
        tsFileProcessor.insert(insertRowNode, costsForMetrics);
      } catch (WriteProcessException e) {
        insertRowsNode.getResults().put(i, RpcUtils.getStatus(e.getErrorCode(), e.getMessage()));
      }
      executedInsertRowNodeList.add(insertRowNode);
    }

    // check memtable size and may asyncTryToFlush the work memtable
    for (Map.Entry<TsFileProcessor, Boolean> entry : tsFileProcessorMapForFlushing.entrySet()) {
      if (entry.getKey().shouldFlush()) {
        fileFlushPolicy.apply(this, entry.getKey(), entry.getValue());
      }
    }

    PERFORMANCE_OVERVIEW_METRICS.recordCreateMemtableBlockCost(costsForMetrics[0]);
    PERFORMANCE_OVERVIEW_METRICS.recordScheduleMemoryBlockCost(costsForMetrics[1]);
    PERFORMANCE_OVERVIEW_METRICS.recordScheduleWalCost(costsForMetrics[2]);
    PERFORMANCE_OVERVIEW_METRICS.recordScheduleMemTableCost(costsForMetrics[3]);

    if (CommonDescriptor.getInstance().getConfig().isLastCacheEnable()) {
      if ((config.getDataRegionConsensusProtocolClass().equals(ConsensusFactory.IOT_CONSENSUS)
          && insertRowsNode.isSyncFromLeaderWhenUsingIoTConsensus())) {
        return;
      }
      // disable updating last cache on follower
      long startTime = System.nanoTime();
      tryToUpdateInsertRowsLastCache(executedInsertRowNodeList);
      PERFORMANCE_OVERVIEW_METRICS.recordScheduleUpdateLastCacheCost(System.nanoTime() - startTime);
    }
  }

  private void tryToUpdateInsertRowsLastCache(List<InsertRowNode> nodeList) {
    DataNodeSchemaCache.getInstance().takeReadLock();
    try {
      for (InsertRowNode node : nodeList) {
        long latestFlushedTime =
            lastFlushTimeMap.getGlobalFlushedTime(node.getDevicePath().getFullPath());
        String[] measurements = node.getMeasurements();
        MeasurementSchema[] measurementSchemas = node.getMeasurementSchemas();
        String[] rawMeasurements = new String[measurements.length];
        for (int i = 0; i < measurements.length; i++) {
          if (measurementSchemas[i] != null) {
            // get raw measurement rather than alias
            rawMeasurements[i] = measurementSchemas[i].getMeasurementId();
          } else {
            rawMeasurements[i] = measurements[i];
          }
        }
        DataNodeSchemaCache.getInstance()
            .updateLastCacheWithoutLock(
                getDatabaseName(),
                node.getDevicePath(),
                rawMeasurements,
                node.getMeasurementSchemas(),
                node.isAligned(),
                node::composeTimeValuePair,
                index -> node.getValues()[index] != null,
                true,
                latestFlushedTime);
      }
    } finally {
      DataNodeSchemaCache.getInstance().releaseReadLock();
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
        if (IoTDBDescriptor.getInstance().getConfig().isQuotaEnable()) {
          if (!DataNodeSpaceQuotaManager.getInstance().checkRegionDisk(databaseName)) {
            throw new ExceedQuotaException(
                "Unable to continue writing data, because the space allocated to the database "
                    + databaseName
                    + " has already used the upper limit",
                TSStatusCode.SPACE_QUOTA_EXCEEDED.getStatusCode());
          }
        }
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
      } catch (ExceedQuotaException e) {
        logger.error(e.getMessage());
        break;
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
      if (workSequenceTsFileProcessors.get(timeRangeId) == null
          && workUnsequenceTsFileProcessors.get(timeRangeId) == null) {
        WritingMetrics.getInstance().recordActiveTimePartitionCount(1);
      }
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
  public Future<?> asyncCloseOneTsFileProcessor(boolean sequence, TsFileProcessor tsFileProcessor) {
    // for sequence tsfile, we update the endTimeMap only when the file is prepared to be closed.
    // for unsequence tsfile, we have maintained the endTimeMap when an insertion comes.
    if (closingSequenceTsFileProcessor.contains(tsFileProcessor)
        || closingUnSequenceTsFileProcessor.contains(tsFileProcessor)
        || tsFileProcessor.alreadyMarkedClosing()) {
      return CompletableFuture.completedFuture(null);
    }
    logger.info(
        "Async close tsfile: {}",
        tsFileProcessor.getTsFileResource().getTsFile().getAbsolutePath());
    Future<?> future;
    if (sequence) {
      closingSequenceTsFileProcessor.add(tsFileProcessor);
      future = tsFileProcessor.asyncClose();

      workSequenceTsFileProcessors.remove(tsFileProcessor.getTimeRangeId());
      // if unsequence files don't contain this time range id, we should remove it's version
      // controller
      if (!workUnsequenceTsFileProcessors.containsKey(tsFileProcessor.getTimeRangeId())) {
        timePartitionIdVersionControllerMap.remove(tsFileProcessor.getTimeRangeId());
      }
    } else {
      closingUnSequenceTsFileProcessor.add(tsFileProcessor);
      future = tsFileProcessor.asyncClose();

      workUnsequenceTsFileProcessors.remove(tsFileProcessor.getTimeRangeId());
      // if sequence files don't contain this time range id, we should remove it's version
      // controller
      if (!workSequenceTsFileProcessors.containsKey(tsFileProcessor.getTimeRangeId())) {
        timePartitionIdVersionControllerMap.remove(tsFileProcessor.getTimeRangeId());
      }
    }
    if (workSequenceTsFileProcessors.get(tsFileProcessor.getTimeRangeId()) == null
        && workUnsequenceTsFileProcessors.get(tsFileProcessor.getTimeRangeId()) == null) {
      WritingMetrics.getInstance().recordActiveTimePartitionCount(-1);
    }
    return future;
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
            FileMetrics.getInstance().deleteTsFile(x.isSeq(), Collections.singletonList(x));
            if (x.getModFile().exists()) {
              FileMetrics.getInstance().decreaseModFileNum(1);
              FileMetrics.getInstance().decreaseModFileSize(x.getModFile().getSize());
            }
          });
      deleteAllSGFolders(TierManager.getInstance().getAllFilesFolders());
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
      if (FSUtils.getFSType(dataRegionDataFolder) != FSType.LOCAL) {
        try {
          fsFactory.deleteDirectory(dataRegionDataFolder.getPath());
        } catch (IOException e) {
          logger.error("Fail to delete data region folder {}", dataRegionDataFolder);
        }
      } else {
        if (dataRegionDataFolder.exists()) {
          org.apache.iotdb.commons.utils.FileUtils.deleteDirectoryAndEmptyParent(
              dataRegionDataFolder);
        }
      }
    }
  }

  /** Iterate each TsFile and try to lock and remove those out of TTL. */
  public synchronized void checkFilesTTL() {
    if (dataTTL == Long.MAX_VALUE) {
      logger.debug("{}: TTL not set, ignore the check", databaseName + "-" + dataRegionId);
      return;
    }
    long ttlLowerBound = CommonDateTimeUtils.currentTime() - dataTTL;
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
    // Try to set the resource to DELETED status and return if it failed
    if (!resource.setStatus(TsFileResourceStatus.DELETED)) {
      return;
    }
    tsFileManager.remove(resource, isSeq);
    // ensure that the file is not used by any queries
    resource.writeLock();
    try {
      // try to delete physical data file
      resource.remove();
      FileMetrics.getInstance().deleteTsFile(isSeq, Collections.singletonList(resource));
      logger.info(
          "Removed a file {} before {} by ttl ({} {})",
          resource.getTsFilePath(),
          new Date(ttlLowerBound),
          dataTTL,
          CommonDescriptor.getInstance().getConfig().getTimestampPrecision());
    } finally {
      resource.writeUnlock();
    }
  }

  public void timedFlushSeqMemTable() {
    int count = 0;
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
          count++;
        }
      }
    } finally {
      writeUnlock();
    }
    WritingMetrics.getInstance().recordTimedFlushMemTableCount(dataRegionId, count);
  }

  public void timedFlushUnseqMemTable() {
    int count = 0;
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
          count++;
        }
      }
    } finally {
      writeUnlock();
    }
    WritingMetrics.getInstance().recordTimedFlushMemTableCount(dataRegionId, count);
  }

  /** This method will be blocked until all tsfile processors are closed. */
  public void syncCloseAllWorkingTsFileProcessors() {
    try {
      List<Future<?>> tsFileProcessorsClosingFutures = asyncCloseAllWorkingTsFileProcessors();
      long startTime = System.currentTimeMillis();
      while (!closingSequenceTsFileProcessor.isEmpty()
          || !closingUnSequenceTsFileProcessor.isEmpty()) {
        synchronized (closeStorageGroupCondition) {
          // double check to avoid unnecessary waiting
          if (!closingSequenceTsFileProcessor.isEmpty()
              || !closingUnSequenceTsFileProcessor.isEmpty()) {
            closeStorageGroupCondition.wait(60_000);
          }
        }
        if (System.currentTimeMillis() - startTime > 60_000) {
          logger.warn(
              "{} has spent {}s to wait for closing all TsFiles.",
              databaseName + "-" + this.dataRegionId,
              (System.currentTimeMillis() - startTime) / 1000);
        }
      }
      for (Future<?> f : tsFileProcessorsClosingFutures) {
        if (f != null) {
          f.get();
        }
      }
    } catch (InterruptedException | ExecutionException e) {
      logger.error(
          "CloseFileNodeCondition error occurs while waiting for closing the storage " + "group {}",
          databaseName + "-" + dataRegionId,
          e);
      Thread.currentThread().interrupt();
    }
  }

  /** close all working tsfile processors */
  public List<Future<?>> asyncCloseAllWorkingTsFileProcessors() {
    writeLock("asyncCloseAllWorkingTsFileProcessors");
    List<Future<?>> futures = new ArrayList<>();
    try {
      logger.info("async force close all files in database: {}", databaseName + "-" + dataRegionId);
      // to avoid concurrent modification problem, we need a new array list
      for (TsFileProcessor tsFileProcessor :
          new ArrayList<>(workSequenceTsFileProcessors.values())) {
        futures.add(asyncCloseOneTsFileProcessor(true, tsFileProcessor));
      }
      // to avoid concurrent modification problem, we need a new array list
      for (TsFileProcessor tsFileProcessor :
          new ArrayList<>(workUnsequenceTsFileProcessors.values())) {
        futures.add(asyncCloseOneTsFileProcessor(false, tsFileProcessor));
      }
    } finally {
      writeUnlock();
    }
    return futures;
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

  /** used for queryengine */
  @Override
  public QueryDataSource query(
      List<PartialPath> pathList,
      String singleDeviceId,
      QueryContext context,
      Filter globalTimeFilter,
      List<Long> timePartitions)
      throws QueryProcessException {
    try {
      List<TsFileResource> seqResources =
          getFileResourceListForQuery(
              tsFileManager.getTsFileList(true, timePartitions),
              pathList,
              singleDeviceId,
              context,
              globalTimeFilter,
              true);
      List<TsFileResource> unseqResources =
          getFileResourceListForQuery(
              tsFileManager.getTsFileList(false, timePartitions),
              pathList,
              singleDeviceId,
              context,
              globalTimeFilter,
              false);

      QUERY_RESOURCE_METRIC_SET.recordQueryResourceNum(SEQUENCE_TSFILE, seqResources.size());
      QUERY_RESOURCE_METRIC_SET.recordQueryResourceNum(UNSEQUENCE_TSFILE, unseqResources.size());

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
      List<PartialPath> pathList,
      String singleDeviceId,
      QueryContext context,
      Filter globalTimeFilter,
      boolean isSeq)
      throws MetadataException {

    if (context.isDebug()) {
      DEBUG_LOGGER.info(
          "Path: {}, get tsfile list: {} isSeq: {} timefilter: {}",
          pathList,
          tsFileResources,
          isSeq,
          (globalTimeFilter == null ? "null" : globalTimeFilter));
    }

    List<TsFileResource> tsfileResourcesForQuery = new ArrayList<>();

    long timeLowerBound =
        dataTTL != Long.MAX_VALUE ? CommonDateTimeUtils.currentTime() - dataTTL : Long.MIN_VALUE;
    context.setQueryTimeLowerBound(timeLowerBound);

    for (TsFileResource tsFileResource : tsFileResources) {
      if (!tsFileResource.isSatisfied(
          singleDeviceId, globalTimeFilter, isSeq, dataTTL, context.isDebug())) {
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
      List<TsFileResource> sealedResource,
      List<TsFileResource> unsealedResource,
      long startTime,
      long endTime) {
    tsFileManager
        .getTsFileList(true, startTime, endTime)
        .forEach(
            tsFileResource -> {
              if (tsFileResource.isClosed()) {
                sealedResource.add(tsFileResource);
              } else {
                unsealedResource.add(tsFileResource);
              }
            });
    tsFileManager
        .getTsFileList(false, startTime, endTime)
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
   * @throws IOException
   */
  public void deleteByDevice(PartialPath pattern, long startTime, long endTime, long searchIndex)
      throws IOException {
    if (SettleService.getINSTANCE().getFilesToBeSettledCount().get() != 0) {
      throw new IOException(
          "Delete failed. " + "Please do not delete until the old files settled.");
    }
    // TODO: how to avoid partial deletion?
    // FIXME: notice that if we may remove a SGProcessor out of memory, we need to close all opened
    // mod files in mergingModification, sequenceFileList, and unsequenceFileList
    writeLock("delete");

    boolean hasReleasedLock = false;

    try {

      Set<PartialPath> devicePaths = new HashSet<>(pattern.getDevicePathPattern());

      // delete Last cache record if necessary
      DataNodeSchemaCache.getInstance().takeWriteLock();
      try {
        DataNodeSchemaCache.getInstance().invalidate(Collections.singletonList(pattern));
      } finally {
        DataNodeSchemaCache.getInstance().releaseWriteLock();
      }

      // write log to impacted working TsFileProcessors
      List<WALFlushListener> walListeners =
          logDeletionInWAL(startTime, endTime, searchIndex, pattern);

      for (WALFlushListener walFlushListener : walListeners) {
        if (walFlushListener.waitForResult() == WALFlushListener.Status.FAILURE) {
          logger.error("Fail to log delete to wal.", walFlushListener.getCause());
          throw walFlushListener.getCause();
        }
      }

      Deletion deletion = new Deletion(pattern, MERGE_MOD_START_VERSION_NUM, startTime, endTime);

      List<TsFileResource> sealedTsFileResource = new ArrayList<>();
      List<TsFileResource> unsealedTsFileResource = new ArrayList<>();
      separateTsFile(sealedTsFileResource, unsealedTsFileResource, startTime, endTime);
      // deviceMatchInfo is used for filter the matched deviceId in TsFileResource
      // deviceMatchInfo contains the DeviceId means this device matched the pattern
      Set<String> deviceMatchInfo = new HashSet<>();
      deleteDataInFiles(unsealedTsFileResource, deletion, devicePaths, deviceMatchInfo);
      writeUnlock();
      hasReleasedLock = true;

      deleteDataInFiles(sealedTsFileResource, deletion, devicePaths, deviceMatchInfo);

    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      if (!hasReleasedLock) {
        writeUnlock();
      }
    }
  }

  public void deleteDataDirectly(
      PartialPath pathToDelete, long startTime, long endTime, long searchIndex) throws IOException {
    logger.info(
        "{} will delete data files directly for deleting data between {} and {}",
        databaseName + "-" + dataRegionId,
        startTime,
        endTime);

    writeLock("deleteDataDirect");
    boolean releasedLock = false;
    try {
      // delete last cache record if necessary
      DataNodeSchemaCache.getInstance().takeWriteLock();
      try {
        DataNodeSchemaCache.getInstance().invalidate(databaseName);
      } finally {
        DataNodeSchemaCache.getInstance().releaseWriteLock();
      }

      // write log to impacted working TsFileProcessors
      List<WALFlushListener> walListeners =
          logDeletionInWAL(startTime, endTime, searchIndex, pathToDelete);

      for (WALFlushListener walFlushListener : walListeners) {
        if (walFlushListener.waitForResult() == WALFlushListener.Status.FAILURE) {
          logger.error("Fail to log delete to wal.", walFlushListener.getCause());
          throw walFlushListener.getCause();
        }
      }
      List<TsFileResource> sealedTsFileResource = new ArrayList<>();
      List<TsFileResource> unsealedTsFileResource = new ArrayList<>();
      separateTsFile(sealedTsFileResource, unsealedTsFileResource, startTime, endTime);
      deleteDataDirectlyInFile(unsealedTsFileResource, pathToDelete, startTime, endTime);
      writeUnlock();
      releasedLock = true;
      deleteDataDirectlyInFile(sealedTsFileResource, pathToDelete, startTime, endTime);
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      if (!releasedLock) {
        writeUnlock();
      }
    }
  }

  private List<WALFlushListener> logDeletionInWAL(
      long startTime, long endTime, long searchIndex, PartialPath path) {
    List<WALFlushListener> walFlushListeners = new ArrayList<>();
    if (config.getWalMode() == WALMode.DISABLE) {
      return walFlushListeners;
    }
    DeleteDataNode deleteDataNode =
        new DeleteDataNode(new PlanNodeId(""), Collections.singletonList(path), startTime, endTime);
    deleteDataNode.setSearchIndex(searchIndex);
    for (Map.Entry<Long, TsFileProcessor> entry : workSequenceTsFileProcessors.entrySet()) {
      if (TimePartitionUtils.satisfyPartitionId(startTime, endTime, entry.getKey())) {
        WALFlushListener walFlushListener = entry.getValue().logDeleteDataNodeInWAL(deleteDataNode);
        walFlushListeners.add(walFlushListener);
      }
    }
    for (Map.Entry<Long, TsFileProcessor> entry : workUnsequenceTsFileProcessors.entrySet()) {
      if (TimePartitionUtils.satisfyPartitionId(startTime, endTime, entry.getKey())) {
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
      Set<String> deviceMatchInfo) {
    long fileStartTime = tsFileResource.getTimeIndex().getMinStartTime();
    long fileEndTime = tsFileResource.getTimeIndex().getMaxEndTime();

    for (PartialPath device : devicePaths) {
      long deviceStartTime, deviceEndTime;
      if (device.hasWildcard()) {
        if (!tsFileResource.isClosed() && fileEndTime == Long.MIN_VALUE) {
          // unsealed seq file
          if (deleteEnd < fileStartTime) {
            // time range of file has not overlapped with the deletion
            return true;
          }
        } else {
          if (deleteEnd < fileStartTime || deleteStart > fileEndTime) {
            // time range of file has not overlapped with the deletion
            return true;
          }
        }
        if (databaseName.contentEquals(device.getDevice())) {
          return false;
        }
        Pair<Long, Long> startAndEndTime =
            tsFileResource.getPossibleStartTimeAndEndTime(device, deviceMatchInfo);
        if (startAndEndTime == null) {
          continue;
        }
        deviceStartTime = startAndEndTime.getLeft();
        deviceEndTime = startAndEndTime.getRight();
      } else {
        String deviceId = device.getFullPath();
        if (tsFileResource.definitelyNotContains(deviceId)) {
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

  // suppress warn of Throwable catch
  @SuppressWarnings("java:S1181")
  private void deleteDataInFiles(
      Collection<TsFileResource> tsFileResourceList,
      Deletion deletion,
      Set<PartialPath> devicePaths,
      Set<String> deviceMatchInfo)
      throws IOException {
    for (TsFileResource tsFileResource : tsFileResourceList) {
      if (canSkipDelete(
          tsFileResource,
          devicePaths,
          deletion.getStartTime(),
          deletion.getEndTime(),
          deviceMatchInfo)) {
        continue;
      }

      ModificationFile modFile = tsFileResource.getModFile();
      if (tsFileResource.isClosed()) {
        long originSize = -1;
        synchronized (modFile) {
          try {
            originSize = modFile.getSize();
            // delete data in sealed file
            if (tsFileResource.isCompacting()) {
              // we have to set modification offset to MAX_VALUE, as the offset of source chunk may
              // change after compaction
              deletion.setFileOffset(Long.MAX_VALUE);
              // write deletion into compaction modification file
              tsFileResource.getCompactionModFile().write(deletion);
              // write deletion into modification file to enable read during compaction
              modFile.write(deletion);
              // remember to close mod file
              tsFileResource.getCompactionModFile().close();
              modFile.close();
            } else {
              deletion.setFileOffset(tsFileResource.getTsFileSize());
              // write deletion into modification file
              boolean modFileExists = modFile.exists();

              modFile.write(deletion);

              // remember to close mod file
              modFile.close();

              // if file length greater than 1M,execute compact.
              modFile.compact();

              if (!modFileExists) {
                FileMetrics.getInstance().increaseModFileNum(1);
              }

              // The file size may be smaller than the original file, so the increment here may be
              // negative
              FileMetrics.getInstance().increaseModFileSize(modFile.getSize() - originSize);
            }
          } catch (Throwable t) {
            if (originSize != -1) {
              modFile.truncate(originSize);
            }
            throw t;
          }
          logger.info(
              "[Deletion] Deletion with path:{}, time:{}-{} written into mods file:{}.",
              deletion.getPath(),
              deletion.getStartTime(),
              deletion.getEndTime(),
              modFile.getFilePath());
        }
      } else {
        // delete data in memory of unsealed file
        tsFileResource.getProcessor().deleteDataInMemory(deletion, devicePaths);
      }
    }
  }

  private void deleteDataDirectlyInFile(
      List<TsFileResource> tsfileResourceList,
      PartialPath pathToDelete,
      long startTime,
      long endTime)
      throws IOException {
    List<TsFileResource> deletedByMods = new ArrayList<>();
    List<TsFileResource> deletedByFiles = new ArrayList<>();
    separateTsFileToDelete(
        new HashSet<>(pathToDelete.getDevicePathPattern()),
        tsfileResourceList,
        deletedByMods,
        deletedByFiles,
        startTime,
        endTime);
    Deletion deletion = new Deletion(pathToDelete, MERGE_MOD_START_VERSION_NUM, startTime, endTime);
    // can be deleted by mods.
    for (TsFileResource tsFileResource : deletedByMods) {
      ModificationFile modFile = tsFileResource.getModFile();
      if (tsFileResource.isClosed()) {
        long originSize = -1;
        synchronized (modFile) {
          try {
            originSize = modFile.getSize();
            // delete data in sealed file
            if (tsFileResource.isCompacting()) {
              // we have to set modification offset to MAX_VALUE, as the offset of source chunk
              // may change after compaction
              deletion.setFileOffset(Long.MAX_VALUE);
              // write deletion into compaction modification file
              tsFileResource.getCompactionModFile().write(deletion);
              // write deletion into modification file to enable read during compaction
              modFile.write(deletion);
              // remember to close mod file
              tsFileResource.getCompactionModFile().close();
              modFile.close();
            } else {
              deletion.setFileOffset(tsFileResource.getTsFileSize());
              // write deletion into modification file
              boolean modFileExists = modFile.exists();

              modFile.write(deletion);

              // remember to close mod file
              modFile.close();

              // if file length greater than 1M,execute compact.
              modFile.compact();

              if (!modFileExists) {
                FileMetrics.getInstance().increaseModFileNum(1);
              }

              // The file size may be smaller than the original file, so the increment here may be
              // negative
              FileMetrics.getInstance().increaseModFileSize(modFile.getSize() - originSize);
            }
          } catch (Throwable t) {
            if (originSize != -1) {
              modFile.truncate(originSize);
            }
            throw t;
          }
          logger.info(
              "[Deletion] Deletion with path:{}, time:{}-{} written into mods file:{}.",
              deletion.getPath(),
              deletion.getStartTime(),
              deletion.getEndTime(),
              modFile.getFilePath());
        }
      } else {
        // delete data in memory of unsealed file
        tsFileResource
            .getProcessor()
            .deleteDataInMemory(deletion, new HashSet<>(pathToDelete.getDevicePathPattern()));
      }
    }

    // can be deleted by files
    for (TsFileResource tsFileResource : deletedByFiles) {
      tsFileManager.remove(tsFileResource, tsFileResource.isSeq());
      tsFileResource.writeLock();
      try {
        FileMetrics.getInstance()
            .deleteTsFile(tsFileResource.isSeq(), Collections.singletonList(tsFileResource));
        if (tsFileResource.getModFile().exists()) {
          FileMetrics.getInstance().decreaseModFileNum(1);
          FileMetrics.getInstance().decreaseModFileSize(tsFileResource.getModFile().getSize());
        }
        tsFileResource.remove();
        logger.info("Remove tsfile {} directly when delete data", tsFileResource.getTsFilePath());
      } finally {
        tsFileResource.writeUnlock();
      }
    }
  }

  private void separateTsFileToDelete(
      Set<PartialPath> pathToDelete,
      List<TsFileResource> tsFileResourceList,
      List<TsFileResource> deletedByMods,
      List<TsFileResource> deletedByFiles,
      long startTime,
      long endTime) {
    Set<String> deviceMatchInfo = new HashSet<>();
    for (TsFileResource file : tsFileResourceList) {
      long fileStartTime = file.getTimeIndex().getMinStartTime();
      long fileEndTime = file.getTimeIndex().getMaxEndTime();

      if (!canSkipDelete(file, pathToDelete, startTime, endTime, deviceMatchInfo)) {
        if (startTime <= fileStartTime
            && endTime >= fileEndTime
            && file.isClosed()
            && file.setStatus(TsFileResourceStatus.DELETED)) {
          deletedByFiles.add(file);
        } else {
          deletedByMods.add(file);
        }
      }
    }
  }

  private void unsequenceFlushCallback(
      TsFileProcessor processor, Map<String, Long> updateMap, long systemFlushTime) {
    if (!config.isEnableSeparateData()
        && CommonDescriptor.getInstance().getConfig().isLastCacheEnable()) {
      // update globalLastFlushTime if and only if isEnableSeparateData is false and
      // isLastCacheEnable is true
      lastFlushTimeMap.updateMultiDeviceGlobalFlushedTime(updateMap);
    }
    if (config.isEnableSeparateData()) {
      TimePartitionManager.getInstance()
          .updateAfterFlushing(
              new DataRegionId(Integer.valueOf(dataRegionId)),
              processor.getTimeRangeId(),
              systemFlushTime,
              lastFlushTimeMap.getMemSize(processor.getTimeRangeId()),
              workSequenceTsFileProcessors.get(processor.getTimeRangeId()) != null);
    }
  }

  private void sequenceFlushCallback(
      TsFileProcessor processor, Map<String, Long> updateMap, long systemFlushTime) {
    if (config.isEnableSeparateData()
        && CommonDescriptor.getInstance().getConfig().isLastCacheEnable()) {
      // update both partitionLastFlushTime and globalLastFlushTime
      lastFlushTimeMap.updateLatestFlushTime(processor.getTimeRangeId(), updateMap);
    } else {
      // isEnableSeparateData is true and isLastCacheEnable is false, then update
      // partitionLastFlushTime only
      lastFlushTimeMap.updateMultiDeviceFlushedTime(processor.getTimeRangeId(), updateMap);
    }

    if (config.isEnableSeparateData()) {
      TimePartitionManager.getInstance()
          .updateAfterFlushing(
              new DataRegionId(Integer.valueOf(dataRegionId)),
              processor.getTimeRangeId(),
              systemFlushTime,
              lastFlushTimeMap.getMemSize(processor.getTimeRangeId()),
              workSequenceTsFileProcessors.get(processor.getTimeRangeId()) != null);
    }
  }

  /** put the memtable back to the MemTablePool and make the metadata in writer visible */
  // TODO please consider concurrency with read and insert method.
  private void closeUnsealedTsFileProcessorCallBack(TsFileProcessor tsFileProcessor)
      throws TsFileProcessorException {
    boolean isEmptyFile =
        tsFileProcessor.isEmpty() || tsFileProcessor.getTsFileResource().isEmpty();
    boolean isValidateTsFileFailed = false;
    if (!isEmptyFile) {
      isValidateTsFileFailed =
          !TsFileValidator.getInstance().validateTsFile(tsFileProcessor.getTsFileResource());
    }
    closeQueryLock.writeLock().lock();
    try {
      tsFileProcessor.close();
      if (isEmptyFile) {
        tsFileProcessor.getTsFileResource().remove();
        tsFileManager.remove(tsFileProcessor.getTsFileResource(), tsFileProcessor.isSequence());
      } else if (isValidateTsFileFailed) {
        String tsFilePath = tsFileProcessor.getTsFileResource().getTsFile().getAbsolutePath();
        renameAndHandleError(tsFilePath, tsFilePath + BROKEN_SUFFIX);
        renameAndHandleError(
            tsFilePath + RESOURCE_SUFFIX, tsFilePath + RESOURCE_SUFFIX + BROKEN_SUFFIX);
        tsFileManager.remove(tsFileProcessor.getTsFileResource(), tsFileProcessor.isSequence());
      } else {
        tsFileResourceManager.registerSealedTsFileResource(tsFileProcessor.getTsFileResource());
      }
    } finally {
      closeQueryLock.writeLock().unlock();
    }
    // closingSequenceTsFileProcessor is a thread safety class.

    synchronized (closeStorageGroupCondition) {
      if (closingSequenceTsFileProcessor.contains(tsFileProcessor)) {
        closingSequenceTsFileProcessor.remove(tsFileProcessor);
      } else {
        closingUnSequenceTsFileProcessor.remove(tsFileProcessor);
      }
      closeStorageGroupCondition.notifyAll();
    }
    if (!isValidateTsFileFailed) {
      TsFileResource tsFileResource = tsFileProcessor.getTsFileResource();
      FileMetrics.getInstance()
          .addTsFile(
              tsFileResource.getDatabaseName(),
              tsFileResource.getDataRegionId(),
              tsFileResource.getTsFileSize(),
              tsFileProcessor.isSequence(),
              tsFileResource.getTsFile().getName());
    }
  }

  protected int executeCompaction() {
    int trySubmitCount = 0;
    List<Long> timePartitions = new ArrayList<>(tsFileManager.getTimePartitions());
    // sort the time partition from largest to smallest
    timePartitions.sort(Comparator.reverseOrder());

    CompactionScheduleSummary summary = new CompactionScheduleSummary();
    if (IoTDBDescriptor.getInstance().getConfig().isEnableCrossSpaceCompaction()) {
      trySubmitCount += executeInsertionCompaction(timePartitions);
      summary.incrementSubmitTaskNum(CompactionTaskType.INSERTION, trySubmitCount);
    }
    if (summary.getSubmitInsertionCrossSpaceCompactionTaskNum() == 0) {
      // the name of this variable is trySubmitCount, because the task submitted to the queue could
      // be
      // evicted due to the low priority of the task
      CompactionScheduler.lockCompactionSelection();
      try {
        for (long timePartition : timePartitions) {
          trySubmitCount +=
              CompactionScheduler.scheduleCompaction(tsFileManager, timePartition, summary);
        }
      } catch (Throwable e) {
        logger.error("Meet error in compaction schedule.", e);
      } finally {
        CompactionScheduler.unlockCompactionSelection();
      }
    }
    if (summary.hasSubmitTask()) {
      CompactionMetrics.getInstance().updateCompactionTaskSelectionNum(summary);
    }
    return trySubmitCount;
  }

  protected int executeInsertionCompaction(List<Long> timePartitions) {
    int trySubmitCount = 0;
    CompactionScheduler.lockCompactionSelection();
    try {
      while (true) {
        int currentSubmitCount = 0;
        Phaser insertionTaskPhaser = new Phaser(1);
        for (long timePartition : timePartitions) {
          currentSubmitCount +=
              CompactionScheduler.scheduleInsertionCompaction(
                  tsFileManager, timePartition, insertionTaskPhaser);
        }
        trySubmitCount += currentSubmitCount;
        insertionTaskPhaser.arriveAndAwaitAdvance();
        if (currentSubmitCount != 0) {
          continue;
        }
        break;
      }
    } catch (Throwable e) {
      logger.error("Meet error in compaction schedule.", e);
    } finally {
      CompactionScheduler.unlockCompactionSelection();
    }
    return trySubmitCount;
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

  /** merge file under this database processor */
  public int compact() {
    writeLock("merge");
    try {
      return executeCompaction();
    } finally {
      writeUnlock();
    }
  }

  private void resetLastCacheWhenLoadingTsFile() throws IllegalPathException {
    if (!CommonDescriptor.getInstance().getConfig().isLastCacheEnable()) {
      return;
    }
    DataNodeSchemaCache.getInstance().takeWriteLock();
    try {
      DataNodeSchemaCache.getInstance().invalidateAll();
    } finally {
      DataNodeSchemaCache.getInstance().releaseWriteLock();
    }
  }

  /**
   * Load a new tsfile to unsequence dir.
   *
   * <p>Then, update the latestTimeForEachDevice and partitionLatestFlushedTimeForEachDevice.
   *
   * @param newTsFileResource tsfile resource @UsedBy load external tsfile module
   * @param deleteOriginFile whether to delete origin tsfile
   * @param isGeneratedByPipe whether the load tsfile request is generated by pipe
   */
  public void loadNewTsFile(
      TsFileResource newTsFileResource, boolean deleteOriginFile, boolean isGeneratedByPipe)
      throws LoadFileException {
    File tsfileToBeInserted = newTsFileResource.getTsFile();
    long newFilePartitionId = newTsFileResource.getTimePartitionWithCheck();

    if (!TsFileValidator.getInstance().validateTsFile(newTsFileResource)) {
      throw new LoadFileException(
          "tsfile validate failed, " + newTsFileResource.getTsFile().getName());
    }

    writeLock("loadNewTsFile");
    try {
      newTsFileResource.setSeq(false);
      String newFileName =
          getNewTsFileName(
              System.currentTimeMillis(),
              getAndSetNewVersion(newFilePartitionId, newTsFileResource),
              0,
              0);

      if (!newFileName.equals(tsfileToBeInserted.getName())) {
        logger.info(
            "TsFile {} must be renamed to {} for loading into the unsequence list.",
            tsfileToBeInserted.getName(),
            newFileName);
        newTsFileResource.setFile(
            fsFactory.getFile(tsfileToBeInserted.getParentFile(), newFileName));
      }
      loadTsFileToUnSequence(
          tsfileToBeInserted, newTsFileResource, newFilePartitionId, deleteOriginFile);

      PipeInsertionDataNodeListener.getInstance()
          .listenToTsFile(dataRegionId, newTsFileResource, true, isGeneratedByPipe);

      FileMetrics.getInstance()
          .addTsFile(
              newTsFileResource.getDatabaseName(),
              newTsFileResource.getDataRegionId(),
              newTsFileResource.getTsFile().length(),
              false,
              newTsFileResource.getTsFile().getName());

      resetLastCacheWhenLoadingTsFile(); // update last cache
      updateLastFlushTime(newTsFileResource); // update last flush time
      long partitionNum = newTsFileResource.getTimePartition();
      updatePartitionFileVersion(partitionNum, newTsFileResource.getVersion());
      logger.info("TsFile {} is successfully loaded in unsequence list.", newFileName);
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
      long timePartitionId = TimePartitionUtils.getTimePartitionId(endTime);
      if (config.isEnableSeparateData()) {
        lastFlushTimeMap.updateOneDeviceFlushedTime(timePartitionId, device, endTime);
      }
      if (CommonDescriptor.getInstance().getConfig().isLastCacheEnable()) {
        lastFlushTimeMap.updateOneDeviceGlobalFlushedTime(device, endTime);
      }
    }
  }

  /**
   * Execute the loading process by the type.
   *
   * @param tsFileResource tsfile resource to be loaded
   * @param filePartitionId the partition id of the new file
   * @param deleteOriginFile whether to delete the original file
   * @return load the file successfully @UsedBy sync module, load external tsfile module.
   */
  private boolean loadTsFileToUnSequence(
      File tsFileToLoad,
      TsFileResource tsFileResource,
      long filePartitionId,
      boolean deleteOriginFile)
      throws LoadFileException, DiskSpaceInsufficientException {
    File targetFile;
    targetFile =
        fsFactory.getFile(
            TierManager.getInstance().getNextFolderForTsFile(0, false),
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

    File resourceFileToLoad = fsFactory.getFile(tsFileToLoad.getAbsolutePath() + RESOURCE_SUFFIX);
    File targetResourceFile = fsFactory.getFile(targetFile.getAbsolutePath() + RESOURCE_SUFFIX);
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
  public boolean unloadTsfile(File fileToBeUnloaded, File targetDir) throws IOException {
    writeLock("unloadTsfile");
    TsFileResource tsFileResourceToBeMoved = null;
    try {
      Iterator<TsFileResource> sequenceIterator = tsFileManager.getIterator(true);
      while (sequenceIterator.hasNext()) {
        TsFileResource sequenceResource = sequenceIterator.next();
        if (sequenceResource.getTsFile().getName().equals(fileToBeUnloaded.getName())) {
          tsFileResourceToBeMoved = sequenceResource;
          tsFileManager.remove(tsFileResourceToBeMoved, true);
          FileMetrics.getInstance()
              .deleteTsFile(true, Collections.singletonList(tsFileResourceToBeMoved));
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
            FileMetrics.getInstance()
                .deleteTsFile(false, Collections.singletonList(tsFileResourceToBeMoved));
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
          CommonDateTimeUtils.convertMilliTimeWithPrecision(
              dataTTL, CommonDescriptor.getInstance().getConfig().getTimestampPrecision());
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

  public void abortCompaction() {
    tsFileManager.setAllowCompaction(false);
    List<AbstractCompactionTask> runningTasks =
        CompactionTaskManager.getInstance().abortCompaction(databaseName + "-" + dataRegionId);
    while (CompactionTaskManager.getInstance().isAnyTaskInListStillRunning(runningTasks)) {
      try {
        TimeUnit.MILLISECONDS.sleep(10);
      } catch (InterruptedException e) {
        logger.error("Thread get interrupted when waiting compaction to finish", e);
        Thread.currentThread().interrupt();
      }
    }
    if (timedCompactionScheduleTask != null) {
      timedCompactionScheduleTask.shutdownNow();
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
    PERFORMANCE_OVERVIEW_METRICS.recordScheduleLockCost(System.nanoTime() - startTime);
    try {
      if (deleted) {
        return;
      }
      Map<TsFileProcessor, Boolean> tsFileProcessorMapForFlushing = new HashMap<>();
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
                          DateTimeUtils.convertLongToDate(insertRowNode.getTime()),
                          DateTimeUtils.convertLongToDate(
                              CommonDateTimeUtils.currentTime() - dataTTL))));
          continue;
        }
        // init map
        long timePartitionId = TimePartitionUtils.getTimePartitionId(insertRowNode.getTime());

        if (config.isEnableSeparateData()
            && !lastFlushTimeMap.checkAndCreateFlushedTimePartition(timePartitionId)) {
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

        boolean isSequence =
            config.isEnableSeparateData()
                && insertRowNode.getTime()
                    > lastFlushTimeMap.getFlushedTime(
                        timePartitionId, insertRowNode.getDevicePath().getFullPath());

        // insert to sequence or unSequence file
        try {
          insertToTsFileProcessor(
              insertRowNode, isSequence, timePartitionId, tsFileProcessorMapForFlushing);
        } catch (WriteProcessException e) {
          insertRowsOfOneDeviceNode
              .getResults()
              .put(i, RpcUtils.getStatus(e.getErrorCode(), e.getMessage()));
        }
      }
      // check memtable size and may asyncTryToFlush the work memtable
      for (Map.Entry<TsFileProcessor, Boolean> entry : tsFileProcessorMapForFlushing.entrySet()) {
        if (entry.getKey().shouldFlush()) {
          fileFlushPolicy.apply(this, entry.getKey(), entry.getValue());
        }
      }
    } finally {
      writeUnlock();
    }
    if (!insertRowsOfOneDeviceNode.getResults().isEmpty()) {
      throw new BatchProcessException("Partial failed inserting rows of one device");
    }
  }

  public void insert(InsertRowsNode insertRowsNode)
      throws BatchProcessException, WriteProcessRejectException {
    if (enableMemControl) {
      StorageEngine.blockInsertionIfReject(null);
    }
    long startTime = System.nanoTime();
    writeLock("InsertRows");
    PERFORMANCE_OVERVIEW_METRICS.recordScheduleLockCost(System.nanoTime() - startTime);
    try {
      if (deleted) {
        return;
      }
      boolean[] areSequence = new boolean[insertRowsNode.getInsertRowNodeList().size()];
      long[] timePartitionIds = new long[insertRowsNode.getInsertRowNodeList().size()];
      for (int i = 0; i < insertRowsNode.getInsertRowNodeList().size(); i++) {
        InsertRowNode insertRowNode = insertRowsNode.getInsertRowNodeList().get(i);
        if (!isAlive(insertRowNode.getTime())) {
          insertRowsNode
              .getResults()
              .put(
                  i,
                  RpcUtils.getStatus(
                      TSStatusCode.OUT_OF_TTL.getStatusCode(),
                      String.format(
                          "Insertion time [%s] is less than ttl time bound [%s]",
                          DateTimeUtils.convertLongToDate(insertRowNode.getTime()),
                          DateTimeUtils.convertLongToDate(
                              CommonDateTimeUtils.currentTime() - dataTTL))));
          continue;
        }
        // init map
        timePartitionIds[i] = TimePartitionUtils.getTimePartitionId(insertRowNode.getTime());

        if (!lastFlushTimeMap.checkAndCreateFlushedTimePartition(timePartitionIds[i])) {
          TimePartitionManager.getInstance()
              .registerTimePartitionInfo(
                  new TimePartitionInfo(
                      new DataRegionId(Integer.parseInt(dataRegionId)),
                      timePartitionIds[i],
                      true,
                      Long.MAX_VALUE,
                      0,
                      tsFileManager.isLatestTimePartition(timePartitionIds[i])));
        }
        areSequence[i] =
            config.isEnableSeparateData()
                && insertRowNode.getTime()
                    > lastFlushTimeMap.getFlushedTime(
                        timePartitionIds[i], insertRowNode.getDevicePath().getFullPath());
      }
      insertToTsFileProcessors(insertRowsNode, areSequence, timePartitionIds);
      if (!insertRowsNode.getResults().isEmpty()) {
        throw new BatchProcessException("Partial failed inserting rows");
      }
    } finally {
      writeUnlock();
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
      } catch (WriteProcessException e) {
        insertMultiTabletsNode
            .getResults()
            .put(i, RpcUtils.getStatus(e.getErrorCode(), e.getMessage()));
      } catch (BatchProcessException e) {
        // for each error
        TSStatus firstStatus = null;
        for (TSStatus status : e.getFailingStatus()) {
          if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            firstStatus = status;
          }
          // return WRITE_PROCESS_REJECT directly for the consensus retry logic
          if (status.getCode() == TSStatusCode.WRITE_PROCESS_REJECT.getStatusCode()) {
            insertMultiTabletsNode.getResults().put(i, status);
            throw new BatchProcessException("Rejected inserting multi tablets");
          }
        }
        insertMultiTabletsNode.getResults().put(i, firstStatus);
      }
    }

    if (!insertMultiTabletsNode.getResults().isEmpty()) {
      throw new BatchProcessException("Partial failed inserting multi tablets");
    }
  }

  /** @return the disk space occupied by this data region, unit is MB */
  public long countRegionDiskSize() {
    AtomicLong diskSize = new AtomicLong(0);
    TierManager.getInstance()
        .getAllLocalFilesFolders()
        .forEach(
            folder -> {
              folder = folder + File.separator + databaseName + File.separator + dataRegionId;
              countFolderDiskSize(folder, diskSize);
            });
    return diskSize.get() / 1024 / 1024;
  }

  /**
   * @param folder the folder's path
   * @param diskSize the disk space occupied by this folder, unit is MB
   */
  private void countFolderDiskSize(String folder, AtomicLong diskSize) {
    File file = FSFactoryProducer.getFSFactory().getFile(folder);
    File[] allFile = file.listFiles();
    if (allFile == null) {
      return;
    }
    for (File f : allFile) {
      if (f.isFile()) {
        diskSize.addAndGet(f.length());
      } else if (f.isDirectory()) {
        countFolderDiskSize(f.getAbsolutePath(), diskSize);
      }
    }
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

  public void setCustomCloseFileListeners(List<CloseFileListener> customCloseFileListeners) {
    this.customCloseFileListeners = customCloseFileListeners;
  }

  public void setCustomFlushListeners(List<FlushListener> customFlushListeners) {
    this.customFlushListeners = customFlushListeners;
  }

  public void setAllowCompaction(boolean allowCompaction) {
    this.tsFileManager.setAllowCompaction(allowCompaction);
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
  public interface SettleTsFileCallBack {

    void call(TsFileResource oldTsFileResource, List<TsFileResource> newTsFileResources)
        throws WriteProcessException;
  }

  public List<Long> getTimePartitions() {
    return new ArrayList<>(partitionMaxFileVersions.keySet());
  }

  public Long getLatestTimePartition() {
    return getTimePartitions().stream().max(Long::compareTo).orElse(0L);
  }

  public String getInsertWriteLockHolder() {
    return insertWriteLockHolder;
  }

  public ScheduledExecutorService getTimedCompactionScheduleTask() {
    return timedCompactionScheduleTask;
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
      FileMetrics.getInstance().deleteRegion(databaseName, dataRegionId);
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
    writeLock("releaseFlushTimeMap");
    try {
      lastFlushTimeMap.removePartition(timePartitionId);
    } finally {
      writeUnlock();
    }
  }

  public long getMemCost() {
    return dataRegionInfo.getMemCost();
  }

  private void renameAndHandleError(String originFileName, String newFileName) {
    try {
      File originFile = new File(originFileName);
      if (originFile.exists()) {
        Files.move(originFile.toPath(), Paths.get(newFileName));
      }
    } catch (IOException e) {
      logger.error("Failed to rename {} to {},", originFileName, newFileName, e);
    }
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
