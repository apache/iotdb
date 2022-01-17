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

import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.compaction.CompactionScheduler;
import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.cross.inplace.manage.MergeManager;
import org.apache.iotdb.db.engine.compaction.inner.utils.InnerSpaceCompactionUtils;
import org.apache.iotdb.db.engine.compaction.task.CompactionRecoverTask;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.flush.CloseFileListener;
import org.apache.iotdb.db.engine.flush.FlushListener;
import org.apache.iotdb.db.engine.flush.TsFileFlushPolicy;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.timeindex.DeviceTimeIndex;
import org.apache.iotdb.db.engine.trigger.executor.TriggerEngine;
import org.apache.iotdb.db.engine.trigger.executor.TriggerEvent;
import org.apache.iotdb.db.engine.upgrade.UpgradeCheckStatus;
import org.apache.iotdb.db.engine.upgrade.UpgradeLog;
import org.apache.iotdb.db.engine.version.SimpleFileVersionController;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.exception.BatchProcessException;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.exception.LoadFileException;
import org.apache.iotdb.db.exception.StorageGroupProcessorException;
import org.apache.iotdb.db.exception.TriggerExecutionException;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.WriteProcessRejectException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.OutOfTTLException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.idtable.IDTable;
import org.apache.iotdb.db.metadata.idtable.IDTableManager;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowsOfOneDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.control.QueryFileManager;
import org.apache.iotdb.db.rescon.TsFileResourceManager;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.service.SettleService;
import org.apache.iotdb.db.service.metrics.Metric;
import org.apache.iotdb.db.service.metrics.MetricsService;
import org.apache.iotdb.db.service.metrics.Tag;
import org.apache.iotdb.db.tools.settle.TsFileAndModSettleTool;
import org.apache.iotdb.db.utils.CopyOnReadLinkedList;
import org.apache.iotdb.db.utils.MmapUtil;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.db.utils.UpgradeUtils;
import org.apache.iotdb.db.writelog.recover.TsFileRecoverPerformer;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
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
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

import static org.apache.iotdb.db.conf.IoTDBConstant.FILE_NAME_SEPARATOR;
import static org.apache.iotdb.db.engine.compaction.cross.inplace.task.CrossSpaceMergeTask.MERGE_SUFFIX;
import static org.apache.iotdb.db.engine.compaction.inner.utils.SizeTieredCompactionLogger.COMPACTION_LOG_NAME;
import static org.apache.iotdb.db.engine.storagegroup.TsFileResource.TEMP_SUFFIX;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

/**
 * For sequence data, a StorageGroupProcessor has some TsFileProcessors, in which there is only one
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
public class VirtualStorageGroupProcessor {

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final Logger DEBUG_LOGGER = LoggerFactory.getLogger("QUERY_DEBUG");

  /**
   * All newly generated chunks after merge have version number 0, so we set merged Modification
   * file version to 1 to take effect
   */
  private static final int MERGE_MOD_START_VERSION_NUM = 1;

  private static final Logger logger = LoggerFactory.getLogger(VirtualStorageGroupProcessor.class);
  /** indicating the file to be loaded already exists locally. */
  private static final int POS_ALREADY_EXIST = -2;
  /** indicating the file to be loaded overlap with some files. */
  private static final int POS_OVERLAP = -3;

  private static final int WAL_BUFFER_SIZE =
      IoTDBDescriptor.getInstance().getConfig().getWalBufferSize() / 2;
  private final boolean enableMemControl = config.isEnableMemControl();
  /**
   * a read write lock for guaranteeing concurrent safety when accessing all fields in this class
   * (i.e., schema, (un)sequenceFileList, work(un)SequenceTsFileProcessor,
   * closing(Un)SequenceTsFileProcessor, latestTimeForEachDevice, and
   * partitionLatestFlushedTimeForEachDevice)
   */
  private final ReadWriteLock insertLock = new ReentrantReadWriteLock();
  /** closeStorageGroupCondition is used to wait for all currently closing TsFiles to be done. */
  private final Object closeStorageGroupCondition = new Object();
  /**
   * avoid some tsfileResource is changed (e.g., from unsealed to sealed) when a query is executed.
   */
  private final ReadWriteLock closeQueryLock = new ReentrantReadWriteLock();
  /** time partition id in the storage group -> tsFileProcessor for this time partition */
  private final TreeMap<Long, TsFileProcessor> workSequenceTsFileProcessors = new TreeMap<>();
  /** time partition id in the storage group -> tsFileProcessor for this time partition */
  private final TreeMap<Long, TsFileProcessor> workUnsequenceTsFileProcessors = new TreeMap<>();

  private final Deque<ByteBuffer> walByteBufferPool = new LinkedList<>();

  // upgrading sequence TsFile resource list
  private List<TsFileResource> upgradeSeqFileList = new LinkedList<>();
  /** sequence tsfile processors which are closing */
  private CopyOnReadLinkedList<TsFileProcessor> closingSequenceTsFileProcessor =
      new CopyOnReadLinkedList<>();
  // upgrading unsequence TsFile resource list
  private List<TsFileResource> upgradeUnseqFileList = new LinkedList<>();

  /** unsequence tsfile processors which are closing */
  private CopyOnReadLinkedList<TsFileProcessor> closingUnSequenceTsFileProcessor =
      new CopyOnReadLinkedList<>();

  private AtomicInteger upgradeFileCount = new AtomicInteger();

  /** virtual storage group id */
  private String virtualStorageGroupId;
  /** logical storage group name */
  private String logicalStorageGroupName;
  /** storage group system directory */
  private File storageGroupSysDir;
  /** manage seqFileList and unSeqFileList */
  private TsFileManager tsFileManager;

  /** manage tsFileResource degrade */
  private TsFileResourceManager tsFileResourceManager = TsFileResourceManager.getInstance();

  /**
   * time partition id -> version controller which assigns a version for each MemTable and
   * deletion/update such that after they are persisted, the order of insertions, deletions and
   * updates can be re-determined. Will be empty if there are not MemTables in memory.
   */
  private HashMap<Long, VersionController> timePartitionIdVersionControllerMap = new HashMap<>();
  /**
   * when the data in a storage group is older than dataTTL, it is considered invalid and will be
   * eventually removed.
   */
  private long dataTTL = Long.MAX_VALUE;
  /** file system factory (local or hdfs) */
  private FSFactory fsFactory = FSFactoryProducer.getFSFactory();
  /** file flush policy */
  private TsFileFlushPolicy fileFlushPolicy;
  /**
   * The max file versions in each partition. By recording this, if several IoTDB instances have the
   * same policy of closing file and their ingestion is identical, then files of the same version in
   * different IoTDB instance will have identical data, providing convenience for data comparison
   * across different instances. partition number -> max version number
   */
  private Map<Long, Long> partitionMaxFileVersions = new HashMap<>();
  /** storage group info for mem control */
  private StorageGroupInfo storageGroupInfo = new StorageGroupInfo(this);
  /**
   * Record the device number of the last TsFile in each storage group, which is applied to
   * initialize the array size of DeviceTimeIndex. It is reasonable to assume that the adjacent
   * files should have similar numbers of devices. Default value: INIT_ARRAY_SIZE = 64
   */
  private int deviceNumInLastClosedTsFile = DeviceTimeIndex.INIT_ARRAY_SIZE;
  /** whether it's ready from recovery */
  private boolean isReady = false;
  /** close file listeners */
  private List<CloseFileListener> customCloseFileListeners = Collections.emptyList();
  /** flush listeners */
  private List<FlushListener> customFlushListeners = Collections.emptyList();

  private int currentWalPoolSize = 0;

  // this field is used to avoid when one writer release bytebuffer back to pool,
  // and the next writer has already arrived, but the check thread get the lock first, it find the
  // pool
  // is not empty, so it free the memory. When the next writer get the lock, it will apply the
  // memory again.
  // So our free memory strategy is only when the expected size less than the current pool size
  // and the pool is not empty and the time interval since the pool is not empty is larger than
  // DEFAULT_POOL_TRIM_INTERVAL_MILLIS
  private long timeWhenPoolNotEmpty = Long.MAX_VALUE;

  private LastFlushTimeManager lastFlushTimeManager = new LastFlushTimeManager();

  /**
   * record the insertWriteLock in SG is being hold by which method, it will be empty string if on
   * one holds the insertWriteLock
   */
  private String insertWriteLockHolder = "";

  private ScheduledExecutorService timedCompactionScheduleTask;

  public static final long COMPACTION_TASK_SUBMIT_DELAY = 20L * 1000L;

  private IDTable idTable;

  /**
   * get the direct byte buffer from pool, each fetch contains two ByteBuffer, return null if fetch
   * fails
   */
  public ByteBuffer[] getWalDirectByteBuffer() {
    ByteBuffer[] res = new ByteBuffer[2];
    synchronized (walByteBufferPool) {
      long startTime = System.nanoTime();
      int MAX_WAL_BYTEBUFFER_NUM =
          config.getConcurrentWritingTimePartition()
              * config.getMaxWalBytebufferNumForEachPartition();
      while (walByteBufferPool.isEmpty() && currentWalPoolSize + 2 > MAX_WAL_BYTEBUFFER_NUM) {
        try {
          walByteBufferPool.wait();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          logger.error(
              "getDirectByteBuffer occurs error while waiting for DirectByteBuffer" + "group {}-{}",
              logicalStorageGroupName,
              virtualStorageGroupId,
              e);
        }
        logger.info(
            "Waiting {} ms for wal direct byte buffer.",
            (System.nanoTime() - startTime) / 1_000_000);
      }
      // If the queue is not empty, it must have at least two.
      if (!walByteBufferPool.isEmpty()) {
        res[0] = walByteBufferPool.pollFirst();
        res[1] = walByteBufferPool.pollFirst();
      } else {
        // if the queue is empty and current size is less than MAX_BYTEBUFFER_NUM
        // we can construct another two more new byte buffer
        try {
          res[0] = ByteBuffer.allocateDirect(WAL_BUFFER_SIZE);
          res[1] = ByteBuffer.allocateDirect(WAL_BUFFER_SIZE);
          currentWalPoolSize += 2;
        } catch (OutOfMemoryError e) {
          logger.error("Allocate ByteBuffers error", e);
          if (res[0] != null) {
            MmapUtil.clean((MappedByteBuffer) res[0]);
          }
          if (res[1] != null) {
            MmapUtil.clean((MappedByteBuffer) res[1]);
          }
          return null;
        }
      }
      // if the pool is empty, set the time back to MAX_VALUE
      if (walByteBufferPool.isEmpty()) {
        timeWhenPoolNotEmpty = Long.MAX_VALUE;
      }
    }
    return res;
  }

  /** put the byteBuffer back to pool */
  public void releaseWalBuffer(ByteBuffer[] byteBuffers) {
    for (ByteBuffer byteBuffer : byteBuffers) {
      byteBuffer.clear();
    }
    synchronized (walByteBufferPool) {
      // if the pool is empty before, update the time
      if (walByteBufferPool.isEmpty()) {
        timeWhenPoolNotEmpty = System.nanoTime();
      }
      walByteBufferPool.addLast(byteBuffers[0]);
      walByteBufferPool.addLast(byteBuffers[1]);
      walByteBufferPool.notifyAll();
    }
  }

  /** trim the size of the pool and release the memory of needless direct byte buffer */
  private void trimTask() {
    synchronized (walByteBufferPool) {
      int expectedSize =
          (workSequenceTsFileProcessors.size() + workUnsequenceTsFileProcessors.size()) * 2;
      // the unit is ms
      long poolNotEmptyIntervalInMS = (System.nanoTime() - timeWhenPoolNotEmpty) / 1_000_000;
      // only when the expected size less than the current pool size
      // and the pool is not empty and the time interval since the pool is not empty is larger than
      // 10s
      // we will trim the size to expectedSize until the pool is empty
      while (expectedSize < currentWalPoolSize
          && !walByteBufferPool.isEmpty()
          && poolNotEmptyIntervalInMS >= config.getWalPoolTrimIntervalInMS()) {
        MmapUtil.clean((MappedByteBuffer) walByteBufferPool.removeLast());
        MmapUtil.clean((MappedByteBuffer) walByteBufferPool.removeLast());
        currentWalPoolSize -= 2;
      }
    }
  }

  /**
   * constrcut a storage group processor
   *
   * @param systemDir system dir path
   * @param virtualStorageGroupId virtual storage group id e.g. 1
   * @param fileFlushPolicy file flush policy
   * @param logicalStorageGroupName logical storage group name e.g. root.sg1
   */
  public VirtualStorageGroupProcessor(
      String systemDir,
      String virtualStorageGroupId,
      TsFileFlushPolicy fileFlushPolicy,
      String logicalStorageGroupName)
      throws StorageGroupProcessorException {
    this.virtualStorageGroupId = virtualStorageGroupId;
    this.logicalStorageGroupName = logicalStorageGroupName;
    this.fileFlushPolicy = fileFlushPolicy;

    storageGroupSysDir = SystemFileFactory.INSTANCE.getFile(systemDir, virtualStorageGroupId);
    this.tsFileManager =
        new TsFileManager(
            logicalStorageGroupName, virtualStorageGroupId, storageGroupSysDir.getPath());
    if (storageGroupSysDir.mkdirs()) {
      logger.info(
          "Storage Group system Directory {} doesn't exist, create it",
          storageGroupSysDir.getPath());
    } else if (!storageGroupSysDir.exists()) {
      logger.error("create Storage Group system Directory {} failed", storageGroupSysDir.getPath());
    }

    // use id table
    if (config.isEnableIDTable()) {
      try {
        idTable = IDTableManager.getInstance().getIDTable(new PartialPath(logicalStorageGroupName));
      } catch (IllegalPathException e) {
        logger.error("failed to create id table");
      }
    }
    // recover tsfiles
    recover();

    if (MetricConfigDescriptor.getInstance().getMetricConfig().getEnableMetric()) {
      MetricsService.getInstance()
          .getMetricManager()
          .getOrCreateAutoGauge(
              Metric.MEM.toString(),
              storageGroupInfo.getMemCost(),
              Long::longValue,
              Tag.NAME.toString(),
              "storageGroup");
    }

    // start trim task at last
    ScheduledExecutorService executorService =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
            ThreadName.WAL_TRIM.getName()
                + "-"
                + logicalStorageGroupName
                + "-"
                + virtualStorageGroupId);
    executorService.scheduleWithFixedDelay(
        this::trimTask,
        config.getWalPoolTrimIntervalInMS(),
        config.getWalPoolTrimIntervalInMS(),
        TimeUnit.MILLISECONDS);
  }

  public String getLogicalStorageGroupName() {
    return logicalStorageGroupName;
  }

  public boolean isReady() {
    return isReady;
  }

  public void setReady(boolean ready) {
    isReady = ready;
  }

  private Map<Long, List<TsFileResource>> splitResourcesByPartition(
      List<TsFileResource> resources) {
    Map<Long, List<TsFileResource>> ret = new HashMap<>();
    for (TsFileResource resource : resources) {
      ret.computeIfAbsent(resource.getTimePartition(), l -> new ArrayList<>()).add(resource);
    }
    return ret;
  }

  /** recover from file */
  private void recover() throws StorageGroupProcessorException {
    logger.info(
        String.format(
            "start recovering virtual storage group %s[%s]",
            logicalStorageGroupName, virtualStorageGroupId));

    try {
      recoverInnerSpaceCompaction(true);
      recoverInnerSpaceCompaction(false);
    } catch (Exception e) {
      throw new StorageGroupProcessorException(e);
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
      Map<Long, List<TsFileResource>> partitionTmpSeqTsFiles =
          splitResourcesByPartition(tmpSeqTsFiles);
      Map<Long, List<TsFileResource>> partitionTmpUnseqTsFiles =
          splitResourcesByPartition(tmpUnseqTsFiles);
      for (List<TsFileResource> value : partitionTmpSeqTsFiles.values()) {
        recoverTsFiles(value, true);
      }
      for (List<TsFileResource> value : partitionTmpUnseqTsFiles.values()) {
        recoverTsFiles(value, false);
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
      throw new StorageGroupProcessorException(e);
    }

    List<TsFileResource> seqTsFileResources = tsFileManager.getTsFileList(true);
    for (TsFileResource resource : seqTsFileResources) {
      long timePartitionId = resource.getTimePartition();
      Map<String, Long> endTimeMap = new HashMap<>();
      for (String deviceId : resource.getDevices()) {
        long endTime = resource.getEndTime(deviceId);
        endTimeMap.put(deviceId, endTime);
      }
      lastFlushTimeManager.setLastTimeAll(timePartitionId, endTimeMap);
      lastFlushTimeManager.setFlushedTimeAll(timePartitionId, endTimeMap);
      lastFlushTimeManager.setGlobalFlushedTimeAll(endTimeMap);
    }

    // recover and start timed compaction thread
    initCompaction();

    logger.info(
        String.format(
            "the virtual storage group %s[%s] is recovered successfully",
            logicalStorageGroupName, virtualStorageGroupId));
  }

  private void initCompaction() {
    timedCompactionScheduleTask =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
            ThreadName.COMPACTION_SCHEDULE.getName()
                + "-"
                + logicalStorageGroupName
                + "-"
                + virtualStorageGroupId);

    CompactionTaskManager.getInstance()
        .submitTask(
            logicalStorageGroupName + "-" + virtualStorageGroupId,
            0,
            new CompactionRecoverTask(
                this::submitTimedCompactionTask,
                tsFileManager,
                logicalStorageGroupName,
                virtualStorageGroupId));
  }

  private void recoverInnerSpaceCompaction(boolean isSequence) throws Exception {
    // search compaction log for SizeTieredCompaction
    List<String> dirs;
    if (isSequence) {
      dirs = DirectoryManager.getInstance().getAllSequenceFileFolders();
    } else {
      dirs = DirectoryManager.getInstance().getAllUnSequenceFileFolders();
    }
    for (String dir : dirs) {
      File storageGroupDir =
          new File(
              dir
                  + File.separator
                  + logicalStorageGroupName
                  + File.separator
                  + virtualStorageGroupId);
      if (!storageGroupDir.exists()) {
        return;
      }
      File[] timePartitionDirs = storageGroupDir.listFiles();
      if (timePartitionDirs == null) {
        return;
      }
      for (File timePartitionDir : timePartitionDirs) {
        if (!timePartitionDir.isDirectory()
            || !Pattern.compile("[0-9]*").matcher(timePartitionDir.getName()).matches()) {
          continue;
        }
        File[] compactionLogs =
            InnerSpaceCompactionUtils.findInnerSpaceCompactionLogs(timePartitionDir.getPath());
        for (File compactionLog : compactionLogs) {
          IoTDBDescriptor.getInstance()
              .getConfig()
              .getInnerCompactionStrategy()
              .getCompactionRecoverTask(
                  tsFileManager.getStorageGroupName(),
                  tsFileManager.getVirtualStorageGroup(),
                  Long.parseLong(
                      timePartitionDir
                          .getPath()
                          .substring(timePartitionDir.getPath().lastIndexOf(File.separator) + 1)),
                  compactionLog,
                  timePartitionDir.getPath(),
                  isSequence)
              .call();
        }
      }
    }

    // search compaction log for old LevelCompaction
    File logFile =
        FSFactoryProducer.getFSFactory()
            .getFile(
                storageGroupSysDir.getAbsolutePath(),
                logicalStorageGroupName + COMPACTION_LOG_NAME);
    if (logFile.exists()) {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .getInnerCompactionStrategy()
          .getCompactionRecoverTask(
              tsFileManager.getStorageGroupName(),
              tsFileManager.getVirtualStorageGroup(),
              -1,
              logFile,
              logFile.getParent(),
              isSequence)
          .call();
    }
  }

  private void submitTimedCompactionTask() {
    timedCompactionScheduleTask.scheduleWithFixedDelay(
        this::executeCompaction,
        COMPACTION_TASK_SUBMIT_DELAY,
        COMPACTION_TASK_SUBMIT_DELAY,
        TimeUnit.MILLISECONDS);
  }

  private void updatePartitionFileVersion(long partitionNum, long fileVersion) {
    long oldVersion = partitionMaxFileVersions.getOrDefault(partitionNum, 0L);
    if (fileVersion > oldVersion) {
      partitionMaxFileVersions.put(partitionNum, fileVersion);
    }
  }

  /**
   * use old seq file to update latestTimeForEachDevice, globalLatestFlushedTimeForEachDevice,
   * partitionLatestFlushedTimeForEachDevice and timePartitionIdVersionControllerMap
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
        lastFlushTimeManager.setLastTime(endTimePartitionId, deviceId, endTime);
        lastFlushTimeManager.setGlobalFlushedTime(deviceId, endTime);

        // set all the covered partition's LatestFlushedTime
        long partitionId = StorageEngine.getTimePartition(resource.getStartTime(deviceId));
        while (partitionId <= endTimePartitionId) {
          lastFlushTimeManager.setFlushedTime(partitionId, deviceId, endTime);
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
      throws IOException, StorageGroupProcessorException {
    List<File> tsFiles = new ArrayList<>();
    List<File> upgradeFiles = new ArrayList<>();
    for (String baseDir : folders) {
      File fileFolder =
          fsFactory.getFile(
              baseDir + File.separator + logicalStorageGroupName, virtualStorageGroupId);
      if (!fileFolder.exists()) {
        continue;
      }

      // old version
      // some TsFileResource may be being persisted when the system crashed, try recovering such
      // resources
      continueFailedRenames(fileFolder, TEMP_SUFFIX);

      // some TsFiles were going to be replaced by the merged files when the system crashed and
      // the process was interrupted before the merged files could be named
      continueFailedRenames(fileFolder, MERGE_SUFFIX);

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

            // some TsFiles were going to be replaced by the merged files when the system crashed
            // and
            // the process was interrupted before the merged files could be named
            continueFailedRenames(partitionFolder, MERGE_SUFFIX);

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
      fileResource.setClosed(true);
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

  /** check if the tsfile's time is smaller than system current time */
  private void checkTsFileTime(File tsFile) throws StorageGroupProcessorException {
    String[] items = tsFile.getName().replace(TSFILE_SUFFIX, "").split(FILE_NAME_SEPARATOR);
    long fileTime = Long.parseLong(items[0]);
    long currentTime = System.currentTimeMillis();
    if (fileTime > currentTime) {
      throw new StorageGroupProcessorException(
          String.format(
              "virtual storage group %s[%s] is down, because the time of tsfile %s is larger than system current time, "
                  + "file time is %d while system current time is %d, please check it.",
              logicalStorageGroupName,
              virtualStorageGroupId,
              tsFile.getAbsolutePath(),
              fileTime,
              currentTime));
    }
  }

  private void recoverTsFiles(List<TsFileResource> tsFiles, boolean isSeq) throws IOException {
    for (int i = 0; i < tsFiles.size(); i++) {
      TsFileResource tsFileResource = tsFiles.get(i);
      long timePartitionId = tsFileResource.getTimePartition();

      TsFileRecoverPerformer recoverPerformer =
          new TsFileRecoverPerformer(
              logicalStorageGroupName
                  + File.separator
                  + virtualStorageGroupId
                  + FILE_NAME_SEPARATOR,
              tsFileResource,
              isSeq,
              i == tsFiles.size() - 1,
              this);

      RestorableTsFileIOWriter writer = null;
      try {
        // this tsfile is not zero level, no need to perform redo wal
        if (TsFileResource.getInnerCompactionCount(tsFileResource.getTsFile().getName()) > 0) {
          writer =
              recoverPerformer.recover(false, this::getWalDirectByteBuffer, this::releaseWalBuffer);
          if (writer != null && writer.hasCrashed()) {
            tsFileManager.addForRecover(tsFileResource, isSeq);
          } else {
            tsFileResource.setClosed(true);
            tsFileManager.add(tsFileResource, isSeq);
            tsFileResourceManager.registerSealedTsFileResource(tsFileResource);
          }
          continue;
        } else {
          writer =
              recoverPerformer.recover(true, this::getWalDirectByteBuffer, this::releaseWalBuffer);
        }

        if (i != tsFiles.size() - 1 || writer == null || !writer.canWrite()) {
          // not the last file or cannot write, just close it
          tsFileResource.close();
          tsFileResourceManager.registerSealedTsFileResource(tsFileResource);
        } else if (writer.canWrite()) {
          // the last file is not closed, continue writing to in
          TsFileProcessor tsFileProcessor;
          if (isSeq) {
            tsFileProcessor =
                new TsFileProcessor(
                    virtualStorageGroupId,
                    storageGroupInfo,
                    tsFileResource,
                    this::closeUnsealedTsFileProcessorCallBack,
                    this::updateLatestFlushTimeCallback,
                    true,
                    writer);
            if (enableMemControl) {
              TsFileProcessorInfo tsFileProcessorInfo = new TsFileProcessorInfo(storageGroupInfo);
              tsFileProcessor.setTsFileProcessorInfo(tsFileProcessorInfo);
              this.storageGroupInfo.initTsFileProcessorInfo(tsFileProcessor);
            }
            workSequenceTsFileProcessors.put(timePartitionId, tsFileProcessor);
          } else {
            tsFileProcessor =
                new TsFileProcessor(
                    virtualStorageGroupId,
                    storageGroupInfo,
                    tsFileResource,
                    this::closeUnsealedTsFileProcessorCallBack,
                    this::unsequenceFlushCallback,
                    false,
                    writer);
            if (enableMemControl) {
              TsFileProcessorInfo tsFileProcessorInfo = new TsFileProcessorInfo(storageGroupInfo);
              tsFileProcessor.setTsFileProcessorInfo(tsFileProcessorInfo);
              this.storageGroupInfo.initTsFileProcessorInfo(tsFileProcessor);
            }
            workUnsequenceTsFileProcessors.put(timePartitionId, tsFileProcessor);
          }
          tsFileResource.setProcessor(tsFileProcessor);
          tsFileResource.removeResourceFile();
          tsFileProcessor.setTimeRangeId(timePartitionId);
          writer.makeMetadataVisible();
          if (enableMemControl) {
            // get chunkMetadata size
            long chunkMetadataSize = 0;
            for (Map<String, List<ChunkMetadata>> metaMap :
                writer.getMetadatasForQuery().values()) {
              for (List<ChunkMetadata> metadatas : metaMap.values()) {
                for (ChunkMetadata chunkMetadata : metadatas) {
                  chunkMetadataSize += chunkMetadata.calculateRamSize();
                }
              }
            }
            tsFileProcessor.getTsFileProcessorInfo().addTSPMemCost(chunkMetadataSize);
          }
        }
        tsFileManager.add(tsFileResource, isSeq);
      } catch (StorageGroupProcessorException | IOException e) {
        logger.warn(
            "Skip TsFile: {} because of error in recover: ", tsFileResource.getTsFilePath(), e);
        continue;
      } finally {
        if (writer != null) {
          writer.close();
        }
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
   * insert one row of data
   *
   * @param insertRowPlan one row of data
   */
  public void insert(InsertRowPlan insertRowPlan)
      throws WriteProcessException, TriggerExecutionException {
    // reject insertions that are out of ttl
    if (!isAlive(insertRowPlan.getTime())) {
      throw new OutOfTTLException(insertRowPlan.getTime(), (System.currentTimeMillis() - dataTTL));
    }
    writeLock("InsertRow");
    try {
      // init map
      long timePartitionId = StorageEngine.getTimePartition(insertRowPlan.getTime());

      lastFlushTimeManager.ensureFlushedTimePartition(timePartitionId);

      boolean isSequence =
          insertRowPlan.getTime()
              > lastFlushTimeManager.getFlushedTime(
                  timePartitionId, insertRowPlan.getDevicePath().getFullPath());

      // is unsequence and user set config to discard out of order data
      if (!isSequence
          && IoTDBDescriptor.getInstance().getConfig().isEnableDiscardOutOfOrderData()) {
        return;
      }

      lastFlushTimeManager.ensureLastTimePartition(timePartitionId);

      // fire trigger before insertion
      TriggerEngine.fire(TriggerEvent.BEFORE_INSERT, insertRowPlan);
      // insert to sequence or unSequence file
      insertToTsFileProcessor(insertRowPlan, isSequence, timePartitionId);
      // fire trigger after insertion
      TriggerEngine.fire(TriggerEvent.AFTER_INSERT, insertRowPlan);
    } finally {
      writeUnlock();
    }
  }

  /**
   * Insert a tablet (rows belonging to the same devices) into this storage group.
   *
   * @throws BatchProcessException if some of the rows failed to be inserted
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void insertTablet(InsertTabletPlan insertTabletPlan)
      throws BatchProcessException, TriggerExecutionException {

    writeLock("insertTablet");
    try {
      TSStatus[] results = new TSStatus[insertTabletPlan.getRowCount()];
      Arrays.fill(results, RpcUtils.SUCCESS_STATUS);
      boolean noFailure = true;

      /*
       * assume that batch has been sorted by client
       */
      int loc = 0;
      while (loc < insertTabletPlan.getRowCount()) {
        long currTime = insertTabletPlan.getTimes()[loc];
        // skip points that do not satisfy TTL
        if (!isAlive(currTime)) {
          results[loc] =
              RpcUtils.getStatus(
                  TSStatusCode.OUT_OF_TTL_ERROR,
                  "time " + currTime + " in current line is out of TTL: " + dataTTL);
          loc++;
          noFailure = false;
        } else {
          break;
        }
      }
      // loc pointing at first legal position
      if (loc == insertTabletPlan.getRowCount()) {
        throw new BatchProcessException(results);
      }

      // fire trigger before insertion
      final int firePosition = loc;
      TriggerEngine.fire(TriggerEvent.BEFORE_INSERT, insertTabletPlan, firePosition);

      // before is first start point
      int before = loc;
      // before time partition
      long beforeTimePartition =
          StorageEngine.getTimePartition(insertTabletPlan.getTimes()[before]);
      // init map
      long lastFlushTime =
          lastFlushTimeManager.ensureFlushedTimePartitionAndInit(
              beforeTimePartition, insertTabletPlan.getDevicePath().getFullPath(), Long.MIN_VALUE);
      // if is sequence
      boolean isSequence = false;
      while (loc < insertTabletPlan.getRowCount()) {
        long time = insertTabletPlan.getTimes()[loc];
        long curTimePartition = StorageEngine.getTimePartition(time);
        // start next partition
        if (curTimePartition != beforeTimePartition) {
          // insert last time partition
          if (isSequence
              || !IoTDBDescriptor.getInstance().getConfig().isEnableDiscardOutOfOrderData()) {
            noFailure =
                insertTabletToTsFileProcessor(
                        insertTabletPlan, before, loc, isSequence, results, beforeTimePartition)
                    && noFailure;
          }
          // re initialize
          before = loc;
          beforeTimePartition = curTimePartition;
          lastFlushTime =
              lastFlushTimeManager.ensureFlushedTimePartitionAndInit(
                  beforeTimePartition,
                  insertTabletPlan.getDevicePath().getFullPath(),
                  Long.MIN_VALUE);

          isSequence = false;
        }
        // still in this partition
        else {
          // judge if we should insert sequence
          if (!isSequence && time > lastFlushTime) {
            // insert into unsequence and then start sequence
            if (!IoTDBDescriptor.getInstance().getConfig().isEnableDiscardOutOfOrderData()) {
              noFailure =
                  insertTabletToTsFileProcessor(
                          insertTabletPlan, before, loc, false, results, beforeTimePartition)
                      && noFailure;
            }
            before = loc;
            isSequence = true;
          }
          loc++;
        }
      }

      // do not forget last part
      if (before < loc
          && (isSequence
              || !IoTDBDescriptor.getInstance().getConfig().isEnableDiscardOutOfOrderData())) {
        noFailure =
            insertTabletToTsFileProcessor(
                    insertTabletPlan, before, loc, isSequence, results, beforeTimePartition)
                && noFailure;
      }
      long globalLatestFlushedTime =
          lastFlushTimeManager.getGlobalFlushedTime(insertTabletPlan.getDevicePath().getFullPath());
      tryToUpdateBatchInsertLastCache(insertTabletPlan, globalLatestFlushedTime);

      if (!noFailure) {
        throw new BatchProcessException(results);
      }

      // fire trigger after insertion
      TriggerEngine.fire(TriggerEvent.AFTER_INSERT, insertTabletPlan, firePosition);
    } finally {
      writeUnlock();
    }
  }

  /** @return whether the given time falls in ttl */
  private boolean isAlive(long time) {
    return dataTTL == Long.MAX_VALUE || (System.currentTimeMillis() - time) <= dataTTL;
  }

  /**
   * insert batch to tsfile processor thread-safety that the caller need to guarantee The rows to be
   * inserted are in the range [start, end) Null value in each column values will be replaced by the
   * subsequent non-null value, e.g., {1, null, 3, null, 5} will be {1, 3, 5, null, 5}
   *
   * @param insertTabletPlan insert a tablet of a device
   * @param sequence whether is sequence
   * @param start start index of rows to be inserted in insertTabletPlan
   * @param end end index of rows to be inserted in insertTabletPlan
   * @param results result array
   * @param timePartitionId time partition id
   * @return false if any failure occurs when inserting the tablet, true otherwise
   */
  private boolean insertTabletToTsFileProcessor(
      InsertTabletPlan insertTabletPlan,
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
      tsFileProcessor.insertTablet(insertTabletPlan, start, end, results);
    } catch (WriteProcessRejectException e) {
      logger.warn("insert to TsFileProcessor rejected, {}", e.getMessage());
      return false;
    } catch (WriteProcessException e) {
      logger.error("insert to TsFileProcessor error ", e);
      return false;
    }

    lastFlushTimeManager.ensureLastTimePartition(timePartitionId);
    // try to update the latest time of the device of this tsRecord
    if (sequence) {
      lastFlushTimeManager.updateLastTime(
          timePartitionId,
          insertTabletPlan.getDevicePath().getFullPath(),
          insertTabletPlan.getTimes()[end - 1]);
    }

    // check memtable size and may async try to flush the work memtable
    if (tsFileProcessor.shouldFlush()) {
      fileFlushPolicy.apply(this, tsFileProcessor, sequence);
    }
    return true;
  }

  private void tryToUpdateBatchInsertLastCache(InsertTabletPlan plan, Long latestFlushedTime) {
    if (!IoTDBDescriptor.getInstance().getConfig().isLastCacheEnabled()) {
      return;
    }
    IMeasurementMNode[] mNodes = plan.getMeasurementMNodes();
    for (int i = 0; i < mNodes.length; i++) {
      if (plan.getColumns()[i] == null) {
        continue;
      }
      // Update cached last value with high priority
      if (mNodes[i] == null) {
        IoTDB.metaManager.updateLastCache(
            plan.getDevicePath().concatNode(plan.getMeasurements()[i]),
            plan.composeLastTimeValuePair(i),
            true,
            latestFlushedTime);
      } else {
        // in stand alone version, the seriesPath is not needed, just use measurementMNodes[i] to
        // update last cache
        IoTDB.metaManager.updateLastCache(
            mNodes[i], plan.composeLastTimeValuePair(i), true, latestFlushedTime);
      }
    }
  }

  private void insertToTsFileProcessor(
      InsertRowPlan insertRowPlan, boolean sequence, long timePartitionId)
      throws WriteProcessException {
    TsFileProcessor tsFileProcessor = getOrCreateTsFileProcessor(timePartitionId, sequence);
    if (tsFileProcessor == null) {
      return;
    }

    tsFileProcessor.insert(insertRowPlan);

    // try to update the latest time of the device of this tsRecord
    lastFlushTimeManager.updateLastTime(
        timePartitionId, insertRowPlan.getDevicePath().getFullPath(), insertRowPlan.getTime());

    long globalLatestFlushTime =
        lastFlushTimeManager.getGlobalFlushedTime(insertRowPlan.getDevicePath().getFullPath());

    tryToUpdateInsertLastCache(insertRowPlan, globalLatestFlushTime);

    // check memtable size and may asyncTryToFlush the work memtable
    if (tsFileProcessor.shouldFlush()) {
      fileFlushPolicy.apply(this, tsFileProcessor, sequence);
    }
  }

  private void tryToUpdateInsertLastCache(InsertRowPlan plan, Long latestFlushedTime) {
    if (!IoTDBDescriptor.getInstance().getConfig().isLastCacheEnabled()) {
      return;
    }
    IMeasurementMNode[] mNodes = plan.getMeasurementMNodes();
    for (int i = 0; i < mNodes.length; i++) {
      if (plan.getValues()[i] == null) {
        continue;
      }
      // Update cached last value with high priority
      if (mNodes[i] == null) {
        IoTDB.metaManager.updateLastCache(
            plan.getDevicePath().concatNode(plan.getMeasurements()[i]),
            plan.composeTimeValuePair(i),
            true,
            latestFlushedTime);
      } else {
        // in stand alone version, the seriesPath is not needed, just use measurementMNodes[i] to
        // update last cache
        IoTDB.metaManager.updateLastCache(
            mNodes[i], plan.composeTimeValuePair(i), true, latestFlushedTime);
      }
    }
  }

  /**
   * mem control module use this method to flush memtable
   *
   * @param tsFileProcessor tsfile processor in which memtable to be flushed
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
      IoTDBDescriptor.getInstance().getConfig().setReadOnly(true);
    } catch (IOException e) {
      logger.error(
          "meet IOException when creating TsFileProcessor, change system mode to read-only", e);
      IoTDBDescriptor.getInstance().getConfig().setReadOnly(true);
    }
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
            logicalStorageGroupName,
            virtualStorageGroupId,
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
              logicalStorageGroupName + File.separator + virtualStorageGroupId,
              fsFactory.getFileWithParent(filePath),
              storageGroupInfo,
              this::closeUnsealedTsFileProcessorCallBack,
              this::updateLatestFlushTimeCallback,
              true);
    } else {
      tsFileProcessor =
          new TsFileProcessor(
              logicalStorageGroupName + File.separator + virtualStorageGroupId,
              fsFactory.getFileWithParent(filePath),
              storageGroupInfo,
              this::closeUnsealedTsFileProcessorCallBack,
              this::unsequenceFlushCallback,
              false);
    }

    if (enableMemControl) {
      TsFileProcessorInfo tsFileProcessorInfo = new TsFileProcessorInfo(storageGroupInfo);
      tsFileProcessor.setTsFileProcessorInfo(tsFileProcessorInfo);
      this.storageGroupInfo.initTsFileProcessorInfo(tsFileProcessor);
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
                logicalStorageGroupName + "-" + this.virtualStorageGroupId,
                (System.currentTimeMillis() - startTime) / 1000);
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error(
            "syncCloseOneTsFileProcessor error occurs while waiting for closing the storage "
                + "group {}",
            logicalStorageGroupName + "-" + virtualStorageGroupId,
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
      updateEndTimeMap(tsFileProcessor);
      tsFileProcessor.asyncClose();

      workSequenceTsFileProcessors.remove(tsFileProcessor.getTimeRangeId());
      // if unsequence files don't contain this time range id, we should remove it's version
      // controller
      if (!workUnsequenceTsFileProcessors.containsKey(tsFileProcessor.getTimeRangeId())) {
        timePartitionIdVersionControllerMap.remove(tsFileProcessor.getTimeRangeId());
      }
      logger.info(
          "close a sequence tsfile processor {}",
          logicalStorageGroupName + "-" + virtualStorageGroupId);
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
   * delete the storageGroup's own folder in folder data/system/storage_groups
   *
   * @param systemDir system dir
   */
  public void deleteFolder(String systemDir) {
    logger.info(
        "{} will close all files for deleting data folder {}",
        logicalStorageGroupName + "-" + virtualStorageGroupId,
        systemDir);
    writeLock("deleteFolder");
    try {
      File storageGroupFolder =
          SystemFileFactory.INSTANCE.getFile(systemDir, virtualStorageGroupId);
      if (storageGroupFolder.exists()) {
        org.apache.iotdb.db.utils.FileUtils.deleteDirectory(storageGroupFolder);
      }
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

  /** release wal buffer */
  public void releaseWalDirectByteBufferPool() {
    synchronized (walByteBufferPool) {
      while (!walByteBufferPool.isEmpty()) {
        MmapUtil.clean((MappedByteBuffer) walByteBufferPool.removeFirst());
        currentWalPoolSize--;
      }
    }
  }

  /** delete tsfile */
  public void syncDeleteDataFiles() {
    logger.info(
        "{} will close all files for deleting data files",
        logicalStorageGroupName + "-" + virtualStorageGroupId);
    writeLock("syncDeleteDataFiles");
    try {

      syncCloseAllWorkingTsFileProcessors();
      // normally, mergingModification is just need to be closed by after a merge task is finished.
      // we close it here just for IT test.
      closeAllResources();
      List<String> folder = DirectoryManager.getInstance().getAllSequenceFileFolders();
      folder.addAll(DirectoryManager.getInstance().getAllUnSequenceFileFolders());
      deleteAllSGFolders(folder);

      this.workSequenceTsFileProcessors.clear();
      this.workUnsequenceTsFileProcessors.clear();
      this.tsFileManager.clear();
      lastFlushTimeManager.clearFlushedTime();
      lastFlushTimeManager.clearGlobalFlushedTime();
      lastFlushTimeManager.clearLastTime();
    } finally {
      writeUnlock();
    }
  }

  private void deleteAllSGFolders(List<String> folder) {
    for (String tsfilePath : folder) {
      File storageGroupFolder =
          fsFactory.getFile(
              tsfilePath, logicalStorageGroupName + File.separator + virtualStorageGroupId);
      if (storageGroupFolder.exists()) {
        org.apache.iotdb.db.utils.FileUtils.deleteDirectory(storageGroupFolder);
      }
    }
  }

  /** Iterate each TsFile and try to lock and remove those out of TTL. */
  public synchronized void checkFilesTTL() {
    if (dataTTL == Long.MAX_VALUE) {
      logger.debug(
          "{}: TTL not set, ignore the check",
          logicalStorageGroupName + "-" + virtualStorageGroupId);
      return;
    }
    long ttlLowerBound = System.currentTimeMillis() - dataTTL;
    logger.debug(
        "{}: TTL removing files before {}",
        logicalStorageGroupName + "-" + virtualStorageGroupId,
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

    // prevent new merges and queries from choosing this file
    resource.setDeleted(true);

    // ensure that the file is not used by any queries
    if (resource.tryWriteLock()) {
      try {
        // try to delete physical data file
        resource.remove();
        tsFileManager.remove(resource, isSeq);
        logger.info(
            "Removed a file {} before {} by ttl ({}ms)",
            resource.getTsFilePath(),
            new Date(ttlLowerBound),
            dataTTL);
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
              "Exceed sequence memtable flush interval, so flush working memtable of time partition {} in storage group {}[{}]",
              tsFileProcessor.getTimeRangeId(),
              logicalStorageGroupName,
              virtualStorageGroupId);
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
              "Exceed unsequence memtable flush interval, so flush working memtable of time partition {} in storage group {}[{}]",
              tsFileProcessor.getTimeRangeId(),
              logicalStorageGroupName,
              virtualStorageGroupId);
          fileFlushPolicy.apply(this, tsFileProcessor, tsFileProcessor.isSequence());
        }
      }
    } finally {
      writeUnlock();
    }
  }

  public void timedCloseTsFileProcessor() {
    writeLock("timedCloseTsFileProcessor");
    try {
      List<TsFileProcessor> seqTsFileProcessors =
          new ArrayList<>(workSequenceTsFileProcessors.values());
      long timeLowerBound =
          System.currentTimeMillis() - config.getCloseTsFileIntervalAfterFlushing();
      for (TsFileProcessor tsFileProcessor : seqTsFileProcessors) {
        // working memtable is null(no more write ops) and last flush time exceeds close interval
        if (tsFileProcessor.getWorkMemTableCreatedTime() == Long.MAX_VALUE
            && tsFileProcessor.getLastWorkMemtableFlushTime() < timeLowerBound) {
          logger.info(
              "Exceed tsfile close interval, so close TsFileProcessor of time partition {} in storage group {}[{}]",
              tsFileProcessor.getTimeRangeId(),
              logicalStorageGroupName,
              virtualStorageGroupId);
          asyncCloseOneTsFileProcessor(true, tsFileProcessor);
        }
      }

      List<TsFileProcessor> unSeqTsFileProcessors =
          new ArrayList<>(workUnsequenceTsFileProcessors.values());
      timeLowerBound = System.currentTimeMillis() - config.getCloseTsFileIntervalAfterFlushing();
      for (TsFileProcessor tsFileProcessor : unSeqTsFileProcessors) {
        // working memtable is null(no more write ops) and last flush time exceeds close interval
        if (tsFileProcessor.getWorkMemTableCreatedTime() == Long.MAX_VALUE
            && tsFileProcessor.getLastWorkMemtableFlushTime() < timeLowerBound) {
          logger.info(
              "Exceed tsfile close interval, so close TsFileProcessor of time partition {} in storage group {}[{}]",
              tsFileProcessor.getTimeRangeId(),
              logicalStorageGroupName,
              virtualStorageGroupId);
          asyncCloseOneTsFileProcessor(false, tsFileProcessor);
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
                logicalStorageGroupName + "-" + this.virtualStorageGroupId,
                (System.currentTimeMillis() - startTime) / 1000);
          }
        }
      } catch (InterruptedException e) {
        logger.error(
            "CloseFileNodeCondition error occurs while waiting for closing the storage "
                + "group {}",
            logicalStorageGroupName + "-" + virtualStorageGroupId,
            e);
        Thread.currentThread().interrupt();
      }
    }
  }

  /** close all working tsfile processors */
  public void asyncCloseAllWorkingTsFileProcessors() {
    writeLock("asyncCloseAllWorkingTsFileProcessors");
    try {
      logger.info(
          "async force close all files in storage group: {}",
          logicalStorageGroupName + "-" + virtualStorageGroupId);
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
      logger.info(
          "force close all processors in storage group: {}",
          logicalStorageGroupName + "-" + virtualStorageGroupId);
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

  /**
   * build query data source by searching all tsfile which fit in query filter
   *
   * @param pathList data paths
   * @param context query context
   * @param timeFilter time filter
   * @param singleDeviceId selected deviceId (not null only when all the selected series are under
   *     the same device)
   * @return query data source
   */
  public QueryDataSource query(
      List<PartialPath> pathList,
      String singleDeviceId,
      QueryContext context,
      QueryFileManager filePathsManager,
      Filter timeFilter)
      throws QueryProcessException {
    readLock();

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
      QueryDataSource dataSource = new QueryDataSource(seqResources, unseqResources);
      // used files should be added before mergeLock is unlocked, or they may be deleted by
      // running merge
      // is null only in tests
      if (filePathsManager != null) {
        filePathsManager.addUsedFilesForQuery(context.getQueryId(), dataSource);
      }
      dataSource.setDataTTL(dataTTL);
      return dataSource;
    } catch (MetadataException e) {
      throw new QueryProcessException(e);
    } finally {
      readUnlock();
    }
  }

  /** lock the read lock of the insert lock */
  public void readLock() {
    // apply read lock for SG insert lock to prevent inconsistent with concurrently writing memtable
    insertLock.readLock().lock();
    // apply read lock for TsFileResource list
    tsFileManager.readLock();
  }

  /** unlock the read lock of insert lock */
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
        dataTTL != Long.MAX_VALUE ? System.currentTimeMillis() - dataTTL : Long.MIN_VALUE;
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

  /**
   * Delete data whose timestamp <= 'timestamp' and belongs to the time series
   * deviceId.measurementId.
   *
   * @param path the timeseries path of the to be deleted.
   * @param startTime the startTime of delete range.
   * @param endTime the endTime of delete range.
   * @param timePartitionFilter
   */
  public void delete(
      PartialPath path,
      long startTime,
      long endTime,
      long planIndex,
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

    try {
      Set<PartialPath> devicePaths = IoTDB.metaManager.getBelongedDevices(path);
      for (PartialPath device : devicePaths) {
        // delete Last cache record if necessary
        tryToDeleteLastCache(device, path, startTime, endTime);
      }

      // write log to impacted working TsFileProcessors
      logDeletion(startTime, endTime, path, timePartitionFilter);

      Deletion deletion = new Deletion(path, MERGE_MOD_START_VERSION_NUM, startTime, endTime);

      deleteDataInFiles(
          tsFileManager.getTsFileList(true),
          deletion,
          devicePaths,
          updatedModFiles,
          planIndex,
          timePartitionFilter);
      deleteDataInFiles(
          tsFileManager.getTsFileList(false),
          deletion,
          devicePaths,
          updatedModFiles,
          planIndex,
          timePartitionFilter);

    } catch (Exception e) {
      // roll back
      for (ModificationFile modFile : updatedModFiles) {
        modFile.abort();
        // remember to close mod file
        modFile.close();
      }
      throw new IOException(e);
    } finally {
      writeUnlock();
    }
  }

  private void logDeletion(
      long startTime, long endTime, PartialPath path, TimePartitionFilter timePartitionFilter)
      throws IOException {
    long timePartitionStartId = StorageEngine.getTimePartition(startTime);
    long timePartitionEndId = StorageEngine.getTimePartition(endTime);
    if (IoTDBDescriptor.getInstance().getConfig().isEnableWal()) {
      DeletePlan deletionPlan = new DeletePlan(startTime, endTime, path);
      for (Map.Entry<Long, TsFileProcessor> entry : workSequenceTsFileProcessors.entrySet()) {
        if (timePartitionStartId <= entry.getKey()
            && entry.getKey() <= timePartitionEndId
            && (timePartitionFilter == null
                || timePartitionFilter.satisfy(logicalStorageGroupName, entry.getKey()))) {
          entry.getValue().getLogNode().write(deletionPlan);
        }
      }

      for (Map.Entry<Long, TsFileProcessor> entry : workUnsequenceTsFileProcessors.entrySet()) {
        if (timePartitionStartId <= entry.getKey()
            && entry.getKey() <= timePartitionEndId
            && (timePartitionFilter == null
                || timePartitionFilter.satisfy(logicalStorageGroupName, entry.getKey()))) {
          entry.getValue().getLogNode().write(deletionPlan);
        }
      }
    }
  }

  private boolean canSkipDelete(
      TsFileResource tsFileResource,
      Set<PartialPath> devicePaths,
      long deleteStart,
      long deleteEnd,
      TimePartitionFilter timePartitionFilter) {
    if (timePartitionFilter != null
        && !timePartitionFilter.satisfy(
            logicalStorageGroupName, tsFileResource.getTimePartition())) {
      return true;
    }
    for (PartialPath device : devicePaths) {
      String deviceId = device.getFullPath();
      long endTime = tsFileResource.getEndTime(deviceId);
      if (endTime == Long.MIN_VALUE) {
        return false;
      }

      if (tsFileResource.isDeviceIdExist(deviceId)
          && (deleteEnd >= tsFileResource.getStartTime(deviceId) && deleteStart <= endTime)) {
        return false;
      }
    }
    return true;
  }

  private void deleteDataInFiles(
      Collection<TsFileResource> tsFileResourceList,
      Deletion deletion,
      Set<PartialPath> devicePaths,
      List<ModificationFile> updatedModFiles,
      long planIndex,
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

      if (tsFileResource.isMerging) {
        // we have to set modification offset to MAX_VALUE, as the offset of source chunk may
        // change after compaction
        deletion.setFileOffset(Long.MAX_VALUE);
        // write deletion into modification file
        tsFileResource.getCompactionModFile().write(deletion);
        // remember to close mod file
        tsFileResource.getCompactionModFile().close();
      } else {
        deletion.setFileOffset(tsFileResource.getTsFileSize());
        // write deletion into modification file
        tsFileResource.getModFile().write(deletion);
        // remember to close mod file
        tsFileResource.getModFile().close();
      }
      logger.info(
          "[Deletion] Deletion with path:{}, time:{}-{} written into mods file:{}.",
          deletion.getPath(),
          deletion.getStartTime(),
          deletion.getEndTime(),
          tsFileResource.getModFile().getFilePath());

      tsFileResource.updatePlanIndexes(planIndex);

      // delete data in memory of unsealed file
      if (!tsFileResource.isClosed()) {
        TsFileProcessor tsfileProcessor = tsFileResource.getProcessor();
        tsfileProcessor.deleteDataInMemory(deletion, devicePaths);
      }

      // add a record in case of rollback
      updatedModFiles.add(tsFileResource.getModFile());
    }
  }

  private void tryToDeleteLastCache(
      PartialPath deviceId, PartialPath originalPath, long startTime, long endTime)
      throws WriteProcessException {
    if (!IoTDBDescriptor.getInstance().getConfig().isLastCacheEnabled()) {
      return;
    }
    try {
      IoTDB.metaManager.deleteLastCacheByDevice(deviceId, originalPath, startTime, endTime);
    } catch (MetadataException e) {
      throw new WriteProcessException(e);
    }
  }

  /**
   * when close an TsFileProcessor, update its EndTimeMap immediately
   *
   * @param tsFileProcessor processor to be closed
   */
  private void updateEndTimeMap(TsFileProcessor tsFileProcessor) {
    TsFileResource resource = tsFileProcessor.getTsFileResource();
    for (String deviceId : resource.getDevices()) {
      resource.updateEndTime(
          deviceId, lastFlushTimeManager.getLastTime(tsFileProcessor.getTimeRangeId(), deviceId));
    }
  }

  private boolean unsequenceFlushCallback(TsFileProcessor processor) {
    return true;
  }

  private boolean updateLatestFlushTimeCallback(TsFileProcessor processor) {
    boolean res = lastFlushTimeManager.updateLatestFlushTime(processor.getTimeRangeId());
    if (!res) {
      logger.warn(
          "Partition: {} does't have latest time for each device. "
              + "No valid record is written into memtable. Flushing tsfile is: {}",
          processor.getTimeRangeId(),
          processor.getTsFileResource().getTsFile());
    }

    return res;
  }

  /**
   * update latest flush time for partition id
   *
   * @param partitionId partition id
   * @param latestFlushTime lastest flush time
   * @return true if update latest flush time success
   */
  private boolean updateLatestFlushTimeToPartition(long partitionId, long latestFlushTime) {
    boolean res =
        lastFlushTimeManager.updateLatestFlushTimeToPartition(partitionId, latestFlushTime);
    if (!res) {
      logger.warn(
          "Partition: {} does't have latest time for each device. "
              + "No valid record is written into memtable.  latest flush time is: {}",
          partitionId,
          latestFlushTime);
    }

    return res;
  }

  /** used for upgrading */
  public void updateNewlyFlushedPartitionLatestFlushedTimeForEachDevice(
      long partitionId, String deviceId, long time) {
    lastFlushTimeManager.updateNewlyFlushedPartitionLatestFlushedTimeForEachDevice(
        partitionId, deviceId, time);
  }

  /** put the memtable back to the MemTablePool and make the metadata in writer visible */
  // TODO please consider concurrency with query and insert method.
  private void closeUnsealedTsFileProcessorCallBack(TsFileProcessor tsFileProcessor)
      throws TsFileProcessorException {
    closeQueryLock.writeLock().lock();
    try {
      tsFileProcessor.close();
      tsFileResourceManager.registerSealedTsFileResource(tsFileProcessor.getTsFileResource());
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
    logger.info(
        "signal closing storage group condition in {}",
        logicalStorageGroupName + "-" + virtualStorageGroupId);
  }

  private void executeCompaction() {
    List<Long> timePartitions = new ArrayList<>(tsFileManager.getTimePartitions());
    // sort the time partition from largest to smallest
    timePartitions.sort((o1, o2) -> (int) (o2 - o1));
    for (long timePartition : timePartitions) {
      CompactionScheduler.scheduleCompaction(tsFileManager, timePartition);
    }
    CompactionTaskManager.getInstance().submitTaskFromTaskQueue();
  }

  /**
   * count all Tsfiles in the storage group which need to be upgraded
   *
   * @return total num of the tsfiles which need to be upgraded in the storage group
   */
  public int countUpgradeFiles() {
    return upgradeFileCount.get();
  }

  /** upgrade all files belongs to this storage group */
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
      lastFlushTimeManager.applyNewlyFlushedTimeToFlushedTime();
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
      // clear Cache , including chunk cache and timeseriesMetadata cache
      ChunkCache.getInstance().clear();
      TimeSeriesMetadataCache.getInstance().clear();

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

  /**
   * merge file under this storage group processor
   *
   * @param isFullMerge whether this merge is a full merge or not
   */
  public void merge(boolean isFullMerge) {
    writeLock("merge");
    try {
      executeCompaction();
    } finally {
      writeUnlock();
    }
  }

  /**
   * Load a new tsfile to storage group processor. The file may have overlap with other files.
   *
   * <p>or unsequence list.
   *
   * <p>Secondly, execute the loading process by the type.
   *
   * <p>Finally, update the latestTimeForEachDevice and partitionLatestFlushedTimeForEachDevice.
   *
   * @param newTsFileResource tsfile resource @UsedBy sync module.
   */
  public void loadNewTsFileForSync(TsFileResource newTsFileResource) throws LoadFileException {
    File tsfileToBeInserted = newTsFileResource.getTsFile();
    long newFilePartitionId = newTsFileResource.getTimePartitionWithCheck();
    writeLock("loadNewTsFileForSync");
    try {
      if (loadTsFileByType(
          LoadTsFileType.LOAD_SEQUENCE,
          tsfileToBeInserted,
          newTsFileResource,
          newFilePartitionId,
          tsFileManager.getSequenceListByTimePartition(newFilePartitionId).size() - 1)) {
        updateLatestTimeMap(newTsFileResource);
      }
      resetLastCacheWhenLoadingTsfile(newTsFileResource);
    } catch (DiskSpaceInsufficientException e) {
      logger.error(
          "Failed to append the tsfile {} to storage group processor {} because the disk space is insufficient.",
          tsfileToBeInserted.getAbsolutePath(),
          tsfileToBeInserted.getParentFile().getName());
      IoTDBDescriptor.getInstance().getConfig().setReadOnly(true);
      throw new LoadFileException(e);
    } catch (IllegalPathException e) {
      logger.error(
          "Failed to reset last cache when loading file {}", newTsFileResource.getTsFilePath());
      throw new LoadFileException(e);
    } finally {
      writeUnlock();
    }
  }

  private void resetLastCacheWhenLoadingTsfile(TsFileResource newTsFileResource)
      throws IllegalPathException {
    for (String device : newTsFileResource.getDevices()) {
      tryToDeleteLastCacheByDevice(new PartialPath(device));
    }
  }

  private void tryToDeleteLastCacheByDevice(PartialPath deviceId) {
    if (!IoTDBDescriptor.getInstance().getConfig().isLastCacheEnabled()) {
      return;
    }
    try {
      IoTDB.metaManager.deleteLastCacheByDevice(deviceId);
    } catch (MetadataException e) {
      // the path doesn't cache in cluster mode now, ignore
    }
  }

  /**
   * Load a new tsfile to storage group processor. Tne file may have overlap with other files.
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
   */
  public void loadNewTsFile(TsFileResource newTsFileResource) throws LoadFileException {
    File tsfileToBeInserted = newTsFileResource.getTsFile();
    long newFilePartitionId = newTsFileResource.getTimePartitionWithCheck();
    writeLock("loadNewTsFile");
    try {
      List<TsFileResource> sequenceList =
          tsFileManager.getSequenceListByTimePartition(newFilePartitionId);

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
          tsFileType, tsfileToBeInserted, newTsFileResource, newFilePartitionId, insertPos);
      resetLastCacheWhenLoadingTsfile(newTsFileResource);

      // update latest time map
      updateLatestTimeMap(newTsFileResource);
      long partitionNum = newTsFileResource.getTimePartition();
      updatePartitionFileVersion(partitionNum, newTsFileResource.getVersion());
      logger.info("TsFile {} is successfully loaded in {} list.", newFileName, renameInfo);
    } catch (DiskSpaceInsufficientException e) {
      logger.error(
          "Failed to append the tsfile {} to storage group processor {} because the disk space is insufficient.",
          tsfileToBeInserted.getAbsolutePath(),
          tsfileToBeInserted.getParentFile().getName());
      IoTDBDescriptor.getInstance().getConfig().setReadOnly(true);
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
    long version = partitionMaxFileVersions.getOrDefault(timePartitionId, -1L) + 1;
    partitionMaxFileVersions.put(timePartitionId, version);
    tsFileResource.setVersion(version);
    return version;
  }

  /**
   * Update latest time in latestTimeForEachDevice and
   * partitionLatestFlushedTimeForEachDevice. @UsedBy sync module, load external tsfile module.
   */
  private void updateLatestTimeMap(TsFileResource newTsFileResource) {
    for (String device : newTsFileResource.getDevices()) {
      long endTime = newTsFileResource.getEndTime(device);
      long timePartitionId = StorageEngine.getTimePartition(endTime);
      lastFlushTimeManager.updateLastTime(timePartitionId, device, endTime);
      lastFlushTimeManager.updateFlushedTime(timePartitionId, device, endTime);
      lastFlushTimeManager.updateGlobalFlushedTime(device, endTime);
    }
  }

  /**
   * Execute the loading process by the type.
   *
   * @param type load type
   * @param tsFileResource tsfile resource to be loaded
   * @param filePartitionId the partition id of the new file
   * @return load the file successfully @UsedBy sync module, load external tsfile module.
   */
  private boolean loadTsFileByType(
      LoadTsFileType type,
      File tsFileToLoad,
      TsFileResource tsFileResource,
      long filePartitionId,
      int insertPos)
      throws LoadFileException, DiskSpaceInsufficientException {
    File targetFile;
    switch (type) {
      case LOAD_UNSEQUENCE:
        targetFile =
            fsFactory.getFile(
                DirectoryManager.getInstance().getNextFolderForUnSequenceFile(),
                logicalStorageGroupName
                    + File.separatorChar
                    + virtualStorageGroupId
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
                logicalStorageGroupName
                    + File.separatorChar
                    + virtualStorageGroupId
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
          tsFileManager.insertToPartitionFileList(tsFileResource, true, 0);
        } else {
          tsFileManager.insertToPartitionFileList(tsFileResource, true, insertPos + 1);
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
      FileUtils.moveFile(tsFileToLoad, targetFile);
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
      FileUtils.moveFile(resourceFileToLoad, targetResourceFile);
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
        FileUtils.moveFile(modFileToLoad, targetModFile);
      } catch (IOException e) {
        logger.error(
            "File renaming failed when loading .mod file. Origin: {}, Target: {}",
            resourceFileToLoad.getAbsolutePath(),
            targetModFile.getAbsolutePath(),
            e);
        throw new LoadFileException(
            String.format(
                "File renaming failed when loading .mod file. Origin: %s, Target: %s, because %s",
                resourceFileToLoad.getAbsolutePath(),
                targetModFile.getAbsolutePath(),
                e.getMessage()));
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

  public void setDataTTL(long dataTTL) {
    this.dataTTL = dataTTL;
  }

  public List<TsFileResource> getSequenceFileTreeSet() {
    return tsFileManager.getTsFileList(true);
  }

  public List<TsFileResource> getUnSequenceFileList() {
    return tsFileManager.getTsFileList(false);
  }

  public String getVirtualStorageGroupId() {
    return virtualStorageGroupId;
  }

  /** @return virtual storage group path, like root.sg1/0 */
  public String getStorageGroupPath() {
    return logicalStorageGroupName + File.separator + virtualStorageGroupId;
  }

  public StorageGroupInfo getStorageGroupInfo() {
    return storageGroupInfo;
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
        || isFileAlreadyExistInClosed(tsFileResource, partitionNum, getSequenceFileTreeSet())
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
      // abort ongoing comapctions and merges
      CompactionTaskManager.getInstance().abortCompaction(logicalStorageGroupName);
      MergeManager.getINSTANCE().abortMerge(logicalStorageGroupName);
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

  // may remove the processorEntrys
  private void removePartitions(
      TimePartitionFilter filter,
      Set<Entry<Long, TsFileProcessor>> processorEntrys,
      boolean sequence) {
    for (Iterator<Entry<Long, TsFileProcessor>> iterator = processorEntrys.iterator();
        iterator.hasNext(); ) {
      Entry<Long, TsFileProcessor> longTsFileProcessorEntry = iterator.next();
      long partitionId = longTsFileProcessorEntry.getKey();
      TsFileProcessor processor = longTsFileProcessorEntry.getValue();
      if (filter.satisfy(logicalStorageGroupName, partitionId)) {
        processor.syncClose();
        iterator.remove();
        processor.getTsFileResource().remove();
        tsFileManager.remove(processor.getTsFileResource(), sequence);
        updateLatestFlushTimeToPartition(partitionId, Long.MIN_VALUE);
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
      if (filter.satisfy(logicalStorageGroupName, tsFileResource.getTimePartition())) {
        tsFileResource.remove();
        tsFileManager.remove(tsFileResource, sequence);
        updateLatestFlushTimeToPartition(tsFileResource.getTimePartition(), Long.MIN_VALUE);
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
   * @param insertRowsOfOneDevicePlan batch of rows belongs to one device
   */
  public void insert(InsertRowsOfOneDevicePlan insertRowsOfOneDevicePlan)
      throws WriteProcessException, TriggerExecutionException {
    writeLock("InsertRowsOfOneDevice");
    try {
      boolean isSequence = false;
      InsertRowPlan[] rowPlans = insertRowsOfOneDevicePlan.getRowPlans();
      for (int i = 0, rowPlansLength = rowPlans.length; i < rowPlansLength; i++) {

        InsertRowPlan plan = rowPlans[i];
        if (!isAlive(plan.getTime()) || insertRowsOfOneDevicePlan.isExecuted(i)) {
          // we do not need to write these part of data, as they can not be queried
          // or the sub-plan has already been executed, we are retrying other sub-plans
          continue;
        }
        // init map
        long timePartitionId = StorageEngine.getTimePartition(plan.getTime());

        lastFlushTimeManager.ensureFlushedTimePartition(timePartitionId);
        // as the plans have been ordered, and we have get the write lock,
        // So, if a plan is sequenced, then all the rest plans are sequenced.
        //
        if (!isSequence) {
          isSequence =
              plan.getTime()
                  > lastFlushTimeManager.getFlushedTime(
                      timePartitionId, plan.getDevicePath().getFullPath());
        }
        // is unsequence and user set config to discard out of order data
        if (!isSequence
            && IoTDBDescriptor.getInstance().getConfig().isEnableDiscardOutOfOrderData()) {
          return;
        }

        lastFlushTimeManager.ensureLastTimePartition(timePartitionId);

        // fire trigger before insertion
        TriggerEngine.fire(TriggerEvent.BEFORE_INSERT, plan);
        // insert to sequence or unSequence file
        insertToTsFileProcessor(plan, isSequence, timePartitionId);
        // fire trigger before insertion
        TriggerEngine.fire(TriggerEvent.AFTER_INSERT, plan);
      }
    } finally {
      writeUnlock();
    }
  }

  @TestOnly
  public long getPartitionMaxFileVersions(long partitionId) {
    return partitionMaxFileVersions.getOrDefault(partitionId, -1L);
  }

  public void addSettleFilesToList(
      List<TsFileResource> seqResourcesToBeSettled,
      List<TsFileResource> unseqResourcesToBeSettled,
      List<String> tsFilePaths) {
    if (tsFilePaths.size() == 0) {
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

    boolean call(TsFileProcessor caller);
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

  public String getInsertWriteLockHolder() {
    return insertWriteLockHolder;
  }

  public ScheduledExecutorService getTimedCompactionScheduleTask() {
    return timedCompactionScheduleTask;
  }

  public IDTable getIdTable() {
    return idTable;
  }
}
