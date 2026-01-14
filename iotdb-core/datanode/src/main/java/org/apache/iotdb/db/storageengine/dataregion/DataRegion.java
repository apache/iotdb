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
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.path.IFullPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.schema.table.InformationSchema;
import org.apache.iotdb.commons.schema.table.TsFileTableSchemaUtil;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.PerformanceOverviewMetrics;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.commons.utils.RetryUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.confignode.rpc.thrift.TDescTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TTableInfo;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.iot.IoTConsensus;
import org.apache.iotdb.consensus.iot.IoTConsensusServerImpl;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.exception.BatchProcessException;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.exception.DataTypeInconsistentException;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.WriteProcessRejectException;
import org.apache.iotdb.db.exception.load.LoadFileException;
import org.apache.iotdb.db.exception.query.OutOfTTLException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.quota.ExceedQuotaException;
import org.apache.iotdb.db.exception.runtime.TableLostRuntimeException;
import org.apache.iotdb.db.pipe.consensus.deletion.DeletionResource;
import org.apache.iotdb.db.pipe.consensus.deletion.DeletionResource.Status;
import org.apache.iotdb.db.pipe.consensus.deletion.DeletionResourceManager;
import org.apache.iotdb.db.pipe.consensus.deletion.persist.PageCacheDeletionBuffer;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryWeightUtil;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.listener.PipeInsertionDataNodeListener;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.queryengine.common.DeviceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.queryengine.metric.QueryResourceMetricSet;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeTTLCache;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.ContinuousSameSearchIndexSeparatorNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsOfOneDeviceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.ObjectNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.LastCacheLoadStrategy;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TableDeviceSchemaCache;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TreeDeviceSchemaCacheManager;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
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
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.RepairUnsortedFileCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduleContext;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduleTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduler;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.flush.CloseFileListener;
import org.apache.iotdb.db.storageengine.dataregion.flush.FlushListener;
import org.apache.iotdb.db.storageengine.dataregion.flush.FlushStatus;
import org.apache.iotdb.db.storageengine.dataregion.flush.TsFileFlushPolicy;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IMemTable;
import org.apache.iotdb.db.storageengine.dataregion.memtable.TsFileProcessor;
import org.apache.iotdb.db.storageengine.dataregion.memtable.TsFileProcessorInfo;
import org.apache.iotdb.db.storageengine.dataregion.modification.IDPredicate;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.TreeDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.v1.ModificationFileV1;
import org.apache.iotdb.db.storageengine.dataregion.read.IQueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSourceForRegionScan;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.IFileScanHandle;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.impl.ClosedFileScanHandleImpl;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileID;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ArrayDeviceTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.FileTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.FileTimeIndexCacheRecorder;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.utils.fileTimeIndexCache.FileTimeIndexCacheReader;
import org.apache.iotdb.db.storageengine.dataregion.utils.validate.TsFileValidator;
import org.apache.iotdb.db.storageengine.dataregion.wal.WALManager;
import org.apache.iotdb.db.storageengine.dataregion.wal.node.IWALNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.node.WALNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.recover.WALRecoverManager;
import org.apache.iotdb.db.storageengine.dataregion.wal.recover.file.SealedTsFileRecoverPerformer;
import org.apache.iotdb.db.storageengine.dataregion.wal.recover.file.UnsealedTsFileRecoverPerformer;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALMode;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.listener.WALFlushListener;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.listener.WALRecoverListener;
import org.apache.iotdb.db.storageengine.load.disk.ILoadDiskSelector;
import org.apache.iotdb.db.storageengine.load.limiter.LoadTsFileRateLimiter;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;
import org.apache.iotdb.db.storageengine.rescon.memory.TimePartitionInfo;
import org.apache.iotdb.db.storageengine.rescon.memory.TimePartitionManager;
import org.apache.iotdb.db.storageengine.rescon.memory.TsFileResourceManager;
import org.apache.iotdb.db.storageengine.rescon.quotas.DataNodeSpaceQuotaManager;
import org.apache.iotdb.db.tools.settle.TsFileAndModSettleTool;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.db.utils.EncryptDBUtils;
import org.apache.iotdb.db.utils.ModificationUtils;
import org.apache.iotdb.db.utils.ObjectTypeUtils;
import org.apache.iotdb.db.utils.ObjectWriter;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.io.BaseEncoding;
import org.apache.thrift.TException;
import org.apache.tsfile.external.commons.io.FileUtils;
import org.apache.tsfile.external.commons.lang3.tuple.Triple;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.IDeviceID.Factory;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.fileSystem.FSType;
import org.apache.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.reader.TsFileLastReader;
import org.apache.tsfile.utils.FSUtils;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.iotdb.commons.conf.IoTDBConstant.FILE_NAME_SEPARATOR;
import static org.apache.iotdb.commons.utils.PathUtils.isTableModelDatabase;
import static org.apache.iotdb.db.queryengine.metric.QueryResourceMetricSet.SEQUENCE_TSFILE;
import static org.apache.iotdb.db.queryengine.metric.QueryResourceMetricSet.UNSEQUENCE_TSFILE;
import static org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource.BROKEN_SUFFIX;
import static org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource.RESOURCE_SUFFIX;
import static org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource.TEMP_SUFFIX;
import static org.apache.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

/**
 * For sequence data, a {@link DataRegion} has some {@link TsFileProcessor}s, in which there is only
 * one {@link TsFileProcessor} in the working status. <br>
 *
 * <p>There are two situations to set the working {@link TsFileProcessor} to closing status:<br>
 *
 * <p>(1) when inserting data into the {@link TsFileProcessor}, and the {@link TsFileProcessor}
 * shouldFlush() (or shouldClose())<br>
 *
 * <p>(2) someone calls syncCloseAllWorkingTsFileProcessors(). (up to now, only flush command from
 * cli will call this method)<br>
 *
 * <p>UnSequence data has the similar process as above.
 *
 * <p>When a sequence {@link TsFileProcessor} is submitted to be flushed, the
 * updateLatestFlushTimeCallback() method will be called as a callback.<br>
 *
 * <p>When a {@link TsFileProcessor} is closed, the closeUnsealedTsFileProcessorCallBack() method
 * will be called as a callback.
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

  /**
   * A read write lock for guaranteeing concurrent safety when accessing all fields in this class
   * (i.e., schema, (un)sequenceFileList, work(un)SequenceTsFileProcessor,
   * closing(Un)SequenceTsFileProcessor, latestTimeForEachDevice, and
   * partitionLatestFlushedTimeForEachDevice)
   */
  private final ReadWriteLock insertLock = new ReentrantReadWriteLock();

  /** Condition to safely delete data region. */
  private final Condition deletedCondition = insertLock.writeLock().newCondition();

  /** Data region has been deleted or not. */
  private volatile boolean deleted = false;

  /** closeStorageGroupCondition is used to wait for all currently closing TsFiles to be done. */
  private final Object closeStorageGroupCondition = new Object();

  /** time partition id in the database -> {@link TsFileProcessor} for this time partition. */
  private final TreeMap<Long, TsFileProcessor> workSequenceTsFileProcessors = new TreeMap<>();

  /** time partition id in the database -> {@link TsFileProcessor} for this time partition. */
  private final TreeMap<Long, TsFileProcessor> workUnsequenceTsFileProcessors = new TreeMap<>();

  /** sequence {@link TsFileProcessor}s which are closing. */
  private final Set<TsFileProcessor> closingSequenceTsFileProcessor = ConcurrentHashMap.newKeySet();

  /** unsequence {@link TsFileProcessor}s which are closing. */
  private final Set<TsFileProcessor> closingUnSequenceTsFileProcessor =
      ConcurrentHashMap.newKeySet();

  /** data region id. */
  private final String dataRegionIdString;

  private final DataRegionId dataRegionId;

  /** database name. */
  private final String databaseName;

  /** data region system directory. */
  private File dataRegionSysDir;

  /** manage seqFileList and unSeqFileList. */
  private final TsFileManager tsFileManager;

  /** manage tsFileResource degrade. */
  private final TsFileResourceManager tsFileResourceManager = TsFileResourceManager.getInstance();

  /** file system factory (local or hdfs). */
  private final FSFactory fsFactory = FSFactoryProducer.getFSFactory();

  /** File flush policy. */
  private TsFileFlushPolicy fileFlushPolicy;

  /**
   * The max file versions in each partition. By recording this, if several IoTDB instances have the
   * same policy of closing file and their ingestion is identical, then files of the same version in
   * different IoTDB instance will have identical data, providing convenience for data comparison
   * across different instances. partition number -> max version number
   */
  private Map<Long, Long> partitionMaxFileVersions = new ConcurrentHashMap<>();

  private static final Cache<TableSchemaCacheKey, Triple<Long, Long, TableSchema>>
      TABLE_SCHEMA_CACHE =
          Caffeine.newBuilder()
              .maximumWeight(
                  IoTDBDescriptor.getInstance().getConfig().getDataNodeTableSchemaCacheSize())
              .weigher(
                  (TableSchemaCacheKey k, Triple<Long, Long, TableSchema> v) ->
                      (int)
                          (PipeMemoryWeightUtil.calculateTableSchemaBytesUsed(v.getRight())
                              + 2 * Long.BYTES))
              .build();

  /** database info for mem control. */
  private final DataRegionInfo dataRegionInfo = new DataRegionInfo(this);

  /** whether it's ready from recovery. */
  private boolean isReady = false;

  private List<Callable<Void>> asyncTsFileResourceRecoverTaskList;

  /** close file listeners. */
  private List<CloseFileListener> customCloseFileListeners = Collections.emptyList();

  /** flush listeners. */
  private List<FlushListener> customFlushListeners = Collections.emptyList();

  private ILastFlushTimeMap lastFlushTimeMap;

  /**
   * Record the insertWriteLock in SG is being hold by which method, it will be empty string if no
   * one holds the insertWriteLock.
   */
  private String insertWriteLockHolder = "";

  private volatile long directBufferMemoryCost = 0;

  private final AtomicBoolean isCompactionSelecting = new AtomicBoolean(false);

  private static final QueryResourceMetricSet QUERY_RESOURCE_METRIC_SET =
      QueryResourceMetricSet.getInstance();

  private static final PerformanceOverviewMetrics PERFORMANCE_OVERVIEW_METRICS =
      PerformanceOverviewMetrics.getInstance();
  private final ExecutorService upgradeModFileThreadPool;

  private final DataRegionMetrics metrics;

  private ILoadDiskSelector ordinaryLoadDiskSelector;
  private ILoadDiskSelector pipeAndIoTV2LoadDiskSelector;

  /**
   * Construct a database processor.
   *
   * @param systemDir system dir path
   * @param dataRegionIdString data region id e.g. 1
   * @param fileFlushPolicy file flush policy
   * @param databaseName database name e.g. root.sg1
   */
  public DataRegion(
      String systemDir,
      String dataRegionIdString,
      TsFileFlushPolicy fileFlushPolicy,
      String databaseName)
      throws DataRegionException {
    this.dataRegionIdString = dataRegionIdString;
    this.dataRegionId = new DataRegionId(Integer.parseInt(dataRegionIdString));
    this.databaseName = databaseName;
    this.fileFlushPolicy = fileFlushPolicy;
    acquireDirectBufferMemory();

    dataRegionSysDir = SystemFileFactory.INSTANCE.getFile(systemDir, dataRegionIdString);
    this.tsFileManager =
        new TsFileManager(databaseName, dataRegionIdString, dataRegionSysDir.getPath());
    if (dataRegionSysDir.mkdirs()) {
      logger.info(
          "Database system Directory {} doesn't exist, create it", dataRegionSysDir.getPath());
    } else if (!dataRegionSysDir.exists()) {
      logger.error("create database system Directory {} failed", dataRegionSysDir.getPath());
    }

    lastFlushTimeMap = new HashLastFlushTimeMap();
    upgradeModFileThreadPool =
        IoTDBThreadPoolFactory.newSingleThreadExecutor(
            databaseName + "-" + dataRegionIdString + "-UpgradeMod");

    // recover tsfiles unless consensus protocol is ratis and storage engine is not ready
    if (config.getDataRegionConsensusProtocolClass().equals(ConsensusFactory.RATIS_CONSENSUS)
        && !StorageEngine.getInstance().isReadyForReadAndWrite()) {
      logger.debug(
          "Skip recovering data region {}[{}] when consensus protocol is ratis and storage engine is not ready.",
          databaseName,
          dataRegionIdString);
      for (String fileFolder : TierManager.getInstance().getAllFilesFolders()) {
        File dataRegionFolder =
            fsFactory.getFile(fileFolder, databaseName + File.separator + dataRegionIdString);
        try {
          fsFactory.deleteDirectory(dataRegionFolder.getPath());
        } catch (IOException e) {
          logger.error(
              "Exception occurs when deleting data region folder for {}-{}",
              databaseName,
              dataRegionIdString,
              e);
        }
        if (FSUtils.getFSType(dataRegionFolder) == FSType.LOCAL) {
          if (dataRegionFolder.mkdirs()) {
            logger.info(
                "Data region directory {} doesn't exist, create it", dataRegionFolder.getPath());
          } else if (!dataRegionFolder.exists()) {
            logger.error("create data region directory {} failed", dataRegionFolder.getPath());
          }
        }
      }
    } else {
      asyncTsFileResourceRecoverTaskList = new ArrayList<>();
      recover();
    }

    initDiskSelector();

    this.metrics = new DataRegionMetrics(this);
    MetricService.getInstance().addMetricSet(metrics);
  }

  @TestOnly
  public DataRegion(String databaseName, String dataRegionIdString) {
    this.databaseName = databaseName;
    this.dataRegionIdString = dataRegionIdString;
    this.dataRegionId = new DataRegionId(Integer.parseInt(this.dataRegionIdString));
    this.tsFileManager = new TsFileManager(databaseName, dataRegionIdString, "");
    this.partitionMaxFileVersions = new HashMap<>();
    partitionMaxFileVersions.put(0L, 0L);
    upgradeModFileThreadPool = null;
    this.metrics = new DataRegionMetrics(this);

    initDiskSelector();
  }

  private void initDiskSelector() {
    final ILoadDiskSelector.DiskDirectorySelector selector =
        (sourceDirectory, fileName, tierLevel) -> {
          try {
            return TierManager.getInstance()
                .getFolderManager(tierLevel, false)
                .getNextWithRetry(folder -> fsFactory.getFile(folder, fileName));
          } catch (DiskSpaceInsufficientException e) {
            throw e;
          } catch (Exception e) {
            throw new LoadFileException(
                String.format("Storage allocation failed for %s (tier %d)", fileName, tierLevel),
                e);
          }
        };

    final String[] dirs =
        Arrays.stream(config.getTierDataDirs()[0])
            .map(v -> fsFactory.getFile(v, IoTDBConstant.UNSEQUENCE_FOLDER_NAME).getPath())
            .toArray(String[]::new);
    ordinaryLoadDiskSelector =
        ILoadDiskSelector.initDiskSelector(config.getLoadDiskSelectStrategy(), dirs, selector);
    pipeAndIoTV2LoadDiskSelector =
        ILoadDiskSelector.initDiskSelector(
            config.getLoadDiskSelectStrategyForIoTV2AndPipe(), dirs, selector);
  }

  @Override
  public String getDatabaseName() {
    return databaseName;
  }

  public boolean isReady() {
    return isReady;
  }

  public List<Callable<Void>> getAsyncTsFileResourceRecoverTaskList() {
    return asyncTsFileResourceRecoverTaskList;
  }

  public void clearAsyncTsFileResourceRecoverTaskList() {
    asyncTsFileResourceRecoverTaskList.clear();
  }

  /** this class is used to store recovering context. */
  private class DataRegionRecoveryContext {

    /** number of files to be recovered. */
    private final long numOfFilesToRecover;

    /** number of already recovered files. */
    private long recoveredFilesNum;

    /** last recovery log time. */
    private long lastLogTime;

    /** recover performers of unsealed TsFiles. */
    private final List<UnsealedTsFileRecoverPerformer> recoverPerformers = new ArrayList<>();

    public DataRegionRecoveryContext(long numOfFilesToRecover) {
      this.numOfFilesToRecover = numOfFilesToRecover;
      this.recoveredFilesNum = 0;
      this.lastLogTime = System.currentTimeMillis();
    }

    public void incrementRecoveredFilesNum() {
      recoveredFilesNum++;
      if (recoveredFilesNum < numOfFilesToRecover) {
        if (System.currentTimeMillis() - lastLogTime > config.getRecoveryLogIntervalInMs()) {
          logger.info(
              "The TsFiles of data region {}[{}] has recovered {}/{}.",
              databaseName,
              dataRegionIdString,
              recoveredFilesNum,
              numOfFilesToRecover);
          lastLogTime = System.currentTimeMillis();
        }
      } else {
        logger.info(
            "The TsFiles of data region {}[{}] has recovered completely {}/{}.",
            databaseName,
            dataRegionIdString,
            numOfFilesToRecover,
            numOfFilesToRecover);
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
      // split by partition so that we can find the last file of each partition and decide to
      // close it or not
      Map<Long, List<TsFileResource>> partitionTmpSeqTsFiles =
          getAllFiles(TierManager.getInstance().getAllLocalSequenceFileFolders());
      Map<Long, List<TsFileResource>> partitionTmpUnseqTsFiles =
          getAllFiles(TierManager.getInstance().getAllLocalUnSequenceFileFolders());
      DataRegionRecoveryContext dataRegionRecoveryContext =
          new DataRegionRecoveryContext(
              partitionTmpSeqTsFiles.values().stream().mapToLong(List::size).sum()
                  + partitionTmpUnseqTsFiles.values().stream().mapToLong(List::size).sum());
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
            if (ModificationFile.getExclusiveMods(resource.getTsFile()).exists()) {
              // update mods file metrics
              resource.getExclusiveModFile();
            } else {
              resource.upgradeModFile(upgradeModFileThreadPool);
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
      for (List<TsFileResource> unseqTsFiles : partitionTmpUnseqTsFiles.values()) {
        List<TsFileResource> unsealedTsFiles = new ArrayList<>();
        // tsFiles without resource file are unsealed
        for (TsFileResource resource : unseqTsFiles) {
          if (resource.resourceFileExists()) {
            FileMetrics.getInstance()
                .addTsFile(
                    resource.getDatabaseName(),
                    resource.getDataRegionId(),
                    resource.getTsFile().length(),
                    false,
                    resource.getTsFile().getName());
          } else {
            WALRecoverListener recoverListener =
                recoverUnsealedTsFile(resource, dataRegionRecoveryContext, false);
            if (recoverListener != null) {
              recoverListeners.add(recoverListener);
            }
            unsealedTsFiles.add(resource);
          }
          if (ModificationFile.getExclusiveMods(resource.getTsFile()).exists()) {
            // update mods file metrics
            resource.getExclusiveModFile();
          } else {
            resource.upgradeModFile(upgradeModFileThreadPool);
          }
        }
        unseqTsFiles.removeAll(unsealedTsFiles);
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
        File logFile = SystemFileFactory.INSTANCE.getFile(dataRegionSysDir, "FileTimeIndexCache_0");
        Map<TsFileID, FileTimeIndex> fileTimeIndexMap = new HashMap<>();
        if (logFile.exists()) {
          try {
            FileTimeIndexCacheReader logReader =
                new FileTimeIndexCacheReader(logFile, dataRegionIdString);
            logReader.read(fileTimeIndexMap);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
        for (Entry<Long, List<TsFileResource>> partitionFiles : partitionTmpSeqTsFiles.entrySet()) {
          Callable<Void> asyncRecoverTask =
              recoverFilesInPartition(
                  partitionFiles.getKey(),
                  dataRegionRecoveryContext,
                  partitionFiles.getValue(),
                  fileTimeIndexMap,
                  true);
          if (asyncRecoverTask != null) {
            asyncTsFileResourceRecoverTaskList.add(asyncRecoverTask);
          }
        }
        for (Entry<Long, List<TsFileResource>> partitionFiles :
            partitionTmpUnseqTsFiles.entrySet()) {
          Callable<Void> asyncRecoverTask =
              recoverFilesInPartition(
                  partitionFiles.getKey(),
                  dataRegionRecoveryContext,
                  partitionFiles.getValue(),
                  fileTimeIndexMap,
                  false);
          if (asyncRecoverTask != null) {
            asyncTsFileResourceRecoverTaskList.add(asyncRecoverTask);
          }
        }
        if (config.isEnableSeparateData()) {
          TimePartitionManager.getInstance()
              .registerTimePartitionInfo(
                  new TimePartitionInfo(
                      new DataRegionId(Integer.parseInt(dataRegionIdString)),
                      latestPartitionId,
                      false,
                      Long.MAX_VALUE,
                      lastFlushTimeMap.getMemSize(latestPartitionId)));
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

    if (asyncTsFileResourceRecoverTaskList.isEmpty()) {
      initCompactionSchedule();
    }

    if (StorageEngine.getInstance().isReadyForReadAndWrite()) {
      if (config.getDataRegionConsensusProtocolClass().equals(ConsensusFactory.IOT_CONSENSUS_V2)) {
        IWALNode walNode =
            WALManager.getInstance()
                .applyForWALNode(databaseName + FILE_NAME_SEPARATOR + dataRegionIdString);
        if (walNode instanceof WALNode) {
          walNode.setSafelyDeletedSearchIndex(Long.MAX_VALUE);
        }
      }
      logger.info(
          "The data region {}[{}] is created successfully", databaseName, dataRegionIdString);
    } else {
      logger.info(
          "The data region {}[{}] is recovered successfully", databaseName, dataRegionIdString);
    }
  }

  private void updatePartitionLastFlushTime(TsFileResource resource) {
    if (config.isEnableSeparateData()) {
      lastFlushTimeMap.updatePartitionFlushedTime(
          resource.getTimePartition(), resource.getTimeIndex().getMaxEndTime());
    }
  }

  protected void updateDeviceLastFlushTime(TsFileResource resource) {
    long timePartitionId = resource.getTimePartition();
    Map<IDeviceID, Long> endTimeMap = new HashMap<>();
    for (IDeviceID deviceId : resource.getDevices()) {
      @SuppressWarnings("OptionalGetWithoutIsPresent") // checked above
      long endTime = resource.getEndTime(deviceId).get();
      endTimeMap.put(deviceId, endTime);
    }
    if (config.isEnableSeparateData()) {
      lastFlushTimeMap.updateMultiDeviceFlushedTime(timePartitionId, endTimeMap);
    }
    if (CommonDescriptor.getInstance().getConfig().isLastCacheEnable()) {
      lastFlushTimeMap.updateMultiDeviceGlobalFlushedTime(endTimeMap);
    }
  }

  protected void upgradeAndUpdateDeviceLastFlushTime(
      long timePartitionId, List<TsFileResource> resources) {
    Map<IDeviceID, Long> endTimeMap = new HashMap<>();
    for (TsFileResource resource : resources) {
      for (IDeviceID deviceId : resource.getDevices()) {
        // checked above
        //noinspection OptionalGetWithoutIsPresent
        long endTime = resource.getEndTime(deviceId).get();
        endTimeMap.put(deviceId, endTime);
      }
    }
    if (config.isEnableSeparateData()) {
      lastFlushTimeMap.upgradeAndUpdateMultiDeviceFlushedTime(timePartitionId, endTimeMap);
    }
    if (CommonDescriptor.getInstance().getConfig().isLastCacheEnable()) {
      lastFlushTimeMap.updateMultiDeviceGlobalFlushedTime(endTimeMap);
    }
  }

  public void initCompactionSchedule() {
    RepairUnsortedFileCompactionTask.recoverAllocatedFileTimestamp(
        tsFileManager.getMaxFileTimestampOfUnSequenceFile());
    CompactionScheduleTaskManager.getInstance().registerDataRegion(this);
  }

  private void recoverCompaction() {
    CompactionRecoverManager compactionRecoverManager =
        new CompactionRecoverManager(tsFileManager, databaseName, dataRegionIdString);
    compactionRecoverManager.recoverCompaction();
  }

  public void updatePartitionFileVersion(long partitionNum, long fileVersion) {
    partitionMaxFileVersions.compute(
        partitionNum,
        (key, oldVersion) ->
            (oldVersion == null || fileVersion > oldVersion) ? fileVersion : oldVersion);
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private Map<Long, List<TsFileResource>> getAllFiles(List<String> folders)
      throws IOException, DataRegionException {
    // "{partition id}/{tsfile name}" -> tsfile file, remove duplicate files in one time partition
    Map<String, File> tsFilePartitionPath2File = new HashMap<>();
    for (String baseDir : folders) {
      File fileFolder =
          fsFactory.getFile(baseDir + File.separator + databaseName, dataRegionIdString);
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
    Map<Long, List<TsFileResource>> ret = new TreeMap<>();
    for (File f : sortedFiles) {
      checkTsFileTime(f, currentTime);
      TsFileResource resource = new TsFileResource(f);
      ret.computeIfAbsent(resource.getTsFileID().timePartitionId, l -> new ArrayList<>())
          .add(resource);
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
    // skip files generated by repair compaction task
    long version = Long.parseLong(items[1]);
    if (version > 0
        && fileTime > currentTime
        && fileTime < RepairUnsortedFileCompactionTask.getInitialAllocatedFileTimestamp()) {
      throw new DataRegionException(
          String.format(
              "data region %s[%s] is down, because the time of tsfile %s is larger than system current time, "
                  + "file time is %d while system current time is %d, please check it.",
              databaseName, dataRegionIdString, tsFile.getAbsolutePath(), fileTime, currentTime));
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
        updateDeviceLastFlushTime(tsFileResource);
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
                new DataRegionId(Integer.parseInt(dataRegionIdString)), timePartitionId);
        TsFileProcessor tsFileProcessor =
            new TsFileProcessor(
                dataRegionIdString,
                dataRegionInfo,
                tsFileResource,
                this::closeUnsealedTsFileProcessorCallBack,
                this::flushCallback,
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
        TsFileProcessorInfo tsFileProcessorInfo = new TsFileProcessorInfo(dataRegionInfo);
        tsFileProcessor.setTsFileProcessorInfo(tsFileProcessorInfo);
        this.dataRegionInfo.initTsFileProcessorInfo(tsFileProcessor);
        // get chunkMetadata size
        long chunkMetadataSize = 0;
        for (Map<String, List<ChunkMetadata>> metaMap : writer.getMetadatasForQuery().values()) {
          for (List<ChunkMetadata> metadataList : metaMap.values()) {
            for (ChunkMetadata chunkMetadata : metadataList) {
              chunkMetadataSize += chunkMetadata.getRetainedSizeInBytes();
            }
          }
        }
        tsFileProcessorInfo.addTSPMemCost(chunkMetadataSize);
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
      TsFileResource sealedTsFile, DataRegionRecoveryContext context) {
    try (SealedTsFileRecoverPerformer recoverPerformer =
        new SealedTsFileRecoverPerformer(sealedTsFile)) {
      recoverPerformer.recover();
      sealedTsFile.close();
      if (!TsFileValidator.getInstance().validateTsFile(sealedTsFile)) {
        sealedTsFile.remove();
        tsFileManager.remove(sealedTsFile, sealedTsFile.isSeq());
      } else {
        tsFileResourceManager.registerSealedTsFileResource(sealedTsFile);
      }
    } catch (Throwable e) {
      logger.error("Fail to recover sealed TsFile {}, skip it.", sealedTsFile.getTsFilePath(), e);
    } finally {
      // update recovery context
      context.incrementRecoveredFilesNum();
    }
  }

  private Callable<Void> recoverFilesInPartition(
      long partitionId,
      DataRegionRecoveryContext context,
      List<TsFileResource> resourceList,
      Map<TsFileID, FileTimeIndex> fileTimeIndexMap,
      boolean isSeq) {
    List<TsFileResource> resourceListForAsyncRecover = new ArrayList<>();
    List<TsFileResource> resourceListForSyncRecover = new ArrayList<>();
    Callable<Void> asyncRecoverTask = null;
    for (TsFileResource tsFileResource : resourceList) {
      tsFileManager.add(tsFileResource, isSeq);
      if (fileTimeIndexMap.containsKey(tsFileResource.getTsFileID())
          && tsFileResource.resourceFileExists()) {
        tsFileResource.setTimeIndex(fileTimeIndexMap.get(tsFileResource.getTsFileID()));
        tsFileResource.setStatus(TsFileResourceStatus.NORMAL);
        resourceListForAsyncRecover.add(tsFileResource);
      } else {
        resourceListForSyncRecover.add(tsFileResource);
      }
    }
    if (!resourceListForAsyncRecover.isEmpty()) {
      asyncRecoverTask =
          asyncRecoverFilesInPartition(partitionId, context, resourceListForAsyncRecover);
    }
    if (!resourceListForSyncRecover.isEmpty()) {
      syncRecoverFilesInPartition(partitionId, context, resourceListForSyncRecover);
    }
    return asyncRecoverTask;
  }

  private Callable<Void> asyncRecoverFilesInPartition(
      long partitionId, DataRegionRecoveryContext context, List<TsFileResource> resourceList) {
    if (config.isEnableSeparateData()) {
      if (!lastFlushTimeMap.checkAndCreateFlushedTimePartition(partitionId, false)) {
        TimePartitionManager.getInstance()
            .registerTimePartitionInfo(
                new TimePartitionInfo(
                    new DataRegionId(Integer.parseInt(dataRegionIdString)),
                    partitionId,
                    false,
                    Long.MAX_VALUE,
                    lastFlushTimeMap.getMemSize(partitionId)));
      }
      for (TsFileResource tsFileResource : resourceList) {
        updatePartitionLastFlushTime(tsFileResource);
      }
      TimePartitionManager.getInstance()
          .updateAfterFlushing(
              new DataRegionId(Integer.parseInt(dataRegionIdString)),
              partitionId,
              System.currentTimeMillis(),
              lastFlushTimeMap.getMemSize(partitionId),
              false);
    }
    for (TsFileResource tsFileResource : resourceList) {
      if (tsFileResource.isUseSharedModFile()) {
        // set a future so that other may know when the mod file is recovered
        tsFileResource.setSharedModFilePathFuture(new CompletableFuture<>());
      }
    }
    return () -> {
      for (TsFileResource tsFileResource : resourceList) {
        try (SealedTsFileRecoverPerformer recoverPerformer =
            new SealedTsFileRecoverPerformer(tsFileResource)) {
          recoverPerformer.recover();
          tsFileResourceManager.registerSealedTsFileResource(tsFileResource);
        } catch (Throwable e) {
          logger.error(
              "Fail to recover sealed TsFile {}, skip it.", tsFileResource.getTsFilePath(), e);
        } finally {
          // update recovery context
          context.incrementRecoveredFilesNum();
        }
      }
      // After recover, replace partition last flush time with device last flush time
      if (config.isEnableSeparateData()) {
        upgradeAndUpdateDeviceLastFlushTime(partitionId, resourceList);
      }

      return null;
    };
  }

  private void syncRecoverFilesInPartition(
      long partitionId, DataRegionRecoveryContext context, List<TsFileResource> resourceList) {
    for (TsFileResource tsFileResource : resourceList) {
      recoverSealedTsFiles(tsFileResource, context);
    }
    FileTimeIndexCacheRecorder.getInstance()
        .logFileTimeIndex(resourceList.toArray(new TsFileResource[0]));
    if (config.isEnableSeparateData()) {
      if (!lastFlushTimeMap.checkAndCreateFlushedTimePartition(partitionId, true)) {
        TimePartitionManager.getInstance()
            .registerTimePartitionInfo(
                new TimePartitionInfo(
                    new DataRegionId(Integer.parseInt(dataRegionIdString)),
                    partitionId,
                    false,
                    Long.MAX_VALUE,
                    lastFlushTimeMap.getMemSize(partitionId)));
      }
      for (TsFileResource tsFileResource : resourceList) {
        if (!tsFileResource.isDeleted()) {
          updateDeviceLastFlushTime(tsFileResource);
        }
      }
      TimePartitionManager.getInstance()
          .updateAfterFlushing(
              new DataRegionId(Integer.parseInt(dataRegionIdString)),
              partitionId,
              System.currentTimeMillis(),
              lastFlushTimeMap.getMemSize(partitionId),
              false);
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
    long ttl = getTTL(insertRowNode);
    if (!CommonUtils.isAlive(insertRowNode.getTime(), ttl)) {
      throw new OutOfTTLException(
          insertRowNode.getTime(), (CommonDateTimeUtils.currentTime() - ttl));
    }
    StorageEngine.blockInsertionIfReject();
    long startTime = System.nanoTime();
    writeLock("InsertRow");
    PERFORMANCE_OVERVIEW_METRICS.recordScheduleLockCost(System.nanoTime() - startTime);
    try {
      if (deleted) {
        return;
      }
      // init map
      long timePartitionId = TimePartitionUtils.getTimePartitionId(insertRowNode.getTime());
      initFlushTimeMap(timePartitionId);

      boolean isSequence =
          config.isEnableSeparateData()
              && insertRowNode.getTime()
                  > lastFlushTimeMap.getFlushedTime(timePartitionId, insertRowNode.getDeviceID());

      // insert to sequence or unSequence file
      TsFileProcessor tsFileProcessor;
      try {
        tsFileProcessor = insertToTsFileProcessor(insertRowNode, isSequence, timePartitionId);
      } catch (DataTypeInconsistentException e) {
        // flush both MemTables so that the new type can be inserted into a new MemTable
        TsFileProcessor workSequenceProcessor = workSequenceTsFileProcessors.get(timePartitionId);
        if (workSequenceProcessor != null) {
          fileFlushPolicy.apply(this, workSequenceProcessor, workSequenceProcessor.isSequence());
        }
        TsFileProcessor workUnsequenceProcessor =
            workUnsequenceTsFileProcessors.get(timePartitionId);
        if (workUnsequenceProcessor != null) {
          fileFlushPolicy.apply(
              this, workUnsequenceProcessor, workUnsequenceProcessor.isSequence());
        }

        isSequence =
            config.isEnableSeparateData()
                && insertRowNode.getTime()
                    > lastFlushTimeMap.getFlushedTime(timePartitionId, insertRowNode.getDeviceID());
        tsFileProcessor = insertToTsFileProcessor(insertRowNode, isSequence, timePartitionId);
      }

      // check memtable size and may asyncTryToFlush the work memtable
      if (tsFileProcessor != null && tsFileProcessor.shouldFlush()) {
        fileFlushPolicy.apply(this, tsFileProcessor, tsFileProcessor.isSequence());
      }
      if (CommonDescriptor.getInstance().getConfig().isLastCacheEnable()
          && (!insertRowNode.isGeneratedByRemoteConsensusLeader())) {
        // disable updating last cache on follower
        startTime = System.nanoTime();
        tryToUpdateInsertRowLastCache(insertRowNode);
        PERFORMANCE_OVERVIEW_METRICS.recordScheduleUpdateLastCacheCost(
            System.nanoTime() - startTime);
      }
    } finally {
      writeUnlock();
    }
  }

  private long getLastFlushTime(long timePartitionID, IDeviceID deviceID) {
    return config.isEnableSeparateData()
        ? lastFlushTimeMap.getFlushedTime(timePartitionID, deviceID)
        : Long.MAX_VALUE;
  }

  private void split(
      InsertTabletNode insertTabletNode,
      int loc,
      int endOffset,
      Map<Long, List<int[]>[]> splitInfo) {
    // before is first start point
    int before = loc;
    long beforeTime = insertTabletNode.getTimes()[before];
    // before time partition
    long beforeTimePartition = TimePartitionUtils.getTimePartitionId(beforeTime);
    // init flush time map
    initFlushTimeMap(beforeTimePartition);

    // if is sequence
    boolean isSequence = false;
    while (loc < endOffset) {
      long time = insertTabletNode.getTimes()[loc];
      final long timePartitionId = TimePartitionUtils.getTimePartitionId(time);

      long lastFlushTime;
      // judge if we should insert sequence
      if (timePartitionId != beforeTimePartition) {
        initFlushTimeMap(timePartitionId);
        lastFlushTime = getLastFlushTime(timePartitionId, insertTabletNode.getDeviceID(loc));
        updateSplitInfo(splitInfo, beforeTimePartition, isSequence, new int[] {before, loc});
        before = loc;
        beforeTimePartition = timePartitionId;
        isSequence = time > lastFlushTime;
      } else if (!isSequence) {
        lastFlushTime = getLastFlushTime(timePartitionId, insertTabletNode.getDeviceID(loc));
        if (time > lastFlushTime) {
          // the same partition and switch to sequence data
          // insert previous range into unsequence
          updateSplitInfo(splitInfo, beforeTimePartition, false, new int[] {before, loc});
          before = loc;
          isSequence = true;
        }
      }
      // else: the same partition and isSequence not changed, just move the cursor forward
      loc++;
    }

    // do not forget last part
    if (before < loc) {
      updateSplitInfo(splitInfo, beforeTimePartition, isSequence, new int[] {before, loc});
    }
  }

  private void updateSplitInfo(
      Map<Long, List<int[]>[]> splitInfo, long partitionId, boolean isSequence, int[] newRange) {
    if (newRange[0] >= newRange[1]) {
      return;
    }

    @SuppressWarnings("unchecked")
    List<int[]>[] rangeLists = splitInfo.computeIfAbsent(partitionId, k -> new List[2]);
    List<int[]> rangeList = rangeLists[isSequence ? 1 : 0];
    if (rangeList == null) {
      rangeList = new ArrayList<>();
      rangeLists[isSequence ? 1 : 0] = rangeList;
    }
    if (!rangeList.isEmpty()) {
      int[] lastRange = rangeList.get(rangeList.size() - 1);
      if (lastRange[1] == newRange[0]) {
        lastRange[1] = newRange[1];
        return;
      }
    }
    rangeList.add(newRange);
  }

  private boolean doInsert(
      InsertTabletNode insertTabletNode,
      Map<Long, List<int[]>[]> splitMap,
      TSStatus[] results,
      long[] infoForMetrics)
      throws DataTypeInconsistentException {
    boolean noFailure = true;
    for (Entry<Long, List<int[]>[]> entry : splitMap.entrySet()) {
      long timePartitionId = entry.getKey();
      List<int[]>[] rangeLists = entry.getValue();
      List<int[]> sequenceRangeList = rangeLists[1];
      if (sequenceRangeList != null) {
        noFailure =
            insertTabletToTsFileProcessor(
                    insertTabletNode,
                    sequenceRangeList,
                    true,
                    results,
                    timePartitionId,
                    noFailure,
                    infoForMetrics)
                && noFailure;
      }
      List<int[]> unSequenceRangeList = rangeLists[0];
      if (unSequenceRangeList != null) {
        noFailure =
            insertTabletToTsFileProcessor(
                    insertTabletNode,
                    unSequenceRangeList,
                    false,
                    results,
                    timePartitionId,
                    noFailure,
                    infoForMetrics)
                && noFailure;
      }
    }
    return noFailure;
  }

  /**
   * Insert a tablet into this database.
   *
   * @throws BatchProcessException if some of the rows failed to be inserted
   */
  @SuppressWarnings({"squid:S3776", "squid:S6541"}) // Suppress high Cognitive Complexity warning
  public void insertTablet(InsertTabletNode insertTabletNode)
      throws BatchProcessException, WriteProcessException {
    StorageEngine.blockInsertionIfReject();
    long startTime = System.nanoTime();
    writeLock("insertTablet");
    PERFORMANCE_OVERVIEW_METRICS.recordScheduleLockCost(System.nanoTime() - startTime);
    try {
      if (deleted) {
        logger.info(
            "Won't insert tablet {}, because region is deleted", insertTabletNode.getSearchIndex());
        return;
      }
      TSStatus[] results = new TSStatus[insertTabletNode.getRowCount()];
      Arrays.fill(results, RpcUtils.SUCCESS_STATUS);
      long[] infoForMetrics = new long[5];
      // infoForMetrics[0]: CreateMemtableBlockTimeCost
      // infoForMetrics[1]: ScheduleMemoryBlockTimeCost
      // infoForMetrics[2]: ScheduleWalTimeCost
      // infoForMetrics[3]: ScheduleMemTableTimeCost
      // infoForMetrics[4]: InsertedPointsNumber
      boolean noFailure = executeInsertTablet(insertTabletNode, results, infoForMetrics);
      updateTsFileProcessorMetric(insertTabletNode, infoForMetrics);

      if (!noFailure) {
        throw new BatchProcessException(results);
      }
    } finally {
      writeUnlock();
    }
  }

  private boolean splitAndInsert(
      int start,
      InsertTabletNode insertTabletNode,
      TSStatus[] results,
      long[] infoForMetrics,
      List<Pair<IDeviceID, Integer>> deviceEndOffsetPairs) {
    final int initialStart = start;
    try {
      Map<Long, List<int[]>[]> splitInfo = new HashMap<>();
      for (Pair<IDeviceID, Integer> deviceEndOffsetPair : deviceEndOffsetPairs) {
        int end = deviceEndOffsetPair.getRight();
        split(insertTabletNode, start, end, splitInfo);
        start = end;
      }
      return doInsert(insertTabletNode, splitInfo, results, infoForMetrics);
    } catch (DataTypeInconsistentException e) {
      // the exception will trigger a flush, which requires the flush time to be recalculated
      start = initialStart;
      Map<Long, List<int[]>[]> splitInfo = new HashMap<>();
      for (Pair<IDeviceID, Integer> deviceEndOffsetPair : deviceEndOffsetPairs) {
        int end = deviceEndOffsetPair.getRight();
        split(insertTabletNode, start, end, splitInfo);
        start = end;
      }
      try {
        return doInsert(insertTabletNode, splitInfo, results, infoForMetrics);
      } catch (DataTypeInconsistentException ex) {
        logger.error("Data inconsistent exception is not supposed to be triggered twice", ex);
        return false;
      }
    }
  }

  private boolean executeInsertTablet(
      InsertTabletNode insertTabletNode, TSStatus[] results, long[] infoForMetrics)
      throws OutOfTTLException {
    boolean noFailure;
    int loc =
        insertTabletNode.shouldCheckTTL()
            ? insertTabletNode.checkTTL(results, getTTL(insertTabletNode))
            : 0;
    noFailure = loc == 0;
    List<Pair<IDeviceID, Integer>> deviceEndOffsetPairs =
        insertTabletNode.splitByDevice(loc, insertTabletNode.getRowCount());
    noFailure =
        splitAndInsert(loc, insertTabletNode, results, infoForMetrics, deviceEndOffsetPairs)
            && noFailure;

    if (CommonDescriptor.getInstance().getConfig().isLastCacheEnable()
        && !insertTabletNode.isGeneratedByRemoteConsensusLeader()) {
      // disable updating last cache on follower
      long startTime = System.nanoTime();
      tryToUpdateInsertTabletLastCache(insertTabletNode);
      PERFORMANCE_OVERVIEW_METRICS.recordScheduleUpdateLastCacheCost(System.nanoTime() - startTime);
    }
    return noFailure;
  }

  private void initFlushTimeMap(long timePartitionId) {
    if (config.isEnableSeparateData()
        && !lastFlushTimeMap.checkAndCreateFlushedTimePartition(timePartitionId, true)) {
      TimePartitionManager.getInstance()
          .registerTimePartitionInfo(
              new TimePartitionInfo(
                  new DataRegionId(Integer.parseInt(dataRegionIdString)),
                  timePartitionId,
                  true,
                  Long.MAX_VALUE,
                  0));
    }
  }

  /**
   * insert batch to tsfile processor thread-safety that the caller need to guarantee The rows to be
   * inserted are in the range [start, end) Null value in each column values will be replaced by the
   * subsequent non-null value, e.g., {1, null, 3, null, 5} will be {1, 3, 5, null, 5}
   *
   * @param insertTabletNode insert a tablet of a device
   * @param sequence whether is sequence
   * @param rangeList start and end index list of rows to be inserted in insertTabletPlan
   * @param results result array
   * @param timePartitionId time partition id
   * @return false if any failure occurs when inserting the tablet, true otherwise
   */
  private boolean insertTabletToTsFileProcessor(
      InsertTabletNode insertTabletNode,
      List<int[]> rangeList,
      boolean sequence,
      TSStatus[] results,
      long timePartitionId,
      boolean noFailure,
      long[] infoForMetrics)
      throws DataTypeInconsistentException {
    if (insertTabletNode.allMeasurementFailed()) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Won't insert tablet {}, because {}",
            insertTabletNode.getSearchIndex(),
            "insertTabletNode allMeasurementFailed");
      }
      return true;
    }

    TsFileProcessor tsFileProcessor = getOrCreateTsFileProcessor(timePartitionId, sequence);
    if (tsFileProcessor == null) {
      for (int[] rangePair : rangeList) {
        int start = rangePair[0];
        int end = rangePair[1];
        for (int i = start; i < end; i++) {
          results[i] =
              RpcUtils.getStatus(
                  TSStatusCode.INTERNAL_SERVER_ERROR,
                  "can not create TsFileProcessor, timePartitionId: " + timePartitionId);
        }
      }
      return false;
    }

    try {
      // register TableSchema (and maybe more) for table insertion
      registerToTsFile(insertTabletNode, tsFileProcessor);
      tsFileProcessor.insertTablet(insertTabletNode, rangeList, results, noFailure, infoForMetrics);
    } catch (DataTypeInconsistentException e) {
      // flush both MemTables so that the new type can be inserted into a new MemTable
      TsFileProcessor workSequenceProcessor = workSequenceTsFileProcessors.get(timePartitionId);
      if (workSequenceProcessor != null) {
        fileFlushPolicy.apply(this, workSequenceProcessor, workSequenceProcessor.isSequence());
      }
      TsFileProcessor workUnsequenceProcessor = workUnsequenceTsFileProcessors.get(timePartitionId);
      if (workUnsequenceProcessor != null) {
        fileFlushPolicy.apply(this, workUnsequenceProcessor, workUnsequenceProcessor.isSequence());
      }
      throw e;
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

  private TableSchema getTableSchemaFromCache(
      final String database, final String tableName, final Pair<Long, Long> currentVersion) {
    final TableSchemaCacheKey key = new TableSchemaCacheKey(database, tableName);
    final Triple<Long, Long, TableSchema> cached = TABLE_SCHEMA_CACHE.getIfPresent(key);
    if (cached == null) {
      return null;
    }
    if (cached.getLeft().equals(currentVersion.getLeft())
        && cached.getMiddle().equals(currentVersion.getRight())) {
      return cached.getRight();
    } else {
      // remove stale entry to avoid unbounded growth (only on version mismatch)
      TABLE_SCHEMA_CACHE.invalidate(key);
      return null;
    }
  }

  private void cacheTableSchema(
      final String database,
      final String tableName,
      final Pair<Long, Long> version,
      final TableSchema tableSchema) {
    if (tableSchema == null) {
      TABLE_SCHEMA_CACHE.invalidate(new TableSchemaCacheKey(database, tableName));
      return;
    }
    TABLE_SCHEMA_CACHE.put(
        new TableSchemaCacheKey(database, tableName),
        Triple.of(version.getLeft(), version.getRight(), tableSchema));
  }

  private static final class TableSchemaCacheKey {
    private final String database;
    private final String tableName;

    private TableSchemaCacheKey(final String database, final String tableName) {
      this.database = database;
      this.tableName = tableName;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof TableSchemaCacheKey)) {
        return false;
      }
      final TableSchemaCacheKey that = (TableSchemaCacheKey) o;
      return Objects.equals(database, that.database) && Objects.equals(tableName, that.tableName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(database, tableName);
    }
  }

  private TsFileProcessor insertTabletWithTypeConsistencyCheck(
      TsFileProcessor tsFileProcessor,
      InsertTabletNode insertTabletNode,
      List<int[]> rangeList,
      TSStatus[] results,
      boolean noFailure,
      long[] infoForMetrics)
      throws WriteProcessException {

    return tsFileProcessor;
  }

  private void registerToTsFile(InsertNode node, TsFileProcessor tsFileProcessor) {
    final String tableName = node.getTableName();
    if (tableName != null) {
      tsFileProcessor.registerToTsFile(
          tableName,
          t -> {
            final String database = getDatabaseName();

            TsTable tsTable = DataNodeTableCache.getInstance().getTable(database, t, false);
            if (tsTable == null) {
              // There is a high probability that the leader node has been executed and is currently
              // located in the follower node.
              if (node.isGeneratedByRemoteConsensusLeader()) {
                // If current node is follower, after request config node and get the answer that
                // table is exist or not, then tell leader node when table is not exist.
                TDescTableResp resp;
                try (ConfigNodeClient client =
                    ConfigNodeClientManager.getInstance()
                        .borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
                  resp = client.describeTable(getDatabaseName(), tableName, false);
                  if (resp == null || resp.tableInfo == null) {
                    TableMetadataImpl.throwTableNotExistsException(getDatabaseName(), tableName);
                  }
                  // For table schema from ConfigNode, we cannot get version info,
                  // so we don't cache it to avoid version mismatch
                  final TableSchema schema =
                      TsFileTableSchemaUtil.tsTableBufferToTableSchemaNoAttribute(
                          ByteBuffer.wrap(resp.getTableInfo()));
                  return schema;
                } catch (TException | ClientManagerException e) {
                  logger.error(
                      "Remote request config node failed that judgment if table is exist, occur exception. {}",
                      e.getMessage());
                  TableMetadataImpl.throwTableNotExistsException(getDatabaseName(), tableName);
                  return null; // unreachable, throwTableNotExistsException always throws
                }
              } else {
                // Here may be invoked by leader node, the table is very unexpected not exist in the
                // DataNodeTableCache
                logger.error(
                    "Due tsTable is null, table schema can't be got, leader node occur special situation need to resolve.");
                throw new TableLostRuntimeException(getDatabaseName(), tableName);
              }
            }

            final Pair<Long, Long> currentVersion = tsTable.getInstanceVersion();
            final TableSchema cachedSchema = getTableSchemaFromCache(database, t, currentVersion);
            if (cachedSchema != null) {
              return cachedSchema;
            }

            final TableSchema schema =
                TsFileTableSchemaUtil.toTsFileTableSchemaNoAttribute(tsTable);
            cacheTableSchema(database, t, currentVersion, schema);
            return schema;
          });
    }
  }

  private void tryToUpdateInsertTabletLastCache(final InsertTabletNode node) {
    node.updateLastCache(getDatabaseName());
  }

  private TsFileProcessor insertToTsFileProcessor(
      InsertRowNode insertRowNode, boolean sequence, long timePartitionId)
      throws WriteProcessException {
    TsFileProcessor tsFileProcessor = getOrCreateTsFileProcessor(timePartitionId, sequence);
    if (tsFileProcessor == null || insertRowNode.allMeasurementFailed()) {
      return null;
    }
    long[] infoForMetrics = new long[5];
    // infoForMetrics[0]: CreateMemtableBlockTimeCost
    // infoForMetrics[1]: ScheduleMemoryBlockTimeCost
    // infoForMetrics[2]: ScheduleWalTimeCost
    // infoForMetrics[3]: ScheduleMemTableTimeCost
    // infoForMetrics[4]: InsertedPointsNumber
    tsFileProcessor.insert(insertRowNode, infoForMetrics);
    updateTsFileProcessorMetric(insertRowNode, infoForMetrics);
    // register TableSchema (and maybe more) for table insertion
    registerToTsFile(insertRowNode, tsFileProcessor);
    return tsFileProcessor;
  }

  private void tryToUpdateInsertRowLastCache(final InsertRowNode node) {
    node.updateLastCache(databaseName);
  }

  private List<InsertRowNode> insertToTsFileProcessors(
      InsertRowsNode insertRowsNode,
      boolean[] areSequence,
      long[] timePartitionIds,
      long[] infoForMetrics) {
    Map<TsFileProcessor, InsertRowsNode> tsFileProcessorMap = new HashMap<>();
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
      int finalI = i;
      tsFileProcessorMap.compute(
          tsFileProcessor,
          (k, v) -> {
            if (v == null) {
              v = insertRowsNode.emptyClone();
              v.setSearchIndex(insertRowNode.getSearchIndex());
              v.setAligned(insertRowNode.isAligned());
              if (insertRowNode.isGeneratedByPipe()) {
                v.markAsGeneratedByPipe();
              }
              if (insertRowNode.isGeneratedByRemoteConsensusLeader()) {
                v.markAsGeneratedByRemoteConsensusLeader();
              }
            }
            if (v.isAligned() != insertRowNode.isAligned()) {
              v.setMixingAlignment(true);
            }
            v.addOneInsertRowNode(insertRowNode, finalI);
            v.updateProgressIndex(insertRowNode.getProgressIndex());
            return v;
          });
    }

    List<InsertRowNode> executedInsertRowNodeList = new ArrayList<>();
    for (Map.Entry<TsFileProcessor, InsertRowsNode> entry : tsFileProcessorMap.entrySet()) {
      TsFileProcessor tsFileProcessor = entry.getKey();
      InsertRowsNode subInsertRowsNode = entry.getValue();
      try {
        tsFileProcessor =
            insertRowsWithTypeConsistencyCheck(tsFileProcessor, subInsertRowsNode, infoForMetrics);
      } catch (WriteProcessException e) {
        insertRowsNode
            .getResults()
            .put(
                subInsertRowsNode.getInsertRowNodeIndexList().get(0),
                RpcUtils.getStatus(e.getErrorCode(), e.getMessage()));
      }
      executedInsertRowNodeList.addAll(subInsertRowsNode.getInsertRowNodeList());

      // check memtable size and may asyncTryToFlush the work memtable
      if (entry.getKey().shouldFlush()) {
        fileFlushPolicy.apply(this, tsFileProcessor, tsFileProcessor.isSequence());
      }
    }
    return executedInsertRowNodeList;
  }

  private TsFileProcessor insertRowsWithTypeConsistencyCheck(
      TsFileProcessor tsFileProcessor, InsertRowsNode subInsertRowsNode, long[] infoForMetrics)
      throws WriteProcessException {
    try {
      // register TableSchema (and maybe more) for table insertion
      registerToTsFile(subInsertRowsNode, tsFileProcessor);
      tsFileProcessor.insertRows(subInsertRowsNode, infoForMetrics);
    } catch (DataTypeInconsistentException e) {
      InsertRowNode firstRow = subInsertRowsNode.getInsertRowNodeList().get(0);
      long timePartitionId = TimePartitionUtils.getTimePartitionId(firstRow.getTime());
      // flush both MemTables so that the new type can be inserted into a new MemTable
      TsFileProcessor workSequenceProcessor = workSequenceTsFileProcessors.get(timePartitionId);
      if (workSequenceProcessor != null) {
        fileFlushPolicy.apply(this, workSequenceProcessor, workSequenceProcessor.isSequence());
      }
      TsFileProcessor workUnsequenceProcessor = workUnsequenceTsFileProcessors.get(timePartitionId);
      if (workUnsequenceProcessor != null) {
        fileFlushPolicy.apply(this, workUnsequenceProcessor, workUnsequenceProcessor.isSequence());
      }

      boolean isSequence =
          config.isEnableSeparateData()
              && firstRow.getTime()
                  > lastFlushTimeMap.getFlushedTime(timePartitionId, firstRow.getDeviceID());
      tsFileProcessor = getOrCreateTsFileProcessor(timePartitionId, isSequence);
      registerToTsFile(subInsertRowsNode, tsFileProcessor);
      tsFileProcessor.insertRows(subInsertRowsNode, infoForMetrics);
    }
    return tsFileProcessor;
  }

  private void tryToUpdateInsertRowsLastCache(List<InsertRowNode> nodeList) {
    for (InsertRowNode node : nodeList) {
      node.updateLastCache(databaseName);
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
    if (closingSequenceTsFileProcessor.contains(tsFileProcessor)
        || closingUnSequenceTsFileProcessor.contains(tsFileProcessor)
        || tsFileProcessor.alreadyMarkedClosing()) {
      return;
    }
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
              new DataRegionId(Integer.parseInt(dataRegionIdString)), timeRangeId);
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
    long version =
        partitionMaxFileVersions.compute(
            timePartitionId, (key, oldVersion) -> (oldVersion == null ? 1 : oldVersion + 1));
    String filePath =
        TsFileNameGenerator.generateNewTsFilePathWithMkdir(
            sequence,
            databaseName,
            dataRegionIdString,
            timePartitionId,
            System.currentTimeMillis(),
            version,
            0,
            0);

    return getTsFileProcessor(sequence, filePath, timePartitionId);
  }

  private TsFileProcessor getTsFileProcessor(
      boolean sequence, String filePath, long timePartitionId) throws IOException {
    TsFileProcessor tsFileProcessor =
        new TsFileProcessor(
            databaseName + FILE_NAME_SEPARATOR + dataRegionIdString,
            fsFactory.getFileWithParent(filePath),
            dataRegionInfo,
            this::closeUnsealedTsFileProcessorCallBack,
            this::flushCallback,
            sequence);

    TsFileProcessorInfo tsFileProcessorInfo = new TsFileProcessorInfo(dataRegionInfo);
    tsFileProcessor.setTsFileProcessorInfo(tsFileProcessorInfo);
    this.dataRegionInfo.initTsFileProcessorInfo(tsFileProcessor);

    tsFileProcessor.addCloseFileListeners(customCloseFileListeners);
    tsFileProcessor.addFlushListeners(customFlushListeners);
    tsFileProcessor.setTimeRangeId(timePartitionId);

    return tsFileProcessor;
  }

  private String getNewTsFileName(long time, long version, int mergeCnt, int unseqCompactionCnt) {
    return TsFileNameGenerator.generateNewTsFileName(time, version, mergeCnt, unseqCompactionCnt);
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
    Future<?> future;
    if (sequence) {
      closingSequenceTsFileProcessor.add(tsFileProcessor);
      future = tsFileProcessor.asyncClose();
      if (future.isDone()) {
        closingSequenceTsFileProcessor.remove(tsFileProcessor);
      }

      workSequenceTsFileProcessors.remove(tsFileProcessor.getTimeRangeId());
    } else {
      closingUnSequenceTsFileProcessor.add(tsFileProcessor);
      future = tsFileProcessor.asyncClose();
      if (future.isDone()) {
        closingUnSequenceTsFileProcessor.remove(tsFileProcessor);
      }

      workUnsequenceTsFileProcessors.remove(tsFileProcessor.getTimeRangeId());
    }
    TsFileResource resource = tsFileProcessor.getTsFileResource();
    logger.info(
        "Async close tsfile: {}, file start time: {}, file end time: {}",
        resource.getTsFile().getAbsolutePath(),
        resource.getFileStartTime(),
        resource.getFileEndTime());
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
        databaseName + "-" + dataRegionIdString,
        systemDir);
    FileTimeIndexCacheRecorder.getInstance()
        .removeFileTimeIndexCache(Integer.parseInt(dataRegionIdString));
    writeLock("deleteFolder");
    try {
      File dataRegionSystemFolder =
          SystemFileFactory.INSTANCE.getFile(
              systemDir + File.separator + databaseName, dataRegionIdString);
      org.apache.iotdb.commons.utils.FileUtils.deleteDirectoryAndEmptyParent(
          dataRegionSystemFolder);
    } finally {
      writeUnlock();
    }
  }

  public void deleteDALFolderAndClose() {
    Optional.ofNullable(DeletionResourceManager.getInstance(dataRegionIdString))
        .ifPresent(
            manager -> {
              manager.close();
              manager.removeDAL();
            });
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
  public void syncDeleteDataFiles() throws TsFileProcessorException {
    logger.info(
        "{} will close all files for deleting data files", databaseName + "-" + dataRegionIdString);
    writeLock("syncDeleteDataFiles");
    try {
      forceCloseAllWorkingTsFileProcessors();
      waitClosingTsFileProcessorFinished();

      // normally, mergingModification is just need to be closed by after a merge task is finished.
      // we close it here just for IT test.
      closeAllResources();
      List<TsFileResource> tsFileResourceList = tsFileManager.getTsFileList(true);
      tsFileResourceList.addAll(tsFileManager.getTsFileList(false));
      tsFileResourceList.forEach(
          x -> {
            FileMetrics.getInstance().deleteTsFile(x.isSeq(), Collections.singletonList(x));
            try {
              x.removeModFile();
            } catch (IOException e) {
              logger.warn("Cannot remove mod file {}", x, e);
            }
          });
      deleteAllSGFolders(TierManager.getInstance().getAllFilesFolders());
      deleteAllObjectFiles(TierManager.getInstance().getAllObjectFileFolders());
      this.workSequenceTsFileProcessors.clear();
      this.workUnsequenceTsFileProcessors.clear();
      this.tsFileManager.clear();
      lastFlushTimeMap.clearFlushedTime();
      lastFlushTimeMap.clearGlobalFlushedTime();
      TimePartitionManager.getInstance()
          .removeTimePartitionInfo(new DataRegionId(Integer.parseInt(dataRegionIdString)));
    } catch (InterruptedException e) {
      logger.error(
          "CloseFileNodeCondition error occurs while waiting for closing the storage " + "group {}",
          databaseName + "-" + dataRegionIdString,
          e);
      Thread.currentThread().interrupt();
    } finally {
      writeUnlock();
    }
  }

  private void deleteAllSGFolders(List<String> folder) {
    for (String tsfilePath : folder) {
      File dataRegionDataFolder =
          fsFactory.getFile(tsfilePath, databaseName + File.separator + dataRegionIdString);
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

  private void deleteAllObjectFiles(List<String> folders) {
    for (String objectFolder : folders) {
      File dataRegionObjectFolder = fsFactory.getFile(objectFolder, dataRegionIdString);
      AtomicLong totalSize = new AtomicLong(0);
      AtomicInteger count = new AtomicInteger(0);
      try (Stream<Path> paths = Files.walk(dataRegionObjectFolder.toPath())) {
        paths
            .filter(Files::isRegularFile)
            .filter(
                path -> {
                  String name = path.getFileName().toString();
                  return name.endsWith(".bin");
                })
            .forEach(
                path -> {
                  count.incrementAndGet();
                  totalSize.addAndGet(path.toFile().length());
                });
      } catch (IOException e) {
        logger.error("Failed to check Object Files: {}", e.getMessage());
      }
      FileMetrics.getInstance().decreaseObjectFileNum(count.get());
      FileMetrics.getInstance().decreaseObjectFileSize(totalSize.get());
      if (FSUtils.getFSType(dataRegionObjectFolder) != FSType.LOCAL) {
        try {
          fsFactory.deleteDirectory(dataRegionObjectFolder.getPath());
        } catch (IOException e) {
          logger.error("Fail to delete data region object folder {}", dataRegionObjectFolder);
        }
      } else {
        if (dataRegionObjectFolder.exists()) {
          org.apache.iotdb.commons.utils.FileUtils.deleteFileOrDirectory(dataRegionObjectFolder);
        }
      }
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
        if (tsFileProcessor.getWorkMemTableUpdateTime() < timeLowerBound) {
          logger.info(
              "Exceed sequence memtable flush interval, so flush working memtable of time partition {} in database {}[{}]",
              tsFileProcessor.getTimeRangeId(),
              databaseName,
              dataRegionIdString);
          fileFlushPolicy.apply(this, tsFileProcessor, tsFileProcessor.isSequence());
          count++;
        }
      }
    } finally {
      writeUnlock();
    }
    WritingMetrics.getInstance().recordTimedFlushMemTableCount(count);
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
        if (tsFileProcessor.getWorkMemTableUpdateTime() < timeLowerBound) {
          logger.info(
              "Exceed unsequence memtable flush interval, so flush working memtable of time partition {} in database {}[{}]",
              tsFileProcessor.getTimeRangeId(),
              databaseName,
              dataRegionIdString);
          fileFlushPolicy.apply(this, tsFileProcessor, tsFileProcessor.isSequence());
          count++;
        }
      }
    } finally {
      writeUnlock();
    }
    WritingMetrics.getInstance().recordTimedFlushMemTableCount(count);
  }

  /** This method will be blocked until all tsfile processors are closed. */
  public void syncCloseAllWorkingTsFileProcessors() {
    try {
      List<Future<?>> tsFileProcessorsClosingFutures = asyncCloseAllWorkingTsFileProcessors();
      for (Future<?> f : tsFileProcessorsClosingFutures) {
        if (f != null) {
          f.get();
        }
      }
    } catch (InterruptedException | ExecutionException e) {
      logger.error(
          "CloseFileNodeCondition error occurs while waiting for closing tsfile processors of {}",
          databaseName + "-" + dataRegionIdString,
          e);
      Thread.currentThread().interrupt();
    }
  }

  public void syncCloseWorkingTsFileProcessors(boolean sequence) {
    try {
      writeLock("syncCloseWorkingTsFileProcessors");
      List<Future<?>> tsFileProcessorsClosingFutures = new ArrayList<>();
      int count = 0;
      try {
        // to avoid concurrent modification problem, we need a new array list
        for (TsFileProcessor tsFileProcessor :
            new ArrayList<>(
                sequence
                    ? workSequenceTsFileProcessors.values()
                    : workUnsequenceTsFileProcessors.values())) {
          tsFileProcessorsClosingFutures.add(
              asyncCloseOneTsFileProcessor(sequence, tsFileProcessor));
          count++;
        }
      } finally {
        writeUnlock();
      }
      WritingMetrics.getInstance().recordManualFlushMemTableCount(count);
      for (Future<?> f : tsFileProcessorsClosingFutures) {
        if (f != null) {
          f.get();
        }
      }
    } catch (InterruptedException | ExecutionException e) {
      logger.error(
          "CloseFileNodeCondition error occurs while waiting for closing tsfile processors of {}",
          databaseName + "-" + dataRegionIdString,
          e);
      Thread.currentThread().interrupt();
    }
  }

  private void waitClosingTsFileProcessorFinished() throws InterruptedException {
    long startTime = System.currentTimeMillis();
    logger.info(
        "Start to wait TsFiles to close, seq files: {}, unseq files: {}",
        closingSequenceTsFileProcessor,
        closingUnSequenceTsFileProcessor);
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
            databaseName + "-" + this.dataRegionIdString,
            (System.currentTimeMillis() - startTime) / 1000);
        logger.warn(
            "Sseq files: {}, unseq files: {}",
            closingSequenceTsFileProcessor,
            closingUnSequenceTsFileProcessor);
      }
    }
  }

  /** close all working tsfile processors */
  public List<Future<?>> asyncCloseAllWorkingTsFileProcessors() {
    writeLock("asyncCloseAllWorkingTsFileProcessors");
    List<Future<?>> futures = new ArrayList<>();
    int count = 0;
    try {
      logger.info(
          "async force close all files in database: {}", databaseName + "-" + dataRegionIdString);
      // to avoid concurrent modification problem, we need a new array list
      for (TsFileProcessor tsFileProcessor :
          new ArrayList<>(workSequenceTsFileProcessors.values())) {
        futures.add(asyncCloseOneTsFileProcessor(true, tsFileProcessor));
        count++;
      }
      // to avoid concurrent modification problem, we need a new array list
      for (TsFileProcessor tsFileProcessor :
          new ArrayList<>(workUnsequenceTsFileProcessors.values())) {
        futures.add(asyncCloseOneTsFileProcessor(false, tsFileProcessor));
        count++;
      }
    } finally {
      writeUnlock();
    }
    WritingMetrics.getInstance().recordManualFlushMemTableCount(count);
    return futures;
  }

  /** force close all working tsfile processors */
  public void forceCloseAllWorkingTsFileProcessors() throws TsFileProcessorException {
    writeLock("forceCloseAllWorkingTsFileProcessors");
    try {
      logger.info(
          "force close all processors in database: {}", databaseName + "-" + dataRegionIdString);
      // to avoid concurrent modification problem, we need a new array list
      List<TsFileResource> closedTsFileResources = new ArrayList<>();
      for (TsFileProcessor tsFileProcessor :
          new ArrayList<>(workSequenceTsFileProcessors.values())) {
        tsFileProcessor.putMemTableBackAndClose();
        closedTsFileResources.add(tsFileProcessor.getTsFileResource());
      }
      // to avoid concurrent modification problem, we need a new array list
      for (TsFileProcessor tsFileProcessor :
          new ArrayList<>(workUnsequenceTsFileProcessors.values())) {
        tsFileProcessor.putMemTableBackAndClose();
        closedTsFileResources.add(tsFileProcessor.getTsFileResource());
      }
      for (TsFileResource resource : closedTsFileResources) {
        FileMetrics.getInstance()
            .addTsFile(
                resource.getDatabaseName(),
                resource.getDataRegionId(),
                resource.getTsFileSize(),
                resource.isSeq(),
                resource.getTsFile().getName());
      }
      WritingMetrics.getInstance().recordActiveTimePartitionCount(-1);
      logger.info("{} files were closed", closedTsFileResources.size());
    } finally {
      writeUnlock();
    }
  }

  @Override
  @TestOnly
  public QueryDataSource query(
      List<IFullPath> pathList,
      IDeviceID singleDeviceId,
      QueryContext context,
      Filter globalTimeFilter,
      List<Long> timePartitions)
      throws QueryProcessException {
    return query(
        pathList, singleDeviceId, context, globalTimeFilter, timePartitions, Long.MAX_VALUE);
  }

  /** used for query engine */
  @Override
  public QueryDataSource query(
      List<IFullPath> pathList,
      IDeviceID singleDeviceId,
      QueryContext context,
      Filter globalTimeFilter,
      List<Long> timePartitions,
      long waitForLockTimeInMs)
      throws QueryProcessException {

    Pair<List<TsFileResource>, List<TsFileResource>> pair =
        tsFileManager.getAllTsFileListForQuery(timePartitions, globalTimeFilter);

    List<TsFileResource> seqTsFileResouceList = pair.left;
    List<TsFileResource> unSeqTsFileResouceList = pair.right;

    List<TsFileProcessor> needToUnLockList = new ArrayList<>();

    boolean success =
        tryGetFLushLock(
            waitForLockTimeInMs,
            singleDeviceId,
            globalTimeFilter,
            context.isDebug(),
            seqTsFileResouceList,
            unSeqTsFileResouceList,
            needToUnLockList);

    if (success) {
      try {
        List<TsFileResource> satisfiedSeqResourceList =
            getFileResourceListForQuery(
                seqTsFileResouceList, pathList, singleDeviceId, context, globalTimeFilter, true);

        List<TsFileResource> satisfiedUnSeqResourceList =
            getFileResourceListForQuery(
                unSeqTsFileResouceList, pathList, singleDeviceId, context, globalTimeFilter, false);

        QUERY_RESOURCE_METRIC_SET.recordQueryResourceNum(
            SEQUENCE_TSFILE, satisfiedSeqResourceList.size());
        QUERY_RESOURCE_METRIC_SET.recordQueryResourceNum(
            UNSEQUENCE_TSFILE, satisfiedUnSeqResourceList.size());
        return new QueryDataSource(
            satisfiedSeqResourceList, satisfiedUnSeqResourceList, databaseName);
      } catch (MetadataException e) {
        throw new QueryProcessException(e);
      } finally {
        clearAlreadyLockedList(needToUnLockList);
      }
    } else {
      // means that failed to acquire lock within the specific time
      return null;
    }
  }

  /**
   * try to get flush lock for each unclosed satisfied tsfile
   *
   * @return true if lock successfully, otherwise false if return false, needToUnLockList will
   *     always be empty because this method is responsible for unlocking all the already-acquiring
   *     lock if return true, the caller is responsible for unlocking all the already-acquiring lock
   *     in needToUnLockList
   */
  private boolean tryGetFLushLock(
      long waitTimeInMs,
      IDeviceID singleDeviceId,
      Filter globalTimeFilter,
      boolean isDebug,
      List<TsFileResource> seqResources,
      List<TsFileResource> unSeqResources,
      List<TsFileProcessor> needToUnLockList) {
    // deal with seq resources
    for (TsFileResource tsFileResource : seqResources) {
      // only need to acquire flush lock for those unclosed and satisfied tsfile
      if (!tsFileResource.isClosed()
          && tsFileResource.isSatisfied(singleDeviceId, globalTimeFilter, true, isDebug)) {
        TsFileProcessor tsFileProcessor = tsFileResource.getProcessor();
        try {
          if (tsFileProcessor == null) {
            // tsFileProcessor == null means this tsfile is being closed, here we try to busy loop
            // until status in TsFileResource has been changed which is supposed to be the last step
            // of closing
            while (!tsFileResource.isClosed() && waitTimeInMs > 0) {
              TimeUnit.MILLISECONDS.sleep(5);
              waitTimeInMs -= 5;
            }
            if (tsFileResource.isClosed()) {
              continue;
            } else {
              clearAlreadyLockedList(needToUnLockList);
              return false;
            }
          }
          long startTime = System.nanoTime();
          if (tsFileProcessor.tryReadLock(waitTimeInMs)) {
            // minus already consumed time
            waitTimeInMs -= (System.nanoTime() - startTime) / 1_000_000;

            needToUnLockList.add(tsFileProcessor);

            // no remaining time slice
            if (waitTimeInMs <= 0) {
              clearAlreadyLockedList(needToUnLockList);
              return false;
            }
          } else {
            clearAlreadyLockedList(needToUnLockList);
            return false;
          }
        } catch (InterruptedException e) {
          clearAlreadyLockedList(needToUnLockList);
          Thread.currentThread().interrupt();
          return false;
        } catch (Throwable throwable) {
          clearAlreadyLockedList(needToUnLockList);
          throw throwable;
        }
      }
    }
    // deal with unSeq resources
    for (TsFileResource tsFileResource : unSeqResources) {
      if (!tsFileResource.isClosed()
          && tsFileResource.isSatisfied(singleDeviceId, globalTimeFilter, false, isDebug)) {
        TsFileProcessor tsFileProcessor = tsFileResource.getProcessor();
        try {
          if (tsFileProcessor == null) {
            // tsFileProcessor == null means this tsfile is being closed, here we try to busy loop
            // until status in TsFileResource has been changed which is supposed to be the last step
            // of closing
            while (!tsFileResource.isClosed() && waitTimeInMs > 0) {
              TimeUnit.MILLISECONDS.sleep(5);
              waitTimeInMs -= 5;
            }
            if (tsFileResource.isClosed()) {
              continue;
            } else {
              clearAlreadyLockedList(needToUnLockList);
              return false;
            }
          }
          long startTime = System.nanoTime();
          if (tsFileProcessor.tryReadLock(waitTimeInMs)) {
            // minus already consumed time
            waitTimeInMs -= (System.nanoTime() - startTime) / 1_000_000;

            needToUnLockList.add(tsFileProcessor);
            // no remaining time slice
            if (waitTimeInMs <= 0) {
              clearAlreadyLockedList(needToUnLockList);
              return false;
            }
          } else {
            clearAlreadyLockedList(needToUnLockList);
            return false;
          }
        } catch (InterruptedException e) {
          clearAlreadyLockedList(needToUnLockList);
          Thread.currentThread().interrupt();
          return false;
        } catch (Throwable throwable) {
          clearAlreadyLockedList(needToUnLockList);
          throw throwable;
        }
      }
    }
    return true;
  }

  private void clearAlreadyLockedList(List<TsFileProcessor> needToUnLockList) {
    for (TsFileProcessor processor : needToUnLockList) {
      processor.readUnLock();
    }
    needToUnLockList.clear();
  }

  @Override
  public IQueryDataSource queryForSeriesRegionScan(
      List<IFullPath> pathList,
      QueryContext queryContext,
      Filter globalTimeFilter,
      List<Long> timePartitions,
      long waitForLockTimeInMs) {
    Pair<List<TsFileResource>, List<TsFileResource>> pair =
        tsFileManager.getAllTsFileListForQuery(timePartitions, globalTimeFilter);

    List<TsFileResource> seqTsFileResouceList = pair.left;
    List<TsFileResource> unSeqTsFileResouceList = pair.right;

    List<TsFileProcessor> needToUnLockList = new ArrayList<>();

    boolean success =
        tryGetFLushLock(
            waitForLockTimeInMs,
            null,
            globalTimeFilter,
            queryContext.isDebug(),
            seqTsFileResouceList,
            unSeqTsFileResouceList,
            needToUnLockList);

    if (success) {
      try {
        List<IFileScanHandle> seqFileScanHandles =
            getFileHandleListForQuery(
                seqTsFileResouceList, pathList, queryContext, globalTimeFilter, true);

        List<IFileScanHandle> unSeqFileScanHandles =
            getFileHandleListForQuery(
                tsFileManager.getTsFileList(false, timePartitions, globalTimeFilter),
                pathList,
                queryContext,
                globalTimeFilter,
                false);

        QUERY_RESOURCE_METRIC_SET.recordQueryResourceNum(
            SEQUENCE_TSFILE, seqFileScanHandles.size());
        QUERY_RESOURCE_METRIC_SET.recordQueryResourceNum(
            UNSEQUENCE_TSFILE, unSeqFileScanHandles.size());
        return new QueryDataSourceForRegionScan(seqFileScanHandles, unSeqFileScanHandles);
      } finally {
        clearAlreadyLockedList(needToUnLockList);
      }
    } else {
      // means that failed to acquire lock within the specific time
      return null;
    }
  }

  private List<IFileScanHandle> getFileHandleListForQuery(
      Collection<TsFileResource> tsFileResources,
      List<IFullPath> partialPaths,
      QueryContext context,
      Filter globalTimeFilter,
      boolean isSeq) {
    List<IFileScanHandle> fileScanHandles = new ArrayList<>();

    for (TsFileResource tsFileResource : tsFileResources) {
      if (!tsFileResource.isSatisfied(null, globalTimeFilter, isSeq, context.isDebug())) {
        continue;
      }
      if (tsFileResource.isClosed()) {
        fileScanHandles.add(new ClosedFileScanHandleImpl(tsFileResource, context));
      } else {
        tsFileResource
            .getProcessor()
            .queryForSeriesRegionScanWithoutLock(
                partialPaths, context, fileScanHandles, globalTimeFilter);
      }
    }
    return fileScanHandles;
  }

  @Override
  public IQueryDataSource queryForDeviceRegionScan(
      Map<IDeviceID, DeviceContext> devicePathToAligned,
      QueryContext queryContext,
      Filter globalTimeFilter,
      List<Long> timePartitions,
      long waitForLockTimeInMs) {

    Pair<List<TsFileResource>, List<TsFileResource>> pair =
        tsFileManager.getAllTsFileListForQuery(timePartitions, globalTimeFilter);

    List<TsFileResource> seqTsFileResouceList = pair.left;
    List<TsFileResource> unSeqTsFileResouceList = pair.right;

    List<TsFileProcessor> needToUnLockList = new ArrayList<>();

    boolean success =
        tryGetFLushLock(
            waitForLockTimeInMs,
            null,
            globalTimeFilter,
            queryContext.isDebug(),
            seqTsFileResouceList,
            unSeqTsFileResouceList,
            needToUnLockList);

    if (success) {
      try {
        List<IFileScanHandle> seqFileScanHandles =
            getFileHandleListForQuery(
                seqTsFileResouceList, devicePathToAligned, queryContext, globalTimeFilter, true);

        List<IFileScanHandle> unSeqFileScanHandles =
            getFileHandleListForQuery(
                tsFileManager.getTsFileList(false, timePartitions, globalTimeFilter),
                devicePathToAligned,
                queryContext,
                globalTimeFilter,
                false);

        QUERY_RESOURCE_METRIC_SET.recordQueryResourceNum(
            SEQUENCE_TSFILE, seqFileScanHandles.size());
        QUERY_RESOURCE_METRIC_SET.recordQueryResourceNum(
            UNSEQUENCE_TSFILE, unSeqFileScanHandles.size());
        return new QueryDataSourceForRegionScan(seqFileScanHandles, unSeqFileScanHandles);
      } finally {
        clearAlreadyLockedList(needToUnLockList);
      }
    } else {
      // means that failed to acquire lock within the specific time
      return null;
    }
  }

  private List<IFileScanHandle> getFileHandleListForQuery(
      Collection<TsFileResource> tsFileResources,
      Map<IDeviceID, DeviceContext> devicePathsToContext,
      QueryContext context,
      Filter globalTimeFilter,
      boolean isSeq) {
    List<IFileScanHandle> fileScanHandles = new ArrayList<>();

    for (TsFileResource tsFileResource : tsFileResources) {
      if (!tsFileResource.isSatisfied(null, globalTimeFilter, isSeq, context.isDebug())) {
        continue;
      }
      if (tsFileResource.isClosed()) {
        fileScanHandles.add(new ClosedFileScanHandleImpl(tsFileResource, context));
      } else {
        tsFileResource
            .getProcessor()
            .queryForDeviceRegionScanWithoutLock(
                devicePathsToContext, context, fileScanHandles, globalTimeFilter);
      }
    }
    return fileScanHandles;
  }

  /** lock the read lock of the insert lock */
  @Override
  public boolean tryReadLock(long waitMillis) {
    try {
      // apply read lock for SG insert lock to prevent inconsistent with concurrently writing
      // memtable
      long startTime = System.nanoTime();
      if (insertLock.readLock().tryLock(waitMillis, TimeUnit.MILLISECONDS)) {
        // minus already consumed time
        waitMillis -= (System.nanoTime() - startTime) / 1_000_000;
        // no remaining time slice
        if (waitMillis <= 0) {
          insertLock.readLock().unlock();
          return false;
        }
        return tryGetTsFileManagerReadLock(waitMillis);
      } else {
        return false;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  private boolean tryGetTsFileManagerReadLock(long waitMillis) {
    // apply read lock for TsFileResource list
    try {
      if (tsFileManager.tryReadLock(waitMillis)) {
        return true;
      } else {
        // failed to acquire tsFileManager read lock, we also need to unlock the insertLock
        insertLock.readLock().unlock();
        return false;
      }
    } catch (InterruptedException e) {
      // failed to acquire tsFileManager read lock, we also need to unlock the insertLock
      insertLock.readLock().unlock();
      Thread.currentThread().interrupt();
      return false;
    }
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
      List<IFullPath> pathList,
      IDeviceID singleDeviceId,
      QueryContext context,
      Filter globalTimeFilter,
      boolean isSeq)
      throws MetadataException {

    List<TsFileResource> tsfileResourcesForQuery = new ArrayList<>();

    for (TsFileResource tsFileResource : tsFileResources) {
      if (!tsFileResource.isSatisfied(singleDeviceId, globalTimeFilter, isSeq, context.isDebug())) {
        continue;
      }
      try {
        if (tsFileResource.isClosed()) {
          tsfileResourcesForQuery.add(tsFileResource);
        } else {
          tsFileResource
              .getProcessor()
              .queryWithoutLock(pathList, context, tsfileResourcesForQuery, globalTimeFilter);
        }
      } catch (IOException e) {
        throw new MetadataException(e);
      }
    }
    return tsfileResourcesForQuery;
  }

  /** Separate tsfiles in TsFileManager to sealedList and unsealedList. */
  private void getTwoKindsOfTsFiles(
      List<TsFileResource> sealedResource,
      List<TsFileResource> unsealedResource,
      long startTime,
      long endTime) {
    List<TsFileResource> tsFileResources = tsFileManager.getTsFileList(true, startTime, endTime);
    tsFileResources.addAll(tsFileManager.getTsFileList(false, startTime, endTime));
    tsFileResources.stream().filter(TsFileResource::isClosed).forEach(sealedResource::add);
    tsFileResources.stream()
        .filter(resource -> !resource.isClosed())
        .forEach(unsealedResource::add);
  }

  public void deleteByDevice(final MeasurementPath pattern, final DeleteDataNode node)
      throws IOException {
    if (SettleService.getINSTANCE().getFilesToBeSettledCount().get() != 0) {
      throw new IOException(
          "Delete failed. " + "Please do not delete until the old files settled.");
    }
    final long startTime = node.getDeleteStartTime();
    final long endTime = node.getDeleteEndTime();
    final long searchIndex = node.getSearchIndex();
    // TODO: how to avoid partial deletion?
    // FIXME: notice that if we may remove a SGProcessor out of memory, we need to close all opened
    // mod files in mergingModification, sequenceFileList, and unsequenceFileList
    writeLock("delete");

    boolean hasReleasedLock = false;

    try {
      if (deleted) {
        return;
      }
      TreeDeviceSchemaCacheManager.getInstance().invalidateLastCache(pattern);
      // write log to impacted working TsFileProcessors
      List<WALFlushListener> walListeners =
          logDeletionInWAL(startTime, endTime, searchIndex, pattern);

      for (WALFlushListener walFlushListener : walListeners) {
        if (walFlushListener.waitForResult() == WALFlushListener.Status.FAILURE) {
          logger.error("Fail to log delete to wal.", walFlushListener.getCause());
          throw walFlushListener.getCause();
        }
      }

      ModEntry deletion = new TreeDeletionEntry(pattern, startTime, endTime);

      List<TsFileResource> sealedTsFileResource = new ArrayList<>();
      List<TsFileResource> unsealedTsFileResource = new ArrayList<>();
      getTwoKindsOfTsFiles(sealedTsFileResource, unsealedTsFileResource, startTime, endTime);
      // deviceMatchInfo is used for filter the matched deviceId in TsFileResource
      // deviceMatchInfo contains the DeviceId means this device matched the pattern
      deleteDataInUnsealedFiles(unsealedTsFileResource, deletion, sealedTsFileResource);
      // capture deleteDataNode and wait it to be persisted to DAL.
      DeletionResource deletionResource =
          PipeInsertionDataNodeListener.getInstance().listenToDeleteData(dataRegionIdString, node);
      // just get result. We have already waited for result in `listenToDeleteData`
      if (deletionResource != null && deletionResource.waitForResult() == Status.FAILURE) {
        throw deletionResource.getCause();
      }
      writeUnlock();
      hasReleasedLock = true;

      deleteDataInSealedFiles(sealedTsFileResource, deletion);
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      if (!hasReleasedLock) {
        writeUnlock();
      }
    }
  }

  public void deleteByTable(RelationalDeleteDataNode node) throws IOException {
    if (node.getDatabaseName() != null && !node.getDatabaseName().equals(databaseName)) {
      // not targeted on this database, return
      return;
    }

    if (SettleService.getINSTANCE().getFilesToBeSettledCount().get() != 0) {
      throw new IOException(
          "Delete failed. " + "Please do not delete until the old files settled.");
    }
    List<TableDeletionEntry> modEntries = node.getModEntries();

    logger.info("[Deletion] Executing table deletion {}", node);

    writeLock("delete");
    boolean hasReleasedLock = false;
    try {
      if (deleted) {
        return;
      }
      String tableName = modEntries.get(0).getTableName();
      TableDeviceSchemaCache.getInstance().invalidateLastCache(getDatabaseName(), tableName);
      List<WALFlushListener> walListeners = logDeletionInWAL(node);

      for (WALFlushListener walFlushListener : walListeners) {
        if (walFlushListener.waitForResult() == WALFlushListener.Status.FAILURE) {
          logger.error("Fail to log delete to wal.", walFlushListener.getCause());
          throw walFlushListener.getCause();
        }
      }

      List<File> objectTableDirs =
          TierManager.getInstance().getAllMatchedObjectDirs(dataRegionIdString, tableName);
      if (!objectTableDirs.isEmpty()) {
        boolean droppingTable = false;
        for (TableDeletionEntry entry : modEntries) {
          if (entry.isDroppingTable()) {
            AtomicLong totalSize = new AtomicLong(0);
            AtomicInteger count = new AtomicInteger(0);
            for (File objectTableDir : objectTableDirs) {
              droppingTable = true;
              try (Stream<Path> paths = Files.walk(objectTableDir.toPath())) {
                paths
                    .filter(Files::isRegularFile)
                    .filter(
                        path -> {
                          String name = path.getFileName().toString();
                          return name.endsWith(".bin");
                        })
                    .forEach(
                        path -> {
                          count.incrementAndGet();
                          totalSize.addAndGet(path.toFile().length());
                        });
              } catch (IOException e) {
                logger.error("Failed to check Object Files: {}", e.getMessage());
              }
              FileUtils.deleteQuietly(objectTableDir);
            }
            FileMetrics.getInstance().decreaseObjectFileNum(count.get());
            FileMetrics.getInstance().decreaseObjectFileSize(totalSize.get());
          }
        }
        if (!droppingTable) {
          deleteObjectFiles(objectTableDirs, modEntries);
        }
      }

      List<List<TsFileResource>> sealedTsFileResourceLists = new ArrayList<>(modEntries.size());
      for (TableDeletionEntry modEntry : modEntries) {
        List<TsFileResource> sealedTsFileResource = new ArrayList<>();
        List<TsFileResource> unsealedTsFileResource = new ArrayList<>();
        getTwoKindsOfTsFiles(
            sealedTsFileResource,
            unsealedTsFileResource,
            modEntry.getStartTime(),
            modEntry.getEndTime());
        logger.debug("[Deletion] unsealed files for {}: {}", modEntry, unsealedTsFileResource);
        deleteDataInUnsealedFiles(unsealedTsFileResource, modEntry, sealedTsFileResource);
        logger.debug("[Deletion] sealed files for {}: {}", modEntry, sealedTsFileResource);
        sealedTsFileResourceLists.add(sealedTsFileResource);
      }

      // capture deleteDataNode and wait it to be persisted to DAL.
      DeletionResource deletionResource =
          PipeInsertionDataNodeListener.getInstance().listenToDeleteData(dataRegionIdString, node);
      // just get result. We have already waited for result in `listenToDeleteData`
      if (deletionResource != null && deletionResource.waitForResult() == Status.FAILURE) {
        throw deletionResource.getCause();
      }

      writeUnlock();
      hasReleasedLock = true;

      for (int i = 0; i < modEntries.size(); i++) {
        deleteDataInSealedFiles(sealedTsFileResourceLists.get(i), modEntries.get(i));
      }
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      if (!hasReleasedLock) {
        writeUnlock();
      }
    }
  }

  public void deleteDataDirectly(MeasurementPath pathToDelete, DeleteDataNode node)
      throws IOException {
    final long startTime = node.getDeleteStartTime();
    final long endTime = node.getDeleteEndTime();
    final long searchIndex = node.getSearchIndex();
    logger.info(
        "{} will delete data files directly for deleting data between {} and {}",
        databaseName + "-" + dataRegionIdString,
        startTime,
        endTime);

    writeLock("deleteDataDirect");
    boolean releasedLock = false;

    try {
      if (deleted) {
        return;
      }
      TreeDeviceSchemaCacheManager.getInstance().invalidateDatabaseLastCache(getDatabaseName());
      // write log to impacted working TsFileProcessors
      List<WALFlushListener> walListeners =
          logDeletionInWAL(startTime, endTime, searchIndex, pathToDelete);

      for (WALFlushListener walFlushListener : walListeners) {
        if (walFlushListener.waitForResult() == WALFlushListener.Status.FAILURE) {
          logger.error("Fail to log delete to wal.", walFlushListener.getCause());
          throw walFlushListener.getCause();
        }
      }
      TreeDeletionEntry deletion = new TreeDeletionEntry(pathToDelete, startTime, endTime);
      List<TsFileResource> sealedTsFileResource = new ArrayList<>();
      List<TsFileResource> unsealedTsFileResource = new ArrayList<>();
      getTwoKindsOfTsFiles(sealedTsFileResource, unsealedTsFileResource, startTime, endTime);
      deleteDataDirectlyInFile(unsealedTsFileResource, deletion);
      // capture deleteDataNode and wait it to be persisted to DAL.
      DeletionResource deletionResource =
          PipeInsertionDataNodeListener.getInstance().listenToDeleteData(dataRegionIdString, node);
      // just get result. We have already waited for result in `listenToDeleteData`
      if (deletionResource != null && deletionResource.waitForResult() == Status.FAILURE) {
        throw deletionResource.getCause();
      }
      writeUnlock();
      releasedLock = true;
      deleteDataDirectlyInFile(sealedTsFileResource, deletion);
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      if (!releasedLock) {
        writeUnlock();
      }
    }
  }

  private List<WALFlushListener> logDeletionInWAL(RelationalDeleteDataNode deleteDataNode) {
    if (config.getWalMode() == WALMode.DISABLE) {
      return Collections.emptyList();
    }
    List<WALFlushListener> walFlushListeners = new ArrayList<>();
    Set<TsFileProcessor> involvedProcessors = new HashSet<>();

    for (TableDeletionEntry modEntry : deleteDataNode.getModEntries()) {
      long startTime = modEntry.getStartTime();
      long endTime = modEntry.getEndTime();
      for (Map.Entry<Long, TsFileProcessor> entry : workSequenceTsFileProcessors.entrySet()) {
        if (TimePartitionUtils.satisfyPartitionId(startTime, endTime, entry.getKey())) {
          involvedProcessors.add(entry.getValue());
        }
      }
      for (Map.Entry<Long, TsFileProcessor> entry : workUnsequenceTsFileProcessors.entrySet()) {
        if (TimePartitionUtils.satisfyPartitionId(startTime, endTime, entry.getKey())) {
          involvedProcessors.add(entry.getValue());
        }
      }
    }

    for (TsFileProcessor involvedProcessor : involvedProcessors) {
      WALFlushListener walFlushListener = involvedProcessor.logDeleteDataNodeInWAL(deleteDataNode);
      walFlushListeners.add(walFlushListener);
    }

    // Some time the deletion operation doesn't have any related tsfile processor or memtable,
    // but it's still necessary to write to the WAL, so that iot consensus can synchronize the
    // delete
    // operation to other nodes.
    if (walFlushListeners.isEmpty()) {
      logger.info("Writing no-file-related deletion to WAL {}", deleteDataNode);
      getWALNode()
          .ifPresent(
              walNode ->
                  walFlushListeners.add(
                      walNode.log(TsFileProcessor.MEMTABLE_NOT_EXIST, deleteDataNode)));
    }
    return walFlushListeners;
  }

  private List<WALFlushListener> logDeletionInWAL(
      long startTime, long endTime, long searchIndex, MeasurementPath path) {
    if (config.getWalMode() == WALMode.DISABLE) {
      return Collections.emptyList();
    }
    List<WALFlushListener> walFlushListeners = new ArrayList<>();
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
    // Some time the deletion operation doesn't have any related tsfile processor or memtable,
    // but it's still necessary to write to the WAL, so that iot consensus can synchronize the
    // delete
    // operation to other nodes.
    if (walFlushListeners.isEmpty()) {
      getWALNode()
          .ifPresent(
              walNode ->
                  walFlushListeners.add(
                      walNode.log(TsFileProcessor.MEMTABLE_NOT_EXIST, deleteDataNode)));
    }
    return walFlushListeners;
  }

  private void deleteObjectFiles(List<File> matchedObjectDirs, List<TableDeletionEntry> modEntries)
      throws IOException {
    for (File matchedObjectDir : matchedObjectDirs) {
      try (Stream<Path> paths =
          Files.find(
              matchedObjectDir.toPath(),
              Integer.MAX_VALUE,
              (path, attrs) ->
                  attrs.isRegularFile()
                      && (path.getFileName().toString().endsWith(".bin")
                          || path.getFileName().toString().endsWith(".tmp")))) {
        paths.forEach(
            path -> {
              Path relativePath = matchedObjectDir.getParentFile().toPath().relativize(path);
              String[] ideviceIdSegments = new String[relativePath.getNameCount() - 2];
              for (int i = 0; i < ideviceIdSegments.length; i++) {
                ideviceIdSegments[i] =
                    CommonDescriptor.getInstance().getConfig().isRestrictObjectLimit()
                        ? relativePath.getName(i).toString()
                        : new String(
                            BaseEncoding.base32()
                                .omitPadding()
                                .decode(relativePath.getName(i).toString()),
                            StandardCharsets.UTF_8);
              }
              IDeviceID iDeviceID = Factory.DEFAULT_FACTORY.create(ideviceIdSegments);
              String measurementId =
                  CommonDescriptor.getInstance().getConfig().isRestrictObjectLimit()
                      ? relativePath.getName(relativePath.getNameCount() - 2).toString()
                      : new String(
                          BaseEncoding.base32()
                              .omitPadding()
                              .decode(
                                  relativePath.getName(relativePath.getNameCount() - 2).toString()),
                          StandardCharsets.UTF_8);
              String fileName = path.getFileName().toString();
              long timestamp = Long.parseLong(fileName.substring(0, fileName.indexOf('.')));
              logger.debug(
                  "timestamp {}, measurementId {}, ideviceId {}",
                  timestamp,
                  measurementId,
                  iDeviceID);
              for (TableDeletionEntry modEntry : modEntries) {
                if (modEntry.affects(iDeviceID, timestamp, timestamp)
                    && modEntry.affects(measurementId)) {
                  ObjectTypeUtils.deleteObjectPath(path.toFile());
                }
              }
            });
      }
    }
  }

  /**
   * For IoTConsensus sync. See <a href="https://github.com/apache/iotdb/pull/12955">github pull
   * request</a> for details.
   */
  public void insertSeparatorToWAL() {
    writeLock("insertSeparatorToWAL");
    try {
      if (deleted) {
        return;
      }
      getWALNode()
          .ifPresent(
              walNode ->
                  walNode.log(
                      TsFileProcessor.MEMTABLE_NOT_EXIST,
                      new ContinuousSameSearchIndexSeparatorNode()));
    } finally {
      writeUnlock();
    }
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  private boolean canSkipDelete(TsFileResource tsFileResource, ModEntry deletion) {
    long fileStartTime = tsFileResource.getTimeIndex().getMinStartTime();
    long fileEndTime =
        tsFileResource.isClosed() || !tsFileResource.isSeq()
            ? tsFileResource.getTimeIndex().getMaxEndTime()
            : Long.MAX_VALUE;

    if (!ModificationUtils.overlap(
        deletion.getStartTime(), deletion.getEndTime(), fileStartTime, fileEndTime)) {
      logger.debug(
          "[Deletion] {} skipped {}, file time [{}, {}]",
          deletion,
          tsFileResource,
          fileStartTime,
          fileEndTime);
      return true;
    }
    ITimeIndex timeIndex = tsFileResource.getTimeIndex();
    if (timeIndex.getTimeIndexType() == ITimeIndex.FILE_TIME_INDEX_TYPE) {
      return false;
    }

    for (IDeviceID device : tsFileResource.getDevices()) {
      // we are iterating the time index so the times are definitely present
      long startTime = tsFileResource.getTimeIndex().getStartTime(device).get();
      long endTime =
          tsFileResource.isClosed()
              ? tsFileResource.getTimeIndex().getEndTime(device).get()
              : Long.MAX_VALUE;
      if (deletion.affects(device, startTime, endTime)) {
        return false;
      }
    }
    logger.debug("[Deletion] {} skipped {}, file time {}", deletion, tsFileResource, timeIndex);
    return true;
  }

  // suppress warn of Throwable catch
  @SuppressWarnings("java:S1181")
  private void deleteDataInUnsealedFiles(
      Collection<TsFileResource> unsealedTsFiles,
      ModEntry deletion,
      Collection<TsFileResource> sealedTsFiles) {
    for (TsFileResource tsFileResource : unsealedTsFiles) {
      if (canSkipDelete(tsFileResource, deletion)) {
        continue;
      }

      if (tsFileResource.isClosed()) {
        sealedTsFiles.add(tsFileResource);
      } else {
        TsFileProcessor tsFileProcessor = tsFileResource.getProcessor();
        if (tsFileProcessor == null) {
          sealedTsFiles.add(tsFileResource);
        } else {
          tsFileProcessor.writeLock();
          if (tsFileResource.isClosed()) {
            sealedTsFiles.add(tsFileResource);
            tsFileProcessor.writeUnlock();
          } else {
            try {
              if (!tsFileProcessor.deleteDataInMemory(deletion)) {
                sealedTsFiles.add(tsFileResource);
              } // else do nothing
            } finally {
              tsFileProcessor.writeUnlock();
            }
          }
        }
      }
    }
  }

  private void deleteDataInSealedFiles(Collection<TsFileResource> sealedTsFiles, ModEntry deletion)
      throws IOException {
    Set<ModificationFile> involvedModificationFiles = new HashSet<>();
    List<TsFileResource> deletedByMods = new ArrayList<>();
    List<TsFileResource> deletedByFiles = new ArrayList<>();
    boolean isDropMeasurementExist = false;
    IDPredicate.IDPredicateType idPredicateType = null;

    if (deletion instanceof TableDeletionEntry) {
      TableDeletionEntry tableDeletionEntry = (TableDeletionEntry) deletion;
      isDropMeasurementExist = !tableDeletionEntry.getPredicate().getMeasurementNames().isEmpty();
      idPredicateType = tableDeletionEntry.getPredicate().getIdPredicateType();
    }

    for (TsFileResource sealedTsFile : sealedTsFiles) {
      if (canSkipDelete(sealedTsFile, deletion)) {
        continue;
      }

      // the tsfile may not be closed here, it should not be added in deletedByFiles
      if (!sealedTsFile.isClosed()) {
        deletedByMods.add(sealedTsFile);
        continue;
      }

      ITimeIndex timeIndex = sealedTsFile.getTimeIndex();

      if ((timeIndex instanceof ArrayDeviceTimeIndex)
          && (deletion.getType() == ModEntry.ModType.TABLE_DELETION)) {
        ArrayDeviceTimeIndex deviceTimeIndex = (ArrayDeviceTimeIndex) timeIndex;
        Set<IDeviceID> devicesInFile = deviceTimeIndex.getDevices();
        boolean onlyOneTable = false;

        if (deletion instanceof TableDeletionEntry) {
          TableDeletionEntry tableDeletionEntry = (TableDeletionEntry) deletion;
          String tableName = tableDeletionEntry.getTableName();
          long matchSize =
              devicesInFile.stream()
                  .filter(
                      device -> {
                        if (logger.isDebugEnabled()) {
                          logger.debug(
                              "device is {}, deviceTable is {}, tableDeletionEntry.getPredicate().matches(device) is {}",
                              device,
                              device.getTableName(),
                              tableDeletionEntry.getPredicate().matches(device));
                        }
                        return tableName.equals(device.getTableName())
                            && tableDeletionEntry.getPredicate().matches(device);
                      })
                  .count();
          onlyOneTable = matchSize == devicesInFile.size();
          if (logger.isDebugEnabled()) {
            logger.debug(
                "tableName is {}, matchSize is {}, onlyOneTable is {}",
                tableName,
                matchSize,
                onlyOneTable);
          }
        }

        if (onlyOneTable) {
          int matchSize = 0;
          for (IDeviceID device : devicesInFile) {
            Optional<Long> optStart = deviceTimeIndex.getStartTime(device);
            Optional<Long> optEnd = deviceTimeIndex.getEndTime(device);
            if (!optStart.isPresent() || !optEnd.isPresent()) {
              continue;
            }

            long fileStartTime = optStart.get();
            long fileEndTime = optEnd.get();

            if (logger.isDebugEnabled()) {
              logger.debug(
                  "tableName is {}, device is {}, deletionStartTime is {}, deletionEndTime is {}, fileStartTime is {}, fileEndTime is {}",
                  device.getTableName(),
                  device,
                  deletion.getStartTime(),
                  deletion.getEndTime(),
                  fileStartTime,
                  fileEndTime);
            }
            if (isFileFullyMatchedByTime(deletion, fileStartTime, fileEndTime)
                && idPredicateType.equals(IDPredicate.IDPredicateType.NOP)
                && !isDropMeasurementExist) {
              ++matchSize;
            } else {
              deletedByMods.add(sealedTsFile);
              break;
            }
          }
          if (matchSize == devicesInFile.size()) {
            deletedByFiles.add(sealedTsFile);
          }

          if (logger.isDebugEnabled()) {
            logger.debug("expect is {}, actual is {}", devicesInFile.size(), matchSize);
            for (TsFileResource tsFileResource : deletedByFiles) {
              logger.debug(
                  "delete tsFileResource is {}", tsFileResource.getTsFile().getAbsolutePath());
            }
          }
        } else {
          involvedModificationFiles.add(sealedTsFile.getModFileForWrite());
        }
      } else {
        involvedModificationFiles.add(sealedTsFile.getModFileForWrite());
      }
    }

    for (TsFileResource tsFileResource : deletedByMods) {
      if (tsFileResource.isClosed()
          || !tsFileResource.getProcessor().deleteDataInMemory(deletion)) {
        involvedModificationFiles.add(tsFileResource.getModFileForWrite());
      } // else do nothing
    }

    if (!deletedByFiles.isEmpty()) {
      deleteTsFileCompletely(deletedByFiles);
      if (logger.isDebugEnabled()) {
        logger.debug(
            "deleteTsFileCompletely execute successful, all tsfile are deleted successfully");
      }
    }

    if (involvedModificationFiles.isEmpty()) {
      logger.info("[Deletion] Deletion {} does not involve any file", deletion);
      return;
    }

    List<Exception> exceptions =
        involvedModificationFiles.parallelStream()
            .map(
                modFile -> {
                  try {
                    modFile.write(deletion);
                    modFile.close();
                  } catch (Exception e) {
                    return e;
                  }
                  return null;
                })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    if (!exceptions.isEmpty()) {
      if (exceptions.size() == 1) {
        throw new IOException(exceptions.get(0));
      } else {
        exceptions.forEach(e -> logger.error("Fail to write modEntry {} to files", deletion, e));
        throw new IOException(
            "Multiple errors occurred while writing mod files, see logs for details.");
      }
    }
    logger.info(
        "[Deletion] Deletion {} is written into {} mod files",
        deletion,
        involvedModificationFiles.size());
  }

  private boolean isFileFullyMatchedByTime(
      ModEntry deletion, long fileStartTime, long fileEndTime) {
    return fileStartTime >= deletion.getStartTime() && fileEndTime <= deletion.getEndTime();
  }

  /** Delete completely TsFile and related supporting files */
  private void deleteTsFileCompletely(List<TsFileResource> tsfileResourceList) {
    for (TsFileResource tsFileResource : tsfileResourceList) {
      tsFileManager.remove(tsFileResource, tsFileResource.isSeq());
      tsFileResource.writeLock();
      try {
        FileMetrics.getInstance()
            .deleteTsFile(tsFileResource.isSeq(), Collections.singletonList(tsFileResource));
        tsFileResource.remove();
        logger.info("Remove tsfile {} directly when delete data", tsFileResource.getTsFilePath());
      } finally {
        tsFileResource.writeUnlock();
      }
    }
  }

  private void deleteDataDirectlyInFile(List<TsFileResource> tsfileResourceList, ModEntry modEntry)
      throws IOException {
    List<TsFileResource> deletedByMods = new ArrayList<>();
    List<TsFileResource> deletedByFiles = new ArrayList<>();
    separateTsFileToDelete(modEntry, tsfileResourceList, deletedByMods, deletedByFiles);

    // can be deleted by mods.
    Set<ModificationFile> involvedModificationFiles = new HashSet<>();
    for (TsFileResource tsFileResource : deletedByMods) {
      if (tsFileResource.isClosed()
          || !tsFileResource.getProcessor().deleteDataInMemory(modEntry)) {
        involvedModificationFiles.add(tsFileResource.getModFileForWrite());
      } // else do nothing
    }

    for (ModificationFile involvedModificationFile : involvedModificationFiles) {
      // delete data in sealed file
      involvedModificationFile.write(modEntry);
      // The file size may be smaller than the original file, so the increment here may be
      // negative
      involvedModificationFile.close();
      logger.debug(
          "[Deletion] Deletion {} written into mods file:{}.", modEntry, involvedModificationFile);
    }

    // can be deleted by files
    for (TsFileResource tsFileResource : deletedByFiles) {
      tsFileManager.remove(tsFileResource, tsFileResource.isSeq());
      tsFileResource.writeLock();
      try {
        FileMetrics.getInstance()
            .deleteTsFile(tsFileResource.isSeq(), Collections.singletonList(tsFileResource));
        tsFileResource.remove();
        logger.info("Remove tsfile {} directly when delete data", tsFileResource.getTsFilePath());
      } finally {
        tsFileResource.writeUnlock();
      }
    }
  }

  private void separateTsFileToDelete(
      ModEntry modEntry,
      List<TsFileResource> tsFileResourceList,
      List<TsFileResource> deletedByMods,
      List<TsFileResource> deletedByFiles) {
    long startTime = modEntry.getStartTime();
    long endTime = modEntry.getEndTime();
    for (TsFileResource file : tsFileResourceList) {
      long fileStartTime = file.getTimeIndex().getMinStartTime();
      long fileEndTime = file.getTimeIndex().getMaxEndTime();

      if (!canSkipDelete(file, modEntry)) {
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

  private void flushCallback(
      TsFileProcessor processor, Map<IDeviceID, Long> updateMap, long systemFlushTime) {
    if (config.isEnableSeparateData()
        && CommonDescriptor.getInstance().getConfig().isLastCacheEnable()) {
      // Update both partitionLastFlushTime and globalLastFlushTime
      lastFlushTimeMap.updateLatestFlushTime(processor.getTimeRangeId(), updateMap);
    } else {
      // isEnableSeparateData is true and isLastCacheEnable is false, then update
      // partitionLastFlushTime only
      lastFlushTimeMap.updateMultiDeviceFlushedTime(processor.getTimeRangeId(), updateMap);
    }

    if (config.isEnableSeparateData()) {
      TimePartitionManager.getInstance()
          .updateAfterFlushing(
              new DataRegionId(Integer.parseInt(dataRegionIdString)),
              processor.getTimeRangeId(),
              systemFlushTime,
              lastFlushTimeMap.getMemSize(processor.getTimeRangeId()),
              workSequenceTsFileProcessors.get(processor.getTimeRangeId()) != null);
    }
  }

  /** Put the memtable back to the MemTablePool and make the metadata in writer visible */
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
    tsFileProcessor.writeLock();
    try {
      tsFileProcessor.closeWithoutSettingResourceStatus();
      if (isEmptyFile) {
        tsFileProcessor.getTsFileResource().remove();
      } else if (isValidateTsFileFailed) {
        String tsFilePath = tsFileProcessor.getTsFileResource().getTsFile().getAbsolutePath();
        renameAndHandleError(tsFilePath, tsFilePath + BROKEN_SUFFIX);
        renameAndHandleError(
            tsFilePath + RESOURCE_SUFFIX, tsFilePath + RESOURCE_SUFFIX + BROKEN_SUFFIX);
      } else {
        tsFileProcessor.getTsFileResource().setStatus(TsFileResourceStatus.NORMAL);
        tsFileResourceManager.registerSealedTsFileResource(tsFileProcessor.getTsFileResource());
      }
    } finally {
      tsFileProcessor.writeUnlock();
    }
    if (isEmptyFile || isValidateTsFileFailed) {
      tsFileManager.remove(tsFileProcessor.getTsFileResource(), tsFileProcessor.isSequence());
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

  public int executeCompaction() throws InterruptedException {
    if (!isCompactionSelecting.compareAndSet(false, true)) {
      return 0;
    }
    CompactionScheduleContext context =
        new CompactionScheduleContext(
            EncryptDBUtils.getFirstEncryptParamFromDatabase(databaseName));
    try {
      List<Long> timePartitions = new ArrayList<>(tsFileManager.getTimePartitions());
      // Sort the time partition from largest to smallest
      timePartitions.sort(Comparator.reverseOrder());

      // schedule insert compaction
      int[] submitCountOfTimePartitions = executeInsertionCompaction(timePartitions, context);

      // schedule the other compactions
      for (int i = 0; i < timePartitions.size(); i++) {
        boolean skipOtherCompactionSchedule =
            submitCountOfTimePartitions[i] > 0
                && !config
                    .getDataRegionConsensusProtocolClass()
                    .equals(ConsensusFactory.IOT_CONSENSUS_V2);
        if (skipOtherCompactionSchedule) {
          continue;
        }
        long timePartition = timePartitions.get(i);
        CompactionScheduler.sharedLockCompactionSelection();
        try {
          CompactionScheduler.scheduleCompaction(tsFileManager, timePartition, context);
        } finally {
          context.clearTimePartitionDeviceInfoCache();
          CompactionScheduler.sharedUnlockCompactionSelection();
        }
      }
      if (context.hasSubmitTask()) {
        CompactionMetrics.getInstance().updateCompactionTaskSelectionNum(context);
      }
    } catch (InterruptedException e) {
      throw e;
    } catch (Throwable e) {
      logger.error("Meet error in compaction schedule.", e);
    } finally {
      isCompactionSelecting.set(false);
    }
    return context.getSubmitCompactionTaskNum();
  }

  /** Schedule settle compaction for ttl check. */
  public int executeTTLCheck() throws InterruptedException {
    while (!isCompactionSelecting.compareAndSet(false, true)) {
      // wait until success
      Thread.sleep(500);
    }
    int trySubmitCount = 0;
    try {
      if (skipCurrentTTLAndModificationCheck()) {
        return 0;
      }
      logger.info(
          "[TTL] {}-{} Start ttl and modification checking.", databaseName, dataRegionIdString);
      CompactionScheduleContext context =
          new CompactionScheduleContext(
              EncryptDBUtils.getFirstEncryptParamFromDatabase(databaseName));
      List<Long> timePartitions = new ArrayList<>(tsFileManager.getTimePartitions());
      // Sort the time partition from smallest to largest
      Collections.sort(timePartitions);

      for (long timePartition : timePartitions) {
        CompactionScheduler.sharedLockCompactionSelection();
        try {
          trySubmitCount +=
              CompactionScheduler.tryToSubmitSettleCompactionTask(
                  tsFileManager, timePartition, context, true);
        } finally {
          context.clearTimePartitionDeviceInfoCache();
          CompactionScheduler.sharedUnlockCompactionSelection();
        }
      }
      if (context.hasSubmitTask()) {
        CompactionMetrics.getInstance().updateCompactionTaskSelectionNum(context);
      }
      logger.info(
          "[TTL] {}-{} Totally select {} all-outdated files and {} partial-outdated files.",
          databaseName,
          dataRegionIdString,
          context.getFullyDirtyFileNum(),
          context.getPartiallyDirtyFileNum());
    } catch (InterruptedException e) {
      throw e;
    } catch (Throwable e) {
      logger.error("Meet error in ttl check.", e);
    } finally {
      isCompactionSelecting.set(false);
    }
    return trySubmitCount;
  }

  public void executeTTLCheckForObjectFiles() throws InterruptedException {
    long startTime = System.currentTimeMillis();
    List<String> allObjectDirs = TierManager.getInstance().getAllObjectFileFolders();
    for (String objectDir : allObjectDirs) {
      File regionObjectDir = new File(objectDir, dataRegionIdString);
      if (!regionObjectDir.isDirectory()) {
        continue;
      }
      CompactionUtils.executeTTLCheckObjectFilesForTableModel(regionObjectDir, databaseName);
    }
    CompactionMetrics.getInstance()
        .updateTTLCheckForObjectFileCost(System.currentTimeMillis() - startTime);
  }

  private boolean skipCurrentTTLAndModificationCheck() {
    if (this.databaseName.equals(InformationSchema.INFORMATION_DATABASE)) {
      return true;
    }
    for (Long timePartition : getTimePartitions()) {
      List<TsFileResource> seqFiles = tsFileManager.getTsFileListSnapshot(timePartition, true);
      List<TsFileResource> unseqFiles = tsFileManager.getTsFileListSnapshot(timePartition, false);
      boolean modFileExists =
          Stream.concat(seqFiles.stream(), unseqFiles.stream())
              .anyMatch(TsFileResource::anyModFileExists);
      if (modFileExists) {
        return false;
      }
    }
    boolean isTableModel = isTableModelDatabase(this.databaseName);
    if (isTableModel) {
      try (final ConfigNodeClient configNodeClient =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        TShowTableResp resp = configNodeClient.showTables(databaseName, false);
        for (TTableInfo tTableInfo : resp.getTableInfoList()) {
          String ttl = tTableInfo.getTTL();
          if (ttl == null || ttl.equals(IoTDBConstant.TTL_INFINITE)) {
            continue;
          }
          long ttlValue = Long.parseLong(ttl);
          if (ttlValue < 0 || ttlValue == Long.MAX_VALUE) {
            continue;
          }
          return false;
        }
        return true;
      } catch (Exception ignored) {
        return false;
      }
    } else {
      try {
        return !DataNodeTTLCache.getInstance().dataInDatabaseMayHaveTTL(databaseName);
      } catch (Exception ignored) {
        return false;
      }
    }
  }

  protected int[] executeInsertionCompaction(
      List<Long> timePartitions, CompactionScheduleContext context) throws InterruptedException {
    int[] trySubmitCountOfTimePartitions = new int[timePartitions.size()];
    CompactionScheduler.sharedLockCompactionSelection();
    try {
      while (true) {
        int currentSubmitCount = 0;
        for (int i = 0; i < timePartitions.size(); i++) {
          long timePartition = timePartitions.get(i);
          int selectedTaskNum =
              CompactionScheduler.scheduleInsertionCompaction(
                  tsFileManager, timePartition, context);
          currentSubmitCount += selectedTaskNum;
          trySubmitCountOfTimePartitions[i] += selectedTaskNum;
          context.clearTimePartitionDeviceInfoCache();
        }
        if (currentSubmitCount <= 0) {
          break;
        }
        context.incrementSubmitTaskNum(CompactionTaskType.INSERTION, currentSubmitCount);
      }
    } catch (InterruptedException e) {
      throw e;
    } catch (Throwable e) {
      logger.error("Meet error in insertion compaction schedule.", e);
    } finally {
      context.clearTimePartitionDeviceInfoCache();
      CompactionScheduler.sharedUnlockCompactionSelection();
    }
    return trySubmitCountOfTimePartitions;
  }

  /**
   * After finishing settling tsfile, we need to do 2 things : (1) move the new tsfile to the
   * correct folder, including deleting its old mods file (2) update the relevant data of this old
   * tsFile in memory ,eg: TsFileSequenceReader, {@link #tsFileManager}, cache, etc.
   */
  private void settleTsFileCallBack(
      TsFileResource oldTsFileResource, List<TsFileResource> newTsFileResources)
      throws WriteProcessException {
    oldTsFileResource.readUnlock();
    oldTsFileResource.writeLock();
    try {
      TsFileAndModSettleTool.moveNewTsFile(oldTsFileResource, newTsFileResources);
      if (!TsFileAndModSettleTool.getInstance().recoverSettleFileMap.isEmpty()) {
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
      FileReaderManager.getInstance().closeFileAndRemoveReader(oldTsFileResource.getTsFileID());
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

  public static Optional<String> getNonSystemDatabaseName(String databaseName) {
    if (Objects.isNull(databaseName)) {
      return Optional.empty();
    }
    if (databaseName.startsWith(SchemaConstant.SYSTEM_DATABASE)) {
      return Optional.empty();
    }
    if (databaseName.startsWith(SchemaConstant.AUDIT_DATABASE)) {
      return Optional.empty();
    }
    int lastIndex = databaseName.lastIndexOf("-");
    if (lastIndex == -1) {
      lastIndex = databaseName.length();
    }
    return Optional.of(databaseName.substring(0, lastIndex));
  }

  public Optional<String> getNonSystemDatabaseName() {
    return getNonSystemDatabaseName(databaseName);
  }

  /** Merge file under this database processor */
  public int compact() {
    writeLock("merge");
    CompactionScheduler.exclusiveLockCompactionSelection();
    try {
      return executeCompaction();
    } catch (InterruptedException ignored) {
      Thread.currentThread().interrupt();
      return 0;
    } finally {
      CompactionScheduler.exclusiveUnlockCompactionSelection();
      writeUnlock();
    }
  }

  public void writeObject(ObjectNode objectNode) throws Exception {
    writeLock("writeObject");
    try {
      String relativeTmpPathString = objectNode.getFilePathString() + ".tmp";
      String objectFileDir = null;
      File objectTmpFile = null;
      for (String objectDir : TierManager.getInstance().getAllObjectFileFolders()) {
        File tmpFile = FSFactoryProducer.getFSFactory().getFile(objectDir, relativeTmpPathString);
        if (tmpFile.exists()) {
          objectFileDir = objectDir;
          objectTmpFile = tmpFile;
          break;
        }
      }
      if (objectTmpFile == null) {
        objectFileDir = TierManager.getInstance().getNextFolderForObjectFile();
        objectTmpFile =
            FSFactoryProducer.getFSFactory().getFile(objectFileDir, relativeTmpPathString);
      }
      try (ObjectWriter writer = new ObjectWriter(objectTmpFile)) {
        writer.write(
            objectNode.isGeneratedByRemoteConsensusLeader(),
            objectNode.getOffset(),
            objectNode.getContent());
      }
      if (objectNode.isEOF()) {
        File objectFile =
            FSFactoryProducer.getFSFactory().getFile(objectFileDir, objectNode.getFilePathString());
        if (objectFile.exists()) {
          String relativeBackPathString = objectNode.getFilePathString() + ".back";
          File objectBackFile =
              FSFactoryProducer.getFSFactory().getFile(objectFileDir, relativeBackPathString);
          Files.move(
              objectFile.toPath(), objectBackFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
          Files.move(
              objectTmpFile.toPath(), objectFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
          FileMetrics.getInstance().decreaseObjectFileNum(1);
          FileMetrics.getInstance().decreaseObjectFileSize(objectBackFile.length());
          Files.delete(objectBackFile.toPath());
        } else {
          Files.move(
              objectTmpFile.toPath(), objectFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
        FileMetrics.getInstance().increaseObjectFileNum(1);
        FileMetrics.getInstance().increaseObjectFileSize(objectFile.length());
      }
      getWALNode()
          .ifPresent(walNode -> walNode.log(TsFileProcessor.MEMTABLE_NOT_EXIST, objectNode));
    } finally {
      writeUnlock();
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
      final TsFileResource newTsFileResource,
      final boolean deleteOriginFile,
      final boolean isGeneratedByPipe,
      final boolean isFromConsensus)
      throws LoadFileException {
    if (DataRegionConsensusImpl.getInstance() instanceof IoTConsensus) {
      final IoTConsensusServerImpl impl =
          ((IoTConsensus) DataRegionConsensusImpl.getInstance()).getImpl(dataRegionId);
      if (Objects.nonNull(impl) && !impl.isActive()) {
        throw new LoadFileException(
            String.format(
                "Peer is inactive and not ready to write request, %s, DataNode Id: %s",
                dataRegionId, IoTDBDescriptor.getInstance().getConfig().getDataNodeId()));
      }
    }

    final File tsfileToBeInserted = newTsFileResource.getTsFile().getAbsoluteFile();
    final long newFilePartitionId = newTsFileResource.getTimePartitionWithCheck();

    if (!TsFileValidator.getInstance().validateTsFile(newTsFileResource)) {
      throw new LoadFileException(
          "tsfile validate failed, " + newTsFileResource.getTsFile().getName());
    }

    TsFileLastReader lastReader = null;
    LastCacheLoadStrategy lastCacheLoadStrategy = config.getLastCacheLoadStrategy();
    if (!isFromConsensus
        && (lastCacheLoadStrategy == LastCacheLoadStrategy.UPDATE
            || lastCacheLoadStrategy == LastCacheLoadStrategy.UPDATE_NO_BLOB)
        && newTsFileResource.getLastValues() == null) {
      try {
        // init reader outside of lock to boost performance
        lastReader =
            new TsFileLastReader(
                newTsFileResource.getTsFilePath(),
                true,
                lastCacheLoadStrategy == LastCacheLoadStrategy.UPDATE_NO_BLOB);
      } catch (IOException e) {
        throw new LoadFileException(e);
      }
    }

    writeLock("loadNewTsFile");
    try {
      if (deleted) {
        logger.info(
            "Won't load TsFile {}, because region is deleted",
            tsfileToBeInserted.getAbsolutePath());
        return;
      }
      newTsFileResource.setSeq(false);
      final String newFileName =
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
          tsfileToBeInserted,
          newTsFileResource,
          newFilePartitionId,
          deleteOriginFile,
          isGeneratedByPipe);

      FileMetrics.getInstance()
          .addTsFile(
              newTsFileResource.getDatabaseName(),
              newTsFileResource.getDataRegionId(),
              newTsFileResource.getTsFile().length(),
              false,
              newTsFileResource.getTsFile().getName());

      if (config.isEnableSeparateData()) {
        final DataRegionId dataRegionId =
            new DataRegionId(Integer.parseInt(this.dataRegionIdString));
        final long timePartitionId = newTsFileResource.getTimePartition();
        if (!lastFlushTimeMap.checkAndCreateFlushedTimePartition(timePartitionId, true)) {
          TimePartitionManager.getInstance()
              .registerTimePartitionInfo(
                  new TimePartitionInfo(
                      dataRegionId,
                      timePartitionId,
                      false,
                      Long.MAX_VALUE,
                      lastFlushTimeMap.getMemSize(timePartitionId)));
        }
        updateDeviceLastFlushTime(newTsFileResource);
        TimePartitionManager.getInstance()
            .updateAfterFlushing(
                dataRegionId,
                timePartitionId,
                System.currentTimeMillis(),
                lastFlushTimeMap.getMemSize(timePartitionId),
                false);
      }

      onTsFileLoaded(newTsFileResource, isFromConsensus, lastReader);
      logger.info("TsFile {} is successfully loaded in unsequence list.", newFileName);
    } catch (final DiskSpaceInsufficientException e) {
      logger.error(
          "Failed to append the tsfile {} to database processor {} because the disk space is insufficient.",
          tsfileToBeInserted.getAbsolutePath(),
          tsfileToBeInserted.getParentFile().getName());
      throw new LoadFileException(e);
    } catch (Exception e) {
      throw new LoadFileException(e);
    } finally {
      writeUnlock();
      if (lastReader != null) {
        try {
          lastReader.close();
        } catch (Exception e) {
          logger.warn("Cannot close last reader after loading TsFile {}", newTsFileResource, e);
        }
      }
    }
  }

  private void onTsFileLoaded(
      TsFileResource newTsFileResource, boolean isFromConsensus, TsFileLastReader lastReader) {
    if (CommonDescriptor.getInstance().getConfig().isLastCacheEnable() && !isFromConsensus) {
      switch (config.getLastCacheLoadStrategy()) {
        case UPDATE:
        case UPDATE_NO_BLOB:
          updateLastCache(newTsFileResource, lastReader);
          break;
        case CLEAN_ALL:
          // The inner cache is shared by TreeDeviceSchemaCacheManager and
          // TableDeviceSchemaCacheManager,
          // so cleaning either of them is enough
          TreeDeviceSchemaCacheManager.getInstance().cleanUp();
          break;
        case CLEAN_DEVICE:
          boolean isTableModel = isTableModelDatabase(databaseName);
          ITimeIndex timeIndex = newTsFileResource.getTimeIndex();
          if (timeIndex instanceof ArrayDeviceTimeIndex) {
            ArrayDeviceTimeIndex deviceTimeIndex = (ArrayDeviceTimeIndex) timeIndex;
            deviceTimeIndex
                .getDevices()
                .forEach(
                    deviceID ->
                        TableDeviceSchemaCache.getInstance()
                            .invalidateLastCache(isTableModel ? databaseName : null, deviceID));
          } else {
            TreeDeviceSchemaCacheManager.getInstance().invalidateDatabaseLastCache(databaseName);
          }
          break;
        default:
          logger.warn(
              "Unrecognized LastCacheLoadStrategy: {}, fall back to CLEAN_ALL",
              IoTDBDescriptor.getInstance().getConfig().getLastCacheLoadStrategy());
          TreeDeviceSchemaCacheManager.getInstance().cleanUp();
          break;
      }
    }
  }

  @SuppressWarnings("java:S112")
  private void updateLastCache(TsFileResource newTsFileResource, TsFileLastReader lastReader) {
    boolean isTableModel = isTableModelDatabase(databaseName);

    Map<IDeviceID, List<Pair<String, TimeValuePair>>> lastValues =
        newTsFileResource.getLastValues();
    if (lastValues != null) {
      for (Entry<IDeviceID, List<Pair<String, TimeValuePair>>> entry : lastValues.entrySet()) {
        IDeviceID deviceID = entry.getKey();
        String[] measurements = entry.getValue().stream().map(Pair::getLeft).toArray(String[]::new);
        TimeValuePair[] timeValuePairs =
            entry.getValue().stream().map(Pair::getRight).toArray(TimeValuePair[]::new);
        if (isTableModel) {
          TableDeviceSchemaCache.getInstance()
              .updateLastCacheIfExists(databaseName, deviceID, measurements, timeValuePairs);
        } else {
          // we do not update schema here, so aligned is not relevant
          TreeDeviceSchemaCacheManager.getInstance()
              .updateLastCacheIfExists(
                  databaseName, deviceID, measurements, timeValuePairs, false, null);
        }
      }
      newTsFileResource.setLastValues(null);
      return;
    }

    if (lastReader != null) {
      while (lastReader.hasNext()) {
        Pair<IDeviceID, List<Pair<String, TimeValuePair>>> nextDevice = lastReader.next();
        IDeviceID deviceID = nextDevice.left;
        String[] measurements = nextDevice.right.stream().map(Pair::getLeft).toArray(String[]::new);
        TimeValuePair[] timeValuePairs =
            nextDevice.right.stream().map(Pair::getRight).toArray(TimeValuePair[]::new);
        if (isTableModel) {
          TableDeviceSchemaCache.getInstance()
              .updateLastCacheIfExists(databaseName, deviceID, measurements, timeValuePairs);
        } else {
          // we do not update schema here, so aligned is not relevant
          TreeDeviceSchemaCacheManager.getInstance()
              .updateLastCacheIfExists(
                  databaseName, deviceID, measurements, timeValuePairs, false, null);
        }
      }
    } else {
      TreeDeviceSchemaCacheManager.getInstance().cleanUp();
    }
  }

  private long getAndSetNewVersion(long timePartitionId, TsFileResource tsFileResource) {
    long version =
        partitionMaxFileVersions.compute(
            timePartitionId, (key, oldVersion) -> (oldVersion == null ? 1 : oldVersion + 1));
    tsFileResource.setVersion(version);
    return version;
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
      final File tsFileToLoad,
      final TsFileResource tsFileResource,
      final long filePartitionId,
      final boolean deleteOriginFile,
      boolean isGeneratedByPipe)
      throws LoadFileException, DiskSpaceInsufficientException {
    final int targetTierLevel = 0;
    final String fileName =
        databaseName
            + File.separatorChar
            + dataRegionIdString
            + File.separatorChar
            + filePartitionId
            + File.separator
            + tsFileResource.getTsFile().getName();
    final File targetFile =
        (tsFileResource.isGeneratedByPipeConsensus() || tsFileResource.isGeneratedByPipe())
            ? pipeAndIoTV2LoadDiskSelector.selectTargetDirectory(
                tsFileToLoad.getParentFile(), fileName, true, targetTierLevel)
            : ordinaryLoadDiskSelector.selectTargetDirectory(
                tsFileToLoad.getParentFile(), fileName, true, targetTierLevel);

    tsFileResource.setFile(targetFile);
    if (tsFileManager.contains(tsFileResource, false)) {
      logger.warn("The file {} has already been loaded in unsequence list", tsFileResource);
      return false;
    }

    logger.info(
        "Load tsfile in unsequence list, move file from {} to {}",
        tsFileToLoad.getAbsolutePath(),
        targetFile.getAbsolutePath());

    LoadTsFileRateLimiter.getInstance().acquire(tsFileResource.getTsFile().length());

    // move file from sync dir to data dir
    if (!targetFile.getParentFile().exists()) {
      targetFile.getParentFile().mkdirs();
    }
    try {
      if (deleteOriginFile) {
        RetryUtils.retryOnException(
            () -> {
              FileUtils.moveFile(tsFileToLoad, targetFile);
              return null;
            });
      } else {
        RetryUtils.retryOnException(
            () -> {
              Files.copy(tsFileToLoad.toPath(), targetFile.toPath());
              return null;
            });
      }
    } catch (final IOException e) {
      logger.warn(
          "File renaming failed when loading tsfile. Origin: {}, Target: {}",
          tsFileToLoad.getAbsolutePath(),
          targetFile.getAbsolutePath(),
          e);
      throw new LoadFileException(
          String.format(
              "File renaming failed when loading tsfile. Origin: %s, Target: %s, because %s",
              tsFileToLoad.getAbsolutePath(), targetFile.getAbsolutePath(), e.getMessage()));
    }

    final File resourceFileToLoad =
        fsFactory.getFile(tsFileToLoad.getAbsolutePath() + RESOURCE_SUFFIX);
    final File targetResourceFile =
        fsFactory.getFile(targetFile.getAbsolutePath() + RESOURCE_SUFFIX);
    try {
      if (deleteOriginFile) {
        RetryUtils.retryOnException(
            () -> {
              FileUtils.moveFile(resourceFileToLoad, targetResourceFile);
              return null;
            });
      } else {
        RetryUtils.retryOnException(
            () -> {
              Files.copy(resourceFileToLoad.toPath(), targetResourceFile.toPath());
              return null;
            });
      }
    } catch (final IOException e) {
      logger.warn(
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

    loadModFile(tsFileToLoad, targetFile, deleteOriginFile, tsFileResource);

    // Listen before the tsFile is added into tsFile manager to avoid it being compacted
    PipeInsertionDataNodeListener.getInstance()
        .listenToTsFile(dataRegionIdString, databaseName, tsFileResource, true);

    tsFileManager.add(tsFileResource, false);

    return true;
  }

  private void loadModFile(
      File tsFileToLoad, File targetTsFile, boolean deleteOriginFile, TsFileResource tsFileResource)
      throws LoadFileException {
    final File oldModFileToLoad = ModificationFileV1.getNormalMods(tsFileToLoad);
    final File newModFileToLoad = ModificationFile.getExclusiveMods(tsFileToLoad);
    if (oldModFileToLoad.exists()) {
      final File oldTargetModFile = ModificationFileV1.getNormalMods(targetTsFile);
      moveModFile(oldModFileToLoad, oldTargetModFile, deleteOriginFile);
      try {
        tsFileResource.upgradeModFile(upgradeModFileThreadPool);
      } catch (IOException e) {
        throw new LoadFileException(e);
      }
    } else if (newModFileToLoad.exists()) {
      final File newTargetModFile = ModificationFile.getExclusiveMods(targetTsFile);
      moveModFile(newModFileToLoad, newTargetModFile, deleteOriginFile);
    }
    // force update mod file metrics
    tsFileResource.getExclusiveModFile();
  }

  @SuppressWarnings("java:S2139")
  private void moveModFile(File modFileToLoad, File targetModFile, boolean deleteOriginFile)
      throws LoadFileException {
    if (modFileToLoad.exists()) {
      // when successfully loaded, the filepath of the resource will be changed to the IoTDB data
      // dir, so we can add a suffix to find the old modification file.
      try {
        RetryUtils.retryOnException(
            () -> {
              Files.deleteIfExists(targetModFile.toPath());
              return null;
            });
      } catch (final IOException e) {
        logger.warn("Cannot delete localModFile {}", targetModFile, e);
      }
      try {
        if (deleteOriginFile) {
          RetryUtils.retryOnException(
              () -> {
                FileUtils.moveFile(modFileToLoad, targetModFile);
                return null;
              });
        } else {
          RetryUtils.retryOnException(
              () -> {
                Files.copy(modFileToLoad.toPath(), targetModFile.toPath());
                return null;
              });
        }
      } catch (final IOException e) {
        logger.warn(
            "File renaming failed when loading .mod file. Origin: {}, Target: {}",
            modFileToLoad.getAbsolutePath(),
            targetModFile.getAbsolutePath(),
            e);
        throw new LoadFileException(
            String.format(
                "File renaming failed when loading .mod file. Origin: %s, Target: %s, because %s",
                modFileToLoad.getAbsolutePath(), targetModFile.getAbsolutePath(), e.getMessage()));
      }
    }
  }

  /**
   * Get all working sequence tsfile processors
   *
   * @return all working sequence tsfile processors
   */
  public Collection<TsFileProcessor> getWorkSequenceTsFileProcessors() {
    return workSequenceTsFileProcessors.values();
  }

  public boolean removeTsFile(File fileToBeRemoved) {
    TsFileResource tsFileResourceToBeRemoved = unloadTsFileInside(fileToBeRemoved);
    if (tsFileResourceToBeRemoved == null) {
      return false;
    }
    tsFileResourceToBeRemoved.writeLock();
    try {
      tsFileResourceToBeRemoved.remove();
      logger.info("Remove tsfile {} successfully.", tsFileResourceToBeRemoved.getTsFile());
    } finally {
      tsFileResourceToBeRemoved.writeUnlock();
    }
    return true;
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
    TsFileResource tsFileResourceToBeMoved = unloadTsFileInside(fileToBeUnloaded);
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

  private TsFileResource unloadTsFileInside(File fileToBeUnloaded) {
    writeLock("unloadTsFileInside");
    TsFileResource unloadedTsFileResource = null;
    try {
      Iterator<TsFileResource> sequenceIterator = tsFileManager.getIterator(true);
      while (sequenceIterator.hasNext()) {
        TsFileResource sequenceResource = sequenceIterator.next();
        if (sequenceResource.getTsFile().getName().equals(fileToBeUnloaded.getName())) {
          unloadedTsFileResource = sequenceResource;
          tsFileManager.remove(unloadedTsFileResource, true);
          FileMetrics.getInstance()
              .deleteTsFile(true, Collections.singletonList(unloadedTsFileResource));
          break;
        }
      }
      if (unloadedTsFileResource == null) {
        Iterator<TsFileResource> unsequenceIterator = tsFileManager.getIterator(false);
        while (unsequenceIterator.hasNext()) {
          TsFileResource unsequenceResource = unsequenceIterator.next();
          if (unsequenceResource.getTsFile().getName().equals(fileToBeUnloaded.getName())) {
            unloadedTsFileResource = unsequenceResource;
            tsFileManager.remove(unloadedTsFileResource, false);
            FileMetrics.getInstance()
                .deleteTsFile(false, Collections.singletonList(unloadedTsFileResource));
            break;
          }
        }
      }
    } finally {
      writeUnlock();
    }
    return unloadedTsFileResource;
  }

  /**
   * Get all working unsequence tsfile processors
   *
   * @return all working unsequence tsfile processors
   */
  public Collection<TsFileProcessor> getWorkUnsequenceTsFileProcessors() {
    return workUnsequenceTsFileProcessors.values();
  }

  public List<TsFileResource> getSequenceFileList() {
    return tsFileManager.getTsFileList(true);
  }

  public List<TsFileResource> getUnSequenceFileList() {
    return tsFileManager.getTsFileList(false);
  }

  @Override
  public String getDataRegionIdString() {
    return dataRegionIdString;
  }

  /**
   * Get the storageGroupPath with dataRegionId.
   *
   * @return data region path, like root.sg1/0
   */
  public String getStorageGroupPath() {
    return databaseName + File.separator + dataRegionIdString;
  }

  public void abortCompaction() {
    tsFileManager.setAllowCompaction(false);
    CompactionScheduleTaskManager.getInstance().unregisterDataRegion(this);
    List<AbstractCompactionTask> runningTasks =
        CompactionTaskManager.getInstance()
            .abortCompaction(databaseName + "-" + dataRegionIdString);
    while (CompactionTaskManager.getInstance().isAnyTaskInListStillRunning(runningTasks)) {
      try {
        TimeUnit.MILLISECONDS.sleep(10);
      } catch (InterruptedException e) {
        logger.error("Thread get interrupted when waiting compaction to finish", e);
        Thread.currentThread().interrupt();
      }
    }
    isCompactionSelecting.set(false);
  }

  public TsFileManager getTsFileResourceManager() {
    return tsFileManager;
  }

  /**
   * Insert batch of rows belongs to one device
   *
   * @param insertRowsOfOneDeviceNode batch of rows belongs to one device
   */
  public void insert(InsertRowsOfOneDeviceNode insertRowsOfOneDeviceNode)
      throws WriteProcessException, BatchProcessException {
    StorageEngine.blockInsertionIfReject();
    long startTime = System.nanoTime();
    writeLock("InsertRowsOfOneDevice");
    PERFORMANCE_OVERVIEW_METRICS.recordScheduleLockCost(System.nanoTime() - startTime);
    try {
      if (deleted) {
        return;
      }
      long ttl = getTTL(insertRowsOfOneDeviceNode);
      Map<TsFileProcessor, InsertRowsNode> tsFileProcessorMap = new HashMap<>();
      for (int i = 0; i < insertRowsOfOneDeviceNode.getInsertRowNodeList().size(); i++) {
        InsertRowNode insertRowNode = insertRowsOfOneDeviceNode.getInsertRowNodeList().get(i);
        if (!CommonUtils.isAlive(insertRowNode.getTime(), ttl)) {
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
                              CommonDateTimeUtils.currentTime() - ttl))));
          continue;
        }
        // init map
        long timePartitionId = TimePartitionUtils.getTimePartitionId(insertRowNode.getTime());

        if (config.isEnableSeparateData()
            && !lastFlushTimeMap.checkAndCreateFlushedTimePartition(timePartitionId, true)) {
          TimePartitionManager.getInstance()
              .registerTimePartitionInfo(
                  new TimePartitionInfo(
                      new DataRegionId(Integer.parseInt(dataRegionIdString)),
                      timePartitionId,
                      true,
                      Long.MAX_VALUE,
                      0));
        }

        boolean isSequence =
            config.isEnableSeparateData()
                && insertRowNode.getTime()
                    > lastFlushTimeMap.getFlushedTime(timePartitionId, insertRowNode.getDeviceID());
        TsFileProcessor tsFileProcessor = getOrCreateTsFileProcessor(timePartitionId, isSequence);
        if (tsFileProcessor == null) {
          continue;
        }
        int finalI = i;
        tsFileProcessorMap.compute(
            tsFileProcessor,
            (k, v) -> {
              if (v == null) {
                v = new InsertRowsNode(insertRowsOfOneDeviceNode.getPlanNodeId());
                v.setSearchIndex(insertRowNode.getSearchIndex());
                v.setAligned(insertRowNode.isAligned());
                if (insertRowNode.isGeneratedByPipe()) {
                  v.markAsGeneratedByPipe();
                }
                if (insertRowNode.isGeneratedByRemoteConsensusLeader()) {
                  v.markAsGeneratedByRemoteConsensusLeader();
                }
              }
              v.addOneInsertRowNode(insertRowNode, finalI);
              v.updateProgressIndex(insertRowNode.getProgressIndex());
              return v;
            });
      }
      List<InsertRowNode> executedInsertRowNodeList = new ArrayList<>();
      long[] infoForMetrics = new long[5];
      // infoForMetrics[0]: CreateMemtableBlockTimeCost
      // infoForMetrics[1]: ScheduleMemoryBlockTimeCost
      // infoForMetrics[2]: ScheduleWalTimeCost
      // infoForMetrics[3]: ScheduleMemTableTimeCost
      // infoForMetrics[4]: InsertedPointsNumber
      for (Map.Entry<TsFileProcessor, InsertRowsNode> entry : tsFileProcessorMap.entrySet()) {
        TsFileProcessor tsFileProcessor = entry.getKey();
        InsertRowsNode subInsertRowsNode = entry.getValue();
        try {
          tsFileProcessor =
              insertRowsWithTypeConsistencyCheck(
                  tsFileProcessor, subInsertRowsNode, infoForMetrics);
        } catch (WriteProcessException e) {
          insertRowsOfOneDeviceNode
              .getResults()
              .put(
                  subInsertRowsNode.getInsertRowNodeIndexList().get(0),
                  RpcUtils.getStatus(e.getErrorCode(), e.getMessage()));
        }
        executedInsertRowNodeList.addAll(subInsertRowsNode.getInsertRowNodeList());

        // check memtable size and may asyncTryToFlush the work memtable
        if (tsFileProcessor.shouldFlush()) {
          fileFlushPolicy.apply(this, tsFileProcessor, tsFileProcessor.isSequence());
        }
      }

      updateTsFileProcessorMetric(insertRowsOfOneDeviceNode, infoForMetrics);
      if (CommonDescriptor.getInstance().getConfig().isLastCacheEnable()
          && !insertRowsOfOneDeviceNode.isGeneratedByRemoteConsensusLeader()) {
        // disable updating last cache on follower
        startTime = System.nanoTime();
        tryToUpdateInsertRowsLastCache(executedInsertRowNodeList);
        PERFORMANCE_OVERVIEW_METRICS.recordScheduleUpdateLastCacheCost(
            System.nanoTime() - startTime);
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
    StorageEngine.blockInsertionIfReject();
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
        long ttl = getTTL(insertRowNode);
        if (!CommonUtils.isAlive(insertRowNode.getTime(), ttl)) {
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
                              CommonDateTimeUtils.currentTime() - ttl))));
          insertRowNode.setFailedMeasurementNumber(insertRowNode.getMeasurements().length);
          insertRowNode.setMeasurements(null);
          continue;
        }
        // init map
        timePartitionIds[i] = TimePartitionUtils.getTimePartitionId(insertRowNode.getTime());

        if (config.isEnableSeparateData()
            && !lastFlushTimeMap.checkAndCreateFlushedTimePartition(timePartitionIds[i], true)) {
          TimePartitionManager.getInstance()
              .registerTimePartitionInfo(
                  new TimePartitionInfo(
                      new DataRegionId(Integer.parseInt(dataRegionIdString)),
                      timePartitionIds[i],
                      true,
                      Long.MAX_VALUE,
                      0));
        }
        areSequence[i] =
            config.isEnableSeparateData()
                && insertRowNode.getTime()
                    > lastFlushTimeMap.getFlushedTime(
                        timePartitionIds[i], insertRowNode.getDeviceID());
      }
      long[] infoForMetrics = new long[5];
      // infoForMetrics[0]: CreateMemtableBlockTimeCost
      // infoForMetrics[1]: ScheduleMemoryBlockTimeCost
      // infoForMetrics[2]: ScheduleWalTimeCost
      // infoForMetrics[3]: ScheduleMemTableTimeCost
      // infoForMetrics[4]: InsertedPointsNumber
      List<InsertRowNode> executedInsertRowNodeList =
          insertToTsFileProcessors(insertRowsNode, areSequence, timePartitionIds, infoForMetrics);
      updateTsFileProcessorMetric(insertRowsNode, infoForMetrics);

      if (CommonDescriptor.getInstance().getConfig().isLastCacheEnable()
          && !insertRowsNode.isGeneratedByRemoteConsensusLeader()) {
        // disable updating last cache on follower
        startTime = System.nanoTime();
        tryToUpdateInsertRowsLastCache(executedInsertRowNodeList);
        PERFORMANCE_OVERVIEW_METRICS.recordScheduleUpdateLastCacheCost(
            System.nanoTime() - startTime);
      }

      if (!insertRowsNode.getResults().isEmpty()) {
        throw new BatchProcessException("Partial failed inserting rows");
      }
    } finally {
      writeUnlock();
    }
  }

  /**
   * Insert batch of tablets belongs to multiple devices
   *
   * @param insertMultiTabletsNode batch of tablets belongs to multiple devices
   */
  public void insertTablets(InsertMultiTabletsNode insertMultiTabletsNode)
      throws BatchProcessException, WriteProcessRejectException {

    StorageEngine.blockInsertionIfReject();
    long startTime = System.nanoTime();
    writeLock("insertTablets");
    PERFORMANCE_OVERVIEW_METRICS.recordScheduleLockCost(System.nanoTime() - startTime);
    try {
      if (deleted) {
        logger.info(
            "Won't insert tablets {}, because region is deleted",
            insertMultiTabletsNode.getSearchIndex());
        return;
      }
      long[] infoForMetrics = new long[5];
      // infoForMetrics[0]: CreateMemtableBlockTimeCost
      // infoForMetrics[1]: ScheduleMemoryBlockTimeCost
      // infoForMetrics[2]: ScheduleWalTimeCost
      // infoForMetrics[3]: ScheduleMemTableTimeCost
      // infoForMetrics[4]: InsertedPointsNumber
      for (int i = 0; i < insertMultiTabletsNode.getInsertTabletNodeList().size(); i++) {
        InsertTabletNode insertTabletNode = insertMultiTabletsNode.getInsertTabletNodeList().get(i);
        TSStatus[] results = new TSStatus[insertTabletNode.getRowCount()];
        Arrays.fill(results, RpcUtils.SUCCESS_STATUS);
        boolean noFailure = false;
        try {
          noFailure = executeInsertTablet(insertTabletNode, results, infoForMetrics);
        } catch (WriteProcessException e) {
          insertMultiTabletsNode
              .getResults()
              .put(i, RpcUtils.getStatus(e.getErrorCode(), e.getMessage()));
        }
        if (!noFailure) {
          // for each error
          TSStatus firstStatus = null;
          for (TSStatus status : results) {
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
      updateTsFileProcessorMetric(insertMultiTabletsNode, infoForMetrics);

    } finally {
      writeUnlock();
    }

    if (!insertMultiTabletsNode.getResults().isEmpty()) {
      throw new BatchProcessException("Partial failed inserting multi tablets");
    }
  }

  private void updateTsFileProcessorMetric(InsertNode insertNode, long[] infoForMetrics) {
    PERFORMANCE_OVERVIEW_METRICS.recordCreateMemtableBlockCost(infoForMetrics[0]);
    PERFORMANCE_OVERVIEW_METRICS.recordScheduleMemoryBlockCost(infoForMetrics[1]);
    PERFORMANCE_OVERVIEW_METRICS.recordScheduleWalCost(infoForMetrics[2]);
    PERFORMANCE_OVERVIEW_METRICS.recordScheduleMemTableCost(infoForMetrics[3]);
    MetricService.getInstance()
        .count(
            infoForMetrics[4],
            Metric.QUANTITY.toString(),
            MetricLevel.CORE,
            Tag.NAME.toString(),
            Metric.POINTS_IN.toString(),
            Tag.DATABASE.toString(),
            databaseName,
            Tag.REGION.toString(),
            dataRegionIdString,
            Tag.TYPE.toString(),
            Metric.MEMTABLE_POINT_COUNT.toString());
    if (!insertNode.isGeneratedByRemoteConsensusLeader()) {
      MetricService.getInstance()
          .count(
              infoForMetrics[4],
              Metric.LEADER_QUANTITY.toString(),
              MetricLevel.CORE,
              Tag.NAME.toString(),
              Metric.POINTS_IN.toString(),
              Tag.DATABASE.toString(),
              databaseName,
              Tag.REGION.toString(),
              dataRegionIdString,
              Tag.TYPE.toString(),
              Metric.MEMTABLE_POINT_COUNT.toString());
    }
  }

  /**
   * @return the disk space occupied by this data region, unit is MB
   */
  public long countRegionDiskSize() {
    AtomicLong diskSize = new AtomicLong(0);
    TierManager.getInstance()
        .getAllLocalFilesFolders()
        .forEach(
            folder -> {
              folder = folder + File.separator + databaseName + File.separator + dataRegionIdString;
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
  public interface UpdateEndTimeCallBack {

    void call(TsFileProcessor caller, Map<IDeviceID, Long> updateMap, long systemFlushTime);
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

  public boolean isDeleted() {
    return deleted;
  }

  /** This method could only be used in iot consensus */
  public Optional<IWALNode> getWALNode() {
    if (!config.getDataRegionConsensusProtocolClass().equals(ConsensusFactory.IOT_CONSENSUS)) {
      return Optional.empty();
    }
    // identifier should be same with getTsFileProcessor method
    return Optional.of(
        WALManager.getInstance()
            .applyForWALNode(databaseName + FILE_NAME_SEPARATOR + dataRegionIdString));
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
      releaseDirectBufferMemory();
      MetricService.getInstance().removeMetricSet(metrics);
      deletedCondition.signalAll();
    } finally {
      writeUnlock();
    }
  }

  private void acquireDirectBufferMemory() throws DataRegionException {
    long acquireDirectBufferMemCost = getAcquireDirectBufferMemCost();
    if (!SystemInfo.getInstance().addDirectBufferMemoryCost(acquireDirectBufferMemCost)) {
      throw new DataRegionException(
          "Total allocated memory for direct buffer will be "
              + (SystemInfo.getInstance().getDirectBufferMemoryCost() + acquireDirectBufferMemCost)
              + ", which is greater than limit mem cost: "
              + SystemInfo.getInstance().getTotalDirectBufferMemorySizeLimit());
    }
    this.directBufferMemoryCost = acquireDirectBufferMemCost;
  }

  public static long getAcquireDirectBufferMemCost() {
    long acquireDirectBufferMemCost = 0;
    if (config.getDataRegionConsensusProtocolClass().equals(ConsensusFactory.IOT_CONSENSUS)
        || config.getDataRegionConsensusProtocolClass().equals(ConsensusFactory.IOT_CONSENSUS_V2)) {
      acquireDirectBufferMemCost =
          config.getWalMode().equals(WALMode.DISABLE) ? 0 : config.getWalBufferSize();
    } else if (config
        .getDataRegionConsensusProtocolClass()
        .equals(ConsensusFactory.RATIS_CONSENSUS)) {
      acquireDirectBufferMemCost = config.getDataRatisConsensusLogAppenderBufferSizeMax();
    }
    if (config.getDataRegionConsensusProtocolClass().equals(ConsensusFactory.IOT_CONSENSUS_V2)) {
      acquireDirectBufferMemCost += PageCacheDeletionBuffer.DAL_BUFFER_SIZE;
    }
    return acquireDirectBufferMemCost;
  }

  private void releaseDirectBufferMemory() {
    SystemInfo.getInstance().decreaseDirectBufferMemoryCost(directBufferMemoryCost);
    // avoid repeated deletion
    this.directBufferMemoryCost = 0;
  }

  /* Be careful, the thread that calls this method may not hold the write lock!!*/
  public void degradeFlushTimeMap(long timePartitionId) {
    lastFlushTimeMap.degradeLastFlushTime(timePartitionId);
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

  public void compactFileTimeIndexCache() {
    tsFileManager.compactFileTimeIndexCache();
  }

  @TestOnly
  public ILastFlushTimeMap getLastFlushTimeMap() {
    return lastFlushTimeMap;
  }

  public TsFileManager getTsFileManager() {
    return tsFileManager;
  }

  private long getTTL(InsertNode insertNode) {
    if (insertNode.getTableName() == null) {
      return DataNodeTTLCache.getInstance().getTTLForTree(insertNode.getTargetPath().getNodes());
    } else {
      return DataNodeTTLCache.getInstance().getTTLForTable(databaseName, insertNode.getTableName());
    }
  }
}
