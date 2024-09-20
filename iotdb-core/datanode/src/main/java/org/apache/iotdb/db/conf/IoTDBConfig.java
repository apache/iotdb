/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.conf;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.property.ClientPoolProperty.DefaultProperty;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.audit.AuditLogOperation;
import org.apache.iotdb.db.audit.AuditLogStorage;
import org.apache.iotdb.db.exception.LoadConfigurationException;
import org.apache.iotdb.db.protocol.thrift.impl.ClientRPCServiceImpl;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.constant.CrossCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.constant.InnerSeqCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.constant.InnerUnseqCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.constant.CompactionPriority;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.constant.CrossCompactionSelector;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.constant.InnerSequenceCompactionSelector;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.constant.InnerUnsequenceCompactionSelector;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.TimeIndexLevel;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALMode;
import org.apache.iotdb.db.utils.datastructure.TVListSortAlgorithm;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.metricsets.system.SystemMetrics;
import org.apache.iotdb.rpc.BaseRpcTransportFactory;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.ZeroCopyRpcTransportFactory;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.fileSystem.FSType;
import org.apache.tsfile.utils.FSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.apache.iotdb.commons.conf.IoTDBConstant.OBJECT_STORAGE_DIR;
import static org.apache.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;

public class IoTDBConfig {

  /* Names of Watermark methods */
  public static final String WATERMARK_GROUPED_LSB = "GroupBasedLSBMethod";
  public static final String CONFIG_NAME = "iotdb-system.properties";
  private static final Logger logger = LoggerFactory.getLogger(IoTDBConfig.class);
  private static final String MULTI_DIR_STRATEGY_PREFIX =
      "org.apache.iotdb.db.storageengine.rescon.disk.strategy.";
  private static final String[] CLUSTER_ALLOWED_MULTI_DIR_STRATEGIES =
      new String[] {"SequenceStrategy", "MaxDiskUsableSpaceFirstStrategy"};
  private static final String DEFAULT_MULTI_DIR_STRATEGY = "SequenceStrategy";

  private static final String STORAGE_GROUP_MATCHER = "([a-zA-Z0-9`_.\\-\\u2E80-\\u9FFF]+)";
  public static final Pattern STORAGE_GROUP_PATTERN = Pattern.compile(STORAGE_GROUP_MATCHER);

  // e.g., a31+/$%#&[]{}3e4, "a.b", 'a.b'
  private static final String NODE_NAME_MATCHER = "([^\n\t]+)";

  // e.g.,  .s1
  private static final String PARTIAL_NODE_MATCHER = "[" + PATH_SEPARATOR + "]" + NODE_NAME_MATCHER;

  private static final String NODE_MATCHER =
      "([" + PATH_SEPARATOR + "])?" + NODE_NAME_MATCHER + "(" + PARTIAL_NODE_MATCHER + ")*";

  public static final Pattern NODE_PATTERN = Pattern.compile(NODE_MATCHER);

  /** Whether to enable the mqtt service. */
  private boolean enableMQTTService = false;

  /** The mqtt service binding host. */
  private String mqttHost = "127.0.0.1";

  /** The mqtt service binding port. */
  private int mqttPort = 1883;

  /** The handler pool size for handing the mqtt messages. */
  private int mqttHandlerPoolSize = 1;

  /** The mqtt message payload formatter. */
  private String mqttPayloadFormatter = "json";

  /** Max mqtt message size. Unit: byte */
  private int mqttMaxMessageSize = 1048576;

  /** Rpc binding address. */
  private String rpcAddress = "0.0.0.0";

  /** whether to use thrift compression. */
  private boolean rpcThriftCompressionEnable = false;

  /** whether to use Snappy compression before sending data through the network */
  private boolean rpcAdvancedCompressionEnable = false;

  /** Port which the JDBC server listens to. */
  private int rpcPort = 6667;

  /** Enable the thrift rpcPort Service ssl. */
  private boolean enableSSL = false;

  /** ssl key Store Path. */
  private String keyStorePath = "";

  /** ssl key Store password. */
  private String keyStorePwd = "";

  /** Rpc Selector thread num */
  private int rpcSelectorThreadCount = 1;

  /** Min concurrent client number */
  private int rpcMinConcurrentClientNum = Runtime.getRuntime().availableProcessors();

  /** Max concurrent client number */
  private int rpcMaxConcurrentClientNum = 65535;

  /** Memory allocated for the write process */
  private long allocateMemoryForStorageEngine = Runtime.getRuntime().maxMemory() * 3 / 10;

  /** Memory allocated for the read process */
  private long allocateMemoryForRead = Runtime.getRuntime().maxMemory() * 3 / 10;

  /** Memory allocated for the mtree */
  private long allocateMemoryForSchema = Runtime.getRuntime().maxMemory() / 10;

  /** Memory allocated for the consensus layer */
  private long allocateMemoryForConsensus = Runtime.getRuntime().maxMemory() / 10;

  /** Memory allocated for the pipe */
  private long allocateMemoryForPipe = Runtime.getRuntime().maxMemory() / 10;

  /** Ratio of memory allocated for buffered arrays */
  private double bufferedArraysMemoryProportion = 0.6;

  /** Flush proportion for system */
  private double flushProportion = 0.4;

  /** Reject proportion for system */
  private double rejectProportion = 0.8;

  /** The proportion of write memory for memtable */
  private double writeProportionForMemtable = 0.76;

  /** The proportion of write memory for compaction */
  private double compactionProportion = 0.2;

  /** The proportion of memtable memory for device path cache */
  private double devicePathCacheProportion = 0.05;

  /**
   * If memory cost of data region increased more than proportion of {@linkplain
   * IoTDBConfig#getAllocateMemoryForStorageEngine()}*{@linkplain
   * IoTDBConfig#getWriteProportionForMemtable()}, report to system.
   */
  private double writeMemoryVariationReportProportion = 0.001;

  /** When inserting rejected, waiting period to check system again. Unit: millisecond */
  private int checkPeriodWhenInsertBlocked = 50;

  /** When inserting rejected exceeds this, throw an exception. Unit: millisecond */
  private int maxWaitingTimeWhenInsertBlockedInMs = 10000;

  /** off heap memory bytes from env */
  private long maxOffHeapMemoryBytes = 0;

  // region Write Ahead Log Configuration
  /** Write mode of wal */
  private volatile WALMode walMode = WALMode.ASYNC;

  /** Max number of wal nodes, each node corresponds to one wal directory */
  private int maxWalNodesNum = 0;

  /**
   * Duration a wal flush operation will wait before calling fsync in the async mode. Unit:
   * millisecond
   */
  private volatile long walAsyncModeFsyncDelayInMs = 1_000;

  /**
   * Duration a wal flush operation will wait before calling fsync in the sync mode. Unit:
   * millisecond
   */
  private volatile long walSyncModeFsyncDelayInMs = 3;

  /** Buffer size of each wal node. Unit: byte */
  private int walBufferSize = 32 * 1024 * 1024;

  /** max total direct buffer off heap memory size proportion */
  private double maxDirectBufferOffHeapMemorySizeProportion = 0.8;

  /** Blocking queue capacity of each wal buffer */
  private int walBufferQueueCapacity = 500;

  /** Size threshold of each wal file. Unit: byte */
  private volatile long walFileSizeThresholdInByte = 30 * 1024 * 1024L;

  /** Size threshold of each checkpoint file. Unit: byte */
  private volatile long checkpointFileSizeThresholdInByte = 3 * 1024 * 1024L;

  /** Minimum ratio of effective information in wal files */
  private volatile double walMinEffectiveInfoRatio = 0.1;

  /**
   * MemTable size threshold for triggering MemTable snapshot in wal. When a memTable's size exceeds
   * this, wal can flush this memtable to disk, otherwise wal will snapshot this memtable in wal.
   * Unit: byte
   */
  private volatile long walMemTableSnapshotThreshold = 8 * 1024 * 1024L;

  /** MemTable's max snapshot number in wal file */
  private volatile int maxWalMemTableSnapshotNum = 1;

  /** The period when outdated wal files are periodically deleted. Unit: millisecond */
  private volatile long deleteWalFilesPeriodInMs = 20 * 1000L;

  /**
   * Enables or disables the automatic clearing of the WAL cache when a memory compaction is
   * triggered. When enabled, the WAL cache will be cleared to release memory during the compaction
   * process.
   */
  private volatile boolean WALCacheShrinkClearEnabled = true;

  // endregion
  /**
   * The cycle when metadata log is periodically forced to be written to disk(in milliseconds) If
   * set this parameter to 0 it means call channel.force(true) after every each operation
   */
  private long syncMlogPeriodInMs = 100;

  /**
   * The size of log buffer for every trigger management operation plan. If the size of a trigger
   * management operation plan is larger than this parameter, the trigger management operation plan
   * will be rejected by TriggerManager. Unit: byte
   */
  private int tlogBufferSize = 1024 * 1024;

  /** System directory, including version file for each database and metadata */
  private String systemDir =
      IoTDBConstant.DN_DEFAULT_DATA_DIR + File.separator + IoTDBConstant.SYSTEM_FOLDER_NAME;

  /** Schema directory, including storage set of values. */
  private String schemaDir =
      IoTDBConstant.DN_DEFAULT_DATA_DIR
          + File.separator
          + IoTDBConstant.SYSTEM_FOLDER_NAME
          + File.separator
          + IoTDBConstant.SCHEMA_FOLDER_NAME;

  /** Query directory, stores temporary files of query */
  private String queryDir =
      IoTDBConstant.DN_DEFAULT_DATA_DIR + File.separator + IoTDBConstant.QUERY_FOLDER_NAME;

  /** External lib directory, stores user-uploaded JAR files */
  private String extDir = IoTDBConstant.EXT_FOLDER_NAME;

  /** External lib directory for UDF, stores user-uploaded JAR files */
  private String udfDir =
      IoTDBConstant.EXT_FOLDER_NAME + File.separator + IoTDBConstant.UDF_FOLDER_NAME;

  /** External temporary lib directory for storing downloaded udf JAR files */
  private String udfTemporaryLibDir = udfDir + File.separator + IoTDBConstant.TMP_FOLDER_NAME;

  /** External lib directory for trigger, stores user-uploaded JAR files */
  private String triggerDir =
      IoTDBConstant.EXT_FOLDER_NAME + File.separator + IoTDBConstant.TRIGGER_FOLDER_NAME;

  /** External temporary lib directory for storing downloaded trigger JAR files */
  private String triggerTemporaryLibDir =
      triggerDir + File.separator + IoTDBConstant.TMP_FOLDER_NAME;

  /** External lib directory for Pipe Plugin, stores user-defined JAR files */
  private String pipeDir =
      IoTDBConstant.EXT_FOLDER_NAME + File.separator + IoTDBConstant.PIPE_FOLDER_NAME;

  /** External temporary lib directory for storing downloaded pipe plugin JAR files */
  private String pipeTemporaryLibDir = pipeDir + File.separator + IoTDBConstant.TMP_FOLDER_NAME;

  /** External lib directory for ext Pipe plugins, stores user-defined JAR files */
  private String extPipeDir =
      IoTDBConstant.EXT_FOLDER_NAME + File.separator + IoTDBConstant.EXT_PIPE_FOLDER_NAME;

  /** External lib directory for MQTT, stores user-uploaded JAR files */
  private String mqttDir =
      IoTDBConstant.EXT_FOLDER_NAME + File.separator + IoTDBConstant.MQTT_FOLDER_NAME;

  /** Tiered data directories. It can be settled as dataDirs = {{"data1"}, {"data2", "data3"}}; */
  private String[][] tierDataDirs = {
    {IoTDBConstant.DN_DEFAULT_DATA_DIR + File.separator + IoTDBConstant.DATA_FOLDER_NAME}
  };

  private String[] loadTsFileDirs = {
    tierDataDirs[0][0] + File.separator + IoTDBConstant.LOAD_TSFILE_FOLDER_NAME
  };

  /** Strategy of multiple directories. */
  private String multiDirStrategyClassName = null;

  private String ratisDataRegionSnapshotDir =
      IoTDBConstant.DN_DEFAULT_DATA_DIR
          + File.separator
          + IoTDBConstant.DATA_FOLDER_NAME
          + File.separator
          + IoTDBConstant.SNAPSHOT_FOLDER_NAME;

  /** Consensus directory. */
  private String consensusDir = IoTDBConstant.DN_DEFAULT_DATA_DIR + File.separator + "consensus";

  private String dataRegionConsensusDir =
      consensusDir + File.separator + IoTDBConstant.DATA_REGION_FOLDER_NAME;

  private String invalidDataRegionConsensusDir =
      consensusDir + File.separator + IoTDBConstant.INVALID_DATA_REGION_FOLDER_NAME;

  private String schemaRegionConsensusDir =
      consensusDir + File.separator + IoTDBConstant.SCHEMA_REGION_FOLDER_NAME;

  /** temp result directory for sortOperator */
  private String sortTmpDir =
      IoTDBConstant.DN_DEFAULT_DATA_DIR + File.separator + IoTDBConstant.TMP_FOLDER_NAME;

  /** Maximum MemTable number. Invalid when enableMemControl is true. */
  private int maxMemtableNumber = 0;

  /** The amount of data iterate each time in server */
  private int batchSize = 100000;

  /** How many threads can concurrently flush. When <= 0, use CPU core number. */
  private int flushThreadCount = Runtime.getRuntime().availableProcessors();

  /** How many threads can concurrently execute query statement. When <= 0, use CPU core number. */
  private int queryThreadCount = Runtime.getRuntime().availableProcessors();

  private int degreeOfParallelism = Math.max(1, Runtime.getRuntime().availableProcessors() / 2);

  private int mergeThresholdOfExplainAnalyze = 10;

  private int modeMapSizeThreshold = 10000;

  /** How many queries can be concurrently executed. When <= 0, use 1000. */
  private int maxAllowedConcurrentQueries = 1000;

  /** How many threads can concurrently evaluate windows. When <= 0, use CPU core number. */
  private int windowEvaluationThreadCount = Runtime.getRuntime().availableProcessors();

  /**
   * Max number of window evaluation tasks that can be pending for execution. When <= 0, the value
   * is 64 by default.
   */
  private int maxPendingWindowEvaluationTasks = 64;

  /** Is the write ahead log enable. */
  private boolean enableIndex = false;

  /** How many threads can concurrently build index. When <= 0, use CPU core number. */
  private int concurrentIndexBuildThread = Runtime.getRuntime().availableProcessors();

  /**
   * the index framework adopts sliding window model to preprocess the original tv list in the
   * subsequence matching task.
   */
  private int defaultIndexWindowRange = 10;

  /** index directory. */
  private String indexRootFolder = "data" + File.separator + "index";

  /** When a unSequence TsFile's file size (in byte) exceed this, the TsFile is forced closed. */
  private long unSeqTsFileSize = 0L;

  /** When a sequence TsFile's file size (in byte) exceed this, the TsFile is forced closed. */
  private long seqTsFileSize = 0L;

  /** Whether to timed flush sequence tsfiles' memtables. */
  private boolean enableTimedFlushSeqMemtable = true;

  /**
   * If a memTable's last update time is older than current time minus this, the memtable will be
   * flushed to disk.(only check sequence tsfiles' memtables) Unit: ms
   */
  private long seqMemtableFlushInterval = 10 * 60 * 1000L;

  /** The interval to check whether sequence memtables need flushing. Unit: ms */
  private long seqMemtableFlushCheckInterval = 30 * 1000L;

  /** Whether to timed flush unsequence tsfiles' memtables. */
  private boolean enableTimedFlushUnseqMemtable = true;

  /**
   * If a memTable's last update time is older than current time minus this, the memtable will be
   * flushed to disk.(only check unsequence tsfiles' memtables) Unit: ms
   */
  private long unseqMemtableFlushInterval = 10 * 60 * 1000L;

  /** The interval to check whether unsequence memtables need flushing. Unit: ms */
  private long unseqMemtableFlushCheckInterval = 30 * 1000L;

  /** The sort algorithm used in TVList */
  private TVListSortAlgorithm tvListSortAlgorithm = TVListSortAlgorithm.TIM;

  /** When average series point number reaches this, flush the memtable to disk */
  private int avgSeriesPointNumberThreshold = 100000;

  /** Enable inner space compaction for sequence files */
  private volatile boolean enableSeqSpaceCompaction = true;

  /** Enable inner space compaction for unsequence files */
  private volatile boolean enableUnseqSpaceCompaction = true;

  /** Compact the unsequence files into the overlapped sequence files */
  private volatile boolean enableCrossSpaceCompaction = true;

  /** Enable the service for AINode */
  private boolean enableAINodeService = false;

  /** The buffer for sort operation */
  private long sortBufferSize = 1024 * 1024L;

  /**
   * The strategy of inner space compaction task. There are just one inner space compaction strategy
   * SIZE_TIRED_COMPACTION:
   */
  private InnerSequenceCompactionSelector innerSequenceCompactionSelector =
      InnerSequenceCompactionSelector.SIZE_TIERED_MULTI_TARGET;

  private InnerSeqCompactionPerformer innerSeqCompactionPerformer =
      InnerSeqCompactionPerformer.READ_CHUNK;

  private InnerUnsequenceCompactionSelector innerUnsequenceCompactionSelector =
      InnerUnsequenceCompactionSelector.SIZE_TIERED_MULTI_TARGET;

  private InnerUnseqCompactionPerformer innerUnseqCompactionPerformer =
      InnerUnseqCompactionPerformer.FAST;

  /**
   * The strategy of cross space compaction task. There are just one cross space compaction strategy
   * SIZE_TIRED_COMPACTION:
   */
  private CrossCompactionSelector crossCompactionSelector = CrossCompactionSelector.REWRITE;

  private CrossCompactionPerformer crossCompactionPerformer = CrossCompactionPerformer.FAST;

  /**
   * The priority of compaction task execution. There are three priority strategy INNER_CROSS:
   * prioritize inner space compaction, reduce the number of files first CROSS INNER: prioritize
   * cross space compaction, eliminate the unsequence files first BALANCE: alternate two compaction
   * types
   */
  private CompactionPriority compactionPriority = CompactionPriority.INNER_CROSS;

  private double chunkMetadataSizeProportion = 0.1;

  private long innerCompactionTotalFileSizeThresholdInByte = 10737418240L;

  private int innerCompactionTotalFileNumThreshold = 100;

  private int maxLevelGapInInnerCompaction = 2;

  /** The target tsfile size in compaction, 2 GB by default */
  private long targetCompactionFileSize = 2147483648L;

  /** The target chunk size in compaction. */
  private long targetChunkSize = 1048576L;

  /** The target chunk point num in compaction. */
  private long targetChunkPointNum = 100000L;

  /**
   * If the chunk size is lower than this threshold, it will be deserialized into points, default is
   * 10 KB
   */
  private long chunkSizeLowerBoundInCompaction = 10240L;

  /**
   * If the chunk point num is lower than this threshold, it will be deserialized into points,
   * default is 1000
   */
  private long chunkPointNumLowerBoundInCompaction = 1000;

  /**
   * If compaction thread cannot acquire the write lock within this timeout, the compaction task
   * will be abort.
   */
  private long compactionAcquireWriteLockTimeout = 60_000L;

  /**
   * When the number of selected files reaches this value, the conditions for constructing a merge
   * task are met.
   */
  private volatile int innerCompactionCandidateFileNum = 30;

  /** The max candidate file num in one cross space compaction task */
  private volatile int fileLimitPerCrossTask = 500;

  /** The max candidate file num in compaction */
  private volatile int totalFileLimitForCompactionTask = 5000;

  /** The max total size of candidate files in one cross space compaction task */
  private volatile long maxCrossCompactionCandidateFileSize = 1024 * 1024 * 1024 * 5L;

  /**
   * Only the unseq files whose level of inner space compaction reaches this value can be selected
   * to participate in the cross space compaction.
   */
  private volatile int minCrossCompactionUnseqFileLevel = 1;

  /** The interval of compaction task schedulation in each virtual database. The unit is ms. */
  private long compactionScheduleIntervalInMs = 60_000L;

  /** The interval of ttl check task in each database. The unit is ms. Default is 2 hours. */
  private long ttlCheckInterval = 7_200_000L;

  /** The number of threads to be set up to check ttl. */
  private int ttlCheckerNum = 1;

  /**
   * The max expired time of device set with ttl. If the expired time exceeds this value, then
   * expired data will be cleaned by compaction. The unit is ms. Default is 1 month.
   */
  private long maxExpiredTime = 2_592_000_000L;

  /**
   * The expired device ratio. If the number of expired device in one tsfile exceeds this value,
   * then expired data of this tsfile will be cleaned by compaction.
   */
  private float expiredDataRatio = 0.3f;

  /**
   * The number of sub compaction threads to be set up to perform compaction. Currently only works
   * for nonAligned data in cross space compaction and unseq inner space compaction.
   */
  private int subCompactionTaskNum = 4;

  /** The number of threads to be set up to select compaction task. */
  private int compactionScheduleThreadNum = 4;

  private volatile boolean enableTsFileValidation = false;

  /** The size of candidate compaction task queue. */
  private int candidateCompactionTaskQueueSize = 50;

  /**
   * When the size of the mods file corresponding to TsFile exceeds this value, inner compaction
   * tasks containing mods files are selected first.
   */
  private volatile long innerCompactionTaskSelectionModsFileThreshold = 128 * 1024L;

  /**
   * When disk availability is lower than the sum of (disk_space_warning_threshold +
   * inner_compaction_task_selection_disk_redundancy), inner compaction tasks containing mods files
   * are selected first.
   */
  private volatile double innerCompactionTaskSelectionDiskRedundancy = 0.05;

  /** The size of global compaction estimation file info cahce. */
  private int globalCompactionFileInfoCacheSize = 1000;

  /** whether to cache meta data(ChunkMetaData and TsFileMetaData) or not. */
  private boolean metaDataCacheEnable = true;

  /** Memory allocated for bloomFilter cache in read process */
  private long allocateMemoryForBloomFilterCache = allocateMemoryForRead / 1001;

  /** Memory allocated for timeSeriesMetaData cache in read process */
  private long allocateMemoryForTimeSeriesMetaDataCache = allocateMemoryForRead * 200 / 1001;

  /** Memory allocated for chunk cache in read process */
  private long allocateMemoryForChunkCache = allocateMemoryForRead * 100 / 1001;

  /** Memory allocated for operators */
  private long allocateMemoryForCoordinator = allocateMemoryForRead * 50 / 1001;

  /** Memory allocated for operators */
  private long allocateMemoryForOperators = allocateMemoryForRead * 200 / 1001;

  /** Memory allocated for operators */
  private long allocateMemoryForDataExchange = allocateMemoryForRead * 200 / 1001;

  /** Max bytes of each FragmentInstance for DataExchange */
  private long maxBytesPerFragmentInstance = allocateMemoryForDataExchange / queryThreadCount;

  /** Memory allocated proportion for timeIndex */
  private long allocateMemoryForTimeIndex = allocateMemoryForRead * 200 / 1001;

  /** Memory allocated proportion for time partition info */
  private long allocateMemoryForTimePartitionInfo = allocateMemoryForStorageEngine * 8 / 10 / 20;

  /**
   * If true, we will estimate each query's possible memory footprint before executing it and deny
   * it if its estimated memory exceeds current free memory
   */
  private boolean enableQueryMemoryEstimation = true;

  /** Cache size of {@code checkAndGetDataTypeCache}. */
  private int mRemoteSchemaCacheSize = 100000;

  /**
   * Set the language version when loading file including error information, default value is "EN"
   */
  private String languageVersion = "EN";

  /** Examining period of cache file reader : 100 seconds. Unit: millisecond */
  private long cacheFileReaderClearPeriod = 100000;

  /** the max executing time of query in ms. Unit: millisecond */
  private long queryTimeoutThreshold = 60000;

  /** the max time to live of a session in ms. Unit: millisecond */
  private int sessionTimeoutThreshold = 0;

  /** Replace implementation class of JDBC service */
  private String rpcImplClassName = ClientRPCServiceImpl.class.getName();

  /**
   * The cluster name that this DataNode joined in the cluster mode. The default value
   * "defaultCluster" will be changed after join cluster
   */
  private String clusterName = "defaultCluster";

  /**
   * The cluster ID that this DataNode joined in the cluster mode. DataNode will fetch cluster ID
   * from ConfigNode and cache it here when first time use it.
   */
  private String clusterId = "";

  /**
   * The DataNodeId of this DataNode for cluster mode. The default value -1 will be changed after
   * join cluster
   */
  private volatile int dataNodeId = -1;

  /** Whether to use chunkBufferPool. */
  private boolean chunkBufferPoolEnable = false;

  /** Switch of creating schema automatically */
  private boolean enableAutoCreateSchema = true;

  /** Register time series as which type when receiving boolean string "true" or "false" */
  private TSDataType booleanStringInferType = TSDataType.BOOLEAN;

  /**
   * register time series as which type when receiving an integer string and using float may lose
   * precision
   */
  private TSDataType integerStringInferType = TSDataType.DOUBLE;

  /** register time series as which type when receiving a floating number string "6.7" */
  private TSDataType floatingStringInferType = TSDataType.DOUBLE;

  /**
   * register time series as which type when receiving the Literal NaN. Values can be DOUBLE, FLOAT
   * or TEXT
   */
  private TSDataType nanStringInferType = TSDataType.DOUBLE;

  /** Database level when creating schema automatically is enabled */
  private int defaultStorageGroupLevel = 1;

  /** BOOLEAN encoding when creating schema automatically is enabled */
  private TSEncoding defaultBooleanEncoding = TSEncoding.RLE;

  /** INT32 encoding when creating schema automatically is enabled */
  private TSEncoding defaultInt32Encoding = TSEncoding.TS_2DIFF;

  /** INT64 encoding when creating schema automatically is enabled */
  private TSEncoding defaultInt64Encoding = TSEncoding.TS_2DIFF;

  /** FLOAT encoding when creating schema automatically is enabled */
  private TSEncoding defaultFloatEncoding = TSEncoding.GORILLA;

  /** DOUBLE encoding when creating schema automatically is enabled */
  private TSEncoding defaultDoubleEncoding = TSEncoding.GORILLA;

  /** TEXT encoding when creating schema automatically is enabled */
  private TSEncoding defaultTextEncoding = TSEncoding.PLAIN;

  /** How many threads will be set up to perform settle tasks. */
  private int settleThreadNum = 1;

  /**
   * If one merge file selection runs for more than this time, it will be ended and its current
   * selection will be used as final selection. When < 0, it means time is unbounded. Unit:
   * millisecond
   */
  private long crossCompactionFileSelectionTimeBudget = 30 * 1000L;

  /**
   * A global merge will be performed each such interval, that is, each database will be merged (if
   * proper merge candidates can be found). Unit: second.
   */
  private long mergeIntervalSec = 0L;

  /** The limit of compaction merge can reach per second. When <= 0, no limit. unit: megabyte */
  private int compactionWriteThroughputMbPerSec = 16;

  /**
   * The limit of compaction read throughput can reach per second. When <= 0, no limit. unit:
   * megabyte
   */
  private int compactionReadThroughputMbPerSec = 0;

  /** The limit of compaction read operation can reach per second. When <= 0, no limit. */
  private int compactionReadOperationPerSec = 0;

  /**
   * How many thread will be set up to perform compaction, 10 by default. Set to 1 when less than or
   * equal to 0.
   */
  private int compactionThreadCount = 10;

  /**
   * How many chunk will be compact in aligned series compaction, 10 by default. Set to
   * Integer.MAX_VALUE when less than or equal to 0.
   */
  private int compactionMaxAlignedSeriesNumInOneBatch = 10;

  /*
   * How many thread will be set up to perform continuous queries. When <= 0, use max(1, CPU core number / 2).
   */
  private int continuousQueryThreadNum =
      Math.max(1, Runtime.getRuntime().availableProcessors() / 2);

  /*
   * Minimum every interval to perform continuous query.
   * The every interval of continuous query instances should not be lower than this limit.
   */
  private long continuousQueryMinimumEveryInterval = 1000;

  /** How much memory may be used in ONE SELECT INTO operation (in Byte). */
  private long intoOperationBufferSizeInByte = 100 * 1024 * 1024L;

  /**
   * The maximum number of rows can be processed in insert-tablet-plan when executing select-into
   * statements.
   */
  private int selectIntoInsertTabletPlanRowLimit = 10000;

  /** The number of threads in the thread pool that execute insert-tablet tasks. */
  private int intoOperationExecutionThreadCount = 2;

  /** Default TSfile storage is in local file system */
  private FSType tsFileStorageFs = FSType.LOCAL;

  /** Enable hdfs or not */
  private boolean enableHDFS = false;

  /** Default core-site.xml file path is /etc/hadoop/conf/core-site.xml */
  private String coreSitePath = "/etc/hadoop/conf/core-site.xml";

  /** Default hdfs-site.xml file path is /etc/hadoop/conf/hdfs-site.xml */
  private String hdfsSitePath = "/etc/hadoop/conf/hdfs-site.xml";

  /** Default HDFS ip is localhost */
  private String hdfsIp = "localhost";

  /** Default HDFS port is 9000 */
  private String hdfsPort = "9000";

  /** Default DFS NameServices is hdfsnamespace */
  private String dfsNameServices = "hdfsnamespace";

  /** Default DFS HA name nodes are nn1 and nn2 */
  private String dfsHaNamenodes = "nn1,nn2";

  /** Default DFS HA automatic failover is enabled */
  private boolean dfsHaAutomaticFailoverEnabled = true;

  /**
   * Default DFS client failover proxy provider is
   * "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
   */
  private String dfsClientFailoverProxyProvider =
      "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider";

  /** whether use kerberos to authenticate hdfs */
  private boolean useKerberos = false;

  /** full path of kerberos keytab file */
  private String kerberosKeytabFilePath = "/path";

  /** kerberos principal */
  private String kerberosPrincipal = "your principal";

  /** the default fill interval in LinearFill and PreviousFill, -1 means infinite past time */
  private int defaultFillInterval = -1;

  /** The default value of primitive array size in array pool */
  private int primitiveArraySize = 64;

  /**
   * Level of TimeIndex, which records the start time and end time of TsFileResource. Currently,
   * DEVICE_TIME_INDEX and FILE_TIME_INDEX are supported, and could not be changed after first set.
   */
  private TimeIndexLevel timeIndexLevel = TimeIndexLevel.ARRAY_DEVICE_TIME_INDEX;

  // just for test
  // wait for 60 second by default.
  private int thriftServerAwaitTimeForStopService = 60;

  // Interval num of tag and attribute records when force flushing to disk
  private int tagAttributeFlushInterval = 1000;

  // In one insert (one device, one timestamp, multiple measurements),
  // if enable partial insert, one measurement failure will not impact other measurements
  private boolean enablePartialInsert = true;

  private boolean enable13DataInsertAdapt = false;

  /**
   * Used to estimate the memory usage of text fields in a UDF query. It is recommended to set this
   * value to be slightly larger than the average length of all text records.
   */
  private int udfInitialByteArrayLengthForMemoryControl = 48;

  /**
   * How much memory may be used in ONE UDF query (in MB).
   *
   * <p>The upper limit is 20% of allocated memory for read.
   *
   * <p>udfMemoryBudgetInMB = udfReaderMemoryBudgetInMB + udfTransformerMemoryBudgetInMB +
   * udfCollectorMemoryBudgetInMB
   */
  private float udfMemoryBudgetInMB = (float) Math.min(30.0f, 0.2 * allocateMemoryForRead);

  private float udfReaderMemoryBudgetInMB = (float) (1.0 / 3 * udfMemoryBudgetInMB);

  private float udfTransformerMemoryBudgetInMB = (float) (1.0 / 3 * udfMemoryBudgetInMB);

  private float udfCollectorMemoryBudgetInMB = (float) (1.0 / 3 * udfMemoryBudgetInMB);

  /** Unit: byte */
  private int thriftMaxFrameSize = 536870912;

  private int thriftDefaultBufferSize = RpcUtils.THRIFT_DEFAULT_BUF_CAPACITY;

  /** time cost(ms) threshold for slow query. Unit: millisecond */
  private long slowQueryThreshold = 30000;

  private int patternMatchingThreshold = 1000000;

  /**
   * whether enable the rpc service. This parameter has no a corresponding field in the
   * iotdb-common.properties
   */
  private boolean enableRpcService = true;

  /** the size of ioTaskQueue */
  private int ioTaskQueueSizeForFlushing = 10;

  /** the number of data regions per user-defined database */
  private int dataRegionNum = 1;

  /** the interval to log recover progress of each vsg when starting iotdb */
  private long recoveryLogIntervalInMs = 5_000L;

  /**
   * Separate sequence and unsequence data or not. If it is false, then all data will be written
   * into unsequence data dir.
   */
  private boolean enableSeparateData = true;

  /** the method to transform device path to device id, can be 'Plain' or 'SHA256' */
  private String deviceIDTransformationMethod = "Plain";

  /**
   * whether create mapping file of id table. This file can map device id in tsfile to device path
   */
  private boolean enableIDTableLogFile = false;

  /** the memory used for metadata cache when using persistent schema */
  private int cachedMNodeSizeInPBTreeMode = -1;

  /** the minimum size (in bytes) of segment inside a pbtree file page */
  private short minimumSegmentInPBTree = 0;

  /** cache size for pages in one pbtree file */
  private int pageCacheSizeInPBTree = 1024;

  /** maximum number of logged pages before log erased */
  private int pbTreeLogSize = 16384;

  /**
   * Maximum number of measurement in one create timeseries plan node. If the number of measurement
   * in user request exceeds this limit, the request will be split.
   */
  private int maxMeasurementNumOfInternalRequest = 10000;

  /** Internal address for data node */
  private String internalAddress = "127.0.0.1";

  /** Internal port for coordinator */
  private int internalPort = 10730;

  /** Port for AINode */
  private int aiNodePort = 10780;

  /** Internal port for dataRegion consensus protocol */
  private int dataRegionConsensusPort = 10760;

  /** Internal port for schemaRegion consensus protocol */
  private int schemaRegionConsensusPort = 10750;

  /** Ip and port of config nodes. */
  private TEndPoint seedConfigNode = new TEndPoint("127.0.0.1", 10710);

  /** The time of data node waiting for the next retry to join into the cluster */
  private long joinClusterRetryIntervalMs = TimeUnit.SECONDS.toMillis(1);

  /**
   * The consensus protocol class for data region. The Datanode should communicate with ConfigNode
   * on startup and set this variable so that the correct class name can be obtained later when the
   * data region consensus layer singleton is initialized
   */
  private String dataRegionConsensusProtocolClass = ConsensusFactory.IOT_CONSENSUS;

  /**
   * The consensus protocol class for schema region. The Datanode should communicate with ConfigNode
   * on startup and set this variable so that the correct class name can be obtained later when the
   * schema region consensus layer singleton is initialized
   */
  private String schemaRegionConsensusProtocolClass = ConsensusFactory.RATIS_CONSENSUS;

  /**
   * The series partition executor class. The Datanode should communicate with ConfigNode on startup
   * and set this variable so that the correct class name can be obtained later when calculating the
   * series partition
   */
  private String seriesPartitionExecutorClass =
      "org.apache.iotdb.commons.partition.executor.hash.BKDRHashExecutor";

  /** The number of series partitions in a database */
  private int seriesPartitionSlotNum = 10000;

  /** Port that mpp data exchange thrift service listen to. */
  private int mppDataExchangePort = 10740;

  /** Core pool size of mpp data exchange. */
  private int mppDataExchangeCorePoolSize = 10;

  /** Max pool size of mpp data exchange. */
  private int mppDataExchangeMaxPoolSize = 10;

  /** Thread keep alive time in ms of mpp data exchange. */
  private int mppDataExchangeKeepAliveTimeInMs = 1000;

  /** Thrift socket and connection timeout between data node and config node. */
  private int connectionTimeoutInMS = (int) TimeUnit.SECONDS.toMillis(60);

  /**
   * ClientManager will have so many selector threads (TAsyncClientManager) to distribute to its
   * clients.
   */
  private int selectorNumOfClientManager =
      Runtime.getRuntime().availableProcessors() / 4 > 0
          ? Runtime.getRuntime().availableProcessors() / 4
          : 1;

  /**
   * The maximum number of clients that can be allocated for a node in a clientManager. When the
   * number of the client to a single node exceeds this number, the thread for applying for a client
   * will be blocked for a while, then ClientManager will throw ClientManagerException if there are
   * no clients after the block time.
   */
  private int maxClientNumForEachNode = DefaultProperty.MAX_CLIENT_NUM_FOR_EACH_NODE;

  /**
   * Cache size of partition cache in {@link
   * org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher}
   */
  private int partitionCacheSize = 1000;

  /** Cache size of user and role */
  private int authorCacheSize = 100;

  /** Cache expire time of user and role */
  private int authorCacheExpireTime = 30;

  /** Number of queues per forwarding trigger */
  private int triggerForwardMaxQueueNumber = 8;

  /** The length of one of the queues per forwarding trigger */
  private int triggerForwardMaxSizePerQueue = 2000;

  /** Trigger forwarding data size per batch */
  private int triggerForwardBatchSize = 50;

  /** Trigger HTTP forward pool size */
  private int triggerForwardHTTPPoolSize = 200;

  /** Trigger HTTP forward pool max connection for per route */
  private int triggerForwardHTTPPOOLMaxPerRoute = 20;

  /** Trigger MQTT forward pool size */
  private int triggerForwardMQTTPoolSize = 4;

  /** How many times will we retry to find an instance of stateful trigger */
  private int retryNumToFindStatefulTrigger = 3;

  /** ThreadPool size for read operation in coordinator */
  private int coordinatorReadExecutorSize = 20;

  /** ThreadPool size for write operation in coordinator */
  private int coordinatorWriteExecutorSize = 50;

  private int[] schemaMemoryProportion = new int[] {5, 4, 1};

  /** Memory allocated for schemaRegion */
  private long allocateMemoryForSchemaRegion = allocateMemoryForSchema * 5 / 10;

  /** Memory allocated for SchemaCache */
  private long allocateMemoryForSchemaCache = allocateMemoryForSchema * 4 / 10;

  /** Memory allocated for PartitionCache */
  private long allocateMemoryForPartitionCache = allocateMemoryForSchema / 10;

  /** Policy of DataNodeSchemaCache eviction */
  private String dataNodeSchemaCacheEvictionPolicy = "FIFO";

  private String readConsistencyLevel = "strong";

  /** Maximum execution time of a DriverTask */
  private int driverTaskExecutionTimeSliceInMs = 200;

  /** Maximum size of wal buffer used in IoTConsensus. Unit: byte */
  private long throttleThreshold = 50 * 1024 * 1024 * 1024L;

  /** Maximum wait time of write cache in IoTConsensus. Unit: ms */
  private long cacheWindowTimeInMs = 10 * 1000L;

  private long dataRatisConsensusLogAppenderBufferSizeMax = 16 * 1024 * 1024L;
  private long schemaRatisConsensusLogAppenderBufferSizeMax = 16 * 1024 * 1024L;

  private long dataRatisConsensusSnapshotTriggerThreshold = 400000L;
  private long schemaRatisConsensusSnapshotTriggerThreshold = 400000L;

  private boolean dataRatisConsensusLogUnsafeFlushEnable = false;
  private boolean schemaRatisConsensusLogUnsafeFlushEnable = false;

  private int dataRatisConsensusLogForceSyncNum = 128;
  private int schemaRatisConsensusLogForceSyncNum = 128;

  private long dataRatisConsensusLogSegmentSizeMax = 24 * 1024 * 1024L;
  private long schemaRatisConsensusLogSegmentSizeMax = 24 * 1024 * 1024L;

  private long dataRatisConsensusGrpcFlowControlWindow = 4 * 1024 * 1024L;
  private long schemaRatisConsensusGrpcFlowControlWindow = 4 * 1024 * 1024L;

  private int dataRatisConsensusGrpcLeaderOutstandingAppendsMax = 128;

  private int schemaRatisConsensusGrpcLeaderOutstandingAppendsMax = 128;

  private long dataRatisConsensusLeaderElectionTimeoutMinMs = 2000L;
  private long schemaRatisConsensusLeaderElectionTimeoutMinMs = 2000L;

  private long dataRatisConsensusLeaderElectionTimeoutMaxMs = 4000L;
  private long schemaRatisConsensusLeaderElectionTimeoutMaxMs = 4000L;

  /** CQ related */
  private long cqMinEveryIntervalInMs = 1_000;

  private long dataRatisConsensusRequestTimeoutMs = 10000L;
  private long schemaRatisConsensusRequestTimeoutMs = 10000L;

  private int dataRatisConsensusMaxRetryAttempts = 10;
  private int schemaRatisConsensusMaxRetryAttempts = 10;
  private long dataRatisConsensusInitialSleepTimeMs = 100L;
  private long schemaRatisConsensusInitialSleepTimeMs = 100L;
  private long dataRatisConsensusMaxSleepTimeMs = 10000L;
  private long schemaRatisConsensusMaxSleepTimeMs = 10000L;

  private long dataRatisConsensusPreserveWhenPurge = 1000L;
  private long schemaRatisConsensusPreserveWhenPurge = 1000L;

  private long ratisFirstElectionTimeoutMinMs = 50L;
  private long ratisFirstElectionTimeoutMaxMs = 150L;

  private long dataRatisLogMax = 20L * 1024 * 1024 * 1024; // 20G
  private long schemaRatisLogMax = 2L * 1024 * 1024 * 1024; // 2G

  private long dataRatisPeriodicSnapshotInterval = 24L * 60 * 60; // 24hr
  private long schemaRatisPeriodicSnapshotInterval = 24L * 60 * 60; // 24hr

  /** whether to enable the audit log * */
  private boolean enableAuditLog = false;

  /** Output location of audit logs * */
  private List<AuditLogStorage> auditLogStorage =
      Arrays.asList(AuditLogStorage.IOTDB, AuditLogStorage.LOGGER);

  /** Indicates the category collection of audit logs * */
  private List<AuditLogOperation> auditLogOperation =
      Arrays.asList(AuditLogOperation.DML, AuditLogOperation.DDL, AuditLogOperation.QUERY);

  /** whether the local write api records audit logs * */
  private boolean enableAuditLogForNativeInsertApi = true;

  // customizedProperties, this should be empty by default.
  private Properties customizedProperties = new Properties();

  // IoTConsensus Config
  private int maxLogEntriesNumPerBatch = 1024;
  private int maxSizePerBatch = 16 * 1024 * 1024;
  private int maxPendingBatchesNum = 5;
  private double maxMemoryRatioForQueue = 0.6;
  private long regionMigrationSpeedLimitBytesPerSecond = 32 * 1024 * 1024L;

  // PipeConsensus Config
  private int pipeConsensusPipelineSize = 5;

  /** Load related */
  private double maxAllocateMemoryRatioForLoad = 0.8;

  private int loadTsFileAnalyzeSchemaBatchFlushTimeSeriesNumber = 4096;
  private long loadTsFileAnalyzeSchemaMemorySizeInBytes =
      0L; // 0 means that the decision will be adaptive based on the number of sequences

  private int loadTsFileMaxDeviceCountToUseDeviceTimeIndex = 10000;

  private long loadMemoryAllocateRetryIntervalMs = 1000L;
  private int loadMemoryAllocateMaxRetries = 5;

  private long loadCleanupTaskExecutionDelayTimeSeconds = 1800L; // 30 min

  private double loadWriteThroughputBytesPerSecond = -1; // Bytes/s

  private boolean loadActiveListeningEnable = true;

  private String[] loadActiveListeningDirs =
      new String[] {
        IoTDBConstant.EXT_FOLDER_NAME
            + File.separator
            + IoTDBConstant.LOAD_TSFILE_FOLDER_NAME
            + File.separator
            + IoTDBConstant.LOAD_TSFILE_ACTIVE_LISTENING_PENDING_FOLDER_NAME
      };

  private String loadActiveListeningPipeDir =
      IoTDBConstant.EXT_FOLDER_NAME
          + File.separator
          + IoTDBConstant.LOAD_TSFILE_FOLDER_NAME
          + File.separator
          + IoTDBConstant.PIPE_FOLDER_NAME;

  private String loadActiveListeningFailDir =
      IoTDBConstant.EXT_FOLDER_NAME
          + File.separator
          + IoTDBConstant.LOAD_TSFILE_FOLDER_NAME
          + File.separator
          + IoTDBConstant.LOAD_TSFILE_ACTIVE_LISTENING_FAILED_FOLDER_NAME;

  private long loadActiveListeningCheckIntervalSeconds = 5L;

  private int loadActiveListeningMaxThreadNum = 8;

  /** Pipe related */
  /** initialized as empty, updated based on the latest `systemDir` during querying */
  private String[] pipeReceiverFileDirs = new String[0];

  private String[] pipeConsensusReceiverFileDirs = new String[0];

  /** Resource control */
  private boolean quotaEnable = false;

  /**
   * 1. FixedIntervalRateLimiter : With this limiter resources will be refilled only after a fixed
   * interval of time. 2. AverageIntervalRateLimiter : This limiter will refill resources at every
   * TimeUnit/resources interval.
   */
  private String RateLimiterType = "FixedIntervalRateLimiter";

  private CompressionType WALCompressionAlgorithm = CompressionType.LZ4;

  IoTDBConfig() {}

  public int getMaxLogEntriesNumPerBatch() {
    return maxLogEntriesNumPerBatch;
  }

  public int getMaxSizePerBatch() {
    return maxSizePerBatch;
  }

  public int getMaxPendingBatchesNum() {
    return maxPendingBatchesNum;
  }

  public double getMaxMemoryRatioForQueue() {
    return maxMemoryRatioForQueue;
  }

  public void setMaxLogEntriesNumPerBatch(int maxLogEntriesNumPerBatch) {
    this.maxLogEntriesNumPerBatch = maxLogEntriesNumPerBatch;
  }

  public long getRegionMigrationSpeedLimitBytesPerSecond() {
    return regionMigrationSpeedLimitBytesPerSecond;
  }

  public void setRegionMigrationSpeedLimitBytesPerSecond(
      long regionMigrationSpeedLimitBytesPerSecond) {
    this.regionMigrationSpeedLimitBytesPerSecond = regionMigrationSpeedLimitBytesPerSecond;
  }

  public int getPipeConsensusPipelineSize() {
    return pipeConsensusPipelineSize;
  }

  public void setPipeConsensusPipelineSize(int pipeConsensusPipelineSize) {
    this.pipeConsensusPipelineSize = pipeConsensusPipelineSize;
  }

  public void setMaxSizePerBatch(int maxSizePerBatch) {
    this.maxSizePerBatch = maxSizePerBatch;
  }

  public void setMaxPendingBatchesNum(int maxPendingBatchesNum) {
    this.maxPendingBatchesNum = maxPendingBatchesNum;
  }

  public void setMaxMemoryRatioForQueue(double maxMemoryRatioForQueue) {
    this.maxMemoryRatioForQueue = maxMemoryRatioForQueue;
  }

  public float getUdfMemoryBudgetInMB() {
    return udfMemoryBudgetInMB;
  }

  public void setUdfMemoryBudgetInMB(float udfMemoryBudgetInMB) {
    this.udfMemoryBudgetInMB = udfMemoryBudgetInMB;
  }

  public float getUdfReaderMemoryBudgetInMB() {
    return udfReaderMemoryBudgetInMB;
  }

  public void setUdfReaderMemoryBudgetInMB(float udfReaderMemoryBudgetInMB) {
    this.udfReaderMemoryBudgetInMB = udfReaderMemoryBudgetInMB;
  }

  public float getUdfTransformerMemoryBudgetInMB() {
    return udfTransformerMemoryBudgetInMB;
  }

  public void setUdfTransformerMemoryBudgetInMB(float udfTransformerMemoryBudgetInMB) {
    this.udfTransformerMemoryBudgetInMB = udfTransformerMemoryBudgetInMB;
  }

  public float getUdfCollectorMemoryBudgetInMB() {
    return udfCollectorMemoryBudgetInMB;
  }

  public void setUdfCollectorMemoryBudgetInMB(float udfCollectorMemoryBudgetInMB) {
    this.udfCollectorMemoryBudgetInMB = udfCollectorMemoryBudgetInMB;
  }

  public boolean isEnableSSL() {
    return enableSSL;
  }

  public void setEnableSSL(boolean enableSSL) {
    this.enableSSL = enableSSL;
  }

  public String getKeyStorePath() {
    return keyStorePath;
  }

  public void setKeyStorePath(String keyStorePath) {
    this.keyStorePath = keyStorePath;
  }

  public String getKeyStorePwd() {
    return keyStorePwd;
  }

  public void setKeyStorePwd(String keyStorePwd) {
    this.keyStorePwd = keyStorePwd;
  }

  public int getUdfInitialByteArrayLengthForMemoryControl() {
    return udfInitialByteArrayLengthForMemoryControl;
  }

  public void setUdfInitialByteArrayLengthForMemoryControl(
      int udfInitialByteArrayLengthForMemoryControl) {
    this.udfInitialByteArrayLengthForMemoryControl = udfInitialByteArrayLengthForMemoryControl;
  }

  public int getDefaultFillInterval() {
    return defaultFillInterval;
  }

  public void setDefaultFillInterval(int defaultFillInterval) {
    this.defaultFillInterval = defaultFillInterval;
  }

  public TimeIndexLevel getTimeIndexLevel() {
    return timeIndexLevel;
  }

  public void setTimeIndexLevel(String timeIndexLevel) {
    this.timeIndexLevel = TimeIndexLevel.valueOf(timeIndexLevel);
  }

  public void updatePath() {
    formulateFolders();
    confirmMultiDirStrategy();
  }

  /** if the folders are relative paths, add IOTDB_DATA_HOME as the path prefix */
  private void formulateFolders() {
    systemDir = addDataHomeDir(systemDir);
    schemaDir = addDataHomeDir(schemaDir);
    consensusDir = addDataHomeDir(consensusDir);
    dataRegionConsensusDir = addDataHomeDir(dataRegionConsensusDir);
    ratisDataRegionSnapshotDir = addDataHomeDir(ratisDataRegionSnapshotDir);
    schemaRegionConsensusDir = addDataHomeDir(schemaRegionConsensusDir);
    indexRootFolder = addDataHomeDir(indexRootFolder);
    extDir = addDataHomeDir(extDir);
    for (int i = 0; i < loadActiveListeningDirs.length; i++) {
      loadActiveListeningDirs[i] = addDataHomeDir(loadActiveListeningDirs[i]);
    }
    loadActiveListeningPipeDir = addDataHomeDir(loadActiveListeningPipeDir);
    loadActiveListeningFailDir = addDataHomeDir(loadActiveListeningFailDir);
    udfDir = addDataHomeDir(udfDir);
    udfTemporaryLibDir = addDataHomeDir(udfTemporaryLibDir);
    triggerDir = addDataHomeDir(triggerDir);
    triggerTemporaryLibDir = addDataHomeDir(triggerTemporaryLibDir);
    pipeDir = addDataHomeDir(pipeDir);
    pipeTemporaryLibDir = addDataHomeDir(pipeTemporaryLibDir);
    for (int i = 0; i < pipeReceiverFileDirs.length; i++) {
      pipeReceiverFileDirs[i] = addDataHomeDir(pipeReceiverFileDirs[i]);
    }
    for (int i = 0; i < pipeConsensusReceiverFileDirs.length; i++) {
      pipeConsensusReceiverFileDirs[i] = addDataHomeDir(pipeConsensusReceiverFileDirs[i]);
    }
    mqttDir = addDataHomeDir(mqttDir);
    extPipeDir = addDataHomeDir(extPipeDir);
    queryDir = addDataHomeDir(queryDir);
    sortTmpDir = addDataHomeDir(sortTmpDir);
    formulateDataDirs(tierDataDirs);
  }

  private void formulateDataDirs(String[][] tierDataDirs) {
    for (int i = 0; i < tierDataDirs.length; i++) {
      for (int j = 0; j < tierDataDirs[i].length; j++) {
        if (tierDataDirs[i][j].equals(OBJECT_STORAGE_DIR)) {
          // Notice: dataNodeId hasn't been initialized
          tierDataDirs[i][j] = FSUtils.getOSDefaultPath(getObjectStorageBucket(), dataNodeId);
        }
        switch (FSUtils.getFSType(tierDataDirs[i][j])) {
          case HDFS:
            tierDataDirs[i][j] = getHdfsDir() + File.separatorChar + tierDataDirs[i][j];
            break;
          case LOCAL:
            tierDataDirs[i][j] = addDataHomeDir(tierDataDirs[i][j]);
            break;
          case OBJECT_STORAGE:
            tierDataDirs[i][j] = FSUtils.getOSDefaultPath(getObjectStorageBucket(), dataNodeId);
            break;
          default:
            break;
        }
      }
    }
    formulateLoadTsFileDirs(tierDataDirs);
  }

  void reloadDataDirs(String[][] newTierDataDirs) throws LoadConfigurationException {
    // format data directories
    formulateDataDirs(newTierDataDirs);
    if (newTierDataDirs.length < this.tierDataDirs.length) {
      String msg = "some data dirs are removed from data_dirs parameter, please add them back.";
      logger.error(msg);
      throw new LoadConfigurationException(msg);
    }
    // make sure old data directories not removed
    for (int i = 0; i < this.tierDataDirs.length; ++i) {
      List<String> newDirs = Arrays.asList(newTierDataDirs[i]);
      for (String oldDir : this.tierDataDirs[i]) {
        if (newDirs.stream()
            .noneMatch(
                newDir ->
                    Objects.equals(
                        new File(newDir).getAbsolutePath(), new File(oldDir).getAbsolutePath()))) {
          String msg =
              String.format("%s is removed from data_dirs parameter, please add it back.", oldDir);
          logger.error(msg);
          throw new LoadConfigurationException(msg);
        }
      }
    }
    this.tierDataDirs = newTierDataDirs;
    reloadSystemMetrics();
  }

  void reloadSystemMetrics() {
    ArrayList<String> diskDirs = new ArrayList<>();
    diskDirs.add(IoTDBDescriptor.getInstance().getConfig().getSystemDir());
    diskDirs.add(IoTDBDescriptor.getInstance().getConfig().getConsensusDir());
    diskDirs.addAll(Arrays.asList(IoTDBDescriptor.getInstance().getConfig().getDataDirs()));
    diskDirs.addAll(Arrays.asList(CommonDescriptor.getInstance().getConfig().getWalDirs()));
    diskDirs.add(CommonDescriptor.getInstance().getConfig().getSyncDir());
    diskDirs.add(IoTDBDescriptor.getInstance().getConfig().getSortTmpDir());
    SystemMetrics.getInstance().setDiskDirs(diskDirs);
  }

  // if IOTDB_DATA_HOME is not set, then we keep dataHomeDir prefix being the same with IOTDB_HOME
  // In this way, we can keep consistent with v0.13.0~2.
  private String addDataHomeDir(String dir) {
    String dataHomeDir = System.getProperty(IoTDBConstant.IOTDB_DATA_HOME, null);
    if (dataHomeDir == null) {
      dataHomeDir = System.getProperty(IoTDBConstant.IOTDB_HOME, null);
    }
    if (dataHomeDir == null) {
      return dir;
    }

    File dataHomeFile = new File(dataHomeDir);
    try {
      dataHomeDir = dataHomeFile.getCanonicalPath();
    } catch (IOException e) {
      logger.error("Fail to get canonical path of {}", dataHomeFile, e);
    }
    return FileUtils.addPrefix2FilePath(dataHomeDir, dir);
  }

  void confirmMultiDirStrategy() {
    if (getMultiDirStrategyClassName() == null) {
      multiDirStrategyClassName = DEFAULT_MULTI_DIR_STRATEGY;
    }
    if (!getMultiDirStrategyClassName().contains(TsFileConstant.PATH_SEPARATOR)) {
      multiDirStrategyClassName = MULTI_DIR_STRATEGY_PREFIX + multiDirStrategyClassName;
    }

    try {
      Class.forName(multiDirStrategyClassName);
    } catch (ClassNotFoundException e) {
      logger.warn(
          "Cannot find given directory strategy {}, using the default value",
          getMultiDirStrategyClassName(),
          e);
      setMultiDirStrategyClassName(MULTI_DIR_STRATEGY_PREFIX + DEFAULT_MULTI_DIR_STRATEGY);
    }
  }

  private String getHdfsDir() {
    String[] hdfsIps = TSFileDescriptor.getInstance().getConfig().getHdfsIp();
    String hdfsDir = "hdfs://";
    if (hdfsIps.length > 1) {
      hdfsDir += TSFileDescriptor.getInstance().getConfig().getDfsNameServices();
    } else {
      hdfsDir += hdfsIps[0] + ":" + TSFileDescriptor.getInstance().getConfig().getHdfsPort();
    }
    return hdfsDir;
  }

  public String[] getDataDirs() {
    return Arrays.stream(tierDataDirs).flatMap(Arrays::stream).toArray(String[]::new);
  }

  public String[] getLocalDataDirs() {
    return Arrays.stream(tierDataDirs)
        .flatMap(Arrays::stream)
        .filter(FSUtils::isLocal)
        .toArray(String[]::new);
  }

  public String[][] getTierDataDirs() {
    return tierDataDirs;
  }

  @SuppressWarnings("javabugs:S6466")
  public void setTierDataDirs(String[][] tierDataDirs) {
    formulateDataDirs(tierDataDirs);
    this.tierDataDirs = tierDataDirs;
    // TODO(szywilliam): rewrite the logic here when ratis supports complete snapshot semantic
    setRatisDataRegionSnapshotDir(
        tierDataDirs[0][0] + File.separator + IoTDBConstant.SNAPSHOT_FOLDER_NAME);
  }

  public String getRpcAddress() {
    return rpcAddress;
  }

  public void setRpcAddress(String rpcAddress) {
    this.rpcAddress = rpcAddress;
  }

  public int getRpcPort() {
    return rpcPort;
  }

  public void setRpcPort(int rpcPort) {
    this.rpcPort = rpcPort;
  }

  public boolean isEnableSeparateData() {
    return enableSeparateData;
  }

  public void setEnableSeparateData(boolean enableSeparateData) {
    this.enableSeparateData = enableSeparateData;
  }

  public String getSystemDir() {
    return systemDir;
  }

  public void setSystemDir(String systemDir) {
    this.systemDir = systemDir;
  }

  public String[] getLoadTsFileDirs() {
    return this.loadTsFileDirs;
  }

  public void formulateLoadTsFileDirs(String[][] tierDataDirs) {
    if (tierDataDirs.length < 1) {
      logger.warn("No data directory is set. loadTsFileDirs is kept as the default value.");
      return;
    }

    final String[] firstTierDataDirs = tierDataDirs[0];
    final String[] newLoadTsFileDirs = new String[firstTierDataDirs.length];
    for (int i = 0; i < firstTierDataDirs.length; i++) {
      newLoadTsFileDirs[i] =
          firstTierDataDirs[i] + File.separator + IoTDBConstant.LOAD_TSFILE_FOLDER_NAME;
    }

    // Update loadTsFileDirs after all newLoadTsFileDirs are generated,
    // or the newLoadTsFileDirs will be used in the middle of the process
    // and cause the undefined behavior.
    this.loadTsFileDirs = newLoadTsFileDirs;
  }

  public String getSchemaDir() {
    return schemaDir;
  }

  public void setSchemaDir(String schemaDir) {
    this.schemaDir = schemaDir;
  }

  public String getQueryDir() {
    return queryDir;
  }

  public void setQueryDir(String queryDir) {
    this.queryDir = queryDir;
  }

  public String getRatisDataRegionSnapshotDir() {
    return ratisDataRegionSnapshotDir;
  }

  public void setRatisDataRegionSnapshotDir(String ratisDataRegionSnapshotDir) {
    this.ratisDataRegionSnapshotDir = ratisDataRegionSnapshotDir;
  }

  public String getConsensusDir() {
    return consensusDir;
  }

  public void setConsensusDir(String consensusDir) {
    this.consensusDir = consensusDir;
    setDataRegionConsensusDir(
        consensusDir + File.separator + IoTDBConstant.DATA_REGION_FOLDER_NAME);
    setSchemaRegionConsensusDir(
        consensusDir + File.separator + IoTDBConstant.SCHEMA_REGION_FOLDER_NAME);
    setInvalidDataRegionConsensusDir(
        consensusDir + File.separator + IoTDBConstant.INVALID_DATA_REGION_FOLDER_NAME);
  }

  public String getDataRegionConsensusDir() {
    return dataRegionConsensusDir;
  }

  public void setDataRegionConsensusDir(String dataRegionConsensusDir) {
    this.dataRegionConsensusDir = dataRegionConsensusDir;
  }

  public String getInvalidDataRegionConsensusDir() {
    return invalidDataRegionConsensusDir;
  }

  public void setInvalidDataRegionConsensusDir(String invalidDataRegionConsensusDir) {
    this.invalidDataRegionConsensusDir = invalidDataRegionConsensusDir;
  }

  public String getSchemaRegionConsensusDir() {
    return schemaRegionConsensusDir;
  }

  public void setSchemaRegionConsensusDir(String schemaRegionConsensusDir) {
    this.schemaRegionConsensusDir = schemaRegionConsensusDir;
  }

  public String getExtDir() {
    return extDir;
  }

  public void setExtDir(String extDir) {
    this.extDir = extDir;
  }

  public String getUdfDir() {
    return udfDir;
  }

  public void setUdfDir(String udfDir) {
    this.udfDir = udfDir;
    updateUdfTemporaryLibDir();
  }

  public String getUdfTemporaryLibDir() {
    return udfTemporaryLibDir;
  }

  public void updateUdfTemporaryLibDir() {
    this.udfTemporaryLibDir = udfDir + File.separator + IoTDBConstant.TMP_FOLDER_NAME;
  }

  public String getTriggerDir() {
    return triggerDir;
  }

  public void setTriggerDir(String triggerDir) {
    this.triggerDir = triggerDir;
    updateTriggerTemporaryLibDir();
  }

  public String getTriggerTemporaryLibDir() {
    return triggerTemporaryLibDir;
  }

  public void updateTriggerTemporaryLibDir() {
    this.triggerTemporaryLibDir = triggerDir + File.separator + IoTDBConstant.TMP_FOLDER_NAME;
  }

  public String getPipeLibDir() {
    return pipeDir;
  }

  public void setPipeLibDir(String pipeDir) {
    this.pipeDir = pipeDir;
    updatePipeTemporaryLibDir();
  }

  public String getPipeTemporaryLibDir() {
    return pipeTemporaryLibDir;
  }

  public void updatePipeTemporaryLibDir() {
    this.pipeTemporaryLibDir = pipeDir + File.separator + IoTDBConstant.TMP_FOLDER_NAME;
  }

  public String getMqttDir() {
    return mqttDir;
  }

  public void setMqttDir(String mqttDir) {
    this.mqttDir = mqttDir;
  }

  public String getMultiDirStrategyClassName() {
    return multiDirStrategyClassName;
  }

  void setMultiDirStrategyClassName(String multiDirStrategyClassName) {
    this.multiDirStrategyClassName = multiDirStrategyClassName;
  }

  public void checkMultiDirStrategyClassName() {
    confirmMultiDirStrategy();
    for (String multiDirStrategy : CLUSTER_ALLOWED_MULTI_DIR_STRATEGIES) {
      // If the multiDirStrategyClassName is one of cluster allowed strategy, the check is passed.
      if (multiDirStrategyClassName.equals(multiDirStrategy)
          || multiDirStrategyClassName.equals(MULTI_DIR_STRATEGY_PREFIX + multiDirStrategy)) {
        return;
      }
    }
    String msg =
        String.format(
            "Cannot set multi_dir_strategy to %s, because cluster mode only allows %s.",
            multiDirStrategyClassName, Arrays.toString(CLUSTER_ALLOWED_MULTI_DIR_STRATEGIES));
    logger.error(msg);
    throw new RuntimeException(msg);
  }

  public int getBatchSize() {
    return batchSize;
  }

  void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  public int getMaxMemtableNumber() {
    return maxMemtableNumber;
  }

  public void setMaxMemtableNumber(int maxMemtableNumber) {
    this.maxMemtableNumber = maxMemtableNumber;
  }

  public int getFlushThreadCount() {
    return flushThreadCount;
  }

  void setFlushThreadCount(int flushThreadCount) {
    this.flushThreadCount = flushThreadCount;
  }

  public int getQueryThreadCount() {
    return queryThreadCount;
  }

  public void setQueryThreadCount(int queryThreadCount) {
    if (queryThreadCount <= 0) {
      queryThreadCount = Runtime.getRuntime().availableProcessors();
    }
    this.queryThreadCount = queryThreadCount;
    this.maxBytesPerFragmentInstance = allocateMemoryForDataExchange / queryThreadCount;
  }

  public void setDegreeOfParallelism(int degreeOfParallelism) {
    if (degreeOfParallelism > 0) {
      this.degreeOfParallelism = degreeOfParallelism;
    }
  }

  public int getDegreeOfParallelism() {
    return degreeOfParallelism;
  }

  public void setMergeThresholdOfExplainAnalyze(int mergeThresholdOfExplainAnalyze) {
    this.mergeThresholdOfExplainAnalyze = mergeThresholdOfExplainAnalyze;
  }

  public int getMergeThresholdOfExplainAnalyze() {
    return mergeThresholdOfExplainAnalyze;
  }

  public int getMaxAllowedConcurrentQueries() {
    return maxAllowedConcurrentQueries;
  }

  public void setMaxAllowedConcurrentQueries(int maxAllowedConcurrentQueries) {
    this.maxAllowedConcurrentQueries = maxAllowedConcurrentQueries;
  }

  public long getMaxBytesPerFragmentInstance() {
    return maxBytesPerFragmentInstance;
  }

  @TestOnly
  public void setMaxBytesPerFragmentInstance(long maxBytesPerFragmentInstance) {
    this.maxBytesPerFragmentInstance = maxBytesPerFragmentInstance;
  }

  public int getWindowEvaluationThreadCount() {
    return windowEvaluationThreadCount;
  }

  public void setWindowEvaluationThreadCount(int windowEvaluationThreadCount) {
    this.windowEvaluationThreadCount = windowEvaluationThreadCount;
  }

  public int getMaxPendingWindowEvaluationTasks() {
    return maxPendingWindowEvaluationTasks;
  }

  public void setMaxPendingWindowEvaluationTasks(int maxPendingWindowEvaluationTasks) {
    this.maxPendingWindowEvaluationTasks = maxPendingWindowEvaluationTasks;
  }

  public long getSeqTsFileSize() {
    return seqTsFileSize;
  }

  public void setSeqTsFileSize(long seqTsFileSize) {
    this.seqTsFileSize = seqTsFileSize;
  }

  public long getUnSeqTsFileSize() {
    return unSeqTsFileSize;
  }

  public void setUnSeqTsFileSize(long unSeqTsFileSize) {
    this.unSeqTsFileSize = unSeqTsFileSize;
  }

  public int getRpcSelectorThreadCount() {
    return rpcSelectorThreadCount;
  }

  public void setRpcSelectorThreadCount(int rpcSelectorThreadCount) {
    this.rpcSelectorThreadCount = rpcSelectorThreadCount;
  }

  public int getRpcMinConcurrentClientNum() {
    return rpcMinConcurrentClientNum;
  }

  public void setRpcMinConcurrentClientNum(int rpcMinConcurrentClientNum) {
    this.rpcMinConcurrentClientNum = rpcMinConcurrentClientNum;
  }

  public int getRpcMaxConcurrentClientNum() {
    return rpcMaxConcurrentClientNum;
  }

  void setRpcMaxConcurrentClientNum(int rpcMaxConcurrentClientNum) {
    this.rpcMaxConcurrentClientNum = rpcMaxConcurrentClientNum;
  }

  public int getmRemoteSchemaCacheSize() {
    return mRemoteSchemaCacheSize;
  }

  public void setmRemoteSchemaCacheSize(int mRemoteSchemaCacheSize) {
    this.mRemoteSchemaCacheSize = mRemoteSchemaCacheSize;
  }

  String getLanguageVersion() {
    return languageVersion;
  }

  void setLanguageVersion(String languageVersion) {
    this.languageVersion = languageVersion;
  }

  public String getIoTDBVersion() {
    return IoTDBConstant.VERSION;
  }

  public String getIoTDBMajorVersion() {
    return IoTDBConstant.MAJOR_VERSION;
  }

  public String getIoTDBMajorVersion(String version) {
    return "UNKNOWN".equals(version)
        ? "UNKNOWN"
        : version.split("\\.")[0] + "." + version.split("\\.")[1];
  }

  public long getCacheFileReaderClearPeriod() {
    return cacheFileReaderClearPeriod;
  }

  public void setCacheFileReaderClearPeriod(long cacheFileReaderClearPeriod) {
    this.cacheFileReaderClearPeriod = cacheFileReaderClearPeriod;
  }

  public long getQueryTimeoutThreshold() {
    return queryTimeoutThreshold;
  }

  public void setQueryTimeoutThreshold(long queryTimeoutThreshold) {
    this.queryTimeoutThreshold = queryTimeoutThreshold;
  }

  public int getSessionTimeoutThreshold() {
    return sessionTimeoutThreshold;
  }

  public void setSessionTimeoutThreshold(int sessionTimeoutThreshold) {
    this.sessionTimeoutThreshold = sessionTimeoutThreshold;
  }

  public String getRpcImplClassName() {
    return rpcImplClassName;
  }

  public void setRpcImplClassName(String rpcImplClassName) {
    this.rpcImplClassName = rpcImplClassName;
  }

  public WALMode getWalMode() {
    return walMode;
  }

  public void setWalMode(WALMode walMode) {
    this.walMode = walMode;
  }

  public int getMaxWalNodesNum() {
    return maxWalNodesNum;
  }

  void setMaxWalNodesNum(int maxWalNodesNum) {
    this.maxWalNodesNum = maxWalNodesNum;
  }

  public long getWalAsyncModeFsyncDelayInMs() {
    return walAsyncModeFsyncDelayInMs;
  }

  void setWalAsyncModeFsyncDelayInMs(long walAsyncModeFsyncDelayInMs) {
    this.walAsyncModeFsyncDelayInMs = walAsyncModeFsyncDelayInMs;
  }

  public long getWalSyncModeFsyncDelayInMs() {
    return walSyncModeFsyncDelayInMs;
  }

  public void setWalSyncModeFsyncDelayInMs(long walSyncModeFsyncDelayInMs) {
    this.walSyncModeFsyncDelayInMs = walSyncModeFsyncDelayInMs;
  }

  public int getWalBufferSize() {
    return walBufferSize;
  }

  public void setWalBufferSize(int walBufferSize) {
    this.walBufferSize = walBufferSize;
  }

  public double getMaxDirectBufferOffHeapMemorySizeProportion() {
    return maxDirectBufferOffHeapMemorySizeProportion;
  }

  public void setMaxDirectBufferOffHeapMemorySizeProportion(
      double maxDirectBufferOffHeapMemorySizeProportion) {
    this.maxDirectBufferOffHeapMemorySizeProportion = maxDirectBufferOffHeapMemorySizeProportion;
  }

  public int getWalBufferQueueCapacity() {
    return walBufferQueueCapacity;
  }

  void setWalBufferQueueCapacity(int walBufferQueueCapacity) {
    this.walBufferQueueCapacity = walBufferQueueCapacity;
  }

  public long getWalFileSizeThresholdInByte() {
    return walFileSizeThresholdInByte;
  }

  public void setWalFileSizeThresholdInByte(long walFileSizeThresholdInByte) {
    this.walFileSizeThresholdInByte = walFileSizeThresholdInByte;
  }

  public long getCheckpointFileSizeThresholdInByte() {
    return checkpointFileSizeThresholdInByte;
  }

  public void setCheckpointFileSizeThresholdInByte(long checkpointFileSizeThresholdInByte) {
    this.checkpointFileSizeThresholdInByte = checkpointFileSizeThresholdInByte;
  }

  public double getWalMinEffectiveInfoRatio() {
    return walMinEffectiveInfoRatio;
  }

  void setWalMinEffectiveInfoRatio(double walMinEffectiveInfoRatio) {
    this.walMinEffectiveInfoRatio = walMinEffectiveInfoRatio;
  }

  public long getWalMemTableSnapshotThreshold() {
    return walMemTableSnapshotThreshold;
  }

  void setWalMemTableSnapshotThreshold(long walMemTableSnapshotThreshold) {
    this.walMemTableSnapshotThreshold = walMemTableSnapshotThreshold;
  }

  public int getMaxWalMemTableSnapshotNum() {
    return maxWalMemTableSnapshotNum;
  }

  void setMaxWalMemTableSnapshotNum(int maxWalMemTableSnapshotNum) {
    this.maxWalMemTableSnapshotNum = maxWalMemTableSnapshotNum;
  }

  public long getDeleteWalFilesPeriodInMs() {
    return deleteWalFilesPeriodInMs;
  }

  void setDeleteWalFilesPeriodInMs(long deleteWalFilesPeriodInMs) {
    this.deleteWalFilesPeriodInMs = deleteWalFilesPeriodInMs;
  }

  public boolean getWALCacheShrinkClearEnabled() {
    return WALCacheShrinkClearEnabled;
  }

  void setWALCacheShrinkClearEnabled(boolean WALCacheShrinkClearEnabled) {
    this.WALCacheShrinkClearEnabled = WALCacheShrinkClearEnabled;
  }

  public boolean isChunkBufferPoolEnable() {
    return chunkBufferPoolEnable;
  }

  void setChunkBufferPoolEnable(boolean chunkBufferPoolEnable) {
    this.chunkBufferPoolEnable = chunkBufferPoolEnable;
  }

  public long getMergeIntervalSec() {
    return mergeIntervalSec;
  }

  void setMergeIntervalSec(long mergeIntervalSec) {
    this.mergeIntervalSec = mergeIntervalSec;
  }

  public double getBufferedArraysMemoryProportion() {
    return bufferedArraysMemoryProportion;
  }

  public void setBufferedArraysMemoryProportion(double bufferedArraysMemoryProportion) {
    this.bufferedArraysMemoryProportion = bufferedArraysMemoryProportion;
  }

  public double getFlushProportion() {
    return flushProportion;
  }

  public void setFlushProportion(double flushProportion) {
    this.flushProportion = flushProportion;
  }

  public double getRejectProportion() {
    return rejectProportion;
  }

  public void setRejectProportion(double rejectProportion) {
    this.rejectProportion = rejectProportion;
  }

  public double getWriteMemoryVariationReportProportion() {
    return writeMemoryVariationReportProportion;
  }

  public void setWriteMemoryVariationReportProportion(double writeMemoryVariationReportProportion) {
    this.writeMemoryVariationReportProportion = writeMemoryVariationReportProportion;
  }

  public long getAllocateMemoryForStorageEngine() {
    return allocateMemoryForStorageEngine;
  }

  public void setAllocateMemoryForStorageEngine(long allocateMemoryForStorageEngine) {
    this.allocateMemoryForStorageEngine = allocateMemoryForStorageEngine;
  }

  public long getAllocateMemoryForSchema() {
    return allocateMemoryForSchema;
  }

  public void setAllocateMemoryForSchema(long allocateMemoryForSchema) {
    this.allocateMemoryForSchema = allocateMemoryForSchema;

    this.allocateMemoryForSchemaRegion = allocateMemoryForSchema * 5 / 10;
    this.allocateMemoryForSchemaCache = allocateMemoryForSchema * 4 / 10;
    this.allocateMemoryForPartitionCache = allocateMemoryForSchema / 10;
  }

  public long getAllocateMemoryForConsensus() {
    return allocateMemoryForConsensus;
  }

  public void setAllocateMemoryForConsensus(long allocateMemoryForConsensus) {
    this.allocateMemoryForConsensus = allocateMemoryForConsensus;
  }

  public long getAllocateMemoryForRead() {
    return allocateMemoryForRead;
  }

  void setAllocateMemoryForRead(long allocateMemoryForRead) {
    this.allocateMemoryForRead = allocateMemoryForRead;

    this.allocateMemoryForBloomFilterCache = allocateMemoryForRead / 1001;
    this.allocateMemoryForTimeSeriesMetaDataCache = allocateMemoryForRead * 200 / 1001;
    this.allocateMemoryForChunkCache = allocateMemoryForRead * 100 / 1001;
    this.allocateMemoryForCoordinator = allocateMemoryForRead * 50 / 1001;
    this.allocateMemoryForOperators = allocateMemoryForRead * 200 / 1001;
    this.allocateMemoryForDataExchange = allocateMemoryForRead * 200 / 1001;
    this.maxBytesPerFragmentInstance = allocateMemoryForDataExchange / queryThreadCount;
    this.allocateMemoryForTimeIndex = allocateMemoryForRead * 200 / 1001;
  }

  public long getAllocateMemoryForPipe() {
    return allocateMemoryForPipe;
  }

  public void setAllocateMemoryForPipe(long allocateMemoryForPipe) {
    this.allocateMemoryForPipe = allocateMemoryForPipe;
  }

  public long getAllocateMemoryForFree() {
    return Runtime.getRuntime().maxMemory()
        - allocateMemoryForStorageEngine
        - allocateMemoryForRead
        - allocateMemoryForSchema;
  }

  public boolean isEnablePartialInsert() {
    return enablePartialInsert;
  }

  public void setEnablePartialInsert(boolean enablePartialInsert) {
    this.enablePartialInsert = enablePartialInsert;
  }

  public boolean isEnable13DataInsertAdapt() {
    return enable13DataInsertAdapt;
  }

  public void setEnable13DataInsertAdapt(boolean enable13DataInsertAdapt) {
    this.enable13DataInsertAdapt = enable13DataInsertAdapt;
  }

  public int getCompactionThreadCount() {
    return compactionThreadCount;
  }

  public void setCompactionThreadCount(int compactionThreadCount) {
    this.compactionThreadCount = compactionThreadCount;
  }

  public int getCompactionMaxAlignedSeriesNumInOneBatch() {
    return compactionMaxAlignedSeriesNumInOneBatch;
  }

  public void setCompactionMaxAlignedSeriesNumInOneBatch(
      int compactionMaxAlignedSeriesNumInOneBatch) {
    this.compactionMaxAlignedSeriesNumInOneBatch = compactionMaxAlignedSeriesNumInOneBatch;
  }

  public int getContinuousQueryThreadNum() {
    return continuousQueryThreadNum;
  }

  public void setContinuousQueryThreadNum(int continuousQueryThreadNum) {
    this.continuousQueryThreadNum = continuousQueryThreadNum;
  }

  public long getContinuousQueryMinimumEveryInterval() {
    return continuousQueryMinimumEveryInterval;
  }

  public void setContinuousQueryMinimumEveryInterval(long minimumEveryInterval) {
    this.continuousQueryMinimumEveryInterval = minimumEveryInterval;
  }

  public long getIntoOperationBufferSizeInByte() {
    return intoOperationBufferSizeInByte;
  }

  public void setIntoOperationBufferSizeInByte(long intoOperationBufferSizeInByte) {
    this.intoOperationBufferSizeInByte = intoOperationBufferSizeInByte;
  }

  public int getSelectIntoInsertTabletPlanRowLimit() {
    return selectIntoInsertTabletPlanRowLimit;
  }

  public void setSelectIntoInsertTabletPlanRowLimit(int selectIntoInsertTabletPlanRowLimit) {
    this.selectIntoInsertTabletPlanRowLimit = selectIntoInsertTabletPlanRowLimit;
  }

  public int getIntoOperationExecutionThreadCount() {
    return intoOperationExecutionThreadCount;
  }

  public void setIntoOperationExecutionThreadCount(int intoOperationExecutionThreadCount) {
    this.intoOperationExecutionThreadCount = intoOperationExecutionThreadCount;
  }

  public int getCompactionWriteThroughputMbPerSec() {
    return compactionWriteThroughputMbPerSec;
  }

  public void setCompactionWriteThroughputMbPerSec(int compactionWriteThroughputMbPerSec) {
    this.compactionWriteThroughputMbPerSec = compactionWriteThroughputMbPerSec;
  }

  public int getCompactionReadThroughputMbPerSec() {
    return compactionReadThroughputMbPerSec;
  }

  public void setCompactionReadThroughputMbPerSec(int compactionReadThroughputMbPerSec) {
    this.compactionReadThroughputMbPerSec = compactionReadThroughputMbPerSec;
  }

  public int getCompactionReadOperationPerSec() {
    return compactionReadOperationPerSec;
  }

  public void setCompactionReadOperationPerSec(int compactionReadOperationPerSec) {
    this.compactionReadOperationPerSec = compactionReadOperationPerSec;
  }

  public boolean isEnableTimedFlushSeqMemtable() {
    return enableTimedFlushSeqMemtable;
  }

  public void setEnableTimedFlushSeqMemtable(boolean enableTimedFlushSeqMemtable) {
    this.enableTimedFlushSeqMemtable = enableTimedFlushSeqMemtable;
  }

  public long getSeqMemtableFlushInterval() {
    return seqMemtableFlushInterval;
  }

  public void setSeqMemtableFlushInterval(long seqMemtableFlushInterval) {
    this.seqMemtableFlushInterval = seqMemtableFlushInterval;
  }

  public long getSeqMemtableFlushCheckInterval() {
    return seqMemtableFlushCheckInterval;
  }

  public void setSeqMemtableFlushCheckInterval(long seqMemtableFlushCheckInterval) {
    this.seqMemtableFlushCheckInterval = seqMemtableFlushCheckInterval;
  }

  public boolean isEnableTimedFlushUnseqMemtable() {
    return enableTimedFlushUnseqMemtable;
  }

  public void setEnableTimedFlushUnseqMemtable(boolean enableTimedFlushUnseqMemtable) {
    this.enableTimedFlushUnseqMemtable = enableTimedFlushUnseqMemtable;
  }

  public long getUnseqMemtableFlushInterval() {
    return unseqMemtableFlushInterval;
  }

  public void setUnseqMemtableFlushInterval(long unseqMemtableFlushInterval) {
    this.unseqMemtableFlushInterval = unseqMemtableFlushInterval;
  }

  public long getUnseqMemtableFlushCheckInterval() {
    return unseqMemtableFlushCheckInterval;
  }

  public void setUnseqMemtableFlushCheckInterval(long unseqMemtableFlushCheckInterval) {
    this.unseqMemtableFlushCheckInterval = unseqMemtableFlushCheckInterval;
  }

  public TVListSortAlgorithm getTvListSortAlgorithm() {
    return tvListSortAlgorithm;
  }

  public void setTvListSortAlgorithm(TVListSortAlgorithm tvListSortAlgorithm) {
    this.tvListSortAlgorithm = tvListSortAlgorithm;
  }

  public int getAvgSeriesPointNumberThreshold() {
    return avgSeriesPointNumberThreshold;
  }

  public void setAvgSeriesPointNumberThreshold(int avgSeriesPointNumberThreshold) {
    this.avgSeriesPointNumberThreshold = avgSeriesPointNumberThreshold;
  }

  public boolean isRpcThriftCompressionEnable() {
    return rpcThriftCompressionEnable;
  }

  public void setRpcThriftCompressionEnable(boolean rpcThriftCompressionEnable) {
    this.rpcThriftCompressionEnable = rpcThriftCompressionEnable;
  }

  public boolean isMetaDataCacheEnable() {
    return metaDataCacheEnable;
  }

  public void setMetaDataCacheEnable(boolean metaDataCacheEnable) {
    this.metaDataCacheEnable = metaDataCacheEnable;
  }

  public long getAllocateMemoryForBloomFilterCache() {
    return allocateMemoryForBloomFilterCache;
  }

  public void setAllocateMemoryForBloomFilterCache(long allocateMemoryForBloomFilterCache) {
    this.allocateMemoryForBloomFilterCache = allocateMemoryForBloomFilterCache;
  }

  public long getAllocateMemoryForTimeSeriesMetaDataCache() {
    return allocateMemoryForTimeSeriesMetaDataCache;
  }

  public void setAllocateMemoryForTimeSeriesMetaDataCache(
      long allocateMemoryForTimeSeriesMetaDataCache) {
    this.allocateMemoryForTimeSeriesMetaDataCache = allocateMemoryForTimeSeriesMetaDataCache;
  }

  public long getAllocateMemoryForChunkCache() {
    return allocateMemoryForChunkCache;
  }

  public void setAllocateMemoryForChunkCache(long allocateMemoryForChunkCache) {
    this.allocateMemoryForChunkCache = allocateMemoryForChunkCache;
  }

  public long getAllocateMemoryForCoordinator() {
    return allocateMemoryForCoordinator;
  }

  public void setAllocateMemoryForCoordinator(long allocateMemoryForCoordinator) {
    this.allocateMemoryForCoordinator = allocateMemoryForCoordinator;
  }

  public long getAllocateMemoryForOperators() {
    return allocateMemoryForOperators;
  }

  public void setAllocateMemoryForOperators(long allocateMemoryForOperators) {
    this.allocateMemoryForOperators = allocateMemoryForOperators;
  }

  public long getAllocateMemoryForDataExchange() {
    return allocateMemoryForDataExchange;
  }

  public void setAllocateMemoryForDataExchange(long allocateMemoryForDataExchange) {
    this.allocateMemoryForDataExchange = allocateMemoryForDataExchange;
    this.maxBytesPerFragmentInstance = allocateMemoryForDataExchange / queryThreadCount;
  }

  public long getAllocateMemoryForTimeIndex() {
    return allocateMemoryForTimeIndex;
  }

  public void setAllocateMemoryForTimeIndex(long allocateMemoryForTimeIndex) {
    this.allocateMemoryForTimeIndex = allocateMemoryForTimeIndex;
  }

  public long getAllocateMemoryForTimePartitionInfo() {
    return allocateMemoryForTimePartitionInfo;
  }

  public void setAllocateMemoryForTimePartitionInfo(long allocateMemoryForTimePartitionInfo) {
    this.allocateMemoryForTimePartitionInfo = allocateMemoryForTimePartitionInfo;
  }

  public boolean isEnableQueryMemoryEstimation() {
    return enableQueryMemoryEstimation;
  }

  public void setEnableQueryMemoryEstimation(boolean enableQueryMemoryEstimation) {
    this.enableQueryMemoryEstimation = enableQueryMemoryEstimation;
  }

  public boolean isAutoCreateSchemaEnabled() {
    return enableAutoCreateSchema;
  }

  public void setAutoCreateSchemaEnabled(boolean enableAutoCreateSchema) {
    this.enableAutoCreateSchema = enableAutoCreateSchema;
  }

  public TSDataType getBooleanStringInferType() {
    return booleanStringInferType;
  }

  public void setBooleanStringInferType(TSDataType booleanStringInferType) {
    if (booleanStringInferType != TSDataType.BOOLEAN && booleanStringInferType != TSDataType.TEXT) {
      logger.warn(
          "Config Property boolean_string_infer_type can only be BOOLEAN or TEXT but is {}",
          booleanStringInferType);
      return;
    }
    this.booleanStringInferType = booleanStringInferType;
  }

  public TSDataType getIntegerStringInferType() {
    return integerStringInferType;
  }

  public void setIntegerStringInferType(TSDataType integerStringInferType) {
    this.integerStringInferType = integerStringInferType;
  }

  public TSDataType getFloatingStringInferType() {
    return floatingStringInferType;
  }

  public void setFloatingStringInferType(TSDataType floatingNumberStringInferType) {
    if (floatingNumberStringInferType != TSDataType.DOUBLE
        && floatingNumberStringInferType != TSDataType.FLOAT
        && floatingNumberStringInferType != TSDataType.TEXT) {
      logger.warn(
          "Config Property floating_string_infer_type can only be FLOAT, DOUBLE or TEXT but is {}",
          floatingNumberStringInferType);
      return;
    }
    this.floatingStringInferType = floatingNumberStringInferType;
  }

  public TSDataType getNanStringInferType() {
    return nanStringInferType;
  }

  public void setNanStringInferType(TSDataType nanStringInferType) {
    if (nanStringInferType != TSDataType.DOUBLE
        && nanStringInferType != TSDataType.FLOAT
        && nanStringInferType != TSDataType.TEXT) {
      logger.warn(
          "Config Property nan_string_infer_type can only be FLOAT, DOUBLE or TEXT but is {}",
          nanStringInferType);
      return;
    }
    this.nanStringInferType = nanStringInferType;
  }

  public int getDefaultStorageGroupLevel() {
    return defaultStorageGroupLevel;
  }

  void setDefaultStorageGroupLevel(int defaultStorageGroupLevel) {
    this.defaultStorageGroupLevel = defaultStorageGroupLevel;
  }

  public TSEncoding getDefaultBooleanEncoding() {
    return defaultBooleanEncoding;
  }

  public void setDefaultBooleanEncoding(TSEncoding defaultBooleanEncoding) {
    this.defaultBooleanEncoding = defaultBooleanEncoding;
  }

  void setDefaultBooleanEncoding(String defaultBooleanEncoding) {
    this.defaultBooleanEncoding = TSEncoding.valueOf(defaultBooleanEncoding);
  }

  public TSEncoding getDefaultInt32Encoding() {
    return defaultInt32Encoding;
  }

  public void setDefaultInt32Encoding(TSEncoding defaultInt32Encoding) {
    this.defaultInt32Encoding = defaultInt32Encoding;
  }

  void setDefaultInt32Encoding(String defaultInt32Encoding) {
    this.defaultInt32Encoding = TSEncoding.valueOf(defaultInt32Encoding);
  }

  public TSEncoding getDefaultInt64Encoding() {
    return defaultInt64Encoding;
  }

  public void setDefaultInt64Encoding(TSEncoding defaultInt64Encoding) {
    this.defaultInt64Encoding = defaultInt64Encoding;
  }

  void setDefaultInt64Encoding(String defaultInt64Encoding) {
    this.defaultInt64Encoding = TSEncoding.valueOf(defaultInt64Encoding);
  }

  public TSEncoding getDefaultFloatEncoding() {
    return defaultFloatEncoding;
  }

  public void setDefaultFloatEncoding(TSEncoding defaultFloatEncoding) {
    this.defaultFloatEncoding = defaultFloatEncoding;
  }

  void setDefaultFloatEncoding(String defaultFloatEncoding) {
    this.defaultFloatEncoding = TSEncoding.valueOf(defaultFloatEncoding);
  }

  public TSEncoding getDefaultDoubleEncoding() {
    return defaultDoubleEncoding;
  }

  public void setDefaultDoubleEncoding(TSEncoding defaultDoubleEncoding) {
    this.defaultDoubleEncoding = defaultDoubleEncoding;
  }

  void setDefaultDoubleEncoding(String defaultDoubleEncoding) {
    this.defaultDoubleEncoding = TSEncoding.valueOf(defaultDoubleEncoding);
  }

  public TSEncoding getDefaultTextEncoding() {
    return defaultTextEncoding;
  }

  public void setDefaultTextEncoding(TSEncoding defaultTextEncoding) {
    this.defaultTextEncoding = defaultTextEncoding;
  }

  void setDefaultTextEncoding(String defaultTextEncoding) {
    this.defaultTextEncoding = TSEncoding.valueOf(defaultTextEncoding);
  }

  FSType getTsFileStorageFs() {
    return tsFileStorageFs;
  }

  void setTsFileStorageFs(String tsFileStorageFs) {
    this.tsFileStorageFs = FSType.valueOf(tsFileStorageFs);
  }

  public boolean isEnableHDFS() {
    return enableHDFS;
  }

  public void setEnableHDFS(boolean enableHDFS) {
    this.enableHDFS = enableHDFS;
  }

  String getCoreSitePath() {
    return coreSitePath;
  }

  void setCoreSitePath(String coreSitePath) {
    this.coreSitePath = coreSitePath;
  }

  String getHdfsSitePath() {
    return hdfsSitePath;
  }

  void setHdfsSitePath(String hdfsSitePath) {
    this.hdfsSitePath = hdfsSitePath;
  }

  public String[] getHdfsIp() {
    return hdfsIp.split(",");
  }

  String getRawHDFSIp() {
    return hdfsIp;
  }

  void setHdfsIp(String[] hdfsIp) {
    this.hdfsIp = String.join(",", hdfsIp);
  }

  String getHdfsPort() {
    return hdfsPort;
  }

  void setHdfsPort(String hdfsPort) {
    this.hdfsPort = hdfsPort;
  }

  public int getSettleThreadNum() {
    return settleThreadNum;
  }

  String getDfsNameServices() {
    return dfsNameServices;
  }

  void setDfsNameServices(String dfsNameServices) {
    this.dfsNameServices = dfsNameServices;
  }

  public String[] getDfsHaNamenodes() {
    return dfsHaNamenodes.split(",");
  }

  String getRawDfsHaNamenodes() {
    return dfsHaNamenodes;
  }

  void setDfsHaNamenodes(String[] dfsHaNamenodes) {
    this.dfsHaNamenodes = String.join(",", dfsHaNamenodes);
  }

  boolean isDfsHaAutomaticFailoverEnabled() {
    return dfsHaAutomaticFailoverEnabled;
  }

  void setDfsHaAutomaticFailoverEnabled(boolean dfsHaAutomaticFailoverEnabled) {
    this.dfsHaAutomaticFailoverEnabled = dfsHaAutomaticFailoverEnabled;
  }

  String getDfsClientFailoverProxyProvider() {
    return dfsClientFailoverProxyProvider;
  }

  void setDfsClientFailoverProxyProvider(String dfsClientFailoverProxyProvider) {
    this.dfsClientFailoverProxyProvider = dfsClientFailoverProxyProvider;
  }

  boolean isUseKerberos() {
    return useKerberos;
  }

  void setUseKerberos(boolean useKerberos) {
    this.useKerberos = useKerberos;
  }

  String getKerberosKeytabFilePath() {
    return kerberosKeytabFilePath;
  }

  void setKerberosKeytabFilePath(String kerberosKeytabFilePath) {
    this.kerberosKeytabFilePath = kerberosKeytabFilePath;
  }

  String getKerberosPrincipal() {
    return kerberosPrincipal;
  }

  void setKerberosPrincipal(String kerberosPrincipal) {
    this.kerberosPrincipal = kerberosPrincipal;
  }

  public int getThriftServerAwaitTimeForStopService() {
    return thriftServerAwaitTimeForStopService;
  }

  public void setThriftServerAwaitTimeForStopService(int thriftServerAwaitTimeForStopService) {
    this.thriftServerAwaitTimeForStopService = thriftServerAwaitTimeForStopService;
  }

  public boolean isEnableMQTTService() {
    return enableMQTTService;
  }

  public void setEnableMQTTService(boolean enableMQTTService) {
    this.enableMQTTService = enableMQTTService;
  }

  public String getMqttHost() {
    return mqttHost;
  }

  public void setMqttHost(String mqttHost) {
    this.mqttHost = mqttHost;
  }

  public int getMqttPort() {
    return mqttPort;
  }

  public void setMqttPort(int mqttPort) {
    this.mqttPort = mqttPort;
  }

  public int getMqttHandlerPoolSize() {
    return mqttHandlerPoolSize;
  }

  public void setMqttHandlerPoolSize(int mqttHandlerPoolSize) {
    this.mqttHandlerPoolSize = mqttHandlerPoolSize;
  }

  public String getMqttPayloadFormatter() {
    return mqttPayloadFormatter;
  }

  public void setMqttPayloadFormatter(String mqttPayloadFormatter) {
    this.mqttPayloadFormatter = mqttPayloadFormatter;
  }

  public int getMqttMaxMessageSize() {
    return mqttMaxMessageSize;
  }

  public void setMqttMaxMessageSize(int mqttMaxMessageSize) {
    this.mqttMaxMessageSize = mqttMaxMessageSize;
  }

  public int getTagAttributeFlushInterval() {
    return tagAttributeFlushInterval;
  }

  public void setTagAttributeFlushInterval(int tagAttributeFlushInterval) {
    this.tagAttributeFlushInterval = tagAttributeFlushInterval;
  }

  public int getPrimitiveArraySize() {
    return primitiveArraySize;
  }

  public void setPrimitiveArraySize(int primitiveArraySize) {
    this.primitiveArraySize = primitiveArraySize;
  }

  public int getThriftMaxFrameSize() {
    return thriftMaxFrameSize;
  }

  public void setThriftMaxFrameSize(int thriftMaxFrameSize) {
    this.thriftMaxFrameSize = thriftMaxFrameSize;
    BaseRpcTransportFactory.setThriftMaxFrameSize(this.thriftMaxFrameSize);
  }

  public int getThriftDefaultBufferSize() {
    return thriftDefaultBufferSize;
  }

  public void setThriftDefaultBufferSize(int thriftDefaultBufferSize) {
    this.thriftDefaultBufferSize = thriftDefaultBufferSize;
    BaseRpcTransportFactory.setDefaultBufferCapacity(this.thriftDefaultBufferSize);
  }

  public int getCheckPeriodWhenInsertBlocked() {
    return checkPeriodWhenInsertBlocked;
  }

  public void setCheckPeriodWhenInsertBlocked(int checkPeriodWhenInsertBlocked) {
    this.checkPeriodWhenInsertBlocked = checkPeriodWhenInsertBlocked;
  }

  public int getMaxWaitingTimeWhenInsertBlocked() {
    return maxWaitingTimeWhenInsertBlockedInMs;
  }

  public void setMaxWaitingTimeWhenInsertBlocked(int maxWaitingTimeWhenInsertBlocked) {
    this.maxWaitingTimeWhenInsertBlockedInMs = maxWaitingTimeWhenInsertBlocked;
  }

  public void setMaxOffHeapMemoryBytes(long maxOffHeapMemoryBytes) {
    this.maxOffHeapMemoryBytes = maxOffHeapMemoryBytes;
  }

  public long getMaxOffHeapMemoryBytes() {
    return maxOffHeapMemoryBytes;
  }

  public long getSlowQueryThreshold() {
    return slowQueryThreshold;
  }

  public void setSlowQueryThreshold(long slowQueryThreshold) {
    this.slowQueryThreshold = slowQueryThreshold;
  }

  public boolean isEnableIndex() {
    return enableIndex;
  }

  public void setEnableIndex(boolean enableIndex) {
    this.enableIndex = enableIndex;
  }

  void setConcurrentIndexBuildThread(int concurrentIndexBuildThread) {
    this.concurrentIndexBuildThread = concurrentIndexBuildThread;
  }

  public int getConcurrentIndexBuildThread() {
    return concurrentIndexBuildThread;
  }

  public String getIndexRootFolder() {
    return indexRootFolder;
  }

  public void setIndexRootFolder(String indexRootFolder) {
    this.indexRootFolder = indexRootFolder;
  }

  public int getDefaultIndexWindowRange() {
    return defaultIndexWindowRange;
  }

  public void setDefaultIndexWindowRange(int defaultIndexWindowRange) {
    this.defaultIndexWindowRange = defaultIndexWindowRange;
  }

  public int getDataRegionNum() {
    return dataRegionNum;
  }

  public void setDataRegionNum(int dataRegionNum) {
    this.dataRegionNum = dataRegionNum;
  }

  public long getRecoveryLogIntervalInMs() {
    return recoveryLogIntervalInMs;
  }

  public void setRecoveryLogIntervalInMs(long recoveryLogIntervalInMs) {
    this.recoveryLogIntervalInMs = recoveryLogIntervalInMs;
  }

  public boolean isRpcAdvancedCompressionEnable() {
    return rpcAdvancedCompressionEnable;
  }

  public void setRpcAdvancedCompressionEnable(boolean rpcAdvancedCompressionEnable) {
    this.rpcAdvancedCompressionEnable = rpcAdvancedCompressionEnable;
    ZeroCopyRpcTransportFactory.setUseSnappy(this.rpcAdvancedCompressionEnable);
  }

  public long getSyncMlogPeriodInMs() {
    return syncMlogPeriodInMs;
  }

  public void setSyncMlogPeriodInMs(long syncMlogPeriodInMs) {
    this.syncMlogPeriodInMs = syncMlogPeriodInMs;
  }

  public int getTlogBufferSize() {
    return tlogBufferSize;
  }

  public void setTlogBufferSize(int tlogBufferSize) {
    this.tlogBufferSize = tlogBufferSize;
  }

  public boolean isEnableRpcService() {
    return enableRpcService;
  }

  public void setEnableRpcService(boolean enableRpcService) {
    this.enableRpcService = enableRpcService;
  }

  public int getIoTaskQueueSizeForFlushing() {
    return ioTaskQueueSizeForFlushing;
  }

  public void setIoTaskQueueSizeForFlushing(int ioTaskQueueSizeForFlushing) {
    this.ioTaskQueueSizeForFlushing = ioTaskQueueSizeForFlushing;
  }

  public boolean isEnableSeqSpaceCompaction() {
    return enableSeqSpaceCompaction;
  }

  public void setEnableSeqSpaceCompaction(boolean enableSeqSpaceCompaction) {
    this.enableSeqSpaceCompaction = enableSeqSpaceCompaction;
  }

  public boolean isEnableUnseqSpaceCompaction() {
    return enableUnseqSpaceCompaction;
  }

  public void setEnableUnseqSpaceCompaction(boolean enableUnseqSpaceCompaction) {
    this.enableUnseqSpaceCompaction = enableUnseqSpaceCompaction;
  }

  public boolean isEnableCrossSpaceCompaction() {
    return enableCrossSpaceCompaction;
  }

  public void setEnableCrossSpaceCompaction(boolean enableCrossSpaceCompaction) {
    this.enableCrossSpaceCompaction = enableCrossSpaceCompaction;
  }

  public boolean isEnableAINodeService() {
    return enableAINodeService;
  }

  public void setEnableAINodeService(boolean enableAINodeService) {
    this.enableAINodeService = enableAINodeService;
  }

  public InnerSequenceCompactionSelector getInnerSequenceCompactionSelector() {
    return innerSequenceCompactionSelector;
  }

  public void setInnerSequenceCompactionSelector(
      InnerSequenceCompactionSelector innerSequenceCompactionSelector) {
    this.innerSequenceCompactionSelector = innerSequenceCompactionSelector;
  }

  public InnerUnsequenceCompactionSelector getInnerUnsequenceCompactionSelector() {
    return innerUnsequenceCompactionSelector;
  }

  public void setInnerUnsequenceCompactionSelector(
      InnerUnsequenceCompactionSelector innerUnsequenceCompactionSelector) {
    this.innerUnsequenceCompactionSelector = innerUnsequenceCompactionSelector;
  }

  public InnerSeqCompactionPerformer getInnerSeqCompactionPerformer() {
    return innerSeqCompactionPerformer;
  }

  public void setInnerSeqCompactionPerformer(
      InnerSeqCompactionPerformer innerSeqCompactionPerformer) {
    this.innerSeqCompactionPerformer = innerSeqCompactionPerformer;
  }

  public InnerUnseqCompactionPerformer getInnerUnseqCompactionPerformer() {
    return innerUnseqCompactionPerformer;
  }

  public void setInnerUnseqCompactionPerformer(
      InnerUnseqCompactionPerformer innerUnseqCompactionPerformer) {
    this.innerUnseqCompactionPerformer = innerUnseqCompactionPerformer;
  }

  public CrossCompactionSelector getCrossCompactionSelector() {
    return crossCompactionSelector;
  }

  public void setCrossCompactionSelector(CrossCompactionSelector crossCompactionSelector) {
    this.crossCompactionSelector = crossCompactionSelector;
  }

  public CrossCompactionPerformer getCrossCompactionPerformer() {
    return crossCompactionPerformer;
  }

  public void setCrossCompactionPerformer(CrossCompactionPerformer crossCompactionPerformer) {
    this.crossCompactionPerformer = crossCompactionPerformer;
  }

  public CompactionPriority getCompactionPriority() {
    return compactionPriority;
  }

  public void setCompactionPriority(CompactionPriority compactionPriority) {
    this.compactionPriority = compactionPriority;
  }

  public long getTargetCompactionFileSize() {
    return targetCompactionFileSize;
  }

  public void setTargetCompactionFileSize(long targetCompactionFileSize) {
    this.targetCompactionFileSize = targetCompactionFileSize;
  }

  public int getMaxLevelGapInInnerCompaction() {
    return maxLevelGapInInnerCompaction;
  }

  public void setMaxLevelGapInInnerCompaction(int maxLevelGapInInnerCompaction) {
    this.maxLevelGapInInnerCompaction = maxLevelGapInInnerCompaction;
  }

  public long getInnerCompactionTotalFileSizeThresholdInByte() {
    return innerCompactionTotalFileSizeThresholdInByte;
  }

  public void setInnerCompactionTotalFileSizeThresholdInByte(
      long innerCompactionTotalFileSizeThresholdInByte) {
    this.innerCompactionTotalFileSizeThresholdInByte = innerCompactionTotalFileSizeThresholdInByte;
  }

  public int getInnerCompactionTotalFileNumThreshold() {
    return innerCompactionTotalFileNumThreshold;
  }

  public void setInnerCompactionTotalFileNumThreshold(int innerCompactionTotalFileNumThreshold) {
    this.innerCompactionTotalFileNumThreshold = innerCompactionTotalFileNumThreshold;
  }

  public long getTargetChunkSize() {
    return targetChunkSize;
  }

  public void setTargetChunkSize(long targetChunkSize) {
    this.targetChunkSize = targetChunkSize;
  }

  public long getChunkSizeLowerBoundInCompaction() {
    return chunkSizeLowerBoundInCompaction;
  }

  public void setChunkSizeLowerBoundInCompaction(long chunkSizeLowerBoundInCompaction) {
    this.chunkSizeLowerBoundInCompaction = chunkSizeLowerBoundInCompaction;
  }

  public long getTargetChunkPointNum() {
    return targetChunkPointNum;
  }

  public void setTargetChunkPointNum(long targetChunkPointNum) {
    this.targetChunkPointNum = targetChunkPointNum;
  }

  public long getChunkPointNumLowerBoundInCompaction() {
    return chunkPointNumLowerBoundInCompaction;
  }

  public void setChunkPointNumLowerBoundInCompaction(long chunkPointNumLowerBoundInCompaction) {
    this.chunkPointNumLowerBoundInCompaction = chunkPointNumLowerBoundInCompaction;
  }

  public long getCompactionAcquireWriteLockTimeout() {
    return compactionAcquireWriteLockTimeout;
  }

  public void setCompactionAcquireWriteLockTimeout(long compactionAcquireWriteLockTimeout) {
    this.compactionAcquireWriteLockTimeout = compactionAcquireWriteLockTimeout;
  }

  public long getCompactionScheduleIntervalInMs() {
    return compactionScheduleIntervalInMs;
  }

  public void setCompactionScheduleIntervalInMs(long compactionScheduleIntervalInMs) {
    this.compactionScheduleIntervalInMs = compactionScheduleIntervalInMs;
  }

  public long getTTlCheckInterval() {
    return ttlCheckInterval;
  }

  public int getTTlCheckerNum() {
    return ttlCheckerNum;
  }

  public void setTtlCheckInterval(long ttlCheckInterval) {
    this.ttlCheckInterval = ttlCheckInterval;
  }

  public long getMaxExpiredTime() {
    return maxExpiredTime;
  }

  public void setMaxExpiredTime(long maxExpiredTime) {
    this.maxExpiredTime = maxExpiredTime;
  }

  public float getExpiredDataRatio() {
    return expiredDataRatio;
  }

  public void setExpiredDataRatio(float expiredDataRatio) {
    this.expiredDataRatio = expiredDataRatio;
  }

  public int getInnerCompactionCandidateFileNum() {
    return innerCompactionCandidateFileNum;
  }

  public void setInnerCompactionCandidateFileNum(int innerCompactionCandidateFileNum) {
    this.innerCompactionCandidateFileNum = innerCompactionCandidateFileNum;
  }

  public int getFileLimitPerCrossTask() {
    return fileLimitPerCrossTask;
  }

  public int getTotalFileLimitForCompactionTask() {
    return totalFileLimitForCompactionTask;
  }

  public void setFileLimitPerCrossTask(int fileLimitPerCrossTask) {
    this.fileLimitPerCrossTask = fileLimitPerCrossTask;
  }

  public long getMaxCrossCompactionCandidateFileSize() {
    return maxCrossCompactionCandidateFileSize;
  }

  public void setMaxCrossCompactionCandidateFileSize(long maxCrossCompactionCandidateFileSize) {
    this.maxCrossCompactionCandidateFileSize = maxCrossCompactionCandidateFileSize;
  }

  public int getMinCrossCompactionUnseqFileLevel() {
    return minCrossCompactionUnseqFileLevel;
  }

  public void setMinCrossCompactionUnseqFileLevel(int minCrossCompactionUnseqFileLevel) {
    this.minCrossCompactionUnseqFileLevel = minCrossCompactionUnseqFileLevel;
  }

  public int getSubCompactionTaskNum() {
    return subCompactionTaskNum;
  }

  public void setSubCompactionTaskNum(int subCompactionTaskNum) {
    this.subCompactionTaskNum = subCompactionTaskNum;
  }

  public int getCompactionScheduleThreadNum() {
    return compactionScheduleThreadNum;
  }

  public void setCompactionScheduleThreadNum(int compactionScheduleThreadNum) {
    this.compactionScheduleThreadNum = compactionScheduleThreadNum;
  }

  public int getCachedMNodeSizeInPBTreeMode() {
    return cachedMNodeSizeInPBTreeMode;
  }

  @TestOnly
  public void setCachedMNodeSizeInPBTreeMode(int cachedMNodeSizeInPBTreeMode) {
    this.cachedMNodeSizeInPBTreeMode = cachedMNodeSizeInPBTreeMode;
  }

  public short getMinimumSegmentInPBTree() {
    return minimumSegmentInPBTree;
  }

  public void setMinimumSegmentInPBTree(short minimumSegmentInPBTree) {
    this.minimumSegmentInPBTree = minimumSegmentInPBTree;
  }

  public int getPageCacheSizeInPBTree() {
    return pageCacheSizeInPBTree;
  }

  public void setPageCacheSizeInPBTree(int pageCacheSizeInPBTree) {
    this.pageCacheSizeInPBTree = pageCacheSizeInPBTree;
  }

  public int getPBTreeLogSize() {
    return pbTreeLogSize;
  }

  public void setPBTreeLogSize(int pbTreeLogSize) {
    this.pbTreeLogSize = pbTreeLogSize;
  }

  public int getMaxMeasurementNumOfInternalRequest() {
    return maxMeasurementNumOfInternalRequest;
  }

  public void setMaxMeasurementNumOfInternalRequest(int maxMeasurementNumOfInternalRequest) {
    this.maxMeasurementNumOfInternalRequest = maxMeasurementNumOfInternalRequest;
  }

  public String getInternalAddress() {
    return internalAddress;
  }

  public void setInternalAddress(String internalAddress) {
    this.internalAddress = internalAddress;
  }

  public int getInternalPort() {
    return internalPort;
  }

  public void setInternalPort(int internalPort) {
    this.internalPort = internalPort;
  }

  public int getAINodePort() {
    return aiNodePort;
  }

  public void setAINodePort(int aiNodePort) {
    this.aiNodePort = aiNodePort;
  }

  public int getDataRegionConsensusPort() {
    return dataRegionConsensusPort;
  }

  public void setDataRegionConsensusPort(int dataRegionConsensusPort) {
    this.dataRegionConsensusPort = dataRegionConsensusPort;
  }

  public int getSchemaRegionConsensusPort() {
    return schemaRegionConsensusPort;
  }

  public void setSchemaRegionConsensusPort(int schemaRegionConsensusPort) {
    this.schemaRegionConsensusPort = schemaRegionConsensusPort;
  }

  public TEndPoint getSeedConfigNode() {
    return seedConfigNode;
  }

  public void setSeedConfigNode(TEndPoint seedConfigNode) {
    this.seedConfigNode = seedConfigNode;
  }

  public long getJoinClusterRetryIntervalMs() {
    return joinClusterRetryIntervalMs;
  }

  public void setJoinClusterRetryIntervalMs(long joinClusterRetryIntervalMs) {
    this.joinClusterRetryIntervalMs = joinClusterRetryIntervalMs;
  }

  public String getDataRegionConsensusProtocolClass() {
    return dataRegionConsensusProtocolClass;
  }

  public void setDataRegionConsensusProtocolClass(String dataRegionConsensusProtocolClass) {
    this.dataRegionConsensusProtocolClass = dataRegionConsensusProtocolClass;
  }

  public String getSchemaRegionConsensusProtocolClass() {
    return schemaRegionConsensusProtocolClass;
  }

  public void setSchemaRegionConsensusProtocolClass(String schemaRegionConsensusProtocolClass) {
    this.schemaRegionConsensusProtocolClass = schemaRegionConsensusProtocolClass;
  }

  public String getSeriesPartitionExecutorClass() {
    return seriesPartitionExecutorClass;
  }

  public void setSeriesPartitionExecutorClass(String seriesPartitionExecutorClass) {
    this.seriesPartitionExecutorClass = seriesPartitionExecutorClass;
  }

  public int getSeriesPartitionSlotNum() {
    return seriesPartitionSlotNum;
  }

  public void setSeriesPartitionSlotNum(int seriesPartitionSlotNum) {
    this.seriesPartitionSlotNum = seriesPartitionSlotNum;
  }

  public int getMppDataExchangePort() {
    return mppDataExchangePort;
  }

  public void setMppDataExchangePort(int mppDataExchangePort) {
    this.mppDataExchangePort = mppDataExchangePort;
  }

  public int getMppDataExchangeCorePoolSize() {
    return mppDataExchangeCorePoolSize;
  }

  public void setMppDataExchangeCorePoolSize(int mppDataExchangeCorePoolSize) {
    this.mppDataExchangeCorePoolSize = mppDataExchangeCorePoolSize;
  }

  public int getMppDataExchangeMaxPoolSize() {
    return mppDataExchangeMaxPoolSize;
  }

  public void setMppDataExchangeMaxPoolSize(int mppDataExchangeMaxPoolSize) {
    this.mppDataExchangeMaxPoolSize = mppDataExchangeMaxPoolSize;
  }

  public int getMppDataExchangeKeepAliveTimeInMs() {
    return mppDataExchangeKeepAliveTimeInMs;
  }

  public void setMppDataExchangeKeepAliveTimeInMs(int mppDataExchangeKeepAliveTimeInMs) {
    this.mppDataExchangeKeepAliveTimeInMs = mppDataExchangeKeepAliveTimeInMs;
  }

  public int getConnectionTimeoutInMS() {
    return connectionTimeoutInMS;
  }

  public void setConnectionTimeoutInMS(int connectionTimeoutInMS) {
    this.connectionTimeoutInMS = connectionTimeoutInMS;
  }

  public int getMaxClientNumForEachNode() {
    return maxClientNumForEachNode;
  }

  public void setMaxClientNumForEachNode(int maxClientNumForEachNode) {
    this.maxClientNumForEachNode = maxClientNumForEachNode;
  }

  public int getSelectorNumOfClientManager() {
    return selectorNumOfClientManager;
  }

  public void setSelectorNumOfClientManager(int selectorNumOfClientManager) {
    this.selectorNumOfClientManager = selectorNumOfClientManager;
  }

  public String getClusterName() {
    return clusterName;
  }

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
    MetricConfigDescriptor.getInstance().getMetricConfig().updateClusterName(clusterName);
  }

  public String getClusterId() {
    return clusterId;
  }

  public void setClusterId(String clusterId) {
    this.clusterId = clusterId;
  }

  public int getDataNodeId() {
    return dataNodeId;
  }

  public void setDataNodeId(int dataNodeId) {
    this.dataNodeId = dataNodeId;
  }

  public int getPartitionCacheSize() {
    return partitionCacheSize;
  }

  public String getExtPipeDir() {
    return extPipeDir;
  }

  public void setExtPipeDir(String extPipeDir) {
    this.extPipeDir = extPipeDir;
  }

  public void setPartitionCacheSize(int partitionCacheSize) {
    this.partitionCacheSize = partitionCacheSize;
  }

  public int getAuthorCacheSize() {
    return authorCacheSize;
  }

  public void setAuthorCacheSize(int authorCacheSize) {
    this.authorCacheSize = authorCacheSize;
  }

  public int getAuthorCacheExpireTime() {
    return authorCacheExpireTime;
  }

  public void setAuthorCacheExpireTime(int authorCacheExpireTime) {
    this.authorCacheExpireTime = authorCacheExpireTime;
  }

  public int getTriggerForwardMaxQueueNumber() {
    return triggerForwardMaxQueueNumber;
  }

  public void setTriggerForwardMaxQueueNumber(int triggerForwardMaxQueueNumber) {
    this.triggerForwardMaxQueueNumber = triggerForwardMaxQueueNumber;
  }

  public int getTriggerForwardMaxSizePerQueue() {
    return triggerForwardMaxSizePerQueue;
  }

  public void setTriggerForwardMaxSizePerQueue(int triggerForwardMaxSizePerQueue) {
    this.triggerForwardMaxSizePerQueue = triggerForwardMaxSizePerQueue;
  }

  public int getTriggerForwardBatchSize() {
    return triggerForwardBatchSize;
  }

  public void setTriggerForwardBatchSize(int triggerForwardBatchSize) {
    this.triggerForwardBatchSize = triggerForwardBatchSize;
  }

  public int getTriggerForwardHTTPPoolSize() {
    return triggerForwardHTTPPoolSize;
  }

  public void setTriggerForwardHTTPPoolSize(int triggerForwardHTTPPoolSize) {
    this.triggerForwardHTTPPoolSize = triggerForwardHTTPPoolSize;
  }

  public int getTriggerForwardHTTPPOOLMaxPerRoute() {
    return triggerForwardHTTPPOOLMaxPerRoute;
  }

  public void setTriggerForwardHTTPPOOLMaxPerRoute(int triggerForwardHTTPPOOLMaxPerRoute) {
    this.triggerForwardHTTPPOOLMaxPerRoute = triggerForwardHTTPPOOLMaxPerRoute;
  }

  public int getTriggerForwardMQTTPoolSize() {
    return triggerForwardMQTTPoolSize;
  }

  public void setTriggerForwardMQTTPoolSize(int triggerForwardMQTTPoolSize) {
    this.triggerForwardMQTTPoolSize = triggerForwardMQTTPoolSize;
  }

  public int getRetryNumToFindStatefulTrigger() {
    return retryNumToFindStatefulTrigger;
  }

  public void setRetryNumToFindStatefulTrigger(int retryNumToFindStatefulTrigger) {
    this.retryNumToFindStatefulTrigger = retryNumToFindStatefulTrigger;
  }

  public int getCoordinatorReadExecutorSize() {
    return coordinatorReadExecutorSize;
  }

  public void setCoordinatorReadExecutorSize(int coordinatorReadExecutorSize) {
    this.coordinatorReadExecutorSize = coordinatorReadExecutorSize;
  }

  public int getCoordinatorWriteExecutorSize() {
    return coordinatorWriteExecutorSize;
  }

  public void setCoordinatorWriteExecutorSize(int coordinatorWriteExecutorSize) {
    this.coordinatorWriteExecutorSize = coordinatorWriteExecutorSize;
  }

  public TEndPoint getAddressAndPort() {
    return new TEndPoint(rpcAddress, rpcPort);
  }

  public int[] getSchemaMemoryProportion() {
    return schemaMemoryProportion;
  }

  public void setSchemaMemoryProportion(int[] schemaMemoryProportion) {
    this.schemaMemoryProportion = schemaMemoryProportion;
  }

  public long getAllocateMemoryForSchemaRegion() {
    return allocateMemoryForSchemaRegion;
  }

  public void setAllocateMemoryForSchemaRegion(long allocateMemoryForSchemaRegion) {
    this.allocateMemoryForSchemaRegion = allocateMemoryForSchemaRegion;
  }

  public long getAllocateMemoryForSchemaCache() {
    return allocateMemoryForSchemaCache;
  }

  public void setAllocateMemoryForSchemaCache(long allocateMemoryForSchemaCache) {
    this.allocateMemoryForSchemaCache = allocateMemoryForSchemaCache;
  }

  public long getAllocateMemoryForPartitionCache() {
    return allocateMemoryForPartitionCache;
  }

  public void setAllocateMemoryForPartitionCache(long allocateMemoryForPartitionCache) {
    this.allocateMemoryForPartitionCache = allocateMemoryForPartitionCache;
  }

  public String getDataNodeSchemaCacheEvictionPolicy() {
    return dataNodeSchemaCacheEvictionPolicy;
  }

  public void setDataNodeSchemaCacheEvictionPolicy(String dataNodeSchemaCacheEvictionPolicy) {
    this.dataNodeSchemaCacheEvictionPolicy = dataNodeSchemaCacheEvictionPolicy;
  }

  public String getReadConsistencyLevel() {
    return readConsistencyLevel;
  }

  public void setReadConsistencyLevel(String readConsistencyLevel) {
    this.readConsistencyLevel = readConsistencyLevel;
  }

  public int getDriverTaskExecutionTimeSliceInMs() {
    return driverTaskExecutionTimeSliceInMs;
  }

  public void setDriverTaskExecutionTimeSliceInMs(int driverTaskExecutionTimeSliceInMs) {
    this.driverTaskExecutionTimeSliceInMs = driverTaskExecutionTimeSliceInMs;
  }

  public double getWriteProportionForMemtable() {
    return writeProportionForMemtable;
  }

  public void setWriteProportionForMemtable(double writeProportionForMemtable) {
    this.writeProportionForMemtable = writeProportionForMemtable;
  }

  public double getCompactionProportion() {
    return compactionProportion;
  }

  public double getDevicePathCacheProportion() {
    return devicePathCacheProportion;
  }

  public void setDevicePathCacheProportion(double devicePathCacheProportion) {
    this.devicePathCacheProportion = devicePathCacheProportion;
  }

  public static String getEnvironmentVariables() {
    return "\n\t"
        + IoTDBConstant.IOTDB_HOME
        + "="
        + System.getProperty(IoTDBConstant.IOTDB_HOME, "null")
        + ";"
        + "\n\t"
        + IoTDBConstant.IOTDB_CONF
        + "="
        + System.getProperty(IoTDBConstant.IOTDB_CONF, "null")
        + ";"
        + "\n\t"
        + IoTDBConstant.IOTDB_DATA_HOME
        + "="
        + System.getProperty(IoTDBConstant.IOTDB_DATA_HOME, "null")
        + ";";
  }

  public void setCompactionProportion(double compactionProportion) {
    this.compactionProportion = compactionProportion;
  }

  public long getThrottleThreshold() {
    return throttleThreshold;
  }

  public void setThrottleThreshold(long throttleThreshold) {
    this.throttleThreshold = throttleThreshold;
  }

  public double getChunkMetadataSizeProportion() {
    return chunkMetadataSizeProportion;
  }

  public void setChunkMetadataSizeProportion(double chunkMetadataSizeProportion) {
    this.chunkMetadataSizeProportion = chunkMetadataSizeProportion;
  }

  public long getCacheWindowTimeInMs() {
    return cacheWindowTimeInMs;
  }

  public void setCacheWindowTimeInMs(long cacheWindowTimeInMs) {
    this.cacheWindowTimeInMs = cacheWindowTimeInMs;
  }

  public long getDataRatisConsensusLogAppenderBufferSizeMax() {
    return dataRatisConsensusLogAppenderBufferSizeMax;
  }

  public void setDataRatisConsensusLogAppenderBufferSizeMax(
      long dataRatisConsensusLogAppenderBufferSizeMax) {
    this.dataRatisConsensusLogAppenderBufferSizeMax = dataRatisConsensusLogAppenderBufferSizeMax;
  }

  public String getConfigMessage() {
    StringBuilder configMessage = new StringBuilder();
    String configContent;
    String[] notShowArray = {
      "NODE_NAME_MATCHER",
      "PARTIAL_NODE_MATCHER",
      "STORAGE_GROUP_MATCHER",
      "STORAGE_GROUP_PATTERN",
      "NODE_MATCHER",
      "NODE_PATTERN"
    };
    List<String> notShowStrings = Arrays.asList(notShowArray);
    for (Field configField : IoTDBConfig.class.getDeclaredFields()) {
      try {
        String configFieldString = configField.getName();
        if (notShowStrings.contains(configFieldString)) {
          continue;
        }
        String configType = configField.getGenericType().getTypeName();
        if (configType.contains("java.lang.String[][]")) {
          String[][] configList = (String[][]) configField.get(this);
          StringBuilder builder = new StringBuilder();
          for (String[] strings : configList) {
            builder.append(Arrays.asList(strings)).append(";");
          }
          configContent = builder.toString();
        } else if (configType.contains("java.lang.String[]")) {
          String[] configList = (String[]) configField.get(this);
          configContent = Arrays.asList(configList).toString();
        } else {
          configContent = configField.get(this).toString();
        }
        configMessage
            .append("\n\t")
            .append(configField.getName())
            .append("=")
            .append(configContent)
            .append(";");
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return configMessage.toString();
  }

  public long getDataRatisConsensusSnapshotTriggerThreshold() {
    return dataRatisConsensusSnapshotTriggerThreshold;
  }

  public void setDataRatisConsensusSnapshotTriggerThreshold(
      long dataRatisConsensusSnapshotTriggerThreshold) {
    this.dataRatisConsensusSnapshotTriggerThreshold = dataRatisConsensusSnapshotTriggerThreshold;
  }

  public boolean isDataRatisConsensusLogUnsafeFlushEnable() {
    return dataRatisConsensusLogUnsafeFlushEnable;
  }

  public void setDataRatisConsensusLogUnsafeFlushEnable(
      boolean dataRatisConsensusLogUnsafeFlushEnable) {
    this.dataRatisConsensusLogUnsafeFlushEnable = dataRatisConsensusLogUnsafeFlushEnable;
  }

  public int getDataRatisConsensusLogForceSyncNum() {
    return dataRatisConsensusLogForceSyncNum;
  }

  public void setDataRatisConsensusLogForceSyncNum(int dataRatisConsensusLogForceSyncNum) {
    this.dataRatisConsensusLogForceSyncNum = dataRatisConsensusLogForceSyncNum;
  }

  public int getSchemaRatisConsensusLogForceSyncNum() {
    return schemaRatisConsensusLogForceSyncNum;
  }

  public void setSchemaRatisConsensusLogForceSyncNum(int schemaRatisConsensusLogForceSyncNum) {
    this.schemaRatisConsensusLogForceSyncNum = schemaRatisConsensusLogForceSyncNum;
  }

  public long getDataRatisConsensusLogSegmentSizeMax() {
    return dataRatisConsensusLogSegmentSizeMax;
  }

  public void setDataRatisConsensusLogSegmentSizeMax(long dataRatisConsensusLogSegmentSizeMax) {
    this.dataRatisConsensusLogSegmentSizeMax = dataRatisConsensusLogSegmentSizeMax;
  }

  public long getDataRatisConsensusGrpcFlowControlWindow() {
    return dataRatisConsensusGrpcFlowControlWindow;
  }

  public void setDataRatisConsensusGrpcFlowControlWindow(
      long dataRatisConsensusGrpcFlowControlWindow) {
    this.dataRatisConsensusGrpcFlowControlWindow = dataRatisConsensusGrpcFlowControlWindow;
  }

  public int getDataRatisConsensusGrpcLeaderOutstandingAppendsMax() {
    return dataRatisConsensusGrpcLeaderOutstandingAppendsMax;
  }

  public void setDataRatisConsensusGrpcLeaderOutstandingAppendsMax(
      int dataRatisConsensusGrpcLeaderOutstandingAppendsMax) {
    this.dataRatisConsensusGrpcLeaderOutstandingAppendsMax =
        dataRatisConsensusGrpcLeaderOutstandingAppendsMax;
  }

  public int getSchemaRatisConsensusGrpcLeaderOutstandingAppendsMax() {
    return schemaRatisConsensusGrpcLeaderOutstandingAppendsMax;
  }

  public void setSchemaRatisConsensusGrpcLeaderOutstandingAppendsMax(
      int schemaRatisConsensusGrpcLeaderOutstandingAppendsMax) {
    this.schemaRatisConsensusGrpcLeaderOutstandingAppendsMax =
        schemaRatisConsensusGrpcLeaderOutstandingAppendsMax;
  }

  public long getDataRatisConsensusLeaderElectionTimeoutMinMs() {
    return dataRatisConsensusLeaderElectionTimeoutMinMs;
  }

  public void setDataRatisConsensusLeaderElectionTimeoutMinMs(
      long dataRatisConsensusLeaderElectionTimeoutMinMs) {
    this.dataRatisConsensusLeaderElectionTimeoutMinMs =
        dataRatisConsensusLeaderElectionTimeoutMinMs;
  }

  public long getDataRatisConsensusLeaderElectionTimeoutMaxMs() {
    return dataRatisConsensusLeaderElectionTimeoutMaxMs;
  }

  public void setDataRatisConsensusLeaderElectionTimeoutMaxMs(
      long dataRatisConsensusLeaderElectionTimeoutMaxMs) {
    this.dataRatisConsensusLeaderElectionTimeoutMaxMs =
        dataRatisConsensusLeaderElectionTimeoutMaxMs;
  }

  public long getSchemaRatisConsensusLogAppenderBufferSizeMax() {
    return schemaRatisConsensusLogAppenderBufferSizeMax;
  }

  public void setSchemaRatisConsensusLogAppenderBufferSizeMax(
      long schemaRatisConsensusLogAppenderBufferSizeMax) {
    this.schemaRatisConsensusLogAppenderBufferSizeMax =
        schemaRatisConsensusLogAppenderBufferSizeMax;
  }

  public long getSchemaRatisConsensusSnapshotTriggerThreshold() {
    return schemaRatisConsensusSnapshotTriggerThreshold;
  }

  public void setSchemaRatisConsensusSnapshotTriggerThreshold(
      long schemaRatisConsensusSnapshotTriggerThreshold) {
    this.schemaRatisConsensusSnapshotTriggerThreshold =
        schemaRatisConsensusSnapshotTriggerThreshold;
  }

  public boolean isSchemaRatisConsensusLogUnsafeFlushEnable() {
    return schemaRatisConsensusLogUnsafeFlushEnable;
  }

  public void setSchemaRatisConsensusLogUnsafeFlushEnable(
      boolean schemaRatisConsensusLogUnsafeFlushEnable) {
    this.schemaRatisConsensusLogUnsafeFlushEnable = schemaRatisConsensusLogUnsafeFlushEnable;
  }

  public long getSchemaRatisConsensusLogSegmentSizeMax() {
    return schemaRatisConsensusLogSegmentSizeMax;
  }

  public void setSchemaRatisConsensusLogSegmentSizeMax(long schemaRatisConsensusLogSegmentSizeMax) {
    this.schemaRatisConsensusLogSegmentSizeMax = schemaRatisConsensusLogSegmentSizeMax;
  }

  public long getSchemaRatisConsensusGrpcFlowControlWindow() {
    return schemaRatisConsensusGrpcFlowControlWindow;
  }

  public void setSchemaRatisConsensusGrpcFlowControlWindow(
      long schemaRatisConsensusGrpcFlowControlWindow) {
    this.schemaRatisConsensusGrpcFlowControlWindow = schemaRatisConsensusGrpcFlowControlWindow;
  }

  public long getSchemaRatisConsensusLeaderElectionTimeoutMinMs() {
    return schemaRatisConsensusLeaderElectionTimeoutMinMs;
  }

  public void setSchemaRatisConsensusLeaderElectionTimeoutMinMs(
      long schemaRatisConsensusLeaderElectionTimeoutMinMs) {
    this.schemaRatisConsensusLeaderElectionTimeoutMinMs =
        schemaRatisConsensusLeaderElectionTimeoutMinMs;
  }

  public long getSchemaRatisConsensusLeaderElectionTimeoutMaxMs() {
    return schemaRatisConsensusLeaderElectionTimeoutMaxMs;
  }

  public void setSchemaRatisConsensusLeaderElectionTimeoutMaxMs(
      long schemaRatisConsensusLeaderElectionTimeoutMaxMs) {
    this.schemaRatisConsensusLeaderElectionTimeoutMaxMs =
        schemaRatisConsensusLeaderElectionTimeoutMaxMs;
  }

  public long getCqMinEveryIntervalInMs() {
    return cqMinEveryIntervalInMs;
  }

  public void setCqMinEveryIntervalInMs(long cqMinEveryIntervalInMs) {
    this.cqMinEveryIntervalInMs = cqMinEveryIntervalInMs;
  }

  public double getUsableCompactionMemoryProportion() {
    return 1.0d - chunkMetadataSizeProportion;
  }

  public int getPatternMatchingThreshold() {
    return patternMatchingThreshold;
  }

  public void setPatternMatchingThreshold(int patternMatchingThreshold) {
    this.patternMatchingThreshold = patternMatchingThreshold;
  }

  public long getDataRatisConsensusRequestTimeoutMs() {
    return dataRatisConsensusRequestTimeoutMs;
  }

  public void setDataRatisConsensusRequestTimeoutMs(long dataRatisConsensusRequestTimeoutMs) {
    this.dataRatisConsensusRequestTimeoutMs = dataRatisConsensusRequestTimeoutMs;
  }

  public long getSchemaRatisConsensusRequestTimeoutMs() {
    return schemaRatisConsensusRequestTimeoutMs;
  }

  public void setSchemaRatisConsensusRequestTimeoutMs(long schemaRatisConsensusRequestTimeoutMs) {
    this.schemaRatisConsensusRequestTimeoutMs = schemaRatisConsensusRequestTimeoutMs;
  }

  public int getDataRatisConsensusMaxRetryAttempts() {
    return dataRatisConsensusMaxRetryAttempts;
  }

  public void setDataRatisConsensusMaxRetryAttempts(int dataRatisConsensusMaxRetryAttempts) {
    this.dataRatisConsensusMaxRetryAttempts = dataRatisConsensusMaxRetryAttempts;
  }

  public int getSchemaRatisConsensusMaxRetryAttempts() {
    return schemaRatisConsensusMaxRetryAttempts;
  }

  public void setSchemaRatisConsensusMaxRetryAttempts(int schemaRatisConsensusMaxRetryAttempts) {
    this.schemaRatisConsensusMaxRetryAttempts = schemaRatisConsensusMaxRetryAttempts;
  }

  public long getDataRatisConsensusInitialSleepTimeMs() {
    return dataRatisConsensusInitialSleepTimeMs;
  }

  public void setDataRatisConsensusInitialSleepTimeMs(long dataRatisConsensusInitialSleepTimeMs) {
    this.dataRatisConsensusInitialSleepTimeMs = dataRatisConsensusInitialSleepTimeMs;
  }

  public long getSchemaRatisConsensusInitialSleepTimeMs() {
    return schemaRatisConsensusInitialSleepTimeMs;
  }

  public void setSchemaRatisConsensusInitialSleepTimeMs(
      long schemaRatisConsensusInitialSleepTimeMs) {
    this.schemaRatisConsensusInitialSleepTimeMs = schemaRatisConsensusInitialSleepTimeMs;
  }

  public long getDataRatisConsensusMaxSleepTimeMs() {
    return dataRatisConsensusMaxSleepTimeMs;
  }

  public void setDataRatisConsensusMaxSleepTimeMs(long dataRatisConsensusMaxSleepTimeMs) {
    this.dataRatisConsensusMaxSleepTimeMs = dataRatisConsensusMaxSleepTimeMs;
  }

  public long getSchemaRatisConsensusMaxSleepTimeMs() {
    return schemaRatisConsensusMaxSleepTimeMs;
  }

  public void setSchemaRatisConsensusMaxSleepTimeMs(long schemaRatisConsensusMaxSleepTimeMs) {
    this.schemaRatisConsensusMaxSleepTimeMs = schemaRatisConsensusMaxSleepTimeMs;
  }

  public Properties getCustomizedProperties() {
    return customizedProperties;
  }

  public void setCustomizedProperties(Properties customizedProperties) {
    this.customizedProperties = customizedProperties;
  }

  public long getDataRatisConsensusPreserveWhenPurge() {
    return dataRatisConsensusPreserveWhenPurge;
  }

  public void setDataRatisConsensusPreserveWhenPurge(long dataRatisConsensusPreserveWhenPurge) {
    this.dataRatisConsensusPreserveWhenPurge = dataRatisConsensusPreserveWhenPurge;
  }

  public long getSchemaRatisConsensusPreserveWhenPurge() {
    return schemaRatisConsensusPreserveWhenPurge;
  }

  public void setSchemaRatisConsensusPreserveWhenPurge(long schemaRatisConsensusPreserveWhenPurge) {
    this.schemaRatisConsensusPreserveWhenPurge = schemaRatisConsensusPreserveWhenPurge;
  }

  public long getRatisFirstElectionTimeoutMinMs() {
    return ratisFirstElectionTimeoutMinMs;
  }

  public void setRatisFirstElectionTimeoutMinMs(long ratisFirstElectionTimeoutMinMs) {
    this.ratisFirstElectionTimeoutMinMs = ratisFirstElectionTimeoutMinMs;
  }

  public long getRatisFirstElectionTimeoutMaxMs() {
    return ratisFirstElectionTimeoutMaxMs;
  }

  public void setRatisFirstElectionTimeoutMaxMs(long ratisFirstElectionTimeoutMaxMs) {
    this.ratisFirstElectionTimeoutMaxMs = ratisFirstElectionTimeoutMaxMs;
  }

  public long getDataRatisLogMax() {
    return dataRatisLogMax;
  }

  public void setDataRatisLogMax(long dataRatisLogMax) {
    this.dataRatisLogMax = dataRatisLogMax;
  }

  public long getSchemaRatisLogMax() {
    return schemaRatisLogMax;
  }

  public void setSchemaRatisLogMax(long schemaRatisLogMax) {
    this.schemaRatisLogMax = schemaRatisLogMax;
  }

  public int getCandidateCompactionTaskQueueSize() {
    return candidateCompactionTaskQueueSize;
  }

  public void setCandidateCompactionTaskQueueSize(int candidateCompactionTaskQueueSize) {
    this.candidateCompactionTaskQueueSize = candidateCompactionTaskQueueSize;
  }

  public int getGlobalCompactionFileInfoCacheSize() {
    return globalCompactionFileInfoCacheSize;
  }

  public void setGlobalCompactionFileInfoCacheSize(int globalCompactionFileInfoCacheSize) {
    this.globalCompactionFileInfoCacheSize = globalCompactionFileInfoCacheSize;
  }

  public boolean isEnableAuditLog() {
    return enableAuditLog;
  }

  public void setEnableAuditLog(boolean enableAuditLog) {
    this.enableAuditLog = enableAuditLog;
  }

  public List<AuditLogStorage> getAuditLogStorage() {
    return auditLogStorage;
  }

  public void setAuditLogStorage(List<AuditLogStorage> auditLogStorage) {
    this.auditLogStorage = auditLogStorage;
  }

  public List<AuditLogOperation> getAuditLogOperation() {
    return auditLogOperation;
  }

  public void setAuditLogOperation(List<AuditLogOperation> auditLogOperation) {
    this.auditLogOperation = auditLogOperation;
  }

  public boolean isEnableAuditLogForNativeInsertApi() {
    return enableAuditLogForNativeInsertApi;
  }

  public void setEnableAuditLogForNativeInsertApi(boolean enableAuditLogForNativeInsertApi) {
    this.enableAuditLogForNativeInsertApi = enableAuditLogForNativeInsertApi;
  }

  public void setModeMapSizeThreshold(int modeMapSizeThreshold) {
    this.modeMapSizeThreshold = modeMapSizeThreshold;
  }

  public int getModeMapSizeThreshold() {
    return modeMapSizeThreshold;
  }

  public double getMaxAllocateMemoryRatioForLoad() {
    return maxAllocateMemoryRatioForLoad;
  }

  public void setMaxAllocateMemoryRatioForLoad(double maxAllocateMemoryRatioForLoad) {
    this.maxAllocateMemoryRatioForLoad = maxAllocateMemoryRatioForLoad;
  }

  public int getLoadTsFileAnalyzeSchemaBatchFlushTimeSeriesNumber() {
    return loadTsFileAnalyzeSchemaBatchFlushTimeSeriesNumber;
  }

  public void setLoadTsFileAnalyzeSchemaBatchFlushTimeSeriesNumber(
      int loadTsFileAnalyzeSchemaBatchFlushTimeSeriesNumber) {
    this.loadTsFileAnalyzeSchemaBatchFlushTimeSeriesNumber =
        loadTsFileAnalyzeSchemaBatchFlushTimeSeriesNumber;
  }

  public long getLoadTsFileAnalyzeSchemaMemorySizeInBytes() {
    return loadTsFileAnalyzeSchemaMemorySizeInBytes;
  }

  public void setLoadTsFileAnalyzeSchemaMemorySizeInBytes(
      long loadTsFileAnalyzeSchemaMemorySizeInBytes) {
    this.loadTsFileAnalyzeSchemaMemorySizeInBytes = loadTsFileAnalyzeSchemaMemorySizeInBytes;
  }

  public int getLoadTsFileMaxDeviceCountToUseDeviceTimeIndex() {
    return loadTsFileMaxDeviceCountToUseDeviceTimeIndex;
  }

  public void setLoadTsFileMaxDeviceCountToUseDeviceTimeIndex(
      int loadTsFileMaxDeviceCountToUseDeviceTimeIndex) {
    this.loadTsFileMaxDeviceCountToUseDeviceTimeIndex =
        loadTsFileMaxDeviceCountToUseDeviceTimeIndex;
  }

  public long getLoadMemoryAllocateRetryIntervalMs() {
    return loadMemoryAllocateRetryIntervalMs;
  }

  public void setLoadMemoryAllocateRetryIntervalMs(long loadMemoryAllocateRetryIntervalMs) {
    this.loadMemoryAllocateRetryIntervalMs = loadMemoryAllocateRetryIntervalMs;
  }

  public int getLoadMemoryAllocateMaxRetries() {
    return loadMemoryAllocateMaxRetries;
  }

  public void setLoadMemoryAllocateMaxRetries(int loadMemoryAllocateMaxRetries) {
    this.loadMemoryAllocateMaxRetries = loadMemoryAllocateMaxRetries;
  }

  public long getLoadCleanupTaskExecutionDelayTimeSeconds() {
    return loadCleanupTaskExecutionDelayTimeSeconds;
  }

  public void setLoadCleanupTaskExecutionDelayTimeSeconds(
      long loadCleanupTaskExecutionDelayTimeSeconds) {
    this.loadCleanupTaskExecutionDelayTimeSeconds = loadCleanupTaskExecutionDelayTimeSeconds;
  }

  public double getLoadWriteThroughputBytesPerSecond() {
    return loadWriteThroughputBytesPerSecond;
  }

  public void setLoadWriteThroughputBytesPerSecond(double loadWriteThroughputBytesPerSecond) {
    this.loadWriteThroughputBytesPerSecond = loadWriteThroughputBytesPerSecond;
  }

  public int getLoadActiveListeningMaxThreadNum() {
    return loadActiveListeningMaxThreadNum;
  }

  public void setLoadActiveListeningMaxThreadNum(int loadActiveListeningMaxThreadNum) {
    this.loadActiveListeningMaxThreadNum = loadActiveListeningMaxThreadNum;
  }

  public long getLoadActiveListeningCheckIntervalSeconds() {
    return loadActiveListeningCheckIntervalSeconds;
  }

  public void setLoadActiveListeningCheckIntervalSeconds(
      long loadActiveListeningCheckIntervalSeconds) {
    this.loadActiveListeningCheckIntervalSeconds = loadActiveListeningCheckIntervalSeconds;
  }

  public String getLoadActiveListeningFailDir() {
    return loadActiveListeningFailDir == null || Objects.equals(loadActiveListeningFailDir, "")
        ? extDir
            + File.separator
            + IoTDBConstant.LOAD_TSFILE_FOLDER_NAME
            + File.separator
            + IoTDBConstant.LOAD_TSFILE_ACTIVE_LISTENING_FAILED_FOLDER_NAME
        : loadActiveListeningFailDir;
  }

  public void setLoadActiveListeningFailDir(String loadActiveListeningFailDir) {
    this.loadActiveListeningFailDir = addDataHomeDir(loadActiveListeningFailDir);
  }

  public String getLoadActiveListeningPipeDir() {
    return loadActiveListeningPipeDir;
  }

  public String[] getLoadActiveListeningDirs() {
    return (Objects.isNull(this.loadActiveListeningDirs)
            || this.loadActiveListeningDirs.length == 0)
        ? new String[] {
          extDir
              + File.separator
              + IoTDBConstant.LOAD_TSFILE_FOLDER_NAME
              + File.separator
              + IoTDBConstant.LOAD_TSFILE_ACTIVE_LISTENING_PENDING_FOLDER_NAME
        }
        : this.loadActiveListeningDirs;
  }

  public void setLoadActiveListeningDirs(String[] loadActiveListeningDirs) {
    for (int i = 0; i < loadActiveListeningDirs.length; i++) {
      loadActiveListeningDirs[i] = addDataHomeDir(loadActiveListeningDirs[i]);
    }
    this.loadActiveListeningDirs = loadActiveListeningDirs;
  }

  public boolean getLoadActiveListeningEnable() {
    return loadActiveListeningEnable;
  }

  public void setLoadActiveListeningEnable(boolean loadActiveListeningEnable) {
    this.loadActiveListeningEnable = loadActiveListeningEnable;
  }

  public void setPipeReceiverFileDirs(String[] pipeReceiverFileDirs) {
    this.pipeReceiverFileDirs = pipeReceiverFileDirs;
  }

  public String[] getPipeReceiverFileDirs() {
    return (Objects.isNull(this.pipeReceiverFileDirs) || this.pipeReceiverFileDirs.length == 0)
        ? new String[] {systemDir + File.separator + "pipe" + File.separator + "receiver"}
        : this.pipeReceiverFileDirs;
  }

  public void setPipeConsensusReceiverFileDirs(String[] pipeConsensusReceiverFileDirs) {
    this.pipeConsensusReceiverFileDirs = pipeConsensusReceiverFileDirs;
  }

  public String[] getPipeConsensusReceiverFileDirs() {
    return (Objects.isNull(this.pipeConsensusReceiverFileDirs)
            || this.pipeConsensusReceiverFileDirs.length == 0)
        ? new String[] {
          systemDir
              + File.separator
              + "pipe"
              + File.separator
              + "consensus"
              + File.separator
              + "receiver"
        }
        : this.pipeConsensusReceiverFileDirs;
  }

  public boolean isQuotaEnable() {
    return quotaEnable;
  }

  public void setQuotaEnable(boolean quotaEnable) {
    this.quotaEnable = quotaEnable;
  }

  public String getRateLimiterType() {
    return RateLimiterType;
  }

  public void setRateLimiterType(String rateLimiterType) {
    RateLimiterType = rateLimiterType;
  }

  public void setSortBufferSize(long sortBufferSize) {
    this.sortBufferSize = sortBufferSize;
  }

  public long getSortBufferSize() {
    return sortBufferSize;
  }

  public void setSortTmpDir(String sortTmpDir) {
    this.sortTmpDir = sortTmpDir;
  }

  public String getSortTmpDir() {
    return sortTmpDir;
  }

  public String getObjectStorageBucket() {
    throw new UnsupportedOperationException("object storage is not supported yet");
  }

  public long getDataRatisPeriodicSnapshotInterval() {
    return dataRatisPeriodicSnapshotInterval;
  }

  public void setDataRatisPeriodicSnapshotInterval(long dataRatisPeriodicSnapshotInterval) {
    this.dataRatisPeriodicSnapshotInterval = dataRatisPeriodicSnapshotInterval;
  }

  public long getSchemaRatisPeriodicSnapshotInterval() {
    return schemaRatisPeriodicSnapshotInterval;
  }

  public void setSchemaRatisPeriodicSnapshotInterval(long schemaRatisPeriodicSnapshotInterval) {
    this.schemaRatisPeriodicSnapshotInterval = schemaRatisPeriodicSnapshotInterval;
  }

  public boolean isEnableTsFileValidation() {
    return enableTsFileValidation;
  }

  public void setEnableTsFileValidation(boolean enableTsFileValidation) {
    this.enableTsFileValidation = enableTsFileValidation;
  }

  public long getInnerCompactionTaskSelectionModsFileThreshold() {
    return innerCompactionTaskSelectionModsFileThreshold;
  }

  public void setInnerCompactionTaskSelectionModsFileThreshold(
      long innerCompactionTaskSelectionModsFileThreshold) {
    this.innerCompactionTaskSelectionModsFileThreshold =
        innerCompactionTaskSelectionModsFileThreshold;
  }

  public double getInnerCompactionTaskSelectionDiskRedundancy() {
    return innerCompactionTaskSelectionDiskRedundancy;
  }

  public void setInnerCompactionTaskSelectionDiskRedundancy(
      double innerCompactionTaskSelectionDiskRedundancy) {
    this.innerCompactionTaskSelectionDiskRedundancy = innerCompactionTaskSelectionDiskRedundancy;
  }

  public TDataNodeLocation generateLocalDataNodeLocation() {
    TDataNodeLocation result = new TDataNodeLocation();
    result.setDataNodeId(getDataNodeId());
    result.setClientRpcEndPoint(new TEndPoint(getInternalAddress(), getRpcPort()));
    result.setInternalEndPoint(new TEndPoint(getInternalAddress(), getInternalPort()));
    result.setMPPDataExchangeEndPoint(
        new TEndPoint(getInternalAddress(), getMppDataExchangePort()));
    result.setDataRegionConsensusEndPoint(
        new TEndPoint(getInternalAddress(), getDataRegionConsensusPort()));
    result.setSchemaRegionConsensusEndPoint(
        new TEndPoint(getInternalAddress(), getSchemaRegionConsensusPort()));
    return result;
  }

  public CompressionType getWALCompressionAlgorithm() {
    return WALCompressionAlgorithm;
  }

  public void setWALCompressionAlgorithm(CompressionType WALCompressionAlgorithm) {
    this.WALCompressionAlgorithm = WALCompressionAlgorithm;
  }
}
