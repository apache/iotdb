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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.compaction.constant.CompactionPriority;
import org.apache.iotdb.db.engine.compaction.constant.CrossCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.constant.CrossCompactionSelector;
import org.apache.iotdb.db.engine.compaction.constant.InnerSeqCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.constant.InnerSequenceCompactionSelector;
import org.apache.iotdb.db.engine.compaction.constant.InnerUnseqCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.constant.InnerUnsequenceCompactionSelector;
import org.apache.iotdb.db.engine.storagegroup.timeindex.TimeIndexLevel;
import org.apache.iotdb.db.exception.LoadConfigurationException;
import org.apache.iotdb.db.metadata.LocalSchemaProcessor;
import org.apache.iotdb.db.service.thrift.impl.InfluxDBServiceImpl;
import org.apache.iotdb.db.service.thrift.impl.TSServiceImpl;
import org.apache.iotdb.db.wal.utils.WALMode;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;

public class IoTDBConfig {

  /* Names of Watermark methods */
  public static final String WATERMARK_GROUPED_LSB = "GroupBasedLSBMethod";
  static final String CONFIG_NAME = "iotdb-datanode.properties";
  private static final Logger logger = LoggerFactory.getLogger(IoTDBConfig.class);
  private static final String MULTI_DIR_STRATEGY_PREFIX =
      "org.apache.iotdb.db.conf.directories.strategy.";
  private static final String DEFAULT_MULTI_DIR_STRATEGY = "MaxDiskUsableSpaceFirstStrategy";

  private static final String STORAGE_GROUP_MATCHER = "([a-zA-Z0-9`_.\\-\\u2E80-\\u9FFF]+)";
  public static final Pattern STORAGE_GROUP_PATTERN = Pattern.compile(STORAGE_GROUP_MATCHER);

  // e.g., a31+/$%#&[]{}3e4, "a.b", 'a.b'
  private static final String NODE_NAME_MATCHER = "([^\n\t]+)";

  // e.g.,  .s1
  private static final String PARTIAL_NODE_MATCHER = "[" + PATH_SEPARATOR + "]" + NODE_NAME_MATCHER;

  private static final String NODE_MATCHER =
      "([" + PATH_SEPARATOR + "])?" + NODE_NAME_MATCHER + "(" + PARTIAL_NODE_MATCHER + ")*";

  public static final Pattern NODE_PATTERN = Pattern.compile(NODE_MATCHER);

  /** Shutdown system or set it to read-only mode when unrecoverable error occurs. */
  private boolean allowReadOnlyWhenErrorsOccur = true;

  /** Status of current system. */
  private volatile SystemStatus status = SystemStatus.NORMAL;

  /** whether to enable the mqtt service. */
  private boolean enableMQTTService = false;

  /** the mqtt service binding host. */
  private String mqttHost = "0.0.0.0";

  /** the mqtt service binding port. */
  private int mqttPort = 1883;

  /** the handler pool size for handing the mqtt messages. */
  private int mqttHandlerPoolSize = 1;

  /** the mqtt message payload formatter. */
  private String mqttPayloadFormatter = "json";

  /** max mqtt message size. Unit: byte */
  private int mqttMaxMessageSize = 1048576;

  /** Rpc binding address. */
  private String rpcAddress = "0.0.0.0";

  /** whether to use thrift compression. */
  private boolean rpcThriftCompressionEnable = false;

  /** whether to use Snappy compression before sending data through the network */
  private boolean rpcAdvancedCompressionEnable = false;

  /** Port which the JDBC server listens to. */
  private int rpcPort = 6667;

  /** Port which the influxdb protocol server listens to. */
  private int influxDBRpcPort = 8086;

  /** Rpc Selector thread num */
  private int rpcSelectorThreadNum = 1;

  /** Min concurrent client number */
  private int rpcMinConcurrentClientNum = Runtime.getRuntime().availableProcessors();

  /** Max concurrent client number */
  private int rpcMaxConcurrentClientNum = 65535;

  /** Memory allocated for the write process */
  private long allocateMemoryForWrite = Runtime.getRuntime().maxMemory() * 4 / 10;

  /** Memory allocated for the read process */
  private long allocateMemoryForRead = Runtime.getRuntime().maxMemory() * 3 / 10;

  /** Memory allocated for the mtree */
  private long allocateMemoryForSchema = Runtime.getRuntime().maxMemory() / 10;

  private volatile int maxQueryDeduplicatedPathNum = 1000;

  /** Ratio of memory allocated for buffered arrays */
  private double bufferedArraysMemoryProportion = 0.6;

  /** Flush proportion for system */
  private double flushProportion = 0.4;

  /** Reject proportion for system */
  private double rejectProportion = 0.8;

  /** If storage group increased more than this threshold, report to system. Unit: byte */
  private long storageGroupSizeReportThreshold = 16 * 1024 * 1024L;

  /** When inserting rejected, waiting period to check system again. Unit: millisecond */
  private int checkPeriodWhenInsertBlocked = 50;

  /** When inserting rejected exceeds this, throw an exception. Unit: millisecond */
  private int maxWaitingTimeWhenInsertBlockedInMs = 10000;

  /** this variable set timestamp precision as millisecond, microsecond or nanosecond */
  private String timestampPrecision = "ms";

  // region Write Ahead Log Configuration
  /** Write mode of wal */
  private volatile WALMode walMode = WALMode.ASYNC;

  /** WAL directories */
  private String[] walDirs = {
    IoTDBConstant.DEFAULT_BASE_DIR + File.separator + IoTDBConstant.WAL_FOLDER_NAME
  };

  /** Max number of wal nodes, each node corresponds to one wal directory */
  private int maxWalNodesNum = 0;

  /** Duration a wal flush operation will wait before calling fsync. Unit: millisecond */
  private volatile long fsyncWalDelayInMs = 3;

  /** Buffer size of each wal node. Unit: byte */
  private int walBufferSize = 16 * 1024 * 1024;

  /** Buffer entry size of each wal buffer. Unit: byte */
  private int walBufferEntrySize = 16 * 1024;

  /** Blocking queue capacity of each wal buffer */
  private int walBufferQueueCapacity = 50;

  /** Size threshold of each wal file. Unit: byte */
  private volatile long walFileSizeThresholdInByte = 10 * 1024 * 1024;

  /** Size threshold of each checkpoint file. Unit: byte */
  private volatile long checkpointFileSizeThresholdInByte = 3 * 1024 * 1024;

  /** Minimum ratio of effective information in wal files */
  private volatile double walMinEffectiveInfoRatio = 0.1;

  /**
   * MemTable size threshold for triggering MemTable snapshot in wal. When a memTable's size exceeds
   * this, wal can flush this memtable to disk, otherwise wal will snapshot this memtable in wal.
   * Unit: byte
   */
  private volatile long walMemTableSnapshotThreshold = 8 * 1024 * 1024;

  /** MemTable's max snapshot number in wal file */
  private volatile int maxWalMemTableSnapshotNum = 1;

  /** The period when outdated wal files are periodically deleted. Unit: millisecond */
  private volatile long deleteWalFilesPeriodInMs = 20 * 1000;
  // endregion

  /**
   * Size of log buffer for every MetaData operation. If the size of a MetaData operation plan is
   * larger than this parameter, then the MetaData operation plan will be rejected by SchemaRegion.
   * Unit: byte
   */
  private int mlogBufferSize = 1024 * 1024;

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

  /** System directory, including version file for each storage group and metadata */
  private String systemDir =
      IoTDBConstant.DEFAULT_BASE_DIR + File.separator + IoTDBConstant.SYSTEM_FOLDER_NAME;

  /** Schema directory, including storage set of values. */
  private String schemaDir =
      IoTDBConstant.DEFAULT_BASE_DIR
          + File.separator
          + IoTDBConstant.SYSTEM_FOLDER_NAME
          + File.separator
          + IoTDBConstant.SCHEMA_FOLDER_NAME;

  /** Performance tracing directory, stores performance tracing files */
  private String tracingDir =
      IoTDBConstant.DEFAULT_BASE_DIR + File.separator + IoTDBConstant.TRACING_FOLDER_NAME;

  /** Query directory, stores temporary files of query */
  private String queryDir =
      IoTDBConstant.DEFAULT_BASE_DIR + File.separator + IoTDBConstant.QUERY_FOLDER_NAME;

  /** External lib directory, stores user-uploaded JAR files */
  private String extDir = IoTDBConstant.EXT_FOLDER_NAME;

  /** External lib directory for UDF, stores user-uploaded JAR files */
  private String udfDir =
      IoTDBConstant.EXT_FOLDER_NAME + File.separator + IoTDBConstant.UDF_FOLDER_NAME;

  /** External temporary lib directory for storing downloaded JAR files */
  private String temporaryLibDir =
      IoTDBConstant.EXT_FOLDER_NAME + File.separator + IoTDBConstant.TMP_FOLDER_NAME;

  /** External lib directory for trigger, stores user-uploaded JAR files */
  private String triggerDir =
      IoTDBConstant.EXT_FOLDER_NAME + File.separator + IoTDBConstant.TRIGGER_FOLDER_NAME;

  /** External lib directory for ext Pipe plugins, stores user-defined JAR files */
  private String extPipeDir =
      IoTDBConstant.EXT_FOLDER_NAME + File.separator + IoTDBConstant.EXT_PIPE_FOLDER_NAME;

  /** External lib directory for MQTT, stores user-uploaded JAR files */
  private String mqttDir =
      IoTDBConstant.EXT_FOLDER_NAME + File.separator + IoTDBConstant.MQTT_FOLDER_NAME;

  /** Data directories. It can be settled as dataDirs = {"data1", "data2", "data3"}; */
  private String[] dataDirs = {
    IoTDBConstant.DEFAULT_BASE_DIR + File.separator + IoTDBConstant.DATA_FOLDER_NAME
  };

  /** Strategy of multiple directories. */
  private String multiDirStrategyClassName = null;

  /** Consensus directory. */
  private String consensusDir = IoTDBConstant.DEFAULT_BASE_DIR + File.separator + "consensus";

  private String dataRegionConsensusDir = consensusDir + File.separator + "data_region";

  private String schemaRegionConsensusDir = consensusDir + File.separator + "schema_region";

  /** Maximum MemTable number. Invalid when enableMemControl is true. */
  private int maxMemtableNumber = 0;

  /** The amount of data iterate each time in server */
  private int batchSize = 100000;

  /** How many threads can concurrently flush. When <= 0, use CPU core number. */
  private int concurrentFlushThread = Runtime.getRuntime().availableProcessors();

  /** How many threads can concurrently execute query statement. When <= 0, use CPU core number. */
  private int concurrentQueryThread = Runtime.getRuntime().availableProcessors();

  /** How many queries can be concurrently executed. When <= 0, use 1000. */
  private int maxAllowedConcurrentQueries = 1000;

  /**
   * How many threads can concurrently read data for raw data query. When <= 0, use CPU core number.
   */
  private int concurrentSubRawQueryThread = 8;

  /** Blocking queue size for read task in raw data query. */
  private int rawQueryBlockingQueueCapacity = 5;

  /** How many threads can concurrently evaluate windows. When <= 0, use CPU core number. */
  private int concurrentWindowEvaluationThread = Runtime.getRuntime().availableProcessors();

  /**
   * Max number of window evaluation tasks that can be pending for execution. When <= 0, the value
   * is 64 by default.
   */
  private int maxPendingWindowEvaluationTasks = 64;

  /** Is the write mem control for writing enable. */
  private boolean enableMemControl = true;

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

  /** When a memTable's size (in byte) exceeds this, the memtable is flushed to disk. Unit: byte */
  private long memtableSizeThreshold = 1024 * 1024 * 1024L;

  /** Whether to timed flush sequence tsfiles' memtables. */
  private boolean enableTimedFlushSeqMemtable = false;

  /**
   * If a memTable's created time is older than current time minus this, the memtable will be
   * flushed to disk.(only check sequence tsfiles' memtables) Unit: ms
   */
  private long seqMemtableFlushInterval = 60 * 60 * 1000L;

  /** The interval to check whether sequence memtables need flushing. Unit: ms */
  private long seqMemtableFlushCheckInterval = 10 * 60 * 1000L;

  /** Whether to timed flush unsequence tsfiles' memtables. */
  private boolean enableTimedFlushUnseqMemtable = true;

  /**
   * If a memTable's created time is older than current time minus this, the memtable will be
   * flushed to disk.(only check unsequence tsfiles' memtables) Unit: ms
   */
  private long unseqMemtableFlushInterval = 60 * 60 * 1000L;

  /** The interval to check whether unsequence memtables need flushing. Unit: ms */
  private long unseqMemtableFlushCheckInterval = 10 * 60 * 1000L;

  /** When average series point number reaches this, flush the memtable to disk */
  private int avgSeriesPointNumberThreshold = 100000;

  /** Enable inner space compaction for sequence files */
  private boolean enableSeqSpaceCompaction = true;

  /** Enable inner space compaction for unsequence files */
  private boolean enableUnseqSpaceCompaction = true;

  /** Compact the unsequence files into the overlapped sequence files */
  private boolean enableCrossSpaceCompaction = true;

  /**
   * The strategy of inner space compaction task. There are just one inner space compaction strategy
   * SIZE_TIRED_COMPACTION:
   */
  private InnerSequenceCompactionSelector innerSequenceCompactionSelector =
      InnerSequenceCompactionSelector.SIZE_TIERED;

  private InnerSeqCompactionPerformer innerSeqCompactionPerformer =
      InnerSeqCompactionPerformer.READ_CHUNK;

  private InnerUnsequenceCompactionSelector innerUnsequenceCompactionSelector =
      InnerUnsequenceCompactionSelector.SIZE_TIERED;

  private InnerUnseqCompactionPerformer innerUnseqCompactionPerformer =
      InnerUnseqCompactionPerformer.READ_POINT;

  /**
   * The strategy of cross space compaction task. There are just one cross space compaction strategy
   * SIZE_TIRED_COMPACTION:
   */
  private CrossCompactionSelector crossCompactionSelector = CrossCompactionSelector.REWRITE;

  private CrossCompactionPerformer crossCompactionPerformer = CrossCompactionPerformer.READ_POINT;

  /**
   * The priority of compaction task execution. There are three priority strategy INNER_CROSS:
   * prioritize inner space compaction, reduce the number of files first CROSS INNER: prioritize
   * cross space compaction, eliminate the unsequence files first BALANCE: alternate two compaction
   * types
   */
  private CompactionPriority compactionPriority = CompactionPriority.BALANCE;

  /** The target tsfile size in compaction, 1 GB by default */
  private long targetCompactionFileSize = 1073741824L;

  /** The target chunk size in compaction. */
  private long targetChunkSize = 1048576L;

  /** The target chunk point num in compaction. */
  private long targetChunkPointNum = 100000L;

  /**
   * If the chunk size is lower than this threshold, it will be deserialized into points, default is
   * 1 KB
   */
  private long chunkSizeLowerBoundInCompaction = 1024L;

  /**
   * If the chunk point num is lower than this threshold, it will be deserialized into points,
   * default is 100
   */
  private long chunkPointNumLowerBoundInCompaction = 100;

  /**
   * If compaction thread cannot acquire the write lock within this timeout, the compaction task
   * will be abort.
   */
  private long compactionAcquireWriteLockTimeout = 60_000L;

  /** The max candidate file num in inner space compaction */
  private int maxInnerCompactionCandidateFileNum = 30;

  /** The max candidate file num in cross space compaction */
  private int maxCrossCompactionCandidateFileNum = 1000;

  /** The interval of compaction task schedulation in each virtual storage group. The unit is ms. */
  private long compactionScheduleIntervalInMs = 60_000L;

  /** The interval of compaction task submission from queue in CompactionTaskMananger */
  private long compactionSubmissionIntervalInMs = 60_000L;

  /**
   * The number of sub compaction threads to be set up to perform compaction. Currently only works
   * for nonAligned data in cross space compaction and unseq inner space compaction.
   */
  private int subCompactionTaskNum = 4;

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

  /** Memory allocated proportion for timeIndex */
  private long allocateMemoryForTimeIndex = allocateMemoryForRead * 200 / 1001;

  /**
   * If true, we will estimate each query's possible memory footprint before executing it and deny
   * it if its estimated memory exceeds current free memory
   */
  private boolean enableQueryMemoryEstimation = true;

  /** Whether to enable Last cache */
  private boolean lastCacheEnable = true;

  /** Set true to enable statistics monitor service, false to disable statistics service. */
  private boolean enableStatMonitor = false;

  /** Set true to enable writing monitor time series. */
  private boolean enableMonitorSeriesWrite = false;

  /** Cache size of {@code checkAndGetDataTypeCache} in {@link LocalSchemaProcessor}. */
  private int schemaRegionDeviceNodeCacheSize = 10000;

  /** Cache size of {@code checkAndGetDataTypeCache} in {@link LocalSchemaProcessor}. */
  private int mRemoteSchemaCacheSize = 100000;

  /** Is external sort enable. */
  private boolean enableExternalSort = true;

  /**
   * The threshold of items in external sort. If the number of chunks participating in sorting
   * exceeds this threshold, external sorting is enabled, otherwise memory sorting is used.
   */
  private int externalSortThreshold = 1000;

  /** White list for sync */
  private String ipWhiteList = "0.0.0.0/0";

  /** The maximum number of retries when the sender fails to synchronize files to the receiver. */
  private int maxNumberOfSyncFileRetry = 5;

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
  private String rpcImplClassName = TSServiceImpl.class.getName();

  /** indicate whether current mode is mpp */
  private boolean mppMode = false;

  /** indicate whether current mode is cluster */
  private boolean isClusterMode = false;

  /**
   * the data node id for cluster mode, the default value -1 should be changed after join cluster
   */
  private int dataNodeId = -1;

  /** Replace implementation class of influxdb protocol service */
  private String influxdbImplClassName = InfluxDBServiceImpl.class.getName();

  /** whether use chunkBufferPool. */
  private boolean chunkBufferPoolEnable = false;

  /** Switch of watermark function */
  private boolean enableWatermark = false;

  /** Secret key for watermark */
  private String watermarkSecretKey = "IoTDB*2019@Beijing";

  /** Bit string of watermark */
  private String watermarkBitString = "100101110100";

  /** Watermark method and parameters */
  private String watermarkMethod = "GroupBasedLSBMethod(embed_row_cycle=2,embed_lsb_num=5)";

  /** Switch of creating schema automatically */
  private boolean enableAutoCreateSchema = true;

  /** register time series as which type when receiving boolean string "true" or "false" */
  private TSDataType booleanStringInferType = TSDataType.BOOLEAN;

  /** register time series as which type when receiving an integer string "67" */
  private TSDataType integerStringInferType = TSDataType.FLOAT;

  /**
   * register time series as which type when receiving an integer string and using float may lose
   * precision num > 2 ^ 24
   */
  private TSDataType longStringInferType = TSDataType.DOUBLE;

  /** register time series as which type when receiving a floating number string "6.7" */
  private TSDataType floatingStringInferType = TSDataType.FLOAT;

  /**
   * register time series as which type when receiving the Literal NaN. Values can be DOUBLE, FLOAT
   * or TEXT
   */
  private TSDataType nanStringInferType = TSDataType.DOUBLE;

  /** Storage group level when creating schema automatically is enabled */
  private int defaultStorageGroupLevel = 1;

  /** BOOLEAN encoding when creating schema automatically is enabled */
  private TSEncoding defaultBooleanEncoding = TSEncoding.RLE;

  /** INT32 encoding when creating schema automatically is enabled */
  private TSEncoding defaultInt32Encoding = TSEncoding.RLE;

  /** INT64 encoding when creating schema automatically is enabled */
  private TSEncoding defaultInt64Encoding = TSEncoding.RLE;

  /** FLOAT encoding when creating schema automatically is enabled */
  private TSEncoding defaultFloatEncoding = TSEncoding.GORILLA;

  /** DOUBLE encoding when creating schema automatically is enabled */
  private TSEncoding defaultDoubleEncoding = TSEncoding.GORILLA;

  /** TEXT encoding when creating schema automatically is enabled */
  private TSEncoding defaultTextEncoding = TSEncoding.PLAIN;

  /** How much memory (in byte) can be used by a single merge task. */
  private long crossCompactionMemoryBudget = (long) (Runtime.getRuntime().maxMemory() * 0.1);

  /** How many threads will be set up to perform upgrade tasks. */
  private int upgradeThreadNum = 1;

  /** How many threads will be set up to perform settle tasks. */
  private int settleThreadNum = 1;

  /**
   * If one merge file selection runs for more than this time, it will be ended and its current
   * selection will be used as final selection. When < 0, it means time is unbounded. Unit:
   * millisecond
   */
  private long crossCompactionFileSelectionTimeBudget = 30 * 1000L;

  /**
   * A global merge will be performed each such interval, that is, each storage group will be merged
   * (if proper merge candidates can be found). Unit: second.
   */
  private long mergeIntervalSec = 0L;

  /** The limit of compaction merge can reach per second */
  private int compactionWriteThroughputMbPerSec = 16;

  /**
   * How many thread will be set up to perform compaction, 10 by default. Set to 1 when less than or
   * equal to 0.
   */
  private int concurrentCompactionThread = 10;

  /*
   * How many thread will be set up to perform continuous queries. When <= 0, use max(1, CPU core number / 2).
   */
  private int continuousQueryThreadNum =
      Math.max(1, Runtime.getRuntime().availableProcessors() / 2);

  /*
   * Maximum number of continuous query tasks that can be pending for execution. When <= 0, the value is
   * 64 by default.
   */
  private int maxPendingContinuousQueryTasks = 64;

  /*
   * Minimum every interval to perform continuous query.
   * The every interval of continuous query instances should not be lower than this limit.
   */
  private long continuousQueryMinimumEveryInterval = 1000;

  /**
   * The size of log buffer for every CQ management operation plan. If the size of a CQ management
   * operation plan is larger than this parameter, the CQ management operation plan will be rejected
   * by CQManager. Unit: byte
   */
  private int cqlogBufferSize = 1024 * 1024;

  /**
   * The maximum number of rows can be processed in insert-tablet-plan when executing select-into
   * statements.
   */
  private int selectIntoInsertTabletPlanRowLimit = 10000;

  /**
   * When the insert plan column count reaches the specified threshold, which means that the plan is
   * relatively large. At this time, may be enabled multithreading. If the tablet is small, the time
   * of each insertion is short. If we enable multithreading, we also need to consider the switching
   * loss between threads, so we need to judge the size of the tablet.
   */
  private int insertMultiTabletEnableMultithreadingColumnThreshold = 10;

  /** Default TSfile storage is in local file system */
  private FSType tsFileStorageFs = FSType.LOCAL;

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

  /** the num of memtable in each storage group */
  private int concurrentWritingTimePartition = 1;

  /** the default fill interval in LinearFill and PreviousFill, -1 means infinite past time */
  private int defaultFillInterval = -1;

  /** The default value of primitive array size in array pool */
  private int primitiveArraySize = 32;

  /** whether enable data partition. If disabled, all data belongs to partition 0 */
  private boolean enablePartition = true;

  /**
   * Time range for partitioning data inside each storage group, the unit is second. Default time is
   * a day.
   */
  private long partitionInterval = 86400;

  /**
   * Level of TimeIndex, which records the start time and end time of TsFileResource. Currently,
   * DEVICE_TIME_INDEX and FILE_TIME_INDEX are supported, and could not be changed after first set.
   */
  private TimeIndexLevel timeIndexLevel = TimeIndexLevel.DEVICE_TIME_INDEX;

  // just for test
  // wait for 60 second by default.
  private int thriftServerAwaitTimeForStopService = 60;

  // max size for tag and attribute of one time series
  private int tagAttributeTotalSize = 700;

  // Interval num of tag and attribute records when force flushing to disk
  private int tagAttributeFlushInterval = 1000;

  // In one insert (one device, one timestamp, multiple measurements),
  // if enable partial insert, one measurement failure will not impact other measurements
  private boolean enablePartialInsert = true;

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

  /** The cached record size (in MB) of each series in group by fill query */
  private float groupByFillCacheSizeInMB = (float) 1.0;

  // time in nanosecond precision when starting up
  private long startUpNanosecond = System.nanoTime();

  /** Unit: byte */
  private int thriftMaxFrameSize = 536870912;

  private int thriftDefaultBufferSize = RpcUtils.THRIFT_DEFAULT_BUF_CAPACITY;

  /** time interval in minute for calculating query frequency. Unit: minute */
  private int frequencyIntervalInMinute = 1;

  /** time cost(ms) threshold for slow query. Unit: millisecond */
  private long slowQueryThreshold = 5000;

  /**
   * whether enable the rpc service. This parameter has no a corresponding field in the
   * iotdb-datanode.properties
   */
  private boolean enableRpcService = true;

  /**
   * whether enable the influxdb rpc service. This parameter has no a corresponding field in the
   * iotdb-datanode.properties
   */
  private boolean enableInfluxDBRpcService = false;

  /** the size of ioTaskQueue */
  private int ioTaskQueueSizeForFlushing = 10;

  /** the number of data regions per user-defined storage group */
  private int dataRegionNum = 1;

  /** the interval to log recover progress of each vsg when starting iotdb */
  private long recoveryLogIntervalInMs = 5_000L;

  private boolean enableDiscardOutOfOrderData = false;

  /** the method to transform device path to device id, can be 'Plain' or 'SHA256' */
  private String deviceIDTransformationMethod = "Plain";

  /** whether to use id table. ATTENTION: id table is not compatible with alias */
  private boolean enableIDTable = false;

  /**
   * whether create mapping file of id table. This file can map device id in tsfile to device path
   */
  private boolean enableIDTableLogFile = false;

  /** whether to use persistent schema mode */
  private String schemaEngineMode = "Memory";

  /** the memory used for metadata cache when using persistent schema */
  private int cachedMNodeSizeInSchemaFileMode = -1;

  /** the minimum size (in bytes) of segment inside a schema file page */
  private short minimumSegmentInSchemaFile = 0;

  /** cache size for pages in one schema file */
  private int pageCacheSizeInSchemaFile = 1024;

  /** Internal address for data node */
  private String internalAddress = "127.0.0.1";

  /** Internal port for coordinator */
  private int internalPort = 9003;

  /** Internal port for dataRegion consensus protocol */
  private int dataRegionConsensusPort = 40010;

  /** Internal port for schemaRegion consensus protocol */
  private int schemaRegionConsensusPort = 50010;

  /** Ip and port of config nodes. */
  private List<TEndPoint> targetConfigNodeList =
      Collections.singletonList(new TEndPoint("127.0.0.1", 22277));

  /** The max time of data node waiting to join into the cluster */
  private long joinClusterTimeOutMs = TimeUnit.SECONDS.toMillis(5);

  /**
   * The consensus protocol class for data region. The Datanode should communicate with ConfigNode
   * on startup and set this variable so that the correct class name can be obtained later when the
   * data region consensus layer singleton is initialized
   */
  private String dataRegionConsensusProtocolClass = ConsensusFactory.RatisConsensus;

  /**
   * The consensus protocol class for schema region. The Datanode should communicate with ConfigNode
   * on startup and set this variable so that the correct class name can be obtained later when the
   * schema region consensus layer singleton is initialized
   */
  private String schemaRegionConsensusProtocolClass = ConsensusFactory.RatisConsensus;

  /**
   * The series partition executor class. The Datanode should communicate with ConfigNode on startup
   * and set this variable so that the correct class name can be obtained later when calculating the
   * series partition
   */
  private String seriesPartitionExecutorClass =
      "org.apache.iotdb.commons.partition.executor.hash.APHashExecutor";

  /** The number of series partitions in a storage group */
  private int seriesPartitionSlotNum = 10000;

  /** Port that mpp data exchange thrift service listen to. */
  private int mppDataExchangePort = 8777;

  /** Core pool size of mpp data exchange. */
  private int mppDataExchangeCorePoolSize = 10;

  /** Max pool size of mpp data exchange. */
  private int mppDataExchangeMaxPoolSize = 10;

  /** Thread keep alive time in ms of mpp data exchange. */
  private int mppDataExchangeKeepAliveTimeInMs = 1000;

  /** Thrift socket and connection timeout between data node and config node. */
  private int connectionTimeoutInMS = (int) TimeUnit.SECONDS.toMillis(20);

  /**
   * ClientManager will have so many selector threads (TAsyncClientManager) to distribute to its
   * clients.
   */
  private int selectorNumOfClientManager =
      Runtime.getRuntime().availableProcessors() / 4 > 0
          ? Runtime.getRuntime().availableProcessors() / 4
          : 1;

  /**
   * Cache size of partition cache in {@link
   * org.apache.iotdb.db.mpp.plan.analyze.ClusterPartitionFetcher}
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

  /** ThreadPool size for read operation in coordinator */
  private int coordinatorReadExecutorSize = 20;

  /** ThreadPool size for write operation in coordinator */
  private int coordinatorWriteExecutorSize = 50;

  /** Memory allocated for schemaRegion */
  private long allocateMemoryForSchemaRegion = allocateMemoryForSchema * 8 / 10;

  /** Memory allocated for SchemaCache */
  private long allocateMemoryForSchemaCache = allocateMemoryForSchema / 10;

  /** Memory allocated for PartitionCache */
  private long allocateMemoryForPartitionCache = 0;

  /** Memory allocated for LastCache */
  private long allocateMemoryForLastCache = allocateMemoryForSchema / 10;

  private String readConsistencyLevel = "strong";

  /** Maximum execution time of a DriverTask */
  /** Maximum execution time of a DriverTask */
  private int driverTaskExecutionTimeSliceInMs = 100;

  /** Maximum size of wal buffer used in MultiLeader consensus. Unit: byte */
  private long throttleThreshold = 50 * 1024 * 1024 * 1024L;

  IoTDBConfig() {}

  public float getUdfMemoryBudgetInMB() {
    return udfMemoryBudgetInMB;
  }

  public void setUdfMemoryBudgetInMB(float udfMemoryBudgetInMB) {
    this.udfMemoryBudgetInMB = udfMemoryBudgetInMB;
  }

  public float getGroupByFillCacheSizeInMB() {
    return groupByFillCacheSizeInMB;
  }

  public void setGroupByFillCacheSizeInMB(float groupByFillCacheSizeInMB) {
    this.groupByFillCacheSizeInMB = groupByFillCacheSizeInMB;
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

  public int getUdfInitialByteArrayLengthForMemoryControl() {
    return udfInitialByteArrayLengthForMemoryControl;
  }

  public void setUdfInitialByteArrayLengthForMemoryControl(
      int udfInitialByteArrayLengthForMemoryControl) {
    this.udfInitialByteArrayLengthForMemoryControl = udfInitialByteArrayLengthForMemoryControl;
  }

  public int getConcurrentWritingTimePartition() {
    return concurrentWritingTimePartition;
  }

  public void setConcurrentWritingTimePartition(int concurrentWritingTimePartition) {
    this.concurrentWritingTimePartition = concurrentWritingTimePartition;
  }

  public int getDefaultFillInterval() {
    return defaultFillInterval;
  }

  public void setDefaultFillInterval(int defaultFillInterval) {
    this.defaultFillInterval = defaultFillInterval;
  }

  public boolean isEnablePartition() {
    return enablePartition;
  }

  public void setEnablePartition(boolean enablePartition) {
    this.enablePartition = enablePartition;
  }

  public long getPartitionInterval() {
    return partitionInterval;
  }

  public void setPartitionInterval(long partitionInterval) {
    this.partitionInterval = partitionInterval;
  }

  public TimeIndexLevel getTimeIndexLevel() {
    return timeIndexLevel;
  }

  public void setTimeIndexLevel(String timeIndexLevel) {
    this.timeIndexLevel = TimeIndexLevel.valueOf(timeIndexLevel);
  }

  void updatePath() {
    formulateFolders();
    confirmMultiDirStrategy();
  }

  /** if the folders are relative paths, add IOTDB_HOME as the path prefix */
  private void formulateFolders() {
    systemDir = addHomeDir(systemDir);
    schemaDir = addHomeDir(schemaDir);
    tracingDir = addHomeDir(tracingDir);
    consensusDir = addHomeDir(consensusDir);
    dataRegionConsensusDir = addHomeDir(dataRegionConsensusDir);
    schemaRegionConsensusDir = addHomeDir(schemaRegionConsensusDir);
    indexRootFolder = addHomeDir(indexRootFolder);
    extDir = addHomeDir(extDir);
    udfDir = addHomeDir(udfDir);
    temporaryLibDir = addHomeDir(temporaryLibDir);
    triggerDir = addHomeDir(triggerDir);
    mqttDir = addHomeDir(mqttDir);
    for (int i = 0; i < walDirs.length; i++) {
      walDirs[i] = addHomeDir(walDirs[i]);
    }
    extPipeDir = addHomeDir(extPipeDir);

    if (TSFileDescriptor.getInstance().getConfig().getTSFileStorageFs().equals(FSType.HDFS)) {
      String hdfsDir = getHdfsDir();
      queryDir = hdfsDir + File.separatorChar + queryDir;
      for (int i = 0; i < dataDirs.length; i++) {
        dataDirs[i] = hdfsDir + File.separatorChar + dataDirs[i];
      }
    } else {
      queryDir = addHomeDir(queryDir);
      for (int i = 0; i < dataDirs.length; i++) {
        dataDirs[i] = addHomeDir(dataDirs[i]);
      }
    }
  }

  void reloadDataDirs(String[] dataDirs) throws LoadConfigurationException {
    // format data directories
    if (TSFileDescriptor.getInstance().getConfig().getTSFileStorageFs().equals(FSType.HDFS)) {
      String hdfsDir = getHdfsDir();
      for (int i = 0; i < dataDirs.length; i++) {
        dataDirs[i] = hdfsDir + File.separatorChar + dataDirs[i];
      }
    } else {
      for (int i = 0; i < dataDirs.length; i++) {
        dataDirs[i] = addHomeDir(dataDirs[i]);
      }
    }
    // make sure old data directories not removed
    HashSet<String> newDirs = new HashSet<>(Arrays.asList(dataDirs));
    for (String oldDir : this.dataDirs) {
      if (!newDirs.contains(oldDir)) {
        String msg =
            String.format("%s is removed from data_dirs parameter, please add it back.", oldDir);
        logger.error(msg);
        throw new LoadConfigurationException(msg);
      }
    }
    this.dataDirs = dataDirs;
    DirectoryManager.getInstance().updateFileFolders();
  }

  private String addHomeDir(String dir) {
    String homeDir = System.getProperty(IoTDBConstant.IOTDB_HOME, null);
    if (!new File(dir).isAbsolute() && homeDir != null && homeDir.length() > 0) {
      if (!homeDir.endsWith(File.separator)) {
        dir = homeDir + File.separatorChar + dir;
      } else {
        dir = homeDir + dir;
      }
    }
    return dir;
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
    return dataDirs;
  }

  public void setDataDirs(String[] dataDirs) {
    this.dataDirs = dataDirs;
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

  public int getInfluxDBRpcPort() {
    return influxDBRpcPort;
  }

  public void setInfluxDBRpcPort(int influxDBRpcPort) {
    this.influxDBRpcPort = influxDBRpcPort;
  }

  public String getTimestampPrecision() {
    return timestampPrecision;
  }

  public void setTimestampPrecision(String timestampPrecision) {
    if (!("ms".equals(timestampPrecision)
        || "us".equals(timestampPrecision)
        || "ns".equals(timestampPrecision))) {
      logger.error(
          "Wrong timestamp precision, please set as: ms, us or ns ! Current is: "
              + timestampPrecision);
      System.exit(-1);
    }
    this.timestampPrecision = timestampPrecision;
  }

  public boolean isEnableDiscardOutOfOrderData() {
    return enableDiscardOutOfOrderData;
  }

  public void setEnableDiscardOutOfOrderData(boolean enableDiscardOutOfOrderData) {
    this.enableDiscardOutOfOrderData = enableDiscardOutOfOrderData;
  }

  public String getSystemDir() {
    return systemDir;
  }

  void setSystemDir(String systemDir) {
    this.systemDir = systemDir;
  }

  public String getSchemaDir() {
    return schemaDir;
  }

  public void setSchemaDir(String schemaDir) {
    this.schemaDir = schemaDir;
  }

  public String getTracingDir() {
    return tracingDir;
  }

  void setTracingDir(String tracingDir) {
    this.tracingDir = tracingDir;
  }

  public String getQueryDir() {
    return queryDir;
  }

  void setQueryDir(String queryDir) {
    this.queryDir = queryDir;
  }

  public String getConsensusDir() {
    return consensusDir;
  }

  public void setConsensusDir(String consensusDir) {
    this.consensusDir = consensusDir;
    setDataRegionConsensusDir(consensusDir + File.separator + "data_region");
    setSchemaRegionConsensusDir(consensusDir + File.separator + "schema_region");
  }

  public String getDataRegionConsensusDir() {
    return dataRegionConsensusDir;
  }

  public void setDataRegionConsensusDir(String dataRegionConsensusDir) {
    this.dataRegionConsensusDir = dataRegionConsensusDir;
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

  public String getTemporaryLibDir() {
    return temporaryLibDir;
  }

  public void setUdfDir(String udfDir) {
    this.udfDir = udfDir;
  }

  public String getTriggerDir() {
    return triggerDir;
  }

  public void setTriggerDir(String triggerDir) {
    this.triggerDir = triggerDir;
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

  public int getConcurrentFlushThread() {
    return concurrentFlushThread;
  }

  void setConcurrentFlushThread(int concurrentFlushThread) {
    this.concurrentFlushThread = concurrentFlushThread;
  }

  public int getConcurrentQueryThread() {
    return concurrentQueryThread;
  }

  public void setConcurrentQueryThread(int concurrentQueryThread) {
    this.concurrentQueryThread = concurrentQueryThread;
  }

  public int getMaxAllowedConcurrentQueries() {
    return maxAllowedConcurrentQueries;
  }

  public void setMaxAllowedConcurrentQueries(int maxAllowedConcurrentQueries) {
    this.maxAllowedConcurrentQueries = maxAllowedConcurrentQueries;
  }

  public int getConcurrentSubRawQueryThread() {
    return concurrentSubRawQueryThread;
  }

  void setConcurrentSubRawQueryThread(int concurrentSubRawQueryThread) {
    this.concurrentSubRawQueryThread = concurrentSubRawQueryThread;
  }

  public long getMaxBytesPerQuery() {
    return allocateMemoryForDataExchange / concurrentQueryThread;
  }

  public int getRawQueryBlockingQueueCapacity() {
    return rawQueryBlockingQueueCapacity;
  }

  public void setRawQueryBlockingQueueCapacity(int rawQueryBlockingQueueCapacity) {
    this.rawQueryBlockingQueueCapacity = rawQueryBlockingQueueCapacity;
  }

  public int getConcurrentWindowEvaluationThread() {
    return concurrentWindowEvaluationThread;
  }

  public void setConcurrentWindowEvaluationThread(int concurrentWindowEvaluationThread) {
    this.concurrentWindowEvaluationThread = concurrentWindowEvaluationThread;
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

  public int getRpcSelectorThreadNum() {
    return rpcSelectorThreadNum;
  }

  public void setRpcSelectorThreadNum(int rpcSelectorThreadNum) {
    this.rpcSelectorThreadNum = rpcSelectorThreadNum;
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

  public int getSchemaRegionDeviceNodeCacheSize() {
    return schemaRegionDeviceNodeCacheSize;
  }

  void setSchemaRegionDeviceNodeCacheSize(int schemaRegionDeviceNodeCacheSize) {
    this.schemaRegionDeviceNodeCacheSize = schemaRegionDeviceNodeCacheSize;
  }

  public int getmRemoteSchemaCacheSize() {
    return mRemoteSchemaCacheSize;
  }

  public void setmRemoteSchemaCacheSize(int mRemoteSchemaCacheSize) {
    this.mRemoteSchemaCacheSize = mRemoteSchemaCacheSize;
  }

  public int getMaxNumberOfSyncFileRetry() {
    return maxNumberOfSyncFileRetry;
  }

  public void setMaxNumberOfSyncFileRetry(int maxNumberOfSyncFileRetry) {
    this.maxNumberOfSyncFileRetry = maxNumberOfSyncFileRetry;
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

  public String getIpWhiteList() {
    return ipWhiteList;
  }

  public void setIpWhiteList(String ipWhiteList) {
    this.ipWhiteList = ipWhiteList;
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

  boolean isAllowReadOnlyWhenErrorsOccur() {
    return allowReadOnlyWhenErrorsOccur;
  }

  void setAllowReadOnlyWhenErrorsOccur(boolean allowReadOnlyWhenErrorsOccur) {
    this.allowReadOnlyWhenErrorsOccur = allowReadOnlyWhenErrorsOccur;
  }

  public boolean isReadOnly() {
    return status == SystemStatus.READ_ONLY
        || (status == SystemStatus.ERROR && allowReadOnlyWhenErrorsOccur);
  }

  public SystemStatus getSystemStatus() {
    return status;
  }

  public void setSystemStatus(SystemStatus newStatus) {
    if (newStatus == SystemStatus.READ_ONLY) {
      logger.error(
          "Change system mode to read-only! Only query statements are permitted!",
          new RuntimeException("System mode is set to READ_ONLY"));
    } else if (newStatus == SystemStatus.ERROR) {
      if (allowReadOnlyWhenErrorsOccur) {
        logger.error(
            "Unrecoverable error occurs! Make system read-only when allow_read_only_when_errors_occur is true.",
            new RuntimeException("System mode is set to READ_ONLY"));
        newStatus = SystemStatus.READ_ONLY;
      } else {
        logger.error(
            "Unrecoverable error occurs! Shutdown system directly when allow_read_only_when_errors_occur is false.",
            new RuntimeException("System mode is set to ERROR"));
        System.exit(-1);
      }
    } else {
      logger.info("Set system mode from {} to NORMAL.", status);
    }
    this.status = newStatus;
  }

  public String getRpcImplClassName() {
    return rpcImplClassName;
  }

  public String getInfluxDBImplClassName() {
    return influxdbImplClassName;
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

  public String[] getWalDirs() {
    return walDirs;
  }

  public void setWalDirs(String[] walDirs) {
    this.walDirs = walDirs;
  }

  public int getMaxWalNodesNum() {
    return maxWalNodesNum;
  }

  void setMaxWalNodesNum(int maxWalNodesNum) {
    this.maxWalNodesNum = maxWalNodesNum;
  }

  public long getFsyncWalDelayInMs() {
    return fsyncWalDelayInMs;
  }

  void setFsyncWalDelayInMs(long fsyncWalDelayInMs) {
    this.fsyncWalDelayInMs = fsyncWalDelayInMs;
  }

  public int getWalBufferSize() {
    return walBufferSize;
  }

  public void setWalBufferSize(int walBufferSize) {
    this.walBufferSize = walBufferSize;
  }

  public int getWalBufferEntrySize() {
    return walBufferEntrySize;
  }

  void setWalBufferEntrySize(int walBufferEntrySize) {
    this.walBufferEntrySize = walBufferEntrySize;
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

  void setWalFileSizeThresholdInByte(long walFileSizeThresholdInByte) {
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

  public boolean isChunkBufferPoolEnable() {
    return chunkBufferPoolEnable;
  }

  void setChunkBufferPoolEnable(boolean chunkBufferPoolEnable) {
    this.chunkBufferPoolEnable = chunkBufferPoolEnable;
  }

  public long getCrossCompactionMemoryBudget() {
    return crossCompactionMemoryBudget;
  }

  public void setCrossCompactionMemoryBudget(long crossCompactionMemoryBudget) {
    this.crossCompactionMemoryBudget = crossCompactionMemoryBudget;
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

  public long getStorageGroupSizeReportThreshold() {
    return storageGroupSizeReportThreshold;
  }

  public void setStorageGroupSizeReportThreshold(long storageGroupSizeReportThreshold) {
    this.storageGroupSizeReportThreshold = storageGroupSizeReportThreshold;
  }

  public long getAllocateMemoryForWrite() {
    return allocateMemoryForWrite;
  }

  public void setAllocateMemoryForWrite(long allocateMemoryForWrite) {
    this.allocateMemoryForWrite = allocateMemoryForWrite;
  }

  public long getAllocateMemoryForSchema() {
    return allocateMemoryForSchema;
  }

  public void setAllocateMemoryForSchema(long allocateMemoryForSchema) {
    this.allocateMemoryForSchema = allocateMemoryForSchema;
  }

  public long getAllocateMemoryForRead() {
    return allocateMemoryForRead;
  }

  void setAllocateMemoryForRead(long allocateMemoryForRead) {
    this.allocateMemoryForRead = allocateMemoryForRead;
  }

  public boolean isEnableExternalSort() {
    return enableExternalSort;
  }

  void setEnableExternalSort(boolean enableExternalSort) {
    this.enableExternalSort = enableExternalSort;
  }

  public int getExternalSortThreshold() {
    return externalSortThreshold;
  }

  void setExternalSortThreshold(int externalSortThreshold) {
    this.externalSortThreshold = externalSortThreshold;
  }

  public boolean isEnablePartialInsert() {
    return enablePartialInsert;
  }

  public void setEnablePartialInsert(boolean enablePartialInsert) {
    this.enablePartialInsert = enablePartialInsert;
  }

  public int getConcurrentCompactionThread() {
    return concurrentCompactionThread;
  }

  public void setConcurrentCompactionThread(int concurrentCompactionThread) {
    this.concurrentCompactionThread = concurrentCompactionThread;
  }

  public int getContinuousQueryThreadNum() {
    return continuousQueryThreadNum;
  }

  public void setContinuousQueryThreadNum(int continuousQueryThreadNum) {
    this.continuousQueryThreadNum = continuousQueryThreadNum;
  }

  public int getMaxPendingContinuousQueryTasks() {
    return maxPendingContinuousQueryTasks;
  }

  public void setMaxPendingContinuousQueryTasks(int maxPendingContinuousQueryTasks) {
    this.maxPendingContinuousQueryTasks = maxPendingContinuousQueryTasks;
  }

  public long getContinuousQueryMinimumEveryInterval() {
    return continuousQueryMinimumEveryInterval;
  }

  public void setContinuousQueryMinimumEveryInterval(long minimumEveryInterval) {
    this.continuousQueryMinimumEveryInterval = minimumEveryInterval;
  }

  public int getCqlogBufferSize() {
    return cqlogBufferSize;
  }

  public void setCqlogBufferSize(int cqlogBufferSize) {
    this.cqlogBufferSize = cqlogBufferSize;
  }

  public void setSelectIntoInsertTabletPlanRowLimit(int selectIntoInsertTabletPlanRowLimit) {
    this.selectIntoInsertTabletPlanRowLimit = selectIntoInsertTabletPlanRowLimit;
  }

  public int getSelectIntoInsertTabletPlanRowLimit() {
    return selectIntoInsertTabletPlanRowLimit;
  }

  public int getInsertMultiTabletEnableMultithreadingColumnThreshold() {
    return insertMultiTabletEnableMultithreadingColumnThreshold;
  }

  public void setInsertMultiTabletEnableMultithreadingColumnThreshold(
      int insertMultiTabletEnableMultithreadingColumnThreshold) {
    this.insertMultiTabletEnableMultithreadingColumnThreshold =
        insertMultiTabletEnableMultithreadingColumnThreshold;
  }

  public int getCompactionWriteThroughputMbPerSec() {
    return compactionWriteThroughputMbPerSec;
  }

  public void setCompactionWriteThroughputMbPerSec(int compactionWriteThroughputMbPerSec) {
    this.compactionWriteThroughputMbPerSec = compactionWriteThroughputMbPerSec;
  }

  public boolean isEnableMemControl() {
    return enableMemControl;
  }

  public void setEnableMemControl(boolean enableMemControl) {
    this.enableMemControl = enableMemControl;
  }

  public long getMemtableSizeThreshold() {
    return memtableSizeThreshold;
  }

  public void setMemtableSizeThreshold(long memtableSizeThreshold) {
    this.memtableSizeThreshold = memtableSizeThreshold;
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

  public int getAvgSeriesPointNumberThreshold() {
    return avgSeriesPointNumberThreshold;
  }

  public void setAvgSeriesPointNumberThreshold(int avgSeriesPointNumberThreshold) {
    this.avgSeriesPointNumberThreshold = avgSeriesPointNumberThreshold;
  }

  public long getCrossCompactionFileSelectionTimeBudget() {
    return crossCompactionFileSelectionTimeBudget;
  }

  void setCrossCompactionFileSelectionTimeBudget(long crossCompactionFileSelectionTimeBudget) {
    this.crossCompactionFileSelectionTimeBudget = crossCompactionFileSelectionTimeBudget;
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
  }

  public long getAllocateMemoryForTimeIndex() {
    return allocateMemoryForTimeIndex;
  }

  public void setAllocateMemoryForTimeIndex(long allocateMemoryForTimeIndex) {
    this.allocateMemoryForTimeIndex = allocateMemoryForTimeIndex;
  }

  public boolean isEnableQueryMemoryEstimation() {
    return enableQueryMemoryEstimation;
  }

  public void setEnableQueryMemoryEstimation(boolean enableQueryMemoryEstimation) {
    this.enableQueryMemoryEstimation = enableQueryMemoryEstimation;
  }

  public boolean isLastCacheEnabled() {
    return lastCacheEnable;
  }

  public void setEnableLastCache(boolean lastCacheEnable) {
    this.lastCacheEnable = lastCacheEnable;
  }

  public boolean isEnableWatermark() {
    return enableWatermark;
  }

  public void setEnableWatermark(boolean enableWatermark) {
    this.enableWatermark = enableWatermark;
  }

  public String getWatermarkSecretKey() {
    return watermarkSecretKey;
  }

  public void setWatermarkSecretKey(String watermarkSecretKey) {
    this.watermarkSecretKey = watermarkSecretKey;
  }

  public String getWatermarkBitString() {
    return watermarkBitString;
  }

  public void setWatermarkBitString(String watermarkBitString) {
    this.watermarkBitString = watermarkBitString;
  }

  String getWatermarkMethod() {
    return this.watermarkMethod;
  }

  public void setWatermarkMethod(String watermarkMethod) {
    this.watermarkMethod = watermarkMethod;
  }

  public String getWatermarkMethodName() {
    return watermarkMethod.split("\\(")[0];
  }

  public int getWatermarkParamMarkRate() {
    return Integer.parseInt(getWatermarkParamValue("embed_row_cycle", "5"));
  }

  public int getWatermarkParamMaxRightBit() {
    return Integer.parseInt(getWatermarkParamValue("embed_lsb_num", "5"));
  }

  private String getWatermarkParamValue(String key, String defaultValue) {
    String res = getWatermarkParamValue(key);
    if (res != null) {
      return res;
    }
    return defaultValue;
  }

  private String getWatermarkParamValue(String key) {
    String pattern = key + "=(\\w*)";
    Pattern r = Pattern.compile(pattern);
    Matcher m = r.matcher(watermarkMethod);
    if (m.find() && m.groupCount() > 0) {
      return m.group(1);
    }
    return null;
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
    this.booleanStringInferType = booleanStringInferType;
  }

  public TSDataType getIntegerStringInferType() {
    return integerStringInferType;
  }

  public void setIntegerStringInferType(TSDataType integerStringInferType) {
    this.integerStringInferType = integerStringInferType;
  }

  public void setLongStringInferType(TSDataType longStringInferType) {
    this.longStringInferType = longStringInferType;
  }

  public TSDataType getLongStringInferType() {
    return longStringInferType;
  }

  public TSDataType getFloatingStringInferType() {
    return floatingStringInferType;
  }

  public void setFloatingStringInferType(TSDataType floatingNumberStringInferType) {
    this.floatingStringInferType = floatingNumberStringInferType;
  }

  public TSDataType getNanStringInferType() {
    return nanStringInferType;
  }

  public void setNanStringInferType(TSDataType nanStringInferType) {
    if (nanStringInferType != TSDataType.DOUBLE
        && nanStringInferType != TSDataType.FLOAT
        && nanStringInferType != TSDataType.TEXT) {
      throw new IllegalArgumentException(
          "Config Property nan_string_infer_type can only be FLOAT, DOUBLE or TEXT but is "
              + nanStringInferType);
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

  public int getUpgradeThreadNum() {
    return upgradeThreadNum;
  }

  public int getSettleThreadNum() {
    return settleThreadNum;
  }

  void setUpgradeThreadNum(int upgradeThreadNum) {
    this.upgradeThreadNum = upgradeThreadNum;
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

  public int getTagAttributeTotalSize() {
    return tagAttributeTotalSize;
  }

  public void setTagAttributeTotalSize(int tagAttributeTotalSize) {
    this.tagAttributeTotalSize = tagAttributeTotalSize;
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

  public long getStartUpNanosecond() {
    return startUpNanosecond;
  }

  public int getThriftMaxFrameSize() {
    return thriftMaxFrameSize;
  }

  public void setThriftMaxFrameSize(int thriftMaxFrameSize) {
    this.thriftMaxFrameSize = thriftMaxFrameSize;
    RpcTransportFactory.setThriftMaxFrameSize(this.thriftMaxFrameSize);
  }

  public int getThriftDefaultBufferSize() {
    return thriftDefaultBufferSize;
  }

  public void setThriftDefaultBufferSize(int thriftDefaultBufferSize) {
    this.thriftDefaultBufferSize = thriftDefaultBufferSize;
    RpcTransportFactory.setDefaultBufferCapacity(this.thriftDefaultBufferSize);
  }

  public int getMaxQueryDeduplicatedPathNum() {
    return maxQueryDeduplicatedPathNum;
  }

  public void setMaxQueryDeduplicatedPathNum(int maxQueryDeduplicatedPathNum) {
    this.maxQueryDeduplicatedPathNum = maxQueryDeduplicatedPathNum;
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

  public int getFrequencyIntervalInMinute() {
    return frequencyIntervalInMinute;
  }

  public void setFrequencyIntervalInMinute(int frequencyIntervalInMinute) {
    this.frequencyIntervalInMinute = frequencyIntervalInMinute;
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
    RpcTransportFactory.setUseSnappy(this.rpcAdvancedCompressionEnable);
  }

  public int getMlogBufferSize() {
    return mlogBufferSize;
  }

  public void setMlogBufferSize(int mlogBufferSize) {
    this.mlogBufferSize = mlogBufferSize;
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

  public boolean isEnableInfluxDBRpcService() {
    return enableInfluxDBRpcService;
  }

  public void setEnableInfluxDBRpcService(boolean enableInfluxDBRpcService) {
    this.enableInfluxDBRpcService = enableInfluxDBRpcService;
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

  public int getMaxInnerCompactionCandidateFileNum() {
    return maxInnerCompactionCandidateFileNum;
  }

  public void setMaxInnerCompactionCandidateFileNum(int maxInnerCompactionCandidateFileNum) {
    this.maxInnerCompactionCandidateFileNum = maxInnerCompactionCandidateFileNum;
  }

  public int getMaxCrossCompactionCandidateFileNum() {
    return maxCrossCompactionCandidateFileNum;
  }

  public void setMaxCrossCompactionCandidateFileNum(int maxCrossCompactionCandidateFileNum) {
    this.maxCrossCompactionCandidateFileNum = maxCrossCompactionCandidateFileNum;
  }

  public long getCompactionSubmissionIntervalInMs() {
    return compactionSubmissionIntervalInMs;
  }

  public void setCompactionSubmissionIntervalInMs(long interval) {
    compactionSubmissionIntervalInMs = interval;
  }

  public int getSubCompactionTaskNum() {
    return subCompactionTaskNum;
  }

  public void setSubCompactionTaskNum(int subCompactionTaskNum) {
    this.subCompactionTaskNum = subCompactionTaskNum;
  }

  public String getDeviceIDTransformationMethod() {
    return deviceIDTransformationMethod;
  }

  public void setDeviceIDTransformationMethod(String deviceIDTransformationMethod) {
    this.deviceIDTransformationMethod = deviceIDTransformationMethod;
  }

  public boolean isEnableIDTable() {
    return enableIDTable;
  }

  public void setEnableIDTable(boolean enableIDTable) {
    this.enableIDTable = enableIDTable;
  }

  public boolean isEnableIDTableLogFile() {
    return enableIDTableLogFile;
  }

  public void setEnableIDTableLogFile(boolean enableIDTableLogFile) {
    this.enableIDTableLogFile = enableIDTableLogFile;
  }

  public String getSchemaEngineMode() {
    return schemaEngineMode;
  }

  public void setSchemaEngineMode(String schemaEngineMode) {
    this.schemaEngineMode = schemaEngineMode;
  }

  public int getCachedMNodeSizeInSchemaFileMode() {
    return cachedMNodeSizeInSchemaFileMode;
  }

  public void setCachedMNodeSizeInSchemaFileMode(int cachedMNodeSizeInSchemaFileMode) {
    this.cachedMNodeSizeInSchemaFileMode = cachedMNodeSizeInSchemaFileMode;
  }

  public short getMinimumSegmentInSchemaFile() {
    return minimumSegmentInSchemaFile;
  }

  public void setMinimumSegmentInSchemaFile(short minimumSegmentInSchemaFile) {
    this.minimumSegmentInSchemaFile = minimumSegmentInSchemaFile;
  }

  public int getPageCacheSizeInSchemaFile() {
    return pageCacheSizeInSchemaFile;
  }

  public void setPageCacheSizeInSchemaFile(int pageCacheSizeInSchemaFile) {
    this.pageCacheSizeInSchemaFile = pageCacheSizeInSchemaFile;
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

  public List<TEndPoint> getTargetConfigNodeList() {
    return targetConfigNodeList;
  }

  public void setTargetConfigNodeList(List<TEndPoint> targetConfigNodeList) {
    this.targetConfigNodeList = targetConfigNodeList;
  }

  public long getJoinClusterTimeOutMs() {
    return joinClusterTimeOutMs;
  }

  public void setJoinClusterTimeOutMs(long joinClusterTimeOutMs) {
    this.joinClusterTimeOutMs = joinClusterTimeOutMs;
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

  public int getSelectorNumOfClientManager() {
    return selectorNumOfClientManager;
  }

  public void setSelectorNumOfClientManager(int selectorNumOfClientManager) {
    this.selectorNumOfClientManager = selectorNumOfClientManager;
  }

  public boolean isMppMode() {
    return mppMode;
  }

  public void setMppMode(boolean mppMode) {
    this.mppMode = mppMode;
  }

  public boolean isClusterMode() {
    return isClusterMode;
  }

  public void setClusterMode(boolean isClusterMode) {
    this.isClusterMode = isClusterMode;
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

  public long getAllocateMemoryForLastCache() {
    return allocateMemoryForLastCache;
  }

  public void setAllocateMemoryForLastCache(long allocateMemoryForLastCache) {
    this.allocateMemoryForLastCache = allocateMemoryForLastCache;
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

  public long getThrottleThreshold() {
    return throttleThreshold;
  }

  public void setThrottleThreshold(long throttleThreshold) {
    this.throttleThreshold = throttleThreshold;
  }

  public String getConfigMessage() {
    String configMessage = "";
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
        if (configType.contains("java.lang.String[]")) {
          String[] configList = (String[]) configField.get(this);
          configContent = Arrays.asList(configList).toString();
        } else {
          configContent = configField.get(this).toString();
        }
        configMessage = configMessage + configField.getName() + "=" + configContent + "; ";
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return configMessage;
  }
}
