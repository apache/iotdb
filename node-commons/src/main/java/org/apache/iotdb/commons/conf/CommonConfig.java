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
package org.apache.iotdb.commons.conf;

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.consensus.ConsensusProtocolClass;
import org.apache.iotdb.commons.enums.HandleSystemErrorStrategy;
import org.apache.iotdb.commons.loadbalance.LeaderDistributionPolicy;
import org.apache.iotdb.commons.loadbalance.RegionGroupExtensionPolicy;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.commons.utils.datastructure.TVListSortAlgorithm;
import org.apache.iotdb.commons.wal.WALMode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CommonConfig {

  public static final String CONF_FILE_NAME = "iotdb-common.properties";
  private static final Logger logger = LoggerFactory.getLogger(CommonConfig.class);

  /** Cluster Configuration */
  // ClusterId, the default value "defaultCluster" will be changed after join cluster
  private volatile String clusterName = "defaultCluster";

  /** Replication configuration */
  // ConfigNodeGroup consensus protocol
  private ConsensusProtocolClass configNodeConsensusProtocolClass =
      ConsensusProtocolClass.RATIS_CONSENSUS;

  // Default number of SchemaRegion replicas
  private int schemaReplicationFactor = 1;
  // SchemaRegion consensus protocol
  private ConsensusProtocolClass schemaRegionConsensusProtocolClass =
      ConsensusProtocolClass.RATIS_CONSENSUS;

  // Default number of DataRegion replicas
  private int dataReplicationFactor = 1;
  // DataRegion consensus protocol
  private ConsensusProtocolClass dataRegionConsensusProtocolClass =
      ConsensusProtocolClass.IOT_CONSENSUS;

  /** Load balancing configuration */
  // Number of SeriesPartitionSlots per StorageGroup
  private int seriesSlotNum = 10000;
  // SeriesPartitionSlot executor class
  private String seriesPartitionExecutorClass =
      "org.apache.iotdb.commons.partition.executor.hash.BKDRHashExecutor";

  // The maximum number of SchemaRegions expected to be managed by each DataNode
  private double schemaRegionPerDataNode = schemaReplicationFactor;
  // The maximum number of DataRegions expected to be managed by each DataNode
  private double dataRegionPerProcessor = 1.0;

  // The policy of extension SchemaRegionGroup for each Database
  private RegionGroupExtensionPolicy schemaRegionGroupExtensionPolicy =
      RegionGroupExtensionPolicy.AUTO;
  // The number of SchemaRegionGroups for each Database when using CUSTOM extension policy
  private int schemaRegionGroupPerDatabase = 1;
  // The policy of extension DataRegionGroup for each Database
  private RegionGroupExtensionPolicy dataRegionGroupExtensionPolicy =
      RegionGroupExtensionPolicy.AUTO;
  // The number of DataRegionGroups for each Database when using CUSTOM extension policy
  private int dataRegionGroupPerDatabase = 1;

  // The least number of SchemaRegionGroup for each Database
  private int leastSchemaRegionGroupNum = 1;
  // The least number of DataRegionGroup for each Database
  private int leastDataRegionGroupNum = 5;

  // DataPartition within the same SeriesPartitionSlot will inherit the allocation result of the
  // previous TimePartitionSlot if set true
  private boolean enableDataPartitionInheritPolicy = false;

  // The policy of cluster RegionGroups' leader distribution
  private LeaderDistributionPolicy leaderDistributionPolicy =
      LeaderDistributionPolicy.MIN_COST_FLOW;
  // Whether to enable auto leader balance for Ratis consensus protocol
  private boolean enableAutoLeaderBalanceForRatisConsensus = false;
  // Whether to enable auto leader balance for IoTConsensus protocol
  private boolean enableAutoLeaderBalanceForIoTConsensus = true;

  /** Cluster management */
  // Time partition interval in milliseconds
  private long timePartitionInterval = 604_800_000L;
  // The heartbeat interval in milliseconds
  private long heartbeatIntervalInMs = 1000;
  // Disk Monitor
  private double diskSpaceWarningThreshold = 0.05;

  /** Memory Control Configuration */
  // Is the writing mem control for writing enable
  private boolean enableMemControl = true;

  // Memory allocated for the write process
  private long allocateMemoryForStorageEngine = Runtime.getRuntime().maxMemory() * 3 / 10;
  // Memory allocated for the read process
  private long allocateMemoryForRead = Runtime.getRuntime().maxMemory() * 3 / 10;
  // Memory allocated for the MTree
  private long allocateMemoryForSchema = Runtime.getRuntime().maxMemory() / 10;
  // Memory allocated for the consensus layer
  private long allocateMemoryForConsensus = Runtime.getRuntime().maxMemory() / 10;

  // Whether the schema memory allocation is default config. Used for cluster mode initialization
  // judgement
  private boolean isDefaultSchemaMemoryConfig = true;
  // Memory allocated for schemaRegion
  private long allocateMemoryForSchemaRegion = allocateMemoryForSchema * 8 / 10;
  // Memory allocated for SchemaCache
  private long allocateMemoryForSchemaCache = allocateMemoryForSchema / 10;
  // Memory allocated for PartitionCache
  private long allocateMemoryForPartitionCache = 0;
  // Memory allocated for LastCache
  private long allocateMemoryForLastCache = allocateMemoryForSchema / 10;

  // The proportion of write memory for memtable
  private double writeProportionForMemtable = 0.8;
  // The proportion of write memory for compaction
  private double compactionProportion = 0.2;
  // Memory allocated proportion for time partition info
  private long allocateMemoryForTimePartitionInfo = allocateMemoryForStorageEngine * 50 / 1001;

  // The num of memtable in each database
  private int concurrentWritingTimePartition = 1;
  // The default value of primitive array size in array pool
  private int primitiveArraySize = 64;
  private double chunkMetadataSizeProportion = 0.1;
  // Flush proportion for system
  private double flushProportion = 0.4;
  // Ratio of memory allocated for buffered arrays
  private double bufferedArraysMemoryProportion = 0.6;
  // Reject proportion for system
  private double rejectProportion = 0.8;
  // If memory cost of data region increased more than proportion of {@linkplain
  // CommonConfig#getAllocateMemoryForStorageEngine()}*{@linkplain
  // CommonConfig#getWriteProportionForMemtable()}, report to system.
  private double writeMemoryVariationReportProportion = 0.001;
  // When inserting rejected, waiting period to check system again. Unit: millisecond
  private int checkPeriodWhenInsertBlocked = 50;
  // The size of ioTaskQueue
  private int ioTaskQueueSizeForFlushing = 10;
  // If true, we will estimate each query's possible memory footprint before executing it and deny
  // it if its estimated memory exceeds current free memory
  private boolean enableQueryMemoryEstimation = true;

  /** Schema Engine Configuration */
  // ThreadPool size for read operation in coordinator
  private int coordinatorReadExecutorSize = 20;
  // ThreadPool size for write operation in coordinator
  private int coordinatorWriteExecutorSize = 50;

  // Cache size of partition cache in {@link
  // org.apache.iotdb.db.mpp.plan.analyze.ClusterPartitionFetcher}
  private int partitionCacheSize = 1000;

  // Size of log buffer for every MetaData operation. If the size of a MetaData operation plan is
  // larger than this parameter, then the MetaData operation plan will be rejected by SchemaRegion.
  // Unit: byte
  private int mlogBufferSize = 1024 * 1024;

  // The cycle when metadata log is periodically forced to be written to disk(in milliseconds) If
  // set this parameter to 0 it means call channel.force(true) after every each operation
  private long syncMlogPeriodInMs = 100;

  // Interval num of tag and attribute records when force flushing to disk
  private int tagAttributeFlushInterval = 1000;
  // max size for tag and attribute of one time series
  private int tagAttributeTotalSize = 700;

  // Maximum number of measurement in one create timeseries plan node. If the number of measurement
  // in user request exceeds this limit, the request will be split.
  private int maxMeasurementNumOfInternalRequest = 10000;

  /** Configurations for creating schema automatically */
  // Switch of creating schema automatically
  private boolean enableAutoCreateSchema = true;
  // Database level when creating schema automatically is enabled
  private int defaultStorageGroupLevel = 1;

  // Register time series as which type when receiving boolean string "true" or "false"
  private TSDataType booleanStringInferType = TSDataType.BOOLEAN;
  // register time series as which type when receiving an integer string "67"
  private TSDataType integerStringInferType = TSDataType.FLOAT;
  // register time series as which type when receiving an integer string and using float may lose
  // precision num > 2 ^ 24
  private TSDataType longStringInferType = TSDataType.DOUBLE;
  // register time series as which type when receiving a floating number string "6.7"
  private TSDataType floatingStringInferType = TSDataType.FLOAT;
  // register time series as which type when receiving the Literal NaN. Values can be DOUBLE, FLOAT
  // or TEXT
  private TSDataType nanStringInferType = TSDataType.DOUBLE;

  // BOOLEAN encoding when creating schema automatically is enabled
  private TSEncoding defaultBooleanEncoding = TSEncoding.RLE;
  // INT32 encoding when creating schema automatically is enabled
  private TSEncoding defaultInt32Encoding = TSEncoding.RLE;
  // INT64 encoding when creating schema automatically is enabled
  private TSEncoding defaultInt64Encoding = TSEncoding.RLE;
  // FLOAT encoding when creating schema automatically is enabled
  private TSEncoding defaultFloatEncoding = TSEncoding.GORILLA;
  // DOUBLE encoding when creating schema automatically is enabled
  private TSEncoding defaultDoubleEncoding = TSEncoding.GORILLA;
  // TEXT encoding when creating schema automatically is enabled
  private TSEncoding defaultTextEncoding = TSEncoding.PLAIN;

  /** Query Configurations */
  // The read consistency level
  private String readConsistencyLevel = "strong";

  // Whether to cache metadata(ChunkMetaData and TsFileMetaData) or not
  private boolean metaDataCacheEnable = true;

  // Memory allocated for bloomFilter cache in read process
  private long allocateMemoryForBloomFilterCache = allocateMemoryForRead / 1001;
  // Memory allocated for chunk cache in read process
  private long allocateMemoryForChunkCache = allocateMemoryForRead * 100 / 1001;
  // Memory allocated for timeSeriesMetaData cache in read process
  private long allocateMemoryForTimeSeriesMetaDataCache = allocateMemoryForRead * 200 / 1001;
  // Memory allocated for operators
  private long allocateMemoryForCoordinator = allocateMemoryForRead * 50 / 1001;
  // Memory allocated for operators
  private long allocateMemoryForOperators = allocateMemoryForRead * 200 / 1001;
  // Memory allocated for operators
  private long allocateMemoryForDataExchange = allocateMemoryForRead * 200 / 1001;
  // Memory allocated proportion for timeIndex
  private long allocateMemoryForTimeIndex = allocateMemoryForRead * 200 / 1001;

  // Whether to enable last cache
  private boolean enableLastCache = true;

  private volatile int maxDeduplicatedPathNum = 1000;

  // Core pool size of mpp data exchange
  private int mppDataExchangeCorePoolSize = 10;
  // Max pool size of mpp data exchange
  private int mppDataExchangeMaxPoolSize = 10;
  // Thread keep alive time in ms of mpp data exchange
  private int mppDataExchangeKeepAliveTimeInMs = 1000;
  // Maximum execution time of a DriverTask
  private int driverTaskExecutionTimeSliceInMs = 100;

  // Maximum capacity of a TsBlock, allow up to two pages
  private int maxTsBlockSizeInBytes = 128 * 1024;
  // Maximum number of lines in a single TsBlock
  private int maxTsBlockLineNumber = 1000;

  // Time cost(ms) threshold for slow query. Unit: millisecond
  private long slowQueryThreshold = 5000;
  // The max executing time of query in ms. Unit: millisecond
  private long queryTimeoutThreshold = 60000;
  // How many queries can be concurrently executed. When <= 0, use 1000
  private int maxAllowedConcurrentQueries = 1000;
  // How many threads can concurrently execute query statement. When <= 0, use CPU core number
  private int queryThreadCount = Runtime.getRuntime().availableProcessors();
  // The amount of data iterate each time in server
  private int batchSize = 100000;

  /** Storage Engine Configuration */
  // This variable set timestamp precision as millisecond, microsecond or nanosecond
  private String timestampPrecision = "ms";
  // Default TTL for databases that are not set TTL by statements, in ms.
  // <p> Notice: if this property is changed, previous created database which are not set TTL will
  // also be affected. Unit: millisecond
  private long defaultTtlInMs = Long.MAX_VALUE;

  // When inserting rejected exceeds this, throw an exception. Unit: millisecond
  private int maxWaitingTimeWhenInsertBlockedInMs = 10000;

  private boolean enableDiscardOutOfOrderData = false;

  // What will the system do when unrecoverable error occurs
  private HandleSystemErrorStrategy handleSystemErrorStrategy =
      HandleSystemErrorStrategy.CHANGE_TO_READ_ONLY;

  // When a memTable's size (in byte) exceeds this, the memtable is flushed to disk. Unit: byte
  private long memtableSizeThreshold = 1024 * 1024 * 1024L;

  // Whether to timed flush sequence tsfiles' memtables
  private boolean enableTimedFlushSeqMemtable = true;

  // If a memTable's created time is older than current time minus this, the memtable will be
  // flushed to disk.(only check sequence tsfiles' memtables) Unit: ms
  private long seqMemtableFlushInterval = 3 * 60 * 60 * 1000L;

  // The interval to check whether sequence memtables need flushing. Unit: ms
  private long seqMemtableFlushCheckInterval = 10 * 60 * 1000L;

  // Whether to timed flush unsequence tsfiles' memtables
  private boolean enableTimedFlushUnseqMemtable = true;

  // If a memTable's created time is older than current time minus this, the memtable will be
  // flushed to disk.(only check unsequence tsfiles' memtables) Unit: ms
  private long unseqMemtableFlushInterval = 3 * 60 * 60 * 1000L;

  // The interval to check whether unsequence memtables need flushing. Unit: ms
  private long unseqMemtableFlushCheckInterval = 10 * 60 * 1000L;

  // The sort algorithm used in TVList
  private TVListSortAlgorithm tvListSortAlgorithm = TVListSortAlgorithm.TIM;

  // When average series point number reaches this, flush the memtable to disk
  private int avgSeriesPointNumberThreshold = 100000;

  // How many threads can concurrently flush. When <= 0, use CPU core number
  private int flushThreadCount = Runtime.getRuntime().availableProcessors();

  // In one insert (one device, one timestamp, multiple measurements),
  // if enable partial insert, one measurement failure will not impact other measurements
  private boolean enablePartialInsert = true;

  // The interval to log recover progress of each vsg when starting iotdb
  private long recoveryLogIntervalInMs = 5_000L;

  // How many threads will be set up to perform upgrade tasks
  private int upgradeThreadCount = 1;

  /** Compaction Configurations */
  // TODO: Move from IoTDBConfig

  /** Write Ahead Log Configuration */
  // Write mode of wal
  private volatile WALMode walMode = WALMode.ASYNC;

  // Max number of wal nodes, each node corresponds to one wal directory
  private int maxWalNodesNum = 0;

  // Duration a wal flush operation will wait before calling fsync. Unit: millisecond
  private volatile long fsyncWalDelayInMs = 3;

  // Buffer size of each wal node. Unit: byte
  private int walBufferSizeInByte = 16 * 1024 * 1024;

  // Blocking queue capacity of each wal buffer
  private int walBufferQueueCapacity = 50;

  // Size threshold of each wal file. Unit: byte
  private volatile long walFileSizeThresholdInByte = 10 * 1024 * 1024L;

  // Minimum ratio of effective information in wal files
  private volatile double walMinEffectiveInfoRatio = 0.1;

  // MemTable size threshold for triggering MemTable snapshot in wal. When a memTable's size exceeds
  // this, wal can flush this memtable to disk, otherwise wal will snapshot this memtable in wal.
  // Unit: byte
  private volatile long walMemTableSnapshotThreshold = 8 * 1024 * 1024L;

  // MemTable's max snapshot number in wal file
  private volatile int maxWalMemTableSnapshotNum = 1;

  // The period when outdated wal files are periodically deleted. Unit: millisecond
  private volatile long deleteWalFilesPeriodInMs = 20 * 1000L;

  // Maximum size of wal buffer used in IoTConsensus. Unit: byte
  private long iotConsensusThrottleThresholdInByte = 50 * 1024 * 1024 * 1024L;

  // Maximum wait time of write cache in IoTConsensus. Unit: ms
  private long iotConsensusCacheWindowTimeInMs = 10 * 1000L;

  /** Watermark Configuration */
  // Switch of watermark function
  private boolean enableWatermark = false;

  // Secret key for watermark
  private String watermarkSecretKey = "IoTDB*2019@Beijing";

  // Bit string of watermark
  private String watermarkBitString = "100101110100";

  // Watermark method and parameters
  private String watermarkMethod = "GroupBasedLSBMethod(embed_row_cycle=2,embed_lsb_num=5)";

  /** Authorization Configuration */
  // The authorizer provider class which extends BasicAuthorizer
  private String authorizerProvider =
      "org.apache.iotdb.commons.auth.authorizer.LocalFileAuthorizer";
  // Open ID Secret
  private String openIdProviderUrl = "";

  private String adminName = "root";
  private String adminPassword = "root";

  // Encryption provider class
  private String iotdbServerEncryptDecryptProvider =
      "org.apache.iotdb.commons.security.encrypt.MessageDigestEncrypt";
  // Encryption provided class parameter
  private String iotdbServerEncryptDecryptProviderParameter;

  // Cache size of user and role
  private int authorCacheSize = 100;
  // Cache expire time of user and role
  private int authorCacheExpireTime = 30;

  /** UDF Configuration */
  // Used to estimate the memory usage of text fields in a UDF query. It is recommended to set this
  // value to be slightly larger than the average length of all text records.
  private int udfInitialByteArrayLengthForMemoryControl = 48;

  // How much memory may be used in ONE UDF query (in MB).
  // The upper limit is 20% of allocated memory for read.
  // udfMemoryBudgetInMB = udfReaderMemoryBudgetInMB +
  // udfTransformerMemoryBudgetInMB + udfCollectorMemoryBudgetInMB
  private float udfMemoryBudgetInMB = (float) Math.min(30.0f, 0.2 * allocateMemoryForRead);

  private float udfReaderMemoryBudgetInMB = (float) (1.0 / 3 * udfMemoryBudgetInMB);

  private float udfTransformerMemoryBudgetInMB = (float) (1.0 / 3 * udfMemoryBudgetInMB);

  private float udfCollectorMemoryBudgetInMB = (float) (1.0 / 3 * udfMemoryBudgetInMB);

  // External lib directory for UDF, stores user-uploaded JAR files
  private String udfDir =
      IoTDBConstant.EXT_FOLDER_NAME + File.separator + IoTDBConstant.UDF_FOLDER_NAME;
  // External temporary lib directory for storing downloaded udf JAR files
  private String udfTemporaryLibDir = udfDir + File.separator + IoTDBConstant.TMP_FOLDER_NAME;

  /** Trigger Configuration */
  // External lib directory for trigger, stores user-uploaded JAR files
  private String triggerDir =
      IoTDBConstant.EXT_FOLDER_NAME + File.separator + IoTDBConstant.TRIGGER_FOLDER_NAME;
  // External temporary lib directory for storing downloaded trigger JAR files
  private String triggerTemporaryLibDir =
      triggerDir + File.separator + IoTDBConstant.TMP_FOLDER_NAME;

  // How many times will we retry to find an instance of stateful trigger
  private int statefulTriggerRetryNumWhenNotFound = 3;

  // The size of log buffer for every trigger management operation plan. If the size of a trigger
  // management operation plan is larger than this parameter, the trigger management operation plan
  // will be rejected by TriggerManager. Unit: byte
  private int tlogBufferSize = 1024 * 1024;

  // Number of queues per forwarding trigger
  private int triggerForwardMaxQueueNumber = 8;
  // The length of one of the queues per forwarding trigger
  private int triggerForwardMaxSizePerQueue = 2000;
  // Trigger forwarding data size per batch
  private int triggerForwardBatchSize = 50;
  // Trigger HTTP forward pool size
  private int triggerForwardHTTPPoolSize = 200;
  // Trigger HTTP forward pool max connection for per route
  private int triggerForwardHTTPPOOLMaxPerRoute = 20;
  // Trigger MQTT forward pool size
  private int triggerForwardMQTTPoolSize = 4;

  /** Select-Into Configuration */
  // The maximum number of rows can be processed in insert-tablet-plan when executing select-into
  // statements.
  private int selectIntoInsertTabletPlanRowLimit = 10000;
  // The number of threads in the thread pool that execute insert-tablet tasks.
  private int intoOperationExecutionThreadCount = 2;

  /** Continuous Query Configuration */
  // How many thread will be set up to perform continuous queries. When <= 0, use max(1, CPU core
  // number / 2).
  private int continuousQueryThreadCount =
      Math.max(1, Runtime.getRuntime().availableProcessors() / 2);

  // Minimum every interval to perform continuous query.
  // The every interval of continuous query instances should not be lower than this limit.
  private long continuousQueryMinEveryIntervalInMs = 1000;

  /** PIPE Configuration */
  // White list for sync
  private String ipWhiteList = "127.0.0.1/32";
  // The maximum number of retries when the sender fails to synchronize files to the receiver
  private int maxNumberOfSyncFileRetry = 5;

  /** RatisConsensus Configuration */
  // RatisConsensus protocol, Max size for a single log append request from leader
  private long configNodeRatisConsensusLogAppenderBufferSize = 4 * 1024 * 1024L;

  private long schemaRegionRatisConsensusLogAppenderBufferSize = 4 * 1024 * 1024L;
  private long dataRegionRatisConsensusLogAppenderBufferSize = 4 * 1024 * 1024L;

  // RatisConsensus protocol, trigger a snapshot when ratis_snapshot_trigger_threshold logs are
  // written
  private long configNodeRatisSnapshotTriggerThreshold = 400000L;
  private long schemaRegionRatisSnapshotTriggerThreshold = 400000L;
  private long dataRegionRatisSnapshotTriggerThreshold = 400000L;

  // RatisConsensus protocol, allow flushing Raft Log asynchronously
  private boolean configNodeRatisLogUnsafeFlushEnable = false;
  private boolean schemaRegionRatisLogUnsafeFlushEnable = false;
  private boolean dataRegionRatisLogUnsafeFlushEnable = false;

  // RatisConsensus protocol, max capacity of a single Raft Log segment
  private long configNodeRatisLogSegmentSizeMax = 24 * 1024 * 1024L;
  private long schemaRegionRatisLogSegmentSizeMax = 24 * 1024 * 1024L;
  private long dataRegionRatisLogSegmentSizeMax = 24 * 1024 * 1024L;
  private long configNodeSimpleConsensusLogSegmentSizeMax = 24 * 1024 * 1024L;

  // RatisConsensus protocol, flow control window for ratis grpc log appender
  private long configNodeRatisGrpcFlowControlWindow = 4 * 1024 * 1024L;
  private long schemaRegionRatisGrpcFlowControlWindow = 4 * 1024 * 1024L;
  private long dataRegionRatisGrpcFlowControlWindow = 4 * 1024 * 1024L;

  // RatisConsensus protocol, min election timeout for leader election
  private long configNodeRatisRpcLeaderElectionTimeoutMinMs = 2000L;
  private long schemaRegionRatisRpcLeaderElectionTimeoutMinMs = 2000L;
  private long dataRegionRatisRpcLeaderElectionTimeoutMinMs = 2000L;

  // RatisConsensus protocol, max election timeout for leader election
  private long configNodeRatisRpcLeaderElectionTimeoutMaxMs = 4000L;
  private long schemaRegionRatisRpcLeaderElectionTimeoutMaxMs = 4000L;
  private long dataRegionRatisRpcLeaderElectionTimeoutMaxMs = 4000L;

  // RatisConsensus protocol, request timeout for ratis client
  private long configNodeRatisRequestTimeoutMs = 10000L;
  private long schemaRegionRatisRequestTimeoutMs = 10000L;
  private long dataRegionRatisRequestTimeoutMs = 10000L;

  // RatisConsensus protocol, exponential back-off retry policy params
  private int configNodeRatisMaxRetryAttempts = 10;
  private long configNodeRatisInitialSleepTimeMs = 100;
  private long configNodeRatisMaxSleepTimeMs = 10000;

  private int dataRegionRatisMaxRetryAttempts = 10;
  private long dataRegionRatisInitialSleepTimeMs = 100;
  private long dataRegionRatisMaxSleepTimeMs = 10000;

  private int schemaRegionRatisMaxRetryAttempts = 10;
  private long schemaRegionRatisInitialSleepTimeMs = 100;
  private long schemaRegionRatisMaxSleepTimeMs = 10000;

  private long configNodeRatisPreserveLogsWhenPurge = 1000;
  private long schemaRegionRatisPreserveLogsWhenPurge = 1000;
  private long dataRegionRatisPreserveLogsWhenPurge = 1000;

  // first election timeout shares between 3 regions
  private long ratisFirstElectionTimeoutMinMs = 50;
  private long ratisFirstElectionTimeoutMaxMs = 150;

  private long configNodeRatisLogMax = 2L * 1024 * 1024 * 1024; // 2G
  private long schemaRegionRatisLogMax = 2L * 1024 * 1024 * 1024; // 2G
  private long dataRegionRatisLogMax = 20L * 1024 * 1024 * 1024; // 20G

  /** Procedure Configuration */
  // Procedure Evict ttl
  private int procedureCompletedEvictTTL = 800;
  // Procedure completed clean interval
  private int procedureCompletedCleanInterval = 30;
  // Procedure core worker threads size
  private int procedureCoreWorkerThreadsCount =
      Math.max(Runtime.getRuntime().availableProcessors() / 4, 16);

  /** MQTT Broker Configuration */
  private boolean enableMqttService = false;

  // The mqtt service binding host
  private String mqttHost = "127.0.0.1";
  // The mqtt service binding port
  private int mqttPort = 1883;
  // The handler pool size for handing the mqtt messages
  private int mqttHandlerPoolSize = 1;
  // The mqtt message payload formatter
  private String mqttPayloadFormatter = "json";
  // Max mqtt message size. Unit: byte
  private int mqttMaxMessageSize = 1048576;

  /** REST Service Configuration */
  // TODO: Move from IoTDBConfig

  /** InfluxDB RPC Service Configuration */
  // Whether enable the influxdb rpc service.
  // This parameter has no a corresponding field in the iotdb-common.properties
  private boolean enableInfluxDBRpcService = false;
  // Port which the influxdb protocol server listens to
  private int influxDBRpcPort = 8086;

  /** Common Folders */
  private String userFolder =
      IoTDBConstant.DEFAULT_BASE_DIR
          + File.separator
          + IoTDBConstant.SYSTEM_FOLDER_NAME
          + File.separator
          + "users";

  private String roleFolder =
      IoTDBConstant.DEFAULT_BASE_DIR
          + File.separator
          + IoTDBConstant.SYSTEM_FOLDER_NAME
          + File.separator
          + "roles";
  private String procedureWalFolder =
      IoTDBConstant.DEFAULT_BASE_DIR
          + File.separator
          + IoTDBConstant.SYSTEM_FOLDER_NAME
          + File.separator
          + "procedure";

  // Sync directory, including the log and hardlink tsfiles
  private String syncDir =
      IoTDBConstant.DEFAULT_BASE_DIR + File.separator + IoTDBConstant.SYNC_FOLDER_NAME;

  /** Internal Configurations(Unconfigurable in .properties file) */
  // NodeStatus
  private volatile NodeStatus status = NodeStatus.Running;

  private volatile String statusReason = null;

  // Default system file storage is in local file system (unsupported)
  private FSType systemFileStorageFs = FSType.LOCAL;

  // Max bytes of each FragmentInstance for DataExchange
  private long maxBytesPerFragmentInstance = allocateMemoryForDataExchange / queryThreadCount;

  private boolean rpcThriftCompressionEnable = false;
  private int connectionTimeoutInMS = (int) TimeUnit.SECONDS.toMillis(20);
  // ClientManager will have so many selector threads (TAsyncClientManager) to distribute to its
  // clients
  private int selectorThreadCountOfClientManager =
      Runtime.getRuntime().availableProcessors() / 4 > 0
          ? Runtime.getRuntime().availableProcessors() / 4
          : 1;
  private int coreClientCountForEachNodeInClientManager = 200;
  private int maxClientCountForEachNodeInClientManager = 300;

  public CommonConfig() {
    // Empty constructor
  }

  public void formulateFolders() {
    String homeDir = System.getProperty(IoTDBConstant.IOTDB_HOME, null);
    if (homeDir == null) {
      homeDir = System.getProperty(IoTDBConstant.CONFIGNODE_HOME, null);
    }

    udfDir = PropertiesUtils.addHomeDir(udfDir, homeDir);
    udfTemporaryLibDir = PropertiesUtils.addHomeDir(udfTemporaryLibDir, homeDir);
    triggerDir = PropertiesUtils.addHomeDir(triggerDir, homeDir);
    triggerTemporaryLibDir = PropertiesUtils.addHomeDir(triggerTemporaryLibDir, homeDir);

    userFolder = PropertiesUtils.addHomeDir(userFolder, homeDir);
    roleFolder = PropertiesUtils.addHomeDir(roleFolder, homeDir);
    procedureWalFolder = PropertiesUtils.addHomeDir(procedureWalFolder, homeDir);
  }

  public String getClusterName() {
    return clusterName;
  }

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  public ConsensusProtocolClass getConfigNodeConsensusProtocolClass() {
    return configNodeConsensusProtocolClass;
  }

  public void setConfigNodeConsensusProtocolClass(
      ConsensusProtocolClass configNodeConsensusProtocolClass) {
    this.configNodeConsensusProtocolClass = configNodeConsensusProtocolClass;
  }

  public int getSchemaReplicationFactor() {
    return schemaReplicationFactor;
  }

  public void setSchemaReplicationFactor(int schemaReplicationFactor) {
    this.schemaReplicationFactor = schemaReplicationFactor;
  }

  public ConsensusProtocolClass getSchemaRegionConsensusProtocolClass() {
    return schemaRegionConsensusProtocolClass;
  }

  public void setSchemaRegionConsensusProtocolClass(
      ConsensusProtocolClass schemaRegionConsensusProtocolClass) {
    this.schemaRegionConsensusProtocolClass = schemaRegionConsensusProtocolClass;
  }

  public int getDataReplicationFactor() {
    return dataReplicationFactor;
  }

  public void setDataReplicationFactor(int dataReplicationFactor) {
    this.dataReplicationFactor = dataReplicationFactor;
  }

  public ConsensusProtocolClass getDataRegionConsensusProtocolClass() {
    return dataRegionConsensusProtocolClass;
  }

  public void setDataRegionConsensusProtocolClass(
      ConsensusProtocolClass dataRegionConsensusProtocolClass) {
    this.dataRegionConsensusProtocolClass = dataRegionConsensusProtocolClass;
  }

  public int getSeriesSlotNum() {
    return seriesSlotNum;
  }

  public void setSeriesSlotNum(int seriesSlotNum) {
    this.seriesSlotNum = seriesSlotNum;
  }

  public String getSeriesPartitionExecutorClass() {
    return seriesPartitionExecutorClass;
  }

  public void setSeriesPartitionExecutorClass(String seriesPartitionExecutorClass) {
    this.seriesPartitionExecutorClass = seriesPartitionExecutorClass;
  }

  public double getSchemaRegionPerDataNode() {
    return schemaRegionPerDataNode;
  }

  public void setSchemaRegionPerDataNode(double schemaRegionPerDataNode) {
    this.schemaRegionPerDataNode = schemaRegionPerDataNode;
  }

  public double getDataRegionPerProcessor() {
    return dataRegionPerProcessor;
  }

  public void setDataRegionPerProcessor(double dataRegionPerProcessor) {
    this.dataRegionPerProcessor = dataRegionPerProcessor;
  }

  public RegionGroupExtensionPolicy getSchemaRegionGroupExtensionPolicy() {
    return schemaRegionGroupExtensionPolicy;
  }

  public void setSchemaRegionGroupExtensionPolicy(
      RegionGroupExtensionPolicy schemaRegionGroupExtensionPolicy) {
    this.schemaRegionGroupExtensionPolicy = schemaRegionGroupExtensionPolicy;
  }

  public int getSchemaRegionGroupPerDatabase() {
    return schemaRegionGroupPerDatabase;
  }

  public void setSchemaRegionGroupPerDatabase(int schemaRegionGroupPerDatabase) {
    this.schemaRegionGroupPerDatabase = schemaRegionGroupPerDatabase;
  }

  public RegionGroupExtensionPolicy getDataRegionGroupExtensionPolicy() {
    return dataRegionGroupExtensionPolicy;
  }

  public void setDataRegionGroupExtensionPolicy(
      RegionGroupExtensionPolicy dataRegionGroupExtensionPolicy) {
    this.dataRegionGroupExtensionPolicy = dataRegionGroupExtensionPolicy;
  }

  public int getDataRegionGroupPerDatabase() {
    return dataRegionGroupPerDatabase;
  }

  public void setDataRegionGroupPerDatabase(int dataRegionGroupPerDatabase) {
    this.dataRegionGroupPerDatabase = dataRegionGroupPerDatabase;
  }

  public int getLeastSchemaRegionGroupNum() {
    return leastSchemaRegionGroupNum;
  }

  public void setLeastSchemaRegionGroupNum(int leastSchemaRegionGroupNum) {
    this.leastSchemaRegionGroupNum = leastSchemaRegionGroupNum;
  }

  public int getLeastDataRegionGroupNum() {
    return leastDataRegionGroupNum;
  }

  public void setLeastDataRegionGroupNum(int leastDataRegionGroupNum) {
    this.leastDataRegionGroupNum = leastDataRegionGroupNum;
  }

  public boolean isEnableDataPartitionInheritPolicy() {
    return enableDataPartitionInheritPolicy;
  }

  public void setEnableDataPartitionInheritPolicy(boolean enableDataPartitionInheritPolicy) {
    this.enableDataPartitionInheritPolicy = enableDataPartitionInheritPolicy;
  }

  public LeaderDistributionPolicy getLeaderDistributionPolicy() {
    return leaderDistributionPolicy;
  }

  public void setLeaderDistributionPolicy(LeaderDistributionPolicy leaderDistributionPolicy) {
    this.leaderDistributionPolicy = leaderDistributionPolicy;
  }

  public boolean isEnableAutoLeaderBalanceForRatisConsensus() {
    return enableAutoLeaderBalanceForRatisConsensus;
  }

  public void setEnableAutoLeaderBalanceForRatisConsensus(
      boolean enableAutoLeaderBalanceForRatisConsensus) {
    this.enableAutoLeaderBalanceForRatisConsensus = enableAutoLeaderBalanceForRatisConsensus;
  }

  public boolean isEnableAutoLeaderBalanceForIoTConsensus() {
    return enableAutoLeaderBalanceForIoTConsensus;
  }

  public void setEnableAutoLeaderBalanceForIoTConsensus(
      boolean enableAutoLeaderBalanceForIoTConsensus) {
    this.enableAutoLeaderBalanceForIoTConsensus = enableAutoLeaderBalanceForIoTConsensus;
  }

  public long getTimePartitionInterval() {
    return timePartitionInterval;
  }

  public void setTimePartitionInterval(long timePartitionInterval) {
    this.timePartitionInterval = timePartitionInterval;
  }

  public long getHeartbeatIntervalInMs() {
    return heartbeatIntervalInMs;
  }

  public void setHeartbeatIntervalInMs(long heartbeatIntervalInMs) {
    this.heartbeatIntervalInMs = heartbeatIntervalInMs;
  }

  public double getDiskSpaceWarningThreshold() {
    return diskSpaceWarningThreshold;
  }

  public void setDiskSpaceWarningThreshold(double diskSpaceWarningThreshold) {
    this.diskSpaceWarningThreshold = diskSpaceWarningThreshold;
  }

  public String getReadConsistencyLevel() {
    return readConsistencyLevel;
  }

  public void setReadConsistencyLevel(String readConsistencyLevel) {
    this.readConsistencyLevel = readConsistencyLevel;
  }

  public long getDefaultTtlInMs() {
    return defaultTtlInMs;
  }

  public void setDefaultTtlInMs(long defaultTtlInMs) {
    this.defaultTtlInMs = defaultTtlInMs;
  }

  public String getAuthorizerProvider() {
    return authorizerProvider;
  }

  public void setAuthorizerProvider(String authorizerProvider) {
    this.authorizerProvider = authorizerProvider;
  }

  public String getOpenIdProviderUrl() {
    return openIdProviderUrl;
  }

  public void setOpenIdProviderUrl(String openIdProviderUrl) {
    this.openIdProviderUrl = openIdProviderUrl;
  }

  public String getIotdbServerEncryptDecryptProvider() {
    return iotdbServerEncryptDecryptProvider;
  }

  public void setIotdbServerEncryptDecryptProvider(String iotdbServerEncryptDecryptProvider) {
    this.iotdbServerEncryptDecryptProvider = iotdbServerEncryptDecryptProvider;
  }

  public String getIotdbServerEncryptDecryptProviderParameter() {
    return iotdbServerEncryptDecryptProviderParameter;
  }

  public void setIotdbServerEncryptDecryptProviderParameter(
      String iotdbServerEncryptDecryptProviderParameter) {
    this.iotdbServerEncryptDecryptProviderParameter = iotdbServerEncryptDecryptProviderParameter;
  }

  public String getAdminName() {
    return adminName;
  }

  public void setAdminName(String adminName) {
    this.adminName = adminName;
  }

  public String getAdminPassword() {
    return adminPassword;
  }

  public void setAdminPassword(String adminPassword) {
    this.adminPassword = adminPassword;
  }

  public String getUdfDir() {
    return udfDir;
  }

  public void setUdfDir(String udfDir) {
    this.udfDir = udfDir;
    updateUdfTemporaryLibDir();
  }

  public void updateUdfTemporaryLibDir() {
    this.udfTemporaryLibDir = udfDir + File.separator + IoTDBConstant.TMP_FOLDER_NAME;
  }

  public String getUdfTemporaryLibDir() {
    return udfTemporaryLibDir;
  }

  public void setUdfTemporaryLibDir(String udfTemporaryLibDir) {
    this.udfTemporaryLibDir = udfTemporaryLibDir;
  }

  public String getTriggerDir() {
    return triggerDir;
  }

  public void setTriggerDir(String triggerDir) {
    this.triggerDir = triggerDir;
    updateTriggerTemporaryLibDir();
  }

  public void updateTriggerTemporaryLibDir() {
    this.triggerTemporaryLibDir = triggerDir + File.separator + IoTDBConstant.TMP_FOLDER_NAME;
  }

  public String getTriggerTemporaryLibDir() {
    return triggerTemporaryLibDir;
  }

  public void setTriggerTemporaryLibDir(String triggerTemporaryLibDir) {
    this.triggerTemporaryLibDir = triggerTemporaryLibDir;
  }

  public int getStatefulTriggerRetryNumWhenNotFound() {
    return statefulTriggerRetryNumWhenNotFound;
  }

  public void setStatefulTriggerRetryNumWhenNotFound(int statefulTriggerRetryNumWhenNotFound) {
    this.statefulTriggerRetryNumWhenNotFound = statefulTriggerRetryNumWhenNotFound;
  }

  public int getTlogBufferSize() {
    return tlogBufferSize;
  }

  public void setTlogBufferSize(int tlogBufferSize) {
    this.tlogBufferSize = tlogBufferSize;
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

  public int getContinuousQueryThreadCount() {
    return continuousQueryThreadCount;
  }

  public void setContinuousQueryThreadCount(int continuousQueryThreadCount) {
    this.continuousQueryThreadCount = continuousQueryThreadCount;
  }

  public long getContinuousQueryMinEveryIntervalInMs() {
    return continuousQueryMinEveryIntervalInMs;
  }

  public void setContinuousQueryMinEveryIntervalInMs(long continuousQueryMinEveryIntervalInMs) {
    this.continuousQueryMinEveryIntervalInMs = continuousQueryMinEveryIntervalInMs;
  }

  public String getIpWhiteList() {
    return ipWhiteList;
  }

  public void setIpWhiteList(String ipWhiteList) {
    this.ipWhiteList = ipWhiteList;
  }

  public int getMaxNumberOfSyncFileRetry() {
    return maxNumberOfSyncFileRetry;
  }

  public void setMaxNumberOfSyncFileRetry(int maxNumberOfSyncFileRetry) {
    this.maxNumberOfSyncFileRetry = maxNumberOfSyncFileRetry;
  }

  public long getConfigNodeRatisConsensusLogAppenderBufferSize() {
    return configNodeRatisConsensusLogAppenderBufferSize;
  }

  public void setConfigNodeRatisConsensusLogAppenderBufferSize(
      long configNodeRatisConsensusLogAppenderBufferSize) {
    this.configNodeRatisConsensusLogAppenderBufferSize =
        configNodeRatisConsensusLogAppenderBufferSize;
  }

  public long getSchemaRegionRatisConsensusLogAppenderBufferSize() {
    return schemaRegionRatisConsensusLogAppenderBufferSize;
  }

  public void setSchemaRegionRatisConsensusLogAppenderBufferSize(
      long schemaRegionRatisConsensusLogAppenderBufferSize) {
    this.schemaRegionRatisConsensusLogAppenderBufferSize =
        schemaRegionRatisConsensusLogAppenderBufferSize;
  }

  public long getDataRegionRatisConsensusLogAppenderBufferSize() {
    return dataRegionRatisConsensusLogAppenderBufferSize;
  }

  public void setDataRegionRatisConsensusLogAppenderBufferSize(
      long dataRegionRatisConsensusLogAppenderBufferSize) {
    this.dataRegionRatisConsensusLogAppenderBufferSize =
        dataRegionRatisConsensusLogAppenderBufferSize;
  }

  public long getConfigNodeRatisSnapshotTriggerThreshold() {
    return configNodeRatisSnapshotTriggerThreshold;
  }

  public void setConfigNodeRatisSnapshotTriggerThreshold(
      long configNodeRatisSnapshotTriggerThreshold) {
    this.configNodeRatisSnapshotTriggerThreshold = configNodeRatisSnapshotTriggerThreshold;
  }

  public long getSchemaRegionRatisSnapshotTriggerThreshold() {
    return schemaRegionRatisSnapshotTriggerThreshold;
  }

  public void setSchemaRegionRatisSnapshotTriggerThreshold(
      long schemaRegionRatisSnapshotTriggerThreshold) {
    this.schemaRegionRatisSnapshotTriggerThreshold = schemaRegionRatisSnapshotTriggerThreshold;
  }

  public long getDataRegionRatisSnapshotTriggerThreshold() {
    return dataRegionRatisSnapshotTriggerThreshold;
  }

  public void setDataRegionRatisSnapshotTriggerThreshold(
      long dataRegionRatisSnapshotTriggerThreshold) {
    this.dataRegionRatisSnapshotTriggerThreshold = dataRegionRatisSnapshotTriggerThreshold;
  }

  public boolean isConfigNodeRatisLogUnsafeFlushEnable() {
    return configNodeRatisLogUnsafeFlushEnable;
  }

  public void setConfigNodeRatisLogUnsafeFlushEnable(boolean configNodeRatisLogUnsafeFlushEnable) {
    this.configNodeRatisLogUnsafeFlushEnable = configNodeRatisLogUnsafeFlushEnable;
  }

  public boolean isSchemaRegionRatisLogUnsafeFlushEnable() {
    return schemaRegionRatisLogUnsafeFlushEnable;
  }

  public void setSchemaRegionRatisLogUnsafeFlushEnable(
      boolean schemaRegionRatisLogUnsafeFlushEnable) {
    this.schemaRegionRatisLogUnsafeFlushEnable = schemaRegionRatisLogUnsafeFlushEnable;
  }

  public boolean isDataRegionRatisLogUnsafeFlushEnable() {
    return dataRegionRatisLogUnsafeFlushEnable;
  }

  public void setDataRegionRatisLogUnsafeFlushEnable(boolean dataRegionRatisLogUnsafeFlushEnable) {
    this.dataRegionRatisLogUnsafeFlushEnable = dataRegionRatisLogUnsafeFlushEnable;
  }

  public long getConfigNodeRatisLogSegmentSizeMax() {
    return configNodeRatisLogSegmentSizeMax;
  }

  public void setConfigNodeRatisLogSegmentSizeMax(long configNodeRatisLogSegmentSizeMax) {
    this.configNodeRatisLogSegmentSizeMax = configNodeRatisLogSegmentSizeMax;
  }

  public long getSchemaRegionRatisLogSegmentSizeMax() {
    return schemaRegionRatisLogSegmentSizeMax;
  }

  public void setSchemaRegionRatisLogSegmentSizeMax(long schemaRegionRatisLogSegmentSizeMax) {
    this.schemaRegionRatisLogSegmentSizeMax = schemaRegionRatisLogSegmentSizeMax;
  }

  public long getDataRegionRatisLogSegmentSizeMax() {
    return dataRegionRatisLogSegmentSizeMax;
  }

  public void setDataRegionRatisLogSegmentSizeMax(long dataRegionRatisLogSegmentSizeMax) {
    this.dataRegionRatisLogSegmentSizeMax = dataRegionRatisLogSegmentSizeMax;
  }

  public long getConfigNodeSimpleConsensusLogSegmentSizeMax() {
    return configNodeSimpleConsensusLogSegmentSizeMax;
  }

  public void setConfigNodeSimpleConsensusLogSegmentSizeMax(
      long configNodeSimpleConsensusLogSegmentSizeMax) {
    this.configNodeSimpleConsensusLogSegmentSizeMax = configNodeSimpleConsensusLogSegmentSizeMax;
  }

  public long getConfigNodeRatisGrpcFlowControlWindow() {
    return configNodeRatisGrpcFlowControlWindow;
  }

  public void setConfigNodeRatisGrpcFlowControlWindow(long configNodeRatisGrpcFlowControlWindow) {
    this.configNodeRatisGrpcFlowControlWindow = configNodeRatisGrpcFlowControlWindow;
  }

  public long getSchemaRegionRatisGrpcFlowControlWindow() {
    return schemaRegionRatisGrpcFlowControlWindow;
  }

  public void setSchemaRegionRatisGrpcFlowControlWindow(
      long schemaRegionRatisGrpcFlowControlWindow) {
    this.schemaRegionRatisGrpcFlowControlWindow = schemaRegionRatisGrpcFlowControlWindow;
  }

  public long getDataRegionRatisGrpcFlowControlWindow() {
    return dataRegionRatisGrpcFlowControlWindow;
  }

  public void setDataRegionRatisGrpcFlowControlWindow(long dataRegionRatisGrpcFlowControlWindow) {
    this.dataRegionRatisGrpcFlowControlWindow = dataRegionRatisGrpcFlowControlWindow;
  }

  public long getConfigNodeRatisRpcLeaderElectionTimeoutMinMs() {
    return configNodeRatisRpcLeaderElectionTimeoutMinMs;
  }

  public void setConfigNodeRatisRpcLeaderElectionTimeoutMinMs(
      long configNodeRatisRpcLeaderElectionTimeoutMinMs) {
    this.configNodeRatisRpcLeaderElectionTimeoutMinMs =
        configNodeRatisRpcLeaderElectionTimeoutMinMs;
  }

  public long getSchemaRegionRatisRpcLeaderElectionTimeoutMinMs() {
    return schemaRegionRatisRpcLeaderElectionTimeoutMinMs;
  }

  public void setSchemaRegionRatisRpcLeaderElectionTimeoutMinMs(
      long schemaRegionRatisRpcLeaderElectionTimeoutMinMs) {
    this.schemaRegionRatisRpcLeaderElectionTimeoutMinMs =
        schemaRegionRatisRpcLeaderElectionTimeoutMinMs;
  }

  public long getDataRegionRatisRpcLeaderElectionTimeoutMinMs() {
    return dataRegionRatisRpcLeaderElectionTimeoutMinMs;
  }

  public void setDataRegionRatisRpcLeaderElectionTimeoutMinMs(
      long dataRegionRatisRpcLeaderElectionTimeoutMinMs) {
    this.dataRegionRatisRpcLeaderElectionTimeoutMinMs =
        dataRegionRatisRpcLeaderElectionTimeoutMinMs;
  }

  public long getConfigNodeRatisRpcLeaderElectionTimeoutMaxMs() {
    return configNodeRatisRpcLeaderElectionTimeoutMaxMs;
  }

  public void setConfigNodeRatisRpcLeaderElectionTimeoutMaxMs(
      long configNodeRatisRpcLeaderElectionTimeoutMaxMs) {
    this.configNodeRatisRpcLeaderElectionTimeoutMaxMs =
        configNodeRatisRpcLeaderElectionTimeoutMaxMs;
  }

  public long getSchemaRegionRatisRpcLeaderElectionTimeoutMaxMs() {
    return schemaRegionRatisRpcLeaderElectionTimeoutMaxMs;
  }

  public void setSchemaRegionRatisRpcLeaderElectionTimeoutMaxMs(
      long schemaRegionRatisRpcLeaderElectionTimeoutMaxMs) {
    this.schemaRegionRatisRpcLeaderElectionTimeoutMaxMs =
        schemaRegionRatisRpcLeaderElectionTimeoutMaxMs;
  }

  public long getDataRegionRatisRpcLeaderElectionTimeoutMaxMs() {
    return dataRegionRatisRpcLeaderElectionTimeoutMaxMs;
  }

  public void setDataRegionRatisRpcLeaderElectionTimeoutMaxMs(
      long dataRegionRatisRpcLeaderElectionTimeoutMaxMs) {
    this.dataRegionRatisRpcLeaderElectionTimeoutMaxMs =
        dataRegionRatisRpcLeaderElectionTimeoutMaxMs;
  }

  public long getConfigNodeRatisRequestTimeoutMs() {
    return configNodeRatisRequestTimeoutMs;
  }

  public void setConfigNodeRatisRequestTimeoutMs(long configNodeRatisRequestTimeoutMs) {
    this.configNodeRatisRequestTimeoutMs = configNodeRatisRequestTimeoutMs;
  }

  public long getSchemaRegionRatisRequestTimeoutMs() {
    return schemaRegionRatisRequestTimeoutMs;
  }

  public void setSchemaRegionRatisRequestTimeoutMs(long schemaRegionRatisRequestTimeoutMs) {
    this.schemaRegionRatisRequestTimeoutMs = schemaRegionRatisRequestTimeoutMs;
  }

  public long getDataRegionRatisRequestTimeoutMs() {
    return dataRegionRatisRequestTimeoutMs;
  }

  public void setDataRegionRatisRequestTimeoutMs(long dataRegionRatisRequestTimeoutMs) {
    this.dataRegionRatisRequestTimeoutMs = dataRegionRatisRequestTimeoutMs;
  }

  public int getConfigNodeRatisMaxRetryAttempts() {
    return configNodeRatisMaxRetryAttempts;
  }

  public void setConfigNodeRatisMaxRetryAttempts(int configNodeRatisMaxRetryAttempts) {
    this.configNodeRatisMaxRetryAttempts = configNodeRatisMaxRetryAttempts;
  }

  public long getConfigNodeRatisInitialSleepTimeMs() {
    return configNodeRatisInitialSleepTimeMs;
  }

  public void setConfigNodeRatisInitialSleepTimeMs(long configNodeRatisInitialSleepTimeMs) {
    this.configNodeRatisInitialSleepTimeMs = configNodeRatisInitialSleepTimeMs;
  }

  public long getConfigNodeRatisMaxSleepTimeMs() {
    return configNodeRatisMaxSleepTimeMs;
  }

  public void setConfigNodeRatisMaxSleepTimeMs(long configNodeRatisMaxSleepTimeMs) {
    this.configNodeRatisMaxSleepTimeMs = configNodeRatisMaxSleepTimeMs;
  }

  public int getDataRegionRatisMaxRetryAttempts() {
    return dataRegionRatisMaxRetryAttempts;
  }

  public void setDataRegionRatisMaxRetryAttempts(int dataRegionRatisMaxRetryAttempts) {
    this.dataRegionRatisMaxRetryAttempts = dataRegionRatisMaxRetryAttempts;
  }

  public long getDataRegionRatisInitialSleepTimeMs() {
    return dataRegionRatisInitialSleepTimeMs;
  }

  public void setDataRegionRatisInitialSleepTimeMs(long dataRegionRatisInitialSleepTimeMs) {
    this.dataRegionRatisInitialSleepTimeMs = dataRegionRatisInitialSleepTimeMs;
  }

  public long getDataRegionRatisMaxSleepTimeMs() {
    return dataRegionRatisMaxSleepTimeMs;
  }

  public void setDataRegionRatisMaxSleepTimeMs(long dataRegionRatisMaxSleepTimeMs) {
    this.dataRegionRatisMaxSleepTimeMs = dataRegionRatisMaxSleepTimeMs;
  }

  public int getSchemaRegionRatisMaxRetryAttempts() {
    return schemaRegionRatisMaxRetryAttempts;
  }

  public void setSchemaRegionRatisMaxRetryAttempts(int schemaRegionRatisMaxRetryAttempts) {
    this.schemaRegionRatisMaxRetryAttempts = schemaRegionRatisMaxRetryAttempts;
  }

  public long getSchemaRegionRatisInitialSleepTimeMs() {
    return schemaRegionRatisInitialSleepTimeMs;
  }

  public void setSchemaRegionRatisInitialSleepTimeMs(long schemaRegionRatisInitialSleepTimeMs) {
    this.schemaRegionRatisInitialSleepTimeMs = schemaRegionRatisInitialSleepTimeMs;
  }

  public long getSchemaRegionRatisMaxSleepTimeMs() {
    return schemaRegionRatisMaxSleepTimeMs;
  }

  public void setSchemaRegionRatisMaxSleepTimeMs(long schemaRegionRatisMaxSleepTimeMs) {
    this.schemaRegionRatisMaxSleepTimeMs = schemaRegionRatisMaxSleepTimeMs;
  }

  public long getConfigNodeRatisPreserveLogsWhenPurge() {
    return configNodeRatisPreserveLogsWhenPurge;
  }

  public void setConfigNodeRatisPreserveLogsWhenPurge(long configNodeRatisPreserveLogsWhenPurge) {
    this.configNodeRatisPreserveLogsWhenPurge = configNodeRatisPreserveLogsWhenPurge;
  }

  public long getSchemaRegionRatisPreserveLogsWhenPurge() {
    return schemaRegionRatisPreserveLogsWhenPurge;
  }

  public void setSchemaRegionRatisPreserveLogsWhenPurge(
      long schemaRegionRatisPreserveLogsWhenPurge) {
    this.schemaRegionRatisPreserveLogsWhenPurge = schemaRegionRatisPreserveLogsWhenPurge;
  }

  public long getDataRegionRatisPreserveLogsWhenPurge() {
    return dataRegionRatisPreserveLogsWhenPurge;
  }

  public void setDataRegionRatisPreserveLogsWhenPurge(long dataRegionRatisPreserveLogsWhenPurge) {
    this.dataRegionRatisPreserveLogsWhenPurge = dataRegionRatisPreserveLogsWhenPurge;
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

  public long getConfigNodeRatisLogMax() {
    return configNodeRatisLogMax;
  }

  public void setConfigNodeRatisLogMax(long configNodeRatisLogMax) {
    this.configNodeRatisLogMax = configNodeRatisLogMax;
  }

  public long getSchemaRegionRatisLogMax() {
    return schemaRegionRatisLogMax;
  }

  public void setSchemaRegionRatisLogMax(long schemaRegionRatisLogMax) {
    this.schemaRegionRatisLogMax = schemaRegionRatisLogMax;
  }

  public long getDataRegionRatisLogMax() {
    return dataRegionRatisLogMax;
  }

  public void setDataRegionRatisLogMax(long dataRegionRatisLogMax) {
    this.dataRegionRatisLogMax = dataRegionRatisLogMax;
  }

  public int getProcedureCompletedEvictTTL() {
    return procedureCompletedEvictTTL;
  }

  public void setProcedureCompletedEvictTTL(int procedureCompletedEvictTTL) {
    this.procedureCompletedEvictTTL = procedureCompletedEvictTTL;
  }

  public int getProcedureCompletedCleanInterval() {
    return procedureCompletedCleanInterval;
  }

  public void setProcedureCompletedCleanInterval(int procedureCompletedCleanInterval) {
    this.procedureCompletedCleanInterval = procedureCompletedCleanInterval;
  }

  public int getProcedureCoreWorkerThreadsCount() {
    return procedureCoreWorkerThreadsCount;
  }

  public void setProcedureCoreWorkerThreadsCount(int procedureCoreWorkerThreadsCount) {
    this.procedureCoreWorkerThreadsCount = procedureCoreWorkerThreadsCount;
  }

  public boolean isEnableMqttService() {
    return enableMqttService;
  }

  public void setEnableMqttService(boolean enableMqttService) {
    this.enableMqttService = enableMqttService;
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

  public NodeStatus getStatus() {
    return status;
  }

  public void setStatus(NodeStatus status) {
    this.status = status;
  }

  public String getUserFolder() {
    return userFolder;
  }

  public void setUserFolder(String userFolder) {
    this.userFolder = userFolder;
  }

  public String getRoleFolder() {
    return roleFolder;
  }

  public void setRoleFolder(String roleFolder) {
    this.roleFolder = roleFolder;
  }

  public String getProcedureWalFolder() {
    return procedureWalFolder;
  }

  public void setProcedureWalFolder(String procedureWalFolder) {
    this.procedureWalFolder = procedureWalFolder;
  }

  public String getSyncDir() {
    return syncDir;
  }

  public void setSyncDir(String syncDir) {
    this.syncDir = syncDir;
  }

  public FSType getSystemFileStorageFs() {
    return systemFileStorageFs;
  }

  public void setSystemFileStorageFs(FSType systemFileStorageFs) {
    this.systemFileStorageFs = systemFileStorageFs;
  }

  public HandleSystemErrorStrategy getHandleSystemErrorStrategy() {
    return handleSystemErrorStrategy;
  }

  public void setHandleSystemErrorStrategy(HandleSystemErrorStrategy handleSystemErrorStrategy) {
    this.handleSystemErrorStrategy = handleSystemErrorStrategy;
  }

  public void handleUnrecoverableError() {
    handleSystemErrorStrategy.handle();
  }

  public boolean isEnableMemControl() {
    return enableMemControl;
  }

  public void setEnableMemControl(boolean enableMemControl) {
    this.enableMemControl = enableMemControl;
  }

  public long getAllocateMemoryForStorageEngine() {
    return allocateMemoryForStorageEngine;
  }

  public void setAllocateMemoryForStorageEngine(long allocateMemoryForStorageEngine) {
    this.allocateMemoryForStorageEngine = allocateMemoryForStorageEngine;
    this.allocateMemoryForTimePartitionInfo = allocateMemoryForStorageEngine * 50 / 1001;
  }

  public long getAllocateMemoryForRead() {
    return allocateMemoryForRead;
  }

  public void setAllocateMemoryForRead(long allocateMemoryForRead) {
    this.allocateMemoryForRead = allocateMemoryForRead;

    this.allocateMemoryForBloomFilterCache = allocateMemoryForRead / 1001;
    this.allocateMemoryForTimeSeriesMetaDataCache = allocateMemoryForRead * 200 / 1001;
    this.allocateMemoryForChunkCache = allocateMemoryForRead * 100 / 1001;
    this.allocateMemoryForCoordinator = allocateMemoryForRead * 50 / 1001;
    this.allocateMemoryForOperators = allocateMemoryForRead * 200 / 1001;
    this.allocateMemoryForDataExchange = allocateMemoryForRead * 200 / 1001;
    this.allocateMemoryForTimeIndex = allocateMemoryForRead * 200 / 1001;
  }

  public long getAllocateMemoryForFree() {
    return Runtime.getRuntime().maxMemory()
        - allocateMemoryForStorageEngine
        - allocateMemoryForRead
        - allocateMemoryForSchema;
  }

  public long getAllocateMemoryForSchema() {
    return allocateMemoryForSchema;
  }

  public void setAllocateMemoryForSchema(long allocateMemoryForSchema) {
    this.allocateMemoryForSchema = allocateMemoryForSchema;

    this.allocateMemoryForSchemaRegion = allocateMemoryForSchema * 8 / 10;
    this.allocateMemoryForSchemaCache = allocateMemoryForSchema / 10;
    this.allocateMemoryForLastCache = allocateMemoryForSchema / 10;
  }

  public long getAllocateMemoryForConsensus() {
    return allocateMemoryForConsensus;
  }

  public void setAllocateMemoryForConsensus(long allocateMemoryForConsensus) {
    this.allocateMemoryForConsensus = allocateMemoryForConsensus;
  }

  public boolean isDefaultSchemaMemoryConfig() {
    return isDefaultSchemaMemoryConfig;
  }

  public void setDefaultSchemaMemoryConfig(boolean defaultSchemaMemoryConfig) {
    isDefaultSchemaMemoryConfig = defaultSchemaMemoryConfig;
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

  public double getWriteProportionForMemtable() {
    return writeProportionForMemtable;
  }

  public void setWriteProportionForMemtable(double writeProportionForMemtable) {
    this.writeProportionForMemtable = writeProportionForMemtable;
  }

  public double getCompactionProportion() {
    return compactionProportion;
  }

  public void setCompactionProportion(double compactionProportion) {
    this.compactionProportion = compactionProportion;
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

  public long getMaxBytesPerFragmentInstance() {
    return maxBytesPerFragmentInstance;
  }

  @TestOnly
  public void setMaxBytesPerFragmentInstance(long maxBytesPerFragmentInstance) {
    this.maxBytesPerFragmentInstance = maxBytesPerFragmentInstance;
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

  public int getConcurrentWritingTimePartition() {
    return concurrentWritingTimePartition;
  }

  public void setConcurrentWritingTimePartition(int concurrentWritingTimePartition) {
    this.concurrentWritingTimePartition = concurrentWritingTimePartition;
  }

  public int getPrimitiveArraySize() {
    return primitiveArraySize;
  }

  public void setPrimitiveArraySize(int primitiveArraySize) {
    this.primitiveArraySize = primitiveArraySize;
  }

  public double getChunkMetadataSizeProportion() {
    return chunkMetadataSizeProportion;
  }

  public void setChunkMetadataSizeProportion(double chunkMetadataSizeProportion) {
    this.chunkMetadataSizeProportion = chunkMetadataSizeProportion;
  }

  public double getFlushProportion() {
    return flushProportion;
  }

  public void setFlushProportion(double flushProportion) {
    this.flushProportion = flushProportion;
  }

  public double getBufferedArraysMemoryProportion() {
    return bufferedArraysMemoryProportion;
  }

  public void setBufferedArraysMemoryProportion(double bufferedArraysMemoryProportion) {
    this.bufferedArraysMemoryProportion = bufferedArraysMemoryProportion;
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

  public int getCheckPeriodWhenInsertBlocked() {
    return checkPeriodWhenInsertBlocked;
  }

  public void setCheckPeriodWhenInsertBlocked(int checkPeriodWhenInsertBlocked) {
    this.checkPeriodWhenInsertBlocked = checkPeriodWhenInsertBlocked;
  }

  public int getIoTaskQueueSizeForFlushing() {
    return ioTaskQueueSizeForFlushing;
  }

  public void setIoTaskQueueSizeForFlushing(int ioTaskQueueSizeForFlushing) {
    this.ioTaskQueueSizeForFlushing = ioTaskQueueSizeForFlushing;
  }

  public boolean isEnableQueryMemoryEstimation() {
    return enableQueryMemoryEstimation;
  }

  public void setEnableQueryMemoryEstimation(boolean enableQueryMemoryEstimation) {
    this.enableQueryMemoryEstimation = enableQueryMemoryEstimation;
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

  public int getUdfInitialByteArrayLengthForMemoryControl() {
    return udfInitialByteArrayLengthForMemoryControl;
  }

  public void setUdfInitialByteArrayLengthForMemoryControl(
      int udfInitialByteArrayLengthForMemoryControl) {
    this.udfInitialByteArrayLengthForMemoryControl = udfInitialByteArrayLengthForMemoryControl;
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

  public int getPartitionCacheSize() {
    return partitionCacheSize;
  }

  public void setPartitionCacheSize(int partitionCacheSize) {
    this.partitionCacheSize = partitionCacheSize;
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

  public int getTagAttributeFlushInterval() {
    return tagAttributeFlushInterval;
  }

  public void setTagAttributeFlushInterval(int tagAttributeFlushInterval) {
    this.tagAttributeFlushInterval = tagAttributeFlushInterval;
  }

  public int getTagAttributeTotalSize() {
    return tagAttributeTotalSize;
  }

  public void setTagAttributeTotalSize(int tagAttributeTotalSize) {
    this.tagAttributeTotalSize = tagAttributeTotalSize;
  }

  public int getMaxMeasurementNumOfInternalRequest() {
    return maxMeasurementNumOfInternalRequest;
  }

  public void setMaxMeasurementNumOfInternalRequest(int maxMeasurementNumOfInternalRequest) {
    this.maxMeasurementNumOfInternalRequest = maxMeasurementNumOfInternalRequest;
  }

  public boolean isEnableAutoCreateSchema() {
    return enableAutoCreateSchema;
  }

  public void setEnableAutoCreateSchema(boolean enableAutoCreateSchema) {
    this.enableAutoCreateSchema = enableAutoCreateSchema;
  }

  public int getDefaultStorageGroupLevel() {
    return defaultStorageGroupLevel;
  }

  public void setDefaultStorageGroupLevel(int defaultStorageGroupLevel) {
    this.defaultStorageGroupLevel = defaultStorageGroupLevel;
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

  public TSDataType getLongStringInferType() {
    return longStringInferType;
  }

  public void setLongStringInferType(TSDataType longStringInferType) {
    this.longStringInferType = longStringInferType;
  }

  public TSDataType getFloatingStringInferType() {
    return floatingStringInferType;
  }

  public void setFloatingStringInferType(TSDataType floatingStringInferType) {
    this.floatingStringInferType = floatingStringInferType;
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

  public TSEncoding getDefaultBooleanEncoding() {
    return defaultBooleanEncoding;
  }

  public void setDefaultBooleanEncoding(TSEncoding defaultBooleanEncoding) {
    this.defaultBooleanEncoding = defaultBooleanEncoding;
  }

  public TSEncoding getDefaultInt32Encoding() {
    return defaultInt32Encoding;
  }

  public void setDefaultInt32Encoding(TSEncoding defaultInt32Encoding) {
    this.defaultInt32Encoding = defaultInt32Encoding;
  }

  public TSEncoding getDefaultInt64Encoding() {
    return defaultInt64Encoding;
  }

  public void setDefaultInt64Encoding(TSEncoding defaultInt64Encoding) {
    this.defaultInt64Encoding = defaultInt64Encoding;
  }

  public TSEncoding getDefaultFloatEncoding() {
    return defaultFloatEncoding;
  }

  public void setDefaultFloatEncoding(TSEncoding defaultFloatEncoding) {
    this.defaultFloatEncoding = defaultFloatEncoding;
  }

  public TSEncoding getDefaultDoubleEncoding() {
    return defaultDoubleEncoding;
  }

  public void setDefaultDoubleEncoding(TSEncoding defaultDoubleEncoding) {
    this.defaultDoubleEncoding = defaultDoubleEncoding;
  }

  public TSEncoding getDefaultTextEncoding() {
    return defaultTextEncoding;
  }

  public void setDefaultTextEncoding(TSEncoding defaultTextEncoding) {
    this.defaultTextEncoding = defaultTextEncoding;
  }

  public boolean isMetaDataCacheEnable() {
    return metaDataCacheEnable;
  }

  public void setMetaDataCacheEnable(boolean metaDataCacheEnable) {
    this.metaDataCacheEnable = metaDataCacheEnable;
  }

  public boolean isEnableLastCache() {
    return enableLastCache;
  }

  public void setEnableLastCache(boolean enableLastCache) {
    this.enableLastCache = enableLastCache;
  }

  public int getMaxDeduplicatedPathNum() {
    return maxDeduplicatedPathNum;
  }

  public void setMaxDeduplicatedPathNum(int maxDeduplicatedPathNum) {
    this.maxDeduplicatedPathNum = maxDeduplicatedPathNum;
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

  public int getDriverTaskExecutionTimeSliceInMs() {
    return driverTaskExecutionTimeSliceInMs;
  }

  public void setDriverTaskExecutionTimeSliceInMs(int driverTaskExecutionTimeSliceInMs) {
    this.driverTaskExecutionTimeSliceInMs = driverTaskExecutionTimeSliceInMs;
  }

  public int getMaxTsBlockSizeInBytes() {
    return maxTsBlockSizeInBytes;
  }

  public void setMaxTsBlockSizeInBytes(int maxTsBlockSizeInBytes) {
    this.maxTsBlockSizeInBytes = maxTsBlockSizeInBytes;
  }

  public int getMaxTsBlockLineNumber() {
    return maxTsBlockLineNumber;
  }

  public void setMaxTsBlockLineNumber(int maxTsBlockLineNumber) {
    this.maxTsBlockLineNumber = maxTsBlockLineNumber;
  }

  public long getSlowQueryThreshold() {
    return slowQueryThreshold;
  }

  public void setSlowQueryThreshold(long slowQueryThreshold) {
    this.slowQueryThreshold = slowQueryThreshold;
  }

  public long getQueryTimeoutThreshold() {
    return queryTimeoutThreshold;
  }

  public void setQueryTimeoutThreshold(long queryTimeoutThreshold) {
    this.queryTimeoutThreshold = queryTimeoutThreshold;
  }

  public int getMaxAllowedConcurrentQueries() {
    return maxAllowedConcurrentQueries;
  }

  public void setMaxAllowedConcurrentQueries(int maxAllowedConcurrentQueries) {
    this.maxAllowedConcurrentQueries = maxAllowedConcurrentQueries;
  }

  public int getQueryThreadCount() {
    return queryThreadCount;
  }

  public void setQueryThreadCount(int queryThreadCount) {
    this.queryThreadCount = queryThreadCount;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  public String getTimestampPrecision() {
    return timestampPrecision;
  }

  public void setTimestampPrecision(String timestampPrecision) {
    if (!("ms".equals(timestampPrecision)
        || "us".equals(timestampPrecision)
        || "ns".equals(timestampPrecision))) {
      logger.error(
          "Wrong timestamp precision, please set as: ms, us or ns ! Current is: {}",
          timestampPrecision);
      System.exit(-1);
    }
    this.timestampPrecision = timestampPrecision;
  }

  public int getMaxWaitingTimeWhenInsertBlockedInMs() {
    return maxWaitingTimeWhenInsertBlockedInMs;
  }

  public void setMaxWaitingTimeWhenInsertBlockedInMs(int maxWaitingTimeWhenInsertBlockedInMs) {
    this.maxWaitingTimeWhenInsertBlockedInMs = maxWaitingTimeWhenInsertBlockedInMs;
  }

  public boolean isEnableDiscardOutOfOrderData() {
    return enableDiscardOutOfOrderData;
  }

  public void setEnableDiscardOutOfOrderData(boolean enableDiscardOutOfOrderData) {
    this.enableDiscardOutOfOrderData = enableDiscardOutOfOrderData;
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

  public int getFlushThreadCount() {
    return flushThreadCount;
  }

  public void setFlushThreadCount(int flushThreadCount) {
    this.flushThreadCount = flushThreadCount;
  }

  public boolean isEnablePartialInsert() {
    return enablePartialInsert;
  }

  public void setEnablePartialInsert(boolean enablePartialInsert) {
    this.enablePartialInsert = enablePartialInsert;
  }

  public long getRecoveryLogIntervalInMs() {
    return recoveryLogIntervalInMs;
  }

  public void setRecoveryLogIntervalInMs(long recoveryLogIntervalInMs) {
    this.recoveryLogIntervalInMs = recoveryLogIntervalInMs;
  }

  public int getUpgradeThreadCount() {
    return upgradeThreadCount;
  }

  public void setUpgradeThreadCount(int upgradeThreadCount) {
    this.upgradeThreadCount = upgradeThreadCount;
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

  public void setMaxWalNodesNum(int maxWalNodesNum) {
    this.maxWalNodesNum = maxWalNodesNum;
  }

  public long getFsyncWalDelayInMs() {
    return fsyncWalDelayInMs;
  }

  public void setFsyncWalDelayInMs(long fsyncWalDelayInMs) {
    this.fsyncWalDelayInMs = fsyncWalDelayInMs;
  }

  public int getWalBufferSizeInByte() {
    return walBufferSizeInByte;
  }

  public void setWalBufferSizeInByte(int walBufferSizeInByte) {
    this.walBufferSizeInByte = walBufferSizeInByte;
  }

  public int getWalBufferQueueCapacity() {
    return walBufferQueueCapacity;
  }

  public void setWalBufferQueueCapacity(int walBufferQueueCapacity) {
    this.walBufferQueueCapacity = walBufferQueueCapacity;
  }

  public long getWalFileSizeThresholdInByte() {
    return walFileSizeThresholdInByte;
  }

  public void setWalFileSizeThresholdInByte(long walFileSizeThresholdInByte) {
    this.walFileSizeThresholdInByte = walFileSizeThresholdInByte;
  }

  public double getWalMinEffectiveInfoRatio() {
    return walMinEffectiveInfoRatio;
  }

  public void setWalMinEffectiveInfoRatio(double walMinEffectiveInfoRatio) {
    this.walMinEffectiveInfoRatio = walMinEffectiveInfoRatio;
  }

  public long getWalMemTableSnapshotThreshold() {
    return walMemTableSnapshotThreshold;
  }

  public void setWalMemTableSnapshotThreshold(long walMemTableSnapshotThreshold) {
    this.walMemTableSnapshotThreshold = walMemTableSnapshotThreshold;
  }

  public int getMaxWalMemTableSnapshotNum() {
    return maxWalMemTableSnapshotNum;
  }

  public void setMaxWalMemTableSnapshotNum(int maxWalMemTableSnapshotNum) {
    this.maxWalMemTableSnapshotNum = maxWalMemTableSnapshotNum;
  }

  public long getDeleteWalFilesPeriodInMs() {
    return deleteWalFilesPeriodInMs;
  }

  public void setDeleteWalFilesPeriodInMs(long deleteWalFilesPeriodInMs) {
    this.deleteWalFilesPeriodInMs = deleteWalFilesPeriodInMs;
  }

  public long getIotConsensusThrottleThresholdInByte() {
    return iotConsensusThrottleThresholdInByte;
  }

  public void setIotConsensusThrottleThresholdInByte(long iotConsensusThrottleThresholdInByte) {
    this.iotConsensusThrottleThresholdInByte = iotConsensusThrottleThresholdInByte;
  }

  public long getIotConsensusCacheWindowTimeInMs() {
    return iotConsensusCacheWindowTimeInMs;
  }

  public void setIotConsensusCacheWindowTimeInMs(long iotConsensusCacheWindowTimeInMs) {
    this.iotConsensusCacheWindowTimeInMs = iotConsensusCacheWindowTimeInMs;
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

  public String getWatermarkMethod() {
    return watermarkMethod;
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

  public boolean isEnableInfluxDBRpcService() {
    return enableInfluxDBRpcService;
  }

  public void setEnableInfluxDBRpcService(boolean enableInfluxDBRpcService) {
    this.enableInfluxDBRpcService = enableInfluxDBRpcService;
  }

  public int getInfluxDBRpcPort() {
    return influxDBRpcPort;
  }

  public void setInfluxDBRpcPort(int influxDBRpcPort) {
    this.influxDBRpcPort = influxDBRpcPort;
  }

  public boolean isReadOnly() {
    return status == NodeStatus.ReadOnly;
  }

  public NodeStatus getNodeStatus() {
    return status;
  }

  public void setNodeStatusToShutdown() {
    logger.info("System will reject write operations when shutting down.");
    this.status = NodeStatus.ReadOnly;
  }

  public void setNodeStatus(NodeStatus newStatus) {
    logger.info("Set system mode from {} to {}.", status, newStatus);
    this.status = newStatus;
    this.statusReason = null;

    switch (newStatus) {
      case ReadOnly:
        logger.error(
            "Change system status to ReadOnly! Only query statements are permitted!",
            new RuntimeException("System mode is set to READ_ONLY"));
        break;
      case Removing:
        logger.info(
            "Change system status to Removing! The current Node is being removed from cluster!");
        break;
      default:
        break;
    }
  }

  public String getStatusReason() {
    return statusReason;
  }

  public void setStatusReason(String statusReason) {
    this.statusReason = statusReason;
  }

  public boolean isRpcThriftCompressionEnable() {
    return rpcThriftCompressionEnable;
  }

  public void setRpcThriftCompressionEnable(boolean rpcThriftCompressionEnable) {
    this.rpcThriftCompressionEnable = rpcThriftCompressionEnable;
  }

  public int getConnectionTimeoutInMS() {
    return connectionTimeoutInMS;
  }

  public void setConnectionTimeoutInMS(int connectionTimeoutInMS) {
    this.connectionTimeoutInMS = connectionTimeoutInMS;
  }

  public int getSelectorThreadCountOfClientManager() {
    return selectorThreadCountOfClientManager;
  }

  public void setSelectorThreadCountOfClientManager(int selectorThreadCountOfClientManager) {
    this.selectorThreadCountOfClientManager = selectorThreadCountOfClientManager;
  }

  public int getCoreClientCountForEachNodeInClientManager() {
    return coreClientCountForEachNodeInClientManager;
  }

  public void setCoreClientCountForEachNodeInClientManager(
      int coreClientCountForEachNodeInClientManager) {
    this.coreClientCountForEachNodeInClientManager = coreClientCountForEachNodeInClientManager;
  }

  public int getMaxClientCountForEachNodeInClientManager() {
    return maxClientCountForEachNodeInClientManager;
  }

  public void setMaxClientCountForEachNodeInClientManager(
      int maxClientCountForEachNodeInClientManager) {
    this.maxClientCountForEachNodeInClientManager = maxClientCountForEachNodeInClientManager;
  }
}
