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
import org.apache.iotdb.db.audit.AuditLogOperation;
import org.apache.iotdb.db.audit.AuditLogStorage;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.compaction.execute.performer.constant.CrossCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.execute.performer.constant.InnerSeqCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.execute.performer.constant.InnerUnseqCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.schedule.constant.CompactionPriority;
import org.apache.iotdb.db.engine.compaction.selector.constant.CrossCompactionSelector;
import org.apache.iotdb.db.engine.compaction.selector.constant.InnerSequenceCompactionSelector;
import org.apache.iotdb.db.engine.compaction.selector.constant.InnerUnsequenceCompactionSelector;
import org.apache.iotdb.db.engine.storagegroup.timeindex.TimeIndexLevel;
import org.apache.iotdb.db.exception.LoadConfigurationException;
import org.apache.iotdb.db.service.thrift.impl.ClientRPCServiceImpl;
import org.apache.iotdb.db.service.thrift.impl.NewInfluxDBServiceImpl;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.fileSystem.FSType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;

public class IoTDBConfig {

  public static final String CONFIG_NAME = "iotdb-datanode.properties";
  private static final Logger logger = LoggerFactory.getLogger(IoTDBConfig.class);

  private static final String STORAGE_GROUP_MATCHER = "([a-zA-Z0-9`_.\\-\\u2E80-\\u9FFF]+)";
  public static final Pattern STORAGE_GROUP_PATTERN = Pattern.compile(STORAGE_GROUP_MATCHER);

  // e.g., a31+/$%#&[]{}3e4, "a.b", 'a.b'
  private static final String NODE_NAME_MATCHER = "([^\n\t]+)";

  // e.g.,  .s1
  private static final String PARTIAL_NODE_MATCHER = "[" + PATH_SEPARATOR + "]" + NODE_NAME_MATCHER;

  private static final String NODE_MATCHER =
    "([" + PATH_SEPARATOR + "])?" + NODE_NAME_MATCHER + "(" + PARTIAL_NODE_MATCHER + ")*";

  public static final Pattern NODE_PATTERN = Pattern.compile(NODE_MATCHER);

  /** DataNode RPC Configuration */
  // Rpc binding address
  private String dnRpcAddress = "127.0.0.1";
  // Port which the JDBC server listens to
  private int dnRpcPort = 6667;
  // Internal address for DataNode
  private String dnInternalAddress = "127.0.0.1";
  // Internal port for coordinator
  private int dnInternalPort = 10730;
  // Port that mpp data exchange thrift service listen to
  private int dnMppDataExchangePort = 10740;
  // Internal port for SchemaRegion consensus protocol
  private int dnSchemaRegionConsensusPort = 10750;
  // Internal port for dataRegion consensus protocol
  private int dnDataRegionConsensusPort = 10760;
  // The time of data node waiting for the next retry to join into the cluster
  private long dnJoinClusterRetryIntervalMs = TimeUnit.SECONDS.toMillis(5);

  /** Target ConfigNodes */
  // Ip and port of ConfigNodes
  private List<TEndPoint> dnTargetConfigNodeList =
      Collections.singletonList(new TEndPoint("127.0.0.1", 10710));

  /** Connection Configuration */
  // The max time to live of a session in ms. Unit: millisecond
  private int dnSessionTimeoutThreshold = 0;
  // Whether to use thrift compression
  private boolean dnRpcThriftCompressionEnable = false;
  // Whether to use Snappy compression before sending data through the network
  private boolean dnRpcAdvancedCompressionEnable = false;
  // Rpc Selector thread num
  private int dnRpcSelectorThreadCount = 1;
  // Min concurrent client number
  private int dnRpcMinConcurrentClientNum = Runtime.getRuntime().availableProcessors();
  // Max concurrent client number
  private int dnRpcMaxConcurrentClientNum = 65535;
  // Thrift max frame size, 512MB by default
  private int dnThriftMaxFrameSize = 536870912;
  // Thrift init buffer size
  private int dnThriftInitBufferSize = RpcUtils.THRIFT_DEFAULT_BUF_CAPACITY;
  // Thrift socket and connection timeout between DataNode and ConfigNode
  private int dnConnectionTimeoutInMS = (int) TimeUnit.SECONDS.toMillis(20);
  // ClientManager will have so many selector threads (TAsyncClientManager) to distribute to its
  // clients
  private int dnSelectorThreadCountOfClientManager =
      Runtime.getRuntime().availableProcessors() / 4 > 0
          ? Runtime.getRuntime().availableProcessors() / 4
          : 1;
  // The maximum number of clients that can be idle for a node's InternalService. When the number of
  // idle clients on a node exceeds this number, newly returned clients will be released
  private int dnCoreClientCountForEachNodeInClientManager = 200;
  // The maximum number of clients that can be applied for a node's InternalService
  private int dnMaxClientCountForEachNodeInClientManager = 300;

  /** Directory Configuration */
  // System directory, including version file for each database and metadata
  private String dnSystemDir =
      IoTDBConstant.DEFAULT_BASE_DIR + File.separator + IoTDBConstant.SYSTEM_FOLDER_NAME;

  // External lib directory, stores user-uploaded JAR files
  private String dnExtDir = IoTDBConstant.EXT_FOLDER_NAME;

  // Data directories. It can be settled as dataDirs = {"data1", "data2", "data3"};
  private String[] dnDataDirs = {
    IoTDBConstant.DEFAULT_BASE_DIR + File.separator + IoTDBConstant.DATA_FOLDER_NAME
  };

  private static final String MULTI_DIR_STRATEGY_PREFIX =
    "org.apache.iotdb.db.conf.directories.strategy.";
  private static final String DEFAULT_MULTI_DIR_STRATEGY = "MaxDiskUsableSpaceFirstStrategy";
  // Strategy of multiple directories
  private String dnMultiDirStrategyClassName = null;

  // Consensus directory
  private String dnConsensusDir = IoTDBConstant.DEFAULT_BASE_DIR + File.separator + "consensus";

  // WAL directories
  private String[] dnWalDirs = {
    IoTDBConstant.DEFAULT_BASE_DIR + File.separator + IoTDBConstant.WAL_FOLDER_NAME
  };

  // Performance tracing directory, stores performance tracing files
  private String dnTracingDir =
      IoTDBConstant.DEFAULT_BASE_DIR + File.separator + IoTDBConstant.TRACING_FOLDER_NAME;

  // Sync directory, including the log and hardlink tsfiles
  private String dnSyncDir =
      IoTDBConstant.DEFAULT_BASE_DIR + File.separator + IoTDBConstant.SYNC_FOLDER_NAME;

  // Schema directory, including storage set of values
  private String schemaDir =
    IoTDBConstant.DEFAULT_BASE_DIR
      + File.separator
      + IoTDBConstant.SYSTEM_FOLDER_NAME
      + File.separator
      + IoTDBConstant.SCHEMA_FOLDER_NAME;

  // Query directory, stores temporary files of query
  private String queryDir =
    IoTDBConstant.DEFAULT_BASE_DIR + File.separator + IoTDBConstant.QUERY_FOLDER_NAME;

  /** Metric Configuration */
  // TODO: Add if necessary

  /** Compaction Configurations */
  // TODO: Move to CommonConfig
  // Enable inner space compaction for sequence files
  private boolean enableSeqSpaceCompaction = true;
  // Enable inner space compaction for unsequence files
  private boolean enableUnseqSpaceCompaction = true;
  // Compact the unsequence files into the overlapped sequence files
  private boolean enableCrossSpaceCompaction = true;

  // The strategy of inner space compaction task. There are just one inner space compaction strategy SIZE_TIRED_COMPACTION:
  private InnerSequenceCompactionSelector innerSequenceCompactionSelector =
    InnerSequenceCompactionSelector.SIZE_TIERED;

  private InnerSeqCompactionPerformer innerSeqCompactionPerformer =
    InnerSeqCompactionPerformer.READ_CHUNK;

  private InnerUnsequenceCompactionSelector innerUnsequenceCompactionSelector =
    InnerUnsequenceCompactionSelector.SIZE_TIERED;

  private InnerUnseqCompactionPerformer innerUnseqCompactionPerformer =
    InnerUnseqCompactionPerformer.READ_POINT;

  // The strategy of cross space compaction task. There are just one cross space compaction strategy
  // SIZE_TIRED_COMPACTION:
  private CrossCompactionSelector crossCompactionSelector = CrossCompactionSelector.REWRITE;
  private CrossCompactionPerformer crossCompactionPerformer = CrossCompactionPerformer.READ_POINT;

  // The priority of compaction task execution. There are three priority strategy INNER_CROSS:
  // prioritize inner space compaction, reduce the number of files first CROSS INNER: prioritize
  // cross space compaction, eliminate the unsequence files first BALANCE: alternate two compaction types
  private CompactionPriority compactionPriority = CompactionPriority.BALANCE;

  // The target tsfile size in compaction, 1 GB by default
  private long targetCompactionFileSize = 1073741824L;

  // The target chunk size in compaction
  private long targetChunkSize = 1048576L;

  // The target chunk point num in compaction
  private long targetChunkPointNum = 100000L;

  // If the chunk size is lower than this threshold, it will be deserialized into points, default is 10 KB
  private long chunkSizeLowerBoundInCompaction = 10240L;

  // If the chunk point num is lower than this threshold, it will be deserialized into points, default is 1000
  private long chunkPointNumLowerBoundInCompaction = 1000;

  // If compaction thread cannot acquire the write lock within this timeout, the compaction task will be abort.
  private long compactionAcquireWriteLockTimeout = 60_000L;

  // The max candidate file num in inner space compaction
  private int maxInnerCompactionCandidateFileNum = 30;

  // The max candidate file num in cross space compaction
  private int maxCrossCompactionCandidateFileNum = 1000;

  // The max total size of candidate files in cross space compaction
  private long maxCrossCompactionCandidateFileSize = 1024 * 1024 * 1024 * 5L;

  // The interval of compaction task schedulation in each virtual database. The unit is ms
  private long compactionScheduleIntervalInMs = 60_000L;

  // The interval of compaction task submission from queue in CompactionTaskMananger
  private long compactionSubmissionIntervalInMs = 60_000L;

  // The number of sub compaction threads to be set up to perform compaction. Currently only works
  // for nonAligned data in cross space compaction and unseq inner space compaction.
  private int subCompactionTaskNum = 4;

  private boolean enableCompactionValidation = true;

  // If one merge file selection runs for more than this time, it will be ended and its current
  // selection will be used as final selection. When < 0, it means time is unbounded. Unit: millisecond
  private long crossCompactionFileSelectionTimeBudget = 30 * 1000L;

  /** Retain Configuration */
  // Default TSfile storage is in local file system
  private FSType tsFileStorageFs = FSType.LOCAL;

  // Default core-site.xml file path is /etc/hadoop/conf/core-site.xml
  private String coreSitePath = "/etc/hadoop/conf/core-site.xml";

  // Default hdfs-site.xml file path is /etc/hadoop/conf/hdfs-site.xml
  private String hdfsSitePath = "/etc/hadoop/conf/hdfs-site.xml";

  // Default HDFS ip is localhost
  private String hdfsIp = "localhost";

  // Default HDFS port is 9000
  private String hdfsPort = "9000";


  /* Names of Watermark methods */

  /** The proportion of write memory for loading TsFile */
  private double loadTsFileProportion = 0.125;

  /** Size threshold of each checkpoint file. Unit: byte */
  private volatile long checkpointFileSizeThresholdInByte = 3 * 1024 * 1024L;

  /** Buffer entry size of each wal buffer. Unit: byte */
  private int walBufferEntrySize = 16 * 1024;

  /** External lib directory for ext Pipe plugins, stores user-defined JAR files */
  private String extPipeDir =
      IoTDBConstant.EXT_FOLDER_NAME + File.separator + IoTDBConstant.EXT_PIPE_FOLDER_NAME;

  /** External lib directory for MQTT, stores user-uploaded JAR files */
  private String mqttDir =
      IoTDBConstant.EXT_FOLDER_NAME + File.separator + IoTDBConstant.MQTT_FOLDER_NAME;

  private String loadTsFileDir =
      dnDataDirs[0] + File.separator + IoTDBConstant.LOAD_TSFILE_FOLDER_NAME;

  private String ratisDataRegionSnapshotDir =
      IoTDBConstant.DEFAULT_BASE_DIR
          + File.separator
          + IoTDBConstant.DATA_FOLDER_NAME
          + File.separator
          + IoTDBConstant.SNAPSHOT_FOLDER_NAME;

  private String dataRegionConsensusDir = dnConsensusDir + File.separator + "data_region";

  private String schemaRegionConsensusDir = dnConsensusDir + File.separator + "schema_region";

  /** Maximum MemTable number. Invalid when enableMemControl is true. */
  private int maxMemtableNumber = 0;

  /** How many threads can concurrently evaluate windows. When <= 0, use CPU core number. */
  private int windowEvaluationThreadCount = Runtime.getRuntime().availableProcessors();

  /**
   * Max number of window evaluation tasks that can be pending for execution. When <= 0, the value
   * is 64 by default.
   */
  private int maxPendingWindowEvaluationTasks = 64;

  /** When a unSequence TsFile's file size (in byte) exceed this, the TsFile is forced closed. */
  private long unSeqTsFileSize = 0L;

  /** When a sequence TsFile's file size (in byte) exceed this, the TsFile is forced closed. */
  private long seqTsFileSize = 0L;

  /** Set true to enable statistics monitor service, false to disable statistics service. */
  private boolean enableStatMonitor = false;

  /** Set true to enable writing monitor time series. */
  private boolean enableMonitorSeriesWrite = false;

  /** Examining period of cache file reader : 100 seconds. Unit: millisecond */
  private long cacheFileReaderClearPeriod = 100000;

  /** Replace implementation class of JDBC service */
  private String rpcImplClassName = ClientRPCServiceImpl.class.getName();

  /** indicate whether current mode is cluster */
  private boolean isClusterMode = false;

  /**
   * The cluster name that this DataNode joined in the cluster mode. The default value
   * "defaultCluster" will be changed after join cluster
   */
  private String clusterName = "defaultCluster";

  /**
   * The DataNodeId of this DataNode for cluster mode. The default value -1 will be changed after
   * join cluster
   */
  private int dataNodeId = -1;

  /** Replace implementation class of influxdb protocol service */
  private String influxdbImplClassName = NewInfluxDBServiceImpl.class.getName();

  /** How many threads will be set up to perform settle tasks. */
  private int settleThreadNum = 1;

  /** The limit of compaction merge can reach per second */
  private int compactionWriteThroughputMbPerSec = 16;

  /**
   * How many thread will be set up to perform compaction, 10 by default. Set to 1 when less than or
   * equal to 0.
   */
  private int compactionThreadCount = 10;




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

  /** Time partition interval in milliseconds */
  private long dnTimePartitionInterval = 604_800_000;

  /**
   * Level of TimeIndex, which records the start time and end time of TsFileResource. Currently,
   * DEVICE_TIME_INDEX and FILE_TIME_INDEX are supported, and could not be changed after first set.
   */
  private TimeIndexLevel timeIndexLevel = TimeIndexLevel.DEVICE_TIME_INDEX;

  // just for test
  // wait for 60 second by default.
  private int thriftServerAwaitTimeForStopService = 60;

  /** The cached record size (in MB) of each series in group by fill query */
  private float groupByFillCacheSizeInMB = (float) 1.0;

  // time in nanosecond precision when starting up
  private long startUpNanosecond = System.nanoTime();

  /**
   * whether enable the rpc service. This parameter has no a corresponding field in the
   * iotdb-common.properties
   */
  private boolean enableRpcService = true;

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

  /** maximum number of logged pages before log erased */
  private int schemaFileLogSize = 16384;

  /** CQ related */
  private long cqMinEveryIntervalInMs = 1_000;

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

  IoTDBConfig() {}

  public float getGroupByFillCacheSizeInMB() {
    return groupByFillCacheSizeInMB;
  }

  public void setGroupByFillCacheSizeInMB(float groupByFillCacheSizeInMB) {
    this.groupByFillCacheSizeInMB = groupByFillCacheSizeInMB;
  }

  public long getDnTimePartitionInterval() {
    return dnTimePartitionInterval;
  }

  public void setDnTimePartitionInterval(long dnTimePartitionInterval) {
    this.dnTimePartitionInterval = dnTimePartitionInterval;
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

  /** if the folders are relative paths, add IOTDB_DATA_HOME as the path prefix */
  private void formulateFolders() {
    dnSystemDir = addDataHomeDir(dnSystemDir);
    schemaDir = addDataHomeDir(schemaDir);
    loadTsFileDir = addDataHomeDir(loadTsFileDir);
    dnTracingDir = addDataHomeDir(dnTracingDir);
    dnConsensusDir = addDataHomeDir(dnConsensusDir);
    dataRegionConsensusDir = addDataHomeDir(dataRegionConsensusDir);
    ratisDataRegionSnapshotDir = addDataHomeDir(ratisDataRegionSnapshotDir);
    schemaRegionConsensusDir = addDataHomeDir(schemaRegionConsensusDir);
    indexRootFolder = addDataHomeDir(indexRootFolder);
    dnExtDir = addDataHomeDir(dnExtDir);
    udfDir = addDataHomeDir(udfDir);
    udfTemporaryLibDir = addDataHomeDir(udfTemporaryLibDir);
    triggerDir = addDataHomeDir(triggerDir);
    triggerTemporaryLibDir = addDataHomeDir(triggerTemporaryLibDir);
    mqttDir = addDataHomeDir(mqttDir);

    extPipeDir = addDataHomeDir(extPipeDir);

    if (TSFileDescriptor.getInstance().getConfig().getTSFileStorageFs().equals(FSType.HDFS)) {
      String hdfsDir = getHdfsDir();
      queryDir = hdfsDir + File.separatorChar + queryDir;
      for (int i = 0; i < dnDataDirs.length; i++) {
        dnDataDirs[i] = hdfsDir + File.separatorChar + dnDataDirs[i];
      }
    } else {
      queryDir = addDataHomeDir(queryDir);
      for (int i = 0; i < dnDataDirs.length; i++) {
        dnDataDirs[i] = addDataHomeDir(dnDataDirs[i]);
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
        dataDirs[i] = addDataHomeDir(dataDirs[i]);
      }
    }
    // make sure old data directories not removed
    HashSet<String> newDirs = new HashSet<>(Arrays.asList(dataDirs));
    for (String oldDir : this.dnDataDirs) {
      if (!newDirs.contains(oldDir)) {
        String msg =
            String.format("%s is removed from data_dirs parameter, please add it back.", oldDir);
        logger.error(msg);
        throw new LoadConfigurationException(msg);
      }
    }
    this.dnDataDirs = dataDirs;
    DirectoryManager.getInstance().updateFileFolders();
  }

  //  private String addHomeDir(String dir) {
  //    return addDirPrefix(System.getProperty(IoTDBConstant.IOTDB_HOME, null), dir);
  //  }

  // if IOTDB_DATA_HOME is not set, then we keep dataHomeDir prefix being the same with IOTDB_HOME
  // In this way, we can keep consistent with v0.13.0~2.
  private String addDataHomeDir(String dir) {
    String dataHomeDir = System.getProperty(IoTDBConstant.IOTDB_DATA_HOME, null);
    if (dataHomeDir == null) {
      dataHomeDir = System.getProperty(IoTDBConstant.IOTDB_HOME, null);
    }
    return addDirPrefix(dataHomeDir, dir);
  }

  private String addDirPrefix(String prefix, String dir) {
    if (!new File(dir).isAbsolute() && prefix != null && prefix.length() > 0) {
      if (!prefix.endsWith(File.separator)) {
        dir = prefix + File.separatorChar + dir;
      } else {
        dir = prefix + dir;
      }
    }
    return dir;
  }

  void confirmMultiDirStrategy() {
    if (getDnMultiDirStrategyClassName() == null) {
      dnMultiDirStrategyClassName = DEFAULT_MULTI_DIR_STRATEGY;
    }
    if (!getDnMultiDirStrategyClassName().contains(TsFileConstant.PATH_SEPARATOR)) {
      dnMultiDirStrategyClassName = MULTI_DIR_STRATEGY_PREFIX + dnMultiDirStrategyClassName;
    }

    try {
      Class.forName(dnMultiDirStrategyClassName);
    } catch (ClassNotFoundException e) {
      logger.warn(
          "Cannot find given directory strategy {}, using the default value",
          getDnMultiDirStrategyClassName(),
          e);
      setDnMultiDirStrategyClassName(MULTI_DIR_STRATEGY_PREFIX + DEFAULT_MULTI_DIR_STRATEGY);
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

  public String[] getDnDataDirs() {
    return dnDataDirs;
  }

  public void setDnDataDirs(String[] dnDataDirs) {
    this.dnDataDirs = dnDataDirs;
    // TODO(szywilliam): rewrite the logic here when ratis supports complete snapshot semantic
    setRatisDataRegionSnapshotDir(
        dnDataDirs[0] + File.separator + IoTDBConstant.SNAPSHOT_FOLDER_NAME);
    setLoadTsFileDir(dnDataDirs[0] + File.separator + IoTDBConstant.LOAD_TSFILE_FOLDER_NAME);
  }

  public String getDnRpcAddress() {
    return dnRpcAddress;
  }

  public void setDnRpcAddress(String dnRpcAddress) {
    this.dnRpcAddress = dnRpcAddress;
  }

  public int getDnRpcPort() {
    return dnRpcPort;
  }

  public void setDnRpcPort(int dnRpcPort) {
    this.dnRpcPort = dnRpcPort;
  }

  public String getDnSystemDir() {
    return dnSystemDir;
  }

  void setDnSystemDir(String dnSystemDir) {
    this.dnSystemDir = dnSystemDir;
  }

  public String getLoadTsFileDir() {
    return loadTsFileDir;
  }

  public void setLoadTsFileDir(String loadTsFileDir) {
    this.loadTsFileDir = loadTsFileDir;
  }

  public String getSchemaDir() {
    return schemaDir;
  }

  public void setSchemaDir(String schemaDir) {
    this.schemaDir = schemaDir;
  }

  public String getDnTracingDir() {
    return dnTracingDir;
  }

  void setDnTracingDir(String dnTracingDir) {
    this.dnTracingDir = dnTracingDir;
  }

  public String getQueryDir() {
    return queryDir;
  }

  void setQueryDir(String queryDir) {
    this.queryDir = queryDir;
  }

  public String getRatisDataRegionSnapshotDir() {
    return ratisDataRegionSnapshotDir;
  }

  public void setRatisDataRegionSnapshotDir(String ratisDataRegionSnapshotDir) {
    this.ratisDataRegionSnapshotDir = ratisDataRegionSnapshotDir;
  }

  public String getDnConsensusDir() {
    return dnConsensusDir;
  }

  public void setDnConsensusDir(String dnConsensusDir) {
    this.dnConsensusDir = dnConsensusDir;
    setDataRegionConsensusDir(dnConsensusDir + File.separator + "data_region");
    setSchemaRegionConsensusDir(dnConsensusDir + File.separator + "schema_region");
  }

  public String[] getDnWalDirs() {
    return dnWalDirs;
  }

  public void setDnWalDirs(String[] dnWalDirs) {
    this.dnWalDirs = dnWalDirs;
  }

  public String getDnSyncDir() {
    return dnSyncDir;
  }

  public void setDnSyncDir(String dnSyncDir) {
    this.dnSyncDir = dnSyncDir;
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

  public String getDnExtDir() {
    return dnExtDir;
  }

  public void setDnExtDir(String dnExtDir) {
    this.dnExtDir = dnExtDir;
  }

  public String getMqttDir() {
    return mqttDir;
  }

  public void setMqttDir(String mqttDir) {
    this.mqttDir = mqttDir;
  }

  public String getDnMultiDirStrategyClassName() {
    return dnMultiDirStrategyClassName;
  }

  void setDnMultiDirStrategyClassName(String dnMultiDirStrategyClassName) {
    this.dnMultiDirStrategyClassName = dnMultiDirStrategyClassName;
  }

  public void checkMultiDirStrategyClassName() {
    if (isClusterMode
        && !(dnMultiDirStrategyClassName.equals(DEFAULT_MULTI_DIR_STRATEGY)
            || dnMultiDirStrategyClassName.equals(
                MULTI_DIR_STRATEGY_PREFIX + DEFAULT_MULTI_DIR_STRATEGY))) {
      String msg =
          String.format(
              "Cannot set multi_dir_strategy to %s, because cluster mode only allows MaxDiskUsableSpaceFirstStrategy.",
              dnMultiDirStrategyClassName);
      logger.error(msg);
      throw new RuntimeException(msg);
    }
  }

  public int getMaxMemtableNumber() {
    return maxMemtableNumber;
  }

  public void setMaxMemtableNumber(int maxMemtableNumber) {
    this.maxMemtableNumber = maxMemtableNumber;
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

  public int getDnRpcSelectorThreadCount() {
    return dnRpcSelectorThreadCount;
  }

  public void setDnRpcSelectorThreadCount(int dnRpcSelectorThreadCount) {
    this.dnRpcSelectorThreadCount = dnRpcSelectorThreadCount;
  }

  public int getDnRpcMinConcurrentClientNum() {
    return dnRpcMinConcurrentClientNum;
  }

  public void setDnRpcMinConcurrentClientNum(int dnRpcMinConcurrentClientNum) {
    this.dnRpcMinConcurrentClientNum = dnRpcMinConcurrentClientNum;
  }

  public int getDnRpcMaxConcurrentClientNum() {
    return dnRpcMaxConcurrentClientNum;
  }

  void setDnRpcMaxConcurrentClientNum(int dnRpcMaxConcurrentClientNum) {
    this.dnRpcMaxConcurrentClientNum = dnRpcMaxConcurrentClientNum;
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

  public int getDnSessionTimeoutThreshold() {
    return dnSessionTimeoutThreshold;
  }

  public void setDnSessionTimeoutThreshold(int dnSessionTimeoutThreshold) {
    this.dnSessionTimeoutThreshold = dnSessionTimeoutThreshold;
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

  public int getWalBufferEntrySize() {
    return walBufferEntrySize;
  }

  void setWalBufferEntrySize(int walBufferEntrySize) {
    this.walBufferEntrySize = walBufferEntrySize;
  }

  public long getCheckpointFileSizeThresholdInByte() {
    return checkpointFileSizeThresholdInByte;
  }

  public void setCheckpointFileSizeThresholdInByte(long checkpointFileSizeThresholdInByte) {
    this.checkpointFileSizeThresholdInByte = checkpointFileSizeThresholdInByte;
  }

  public int getCompactionThreadCount() {
    return compactionThreadCount;
  }

  public void setCompactionThreadCount(int compactionThreadCount) {
    this.compactionThreadCount = compactionThreadCount;
  }

  public int getCompactionWriteThroughputMbPerSec() {
    return compactionWriteThroughputMbPerSec;
  }

  public void setCompactionWriteThroughputMbPerSec(int compactionWriteThroughputMbPerSec) {
    this.compactionWriteThroughputMbPerSec = compactionWriteThroughputMbPerSec;
  }

  public long getCrossCompactionFileSelectionTimeBudget() {
    return crossCompactionFileSelectionTimeBudget;
  }

  void setCrossCompactionFileSelectionTimeBudget(long crossCompactionFileSelectionTimeBudget) {
    this.crossCompactionFileSelectionTimeBudget = crossCompactionFileSelectionTimeBudget;
  }

  public boolean isDnRpcThriftCompressionEnable() {
    return dnRpcThriftCompressionEnable;
  }

  public void setDnRpcThriftCompressionEnable(boolean dnRpcThriftCompressionEnable) {
    this.dnRpcThriftCompressionEnable = dnRpcThriftCompressionEnable;
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

  public long getStartUpNanosecond() {
    return startUpNanosecond;
  }

  public int getDnThriftMaxFrameSize() {
    return dnThriftMaxFrameSize;
  }

  public void setDnThriftMaxFrameSize(int dnThriftMaxFrameSize) {
    this.dnThriftMaxFrameSize = dnThriftMaxFrameSize;
    RpcTransportFactory.setThriftMaxFrameSize(this.dnThriftMaxFrameSize);
  }

  public int getDnThriftInitBufferSize() {
    return dnThriftInitBufferSize;
  }

  public void setDnThriftInitBufferSize(int dnThriftInitBufferSize) {
    this.dnThriftInitBufferSize = dnThriftInitBufferSize;
    RpcTransportFactory.setDefaultBufferCapacity(this.dnThriftInitBufferSize);
  }

  public boolean isDnRpcAdvancedCompressionEnable() {
    return dnRpcAdvancedCompressionEnable;
  }

  public void setDnRpcAdvancedCompressionEnable(boolean dnRpcAdvancedCompressionEnable) {
    this.dnRpcAdvancedCompressionEnable = dnRpcAdvancedCompressionEnable;
    RpcTransportFactory.setUseSnappy(this.dnRpcAdvancedCompressionEnable);
  }

  public boolean isEnableRpcService() {
    return enableRpcService;
  }

  public void setEnableRpcService(boolean enableRpcService) {
    this.enableRpcService = enableRpcService;
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

  public long getMaxCrossCompactionCandidateFileSize() {
    return maxCrossCompactionCandidateFileSize;
  }

  public void setMaxCrossCompactionCandidateFileSize(long maxCrossCompactionCandidateFileSize) {
    this.maxCrossCompactionCandidateFileSize = maxCrossCompactionCandidateFileSize;
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

  public int getSchemaFileLogSize() {
    return schemaFileLogSize;
  }

  public void setSchemaFileLogSize(int schemaFileLogSize) {
    this.schemaFileLogSize = schemaFileLogSize;
  }

  public String getDnInternalAddress() {
    return dnInternalAddress;
  }

  public void setDnInternalAddress(String dnInternalAddress) {
    this.dnInternalAddress = dnInternalAddress;
  }

  public int getDnInternalPort() {
    return dnInternalPort;
  }

  public void setDnInternalPort(int dnInternalPort) {
    this.dnInternalPort = dnInternalPort;
  }

  public int getDnDataRegionConsensusPort() {
    return dnDataRegionConsensusPort;
  }

  public void setDnDataRegionConsensusPort(int dnDataRegionConsensusPort) {
    this.dnDataRegionConsensusPort = dnDataRegionConsensusPort;
  }

  public int getDnSchemaRegionConsensusPort() {
    return dnSchemaRegionConsensusPort;
  }

  public void setDnSchemaRegionConsensusPort(int dnSchemaRegionConsensusPort) {
    this.dnSchemaRegionConsensusPort = dnSchemaRegionConsensusPort;
  }

  public List<TEndPoint> getDnTargetConfigNodeList() {
    return dnTargetConfigNodeList;
  }

  public void setDnTargetConfigNodeList(List<TEndPoint> dnTargetConfigNodeList) {
    this.dnTargetConfigNodeList = dnTargetConfigNodeList;
  }

  public long getDnJoinClusterRetryIntervalMs() {
    return dnJoinClusterRetryIntervalMs;
  }

  public void setDnJoinClusterRetryIntervalMs(long dnJoinClusterRetryIntervalMs) {
    this.dnJoinClusterRetryIntervalMs = dnJoinClusterRetryIntervalMs;
  }

  public int getDnMppDataExchangePort() {
    return dnMppDataExchangePort;
  }

  public void setDnMppDataExchangePort(int dnMppDataExchangePort) {
    this.dnMppDataExchangePort = dnMppDataExchangePort;
  }

  public int getDnConnectionTimeoutInMS() {
    return dnConnectionTimeoutInMS;
  }

  public void setDnConnectionTimeoutInMS(int dnConnectionTimeoutInMS) {
    this.dnConnectionTimeoutInMS = dnConnectionTimeoutInMS;
  }

  public int getDnMaxClientCountForEachNodeInClientManager() {
    return dnMaxClientCountForEachNodeInClientManager;
  }

  public void setDnMaxClientCountForEachNodeInClientManager(
      int dnMaxClientCountForEachNodeInClientManager) {
    this.dnMaxClientCountForEachNodeInClientManager = dnMaxClientCountForEachNodeInClientManager;
  }

  public int getDnCoreClientCountForEachNodeInClientManager() {
    return dnCoreClientCountForEachNodeInClientManager;
  }

  public void setDnCoreClientCountForEachNodeInClientManager(
      int dnCoreClientCountForEachNodeInClientManager) {
    this.dnCoreClientCountForEachNodeInClientManager = dnCoreClientCountForEachNodeInClientManager;
  }

  public int getDnSelectorThreadCountOfClientManager() {
    return dnSelectorThreadCountOfClientManager;
  }

  public void setDnSelectorThreadCountOfClientManager(int dnSelectorThreadCountOfClientManager) {
    this.dnSelectorThreadCountOfClientManager = dnSelectorThreadCountOfClientManager;
  }

  public boolean isClusterMode() {
    return isClusterMode;
  }

  public void setClusterMode(boolean isClusterMode) {
    this.isClusterMode = isClusterMode;
    checkMultiDirStrategyClassName();
  }

  public String getClusterName() {
    return clusterName;
  }

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  public int getDataNodeId() {
    return dataNodeId;
  }

  public void setDataNodeId(int dataNodeId) {
    this.dataNodeId = dataNodeId;
  }

  public String getExtPipeDir() {
    return extPipeDir;
  }

  public void setExtPipeDir(String extPipeDir) {
    this.extPipeDir = extPipeDir;
  }

  public TEndPoint getAddressAndPort() {
    return new TEndPoint(dnRpcAddress, dnRpcPort);
  }

  public double getLoadTsFileProportion() {
    return loadTsFileProportion;
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

  public long getCqMinEveryIntervalInMs() {
    return cqMinEveryIntervalInMs;
  }

  public void setCqMinEveryIntervalInMs(long cqMinEveryIntervalInMs) {
    this.cqMinEveryIntervalInMs = cqMinEveryIntervalInMs;
  }


  public Properties getCustomizedProperties() {
    return customizedProperties;
  }

  public void setCustomizedProperties(Properties customizedProperties) {
    this.customizedProperties = customizedProperties;
  }

  public boolean isEnableCompactionValidation() {
    return enableCompactionValidation;
  }

  public void setEnableCompactionValidation(boolean enableCompactionValidation) {
    this.enableCompactionValidation = enableCompactionValidation;
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
}
