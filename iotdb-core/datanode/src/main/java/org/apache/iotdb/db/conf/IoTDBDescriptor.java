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

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.ConfigurationFileUtils;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.BadNodeUrlException;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.utils.NodeUrlUtils;
import org.apache.iotdb.confignode.rpc.thrift.TCQConfig;
import org.apache.iotdb.confignode.rpc.thrift.TGlobalConfig;
import org.apache.iotdb.confignode.rpc.thrift.TRatisConfig;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.service.metrics.IoTDBInternalLocalReporter;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.constant.CrossCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.constant.InnerSeqCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.constant.InnerUnseqCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduleTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.constant.CompactionPriority;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.constant.CrossCompactionSelector;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.constant.InnerSequenceCompactionSelector;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.constant.InnerUnsequenceCompactionSelector;
import org.apache.iotdb.db.storageengine.dataregion.wal.WALManager;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALMode;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;
import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.db.utils.MemUtils;
import org.apache.iotdb.db.utils.datastructure.TVListSortAlgorithm;
import org.apache.iotdb.external.api.IPropertiesLoader;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.config.ReloadLevel;
import org.apache.iotdb.metrics.metricsets.system.SystemMetrics;
import org.apache.iotdb.metrics.reporter.iotdb.IoTDBInternalMemoryReporter;
import org.apache.iotdb.metrics.reporter.iotdb.IoTDBInternalReporter;
import org.apache.iotdb.metrics.utils.InternalReporterType;
import org.apache.iotdb.metrics.utils.NodeType;
import org.apache.iotdb.rpc.DeepCopyRpcTransportFactory;
import org.apache.iotdb.rpc.ZeroCopyRpcTransportFactory;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.fileSystem.FSType;
import org.apache.tsfile.utils.FilePathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.Set;

public class IoTDBDescriptor {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBDescriptor.class);

  private static final CommonDescriptor commonDescriptor = CommonDescriptor.getInstance();

  private static final IoTDBConfig conf = new IoTDBConfig();

  private static final long MAX_THROTTLE_THRESHOLD = 600 * 1024 * 1024 * 1024L;

  private static final long MIN_THROTTLE_THRESHOLD = 50 * 1024 * 1024 * 1024L;

  private static final double MAX_DIR_USE_PROPORTION = 0.8;

  private static final double MIN_DIR_USE_PROPORTION = 0.5;

  static {
    URL systemConfigUrl = getPropsUrl(CommonConfig.SYSTEM_CONFIG_NAME);
    URL configNodeUrl = getPropsUrl(CommonConfig.OLD_CONFIG_NODE_CONFIG_NAME);
    URL dataNodeUrl = getPropsUrl(CommonConfig.OLD_DATA_NODE_CONFIG_NAME);
    URL commonConfigUrl = getPropsUrl(CommonConfig.OLD_COMMON_CONFIG_NAME);
    try {
      ConfigurationFileUtils.checkAndMayUpdate(
          systemConfigUrl, configNodeUrl, dataNodeUrl, commonConfigUrl);
    } catch (Exception e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      LOGGER.error("Failed to update config file", e);
    }
  }

  protected IoTDBDescriptor() {
    loadProps();
    ServiceLoader<IPropertiesLoader> propertiesLoaderServiceLoader =
        ServiceLoader.load(IPropertiesLoader.class);
    for (IPropertiesLoader loader : propertiesLoaderServiceLoader) {
      LOGGER.info("Will reload properties from {} ", loader.getClass().getName());
      Properties properties = loader.loadProperties();
      try {
        loadProperties(properties);
      } catch (Exception e) {
        LOGGER.error(
            "Failed to reload properties from {}, reject DataNode startup.",
            loader.getClass().getName(),
            e);
        System.exit(-1);
      }
      conf.setCustomizedProperties(loader.getCustomizedProperties());
      TSFileDescriptor.getInstance().overwriteConfigByCustomSettings(properties);
      TSFileDescriptor.getInstance()
          .getConfig()
          .setCustomizedProperties(loader.getCustomizedProperties());
    }
  }

  public static IoTDBDescriptor getInstance() {
    return IoTDBDescriptorHolder.INSTANCE;
  }

  public IoTDBConfig getConfig() {
    return conf;
  }

  /**
   * get props url location
   *
   * @return url object if location exit, otherwise null.
   */
  public static URL getPropsUrl(String configFileName) {
    String urlString = commonDescriptor.getConfDir();
    if (urlString == null) {
      // If urlString wasn't provided, try to find a default config in the root of the classpath.
      URL uri = IoTDBConfig.class.getResource("/" + configFileName);
      if (uri != null) {
        return uri;
      }
      LOGGER.warn(
          "Cannot find IOTDB_HOME or IOTDB_CONF environment variable when loading "
              + "config file {}, use default configuration",
          configFileName);
      // update all data seriesPath
      conf.updatePath();
      return null;
    }
    // If a config location was provided, but it doesn't end with a properties file,
    // append the default location.
    else if (!urlString.endsWith(".properties")) {
      urlString += (File.separatorChar + configFileName);
    }

    // If the url doesn't start with "file:" or "classpath:", it's provided as a no path.
    // So we need to add it to make it a real URL.
    if (!urlString.startsWith("file:") && !urlString.startsWith("classpath:")) {
      urlString = "file:" + urlString;
    }
    try {
      return new URL(urlString);
    } catch (MalformedURLException e) {
      LOGGER.warn("get url failed", e);
      return null;
    }
  }

  /** load a property file and set TsfileDBConfig variables. */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private void loadProps() {
    Properties commonProperties = new Properties();
    // if new properties file exist, skip old properties files
    URL url = getPropsUrl(CommonConfig.SYSTEM_CONFIG_NAME);
    if (url != null) {
      try (InputStream inputStream = url.openStream()) {
        LOGGER.info("Start to read config file {}", url);
        Properties properties = new Properties();
        properties.load(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
        commonProperties.putAll(properties);
        loadProperties(commonProperties);
      } catch (FileNotFoundException e) {
        LOGGER.error("Fail to find config file {}, reject DataNode startup.", url, e);
        System.exit(-1);
      } catch (IOException e) {
        LOGGER.error("Cannot load config file, reject DataNode startup.", e);
        System.exit(-1);
      } catch (Exception e) {
        LOGGER.error("Incorrect format in config file, reject DataNode startup.", e);
        System.exit(-1);
      } finally {
        // update all data seriesPath
        conf.updatePath();
        commonDescriptor.getConfig().updatePath(System.getProperty(IoTDBConstant.IOTDB_HOME, null));
        MetricConfigDescriptor.getInstance().loadProps(commonProperties, false);
        MetricConfigDescriptor.getInstance()
            .getMetricConfig()
            .updateRpcInstance(NodeType.DATANODE, SchemaConstant.SYSTEM_DATABASE);
      }
    } else {
      LOGGER.warn(
          "Couldn't load the configuration {} from any of the known sources.",
          CommonConfig.SYSTEM_CONFIG_NAME);
    }
  }

  public void loadProperties(Properties properties) throws BadNodeUrlException, IOException {
    conf.setClusterName(
        properties.getProperty(IoTDBConstant.CLUSTER_NAME, conf.getClusterName()).trim());

    conf.setRpcAddress(
        properties.getProperty(IoTDBConstant.DN_RPC_ADDRESS, conf.getRpcAddress()).trim());

    conf.setRpcThriftCompressionEnable(
        Boolean.parseBoolean(
            properties
                .getProperty(
                    "dn_rpc_thrift_compression_enable",
                    Boolean.toString(conf.isRpcThriftCompressionEnable()))
                .trim()));

    conf.setRpcAdvancedCompressionEnable(
        Boolean.parseBoolean(
            properties
                .getProperty(
                    "dn_rpc_advanced_compression_enable",
                    Boolean.toString(conf.isRpcAdvancedCompressionEnable()))
                .trim()));

    conf.setConnectionTimeoutInMS(
        Integer.parseInt(
            properties
                .getProperty(
                    "dn_connection_timeout_ms", String.valueOf(conf.getConnectionTimeoutInMS()))
                .trim()));

    if (properties.getProperty("dn_max_connection_for_internal_service", null) != null) {
      conf.setMaxClientNumForEachNode(
          Integer.parseInt(
              properties.getProperty("dn_max_connection_for_internal_service").trim()));
      LOGGER.warn(
          "The parameter dn_max_connection_for_internal_service is out of date. Please rename it to dn_max_client_count_for_each_node_in_client_manager.");
    }
    conf.setMaxClientNumForEachNode(
        Integer.parseInt(
            properties
                .getProperty(
                    "dn_max_client_count_for_each_node_in_client_manager",
                    String.valueOf(conf.getMaxClientNumForEachNode()))
                .trim()));

    conf.setSelectorNumOfClientManager(
        Integer.parseInt(
            properties
                .getProperty(
                    "dn_selector_thread_count_of_client_manager",
                    String.valueOf(conf.getSelectorNumOfClientManager()))
                .trim()));

    conf.setRpcPort(
        Integer.parseInt(
            properties
                .getProperty(IoTDBConstant.DN_RPC_PORT, Integer.toString(conf.getRpcPort()))
                .trim()));

    conf.setEnableAINodeService(
        Boolean.parseBoolean(
            properties
                .getProperty(
                    "enable_ainode_rpc_service", Boolean.toString(conf.isEnableAINodeService()))
                .trim()));

    conf.setAINodePort(
        Integer.parseInt(
            properties
                .getProperty("ainode_rpc_port", Integer.toString(conf.getAINodePort()))
                .trim()));

    conf.setBufferedArraysMemoryProportion(
        Double.parseDouble(
            properties
                .getProperty(
                    "buffered_arrays_memory_proportion",
                    Double.toString(conf.getBufferedArraysMemoryProportion()))
                .trim()));

    conf.setFlushProportion(
        Double.parseDouble(
            properties
                .getProperty("flush_proportion", Double.toString(conf.getFlushProportion()))
                .trim()));

    final double rejectProportion =
        Double.parseDouble(
            properties
                .getProperty("reject_proportion", Double.toString(conf.getRejectProportion()))
                .trim());

    final double devicePathCacheProportion =
        Double.parseDouble(
            properties
                .getProperty(
                    "device_path_cache_proportion",
                    Double.toString(conf.getDevicePathCacheProportion()))
                .trim());

    if (rejectProportion + devicePathCacheProportion >= 1) {
      LOGGER.warn(
          "The sum of write_memory_proportion and device_path_cache_proportion is too large, use default values 0.8 and 0.05.");
    } else {
      conf.setRejectProportion(rejectProportion);
      conf.setDevicePathCacheProportion(devicePathCacheProportion);
    }

    conf.setWriteMemoryVariationReportProportion(
        Double.parseDouble(
            properties
                .getProperty(
                    "write_memory_variation_report_proportion",
                    Double.toString(conf.getWriteMemoryVariationReportProportion()))
                .trim()));

    conf.setMetaDataCacheEnable(
        Boolean.parseBoolean(
            properties
                .getProperty(
                    "meta_data_cache_enable", Boolean.toString(conf.isMetaDataCacheEnable()))
                .trim()));

    initMemoryAllocate(properties);

    String systemDir = properties.getProperty("dn_system_dir");
    if (systemDir == null) {
      systemDir = properties.getProperty("base_dir");
      if (systemDir != null) {
        systemDir = FilePathUtils.regularizePath(systemDir) + IoTDBConstant.SYSTEM_FOLDER_NAME;
      } else {
        systemDir = conf.getSystemDir();
      }
    }
    conf.setSystemDir(systemDir);

    conf.setSchemaDir(
        FilePathUtils.regularizePath(conf.getSystemDir()) + IoTDBConstant.SCHEMA_FOLDER_NAME);

    conf.setQueryDir(
        FilePathUtils.regularizePath(conf.getSystemDir() + IoTDBConstant.QUERY_FOLDER_NAME));
    String[] defaultTierDirs = new String[conf.getTierDataDirs().length];
    for (int i = 0; i < defaultTierDirs.length; ++i) {
      defaultTierDirs[i] = String.join(",", conf.getTierDataDirs()[i]);
    }
    conf.setTierDataDirs(
        parseDataDirs(
            properties.getProperty(
                "dn_data_dirs", String.join(IoTDBConstant.TIER_SEPARATOR, defaultTierDirs))));

    conf.setConsensusDir(properties.getProperty("dn_consensus_dir", conf.getConsensusDir()));

    long forceMlogPeriodInMs =
        Long.parseLong(
            properties.getProperty(
                "sync_mlog_period_in_ms", Long.toString(conf.getSyncMlogPeriodInMs())));
    if (forceMlogPeriodInMs > 0) {
      conf.setSyncMlogPeriodInMs(forceMlogPeriodInMs);
    }

    String oldMultiDirStrategyClassName = conf.getMultiDirStrategyClassName();
    conf.setMultiDirStrategyClassName(
        properties.getProperty("dn_multi_dir_strategy", conf.getMultiDirStrategyClassName()));
    try {
      conf.checkMultiDirStrategyClassName();
    } catch (Exception e) {
      conf.setMultiDirStrategyClassName(oldMultiDirStrategyClassName);
      throw e;
    }

    conf.setBatchSize(
        Integer.parseInt(
            properties.getProperty("batch_size", Integer.toString(conf.getBatchSize()))));

    conf.setTvListSortAlgorithm(
        TVListSortAlgorithm.valueOf(
            properties.getProperty(
                "tvlist_sort_algorithm", conf.getTvListSortAlgorithm().toString())));

    conf.setAvgSeriesPointNumberThreshold(
        Integer.parseInt(
            properties.getProperty(
                "avg_series_point_number_threshold",
                Integer.toString(conf.getAvgSeriesPointNumberThreshold()))));

    conf.setCheckPeriodWhenInsertBlocked(
        Integer.parseInt(
            properties.getProperty(
                "check_period_when_insert_blocked",
                Integer.toString(conf.getCheckPeriodWhenInsertBlocked()))));

    conf.setMaxWaitingTimeWhenInsertBlocked(
        Integer.parseInt(
            properties.getProperty(
                "max_waiting_time_when_insert_blocked",
                Integer.toString(conf.getMaxWaitingTimeWhenInsertBlocked()))));

    String offHeapMemoryStr = System.getProperty("OFF_HEAP_MEMORY");
    conf.setMaxOffHeapMemoryBytes(MemUtils.strToBytesCnt(offHeapMemoryStr));

    conf.setIoTaskQueueSizeForFlushing(
        Integer.parseInt(
            properties.getProperty(
                "io_task_queue_size_for_flushing",
                Integer.toString(conf.getIoTaskQueueSizeForFlushing()))));
    boolean enableWALCompression =
        Boolean.parseBoolean(properties.getProperty("enable_wal_compression", "true"));
    conf.setWALCompressionAlgorithm(
        enableWALCompression ? CompressionType.LZ4 : CompressionType.UNCOMPRESSED);

    conf.setCompactionScheduleIntervalInMs(
        Long.parseLong(
            properties.getProperty(
                "compaction_schedule_interval_in_ms",
                Long.toString(conf.getCompactionScheduleIntervalInMs()))));

    conf.setEnableCrossSpaceCompaction(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_cross_space_compaction",
                Boolean.toString(conf.isEnableCrossSpaceCompaction()))));

    conf.setEnableSeqSpaceCompaction(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_seq_space_compaction",
                Boolean.toString(conf.isEnableSeqSpaceCompaction()))));

    conf.setEnableUnseqSpaceCompaction(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_unseq_space_compaction",
                Boolean.toString(conf.isEnableUnseqSpaceCompaction()))));

    conf.setCrossCompactionSelector(
        CrossCompactionSelector.getCrossCompactionSelector(
            properties.getProperty(
                "cross_selector", conf.getCrossCompactionSelector().toString())));

    conf.setInnerSequenceCompactionSelector(
        InnerSequenceCompactionSelector.getInnerSequenceCompactionSelector(
            properties.getProperty(
                "inner_seq_selector", conf.getInnerSequenceCompactionSelector().toString())));

    conf.setInnerUnsequenceCompactionSelector(
        InnerUnsequenceCompactionSelector.getInnerUnsequenceCompactionSelector(
            properties.getProperty(
                "inner_unseq_selector", conf.getInnerUnsequenceCompactionSelector().toString())));

    conf.setInnerSeqCompactionPerformer(
        InnerSeqCompactionPerformer.getInnerSeqCompactionPerformer(
            properties.getProperty(
                "inner_seq_performer", conf.getInnerSeqCompactionPerformer().toString())));

    conf.setInnerUnseqCompactionPerformer(
        InnerUnseqCompactionPerformer.getInnerUnseqCompactionPerformer(
            properties.getProperty(
                "inner_unseq_performer", conf.getInnerUnseqCompactionPerformer().toString())));

    conf.setCrossCompactionPerformer(
        CrossCompactionPerformer.getCrossCompactionPerformer(
            properties.getProperty(
                "cross_performer", conf.getCrossCompactionPerformer().toString())));

    conf.setCompactionPriority(
        CompactionPriority.valueOf(
            properties.getProperty(
                "compaction_priority", conf.getCompactionPriority().toString())));

    int subtaskNum =
        Integer.parseInt(
            properties.getProperty(
                "sub_compaction_thread_count", Integer.toString(conf.getSubCompactionTaskNum())));
    subtaskNum = subtaskNum <= 0 ? 1 : subtaskNum;
    conf.setSubCompactionTaskNum(subtaskNum);

    int compactionScheduleThreadNum =
        Integer.parseInt(
            properties.getProperty(
                "compaction_schedule_thread_num",
                Integer.toString(conf.getCompactionScheduleThreadNum())));
    compactionScheduleThreadNum =
        compactionScheduleThreadNum <= 0 ? 1 : compactionScheduleThreadNum;
    conf.setCompactionScheduleThreadNum(compactionScheduleThreadNum);

    conf.setQueryTimeoutThreshold(
        Long.parseLong(
            properties.getProperty(
                "query_timeout_threshold", Long.toString(conf.getQueryTimeoutThreshold()))));

    conf.setSessionTimeoutThreshold(
        Integer.parseInt(
            properties.getProperty(
                "dn_session_timeout_threshold",
                Integer.toString(conf.getSessionTimeoutThreshold()))));

    conf.setFlushThreadCount(
        Integer.parseInt(
            properties.getProperty(
                "flush_thread_count", Integer.toString(conf.getFlushThreadCount()))));

    if (conf.getFlushThreadCount() <= 0) {
      conf.setFlushThreadCount(Runtime.getRuntime().availableProcessors());
    }

    // start: index parameter setting
    conf.setIndexRootFolder(properties.getProperty("index_root_dir", conf.getIndexRootFolder()));

    conf.setEnableIndex(
        Boolean.parseBoolean(
            properties.getProperty("enable_index", Boolean.toString(conf.isEnableIndex()))));

    conf.setConcurrentIndexBuildThread(
        Integer.parseInt(
            properties.getProperty(
                "concurrent_index_build_thread",
                Integer.toString(conf.getConcurrentIndexBuildThread()))));
    if (conf.getConcurrentIndexBuildThread() <= 0) {
      conf.setConcurrentIndexBuildThread(Runtime.getRuntime().availableProcessors());
    }

    conf.setDefaultIndexWindowRange(
        Integer.parseInt(
            properties.getProperty(
                "default_index_window_range",
                Integer.toString(conf.getDefaultIndexWindowRange()))));

    conf.setQueryThreadCount(
        Integer.parseInt(
            properties.getProperty(
                "query_thread_count", Integer.toString(conf.getQueryThreadCount()))));

    if (conf.getQueryThreadCount() <= 0) {
      conf.setQueryThreadCount(Runtime.getRuntime().availableProcessors());
    }

    conf.setDegreeOfParallelism(
        Integer.parseInt(
            properties.getProperty(
                "degree_of_query_parallelism", Integer.toString(conf.getDegreeOfParallelism()))));

    if (conf.getDegreeOfParallelism() <= 0) {
      conf.setDegreeOfParallelism(Runtime.getRuntime().availableProcessors() / 2);
    }

    conf.setMergeThresholdOfExplainAnalyze(
        Integer.parseInt(
            properties.getProperty(
                "merge_threshold_of_explain_analyze",
                Integer.toString(conf.getMergeThresholdOfExplainAnalyze()))));

    conf.setModeMapSizeThreshold(
        Integer.parseInt(
            properties.getProperty(
                "mode_map_size_threshold", Integer.toString(conf.getModeMapSizeThreshold()))));

    if (conf.getModeMapSizeThreshold() <= 0) {
      conf.setModeMapSizeThreshold(10000);
    }

    conf.setMaxAllowedConcurrentQueries(
        Integer.parseInt(
            properties.getProperty(
                "max_allowed_concurrent_queries",
                Integer.toString(conf.getMaxAllowedConcurrentQueries()))));

    if (conf.getMaxAllowedConcurrentQueries() <= 0) {
      conf.setMaxAllowedConcurrentQueries(1000);
    }

    conf.setmRemoteSchemaCacheSize(
        Integer.parseInt(
            properties
                .getProperty(
                    "remote_schema_cache_size", Integer.toString(conf.getmRemoteSchemaCacheSize()))
                .trim()));

    conf.setLanguageVersion(
        properties.getProperty("language_version", conf.getLanguageVersion()).trim());

    if (properties.containsKey("chunk_buffer_pool_enable")) {
      conf.setChunkBufferPoolEnable(
          Boolean.parseBoolean(properties.getProperty("chunk_buffer_pool_enable")));
    }
    conf.setMergeIntervalSec(
        Long.parseLong(
            properties.getProperty(
                "merge_interval_sec", Long.toString(conf.getMergeIntervalSec()))));
    conf.setCompactionThreadCount(
        Integer.parseInt(
            properties.getProperty(
                "compaction_thread_count", Integer.toString(conf.getCompactionThreadCount()))));
    int maxConcurrentAlignedSeriesInCompaction =
        Integer.parseInt(
            properties.getProperty(
                "compaction_max_aligned_series_num_in_one_batch",
                Integer.toString(conf.getCompactionMaxAlignedSeriesNumInOneBatch())));
    conf.setCompactionMaxAlignedSeriesNumInOneBatch(
        maxConcurrentAlignedSeriesInCompaction <= 0
            ? Integer.MAX_VALUE
            : maxConcurrentAlignedSeriesInCompaction);
    conf.setChunkMetadataSizeProportion(
        Double.parseDouble(
            properties.getProperty(
                "chunk_metadata_size_proportion",
                Double.toString(conf.getChunkMetadataSizeProportion()))));
    conf.setTargetCompactionFileSize(
        Long.parseLong(
            properties.getProperty(
                "target_compaction_file_size", Long.toString(conf.getTargetCompactionFileSize()))));
    conf.setInnerCompactionTotalFileSizeThresholdInByte(
        Long.parseLong(
            properties.getProperty(
                "inner_compaction_total_file_size_threshold",
                Long.toString(conf.getInnerCompactionTotalFileSizeThresholdInByte()))));
    conf.setInnerCompactionTotalFileNumThreshold(
        Integer.parseInt(
            properties.getProperty(
                "inner_compaction_total_file_num_threshold",
                Integer.toString(conf.getInnerCompactionTotalFileNumThreshold()))));
    conf.setMaxLevelGapInInnerCompaction(
        Integer.parseInt(
            properties.getProperty(
                "max_level_gap_in_inner_compaction",
                Integer.toString(conf.getMaxLevelGapInInnerCompaction()))));

    conf.setTargetChunkSize(
        Long.parseLong(
            properties.getProperty("target_chunk_size", Long.toString(conf.getTargetChunkSize()))));
    conf.setTargetChunkPointNum(
        Long.parseLong(
            properties.getProperty(
                "target_chunk_point_num", Long.toString(conf.getTargetChunkPointNum()))));
    conf.setChunkPointNumLowerBoundInCompaction(
        Long.parseLong(
            properties.getProperty(
                "chunk_point_num_lower_bound_in_compaction",
                Long.toString(conf.getChunkPointNumLowerBoundInCompaction()))));
    conf.setChunkSizeLowerBoundInCompaction(
        Long.parseLong(
            properties.getProperty(
                "chunk_size_lower_bound_in_compaction",
                Long.toString(conf.getChunkSizeLowerBoundInCompaction()))));
    conf.setInnerCompactionCandidateFileNum(
        Integer.parseInt(
            properties.getProperty(
                "inner_compaction_candidate_file_num",
                Integer.toString(conf.getInnerCompactionCandidateFileNum()))));
    conf.setFileLimitPerCrossTask(
        Integer.parseInt(
            properties.getProperty(
                "max_cross_compaction_candidate_file_num",
                Integer.toString(conf.getFileLimitPerCrossTask()))));
    conf.setMaxCrossCompactionCandidateFileSize(
        Long.parseLong(
            properties.getProperty(
                "max_cross_compaction_candidate_file_size",
                Long.toString(conf.getMaxCrossCompactionCandidateFileSize()))));
    conf.setMinCrossCompactionUnseqFileLevel(
        Integer.parseInt(
            properties.getProperty(
                "min_cross_compaction_unseq_file_level",
                Integer.toString(conf.getMinCrossCompactionUnseqFileLevel()))));

    conf.setCompactionWriteThroughputMbPerSec(
        Integer.parseInt(
            properties.getProperty(
                "compaction_write_throughput_mb_per_sec",
                Integer.toString(conf.getCompactionWriteThroughputMbPerSec()))));

    conf.setCompactionReadThroughputMbPerSec(
        Integer.parseInt(
            properties.getProperty(
                "compaction_read_throughput_mb_per_sec",
                Integer.toString(conf.getCompactionReadThroughputMbPerSec()))));

    conf.setCompactionReadOperationPerSec(
        Integer.parseInt(
            properties.getProperty(
                "compaction_read_operation_per_sec",
                Integer.toString(conf.getCompactionReadOperationPerSec()))));

    conf.setEnableTsFileValidation(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_tsfile_validation", String.valueOf(conf.isEnableTsFileValidation()))));
    conf.setCandidateCompactionTaskQueueSize(
        Integer.parseInt(
            properties.getProperty(
                "candidate_compaction_task_queue_size",
                Integer.toString(conf.getCandidateCompactionTaskQueueSize()))));

    conf.setInnerCompactionTaskSelectionDiskRedundancy(
        Double.parseDouble(
            properties.getProperty(
                "inner_compaction_task_selection_disk_redundancy",
                Double.toString(conf.getInnerCompactionTaskSelectionDiskRedundancy()))));

    conf.setInnerCompactionTaskSelectionModsFileThreshold(
        Long.parseLong(
            properties.getProperty(
                "inner_compaction_task_selection_mods_file_threshold",
                Long.toString(conf.getInnerCompactionTaskSelectionModsFileThreshold()))));

    conf.setTtlCheckInterval(
        Long.parseLong(
            properties.getProperty(
                "ttl_check_interval", Long.toString(conf.getTTlCheckInterval()))));

    conf.setMaxExpiredTime(
        Long.parseLong(
            properties.getProperty("max_expired_time", Long.toString(conf.getMaxExpiredTime()))));

    conf.setExpiredDataRatio(
        Float.parseFloat(
            properties.getProperty(
                "expired_data_ratio", Float.toString(conf.getExpiredDataRatio()))));

    conf.setEnablePartialInsert(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_partial_insert", String.valueOf(conf.isEnablePartialInsert()))));

    conf.setEnable13DataInsertAdapt(
        Boolean.parseBoolean(
            properties.getProperty(
                "0.13_data_insert_adapt", String.valueOf(conf.isEnable13DataInsertAdapt()))));

    int rpcSelectorThreadNum =
        Integer.parseInt(
            properties.getProperty(
                "dn_rpc_selector_thread_count",
                Integer.toString(conf.getRpcSelectorThreadCount()).trim()));
    if (rpcSelectorThreadNum <= 0) {
      rpcSelectorThreadNum = 1;
    }

    conf.setRpcSelectorThreadCount(rpcSelectorThreadNum);

    int minConcurrentClientNum =
        Integer.parseInt(
            properties.getProperty(
                "dn_rpc_min_concurrent_client_num",
                Integer.toString(conf.getRpcMinConcurrentClientNum()).trim()));
    if (minConcurrentClientNum <= 0) {
      minConcurrentClientNum = Runtime.getRuntime().availableProcessors();
    }

    conf.setRpcMinConcurrentClientNum(minConcurrentClientNum);

    int maxConcurrentClientNum =
        Integer.parseInt(
            properties.getProperty(
                "dn_rpc_max_concurrent_client_num",
                Integer.toString(conf.getRpcMaxConcurrentClientNum()).trim()));
    if (maxConcurrentClientNum <= 0) {
      maxConcurrentClientNum = 65535;
    }

    conf.setRpcMaxConcurrentClientNum(maxConcurrentClientNum);

    loadAutoCreateSchemaProps(properties);

    conf.setTsFileStorageFs(
        properties.getProperty("tsfile_storage_fs", conf.getTsFileStorageFs().toString()));
    conf.setEnableHDFS(
        Boolean.parseBoolean(
            properties.getProperty("enable_hdfs", String.valueOf(conf.isEnableHDFS()))));
    conf.setCoreSitePath(properties.getProperty("core_site_path", conf.getCoreSitePath()));
    conf.setHdfsSitePath(properties.getProperty("hdfs_site_path", conf.getHdfsSitePath()));
    conf.setHdfsIp(properties.getProperty("hdfs_ip", conf.getRawHDFSIp()).split(","));
    conf.setHdfsPort(properties.getProperty("hdfs_port", conf.getHdfsPort()));
    conf.setDfsNameServices(properties.getProperty("dfs_nameservices", conf.getDfsNameServices()));
    conf.setDfsHaNamenodes(
        properties.getProperty("dfs_ha_namenodes", conf.getRawDfsHaNamenodes()).split(","));
    conf.setDfsHaAutomaticFailoverEnabled(
        Boolean.parseBoolean(
            properties.getProperty(
                "dfs_ha_automatic_failover_enabled",
                String.valueOf(conf.isDfsHaAutomaticFailoverEnabled()))));
    conf.setDfsClientFailoverProxyProvider(
        properties.getProperty(
            "dfs_client_failover_proxy_provider", conf.getDfsClientFailoverProxyProvider()));
    conf.setUseKerberos(
        Boolean.parseBoolean(
            properties.getProperty("hdfs_use_kerberos", String.valueOf(conf.isUseKerberos()))));
    conf.setKerberosKeytabFilePath(
        properties.getProperty("kerberos_keytab_file_path", conf.getKerberosKeytabFilePath()));
    conf.setKerberosPrincipal(
        properties.getProperty("kerberos_principal", conf.getKerberosPrincipal()));

    // The default fill interval in LinearFill and PreviousFill
    conf.setDefaultFillInterval(
        Integer.parseInt(
            properties.getProperty(
                "default_fill_interval", String.valueOf(conf.getDefaultFillInterval()))));

    conf.setTagAttributeFlushInterval(
        Integer.parseInt(
            properties.getProperty(
                "tag_attribute_flush_interval",
                String.valueOf(conf.getTagAttributeFlushInterval()))));

    conf.setPrimitiveArraySize(
        (Integer.parseInt(
            properties.getProperty(
                "primitive_array_size", String.valueOf(conf.getPrimitiveArraySize())))));

    conf.setThriftMaxFrameSize(
        Integer.parseInt(
            properties.getProperty(
                "dn_thrift_max_frame_size", String.valueOf(conf.getThriftMaxFrameSize()))));

    if (conf.getThriftMaxFrameSize() < IoTDBConstant.LEFT_SIZE_IN_REQUEST * 2) {
      conf.setThriftMaxFrameSize(IoTDBConstant.LEFT_SIZE_IN_REQUEST * 2);
    }

    conf.setThriftDefaultBufferSize(
        Integer.parseInt(
            properties.getProperty(
                "dn_thrift_init_buffer_size", String.valueOf(conf.getThriftDefaultBufferSize()))));

    conf.setSlowQueryThreshold(
        Long.parseLong(
            properties.getProperty(
                "slow_query_threshold", String.valueOf(conf.getSlowQueryThreshold()))));

    conf.setDataRegionNum(
        Integer.parseInt(
            properties.getProperty("data_region_num", String.valueOf(conf.getDataRegionNum()))));

    conf.setRecoveryLogIntervalInMs(
        Long.parseLong(
            properties.getProperty(
                "recovery_log_interval_in_ms", String.valueOf(conf.getRecoveryLogIntervalInMs()))));

    conf.setEnableSeparateData(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_separate_data", Boolean.toString(conf.isEnableSeparateData()))));

    conf.setWindowEvaluationThreadCount(
        Integer.parseInt(
            properties.getProperty(
                "window_evaluation_thread_count",
                Integer.toString(conf.getWindowEvaluationThreadCount()))));
    if (conf.getWindowEvaluationThreadCount() <= 0) {
      conf.setWindowEvaluationThreadCount(Runtime.getRuntime().availableProcessors());
    }

    conf.setMaxPendingWindowEvaluationTasks(
        Integer.parseInt(
            properties.getProperty(
                "max_pending_window_evaluation_tasks",
                Integer.toString(conf.getMaxPendingWindowEvaluationTasks()))));
    if (conf.getMaxPendingWindowEvaluationTasks() <= 0) {
      conf.setMaxPendingWindowEvaluationTasks(64);
    }

    conf.setCachedMNodeSizeInPBTreeMode(
        Integer.parseInt(
            properties.getProperty(
                "cached_mnode_size_in_pbtree_mode",
                String.valueOf(conf.getCachedMNodeSizeInPBTreeMode()))));

    conf.setMinimumSegmentInPBTree(
        Short.parseShort(
            properties.getProperty(
                "minimum_pbtree_segment_in_bytes",
                String.valueOf(conf.getMinimumSegmentInPBTree()))));

    conf.setPageCacheSizeInPBTree(
        Integer.parseInt(
            properties.getProperty(
                "page_cache_in_pbtree", String.valueOf(conf.getPageCacheSizeInPBTree()))));

    conf.setPBTreeLogSize(
        Integer.parseInt(
            properties.getProperty("pbtree_log_size", String.valueOf(conf.getPBTreeLogSize()))));

    conf.setMaxMeasurementNumOfInternalRequest(
        Integer.parseInt(
            properties.getProperty(
                "max_measurement_num_of_internal_request",
                String.valueOf(conf.getMaxMeasurementNumOfInternalRequest()))));

    // mqtt
    loadMqttProps(properties);

    conf.setIntoOperationBufferSizeInByte(
        Long.parseLong(
            properties.getProperty(
                "into_operation_buffer_size_in_byte",
                String.valueOf(conf.getIntoOperationBufferSizeInByte()))));
    conf.setSelectIntoInsertTabletPlanRowLimit(
        Integer.parseInt(
            properties.getProperty(
                "select_into_insert_tablet_plan_row_limit",
                String.valueOf(conf.getSelectIntoInsertTabletPlanRowLimit()))));
    conf.setIntoOperationExecutionThreadCount(
        Integer.parseInt(
            properties.getProperty(
                "into_operation_execution_thread_count",
                String.valueOf(conf.getIntoOperationExecutionThreadCount()))));
    if (conf.getIntoOperationExecutionThreadCount() <= 0) {
      conf.setIntoOperationExecutionThreadCount(2);
    }

    conf.setExtPipeDir(properties.getProperty("ext_pipe_dir", conf.getExtPipeDir()).trim());

    // At the same time, set TSFileConfig
    List<FSType> fsTypes = new ArrayList<>();
    fsTypes.add(FSType.LOCAL);
    if (Boolean.parseBoolean(
        properties.getProperty("enable_hdfs", String.valueOf(conf.isEnableHDFS())))) {
      fsTypes.add(FSType.HDFS);
    }
    TSFileDescriptor.getInstance().getConfig().setTSFileStorageFs(fsTypes.toArray(new FSType[0]));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setCoreSitePath(properties.getProperty("core_site_path", conf.getCoreSitePath()));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setHdfsSitePath(properties.getProperty("hdfs_site_path", conf.getHdfsSitePath()));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setHdfsIp(properties.getProperty("hdfs_ip", conf.getRawHDFSIp()).split(","));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setHdfsPort(properties.getProperty("hdfs_port", conf.getHdfsPort()));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setDfsNameServices(properties.getProperty("dfs_nameservices", conf.getDfsNameServices()));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setDfsHaNamenodes(
            properties.getProperty("dfs_ha_namenodes", conf.getRawDfsHaNamenodes()).split(","));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setDfsHaAutomaticFailoverEnabled(
            Boolean.parseBoolean(
                properties.getProperty(
                    "dfs_ha_automatic_failover_enabled",
                    String.valueOf(conf.isDfsHaAutomaticFailoverEnabled()))));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setDfsClientFailoverProxyProvider(
            properties.getProperty(
                "dfs_client_failover_proxy_provider", conf.getDfsClientFailoverProxyProvider()));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setPatternMatchingThreshold(
            Integer.parseInt(
                properties.getProperty(
                    "pattern_matching_threshold",
                    String.valueOf(conf.getPatternMatchingThreshold()))));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setUseKerberos(
            Boolean.parseBoolean(
                properties.getProperty("hdfs_use_kerberos", String.valueOf(conf.isUseKerberos()))));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setKerberosKeytabFilePath(
            properties.getProperty("kerberos_keytab_file_path", conf.getKerberosKeytabFilePath()));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setKerberosPrincipal(
            properties.getProperty("kerberos_principal", conf.getKerberosPrincipal()));
    TSFileDescriptor.getInstance().getConfig().setBatchSize(conf.getBatchSize());

    conf.setCoordinatorReadExecutorSize(
        Integer.parseInt(
            properties.getProperty(
                "coordinator_read_executor_size",
                Integer.toString(conf.getCoordinatorReadExecutorSize()))));
    conf.setCoordinatorWriteExecutorSize(
        Integer.parseInt(
            properties.getProperty(
                "coordinator_write_executor_size",
                Integer.toString(conf.getCoordinatorWriteExecutorSize()))));

    // Commons
    commonDescriptor.loadCommonProps(properties);
    commonDescriptor.initCommonConfigDir(conf.getSystemDir());

    loadWALProps(properties);

    // Timed flush memtable
    loadTimedService(properties);

    // Set tsfile-format config
    loadTsFileProps(properties);

    // Make RPCTransportFactory taking effect.
    ZeroCopyRpcTransportFactory.reInit();
    DeepCopyRpcTransportFactory.reInit();

    // UDF
    loadUDFProps(properties);

    // Thrift ssl
    initThriftSSL(properties);

    // Trigger
    loadTriggerProps(properties);

    // CQ
    loadCQProps(properties);

    // Load TsFile
    loadLoadTsFileProps(properties);

    // Pipe
    loadPipeProps(properties);

    // Cluster
    loadClusterProps(properties);

    // Shuffle
    loadShuffleProps(properties);

    // Author cache
    loadAuthorCache(properties);

    conf.setQuotaEnable(
        Boolean.parseBoolean(
            properties.getProperty("quota_enable", String.valueOf(conf.isQuotaEnable()))));

    // The buffer for sort operator to calculate
    conf.setSortBufferSize(
        Long.parseLong(
            properties
                .getProperty("sort_buffer_size_in_bytes", Long.toString(conf.getSortBufferSize()))
                .trim()));

    // tmp filePath for sort operator
    conf.setSortTmpDir(properties.getProperty("sort_tmp_dir", conf.getSortTmpDir()));

    conf.setRateLimiterType(properties.getProperty("rate_limiter_type", conf.getRateLimiterType()));

    conf.setDataNodeSchemaCacheEvictionPolicy(
        properties.getProperty(
            "datanode_schema_cache_eviction_policy", conf.getDataNodeSchemaCacheEvictionPolicy()));

    loadIoTConsensusProps(properties);
    loadPipeConsensusProps(properties);
  }

  private void reloadConsensusProps(Properties properties) throws IOException {
    loadIoTConsensusProps(properties);
    loadPipeConsensusProps(properties);
    DataRegionConsensusImpl.reloadConsensusConfig();
  }

  private void loadIoTConsensusProps(Properties properties) throws IOException {
    conf.setMaxLogEntriesNumPerBatch(
        Integer.parseInt(
            properties
                .getProperty(
                    "data_region_iot_max_log_entries_num_per_batch",
                    ConfigurationFileUtils.getConfigurationDefaultValue(
                        "data_region_iot_max_log_entries_num_per_batch"))
                .trim()));
    conf.setMaxSizePerBatch(
        Integer.parseInt(
            properties
                .getProperty(
                    "data_region_iot_max_size_per_batch",
                    ConfigurationFileUtils.getConfigurationDefaultValue(
                        "data_region_iot_max_size_per_batch"))
                .trim()));
    conf.setMaxPendingBatchesNum(
        Integer.parseInt(
            properties
                .getProperty(
                    "data_region_iot_max_pending_batches_num",
                    ConfigurationFileUtils.getConfigurationDefaultValue(
                        "data_region_iot_max_pending_batches_num"))
                .trim()));
    conf.setMaxMemoryRatioForQueue(
        Double.parseDouble(
            properties
                .getProperty(
                    "data_region_iot_max_memory_ratio_for_queue",
                    ConfigurationFileUtils.getConfigurationDefaultValue(
                        "data_region_iot_max_memory_ratio_for_queue"))
                .trim()));
    conf.setRegionMigrationSpeedLimitBytesPerSecond(
        Long.parseLong(
            properties
                .getProperty(
                    "region_migration_speed_limit_bytes_per_second",
                    ConfigurationFileUtils.getConfigurationDefaultValue(
                        "region_migration_speed_limit_bytes_per_second"))
                .trim()));
  }

  private void loadPipeConsensusProps(Properties properties) throws IOException {
    conf.setPipeConsensusPipelineSize(
        Integer.parseInt(
            properties.getProperty(
                "fast_iot_consensus_pipeline_size",
                ConfigurationFileUtils.getConfigurationDefaultValue(
                    "fast_iot_consensus_pipeline_size"))));
    if (conf.getPipeConsensusPipelineSize() <= 0) {
      conf.setPipeConsensusPipelineSize(5);
    }
  }

  private void loadAuthorCache(Properties properties) {
    conf.setAuthorCacheSize(
        Integer.parseInt(
            properties.getProperty(
                "author_cache_size", String.valueOf(conf.getAuthorCacheSize()))));
    conf.setAuthorCacheExpireTime(
        Integer.parseInt(
            properties.getProperty(
                "author_cache_expire_time", String.valueOf(conf.getAuthorCacheExpireTime()))));
  }

  private void loadWALProps(Properties properties) throws IOException {
    conf.setWalMode(
        WALMode.valueOf((properties.getProperty("wal_mode", conf.getWalMode().toString()))));

    int maxWalNodesNum =
        Integer.parseInt(
            properties.getProperty(
                "max_wal_nodes_num", Integer.toString(conf.getMaxWalNodesNum())));
    if (maxWalNodesNum > 0) {
      conf.setMaxWalNodesNum(maxWalNodesNum);
    }

    int walBufferSize =
        Integer.parseInt(
            properties.getProperty(
                "wal_buffer_size_in_byte", Integer.toString(conf.getWalBufferSize())));
    if (walBufferSize > 0) {
      conf.setWalBufferSize(walBufferSize);
    }

    int walBufferQueueCapacity =
        Integer.parseInt(
            properties.getProperty(
                "wal_buffer_queue_capacity", Integer.toString(conf.getWalBufferQueueCapacity())));
    if (walBufferQueueCapacity > 0) {
      conf.setWalBufferQueueCapacity(walBufferQueueCapacity);
    }

    boolean WALInsertNodeCacheShrinkClearEnabled =
        Boolean.parseBoolean(
            properties.getProperty(
                "wal_cache_shrink_clear_enabled",
                Boolean.toString(conf.getWALCacheShrinkClearEnabled())));
    if (conf.getWALCacheShrinkClearEnabled() != WALInsertNodeCacheShrinkClearEnabled) {
      conf.setWALCacheShrinkClearEnabled(WALInsertNodeCacheShrinkClearEnabled);
    }

    loadWALHotModifiedProps(properties);
  }

  private void loadCompactionHotModifiedProps(Properties properties)
      throws InterruptedException, IOException {
    boolean compactionTaskConfigHotModified = loadCompactionTaskHotModifiedProps(properties);
    if (compactionTaskConfigHotModified) {
      CompactionTaskManager.getInstance().incrCompactionConfigVersion();
    }
    // hot load compaction schedule task manager configurations
    int compactionScheduleThreadNum =
        Integer.parseInt(
            properties.getProperty(
                "compaction_schedule_thread_num",
                ConfigurationFileUtils.getConfigurationDefaultValue(
                    "compaction_schedule_thread_num")));
    compactionScheduleThreadNum =
        compactionScheduleThreadNum <= 0 ? 1 : compactionScheduleThreadNum;
    conf.setCompactionScheduleThreadNum(compactionScheduleThreadNum);

    CompactionScheduleTaskManager.getInstance().checkAndMayApplyConfigurationChange();
    // hot load compaction task manager configurations
    loadCompactionIsEnabledHotModifiedProps(properties);
    boolean restartCompactionTaskManager = loadCompactionThreadCountHotModifiedProps(properties);
    restartCompactionTaskManager |= loadCompactionSubTaskCountHotModifiedProps(properties);
    if (restartCompactionTaskManager) {
      CompactionTaskManager.getInstance().restart();
    }

    // hot load compaction rate limit configurations
    CompactionTaskManager.getInstance()
        .setCompactionReadOperationRate(conf.getCompactionReadOperationPerSec());
    CompactionTaskManager.getInstance()
        .setCompactionReadThroughputRate(conf.getCompactionReadThroughputMbPerSec());
    CompactionTaskManager.getInstance()
        .setWriteMergeRate(conf.getCompactionWriteThroughputMbPerSec());
  }

  private boolean loadCompactionTaskHotModifiedProps(Properties properties) throws IOException {
    boolean configModified = false;
    // update merge_write_throughput_mb_per_sec
    int compactionWriteThroughput = conf.getCompactionWriteThroughputMbPerSec();
    conf.setCompactionWriteThroughputMbPerSec(
        Integer.parseInt(
            properties.getProperty(
                "compaction_write_throughput_mb_per_sec",
                ConfigurationFileUtils.getConfigurationDefaultValue(
                    "compaction_write_throughput_mb_per_sec"))));
    configModified |= compactionWriteThroughput != conf.getCompactionWriteThroughputMbPerSec();

    // update compaction_read_operation_per_sec
    int compactionReadOperation = conf.getCompactionReadOperationPerSec();
    conf.setCompactionReadOperationPerSec(
        Integer.parseInt(
            properties.getProperty(
                "compaction_read_operation_per_sec",
                ConfigurationFileUtils.getConfigurationDefaultValue(
                    "compaction_read_operation_per_sec"))));
    configModified |= compactionReadOperation != conf.getCompactionReadOperationPerSec();

    // update compaction_read_throughput_mb_per_sec
    int compactionReadThroughput = conf.getCompactionReadThroughputMbPerSec();
    conf.setCompactionReadThroughputMbPerSec(
        Integer.parseInt(
            properties.getProperty(
                "compaction_read_throughput_mb_per_sec",
                ConfigurationFileUtils.getConfigurationDefaultValue(
                    "compaction_read_throughput_mb_per_sec"))));
    configModified |= compactionReadThroughput != conf.getCompactionReadThroughputMbPerSec();

    // update inner_compaction_candidate_file_num
    int maxInnerCompactionCandidateFileNum = conf.getInnerCompactionCandidateFileNum();
    conf.setInnerCompactionCandidateFileNum(
        Integer.parseInt(
            properties.getProperty(
                "inner_compaction_candidate_file_num",
                ConfigurationFileUtils.getConfigurationDefaultValue(
                    "inner_compaction_candidate_file_num"))));
    configModified |=
        maxInnerCompactionCandidateFileNum != conf.getInnerCompactionCandidateFileNum();

    // update target_compaction_file_size
    long targetCompactionFilesize = conf.getTargetCompactionFileSize();
    conf.setTargetCompactionFileSize(
        Long.parseLong(
            properties.getProperty(
                "target_compaction_file_size",
                ConfigurationFileUtils.getConfigurationDefaultValue(
                    "target_compaction_file_size"))));
    configModified |= targetCompactionFilesize != conf.getTargetCompactionFileSize();

    // update max_cross_compaction_candidate_file_num
    int maxCrossCompactionCandidateFileNum = conf.getFileLimitPerCrossTask();
    conf.setFileLimitPerCrossTask(
        Integer.parseInt(
            properties.getProperty(
                "max_cross_compaction_candidate_file_num",
                ConfigurationFileUtils.getConfigurationDefaultValue(
                    "max_cross_compaction_candidate_file_num"))));
    configModified |= maxCrossCompactionCandidateFileNum != conf.getFileLimitPerCrossTask();

    // update max_cross_compaction_candidate_file_size
    long maxCrossCompactionCandidateFileSize = conf.getMaxCrossCompactionCandidateFileSize();
    conf.setMaxCrossCompactionCandidateFileSize(
        Long.parseLong(
            properties.getProperty(
                "max_cross_compaction_candidate_file_size",
                ConfigurationFileUtils.getConfigurationDefaultValue(
                    "max_cross_compaction_candidate_file_size"))));
    configModified |=
        maxCrossCompactionCandidateFileSize != conf.getMaxCrossCompactionCandidateFileSize();

    // update min_cross_compaction_unseq_file_level
    int minCrossCompactionCandidateFileNum = conf.getMinCrossCompactionUnseqFileLevel();
    conf.setMinCrossCompactionUnseqFileLevel(
        Integer.parseInt(
            properties.getProperty(
                "min_cross_compaction_unseq_file_level",
                ConfigurationFileUtils.getConfigurationDefaultValue(
                    "min_cross_compaction_unseq_file_level"))));
    configModified |=
        minCrossCompactionCandidateFileNum != conf.getMinCrossCompactionUnseqFileLevel();

    // update inner_compaction_task_selection_disk_redundancy
    double innerCompactionTaskSelectionDiskRedundancy =
        conf.getInnerCompactionTaskSelectionDiskRedundancy();
    conf.setInnerCompactionTaskSelectionDiskRedundancy(
        Double.parseDouble(
            properties.getProperty(
                "inner_compaction_task_selection_disk_redundancy",
                ConfigurationFileUtils.getConfigurationDefaultValue(
                    "inner_compaction_task_selection_disk_redundancy"))));
    configModified |=
        (Math.abs(
                innerCompactionTaskSelectionDiskRedundancy
                    - conf.getInnerCompactionTaskSelectionDiskRedundancy())
            > 0.001);

    // update inner_compaction_task_selection_mods_file_threshold
    long innerCompactionTaskSelectionModsFileThreshold =
        conf.getInnerCompactionTaskSelectionModsFileThreshold();
    conf.setInnerCompactionTaskSelectionModsFileThreshold(
        Long.parseLong(
            properties.getProperty(
                "inner_compaction_task_selection_mods_file_threshold",
                ConfigurationFileUtils.getConfigurationDefaultValue(
                    "inner_compaction_task_selection_mods_file_threshold"))));
    configModified |=
        innerCompactionTaskSelectionModsFileThreshold
            != conf.getInnerCompactionTaskSelectionModsFileThreshold();

    // update inner_seq_selector
    InnerSequenceCompactionSelector innerSequenceCompactionSelector =
        conf.getInnerSequenceCompactionSelector();
    conf.setInnerSequenceCompactionSelector(
        InnerSequenceCompactionSelector.getInnerSequenceCompactionSelector(
            properties.getProperty(
                "inner_seq_selector",
                ConfigurationFileUtils.getConfigurationDefaultValue("inner_seq_selector"))));
    configModified |= innerSequenceCompactionSelector != conf.getInnerSequenceCompactionSelector();

    // update inner_unseq_selector
    InnerUnsequenceCompactionSelector innerUnsequenceCompactionSelector =
        conf.getInnerUnsequenceCompactionSelector();
    conf.setInnerUnsequenceCompactionSelector(
        InnerUnsequenceCompactionSelector.getInnerUnsequenceCompactionSelector(
            properties.getProperty(
                "inner_unseq_selector",
                ConfigurationFileUtils.getConfigurationDefaultValue("inner_unseq_selector"))));
    configModified |=
        innerUnsequenceCompactionSelector != conf.getInnerUnsequenceCompactionSelector();

    // update inner_compaction_total_file_size_threshold
    long innerCompactionFileSizeThresholdInByte =
        conf.getInnerCompactionTotalFileSizeThresholdInByte();
    conf.setInnerCompactionTotalFileSizeThresholdInByte(
        Long.parseLong(
            properties.getProperty(
                "inner_compaction_total_file_size_threshold",
                ConfigurationFileUtils.getConfigurationDefaultValue(
                    "inner_compaction_total_file_size_threshold"))));
    configModified |=
        innerCompactionFileSizeThresholdInByte
            != conf.getInnerCompactionTotalFileSizeThresholdInByte();

    // update inner_compaction_total_file_num_threshold
    int innerCompactionTotalFileNumThreshold = conf.getInnerCompactionTotalFileNumThreshold();
    conf.setInnerCompactionTotalFileNumThreshold(
        Integer.parseInt(
            properties.getProperty(
                "inner_compaction_total_file_num_threshold",
                ConfigurationFileUtils.getConfigurationDefaultValue(
                    "inner_compaction_total_file_num_threshold"))));
    configModified |=
        innerCompactionTotalFileNumThreshold != conf.getInnerCompactionTotalFileNumThreshold();

    // update max_level_gap_in_inner_compaction
    int maxLevelGapInInnerCompaction = conf.getMaxLevelGapInInnerCompaction();
    conf.setMaxLevelGapInInnerCompaction(
        Integer.parseInt(
            properties.getProperty(
                "max_level_gap_in_inner_compaction",
                ConfigurationFileUtils.getConfigurationDefaultValue(
                    "max_level_gap_in_inner_compaction"))));
    configModified |= maxLevelGapInInnerCompaction != conf.getMaxLevelGapInInnerCompaction();

    // update compaction_max_aligned_series_num_in_one_batch
    int compactionMaxAlignedSeriesNumInOneBatch = conf.getCompactionMaxAlignedSeriesNumInOneBatch();
    int newCompactionMaxAlignedSeriesNumInOneBatch =
        Integer.parseInt(
            properties.getProperty(
                "compaction_max_aligned_series_num_in_one_batch",
                ConfigurationFileUtils.getConfigurationDefaultValue(
                    "compaction_max_aligned_series_num_in_one_batch")));
    conf.setCompactionMaxAlignedSeriesNumInOneBatch(
        newCompactionMaxAlignedSeriesNumInOneBatch > 0
            ? newCompactionMaxAlignedSeriesNumInOneBatch
            : Integer.MAX_VALUE);
    configModified |=
        compactionMaxAlignedSeriesNumInOneBatch
            != conf.getCompactionMaxAlignedSeriesNumInOneBatch();
    return configModified;
  }

  private boolean loadCompactionThreadCountHotModifiedProps(Properties properties)
      throws IOException {
    int newConfigCompactionThreadCount =
        Integer.parseInt(
            properties.getProperty(
                "compaction_thread_count",
                ConfigurationFileUtils.getConfigurationDefaultValue("compaction_thread_count")));
    if (newConfigCompactionThreadCount <= 0) {
      LOGGER.error("compaction_thread_count must greater than 0");
      return false;
    }
    if (newConfigCompactionThreadCount == conf.getCompactionThreadCount()) {
      return false;
    }
    conf.setCompactionThreadCount(
        Integer.parseInt(
            properties.getProperty(
                "compaction_thread_count",
                ConfigurationFileUtils.getConfigurationDefaultValue("compaction_thread_count"))));
    return true;
  }

  private boolean loadCompactionSubTaskCountHotModifiedProps(Properties properties)
      throws IOException {
    int newConfigSubtaskNum =
        Integer.parseInt(
            properties.getProperty(
                "sub_compaction_thread_count",
                ConfigurationFileUtils.getConfigurationDefaultValue(
                    "sub_compaction_thread_count")));
    if (newConfigSubtaskNum <= 0) {
      LOGGER.error("sub_compaction_thread_count must greater than 0");
      return false;
    }
    if (newConfigSubtaskNum == conf.getSubCompactionTaskNum()) {
      return false;
    }
    conf.setSubCompactionTaskNum(newConfigSubtaskNum);
    return true;
  }

  private void loadCompactionIsEnabledHotModifiedProps(Properties properties) throws IOException {
    boolean isCompactionEnabled =
        conf.isEnableSeqSpaceCompaction()
            || conf.isEnableUnseqSpaceCompaction()
            || conf.isEnableCrossSpaceCompaction();
    boolean newConfigEnableCrossSpaceCompaction =
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_cross_space_compaction",
                ConfigurationFileUtils.getConfigurationDefaultValue(
                    "enable_cross_space_compaction")));
    boolean newConfigEnableSeqSpaceCompaction =
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_seq_space_compaction",
                ConfigurationFileUtils.getConfigurationDefaultValue(
                    "enable_seq_space_compaction")));
    boolean newConfigEnableUnseqSpaceCompaction =
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_unseq_space_compaction",
                ConfigurationFileUtils.getConfigurationDefaultValue(
                    "enable_unseq_space_compaction")));
    boolean compactionEnabledInNewConfig =
        newConfigEnableCrossSpaceCompaction
            || newConfigEnableSeqSpaceCompaction
            || newConfigEnableUnseqSpaceCompaction;

    if (!isCompactionEnabled && compactionEnabledInNewConfig) {
      LOGGER.error("Compaction cannot start in current status.");
      return;
    }

    conf.setEnableCrossSpaceCompaction(newConfigEnableCrossSpaceCompaction);
    conf.setEnableSeqSpaceCompaction(newConfigEnableSeqSpaceCompaction);
    conf.setEnableUnseqSpaceCompaction(newConfigEnableUnseqSpaceCompaction);
  }

  private void loadWALHotModifiedProps(Properties properties) throws IOException {
    long walAsyncModeFsyncDelayInMs =
        Long.parseLong(
            properties.getProperty(
                "wal_async_mode_fsync_delay_in_ms",
                ConfigurationFileUtils.getConfigurationDefaultValue(
                    "wal_async_mode_fsync_delay_in_ms")));
    if (walAsyncModeFsyncDelayInMs > 0) {
      conf.setWalAsyncModeFsyncDelayInMs(walAsyncModeFsyncDelayInMs);
    }

    long walSyncModeFsyncDelayInMs =
        Long.parseLong(
            properties.getProperty(
                "wal_sync_mode_fsync_delay_in_ms",
                ConfigurationFileUtils.getConfigurationDefaultValue(
                    "wal_sync_mode_fsync_delay_in_ms")));
    if (walSyncModeFsyncDelayInMs > 0) {
      conf.setWalSyncModeFsyncDelayInMs(walSyncModeFsyncDelayInMs);
    }

    long walFileSizeThreshold =
        Long.parseLong(
            properties.getProperty(
                "wal_file_size_threshold_in_byte",
                ConfigurationFileUtils.getConfigurationDefaultValue(
                    "wal_file_size_threshold_in_byte")));
    if (walFileSizeThreshold > 0) {
      conf.setWalFileSizeThresholdInByte(walFileSizeThreshold);
    }

    double walMinEffectiveInfoRatio =
        Double.parseDouble(
            properties.getProperty(
                "wal_min_effective_info_ratio",
                ConfigurationFileUtils.getConfigurationDefaultValue(
                    "wal_min_effective_info_ratio")));
    if (walMinEffectiveInfoRatio > 0) {
      conf.setWalMinEffectiveInfoRatio(walMinEffectiveInfoRatio);
    }

    long walMemTableSnapshotThreshold =
        Long.parseLong(
            properties.getProperty(
                "wal_memtable_snapshot_threshold_in_byte",
                ConfigurationFileUtils.getConfigurationDefaultValue(
                    "wal_memtable_snapshot_threshold_in_byte")));
    if (walMemTableSnapshotThreshold > 0) {
      conf.setWalMemTableSnapshotThreshold(walMemTableSnapshotThreshold);
    }

    int maxWalMemTableSnapshotNum =
        Integer.parseInt(
            properties.getProperty(
                "max_wal_memtable_snapshot_num",
                ConfigurationFileUtils.getConfigurationDefaultValue(
                    "max_wal_memtable_snapshot_num")));
    if (maxWalMemTableSnapshotNum > 0) {
      conf.setMaxWalMemTableSnapshotNum(maxWalMemTableSnapshotNum);
    }

    long deleteWalFilesPeriod =
        Long.parseLong(
            properties.getProperty(
                "delete_wal_files_period_in_ms",
                ConfigurationFileUtils.getConfigurationDefaultValue(
                    "delete_wal_files_period_in_ms")));
    if (deleteWalFilesPeriod > 0) {
      conf.setDeleteWalFilesPeriodInMs(deleteWalFilesPeriod);
    }

    long throttleDownThresholdInByte =
        Long.parseLong(
            properties.getProperty(
                "iot_consensus_throttle_threshold_in_byte",
                ConfigurationFileUtils.getConfigurationDefaultValue(
                    "iot_consensus_throttle_threshold_in_byte")));
    if (throttleDownThresholdInByte > 0) {
      conf.setThrottleThreshold(throttleDownThresholdInByte);
    }

    long cacheWindowInMs =
        Long.parseLong(
            properties.getProperty(
                "iot_consensus_cache_window_time_in_ms",
                ConfigurationFileUtils.getConfigurationDefaultValue(
                    "iot_consensus_cache_window_time_in_ms")));
    if (cacheWindowInMs > 0) {
      conf.setCacheWindowTimeInMs(cacheWindowInMs);
    }
  }

  public long getThrottleThresholdWithDirs() {
    ArrayList<String> dataDiskDirs = new ArrayList<>(Arrays.asList(conf.getDataDirs()));
    ArrayList<String> walDiskDirs =
        new ArrayList<>(Arrays.asList(commonDescriptor.getConfig().getWalDirs()));
    Set<FileStore> dataFileStores = SystemMetrics.getFileStores(dataDiskDirs);
    Set<FileStore> walFileStores = SystemMetrics.getFileStores(walDiskDirs);
    double dirUseProportion = 0;
    dataFileStores.retainAll(walFileStores);
    // if there is no common disk between data and wal, use more usableSpace.
    if (dataFileStores.isEmpty()) {
      dirUseProportion = MAX_DIR_USE_PROPORTION;
    } else {
      dirUseProportion = MIN_DIR_USE_PROPORTION;
    }
    long newThrottleThreshold = Long.MAX_VALUE;
    for (FileStore fileStore : walFileStores) {
      try {
        newThrottleThreshold = Math.min(newThrottleThreshold, fileStore.getUsableSpace());
      } catch (IOException e) {
        LOGGER.error("Failed to get file size of {}, because", fileStore, e);
      }
    }
    newThrottleThreshold = (long) (newThrottleThreshold * dirUseProportion * walFileStores.size());
    // the new throttle threshold should between MIN_THROTTLE_THRESHOLD and MAX_THROTTLE_THRESHOLD
    return Math.max(Math.min(newThrottleThreshold, MAX_THROTTLE_THRESHOLD), MIN_THROTTLE_THRESHOLD);
  }

  private void loadAutoCreateSchemaProps(Properties properties) throws IOException {
    conf.setAutoCreateSchemaEnabled(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_auto_create_schema",
                ConfigurationFileUtils.getConfigurationDefaultValue("enable_auto_create_schema"))));
    conf.setBooleanStringInferType(
        TSDataType.valueOf(
            properties.getProperty(
                "boolean_string_infer_type",
                ConfigurationFileUtils.getConfigurationDefaultValue("boolean_string_infer_type"))));
    conf.setIntegerStringInferType(
        TSDataType.valueOf(
            properties.getProperty(
                "integer_string_infer_type",
                ConfigurationFileUtils.getConfigurationDefaultValue("integer_string_infer_type"))));
    conf.setFloatingStringInferType(
        TSDataType.valueOf(
            properties.getProperty(
                "floating_string_infer_type",
                ConfigurationFileUtils.getConfigurationDefaultValue(
                    "floating_string_infer_type"))));
    conf.setNanStringInferType(
        TSDataType.valueOf(
            properties.getProperty(
                "nan_string_infer_type",
                ConfigurationFileUtils.getConfigurationDefaultValue("nan_string_infer_type"))));
    conf.setDefaultStorageGroupLevel(
        Integer.parseInt(
            properties.getProperty(
                "default_storage_group_level",
                ConfigurationFileUtils.getConfigurationDefaultValue(
                    "default_storage_group_level"))));
    conf.setDefaultBooleanEncoding(
        properties.getProperty(
            "default_boolean_encoding",
            ConfigurationFileUtils.getConfigurationDefaultValue("default_boolean_encoding")));
    conf.setDefaultInt32Encoding(
        properties.getProperty(
            "default_int32_encoding",
            ConfigurationFileUtils.getConfigurationDefaultValue("default_int32_encoding")));
    conf.setDefaultInt64Encoding(
        properties.getProperty(
            "default_int64_encoding",
            ConfigurationFileUtils.getConfigurationDefaultValue("default_int64_encoding")));
    conf.setDefaultFloatEncoding(
        properties.getProperty(
            "default_float_encoding",
            ConfigurationFileUtils.getConfigurationDefaultValue("default_float_encoding")));
    conf.setDefaultDoubleEncoding(
        properties.getProperty(
            "default_double_encoding",
            ConfigurationFileUtils.getConfigurationDefaultValue("default_double_encoding")));
    conf.setDefaultTextEncoding(
        properties.getProperty(
            "default_text_encoding",
            ConfigurationFileUtils.getConfigurationDefaultValue("default_text_encoding")));
  }

  private void loadTsFileProps(Properties properties) throws IOException {
    TSFileDescriptor.getInstance()
        .getConfig()
        .setGroupSizeInByte(
            Integer.parseInt(
                properties.getProperty(
                    "group_size_in_byte",
                    ConfigurationFileUtils.getConfigurationDefaultValue("group_size_in_byte"))));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setPageSizeInByte(
            Integer.parseInt(
                properties.getProperty(
                    "page_size_in_byte",
                    ConfigurationFileUtils.getConfigurationDefaultValue("page_size_in_byte"))));
    if (TSFileDescriptor.getInstance().getConfig().getPageSizeInByte()
        > TSFileDescriptor.getInstance().getConfig().getGroupSizeInByte()) {
      LOGGER.warn("page_size is greater than group size, will set it as the same with group size");
      TSFileDescriptor.getInstance()
          .getConfig()
          .setPageSizeInByte(TSFileDescriptor.getInstance().getConfig().getGroupSizeInByte());
    }
    TSFileDescriptor.getInstance()
        .getConfig()
        .setMaxNumberOfPointsInPage(
            Integer.parseInt(
                properties.getProperty(
                    "max_number_of_points_in_page",
                    ConfigurationFileUtils.getConfigurationDefaultValue(
                        "max_number_of_points_in_page"))));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setFloatPrecision(
            Integer.parseInt(
                properties.getProperty(
                    "float_precision",
                    ConfigurationFileUtils.getConfigurationDefaultValue("float_precision"))));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setValueEncoder(
            properties.getProperty(
                "value_encoder",
                ConfigurationFileUtils.getConfigurationDefaultValue("value_encoder")));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setCompressor(
            properties.getProperty(
                "compressor", ConfigurationFileUtils.getConfigurationDefaultValue("compressor")));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setMaxTsBlockSizeInBytes(
            Integer.parseInt(
                properties.getProperty(
                    "max_tsblock_size_in_bytes",
                    ConfigurationFileUtils.getConfigurationDefaultValue(
                        "max_tsblock_size_in_bytes"))));

    // min(default_size, maxBytesForQuery)
    TSFileDescriptor.getInstance()
        .getConfig()
        .setMaxTsBlockSizeInBytes(
            (int)
                Math.min(
                    TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes(),
                    conf.getMaxBytesPerFragmentInstance()));

    TSFileDescriptor.getInstance()
        .getConfig()
        .setMaxTsBlockLineNumber(
            Integer.parseInt(
                properties.getProperty(
                    "max_tsblock_line_number",
                    ConfigurationFileUtils.getConfigurationDefaultValue(
                        "max_tsblock_line_number"))));
  }

  // Mqtt related
  private void loadMqttProps(Properties properties) {
    conf.setMqttDir(properties.getProperty("mqtt_root_dir", conf.getMqttDir()));

    if (properties.getProperty(IoTDBConstant.MQTT_HOST_NAME) != null) {
      conf.setMqttHost(properties.getProperty(IoTDBConstant.MQTT_HOST_NAME));
    } else {
      LOGGER.info("MQTT host is not configured, will use dn_rpc_address.");
      conf.setMqttHost(
          properties.getProperty(IoTDBConstant.DN_RPC_ADDRESS, conf.getRpcAddress().trim()));
    }

    if (properties.getProperty(IoTDBConstant.MQTT_PORT_NAME) != null) {
      conf.setMqttPort(Integer.parseInt(properties.getProperty(IoTDBConstant.MQTT_PORT_NAME)));
    }

    if (properties.getProperty(IoTDBConstant.MQTT_HANDLER_POOL_SIZE_NAME) != null) {
      conf.setMqttHandlerPoolSize(
          Integer.parseInt(properties.getProperty(IoTDBConstant.MQTT_HANDLER_POOL_SIZE_NAME)));
    }

    if (properties.getProperty(IoTDBConstant.MQTT_PAYLOAD_FORMATTER_NAME) != null) {
      conf.setMqttPayloadFormatter(
          properties.getProperty(IoTDBConstant.MQTT_PAYLOAD_FORMATTER_NAME));
    }

    if (properties.getProperty(IoTDBConstant.ENABLE_MQTT) != null) {
      conf.setEnableMQTTService(
          Boolean.parseBoolean(properties.getProperty(IoTDBConstant.ENABLE_MQTT)));
    }

    if (properties.getProperty(IoTDBConstant.MQTT_MAX_MESSAGE_SIZE) != null) {
      conf.setMqttMaxMessageSize(
          Integer.parseInt(properties.getProperty(IoTDBConstant.MQTT_MAX_MESSAGE_SIZE)));
    }
  }

  // timed flush memtable
  private void loadTimedService(Properties properties) throws IOException {
    conf.setEnableTimedFlushSeqMemtable(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_timed_flush_seq_memtable",
                ConfigurationFileUtils.getConfigurationDefaultValue(
                    "enable_timed_flush_seq_memtable"))));

    long seqMemTableFlushInterval =
        Long.parseLong(
            properties
                .getProperty(
                    "seq_memtable_flush_interval_in_ms",
                    ConfigurationFileUtils.getConfigurationDefaultValue(
                        "seq_memtable_flush_interval_in_ms"))
                .trim());
    if (seqMemTableFlushInterval > 0) {
      conf.setSeqMemtableFlushInterval(seqMemTableFlushInterval);
    }

    long seqMemTableFlushCheckInterval =
        Long.parseLong(
            properties
                .getProperty(
                    "seq_memtable_flush_check_interval_in_ms",
                    ConfigurationFileUtils.getConfigurationDefaultValue(
                        "seq_memtable_flush_check_interval_in_ms"))
                .trim());
    if (seqMemTableFlushCheckInterval > 0) {
      conf.setSeqMemtableFlushCheckInterval(seqMemTableFlushCheckInterval);
    }

    conf.setEnableTimedFlushUnseqMemtable(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_timed_flush_unseq_memtable",
                ConfigurationFileUtils.getConfigurationDefaultValue(
                    "enable_timed_flush_unseq_memtable"))));

    long unseqMemTableFlushInterval =
        Long.parseLong(
            properties
                .getProperty(
                    "unseq_memtable_flush_interval_in_ms",
                    ConfigurationFileUtils.getConfigurationDefaultValue(
                        "unseq_memtable_flush_interval_in_ms"))
                .trim());
    if (unseqMemTableFlushInterval > 0) {
      conf.setUnseqMemtableFlushInterval(unseqMemTableFlushInterval);
    }

    long unseqMemTableFlushCheckInterval =
        Long.parseLong(
            properties
                .getProperty(
                    "unseq_memtable_flush_check_interval_in_ms",
                    ConfigurationFileUtils.getConfigurationDefaultValue(
                        "unseq_memtable_flush_check_interval_in_ms"))
                .trim());
    if (unseqMemTableFlushCheckInterval > 0) {
      conf.setUnseqMemtableFlushCheckInterval(unseqMemTableFlushCheckInterval);
    }
  }

  private String[][] parseDataDirs(String dataDirs) {
    String[] tiers = dataDirs.split(IoTDBConstant.TIER_SEPARATOR);
    String[][] tierDataDirs = new String[tiers.length][];
    for (int i = 0; i < tiers.length; ++i) {
      tierDataDirs[i] = tiers[i].split(",");
    }
    return tierDataDirs;
  }

  public synchronized void loadHotModifiedProps(Properties properties)
      throws QueryProcessException {
    try {
      // update data dirs
      String dataDirs = properties.getProperty("dn_data_dirs", null);
      if (dataDirs != null) {
        conf.reloadDataDirs(parseDataDirs(dataDirs));
      }

      // update dir strategy
      String multiDirStrategyClassName =
          properties.getProperty(
              "dn_multi_dir_strategy",
              ConfigurationFileUtils.getConfigurationDefaultValue("dn_multi_dir_strategy"));
      if (multiDirStrategyClassName != null
          && !multiDirStrategyClassName.equals(conf.getMultiDirStrategyClassName())) {
        conf.setMultiDirStrategyClassName(multiDirStrategyClassName);
        conf.confirmMultiDirStrategy();
      }

      TierManager.getInstance().resetFolders();

      // update timed flush & close conf
      loadTimedService(properties);
      StorageEngine.getInstance().rebootTimedService();
      // update params of creating schema automatically
      loadAutoCreateSchemaProps(properties);

      // update tsfile-format config
      loadTsFileProps(properties);
      // update cluster name
      conf.setClusterName(
          properties.getProperty(
              IoTDBConstant.CLUSTER_NAME,
              ConfigurationFileUtils.getConfigurationDefaultValue(IoTDBConstant.CLUSTER_NAME)));
      // update slow_query_threshold
      conf.setSlowQueryThreshold(
          Long.parseLong(
              properties.getProperty(
                  "slow_query_threshold",
                  ConfigurationFileUtils.getConfigurationDefaultValue("slow_query_threshold"))));
      // update select into operation max buffer size
      conf.setIntoOperationBufferSizeInByte(
          Long.parseLong(
              properties.getProperty(
                  "into_operation_buffer_size_in_byte",
                  ConfigurationFileUtils.getConfigurationDefaultValue(
                      "into_operation_buffer_size_in_byte"))));
      // update insert-tablet-plan's row limit for select-into
      conf.setSelectIntoInsertTabletPlanRowLimit(
          Integer.parseInt(
              properties.getProperty(
                  "select_into_insert_tablet_plan_row_limit",
                  ConfigurationFileUtils.getConfigurationDefaultValue(
                      "select_into_insert_tablet_plan_row_limit"))));

      // update enable query memory estimation for memory control
      conf.setEnableQueryMemoryEstimation(
          Boolean.parseBoolean(
              properties.getProperty(
                  "enable_query_memory_estimation",
                  ConfigurationFileUtils.getConfigurationDefaultValue(
                      "enable_query_memory_estimation"))));

      conf.setEnableTsFileValidation(
          Boolean.parseBoolean(
              properties.getProperty(
                  "enable_tsfile_validation",
                  ConfigurationFileUtils.getConfigurationDefaultValue(
                      "enable_tsfile_validation"))));

      // update wal config
      long prevDeleteWalFilesPeriodInMs = conf.getDeleteWalFilesPeriodInMs();
      loadWALHotModifiedProps(properties);
      if (prevDeleteWalFilesPeriodInMs != conf.getDeleteWalFilesPeriodInMs()) {
        WALManager.getInstance().rebootWALDeleteThread();
      }

      // update compaction config
      loadCompactionHotModifiedProps(properties);

      // update load config
      loadLoadTsFileHotModifiedProp(properties);

      // update pipe config
      commonDescriptor
          .getConfig()
          .setPipeAllSinksRateLimitBytesPerSecond(
              Double.parseDouble(
                  properties.getProperty(
                      "pipe_all_sinks_rate_limit_bytes_per_second",
                      ConfigurationFileUtils.getConfigurationDefaultValue(
                          "pipe_all_sinks_rate_limit_bytes_per_second"))));

      // update merge_threshold_of_explain_analyze
      conf.setMergeThresholdOfExplainAnalyze(
          Integer.parseInt(
              properties.getProperty(
                  "merge_threshold_of_explain_analyze",
                  ConfigurationFileUtils.getConfigurationDefaultValue(
                      "merge_threshold_of_explain_analyze"))));
      boolean enableWALCompression =
          Boolean.parseBoolean(
              properties.getProperty(
                  "enable_wal_compression",
                  ConfigurationFileUtils.getConfigurationDefaultValue("enable_wal_compression")));
      conf.setWALCompressionAlgorithm(
          enableWALCompression ? CompressionType.LZ4 : CompressionType.UNCOMPRESSED);

      // update Consensus config
      reloadConsensusProps(properties);

      // update retry config
      commonDescriptor.loadRetryProperties(properties);
    } catch (Exception e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new QueryProcessException(String.format("Fail to reload configuration because %s", e));
    }
  }

  public synchronized void loadHotModifiedProps() throws QueryProcessException {
    URL url = getPropsUrl(CommonConfig.SYSTEM_CONFIG_NAME);
    if (url == null) {
      LOGGER.warn("Couldn't load the configuration from any of the known sources.");
      return;
    }

    Properties commonProperties = new Properties();
    try (InputStream inputStream = url.openStream()) {
      LOGGER.info("Start to reload config file {}", url);
      commonProperties.load(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
      ConfigurationFileUtils.getConfigurationDefaultValue();
      loadHotModifiedProps(commonProperties);
    } catch (Exception e) {
      LOGGER.warn("Fail to reload config file {}", url, e);
      throw new QueryProcessException(
          String.format("Fail to reload config file %s because %s", url, e.getMessage()));
    } finally {
      ConfigurationFileUtils.releaseDefault();
    }
    reloadMetricProperties(commonProperties);
  }

  public void reloadMetricProperties(Properties properties) {
    ReloadLevel reloadLevel = MetricConfigDescriptor.getInstance().loadHotProps(properties, false);
    LOGGER.info("Reload metric service in level {}", reloadLevel);
    if (reloadLevel == ReloadLevel.RESTART_INTERNAL_REPORTER) {
      IoTDBInternalReporter internalReporter;
      if (MetricConfigDescriptor.getInstance().getMetricConfig().getInternalReportType()
          == InternalReporterType.IOTDB) {
        internalReporter = new IoTDBInternalLocalReporter();
      } else {
        internalReporter = new IoTDBInternalMemoryReporter();
      }
      MetricService.getInstance().reloadInternalReporter(internalReporter);
    } else {
      MetricService.getInstance().reloadService(reloadLevel);
    }
  }

  private void initMemoryAllocate(Properties properties) {
    String memoryAllocateProportion = properties.getProperty("datanode_memory_proportion", null);
    if (memoryAllocateProportion == null) {
      memoryAllocateProportion =
          properties.getProperty("storage_query_schema_consensus_free_memory_proportion");
      if (memoryAllocateProportion != null) {
        LOGGER.warn(
            "The parameter storage_query_schema_consensus_free_memory_proportion is deprecated since v1.2.3, "
                + "please use datanode_memory_proportion instead.");
      }
    }

    if (memoryAllocateProportion != null) {
      String[] proportions = memoryAllocateProportion.split(":");
      int proportionSum = 0;
      for (String proportion : proportions) {
        proportionSum += Integer.parseInt(proportion.trim());
      }
      long maxMemoryAvailable = Runtime.getRuntime().maxMemory();
      if (proportionSum != 0) {
        conf.setAllocateMemoryForStorageEngine(
            maxMemoryAvailable * Integer.parseInt(proportions[0].trim()) / proportionSum);
        conf.setAllocateMemoryForRead(
            maxMemoryAvailable * Integer.parseInt(proportions[1].trim()) / proportionSum);
        conf.setAllocateMemoryForSchema(
            maxMemoryAvailable * Integer.parseInt(proportions[2].trim()) / proportionSum);
        conf.setAllocateMemoryForConsensus(
            maxMemoryAvailable * Integer.parseInt(proportions[3].trim()) / proportionSum);
        // if pipe proportion is set, use it, otherwise use the default value
        if (proportions.length >= 6) {
          conf.setAllocateMemoryForPipe(
              maxMemoryAvailable * Integer.parseInt(proportions[4].trim()) / proportionSum);
        } else {
          conf.setAllocateMemoryForPipe(
              (maxMemoryAvailable
                      - (conf.getAllocateMemoryForStorageEngine()
                          + conf.getAllocateMemoryForRead()
                          + conf.getAllocateMemoryForSchema()
                          + conf.getAllocateMemoryForConsensus()))
                  / 2);
        }
      }
    }

    LOGGER.info("initial allocateMemoryForRead = {}", conf.getAllocateMemoryForRead());
    LOGGER.info("initial allocateMemoryForWrite = {}", conf.getAllocateMemoryForStorageEngine());
    LOGGER.info("initial allocateMemoryForSchema = {}", conf.getAllocateMemoryForSchema());
    LOGGER.info("initial allocateMemoryForConsensus = {}", conf.getAllocateMemoryForConsensus());
    LOGGER.info("initial allocateMemoryForPipe = {}", conf.getAllocateMemoryForPipe());

    initSchemaMemoryAllocate(properties);
    initStorageEngineAllocate(properties);

    conf.setEnableQueryMemoryEstimation(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_query_memory_estimation",
                Boolean.toString(conf.isEnableQueryMemoryEstimation()))));

    String queryMemoryAllocateProportion =
        properties.getProperty("chunk_timeseriesmeta_free_memory_proportion");
    if (queryMemoryAllocateProportion != null) {
      String[] proportions = queryMemoryAllocateProportion.split(":");
      int proportionSum = 0;
      for (String proportion : proportions) {
        proportionSum += Integer.parseInt(proportion.trim());
      }
      long maxMemoryAvailable = conf.getAllocateMemoryForRead();
      if (proportionSum != 0) {
        try {
          conf.setAllocateMemoryForBloomFilterCache(
              maxMemoryAvailable * Integer.parseInt(proportions[0].trim()) / proportionSum);
          conf.setAllocateMemoryForChunkCache(
              maxMemoryAvailable * Integer.parseInt(proportions[1].trim()) / proportionSum);
          conf.setAllocateMemoryForTimeSeriesMetaDataCache(
              maxMemoryAvailable * Integer.parseInt(proportions[2].trim()) / proportionSum);
          conf.setAllocateMemoryForCoordinator(
              maxMemoryAvailable * Integer.parseInt(proportions[3].trim()) / proportionSum);
          conf.setAllocateMemoryForOperators(
              maxMemoryAvailable * Integer.parseInt(proportions[4].trim()) / proportionSum);
          conf.setAllocateMemoryForDataExchange(
              maxMemoryAvailable * Integer.parseInt(proportions[5].trim()) / proportionSum);
          conf.setAllocateMemoryForTimeIndex(
              maxMemoryAvailable * Integer.parseInt(proportions[6].trim()) / proportionSum);
        } catch (Exception e) {
          throw new RuntimeException(
              "Each subsection of configuration item chunkmeta_chunk_timeseriesmeta_free_memory_proportion"
                  + " should be an integer, which is "
                  + queryMemoryAllocateProportion,
              e);
        }
      }
    }

    // metadata cache is disabled, we need to move all their allocated memory to other parts
    if (!conf.isMetaDataCacheEnable()) {
      long sum =
          conf.getAllocateMemoryForBloomFilterCache()
              + conf.getAllocateMemoryForChunkCache()
              + conf.getAllocateMemoryForTimeSeriesMetaDataCache();
      conf.setAllocateMemoryForBloomFilterCache(0);
      conf.setAllocateMemoryForChunkCache(0);
      conf.setAllocateMemoryForTimeSeriesMetaDataCache(0);
      long partForDataExchange = sum / 2;
      long partForOperators = sum - partForDataExchange;
      conf.setAllocateMemoryForDataExchange(
          conf.getAllocateMemoryForDataExchange() + partForDataExchange);
      conf.setAllocateMemoryForOperators(conf.getAllocateMemoryForOperators() + partForOperators);
    }
  }

  @SuppressWarnings("java:S3518")
  private void initStorageEngineAllocate(Properties properties) {
    long storageMemoryTotal = conf.getAllocateMemoryForStorageEngine();
    String valueOfStorageEngineMemoryProportion =
        properties.getProperty("storage_engine_memory_proportion");
    if (valueOfStorageEngineMemoryProportion != null) {
      String[] storageProportionArray = valueOfStorageEngineMemoryProportion.split(":");
      int storageEngineMemoryProportion = 0;
      for (String proportion : storageProportionArray) {
        int proportionValue = Integer.parseInt(proportion.trim());
        if (proportionValue <= 0) {
          LOGGER.warn(
              "The value of storage_engine_memory_proportion is illegal, use default value 8:2 .");
          return;
        }
        storageEngineMemoryProportion += proportionValue;
      }
      conf.setCompactionProportion(
          (double) Integer.parseInt(storageProportionArray[1].trim())
              / (double) storageEngineMemoryProportion);

      String valueOfWriteMemoryProportion = properties.getProperty("write_memory_proportion");
      if (valueOfWriteMemoryProportion != null) {
        String[] writeProportionArray = valueOfWriteMemoryProportion.split(":");
        int writeMemoryProportion = 0;
        for (String proportion : writeProportionArray) {
          int proportionValue = Integer.parseInt(proportion.trim());
          writeMemoryProportion += proportionValue;
          if (proportionValue <= 0) {
            LOGGER.warn(
                "The value of write_memory_proportion is illegal, use default value 19:1 .");
            return;
          }
        }

        double writeAllProportionOfStorageEngineMemory =
            (double) Integer.parseInt(storageProportionArray[0].trim())
                / storageEngineMemoryProportion;
        double memTableProportion =
            (double) Integer.parseInt(writeProportionArray[0].trim()) / writeMemoryProportion;
        double timePartitionInfoProportion =
            (double) Integer.parseInt(writeProportionArray[1].trim()) / writeMemoryProportion;
        // writeProportionForMemtable = 8/10 * 19/20 = 0.76 default
        conf.setWriteProportionForMemtable(
            writeAllProportionOfStorageEngineMemory * memTableProportion);

        // allocateMemoryForTimePartitionInfo = storageMemoryTotal * 8/10 * 1/20 default
        conf.setAllocateMemoryForTimePartitionInfo(
            (long)
                ((writeAllProportionOfStorageEngineMemory * timePartitionInfoProportion)
                    * storageMemoryTotal));
      }
    }
  }

  @SuppressWarnings("squid:S3518")
  private void initSchemaMemoryAllocate(Properties properties) {
    long schemaMemoryTotal = conf.getAllocateMemoryForSchema();

    String schemaMemoryPortionInput = properties.getProperty("schema_memory_proportion");
    if (schemaMemoryPortionInput != null) {
      String[] proportions = schemaMemoryPortionInput.split(":");
      int loadedProportionSum = 0;
      for (String proportion : proportions) {
        loadedProportionSum += Integer.parseInt(proportion.trim());
      }

      if (loadedProportionSum != 0) {
        conf.setSchemaMemoryProportion(
            new int[] {
              Integer.parseInt(proportions[0].trim()),
              Integer.parseInt(proportions[1].trim()),
              Integer.parseInt(proportions[2].trim())
            });
      }

    } else {
      schemaMemoryPortionInput = properties.getProperty("schema_memory_allocate_proportion");
      if (schemaMemoryPortionInput != null) {
        String[] proportions = schemaMemoryPortionInput.split(":");
        int loadedProportionSum = 0;
        for (String proportion : proportions) {
          loadedProportionSum += Integer.parseInt(proportion.trim());
        }

        if (loadedProportionSum != 0) {
          conf.setSchemaMemoryProportion(
              new int[] {
                Integer.parseInt(proportions[0].trim()),
                Integer.parseInt(proportions[1].trim()) + Integer.parseInt(proportions[3].trim()),
                Integer.parseInt(proportions[2].trim())
              });
        }
      }
    }

    int proportionSum = 0;
    for (int proportion : conf.getSchemaMemoryProportion()) {
      proportionSum += proportion;
    }

    conf.setAllocateMemoryForSchemaRegion(
        schemaMemoryTotal * conf.getSchemaMemoryProportion()[0] / proportionSum);
    LOGGER.info("allocateMemoryForSchemaRegion = {}", conf.getAllocateMemoryForSchemaRegion());

    conf.setAllocateMemoryForSchemaCache(
        schemaMemoryTotal * conf.getSchemaMemoryProportion()[1] / proportionSum);
    LOGGER.info("allocateMemoryForSchemaCache = {}", conf.getAllocateMemoryForSchemaCache());

    conf.setAllocateMemoryForPartitionCache(
        schemaMemoryTotal * conf.getSchemaMemoryProportion()[2] / proportionSum);
    LOGGER.info("allocateMemoryForPartitionCache = {}", conf.getAllocateMemoryForPartitionCache());
  }

  private void loadLoadTsFileProps(Properties properties) {
    conf.setMaxAllocateMemoryRatioForLoad(
        Double.parseDouble(
            properties.getProperty(
                "max_allocate_memory_ratio_for_load",
                String.valueOf(conf.getMaxAllocateMemoryRatioForLoad()))));
    conf.setLoadTsFileAnalyzeSchemaBatchFlushTimeSeriesNumber(
        Integer.parseInt(
            properties.getProperty(
                "load_tsfile_analyze_schema_batch_flush_time_series_number",
                String.valueOf(conf.getLoadTsFileAnalyzeSchemaBatchFlushTimeSeriesNumber()))));
    conf.setLoadTsFileAnalyzeSchemaMemorySizeInBytes(
        Long.parseLong(
            properties.getProperty(
                "load_tsfile_analyze_schema_memory_size_in_bytes",
                String.valueOf(conf.getLoadTsFileAnalyzeSchemaMemorySizeInBytes()))));
    conf.setLoadTsFileMaxDeviceCountToUseDeviceTimeIndex(
        Integer.parseInt(
            properties.getProperty(
                "load_tsfile_max_device_count_to_use_device_index",
                String.valueOf(conf.getLoadTsFileMaxDeviceCountToUseDeviceTimeIndex()))));
    conf.setLoadCleanupTaskExecutionDelayTimeSeconds(
        Long.parseLong(
            properties.getProperty(
                "load_clean_up_task_execution_delay_time_seconds",
                String.valueOf(conf.getLoadCleanupTaskExecutionDelayTimeSeconds()))));
    conf.setLoadWriteThroughputBytesPerSecond(
        Double.parseDouble(
            properties.getProperty(
                "load_write_throughput_bytes_per_second",
                String.valueOf(conf.getLoadWriteThroughputBytesPerSecond()))));

    conf.setLoadActiveListeningEnable(
        Boolean.parseBoolean(
            properties.getProperty(
                "load_active_listening_enable",
                Boolean.toString(conf.getLoadActiveListeningEnable()))));
    conf.setLoadActiveListeningDirs(
        Arrays.stream(
                properties
                    .getProperty(
                        "load_active_listening_dirs",
                        String.join(",", conf.getLoadActiveListeningDirs()))
                    .trim()
                    .split(","))
            .filter(dir -> !dir.isEmpty())
            .toArray(String[]::new));
    conf.setLoadActiveListeningFailDir(
        properties.getProperty(
            "load_active_listening_fail_dir", conf.getLoadActiveListeningFailDir()));

    final long loadActiveListeningCheckIntervalSeconds =
        Long.parseLong(
            properties.getProperty(
                "load_active_listening_check_interval_seconds",
                Long.toString(conf.getLoadActiveListeningCheckIntervalSeconds())));
    conf.setLoadActiveListeningCheckIntervalSeconds(
        loadActiveListeningCheckIntervalSeconds <= 0
            ? conf.getLoadActiveListeningCheckIntervalSeconds()
            : loadActiveListeningCheckIntervalSeconds);

    final int defaultLoadActiveListeningMaxThreadNum =
        Math.min(
            conf.getLoadActiveListeningMaxThreadNum(),
            Math.max(1, Runtime.getRuntime().availableProcessors() / 2));
    final int loadActiveListeningMaxThreadNum =
        Integer.parseInt(
            properties.getProperty(
                "load_active_listening_max_thread_num",
                Integer.toString(defaultLoadActiveListeningMaxThreadNum)));
    conf.setLoadActiveListeningMaxThreadNum(
        loadActiveListeningMaxThreadNum <= 0
            ? defaultLoadActiveListeningMaxThreadNum
            : loadActiveListeningMaxThreadNum);
  }

  private void loadLoadTsFileHotModifiedProp(Properties properties) throws IOException {
    conf.setLoadCleanupTaskExecutionDelayTimeSeconds(
        Long.parseLong(
            properties.getProperty(
                "load_clean_up_task_execution_delay_time_seconds",
                ConfigurationFileUtils.getConfigurationDefaultValue(
                    "load_clean_up_task_execution_delay_time_seconds"))));

    conf.setLoadWriteThroughputBytesPerSecond(
        Double.parseDouble(
            properties.getProperty(
                "load_write_throughput_bytes_per_second",
                ConfigurationFileUtils.getConfigurationDefaultValue(
                    "load_write_throughput_bytes_per_second"))));

    conf.setLoadActiveListeningEnable(
        Boolean.parseBoolean(
            properties.getProperty(
                "load_active_listening_enable",
                ConfigurationFileUtils.getConfigurationDefaultValue(
                    "load_active_listening_enable"))));
    conf.setLoadActiveListeningDirs(
        Arrays.stream(
                properties
                    .getProperty(
                        "load_active_listening_dirs",
                        String.join(
                            ",",
                            ConfigurationFileUtils.getConfigurationDefaultValue(
                                "load_active_listening_dirs")))
                    .trim()
                    .split(","))
            .filter(dir -> !dir.isEmpty())
            .toArray(String[]::new));
    conf.setLoadActiveListeningFailDir(
        properties.getProperty(
            "load_active_listening_fail_dir",
            ConfigurationFileUtils.getConfigurationDefaultValue("load_active_listening_fail_dir")));
  }

  @SuppressWarnings("squid:S3518") // "proportionSum" can't be zero
  private void loadUDFProps(Properties properties) {
    String initialByteArrayLengthForMemoryControl =
        properties.getProperty("udf_initial_byte_array_length_for_memory_control");
    if (initialByteArrayLengthForMemoryControl != null) {
      conf.setUdfInitialByteArrayLengthForMemoryControl(
          Integer.parseInt(initialByteArrayLengthForMemoryControl));
    }

    conf.setUdfDir(properties.getProperty("udf_lib_dir", conf.getUdfDir()));

    String memoryBudgetInMb = properties.getProperty("udf_memory_budget_in_mb");
    if (memoryBudgetInMb != null) {
      conf.setUdfMemoryBudgetInMB(
          (float)
              Math.min(Float.parseFloat(memoryBudgetInMb), 0.2 * conf.getAllocateMemoryForRead()));
    }

    String readerTransformerCollectorMemoryProportion =
        properties.getProperty("udf_reader_transformer_collector_memory_proportion");
    if (readerTransformerCollectorMemoryProportion != null) {
      String[] proportions = readerTransformerCollectorMemoryProportion.split(":");
      int proportionSum = 0;
      for (String proportion : proportions) {
        proportionSum += Integer.parseInt(proportion.trim());
      }
      float maxMemoryAvailable = conf.getUdfMemoryBudgetInMB();
      try {
        conf.setUdfReaderMemoryBudgetInMB(
            maxMemoryAvailable * Integer.parseInt(proportions[0].trim()) / proportionSum);
        conf.setUdfTransformerMemoryBudgetInMB(
            maxMemoryAvailable * Integer.parseInt(proportions[1].trim()) / proportionSum);
        conf.setUdfCollectorMemoryBudgetInMB(
            maxMemoryAvailable * Integer.parseInt(proportions[2].trim()) / proportionSum);
      } catch (Exception e) {
        throw new RuntimeException(
            "Each subsection of configuration item udf_reader_transformer_collector_memory_proportion"
                + " should be an integer, which is "
                + readerTransformerCollectorMemoryProportion,
            e);
      }
    }
  }

  private void initThriftSSL(Properties properties) {
    conf.setEnableSSL(
        Boolean.parseBoolean(
            properties.getProperty("enable_thrift_ssl", Boolean.toString(conf.isEnableSSL()))));
    conf.setKeyStorePath(properties.getProperty("key_store_path", conf.getKeyStorePath()).trim());
    conf.setKeyStorePwd(properties.getProperty("key_store_pwd", conf.getKeyStorePath()).trim());
  }

  private void loadTriggerProps(Properties properties) {
    conf.setTriggerDir(properties.getProperty("trigger_lib_dir", conf.getTriggerDir()));
    conf.setRetryNumToFindStatefulTrigger(
        Integer.parseInt(
            properties.getProperty(
                "stateful_trigger_retry_num_when_not_found",
                Integer.toString(conf.getRetryNumToFindStatefulTrigger()))));

    int tlogBufferSize =
        Integer.parseInt(
            properties.getProperty("tlog_buffer_size", Integer.toString(conf.getTlogBufferSize())));
    if (tlogBufferSize > 0) {
      conf.setTlogBufferSize(tlogBufferSize);
    }

    conf.setTriggerForwardMaxQueueNumber(
        Integer.parseInt(
            properties.getProperty(
                "trigger_forward_max_queue_number",
                Integer.toString(conf.getTriggerForwardMaxQueueNumber()))));
    conf.setTriggerForwardMaxSizePerQueue(
        Integer.parseInt(
            properties.getProperty(
                "trigger_forward_max_size_per_queue",
                Integer.toString(conf.getTriggerForwardMaxSizePerQueue()))));
    conf.setTriggerForwardBatchSize(
        Integer.parseInt(
            properties.getProperty(
                "trigger_forward_batch_size",
                Integer.toString(conf.getTriggerForwardBatchSize()))));
    conf.setTriggerForwardHTTPPoolSize(
        Integer.parseInt(
            properties.getProperty(
                "trigger_forward_http_pool_size",
                Integer.toString(conf.getTriggerForwardHTTPPoolSize()))));
    conf.setTriggerForwardHTTPPOOLMaxPerRoute(
        Integer.parseInt(
            properties.getProperty(
                "trigger_forward_http_pool_max_per_route",
                Integer.toString(conf.getTriggerForwardHTTPPOOLMaxPerRoute()))));
    conf.setTriggerForwardMQTTPoolSize(
        Integer.parseInt(
            properties.getProperty(
                "trigger_forward_mqtt_pool_size",
                Integer.toString(conf.getTriggerForwardMQTTPoolSize()))));
  }

  private void loadPipeProps(Properties properties) {
    conf.setPipeLibDir(properties.getProperty("pipe_lib_dir", conf.getPipeLibDir()));

    conf.setPipeReceiverFileDirs(
        Arrays.stream(
                Optional.ofNullable(properties.getProperty("dn_pipe_receiver_file_dirs"))
                    .orElse(
                        properties.getProperty(
                            "pipe_receiver_file_dirs",
                            String.join(",", conf.getPipeReceiverFileDirs())))
                    .trim()
                    .split(","))
            .filter(dir -> !dir.isEmpty())
            .toArray(String[]::new));

    conf.setPipeConsensusReceiverFileDirs(
        Arrays.stream(
                properties
                    .getProperty(
                        "pipe_consensus_receiver_file_dirs",
                        String.join(",", conf.getPipeConsensusReceiverFileDirs()))
                    .trim()
                    .split(","))
            .filter(dir -> !dir.isEmpty())
            .toArray(String[]::new));
  }

  private void loadCQProps(Properties properties) {
    conf.setContinuousQueryThreadNum(
        Integer.parseInt(
            properties.getProperty(
                "continuous_query_thread_num",
                Integer.toString(conf.getContinuousQueryThreadNum()))));
    if (conf.getContinuousQueryThreadNum() <= 0) {
      conf.setContinuousQueryThreadNum(Runtime.getRuntime().availableProcessors() / 2);
    }

    conf.setContinuousQueryMinimumEveryInterval(
        DateTimeUtils.convertDurationStrToLong(
            properties.getProperty("continuous_query_minimum_every_interval", "1s"),
            CommonDescriptor.getInstance().getConfig().getTimestampPrecision(),
            false));
  }

  public void loadClusterProps(Properties properties) throws IOException {
    String configNodeUrls = properties.getProperty(IoTDBConstant.DN_SEED_CONFIG_NODE);
    if (configNodeUrls == null) {
      configNodeUrls = properties.getProperty(IoTDBConstant.DN_TARGET_CONFIG_NODE_LIST);
      LOGGER.warn(
          "The parameter dn_target_config_node_list has been abandoned, "
              + "only the first ConfigNode address will be used to join in the cluster. "
              + "Please use dn_seed_config_node instead.");
    }
    if (configNodeUrls != null) {
      try {
        configNodeUrls = configNodeUrls.trim();
        conf.setSeedConfigNode(NodeUrlUtils.parseTEndPointUrls(configNodeUrls).get(0));
      } catch (BadNodeUrlException e) {
        LOGGER.error(
            "ConfigNodes are set in wrong format, please set them like 127.0.0.1:10710", e);
      }
    }

    conf.setInternalAddress(
        properties
            .getProperty(IoTDBConstant.DN_INTERNAL_ADDRESS, conf.getInternalAddress())
            .trim());

    conf.setInternalPort(
        Integer.parseInt(
            properties
                .getProperty(
                    IoTDBConstant.DN_INTERNAL_PORT, Integer.toString(conf.getInternalPort()))
                .trim()));

    conf.setDataRegionConsensusPort(
        Integer.parseInt(
            properties
                .getProperty(
                    "dn_data_region_consensus_port",
                    Integer.toString(conf.getDataRegionConsensusPort()))
                .trim()));

    conf.setSchemaRegionConsensusPort(
        Integer.parseInt(
            properties
                .getProperty(
                    "dn_schema_region_consensus_port",
                    Integer.toString(conf.getSchemaRegionConsensusPort()))
                .trim()));
    conf.setJoinClusterRetryIntervalMs(
        Long.parseLong(
            properties
                .getProperty(
                    "dn_join_cluster_retry_interval_ms",
                    Long.toString(conf.getJoinClusterRetryIntervalMs()))
                .trim()));
  }

  public void loadShuffleProps(Properties properties) {
    conf.setMppDataExchangePort(
        Integer.parseInt(
            properties.getProperty(
                "dn_mpp_data_exchange_port", Integer.toString(conf.getMppDataExchangePort()))));
    conf.setMppDataExchangeCorePoolSize(
        Integer.parseInt(
            properties.getProperty(
                "mpp_data_exchange_core_pool_size",
                Integer.toString(conf.getMppDataExchangeCorePoolSize()))));
    conf.setMppDataExchangeMaxPoolSize(
        Integer.parseInt(
            properties.getProperty(
                "mpp_data_exchange_max_pool_size",
                Integer.toString(conf.getMppDataExchangeMaxPoolSize()))));
    conf.setMppDataExchangeKeepAliveTimeInMs(
        Integer.parseInt(
            properties.getProperty(
                "mpp_data_exchange_keep_alive_time_in_ms",
                Integer.toString(conf.getMppDataExchangeKeepAliveTimeInMs()))));

    conf.setPartitionCacheSize(
        Integer.parseInt(
            properties.getProperty(
                "partition_cache_size", Integer.toString(conf.getPartitionCacheSize()))));

    conf.setDriverTaskExecutionTimeSliceInMs(
        Integer.parseInt(
            properties.getProperty(
                "driver_task_execution_time_slice_in_ms",
                Integer.toString(conf.getDriverTaskExecutionTimeSliceInMs()))));
  }

  /** Get default encode algorithm by data type */
  public TSEncoding getDefaultEncodingByType(TSDataType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return conf.getDefaultBooleanEncoding();
      case INT32:
      case DATE:
        return conf.getDefaultInt32Encoding();
      case INT64:
      case TIMESTAMP:
        return conf.getDefaultInt64Encoding();
      case FLOAT:
        return conf.getDefaultFloatEncoding();
      case DOUBLE:
        return conf.getDefaultDoubleEncoding();
      case STRING:
      case BLOB:
      case TEXT:
      default:
        return conf.getDefaultTextEncoding();
    }
  }

  // These configurations are received from config node when registering
  public void loadGlobalConfig(TGlobalConfig globalConfig) {
    conf.setSeriesPartitionExecutorClass(globalConfig.getSeriesPartitionExecutorClass());
    conf.setSeriesPartitionSlotNum(globalConfig.getSeriesPartitionSlotNum());
    conf.setReadConsistencyLevel(globalConfig.getReadConsistencyLevel());
  }

  public void loadRatisConfig(TRatisConfig ratisConfig) {
    conf.setDataRatisConsensusLogAppenderBufferSizeMax(ratisConfig.getDataAppenderBufferSize());
    conf.setSchemaRatisConsensusLogAppenderBufferSizeMax(ratisConfig.getSchemaAppenderBufferSize());

    conf.setDataRatisConsensusSnapshotTriggerThreshold(
        ratisConfig.getDataSnapshotTriggerThreshold());
    conf.setSchemaRatisConsensusSnapshotTriggerThreshold(
        ratisConfig.getSchemaSnapshotTriggerThreshold());

    conf.setDataRatisConsensusLogUnsafeFlushEnable(ratisConfig.isDataLogUnsafeFlushEnable());
    conf.setSchemaRatisConsensusLogUnsafeFlushEnable(ratisConfig.isSchemaLogUnsafeFlushEnable());

    conf.setDataRatisConsensusLogForceSyncNum(ratisConfig.getDataRegionLogForceSyncNum());
    conf.setSchemaRatisConsensusLogForceSyncNum(ratisConfig.getSchemaRegionLogForceSyncNum());

    conf.setDataRatisConsensusLogSegmentSizeMax(ratisConfig.getDataLogSegmentSizeMax());
    conf.setSchemaRatisConsensusLogSegmentSizeMax(ratisConfig.getSchemaLogSegmentSizeMax());

    conf.setDataRatisConsensusGrpcFlowControlWindow(ratisConfig.getDataGrpcFlowControlWindow());
    conf.setSchemaRatisConsensusGrpcFlowControlWindow(ratisConfig.getSchemaGrpcFlowControlWindow());

    conf.setDataRatisConsensusGrpcLeaderOutstandingAppendsMax(
        ratisConfig.getDataRegionGrpcLeaderOutstandingAppendsMax());
    conf.setSchemaRatisConsensusGrpcLeaderOutstandingAppendsMax(
        ratisConfig.getSchemaRegionGrpcLeaderOutstandingAppendsMax());

    conf.setDataRatisConsensusLeaderElectionTimeoutMinMs(
        ratisConfig.getDataLeaderElectionTimeoutMin());
    conf.setSchemaRatisConsensusLeaderElectionTimeoutMinMs(
        ratisConfig.getSchemaLeaderElectionTimeoutMin());

    conf.setDataRatisConsensusLeaderElectionTimeoutMaxMs(
        ratisConfig.getDataLeaderElectionTimeoutMax());
    conf.setSchemaRatisConsensusLeaderElectionTimeoutMaxMs(
        ratisConfig.getSchemaLeaderElectionTimeoutMax());

    conf.setDataRatisConsensusRequestTimeoutMs(ratisConfig.getDataRequestTimeout());
    conf.setSchemaRatisConsensusRequestTimeoutMs(ratisConfig.getSchemaRequestTimeout());

    conf.setDataRatisConsensusMaxRetryAttempts(ratisConfig.getDataMaxRetryAttempts());
    conf.setDataRatisConsensusInitialSleepTimeMs(ratisConfig.getDataInitialSleepTime());
    conf.setDataRatisConsensusMaxSleepTimeMs(ratisConfig.getDataMaxSleepTime());

    conf.setSchemaRatisConsensusMaxRetryAttempts(ratisConfig.getSchemaMaxRetryAttempts());
    conf.setSchemaRatisConsensusInitialSleepTimeMs(ratisConfig.getSchemaInitialSleepTime());
    conf.setSchemaRatisConsensusMaxSleepTimeMs(ratisConfig.getSchemaMaxSleepTime());

    conf.setDataRatisConsensusPreserveWhenPurge(ratisConfig.getDataPreserveWhenPurge());
    conf.setSchemaRatisConsensusPreserveWhenPurge(ratisConfig.getSchemaPreserveWhenPurge());

    conf.setRatisFirstElectionTimeoutMinMs(ratisConfig.getFirstElectionTimeoutMin());
    conf.setRatisFirstElectionTimeoutMaxMs(ratisConfig.getFirstElectionTimeoutMax());

    conf.setSchemaRatisLogMax(ratisConfig.getSchemaRegionRatisLogMax());
    conf.setDataRatisLogMax(ratisConfig.getDataRegionRatisLogMax());

    conf.setSchemaRatisPeriodicSnapshotInterval(
        ratisConfig.getSchemaRegionPeriodicSnapshotInterval());
    conf.setDataRatisPeriodicSnapshotInterval(ratisConfig.getDataRegionPeriodicSnapshotInterval());
  }

  public void loadCQConfig(TCQConfig cqConfig) {
    conf.setCqMinEveryIntervalInMs(cqConfig.getCqMinEveryIntervalInMs());
  }

  public void reclaimConsensusMemory() {
    conf.setAllocateMemoryForStorageEngine(
        conf.getAllocateMemoryForStorageEngine() + conf.getAllocateMemoryForConsensus());
    SystemInfo.getInstance().allocateWriteMemory();
  }

  private static class IoTDBDescriptorHolder {

    private static final IoTDBDescriptor INSTANCE = new IoTDBDescriptor();

    private IoTDBDescriptorHolder() {}
  }
}
