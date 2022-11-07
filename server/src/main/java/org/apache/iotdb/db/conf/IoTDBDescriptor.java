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
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.BadNodeUrlException;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.utils.NodeUrlUtils;
import org.apache.iotdb.confignode.rpc.thrift.TCQConfig;
import org.apache.iotdb.confignode.rpc.thrift.TGlobalConfig;
import org.apache.iotdb.confignode.rpc.thrift.TRatisConfig;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.compaction.constant.CompactionPriority;
import org.apache.iotdb.db.engine.compaction.constant.CrossCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.constant.CrossCompactionSelector;
import org.apache.iotdb.db.engine.compaction.constant.InnerSeqCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.constant.InnerSequenceCompactionSelector;
import org.apache.iotdb.db.engine.compaction.constant.InnerUnseqCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.constant.InnerUnsequenceCompactionSelector;
import org.apache.iotdb.db.exception.BadNodeUrlFormatException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.utils.DateTimeUtils;
import org.apache.iotdb.db.rescon.SystemInfo;
import org.apache.iotdb.db.utils.datastructure.TVListSortAlgorithm;
import org.apache.iotdb.db.wal.WALManager;
import org.apache.iotdb.db.wal.utils.WALMode;
import org.apache.iotdb.external.api.IPropertiesLoader;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.config.ReloadLevel;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSType;
import org.apache.iotdb.tsfile.utils.FilePathUtils;

import com.google.common.net.InetAddresses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.ServiceLoader;

public class IoTDBDescriptor {

  private static final Logger logger = LoggerFactory.getLogger(IoTDBDescriptor.class);

  private final CommonDescriptor commonDescriptor = CommonDescriptor.getInstance();

  private final IoTDBConfig conf = new IoTDBConfig();

  protected IoTDBDescriptor() {
    loadProps();
    ServiceLoader<IPropertiesLoader> propertiesLoaderServiceLoader =
        ServiceLoader.load(IPropertiesLoader.class);
    for (IPropertiesLoader loader : propertiesLoaderServiceLoader) {
      logger.info("Will reload properties from {} ", loader.getClass().getName());
      Properties properties = loader.loadProperties();
      loadProperties(properties);
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
  public URL getPropsUrl(String configFileName) {
    // Check if a config-directory was specified first.
    String urlString = System.getProperty(IoTDBConstant.IOTDB_CONF, null);
    // If it wasn't, check if a home directory was provided (This usually contains a config)
    if (urlString == null) {
      urlString = System.getProperty(IoTDBConstant.IOTDB_HOME, null);
      if (urlString != null) {
        urlString = urlString + File.separatorChar + "conf" + File.separatorChar + configFileName;
      } else {
        // If this too wasn't provided, try to find a default config in the root of the classpath.
        URL uri = IoTDBConfig.class.getResource("/" + configFileName);
        if (uri != null) {
          return uri;
        }
        logger.warn(
            "Cannot find IOTDB_HOME or IOTDB_CONF environment variable when loading "
                + "config file {}, use default configuration",
            configFileName);
        // update all data seriesPath
        conf.updatePath();
        return null;
      }
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
      return null;
    }
  }

  /** load an property file and set TsfileDBConfig variables. */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private void loadProps() {
    URL url = getPropsUrl(CommonConfig.CONFIG_NAME);
    Properties commonProperties = new Properties();
    if (url != null) {
      try (InputStream inputStream = url.openStream()) {
        logger.info("Start to read config file {}", url);
        commonProperties.load(inputStream);
      } catch (FileNotFoundException e) {
        logger.warn("Fail to find config file {}", url, e);
      } catch (IOException e) {
        logger.warn("Cannot load config file, use default configuration", e);
      } catch (Exception e) {
        logger.warn("Incorrect format in config file, use default configuration", e);
      }
    } else {
      logger.warn(
          "Couldn't load the configuration {} from any of the known sources.",
          CommonConfig.CONFIG_NAME);
    }
    url = getPropsUrl(IoTDBConfig.CONFIG_NAME);
    if (url != null) {
      try (InputStream inputStream = url.openStream()) {
        logger.info("Start to read config file {}", url);
        Properties properties = new Properties();
        properties.load(inputStream);
        commonProperties.putAll(properties);
        loadProperties(commonProperties);
      } catch (FileNotFoundException e) {
        logger.warn("Fail to find config file {}", url, e);
      } catch (IOException e) {
        logger.warn("Cannot load config file, use default configuration", e);
      } catch (Exception e) {
        logger.warn("Incorrect format in config file, use default configuration", e);
      } finally {
        // update all data seriesPath
        conf.updatePath();
        commonDescriptor.getConfig().updatePath(System.getProperty(IoTDBConstant.IOTDB_HOME, null));
        MetricConfigDescriptor.getInstance()
            .getMetricConfig()
            .updateRpcInstance(conf.getRpcAddress(), conf.getRpcPort());
      }
    } else {
      logger.warn(
          "Couldn't load the configuration {} from any of the known sources.",
          IoTDBConfig.CONFIG_NAME);
    }
  }

  public void loadProperties(Properties properties) {

    conf.setRpcAddress(
        properties.getProperty(IoTDBConstant.DN_RPC_ADDRESS, conf.getRpcAddress()).trim());

    // TODO: Use FQDN  to identify our nodes afterwards
    try {
      replaceHostnameWithIP();
    } catch (Exception e) {
      logger.info(String.format("replace hostname with ip failed, %s", e.getMessage()));
    }

    conf.setRpcThriftCompressionEnable(
        Boolean.parseBoolean(
            properties
                .getProperty(
                    "rpc_thrift_compression_enable",
                    Boolean.toString(conf.isRpcThriftCompressionEnable()))
                .trim()));

    conf.setRpcAdvancedCompressionEnable(
        Boolean.parseBoolean(
            properties
                .getProperty(
                    "rpc_advanced_compression_enable",
                    Boolean.toString(conf.isRpcAdvancedCompressionEnable()))
                .trim()));

    conf.setConnectionTimeoutInMS(
        Integer.parseInt(
            properties
                .getProperty(
                    "connection_timeout_ms", String.valueOf(conf.getConnectionTimeoutInMS()))
                .trim()));

    conf.setMaxConnectionForInternalService(
        Integer.parseInt(
            properties
                .getProperty(
                    "max_connection_for_internal_service",
                    String.valueOf(conf.getMaxConnectionForInternalService()))
                .trim()));

    conf.setCoreConnectionForInternalService(
        Integer.parseInt(
            properties
                .getProperty(
                    "core_connection_for_internal_service",
                    String.valueOf(conf.getCoreConnectionForInternalService()))
                .trim()));

    conf.setSelectorNumOfClientManager(
        Integer.parseInt(
            properties
                .getProperty(
                    "selector_thread_count_of_client_manager",
                    String.valueOf(conf.getSelectorNumOfClientManager()))
                .trim()));

    conf.setRpcPort(
        Integer.parseInt(
            properties
                .getProperty(IoTDBConstant.DN_RPC_PORT, Integer.toString(conf.getRpcPort()))
                .trim()));

    conf.setEnableInfluxDBRpcService(
        Boolean.parseBoolean(
            properties
                .getProperty(
                    "enable_influxdb_rpc_service",
                    Boolean.toString(conf.isEnableInfluxDBRpcService()))
                .trim()));

    conf.setInfluxDBRpcPort(
        Integer.parseInt(
            properties
                .getProperty("influxdb_rpc_port", Integer.toString(conf.getInfluxDBRpcPort()))
                .trim()));

    conf.setTimestampPrecision(
        properties.getProperty("timestamp_precision", conf.getTimestampPrecision()).trim());

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

    conf.setRejectProportion(
        Double.parseDouble(
            properties
                .getProperty("reject_proportion", Double.toString(conf.getRejectProportion()))
                .trim()));

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

    loadWALProps(properties);

    String systemDir = properties.getProperty("dn_system_dir");
    if (systemDir == null) {
      systemDir = properties.getProperty("base_dir");
      if (systemDir != null) {
        systemDir =
            FilePathUtils.regularizePath(systemDir.trim()) + IoTDBConstant.SYSTEM_FOLDER_NAME;
      } else {
        systemDir = conf.getSystemDir();
      }
    }
    conf.setSystemDir(systemDir);

    conf.setSchemaDir(
        FilePathUtils.regularizePath(conf.getSystemDir()) + IoTDBConstant.SCHEMA_FOLDER_NAME);

    conf.setQueryDir(
        FilePathUtils.regularizePath(conf.getSystemDir() + IoTDBConstant.QUERY_FOLDER_NAME));

    conf.setTracingDir(properties.getProperty("dn_tracing_dir", conf.getTracingDir()).trim());

    conf.setDataDirs(
        properties.getProperty("dn_data_dirs", conf.getDataDirs()[0]).trim().split(","));

    conf.setConsensusDir(properties.getProperty("dn_consensus_dir", conf.getConsensusDir()).trim());

    int mlogBufferSize =
        Integer.parseInt(
            properties
                .getProperty("mlog_buffer_size", Integer.toString(conf.getMlogBufferSize()))
                .trim());
    if (mlogBufferSize > 0) {
      conf.setMlogBufferSize(mlogBufferSize);
    }

    long forceMlogPeriodInMs =
        Long.parseLong(
            properties
                .getProperty("sync_mlog_period_in_ms", Long.toString(conf.getSyncMlogPeriodInMs()))
                .trim());
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
            properties.getProperty("batch_size", Integer.toString(conf.getBatchSize())).trim()));

    conf.setEnableMemControl(
        (Boolean.parseBoolean(
            properties
                .getProperty("enable_mem_control", Boolean.toString(conf.isEnableMemControl()))
                .trim())));
    logger.info("IoTDB enable memory control: {}", conf.isEnableMemControl());

    long seqTsFileSize =
        Long.parseLong(
            properties
                .getProperty("seq_tsfile_size", Long.toString(conf.getSeqTsFileSize()))
                .trim());
    if (seqTsFileSize >= 0) {
      conf.setSeqTsFileSize(seqTsFileSize);
    }

    long unSeqTsFileSize =
        Long.parseLong(
            properties
                .getProperty("unseq_tsfile_size", Long.toString(conf.getUnSeqTsFileSize()))
                .trim());
    if (unSeqTsFileSize >= 0) {
      conf.setUnSeqTsFileSize(unSeqTsFileSize);
    }

    long memTableSizeThreshold =
        Long.parseLong(
            properties
                .getProperty(
                    "memtable_size_threshold", Long.toString(conf.getMemtableSizeThreshold()))
                .trim());
    if (memTableSizeThreshold > 0) {
      conf.setMemtableSizeThreshold(memTableSizeThreshold);
    }

    conf.setTvListSortAlgorithm(
        TVListSortAlgorithm.valueOf(
            properties
                .getProperty("tvlist_sort_algorithm", conf.getTvListSortAlgorithm().toString())
                .trim()));

    conf.setAvgSeriesPointNumberThreshold(
        Integer.parseInt(
            properties
                .getProperty(
                    "avg_series_point_number_threshold",
                    Integer.toString(conf.getAvgSeriesPointNumberThreshold()))
                .trim()));

    conf.setCheckPeriodWhenInsertBlocked(
        Integer.parseInt(
            properties
                .getProperty(
                    "check_period_when_insert_blocked",
                    Integer.toString(conf.getCheckPeriodWhenInsertBlocked()))
                .trim()));

    conf.setMaxWaitingTimeWhenInsertBlocked(
        Integer.parseInt(
            properties
                .getProperty(
                    "max_waiting_time_when_insert_blocked",
                    Integer.toString(conf.getMaxWaitingTimeWhenInsertBlocked()))
                .trim()));

    conf.setIoTaskQueueSizeForFlushing(
        Integer.parseInt(
            properties
                .getProperty(
                    "io_task_queue_size_for_flushing",
                    Integer.toString(conf.getIoTaskQueueSizeForFlushing()))
                .trim()));

    conf.setCompactionScheduleIntervalInMs(
        Long.parseLong(
            properties
                .getProperty(
                    "compaction_schedule_interval_in_ms",
                    Long.toString(conf.getCompactionScheduleIntervalInMs()))
                .trim()));

    conf.setCompactionSubmissionIntervalInMs(
        Long.parseLong(
            properties
                .getProperty(
                    "compaction_submission_interval_in_ms",
                    Long.toString(conf.getCompactionSubmissionIntervalInMs()))
                .trim()));

    conf.setEnableCrossSpaceCompaction(
        Boolean.parseBoolean(
            properties
                .getProperty(
                    "enable_cross_space_compaction",
                    Boolean.toString(conf.isEnableCrossSpaceCompaction()))
                .trim()));

    conf.setEnableSeqSpaceCompaction(
        Boolean.parseBoolean(
            properties
                .getProperty(
                    "enable_seq_space_compaction",
                    Boolean.toString(conf.isEnableSeqSpaceCompaction()))
                .trim()));

    conf.setEnableUnseqSpaceCompaction(
        Boolean.parseBoolean(
            properties
                .getProperty(
                    "enable_unseq_space_compaction",
                    Boolean.toString(conf.isEnableUnseqSpaceCompaction()))
                .trim()));

    conf.setCrossCompactionSelector(
        CrossCompactionSelector.getCrossCompactionSelector(
            properties
                .getProperty("cross_selector", conf.getCrossCompactionSelector().toString())
                .trim()));

    conf.setInnerSequenceCompactionSelector(
        InnerSequenceCompactionSelector.getInnerSequenceCompactionSelector(
            properties
                .getProperty(
                    "inner_seq_selector", conf.getInnerSequenceCompactionSelector().toString())
                .trim()));

    conf.setInnerUnsequenceCompactionSelector(
        InnerUnsequenceCompactionSelector.getInnerUnsequenceCompactionSelector(
            properties
                .getProperty(
                    "inner_unseq_selector", conf.getInnerUnsequenceCompactionSelector().toString())
                .trim()));

    conf.setInnerSeqCompactionPerformer(
        InnerSeqCompactionPerformer.getInnerSeqCompactionPerformer(
            properties
                .getProperty(
                    "inner_seq_performer", conf.getInnerSeqCompactionPerformer().toString())
                .trim()));

    conf.setInnerUnseqCompactionPerformer(
        InnerUnseqCompactionPerformer.getInnerUnseqCompactionPerformer(
            properties
                .getProperty(
                    "inner_unseq_performer", conf.getInnerUnseqCompactionPerformer().toString())
                .trim()));

    conf.setCrossCompactionPerformer(
        CrossCompactionPerformer.getCrossCompactionPerformer(
            properties
                .getProperty("cross_performer", conf.getCrossCompactionPerformer().toString())
                .trim()));

    conf.setCompactionPriority(
        CompactionPriority.valueOf(
            properties
                .getProperty("compaction_priority", conf.getCompactionPriority().toString())
                .trim()));

    int subtaskNum =
        Integer.parseInt(
            properties
                .getProperty(
                    "sub_compaction_thread_count", Integer.toString(conf.getSubCompactionTaskNum()))
                .trim());
    subtaskNum = subtaskNum <= 0 ? 1 : subtaskNum;
    conf.setSubCompactionTaskNum(subtaskNum);

    conf.setQueryTimeoutThreshold(
        Long.parseLong(
            properties
                .getProperty(
                    "query_timeout_threshold", Long.toString(conf.getQueryTimeoutThreshold()))
                .trim()));

    conf.setSessionTimeoutThreshold(
        Integer.parseInt(
            properties
                .getProperty(
                    "session_timeout_threshold",
                    Integer.toString(conf.getSessionTimeoutThreshold()))
                .trim()));
    conf.setMaxNumberOfSyncFileRetry(
        Integer.parseInt(
            properties
                .getProperty(
                    "max_number_of_sync_file_retry",
                    Integer.toString(conf.getMaxNumberOfSyncFileRetry()))
                .trim()));

    conf.setIpWhiteList(properties.getProperty("ip_white_list", conf.getIpWhiteList()).trim());

    conf.setFlushThreadCount(
        Integer.parseInt(
            properties
                .getProperty("flush_thread_count", Integer.toString(conf.getFlushThreadCount()))
                .trim()));

    if (conf.getFlushThreadCount() <= 0) {
      conf.setFlushThreadCount(Runtime.getRuntime().availableProcessors());
    }

    // start: index parameter setting
    conf.setIndexRootFolder(
        properties.getProperty("index_root_dir", conf.getIndexRootFolder()).trim());

    conf.setEnableIndex(
        Boolean.parseBoolean(
            properties.getProperty("enable_index", Boolean.toString(conf.isEnableIndex())).trim()));

    conf.setConcurrentIndexBuildThread(
        Integer.parseInt(
            properties
                .getProperty(
                    "concurrent_index_build_thread",
                    Integer.toString(conf.getConcurrentIndexBuildThread()))
                .trim()));
    if (conf.getConcurrentIndexBuildThread() <= 0) {
      conf.setConcurrentIndexBuildThread(Runtime.getRuntime().availableProcessors());
    }

    conf.setDefaultIndexWindowRange(
        Integer.parseInt(
            properties
                .getProperty(
                    "default_index_window_range",
                    Integer.toString(conf.getDefaultIndexWindowRange()))
                .trim()));

    conf.setQueryThreadCount(
        Integer.parseInt(
            properties
                .getProperty("query_thread_count", Integer.toString(conf.getQueryThreadCount()))
                .trim()));

    if (conf.getQueryThreadCount() <= 0) {
      conf.setQueryThreadCount(Runtime.getRuntime().availableProcessors());
    }

    conf.setMaxAllowedConcurrentQueries(
        Integer.parseInt(
            properties
                .getProperty(
                    "max_allowed_concurrent_queries",
                    Integer.toString(conf.getMaxAllowedConcurrentQueries()))
                .trim()));

    if (conf.getMaxAllowedConcurrentQueries() <= 0) {
      conf.setMaxAllowedConcurrentQueries(1000);
    }

    conf.setSubRawQueryThreadCount(
        Integer.parseInt(
            properties
                .getProperty(
                    "sub_rawQuery_thread_count", Integer.toString(conf.getSubRawQueryThreadCount()))
                .trim()));

    if (conf.getSubRawQueryThreadCount() <= 0) {
      conf.setSubRawQueryThreadCount(Runtime.getRuntime().availableProcessors());
    }

    conf.setRawQueryBlockingQueueCapacity(
        Integer.parseInt(
            properties
                .getProperty(
                    "raw_query_blocking_queue_capacity",
                    Integer.toString(conf.getRawQueryBlockingQueueCapacity()))
                .trim()));

    conf.setSchemaRegionDeviceNodeCacheSize(
        Integer.parseInt(
            properties
                .getProperty(
                    "schema_region_device_node_cache_size",
                    Integer.toString(conf.getSchemaRegionDeviceNodeCacheSize()))
                .trim()));

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
          Boolean.parseBoolean(properties.getProperty("chunk_buffer_pool_enable").trim()));
    }

    conf.setEnableExternalSort(
        Boolean.parseBoolean(
            properties
                .getProperty("enable_external_sort", Boolean.toString(conf.isEnableExternalSort()))
                .trim()));
    conf.setExternalSortThreshold(
        Integer.parseInt(
            properties
                .getProperty(
                    "external_sort_threshold", Integer.toString(conf.getExternalSortThreshold()))
                .trim()));
    conf.setUpgradeThreadCount(
        Integer.parseInt(
            properties
                .getProperty("upgrade_thread_count", Integer.toString(conf.getUpgradeThreadCount()))
                .trim()));
    conf.setCrossCompactionFileSelectionTimeBudget(
        Long.parseLong(
            properties
                .getProperty(
                    "cross_compaction_file_selection_time_budget",
                    Long.toString(conf.getCrossCompactionFileSelectionTimeBudget()))
                .trim()));
    conf.setMergeIntervalSec(
        Long.parseLong(
            properties
                .getProperty("merge_interval_sec", Long.toString(conf.getMergeIntervalSec()))
                .trim()));
    conf.setCompactionThreadCount(
        Integer.parseInt(
            properties
                .getProperty(
                    "compaction_thread_count", Integer.toString(conf.getCompactionThreadCount()))
                .trim()));
    conf.setChunkMetadataSizeProportion(
        Double.parseDouble(
            properties
                .getProperty(
                    "chunk_metadata_size_proportion",
                    Double.toString(conf.getChunkMetadataSizeProportion()))
                .trim()));
    conf.setTargetCompactionFileSize(
        Long.parseLong(
            properties
                .getProperty(
                    "target_compaction_file_size",
                    Long.toString(conf.getTargetCompactionFileSize()))
                .trim()));
    conf.setTargetChunkSize(
        Long.parseLong(
            properties
                .getProperty("target_chunk_size", Long.toString(conf.getTargetChunkSize()))
                .trim()));
    conf.setTargetChunkPointNum(
        Long.parseLong(
            properties
                .getProperty("target_chunk_point_num", Long.toString(conf.getTargetChunkPointNum()))
                .trim()));
    conf.setChunkPointNumLowerBoundInCompaction(
        Long.parseLong(
            properties
                .getProperty(
                    "chunk_size_lower_bound_in_compaction",
                    Long.toString(conf.getChunkPointNumLowerBoundInCompaction()))
                .trim()));
    conf.setChunkSizeLowerBoundInCompaction(
        Long.parseLong(
            properties
                .getProperty(
                    "chunk_size_lower_bound_in_compaction",
                    Long.toString(conf.getChunkSizeLowerBoundInCompaction()))
                .trim()));
    conf.setMaxInnerCompactionCandidateFileNum(
        Integer.parseInt(
            properties
                .getProperty(
                    "max_inner_compaction_candidate_file_num",
                    Integer.toString(conf.getMaxInnerCompactionCandidateFileNum()))
                .trim()));
    conf.setMaxCrossCompactionCandidateFileNum(
        Integer.parseInt(
            properties
                .getProperty(
                    "max_cross_compaction_candidate_file_num",
                    Integer.toString(conf.getMaxCrossCompactionCandidateFileNum()))
                .trim()));
    conf.setMaxCrossCompactionCandidateFileSize(
        Long.parseLong(
            properties
                .getProperty(
                    "max_cross_compaction_candidate_file_size",
                    Long.toString(conf.getMaxCrossCompactionCandidateFileSize()))
                .trim()));

    conf.setCompactionWriteThroughputMbPerSec(
        Integer.parseInt(
            properties
                .getProperty(
                    "compaction_write_throughput_mb_per_sec",
                    Integer.toString(conf.getCompactionWriteThroughputMbPerSec()))
                .trim()));

    conf.setEnablePartialInsert(
        Boolean.parseBoolean(
            properties
                .getProperty("enable_partial_insert", String.valueOf(conf.isEnablePartialInsert()))
                .trim()));

    int rpcSelectorThreadNum =
        Integer.parseInt(
            properties
                .getProperty(
                    "rpc_selector_thread_count", Integer.toString(conf.getRpcSelectorThreadCount()))
                .trim());
    if (rpcSelectorThreadNum <= 0) {
      rpcSelectorThreadNum = 1;
    }

    conf.setRpcSelectorThreadCount(rpcSelectorThreadNum);

    int minConcurrentClientNum =
        Integer.parseInt(
            properties
                .getProperty(
                    "rpc_min_concurrent_client_num",
                    Integer.toString(conf.getRpcMinConcurrentClientNum()))
                .trim());
    if (minConcurrentClientNum <= 0) {
      minConcurrentClientNum = Runtime.getRuntime().availableProcessors();
    }

    conf.setRpcMinConcurrentClientNum(minConcurrentClientNum);

    int maxConcurrentClientNum =
        Integer.parseInt(
            properties
                .getProperty(
                    "rpc_max_concurrent_client_num",
                    Integer.toString(conf.getRpcMaxConcurrentClientNum()))
                .trim());
    if (maxConcurrentClientNum <= 0) {
      maxConcurrentClientNum = 65535;
    }

    conf.setRpcMaxConcurrentClientNum(maxConcurrentClientNum);

    conf.setEnableWatermark(
        Boolean.parseBoolean(
            properties
                .getProperty("watermark_module_opened", Boolean.toString(conf.isEnableWatermark()))
                .trim()));
    conf.setWatermarkSecretKey(
        properties.getProperty("watermark_secret_key", conf.getWatermarkSecretKey()).trim());
    conf.setWatermarkBitString(
        properties.getProperty("watermark_bit_string", conf.getWatermarkBitString()).trim());
    conf.setWatermarkMethod(
        properties.getProperty("watermark_method", conf.getWatermarkMethod()).trim());

    loadAutoCreateSchemaProps(properties);

    conf.setTsFileStorageFs(
        properties.getProperty("tsfile_storage_fs", conf.getTsFileStorageFs().toString()).trim());
    conf.setCoreSitePath(properties.getProperty("core_site_path", conf.getCoreSitePath()).trim());
    conf.setHdfsSitePath(properties.getProperty("hdfs_site_path", conf.getHdfsSitePath()).trim());
    conf.setHdfsIp(properties.getProperty("hdfs_ip", conf.getRawHDFSIp()).trim().split(","));
    conf.setHdfsPort(properties.getProperty("hdfs_port", conf.getHdfsPort()).trim());
    conf.setDfsNameServices(
        properties.getProperty("dfs_nameservices", conf.getDfsNameServices()).trim());
    conf.setDfsHaNamenodes(
        properties.getProperty("dfs_ha_namenodes", conf.getRawDfsHaNamenodes()).trim().split(","));
    conf.setDfsHaAutomaticFailoverEnabled(
        Boolean.parseBoolean(
            properties
                .getProperty(
                    "dfs_ha_automatic_failover_enabled",
                    String.valueOf(conf.isDfsHaAutomaticFailoverEnabled()))
                .trim()));
    conf.setDfsClientFailoverProxyProvider(
        properties
            .getProperty(
                "dfs_client_failover_proxy_provider", conf.getDfsClientFailoverProxyProvider())
            .trim());
    conf.setUseKerberos(
        Boolean.parseBoolean(
            properties
                .getProperty("hdfs_use_kerberos", String.valueOf(conf.isUseKerberos()))
                .trim()));
    conf.setKerberosKeytabFilePath(
        properties
            .getProperty("kerberos_keytab_file_path", conf.getKerberosKeytabFilePath())
            .trim());
    conf.setKerberosPrincipal(
        properties.getProperty("kerberos_principal", conf.getKerberosPrincipal()).trim());

    // the num of memtables in each storage group
    conf.setConcurrentWritingTimePartition(
        Integer.parseInt(
            properties
                .getProperty(
                    "concurrent_writing_time_partition",
                    String.valueOf(conf.getConcurrentWritingTimePartition()))
                .trim()));

    // the default fill interval in LinearFill and PreviousFill
    conf.setDefaultFillInterval(
        Integer.parseInt(
            properties
                .getProperty("default_fill_interval", String.valueOf(conf.getDefaultFillInterval()))
                .trim()));

    conf.setTagAttributeTotalSize(
        Integer.parseInt(
            properties
                .getProperty(
                    "tag_attribute_total_size", String.valueOf(conf.getTagAttributeTotalSize()))
                .trim()));

    conf.setTagAttributeFlushInterval(
        Integer.parseInt(
            properties
                .getProperty(
                    "tag_attribute_flush_interval",
                    String.valueOf(conf.getTagAttributeFlushInterval()))
                .trim()));

    conf.setPrimitiveArraySize(
        (Integer.parseInt(
            properties
                .getProperty("primitive_array_size", String.valueOf(conf.getPrimitiveArraySize()))
                .trim())));

    conf.setThriftMaxFrameSize(
        Integer.parseInt(
            properties
                .getProperty("thrift_max_frame_size", String.valueOf(conf.getThriftMaxFrameSize()))
                .trim()));

    if (conf.getThriftMaxFrameSize() < IoTDBConstant.LEFT_SIZE_IN_REQUEST * 2) {
      conf.setThriftMaxFrameSize(IoTDBConstant.LEFT_SIZE_IN_REQUEST * 2);
    }

    conf.setThriftDefaultBufferSize(
        Integer.parseInt(
            properties
                .getProperty(
                    "thrift_init_buffer_size", String.valueOf(conf.getThriftDefaultBufferSize()))
                .trim()));

    conf.setFrequencyIntervalInMinute(
        Integer.parseInt(
            properties
                .getProperty(
                    "frequency_interval_in_minute",
                    String.valueOf(conf.getFrequencyIntervalInMinute()))
                .trim()));

    conf.setSlowQueryThreshold(
        Long.parseLong(
            properties
                .getProperty("slow_query_threshold", String.valueOf(conf.getSlowQueryThreshold()))
                .trim()));

    conf.setDataRegionNum(
        Integer.parseInt(
            properties
                .getProperty("data_region_num", String.valueOf(conf.getDataRegionNum()))
                .trim()));

    conf.setRecoveryLogIntervalInMs(
        Long.parseLong(
            properties
                .getProperty(
                    "recovery_log_interval_in_ms",
                    String.valueOf(conf.getRecoveryLogIntervalInMs()))
                .trim()));

    conf.setEnableDiscardOutOfOrderData(
        Boolean.parseBoolean(
            properties
                .getProperty(
                    "enable_discard_out_of_order_data",
                    Boolean.toString(conf.isEnableDiscardOutOfOrderData()))
                .trim()));

    conf.setWindowEvaluationThreadCount(
        Integer.parseInt(
            properties
                .getProperty(
                    "window_evaluation_thread_count",
                    Integer.toString(conf.getWindowEvaluationThreadCount()))
                .trim()));
    if (conf.getWindowEvaluationThreadCount() <= 0) {
      conf.setWindowEvaluationThreadCount(Runtime.getRuntime().availableProcessors());
    }

    conf.setMaxPendingWindowEvaluationTasks(
        Integer.parseInt(
            properties
                .getProperty(
                    "max_pending_window_evaluation_tasks",
                    Integer.toString(conf.getMaxPendingWindowEvaluationTasks()))
                .trim()));
    if (conf.getMaxPendingWindowEvaluationTasks() <= 0) {
      conf.setMaxPendingWindowEvaluationTasks(64);
    }

    // id table related configuration
    conf.setDeviceIDTransformationMethod(
        properties
            .getProperty("device_id_transformation_method", conf.getDeviceIDTransformationMethod())
            .trim());

    conf.setEnableIDTable(
        Boolean.parseBoolean(
            properties
                .getProperty("enable_id_table", String.valueOf(conf.isEnableIDTable()))
                .trim()));

    conf.setEnableIDTableLogFile(
        Boolean.parseBoolean(
            properties
                .getProperty(
                    "enable_id_table_log_file", String.valueOf(conf.isEnableIDTableLogFile()))
                .trim()));

    conf.setSchemaEngineMode(
        properties
            .getProperty("schema_engine_mode", String.valueOf(conf.getSchemaEngineMode()))
            .trim());

    conf.setEnableLastCache(
        Boolean.parseBoolean(
            properties
                .getProperty("enable_last_cache", Boolean.toString(conf.isLastCacheEnabled()))
                .trim()));

    if (conf.getSchemaEngineMode().equals("Rocksdb_based")) {
      conf.setEnableLastCache(false);
    }

    conf.setCachedMNodeSizeInSchemaFileMode(
        Integer.parseInt(
            properties
                .getProperty(
                    "cached_mnode_size_in_schema_file_mode",
                    String.valueOf(conf.getCachedMNodeSizeInSchemaFileMode()))
                .trim()));

    conf.setMinimumSegmentInSchemaFile(
        Short.parseShort(
            properties
                .getProperty(
                    "minimum_schema_file_segment_in_bytes",
                    String.valueOf(conf.getMinimumSegmentInSchemaFile()))
                .trim()));

    conf.setPageCacheSizeInSchemaFile(
        Integer.parseInt(
            properties
                .getProperty(
                    "page_cache_in_schema_file",
                    String.valueOf(conf.getPageCacheSizeInSchemaFile()))
                .trim()));

    conf.setSchemaFileLogSize(
        Integer.parseInt(
            properties
                .getProperty("schema_file_log_size", String.valueOf(conf.getSchemaFileLogSize()))
                .trim()));

    // mqtt
    loadMqttProps(properties);

    conf.setEnablePartition(
        Boolean.parseBoolean(
            properties
                .getProperty("enable_partition", String.valueOf(conf.isEnablePartition()))
                .trim()));

    conf.setTimePartitionIntervalForStorage(
        Long.parseLong(
            properties
                .getProperty(
                    "time_partition_interval_for_storage",
                    String.valueOf(conf.getTimePartitionIntervalForStorage()))
                .trim()));

    conf.setSelectIntoInsertTabletPlanRowLimit(
        Integer.parseInt(
            properties
                .getProperty(
                    "select_into_insert_tablet_plan_row_limit",
                    String.valueOf(conf.getSelectIntoInsertTabletPlanRowLimit()))
                .trim()));

    conf.setExtPipeDir(properties.getProperty("ext_pipe_dir", conf.getExtPipeDir()).trim());

    conf.setInsertMultiTabletEnableMultithreadingColumnThreshold(
        Integer.parseInt(
            properties
                .getProperty(
                    "insert_multi_tablet_enable_multithreading_column_threshold",
                    String.valueOf(conf.getInsertMultiTabletEnableMultithreadingColumnThreshold()))
                .trim()));

    // At the same time, set TSFileConfig
    TSFileDescriptor.getInstance()
        .getConfig()
        .setTSFileStorageFs(
            FSType.valueOf(
                properties
                    .getProperty("tsfile_storage_fs", conf.getTsFileStorageFs().name())
                    .trim()));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setCoreSitePath(properties.getProperty("core_site_path", conf.getCoreSitePath()).trim());
    TSFileDescriptor.getInstance()
        .getConfig()
        .setHdfsSitePath(properties.getProperty("hdfs_site_path", conf.getHdfsSitePath()).trim());
    TSFileDescriptor.getInstance()
        .getConfig()
        .setHdfsIp(properties.getProperty("hdfs_ip", conf.getRawHDFSIp()).trim().split(","));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setHdfsPort(properties.getProperty("hdfs_port", conf.getHdfsPort()).trim());
    TSFileDescriptor.getInstance()
        .getConfig()
        .setDfsNameServices(
            properties.getProperty("dfs_nameservices", conf.getDfsNameServices()).trim());
    TSFileDescriptor.getInstance()
        .getConfig()
        .setDfsHaNamenodes(
            properties
                .getProperty("dfs_ha_namenodes", conf.getRawDfsHaNamenodes())
                .trim()
                .split(","));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setDfsHaAutomaticFailoverEnabled(
            Boolean.parseBoolean(
                properties
                    .getProperty(
                        "dfs_ha_automatic_failover_enabled",
                        String.valueOf(conf.isDfsHaAutomaticFailoverEnabled()))
                    .trim()));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setDfsClientFailoverProxyProvider(
            properties
                .getProperty(
                    "dfs_client_failover_proxy_provider", conf.getDfsClientFailoverProxyProvider())
                .trim());
    TSFileDescriptor.getInstance()
        .getConfig()
        .setPatternMatchingThreshold(
            Integer.parseInt(
                properties
                    .getProperty(
                        "pattern_matching_threshold",
                        String.valueOf(conf.getPatternMatchingThreshold()))
                    .trim()));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setUseKerberos(
            Boolean.parseBoolean(
                properties
                    .getProperty("hdfs_use_kerberos", String.valueOf(conf.isUseKerberos()))
                    .trim()));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setKerberosKeytabFilePath(
            properties
                .getProperty("kerberos_keytab_file_path", conf.getKerberosKeytabFilePath())
                .trim());
    TSFileDescriptor.getInstance()
        .getConfig()
        .setKerberosPrincipal(
            properties.getProperty("kerberos_principal", conf.getKerberosPrincipal()).trim());
    TSFileDescriptor.getInstance().getConfig().setBatchSize(conf.getBatchSize());

    conf.setCoordinatorReadExecutorSize(
        Integer.parseInt(
            properties
                .getProperty(
                    "coordinator_read_executor_size",
                    Integer.toString(conf.getCoordinatorReadExecutorSize()))
                .trim()));
    conf.setCoordinatorWriteExecutorSize(
        Integer.parseInt(
            properties
                .getProperty(
                    "coordinator_write_executor_size",
                    Integer.toString(conf.getCoordinatorWriteExecutorSize()))
                .trim()));

    // commons
    commonDescriptor.loadCommonProps(properties);
    commonDescriptor.initCommonConfigDir(conf.getSystemDir());

    // timed flush memtable
    loadTimedService(properties);

    // set tsfile-format config
    loadTsFileProps(properties);

    // make RPCTransportFactory taking effect.
    RpcTransportFactory.reInit();

    // UDF
    loadUDFProps(properties);

    // trigger
    loadTriggerProps(properties);

    // CQ
    loadCQProps(properties);

    // cluster
    loadClusterProps(properties);

    // shuffle
    loadShuffleProps(properties);

    // author cache
    loadAuthorCache(properties);

    conf.setTimePartitionIntervalForStorage(
        DateTimeUtils.convertMilliTimeWithPrecision(
            conf.getTimePartitionIntervalForStorage(), conf.getTimestampPrecision()));

    if (!conf.isClusterMode()) {
      conf.setTimePartitionIntervalForRouting(conf.getTimePartitionIntervalForStorage());
    }
  }

  private void loadAuthorCache(Properties properties) {
    conf.setAuthorCacheSize(
        Integer.parseInt(
            properties
                .getProperty("author_cache_size", String.valueOf(conf.getAuthorCacheSize()))
                .trim()));
    conf.setAuthorCacheExpireTime(
        Integer.parseInt(
            properties
                .getProperty(
                    "author_cache_expire_time", String.valueOf(conf.getAuthorCacheExpireTime()))
                .trim()));
  }

  // to keep consistent with the cluster module.
  private void replaceHostnameWithIP() throws UnknownHostException, BadNodeUrlFormatException {
    boolean isInvalidRpcIp = InetAddresses.isInetAddress(conf.getRpcAddress());
    if (!isInvalidRpcIp) {
      conf.setRpcAddress(InetAddress.getByName(conf.getRpcAddress()).getHostAddress());
    }

    boolean isInvalidInternalIp = InetAddresses.isInetAddress(conf.getInternalAddress());
    if (!isInvalidInternalIp) {
      conf.setInternalAddress(InetAddress.getByName(conf.getInternalAddress()).getHostAddress());
    }

    for (TEndPoint configNode : conf.getTargetConfigNodeList()) {
      boolean isInvalidNodeIp = InetAddresses.isInetAddress(configNode.ip);
      if (!isInvalidNodeIp) {
        String newNodeIP = InetAddress.getByName(configNode.ip).getHostAddress();
        configNode.setIp(newNodeIP);
      }
    }

    logger.debug(
        "after replace, the rpcIP={}, internalIP={}, configNodeUrls={}",
        conf.getRpcAddress(),
        conf.getInternalAddress(),
        conf.getTargetConfigNodeList());
  }

  private void loadWALProps(Properties properties) {
    conf.setWalMode(
        WALMode.valueOf((properties.getProperty("wal_mode", conf.getWalMode().toString()).trim())));

    int maxWalNodesNum =
        Integer.parseInt(
            properties
                .getProperty("max_wal_nodes_num", Integer.toString(conf.getMaxWalNodesNum()))
                .trim());
    if (maxWalNodesNum > 0) {
      conf.setMaxWalNodesNum(maxWalNodesNum);
    }

    int walBufferSize =
        Integer.parseInt(
            properties
                .getProperty("wal_buffer_size_in_byte", Integer.toString(conf.getWalBufferSize()))
                .trim());
    if (walBufferSize > 0) {
      conf.setWalBufferSize(walBufferSize);
    }

    int walBufferEntrySize =
        Integer.parseInt(
            properties
                .getProperty(
                    "wal_buffer_entry_size_in_byte", Integer.toString(conf.getWalBufferEntrySize()))
                .trim());
    if (walBufferEntrySize > 0) {
      conf.setWalBufferEntrySize(walBufferEntrySize);
    }

    int walBufferQueueCapacity =
        Integer.parseInt(
            properties
                .getProperty(
                    "wal_buffer_queue_capacity", Integer.toString(conf.getWalBufferQueueCapacity()))
                .trim());
    if (walBufferQueueCapacity > 0) {
      conf.setWalBufferQueueCapacity(walBufferQueueCapacity);
    }

    loadWALHotModifiedProps(properties);
  }

  private void loadWALHotModifiedProps(Properties properties) {
    long fsyncWalDelayInMs =
        Long.parseLong(
            properties
                .getProperty("fsync_wal_delay_in_ms", Long.toString(conf.getFsyncWalDelayInMs()))
                .trim());
    if (fsyncWalDelayInMs > 0) {
      conf.setFsyncWalDelayInMs(fsyncWalDelayInMs);
    }

    long walFileSizeThreshold =
        Long.parseLong(
            properties
                .getProperty(
                    "wal_file_size_threshold_in_byte",
                    Long.toString(conf.getWalFileSizeThresholdInByte()))
                .trim());
    if (walFileSizeThreshold > 0) {
      conf.setWalFileSizeThresholdInByte(walFileSizeThreshold);
    }

    double walMinEffectiveInfoRatio =
        Double.parseDouble(
            properties
                .getProperty(
                    "wal_min_effective_info_ratio",
                    Double.toString(conf.getWalMinEffectiveInfoRatio()))
                .trim());
    if (walMinEffectiveInfoRatio > 0) {
      conf.setWalMinEffectiveInfoRatio(walMinEffectiveInfoRatio);
    }

    long walMemTableSnapshotThreshold =
        Long.parseLong(
            properties
                .getProperty(
                    "wal_memtable_snapshot_threshold_in_byte",
                    Long.toString(conf.getWalMemTableSnapshotThreshold()))
                .trim());
    if (walMemTableSnapshotThreshold > 0) {
      conf.setWalMemTableSnapshotThreshold(walMemTableSnapshotThreshold);
    }

    int maxWalMemTableSnapshotNum =
        Integer.parseInt(
            properties
                .getProperty(
                    "max_wal_memtable_snapshot_num",
                    Integer.toString(conf.getMaxWalMemTableSnapshotNum()))
                .trim());
    if (maxWalMemTableSnapshotNum > 0) {
      conf.setMaxWalMemTableSnapshotNum(maxWalMemTableSnapshotNum);
    }

    long deleteWalFilesPeriod =
        Long.parseLong(
            properties
                .getProperty(
                    "delete_wal_files_period_in_ms",
                    Long.toString(conf.getDeleteWalFilesPeriodInMs()))
                .trim());
    if (deleteWalFilesPeriod > 0) {
      conf.setDeleteWalFilesPeriodInMs(deleteWalFilesPeriod);
    }

    long throttleDownThresholdInByte =
        Long.parseLong(
            properties
                .getProperty(
                    "multi_leader_throttle_threshold_in_byte",
                    Long.toString(conf.getThrottleThreshold()))
                .trim());
    if (throttleDownThresholdInByte > 0) {
      conf.setThrottleThreshold(throttleDownThresholdInByte);
    }

    long cacheWindowInMs =
        Long.parseLong(
            properties
                .getProperty(
                    "multi_leader_cache_window_time_in_ms",
                    Long.toString(conf.getCacheWindowTimeInMs()))
                .trim());
    if (cacheWindowInMs > 0) {
      conf.setCacheWindowTimeInMs(cacheWindowInMs);
    }
  }

  private void loadAutoCreateSchemaProps(Properties properties) {
    conf.setAutoCreateSchemaEnabled(
        Boolean.parseBoolean(
            properties
                .getProperty(
                    "enable_auto_create_schema", Boolean.toString(conf.isAutoCreateSchemaEnabled()))
                .trim()));
    conf.setBooleanStringInferType(
        TSDataType.valueOf(
            properties
                .getProperty(
                    "boolean_string_infer_type", conf.getBooleanStringInferType().toString())
                .trim()));
    conf.setIntegerStringInferType(
        TSDataType.valueOf(
            properties
                .getProperty(
                    "integer_string_infer_type", conf.getIntegerStringInferType().toString())
                .trim()));
    conf.setLongStringInferType(
        TSDataType.valueOf(
            properties
                .getProperty("long_string_infer_type", conf.getLongStringInferType().toString())
                .trim()));
    conf.setFloatingStringInferType(
        TSDataType.valueOf(
            properties
                .getProperty(
                    "floating_string_infer_type", conf.getFloatingStringInferType().toString())
                .trim()));
    conf.setNanStringInferType(
        TSDataType.valueOf(
            properties
                .getProperty("nan_string_infer_type", conf.getNanStringInferType().toString())
                .trim()));
    conf.setDefaultStorageGroupLevel(
        Integer.parseInt(
            properties
                .getProperty(
                    "default_storage_group_level",
                    Integer.toString(conf.getDefaultStorageGroupLevel()))
                .trim()));
    conf.setDefaultBooleanEncoding(
        properties
            .getProperty("default_boolean_encoding", conf.getDefaultBooleanEncoding().toString())
            .trim());
    conf.setDefaultInt32Encoding(
        properties
            .getProperty("default_int32_encoding", conf.getDefaultInt32Encoding().toString())
            .trim());
    conf.setDefaultInt64Encoding(
        properties
            .getProperty("default_int64_encoding", conf.getDefaultInt64Encoding().toString())
            .trim());
    conf.setDefaultFloatEncoding(
        properties
            .getProperty("default_float_encoding", conf.getDefaultFloatEncoding().toString())
            .trim());
    conf.setDefaultDoubleEncoding(
        properties
            .getProperty("default_double_encoding", conf.getDefaultDoubleEncoding().toString())
            .trim());
    conf.setDefaultTextEncoding(
        properties
            .getProperty("default_text_encoding", conf.getDefaultTextEncoding().toString())
            .trim());
  }

  private void loadTsFileProps(Properties properties) {
    TSFileDescriptor.getInstance()
        .getConfig()
        .setGroupSizeInByte(
            Integer.parseInt(
                properties
                    .getProperty(
                        "group_size_in_byte",
                        Integer.toString(
                            TSFileDescriptor.getInstance().getConfig().getGroupSizeInByte()))
                    .trim()));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setPageSizeInByte(
            Integer.parseInt(
                properties
                    .getProperty(
                        "page_size_in_byte",
                        Integer.toString(
                            TSFileDescriptor.getInstance().getConfig().getPageSizeInByte()))
                    .trim()));
    if (TSFileDescriptor.getInstance().getConfig().getPageSizeInByte()
        > TSFileDescriptor.getInstance().getConfig().getGroupSizeInByte()) {
      logger.warn("page_size is greater than group size, will set it as the same with group size");
      TSFileDescriptor.getInstance()
          .getConfig()
          .setPageSizeInByte(TSFileDescriptor.getInstance().getConfig().getGroupSizeInByte());
    }
    TSFileDescriptor.getInstance()
        .getConfig()
        .setMaxNumberOfPointsInPage(
            Integer.parseInt(
                properties
                    .getProperty(
                        "max_number_of_points_in_page",
                        Integer.toString(
                            TSFileDescriptor.getInstance()
                                .getConfig()
                                .getMaxNumberOfPointsInPage()))
                    .trim()));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setMaxStringLength(
            Integer.parseInt(
                properties
                    .getProperty(
                        "max_string_length",
                        Integer.toString(
                            TSFileDescriptor.getInstance().getConfig().getMaxStringLength()))
                    .trim()));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setBloomFilterErrorRate(
            Double.parseDouble(
                properties
                    .getProperty(
                        "bloom_filter_error_rate",
                        Double.toString(
                            TSFileDescriptor.getInstance().getConfig().getBloomFilterErrorRate()))
                    .trim()));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setFloatPrecision(
            Integer.parseInt(
                properties
                    .getProperty(
                        "float_precision",
                        Integer.toString(
                            TSFileDescriptor.getInstance().getConfig().getFloatPrecision()))
                    .trim()));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setTimeEncoder(
            properties
                .getProperty(
                    "time_encoder", TSFileDescriptor.getInstance().getConfig().getTimeEncoder())
                .trim());
    TSFileDescriptor.getInstance()
        .getConfig()
        .setValueEncoder(
            properties
                .getProperty(
                    "value_encoder", TSFileDescriptor.getInstance().getConfig().getValueEncoder())
                .trim());
    TSFileDescriptor.getInstance()
        .getConfig()
        .setCompressor(
            properties
                .getProperty(
                    "compressor",
                    TSFileDescriptor.getInstance().getConfig().getCompressor().toString())
                .trim());
    TSFileDescriptor.getInstance()
        .getConfig()
        .setMaxDegreeOfIndexNode(
            Integer.parseInt(
                properties
                    .getProperty(
                        "max_degree_of_index_node",
                        Integer.toString(
                            TSFileDescriptor.getInstance().getConfig().getMaxDegreeOfIndexNode()))
                    .trim()));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setMaxTsBlockSizeInBytes(
            Integer.parseInt(
                properties
                    .getProperty(
                        "max_tsblock_size_in_bytes",
                        Integer.toString(
                            TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes()))
                    .trim()));

    // min(default_size, maxBytesForQuery)
    TSFileDescriptor.getInstance()
        .getConfig()
        .setMaxTsBlockSizeInBytes(
            (int)
                Math.min(
                    TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes(),
                    conf.getMaxBytesPerQuery()));

    TSFileDescriptor.getInstance()
        .getConfig()
        .setMaxTsBlockLineNumber(
            Integer.parseInt(
                properties
                    .getProperty(
                        "max_tsblock_line_number",
                        Integer.toString(
                            TSFileDescriptor.getInstance().getConfig().getMaxTsBlockLineNumber()))
                    .trim()));
  }

  // Mqtt related
  private void loadMqttProps(Properties properties) {
    conf.setMqttDir(properties.getProperty("mqtt_root_dir", conf.getMqttDir()).trim());

    if (properties.getProperty(IoTDBConstant.MQTT_HOST_NAME) != null) {
      conf.setMqttHost(properties.getProperty(IoTDBConstant.MQTT_HOST_NAME).trim());
    }

    if (properties.getProperty(IoTDBConstant.MQTT_PORT_NAME) != null) {
      conf.setMqttPort(
          Integer.parseInt(properties.getProperty(IoTDBConstant.MQTT_PORT_NAME).trim()));
    }

    if (properties.getProperty(IoTDBConstant.MQTT_HANDLER_POOL_SIZE_NAME) != null) {
      conf.setMqttHandlerPoolSize(
          Integer.parseInt(
              properties.getProperty(IoTDBConstant.MQTT_HANDLER_POOL_SIZE_NAME).trim()));
    }

    if (properties.getProperty(IoTDBConstant.MQTT_PAYLOAD_FORMATTER_NAME) != null) {
      conf.setMqttPayloadFormatter(
          properties.getProperty(IoTDBConstant.MQTT_PAYLOAD_FORMATTER_NAME).trim());
    }

    if (properties.getProperty(IoTDBConstant.ENABLE_MQTT) != null) {
      conf.setEnableMQTTService(
          Boolean.parseBoolean(properties.getProperty(IoTDBConstant.ENABLE_MQTT).trim()));
    }

    if (properties.getProperty(IoTDBConstant.MQTT_MAX_MESSAGE_SIZE) != null) {
      conf.setMqttMaxMessageSize(
          Integer.parseInt(properties.getProperty(IoTDBConstant.MQTT_MAX_MESSAGE_SIZE).trim()));
    }
  }

  // timed flush memtable
  private void loadTimedService(Properties properties) {
    conf.setEnableTimedFlushSeqMemtable(
        Boolean.parseBoolean(
            properties
                .getProperty(
                    "enable_timed_flush_seq_memtable",
                    Boolean.toString(conf.isEnableTimedFlushSeqMemtable()))
                .trim()));

    long seqMemTableFlushInterval =
        Long.parseLong(
            properties
                .getProperty(
                    "seq_memtable_flush_interval_in_ms",
                    Long.toString(conf.getSeqMemtableFlushInterval()))
                .trim());
    if (seqMemTableFlushInterval > 0) {
      conf.setSeqMemtableFlushInterval(seqMemTableFlushInterval);
    }

    long seqMemTableFlushCheckInterval =
        Long.parseLong(
            properties
                .getProperty(
                    "seq_memtable_flush_check_interval_in_ms",
                    Long.toString(conf.getSeqMemtableFlushCheckInterval()))
                .trim());
    if (seqMemTableFlushCheckInterval > 0) {
      conf.setSeqMemtableFlushCheckInterval(seqMemTableFlushCheckInterval);
    }

    conf.setEnableTimedFlushUnseqMemtable(
        Boolean.parseBoolean(
            properties
                .getProperty(
                    "enable_timed_flush_unseq_memtable",
                    Boolean.toString(conf.isEnableTimedFlushUnseqMemtable()))
                .trim()));

    long unseqMemTableFlushInterval =
        Long.parseLong(
            properties
                .getProperty(
                    "unseq_memtable_flush_interval_in_ms",
                    Long.toString(conf.getUnseqMemtableFlushInterval()))
                .trim());
    if (unseqMemTableFlushInterval > 0) {
      conf.setUnseqMemtableFlushInterval(unseqMemTableFlushInterval);
    }

    long unseqMemTableFlushCheckInterval =
        Long.parseLong(
            properties
                .getProperty(
                    "unseq_memtable_flush_check_interval_in_ms",
                    Long.toString(conf.getUnseqMemtableFlushCheckInterval()))
                .trim());
    if (unseqMemTableFlushCheckInterval > 0) {
      conf.setUnseqMemtableFlushCheckInterval(unseqMemTableFlushCheckInterval);
    }
  }

  public void loadHotModifiedProps(Properties properties) throws QueryProcessException {
    try {
      // update data dirs
      String dataDirs = properties.getProperty("dn_data_dirs", null);
      if (dataDirs != null) {
        conf.reloadDataDirs(dataDirs.trim().split(","));
      }

      // update dir strategy, must update after data dirs
      String multiDirStrategyClassName = properties.getProperty("dn_multi_dir_strategy", null);
      if (multiDirStrategyClassName != null
          && !multiDirStrategyClassName.equals(conf.getMultiDirStrategyClassName())) {
        conf.setMultiDirStrategyClassName(multiDirStrategyClassName.trim());
        conf.confirmMultiDirStrategy();
        DirectoryManager.getInstance().updateDirectoryStrategy();
      }

      // update timed flush & close conf
      loadTimedService(properties);
      StorageEngine.getInstance().rebootTimedService();

      long seqTsFileSize =
          Long.parseLong(
              properties
                  .getProperty("seq_tsfile_size", Long.toString(conf.getSeqTsFileSize()))
                  .trim());
      if (seqTsFileSize >= 0) {
        conf.setSeqTsFileSize(seqTsFileSize);
      }

      long unSeqTsFileSize =
          Long.parseLong(
              properties
                  .getProperty("unseq_tsfile_size", Long.toString(conf.getUnSeqTsFileSize()))
                  .trim());
      if (unSeqTsFileSize >= 0) {
        conf.setUnSeqTsFileSize(unSeqTsFileSize);
      }

      long memTableSizeThreshold =
          Long.parseLong(
              properties
                  .getProperty(
                      "memtable_size_threshold", Long.toString(conf.getMemtableSizeThreshold()))
                  .trim());
      if (memTableSizeThreshold > 0) {
        conf.setMemtableSizeThreshold(memTableSizeThreshold);
      }

      // update params of creating schema automatically
      loadAutoCreateSchemaProps(properties);

      // update tsfile-format config
      loadTsFileProps(properties);

      // update max_deduplicated_path_num
      conf.setMaxQueryDeduplicatedPathNum(
          Integer.parseInt(
              properties
                  .getProperty(
                      "max_deduplicated_path_num",
                      Integer.toString(conf.getMaxQueryDeduplicatedPathNum()))
                  .trim()));
      // update frequency_interval_in_minute
      conf.setFrequencyIntervalInMinute(
          Integer.parseInt(
              properties
                  .getProperty(
                      "frequency_interval_in_minute",
                      Integer.toString(conf.getFrequencyIntervalInMinute()))
                  .trim()));
      // update slow_query_threshold
      conf.setSlowQueryThreshold(
          Long.parseLong(
              properties
                  .getProperty("slow_query_threshold", Long.toString(conf.getSlowQueryThreshold()))
                  .trim()));
      // update merge_write_throughput_mb_per_sec
      conf.setCompactionWriteThroughputMbPerSec(
          Integer.parseInt(
              properties
                  .getProperty(
                      "merge_write_throughput_mb_per_sec",
                      Integer.toString(conf.getCompactionWriteThroughputMbPerSec()))
                  .trim()));

      // update insert-tablet-plan's row limit for select-into
      conf.setSelectIntoInsertTabletPlanRowLimit(
          Integer.parseInt(
              properties
                  .getProperty(
                      "select_into_insert_tablet_plan_row_limit",
                      String.valueOf(conf.getSelectIntoInsertTabletPlanRowLimit()))
                  .trim()));

      // update sync config
      conf.setMaxNumberOfSyncFileRetry(
          Integer.parseInt(
              properties
                  .getProperty(
                      "max_number_of_sync_file_retry",
                      Integer.toString(conf.getMaxNumberOfSyncFileRetry()))
                  .trim()));
      conf.setIpWhiteList(properties.getProperty("ip_white_list", conf.getIpWhiteList()).trim());

      // update enable query memory estimation for memory control
      conf.setEnableQueryMemoryEstimation(
          Boolean.parseBoolean(
              properties
                  .getProperty(
                      "enable_query_memory_estimation",
                      Boolean.toString(conf.isEnableQueryMemoryEstimation()))
                  .trim()));

      // update wal config
      long prevDeleteWalFilesPeriodInMs = conf.getDeleteWalFilesPeriodInMs();
      loadWALHotModifiedProps(properties);
      if (prevDeleteWalFilesPeriodInMs != conf.getDeleteWalFilesPeriodInMs()) {
        WALManager.getInstance().rebootWALDeleteThread();
      }
    } catch (Exception e) {
      throw new QueryProcessException(String.format("Fail to reload configuration because %s", e));
    }
  }

  public void loadHotModifiedProps() throws QueryProcessException {
    URL url = getPropsUrl(CommonConfig.CONFIG_NAME);
    if (url == null) {
      logger.warn("Couldn't load the configuration from any of the known sources.");
      return;
    }

    Properties commonProperties = new Properties();
    try (InputStream inputStream = url.openStream()) {
      logger.info("Start to reload config file {}", url);
      commonProperties.load(inputStream);
    } catch (Exception e) {
      logger.warn("Fail to reload config file {}", url, e);
      throw new QueryProcessException(
          String.format("Fail to reload config file %s because %s", url, e.getMessage()));
    }

    url = getPropsUrl(IoTDBConfig.CONFIG_NAME);
    if (url == null) {
      logger.warn("Couldn't load the configuration from any of the known sources.");
      return;
    }
    try (InputStream inputStream = url.openStream()) {
      logger.info("Start to reload config file {}", url);
      Properties properties = new Properties();
      properties.load(inputStream);
      commonProperties.putAll(properties);
      loadHotModifiedProps(commonProperties);
    } catch (Exception e) {
      logger.warn("Fail to reload config file {}", url, e);
      throw new QueryProcessException(
          String.format("Fail to reload config file %s because %s", url, e.getMessage()));
    }
    ReloadLevel reloadLevel = MetricConfigDescriptor.getInstance().loadHotProps();
    MetricService.getInstance().reloadProperties(reloadLevel);
  }

  private void initMemoryAllocate(Properties properties) {
    String memoryAllocateProportion =
        properties.getProperty("write_read_schema_free_memory_proportion");
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
      }
    }

    logger.info("initial allocateMemoryForRead = {}", conf.getAllocateMemoryForRead());
    logger.info("initial allocateMemoryForWrite = {}", conf.getAllocateMemoryForStorageEngine());
    logger.info("initial allocateMemoryForSchema = {}", conf.getAllocateMemoryForSchema());
    logger.info("initial allocateMemoryForConsensus = {}", conf.getAllocateMemoryForConsensus());

    initSchemaMemoryAllocate(properties);
    initStorageEngineAllocate(properties);

    conf.setMaxQueryDeduplicatedPathNum(
        Integer.parseInt(
            properties
                .getProperty(
                    "max_deduplicated_path_num",
                    Integer.toString(conf.getMaxQueryDeduplicatedPathNum()))
                .trim()));

    conf.setEnableQueryMemoryEstimation(
        Boolean.parseBoolean(
            properties
                .getProperty(
                    "enable_query_memory_estimation",
                    Boolean.toString(conf.isEnableQueryMemoryEstimation()))
                .trim()));

    String queryMemoryAllocateProportion =
        properties.getProperty("chunk_timeseriesmeta_free_memory_proportion").trim();
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
                  + queryMemoryAllocateProportion);
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

  private void initStorageEngineAllocate(Properties properties) {
    String allocationRatio =
        properties.getProperty("storage_engine_memory_proportion", "8:2").trim();
    String[] proportions = allocationRatio.split(":");
    int proportionForMemTable = Integer.parseInt(proportions[0].trim());
    int proportionForCompaction = Integer.parseInt(proportions[1].trim());
    conf.setWriteProportion(
        ((double) (proportionForMemTable)
            / (double) (proportionForCompaction + proportionForMemTable)));
    conf.setCompactionProportion(
        ((double) (proportionForCompaction)
            / (double) (proportionForCompaction + proportionForMemTable)));
  }

  private void initSchemaMemoryAllocate(Properties properties) {
    long schemaMemoryTotal = conf.getAllocateMemoryForSchema();

    int proportionSum = 10;
    int schemaRegionProportion = 8;
    int schemaCacheProportion = 1;
    int partitionCacheProportion = 0;
    int lastCacheProportion = 1;

    String schemaMemoryAllocatePortion =
        properties.getProperty("schema_memory_allocate_proportion");
    if (schemaMemoryAllocatePortion != null) {
      conf.setDefaultSchemaMemoryConfig(false);
      String[] proportions = schemaMemoryAllocatePortion.split(":");
      int loadedProportionSum = 0;
      for (String proportion : proportions) {
        loadedProportionSum += Integer.parseInt(proportion.trim());
      }

      if (loadedProportionSum != 0) {
        proportionSum = loadedProportionSum;
        schemaRegionProportion = Integer.parseInt(proportions[0].trim());
        schemaCacheProportion = Integer.parseInt(proportions[1].trim());
        partitionCacheProportion = Integer.parseInt(proportions[2].trim());
        lastCacheProportion = Integer.parseInt(proportions[3].trim());
      }
    } else {
      conf.setDefaultSchemaMemoryConfig(true);
    }

    conf.setAllocateMemoryForSchemaRegion(
        schemaMemoryTotal * schemaRegionProportion / proportionSum);
    logger.info("allocateMemoryForSchemaRegion = {}", conf.getAllocateMemoryForSchemaRegion());

    conf.setAllocateMemoryForSchemaCache(schemaMemoryTotal * schemaCacheProportion / proportionSum);
    logger.info("allocateMemoryForSchemaCache = {}", conf.getAllocateMemoryForSchemaCache());

    conf.setAllocateMemoryForPartitionCache(
        schemaMemoryTotal * partitionCacheProportion / proportionSum);
    logger.info("allocateMemoryForPartitionCache = {}", conf.getAllocateMemoryForPartitionCache());

    conf.setAllocateMemoryForLastCache(schemaMemoryTotal * lastCacheProportion / proportionSum);
    logger.info("allocateMemoryForLastCache = {}", conf.getAllocateMemoryForLastCache());
  }

  @SuppressWarnings("squid:S3518") // "proportionSum" can't be zero
  private void loadUDFProps(Properties properties) {
    String initialByteArrayLengthForMemoryControl =
        properties.getProperty("udf_initial_byte_array_length_for_memory_control");
    if (initialByteArrayLengthForMemoryControl != null) {
      conf.setUdfInitialByteArrayLengthForMemoryControl(
          Integer.parseInt(initialByteArrayLengthForMemoryControl.trim()));
    }

    conf.setUdfDir(properties.getProperty("udf_lib_dir", conf.getUdfDir()).trim());

    String memoryBudgetInMb = properties.getProperty("udf_memory_budget_in_mb");
    if (memoryBudgetInMb != null) {
      conf.setUdfMemoryBudgetInMB(
          (float)
              Math.min(
                  Float.parseFloat(memoryBudgetInMb.trim()),
                  0.2 * conf.getAllocateMemoryForRead()));
    }

    String groupByFillCacheSizeInMB = properties.getProperty("group_by_fill_cache_size_in_mb");
    if (groupByFillCacheSizeInMB != null) {
      conf.setGroupByFillCacheSizeInMB(Float.parseFloat(groupByFillCacheSizeInMB.trim()));
    }

    String readerTransformerCollectorMemoryProportion =
        properties.getProperty("udf_reader_transformer_collector_memory_proportion");
    if (readerTransformerCollectorMemoryProportion != null) {
      String[] proportions = readerTransformerCollectorMemoryProportion.trim().split(":");
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
                + readerTransformerCollectorMemoryProportion);
      }
    }
  }

  private void loadTriggerProps(Properties properties) {
    conf.setTriggerDir(properties.getProperty("trigger_root_dir", conf.getTriggerDir()).trim());
    conf.setTriggerTemporaryLibDir(
        properties
            .getProperty("trigger_temporary_lib_dir", conf.getTriggerTemporaryLibDir())
            .trim());
    conf.setRetryNumToFindStatefulTrigger(
        Integer.parseInt(
            properties
                .getProperty(
                    "stateful_trigger_retry_num_when_not_found",
                    Integer.toString(conf.getRetryNumToFindStatefulTrigger()))
                .trim()));

    int tlogBufferSize =
        Integer.parseInt(
            properties
                .getProperty("tlog_buffer_size", Integer.toString(conf.getTlogBufferSize()))
                .trim());
    if (tlogBufferSize > 0) {
      conf.setTlogBufferSize(tlogBufferSize);
    }

    conf.setTriggerForwardMaxQueueNumber(
        Integer.parseInt(
            properties
                .getProperty(
                    "trigger_forward_max_queue_number",
                    Integer.toString(conf.getTriggerForwardMaxQueueNumber()))
                .trim()));
    conf.setTriggerForwardMaxSizePerQueue(
        Integer.parseInt(
            properties
                .getProperty(
                    "trigger_forward_max_size_per_queue",
                    Integer.toString(conf.getTriggerForwardMaxSizePerQueue()))
                .trim()));
    conf.setTriggerForwardBatchSize(
        Integer.parseInt(
            properties
                .getProperty(
                    "trigger_forward_batch_size",
                    Integer.toString(conf.getTriggerForwardBatchSize()))
                .trim()));
    conf.setTriggerForwardHTTPPoolSize(
        Integer.parseInt(
            properties
                .getProperty(
                    "trigger_forward_http_pool_size",
                    Integer.toString(conf.getTriggerForwardHTTPPoolSize()))
                .trim()));
    conf.setTriggerForwardHTTPPOOLMaxPerRoute(
        Integer.parseInt(
            properties
                .getProperty(
                    "trigger_forward_http_pool_max_per_route",
                    Integer.toString(conf.getTriggerForwardHTTPPOOLMaxPerRoute()))
                .trim()));
    conf.setTriggerForwardMQTTPoolSize(
        Integer.parseInt(
            properties
                .getProperty(
                    "trigger_forward_mqtt_pool_size",
                    Integer.toString(conf.getTriggerForwardMQTTPoolSize()))
                .trim()));
  }

  private void loadCQProps(Properties properties) {
    conf.setContinuousQueryThreadNum(
        Integer.parseInt(
            properties
                .getProperty(
                    "continuous_query_thread_num",
                    Integer.toString(conf.getContinuousQueryThreadNum()))
                .trim()));
    if (conf.getContinuousQueryThreadNum() <= 0) {
      conf.setContinuousQueryThreadNum(Runtime.getRuntime().availableProcessors() / 2);
    }

    conf.setMaxPendingContinuousQueryTasks(
        Integer.parseInt(
            properties
                .getProperty(
                    "max_pending_continuous_query_tasks",
                    Integer.toString(conf.getMaxPendingContinuousQueryTasks()))
                .trim()));
    if (conf.getMaxPendingContinuousQueryTasks() <= 0) {
      conf.setMaxPendingContinuousQueryTasks(64);
    }

    conf.setContinuousQueryMinimumEveryInterval(
        DateTimeUtils.convertDurationStrToLong(
            properties.getProperty("continuous_query_minimum_every_interval", "1s").trim(),
            conf.getTimestampPrecision()));

    conf.setCqlogBufferSize(
        Integer.parseInt(
            properties
                .getProperty("cqlog_buffer_size", Integer.toString(conf.getCqlogBufferSize()))
                .trim()));
  }

  public void loadClusterProps(Properties properties) {
    String configNodeUrls = properties.getProperty(IoTDBConstant.DN_TARGET_CONFIG_NODES);
    if (configNodeUrls != null) {
      try {
        conf.setTargetConfigNodeList(NodeUrlUtils.parseTEndPointUrls(configNodeUrls.trim()));
      } catch (BadNodeUrlException e) {
        logger.error(
            "Config nodes are set in wrong format, please set them like 127.0.0.1:22277,127.0.0.1:22281");
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
            properties
                .getProperty(
                    "dn_mpp_data_exchange_port", Integer.toString(conf.getMppDataExchangePort()))
                .trim()));
    conf.setMppDataExchangeCorePoolSize(
        Integer.parseInt(
            properties
                .getProperty(
                    "mpp_data_exchange_core_pool_size",
                    Integer.toString(conf.getMppDataExchangeCorePoolSize()))
                .trim()));
    conf.setMppDataExchangeMaxPoolSize(
        Integer.parseInt(
            properties
                .getProperty(
                    "mpp_data_exchange_max_pool_size",
                    Integer.toString(conf.getMppDataExchangeMaxPoolSize()))
                .trim()));
    conf.setMppDataExchangeKeepAliveTimeInMs(
        Integer.parseInt(
            properties
                .getProperty(
                    "mpp_data_exchange_keep_alive_time_in_ms",
                    Integer.toString(conf.getMppDataExchangeKeepAliveTimeInMs()))
                .trim()));

    conf.setPartitionCacheSize(
        Integer.parseInt(
            properties
                .getProperty("partition_cache_size", Integer.toString(conf.getPartitionCacheSize()))
                .trim()));

    conf.setDriverTaskExecutionTimeSliceInMs(
        Integer.parseInt(
            properties
                .getProperty(
                    "driver_task_execution_time_slice_in_ms",
                    Integer.toString(conf.getDriverTaskExecutionTimeSliceInMs()))
                .trim()));
  }

  /** Get default encode algorithm by data type */
  public TSEncoding getDefaultEncodingByType(TSDataType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return conf.getDefaultBooleanEncoding();
      case INT32:
        return conf.getDefaultInt32Encoding();
      case INT64:
        return conf.getDefaultInt64Encoding();
      case FLOAT:
        return conf.getDefaultFloatEncoding();
      case DOUBLE:
        return conf.getDefaultDoubleEncoding();
      default:
        return conf.getDefaultTextEncoding();
    }
  }

  // These configurations are received from config node when registering
  public void loadGlobalConfig(TGlobalConfig globalConfig) {
    conf.setSeriesPartitionExecutorClass(globalConfig.getSeriesPartitionExecutorClass());
    conf.setSeriesPartitionSlotNum(globalConfig.getSeriesPartitionSlotNum());
    conf.setTimePartitionIntervalForRouting(
        DateTimeUtils.convertMilliTimeWithPrecision(
            globalConfig.timePartitionInterval, conf.getTimestampPrecision()));
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

    conf.setDataRatisConsensusLogSegmentSizeMax(ratisConfig.getDataLogSegmentSizeMax());
    conf.setSchemaRatisConsensusLogSegmentSizeMax(ratisConfig.getSchemaLogSegmentSizeMax());

    conf.setDataRatisConsensusGrpcFlowControlWindow(ratisConfig.getDataGrpcFlowControlWindow());
    conf.setSchemaRatisConsensusGrpcFlowControlWindow(ratisConfig.getSchemaGrpcFlowControlWindow());

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
  }

  public void loadCQConfig(TCQConfig cqConfig) {
    conf.setCqMinEveryIntervalInMs(cqConfig.getCqMinEveryIntervalInMs());
  }

  public void reclaimConsensusMemory() {
    conf.setAllocateMemoryForStorageEngine(
        conf.getAllocateMemoryForStorageEngine() + conf.getAllocateMemoryForConsensus());
    SystemInfo.getInstance().allocateWriteMemory();
  }

  public void initClusterSchemaMemoryAllocate() {
    if (!conf.isDefaultSchemaMemoryConfig()) {
      // the config has already been updated as user config in properties file
      return;
    }

    // process the default schema memory allocate

    long schemaMemoryTotal = conf.getAllocateMemoryForSchema();

    int proportionSum = 10;
    int schemaRegionProportion = 5;
    int schemaCacheProportion = 3;
    int partitionCacheProportion = 1;
    int lastCacheProportion = 1;

    conf.setAllocateMemoryForSchemaRegion(
        schemaMemoryTotal * schemaRegionProportion / proportionSum);
    logger.info(
        "Cluster allocateMemoryForSchemaRegion = {}", conf.getAllocateMemoryForSchemaRegion());

    conf.setAllocateMemoryForSchemaCache(schemaMemoryTotal * schemaCacheProportion / proportionSum);
    logger.info(
        "Cluster allocateMemoryForSchemaCache = {}", conf.getAllocateMemoryForSchemaCache());

    conf.setAllocateMemoryForPartitionCache(
        schemaMemoryTotal * partitionCacheProportion / proportionSum);
    logger.info(
        "Cluster allocateMemoryForPartitionCache = {}", conf.getAllocateMemoryForPartitionCache());

    conf.setAllocateMemoryForLastCache(schemaMemoryTotal * lastCacheProportion / proportionSum);
    logger.info("Cluster allocateMemoryForLastCache = {}", conf.getAllocateMemoryForLastCache());
  }

  private static class IoTDBDescriptorHolder {

    private static final IoTDBDescriptor INSTANCE = new IoTDBDescriptor();

    private IoTDBDescriptorHolder() {}
  }
}
