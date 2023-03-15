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
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.BadNodeUrlException;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.utils.NodeUrlUtils;
import org.apache.iotdb.confignode.rpc.thrift.TCQConfig;
import org.apache.iotdb.confignode.rpc.thrift.TGlobalConfig;
import org.apache.iotdb.confignode.rpc.thrift.TRatisConfig;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.compaction.execute.performer.constant.CrossCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.execute.performer.constant.InnerSeqCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.execute.performer.constant.InnerUnseqCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.schedule.constant.CompactionPriority;
import org.apache.iotdb.db.engine.compaction.selector.constant.CrossCompactionSelector;
import org.apache.iotdb.db.engine.compaction.selector.constant.InnerSequenceCompactionSelector;
import org.apache.iotdb.db.engine.compaction.selector.constant.InnerUnsequenceCompactionSelector;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.rescon.SystemInfo;
import org.apache.iotdb.db.service.metrics.IoTDBInternalLocalReporter;
import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.db.utils.datastructure.TVListSortAlgorithm;
import org.apache.iotdb.db.wal.WALManager;
import org.apache.iotdb.db.wal.utils.WALMode;
import org.apache.iotdb.external.api.IPropertiesLoader;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.config.ReloadLevel;
import org.apache.iotdb.metrics.reporter.iotdb.IoTDBInternalMemoryReporter;
import org.apache.iotdb.metrics.reporter.iotdb.IoTDBInternalReporter;
import org.apache.iotdb.metrics.utils.InternalReporterType;
import org.apache.iotdb.metrics.utils.NodeType;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSType;
import org.apache.iotdb.tsfile.utils.FilePathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
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
        MetricConfigDescriptor.getInstance().loadProps(commonProperties);
        MetricConfigDescriptor.getInstance()
            .getMetricConfig()
            .updateRpcInstance(conf.getClusterName(), conf.getDataNodeId(), NodeType.DATANODE);
      }
    } else {
      logger.warn(
          "Couldn't load the configuration {} from any of the known sources.",
          IoTDBConfig.CONFIG_NAME);
    }
  }

  public void loadProperties(Properties properties) {

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

    if (properties.getProperty("dn_core_client_count_for_each_node_in_client_manager", null)
        != null) {
      conf.setCoreClientNumForEachNode(
          Integer.parseInt(
              properties.getProperty("dn_core_connection_for_internal_service").trim()));
      logger.warn(
          "The parameter dn_core_connection_for_internal_service is out of date. Please rename it to dn_core_client_count_for_each_node_in_client_manager.");
    }
    conf.setCoreClientNumForEachNode(
        Integer.parseInt(
            properties
                .getProperty(
                    "dn_core_client_count_for_each_node_in_client_manager",
                    String.valueOf(conf.getCoreClientNumForEachNode()))
                .trim()));

    if (properties.getProperty("dn_max_client_count_for_each_node_in_client_manager", null)
        != null) {
      conf.setMaxClientNumForEachNode(
          Integer.parseInt(
              properties
                  .getProperty("dn_max_client_count_for_each_node_in_client_manager")
                  .trim()));
      logger.warn(
          "The parameter dn_max_client_count_for_each_node_in_client_manager is out of date. Please rename it to dn_max_client_count_for_each_node_in_client_manager.");
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

    conf.setDataDirs(properties.getProperty("dn_data_dirs", conf.getDataDirs()[0]).split(","));

    conf.setConsensusDir(properties.getProperty("dn_consensus_dir", conf.getConsensusDir()));

    int mlogBufferSize =
        Integer.parseInt(
            properties.getProperty("mlog_buffer_size", Integer.toString(conf.getMlogBufferSize())));
    if (mlogBufferSize > 0) {
      conf.setMlogBufferSize(mlogBufferSize);
    }

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

    conf.setEnableMemControl(
        (Boolean.parseBoolean(
            properties.getProperty(
                "enable_mem_control", Boolean.toString(conf.isEnableMemControl())))));
    logger.info("IoTDB enable memory control: {}", conf.isEnableMemControl());

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

    conf.setIoTaskQueueSizeForFlushing(
        Integer.parseInt(
            properties.getProperty(
                "io_task_queue_size_for_flushing",
                Integer.toString(conf.getIoTaskQueueSizeForFlushing()))));

    conf.setCompactionScheduleIntervalInMs(
        Long.parseLong(
            properties.getProperty(
                "compaction_schedule_interval_in_ms",
                Long.toString(conf.getCompactionScheduleIntervalInMs()))));

    conf.setCompactionSubmissionIntervalInMs(
        Long.parseLong(
            properties.getProperty(
                "compaction_submission_interval_in_ms",
                Long.toString(conf.getCompactionSubmissionIntervalInMs()))));

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

    conf.setQueryTimeoutThreshold(
        Long.parseLong(
            properties.getProperty(
                "query_timeout_threshold", Long.toString(conf.getQueryTimeoutThreshold()))));

    conf.setSessionTimeoutThreshold(
        Integer.parseInt(
            properties.getProperty(
                "dn_session_timeout_threshold",
                Integer.toString(conf.getSessionTimeoutThreshold()))));
    conf.setMaxNumberOfSyncFileRetry(
        Integer.parseInt(
            properties
                .getProperty(
                    "max_number_of_sync_file_retry",
                    Integer.toString(conf.getMaxNumberOfSyncFileRetry()))
                .trim()));

    conf.setIpWhiteList(properties.getProperty("ip_white_list", conf.getIpWhiteList()));

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

    conf.setUpgradeThreadCount(
        Integer.parseInt(
            properties.getProperty(
                "upgrade_thread_count", Integer.toString(conf.getUpgradeThreadCount()))));
    conf.setCrossCompactionFileSelectionTimeBudget(
        Long.parseLong(
            properties.getProperty(
                "cross_compaction_file_selection_time_budget",
                Long.toString(conf.getCrossCompactionFileSelectionTimeBudget()))));
    conf.setMergeIntervalSec(
        Long.parseLong(
            properties.getProperty(
                "merge_interval_sec", Long.toString(conf.getMergeIntervalSec()))));
    conf.setCompactionThreadCount(
        Integer.parseInt(
            properties.getProperty(
                "compaction_thread_count", Integer.toString(conf.getCompactionThreadCount()))));
    conf.setChunkMetadataSizeProportion(
        Double.parseDouble(
            properties.getProperty(
                "chunk_metadata_size_proportion",
                Double.toString(conf.getChunkMetadataSizeProportion()))));
    conf.setTargetCompactionFileSize(
        Long.parseLong(
            properties.getProperty(
                "target_compaction_file_size", Long.toString(conf.getTargetCompactionFileSize()))));
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
    conf.setMaxInnerCompactionCandidateFileNum(
        Integer.parseInt(
            properties.getProperty(
                "max_inner_compaction_candidate_file_num",
                Integer.toString(conf.getMaxInnerCompactionCandidateFileNum()))));
    conf.setMaxCrossCompactionCandidateFileNum(
        Integer.parseInt(
            properties.getProperty(
                "max_cross_compaction_candidate_file_num",
                Integer.toString(conf.getMaxCrossCompactionCandidateFileNum()))));
    conf.setMaxCrossCompactionCandidateFileSize(
        Long.parseLong(
            properties.getProperty(
                "max_cross_compaction_candidate_file_size",
                Long.toString(conf.getMaxCrossCompactionCandidateFileSize()))));

    conf.setCompactionWriteThroughputMbPerSec(
        Integer.parseInt(
            properties.getProperty(
                "compaction_write_throughput_mb_per_sec",
                Integer.toString(conf.getCompactionWriteThroughputMbPerSec()))));

    conf.setEnableCompactionValidation(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_compaction_validation",
                Boolean.toString(conf.isEnableCompactionValidation()))));
    conf.setCandidateCompactionTaskQueueSize(
        Integer.parseInt(
            properties.getProperty(
                "candidate_compaction_task_queue_size",
                Integer.toString(conf.getCandidateCompactionTaskQueueSize()))));

    conf.setEnablePartialInsert(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_partial_insert", String.valueOf(conf.isEnablePartialInsert()))));

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

    conf.setEnableWatermark(
        Boolean.parseBoolean(
            properties.getProperty(
                "watermark_module_opened", Boolean.toString(conf.isEnableWatermark()).trim())));
    conf.setWatermarkSecretKey(
        properties.getProperty("watermark_secret_key", conf.getWatermarkSecretKey()));
    conf.setWatermarkBitString(
        properties.getProperty("watermark_bit_string", conf.getWatermarkBitString()));
    conf.setWatermarkMethod(properties.getProperty("watermark_method", conf.getWatermarkMethod()));

    loadAutoCreateSchemaProps(properties);

    conf.setTsFileStorageFs(
        properties.getProperty("tsfile_storage_fs", conf.getTsFileStorageFs().toString()));
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

    // the num of memtables in each database
    conf.setConcurrentWritingTimePartition(
        Integer.parseInt(
            properties.getProperty(
                "concurrent_writing_time_partition",
                String.valueOf(conf.getConcurrentWritingTimePartition()))));

    // the default fill interval in LinearFill and PreviousFill
    conf.setDefaultFillInterval(
        Integer.parseInt(
            properties.getProperty(
                "default_fill_interval", String.valueOf(conf.getDefaultFillInterval()))));

    conf.setTagAttributeTotalSize(
        Integer.parseInt(
            properties.getProperty(
                "tag_attribute_total_size", String.valueOf(conf.getTagAttributeTotalSize()))));

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

    conf.setFrequencyIntervalInMinute(
        Integer.parseInt(
            properties.getProperty(
                "frequency_interval_in_minute",
                String.valueOf(conf.getFrequencyIntervalInMinute()))));

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

    conf.setEnableDiscardOutOfOrderData(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_discard_out_of_order_data",
                Boolean.toString(conf.isEnableDiscardOutOfOrderData()))));

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

    // id table related configuration
    conf.setDeviceIDTransformationMethod(
        properties.getProperty(
            "device_id_transformation_method", conf.getDeviceIDTransformationMethod()));

    conf.setEnableIDTable(
        Boolean.parseBoolean(
            properties.getProperty("enable_id_table", String.valueOf(conf.isEnableIDTable()))));

    conf.setEnableIDTableLogFile(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_id_table_log_file", String.valueOf(conf.isEnableIDTableLogFile()))));

    conf.setSchemaEngineMode(
        properties.getProperty("schema_engine_mode", String.valueOf(conf.getSchemaEngineMode())));

    conf.setEnableLastCache(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_last_cache", Boolean.toString(conf.isLastCacheEnabled()))));

    if (conf.getSchemaEngineMode().equals("Rocksdb_based")) {
      conf.setEnableLastCache(false);
    }

    conf.setCachedMNodeSizeInSchemaFileMode(
        Integer.parseInt(
            properties.getProperty(
                "cached_mnode_size_in_schema_file_mode",
                String.valueOf(conf.getCachedMNodeSizeInSchemaFileMode()))));

    conf.setMinimumSegmentInSchemaFile(
        Short.parseShort(
            properties.getProperty(
                "minimum_schema_file_segment_in_bytes",
                String.valueOf(conf.getMinimumSegmentInSchemaFile()))));

    conf.setPageCacheSizeInSchemaFile(
        Integer.parseInt(
            properties.getProperty(
                "page_cache_in_schema_file", String.valueOf(conf.getPageCacheSizeInSchemaFile()))));

    conf.setSchemaFileLogSize(
        Integer.parseInt(
            properties.getProperty(
                "schema_file_log_size", String.valueOf(conf.getSchemaFileLogSize()))));

    conf.setMaxMeasurementNumOfInternalRequest(
        Integer.parseInt(
            properties.getProperty(
                "max_measurement_num_of_internal_request",
                String.valueOf(conf.getMaxMeasurementNumOfInternalRequest()))));

    // mqtt
    loadMqttProps(properties);

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
    TSFileDescriptor.getInstance()
        .getConfig()
        .setTSFileStorageFs(
            FSType.valueOf(
                properties.getProperty("tsfile_storage_fs", conf.getTsFileStorageFs().name())));
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

    conf.setTimePartitionInterval(
        DateTimeUtils.convertMilliTimeWithPrecision(
            conf.getTimePartitionInterval(), conf.getTimestampPrecision()));
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

  private void loadWALProps(Properties properties) {
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

    int walBufferEntrySize =
        Integer.parseInt(
            properties.getProperty(
                "wal_buffer_entry_size_in_byte", Integer.toString(conf.getWalBufferEntrySize())));
    if (walBufferEntrySize > 0) {
      conf.setWalBufferEntrySize(walBufferEntrySize);
    }

    int walBufferQueueCapacity =
        Integer.parseInt(
            properties.getProperty(
                "wal_buffer_queue_capacity", Integer.toString(conf.getWalBufferQueueCapacity())));
    if (walBufferQueueCapacity > 0) {
      conf.setWalBufferQueueCapacity(walBufferQueueCapacity);
    }

    loadWALHotModifiedProps(properties);
  }

  private void loadWALHotModifiedProps(Properties properties) {
    long fsyncWalDelayInMs =
        Long.parseLong(
            properties.getProperty(
                "fsync_wal_delay_in_ms", Long.toString(conf.getFsyncWalDelayInMs())));
    if (fsyncWalDelayInMs > 0) {
      conf.setFsyncWalDelayInMs(fsyncWalDelayInMs);
    }

    long walFileSizeThreshold =
        Long.parseLong(
            properties.getProperty(
                "wal_file_size_threshold_in_byte",
                Long.toString(conf.getWalFileSizeThresholdInByte())));
    if (walFileSizeThreshold > 0) {
      conf.setWalFileSizeThresholdInByte(walFileSizeThreshold);
    }

    double walMinEffectiveInfoRatio =
        Double.parseDouble(
            properties.getProperty(
                "wal_min_effective_info_ratio",
                Double.toString(conf.getWalMinEffectiveInfoRatio())));
    if (walMinEffectiveInfoRatio > 0) {
      conf.setWalMinEffectiveInfoRatio(walMinEffectiveInfoRatio);
    }

    long walMemTableSnapshotThreshold =
        Long.parseLong(
            properties.getProperty(
                "wal_memtable_snapshot_threshold_in_byte",
                Long.toString(conf.getWalMemTableSnapshotThreshold())));
    if (walMemTableSnapshotThreshold > 0) {
      conf.setWalMemTableSnapshotThreshold(walMemTableSnapshotThreshold);
    }

    int maxWalMemTableSnapshotNum =
        Integer.parseInt(
            properties.getProperty(
                "max_wal_memtable_snapshot_num",
                Integer.toString(conf.getMaxWalMemTableSnapshotNum())));
    if (maxWalMemTableSnapshotNum > 0) {
      conf.setMaxWalMemTableSnapshotNum(maxWalMemTableSnapshotNum);
    }

    long deleteWalFilesPeriod =
        Long.parseLong(
            properties.getProperty(
                "delete_wal_files_period_in_ms",
                Long.toString(conf.getDeleteWalFilesPeriodInMs())));
    if (deleteWalFilesPeriod > 0) {
      conf.setDeleteWalFilesPeriodInMs(deleteWalFilesPeriod);
    }

    long throttleDownThresholdInByte =
        Long.parseLong(
            properties.getProperty(
                "iot_consensus_throttle_threshold_in_byte",
                Long.toString(conf.getThrottleThreshold())));
    if (throttleDownThresholdInByte > 0) {
      conf.setThrottleThreshold(throttleDownThresholdInByte);
    }

    long cacheWindowInMs =
        Long.parseLong(
            properties.getProperty(
                "iot_consensus_cache_window_time_in_ms",
                Long.toString(conf.getCacheWindowTimeInMs())));
    if (cacheWindowInMs > 0) {
      conf.setCacheWindowTimeInMs(cacheWindowInMs);
    }
  }

  private void loadAutoCreateSchemaProps(Properties properties) {
    conf.setAutoCreateSchemaEnabled(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_auto_create_schema",
                Boolean.toString(conf.isAutoCreateSchemaEnabled()).trim())));
    conf.setBooleanStringInferType(
        TSDataType.valueOf(
            properties.getProperty(
                "boolean_string_infer_type", conf.getBooleanStringInferType().toString())));
    conf.setIntegerStringInferType(
        TSDataType.valueOf(
            properties.getProperty(
                "integer_string_infer_type", conf.getIntegerStringInferType().toString())));
    conf.setLongStringInferType(
        TSDataType.valueOf(
            properties.getProperty(
                "long_string_infer_type", conf.getLongStringInferType().toString())));
    conf.setFloatingStringInferType(
        TSDataType.valueOf(
            properties.getProperty(
                "floating_string_infer_type", conf.getFloatingStringInferType().toString())));
    conf.setNanStringInferType(
        TSDataType.valueOf(
            properties.getProperty(
                "nan_string_infer_type", conf.getNanStringInferType().toString())));
    conf.setDefaultStorageGroupLevel(
        Integer.parseInt(
            properties.getProperty(
                "default_storage_group_level",
                Integer.toString(conf.getDefaultStorageGroupLevel()))));
    conf.setDefaultBooleanEncoding(
        properties.getProperty(
            "default_boolean_encoding", conf.getDefaultBooleanEncoding().toString()));
    conf.setDefaultInt32Encoding(
        properties.getProperty(
            "default_int32_encoding", conf.getDefaultInt32Encoding().toString()));
    conf.setDefaultInt64Encoding(
        properties.getProperty(
            "default_int64_encoding", conf.getDefaultInt64Encoding().toString()));
    conf.setDefaultFloatEncoding(
        properties.getProperty(
            "default_float_encoding", conf.getDefaultFloatEncoding().toString()));
    conf.setDefaultDoubleEncoding(
        properties.getProperty(
            "default_double_encoding", conf.getDefaultDoubleEncoding().toString()));
    conf.setDefaultTextEncoding(
        properties.getProperty("default_text_encoding", conf.getDefaultTextEncoding().toString()));
  }

  private void loadTsFileProps(Properties properties) {
    TSFileDescriptor.getInstance()
        .getConfig()
        .setGroupSizeInByte(
            Integer.parseInt(
                properties.getProperty(
                    "group_size_in_byte",
                    Integer.toString(
                        TSFileDescriptor.getInstance().getConfig().getGroupSizeInByte()))));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setPageSizeInByte(
            Integer.parseInt(
                properties.getProperty(
                    "page_size_in_byte",
                    Integer.toString(
                        TSFileDescriptor.getInstance().getConfig().getPageSizeInByte()))));
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
                properties.getProperty(
                    "max_number_of_points_in_page",
                    Integer.toString(
                        TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage()))));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setMaxStringLength(
            Integer.parseInt(
                properties.getProperty(
                    "max_string_length",
                    Integer.toString(
                        TSFileDescriptor.getInstance().getConfig().getMaxStringLength()))));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setBloomFilterErrorRate(
            Double.parseDouble(
                properties.getProperty(
                    "bloom_filter_error_rate",
                    Double.toString(
                        TSFileDescriptor.getInstance().getConfig().getBloomFilterErrorRate()))));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setFloatPrecision(
            Integer.parseInt(
                properties.getProperty(
                    "float_precision",
                    Integer.toString(
                        TSFileDescriptor.getInstance().getConfig().getFloatPrecision()))));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setTimeEncoder(
            properties.getProperty(
                "time_encoder", TSFileDescriptor.getInstance().getConfig().getTimeEncoder()));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setValueEncoder(
            properties.getProperty(
                "value_encoder", TSFileDescriptor.getInstance().getConfig().getValueEncoder()));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setCompressor(
            properties.getProperty(
                "compressor",
                TSFileDescriptor.getInstance().getConfig().getCompressor().toString()));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setMaxDegreeOfIndexNode(
            Integer.parseInt(
                properties.getProperty(
                    "max_degree_of_index_node",
                    Integer.toString(
                        TSFileDescriptor.getInstance().getConfig().getMaxDegreeOfIndexNode()))));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setMaxTsBlockSizeInBytes(
            Integer.parseInt(
                properties.getProperty(
                    "max_tsblock_size_in_bytes",
                    Integer.toString(
                        TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes()))));

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
                    Integer.toString(
                        TSFileDescriptor.getInstance().getConfig().getMaxTsBlockLineNumber()))));
  }

  // Mqtt related
  private void loadMqttProps(Properties properties) {
    conf.setMqttDir(properties.getProperty("mqtt_root_dir", conf.getMqttDir()));

    if (properties.getProperty(IoTDBConstant.MQTT_HOST_NAME) != null) {
      conf.setMqttHost(properties.getProperty(IoTDBConstant.MQTT_HOST_NAME));
    } else {
      logger.info("MQTT host is not configured, will use dn_rpc_address.");
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
  private void loadTimedService(Properties properties) {
    conf.setEnableTimedFlushSeqMemtable(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_timed_flush_seq_memtable",
                Boolean.toString(conf.isEnableTimedFlushSeqMemtable()))));

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
            properties.getProperty(
                "enable_timed_flush_unseq_memtable",
                Boolean.toString(conf.isEnableTimedFlushUnseqMemtable()))));

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
        conf.reloadDataDirs(dataDirs.split(","));
      }

      // update dir strategy, must update after data dirs
      String multiDirStrategyClassName = properties.getProperty("dn_multi_dir_strategy", null);
      if (multiDirStrategyClassName != null
          && !multiDirStrategyClassName.equals(conf.getMultiDirStrategyClassName())) {
        conf.setMultiDirStrategyClassName(multiDirStrategyClassName);
        conf.confirmMultiDirStrategy();
        DirectoryManager.getInstance().updateDirectoryStrategy();
      }

      // update timed flush & close conf
      loadTimedService(properties);
      StorageEngine.getInstance().rebootTimedService();

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

      // update frequency_interval_in_minute
      conf.setFrequencyIntervalInMinute(
          Integer.parseInt(
              properties.getProperty(
                  "frequency_interval_in_minute",
                  Integer.toString(conf.getFrequencyIntervalInMinute()))));
      // update slow_query_threshold
      conf.setSlowQueryThreshold(
          Long.parseLong(
              properties.getProperty(
                  "slow_query_threshold", Long.toString(conf.getSlowQueryThreshold()))));
      // update merge_write_throughput_mb_per_sec
      conf.setCompactionWriteThroughputMbPerSec(
          Integer.parseInt(
              properties.getProperty(
                  "merge_write_throughput_mb_per_sec",
                  Integer.toString(conf.getCompactionWriteThroughputMbPerSec()))));
      // update insert-tablet-plan's row limit for select-into
      conf.setSelectIntoInsertTabletPlanRowLimit(
          Integer.parseInt(
              properties.getProperty(
                  "select_into_insert_tablet_plan_row_limit",
                  String.valueOf(conf.getSelectIntoInsertTabletPlanRowLimit()))));

      // update sync config
      conf.setMaxNumberOfSyncFileRetry(
          Integer.parseInt(
              properties
                  .getProperty(
                      "max_number_of_sync_file_retry",
                      Integer.toString(conf.getMaxNumberOfSyncFileRetry()))
                  .trim()));
      conf.setIpWhiteList(properties.getProperty("ip_white_list", conf.getIpWhiteList()));

      // update enable query memory estimation for memory control
      conf.setEnableQueryMemoryEstimation(
          Boolean.parseBoolean(
              properties.getProperty(
                  "enable_query_memory_estimation",
                  Boolean.toString(conf.isEnableQueryMemoryEstimation()))));

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
    ReloadLevel reloadLevel = MetricConfigDescriptor.getInstance().loadHotProps(commonProperties);
    logger.info("Reload metric service in level {}", reloadLevel);
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
    String memoryAllocateProportion =
        properties.getProperty("storage_query_schema_consensus_free_memory_proportion");
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
    String allocationRatio = properties.getProperty("storage_engine_memory_proportion", "8:2");
    String[] proportions = allocationRatio.split(":");
    int proportionForWrite = Integer.parseInt(proportions[0].trim());
    int proportionForCompaction = Integer.parseInt(proportions[1].trim());

    double writeProportion =
        ((double) (proportionForWrite) / (double) (proportionForCompaction + proportionForWrite));

    String allocationRatioForWrite = properties.getProperty("write_memory_proportion", "19:1");
    proportions = allocationRatioForWrite.split(":");
    int proportionForMemTable = Integer.parseInt(proportions[0].trim());
    int proportionForTimePartitionInfo = Integer.parseInt(proportions[1].trim());

    double memtableProportionForWrite =
        ((double) (proportionForMemTable)
            / (double) (proportionForMemTable + proportionForTimePartitionInfo));

    double timePartitionInfoForWrite =
        ((double) (proportionForTimePartitionInfo)
            / (double) (proportionForMemTable + proportionForTimePartitionInfo));
    conf.setWriteProportionForMemtable(writeProportion * memtableProportionForWrite);

    conf.setAllocateMemoryForTimePartitionInfo(
        (long)
            ((writeProportion * timePartitionInfoForWrite)
                * conf.getAllocateMemoryForStorageEngine()));

    conf.setCompactionProportion(
        ((double) (proportionForCompaction)
            / (double) (proportionForCompaction + proportionForWrite)));
  }

  private void initSchemaMemoryAllocate(Properties properties) {
    long schemaMemoryTotal = conf.getAllocateMemoryForSchema();

    int proportionSum = 10;
    int schemaRegionProportion = 5;
    int schemaCacheProportion = 3;
    int partitionCacheProportion = 1;
    int lastCacheProportion = 1;

    String schemaMemoryAllocatePortion =
        properties.getProperty("schema_memory_allocate_proportion");
    if (schemaMemoryAllocatePortion != null) {
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
                + readerTransformerCollectorMemoryProportion);
      }
    }
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
            conf.getTimestampPrecision()));
  }

  public void loadClusterProps(Properties properties) {
    String configNodeUrls = properties.getProperty(IoTDBConstant.DN_TARGET_CONFIG_NODE_LIST);
    if (configNodeUrls != null) {
      try {
        conf.setTargetConfigNodeList(NodeUrlUtils.parseTEndPointUrls(configNodeUrls));
      } catch (BadNodeUrlException e) {
        logger.error(
            "Config nodes are set in wrong format, please set them like 127.0.0.1:10710,127.0.0.1:10712");
      }
    }

    conf.setInternalAddress(
        properties.getProperty(IoTDBConstant.DN_INTERNAL_ADDRESS, conf.getInternalAddress()));

    conf.setInternalPort(
        Integer.parseInt(
            properties.getProperty(
                IoTDBConstant.DN_INTERNAL_PORT, Integer.toString(conf.getInternalPort()))));

    conf.setDataRegionConsensusPort(
        Integer.parseInt(
            properties.getProperty(
                "dn_data_region_consensus_port",
                Integer.toString(conf.getDataRegionConsensusPort()))));

    conf.setSchemaRegionConsensusPort(
        Integer.parseInt(
            properties.getProperty(
                "dn_schema_region_consensus_port",
                Integer.toString(conf.getSchemaRegionConsensusPort()))));
    conf.setJoinClusterRetryIntervalMs(
        Long.parseLong(
            properties.getProperty(
                "dn_join_cluster_retry_interval_ms",
                Long.toString(conf.getJoinClusterRetryIntervalMs()))));
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
    conf.setTimePartitionInterval(
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

    conf.setSchemaRatisLogMax(ratisConfig.getSchemaRegionRatisLogMax());
    conf.setDataRatisLogMax(ratisConfig.getDataRegionRatisLogMax());
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
