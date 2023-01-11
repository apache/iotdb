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
import org.apache.iotdb.db.wal.WALManager;
import org.apache.iotdb.db.wal.utils.WALMode;
import org.apache.iotdb.external.api.IPropertiesLoader;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.config.ReloadLevel;
import org.apache.iotdb.metrics.reporter.iotdb.IoTDBInternalMemoryReporter;
import org.apache.iotdb.metrics.reporter.iotdb.IoTDBInternalReporter;
import org.apache.iotdb.metrics.utils.InternalReporterType;
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

  private final IoTDBConfig CONF = new IoTDBConfig();

  protected IoTDBDescriptor() {
    loadProps();
    ServiceLoader<IPropertiesLoader> propertiesLoaderServiceLoader =
        ServiceLoader.load(IPropertiesLoader.class);
    for (IPropertiesLoader loader : propertiesLoaderServiceLoader) {
      logger.info("Will reload properties from {} ", loader.getClass().getName());
      Properties properties = loader.loadProperties();
      loadProperties(properties);
      CONF.setCustomizedProperties(loader.getCustomizedProperties());
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
    return CONF;
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
        CONF.updatePath();
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
    URL url = getPropsUrl(CommonConfig.CONF_FILE_NAME);
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
          CommonConfig.CONF_FILE_NAME);
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
        CONF.updatePath();
        commonDescriptor.getConfig().updatePath(System.getProperty(IoTDBConstant.IOTDB_HOME, null));
        MetricConfigDescriptor.getInstance().loadProps(commonProperties);
        MetricConfigDescriptor.getInstance()
            .getMetricConfig()
            .updateRpcInstance(CONF.getDnInternalAddress(), CONF.getDnInternalPort());
      }
    } else {
      logger.warn(
          "Couldn't load the configuration {} from any of the known sources.",
          IoTDBConfig.CONFIG_NAME);
    }
  }

  public void loadProperties(Properties properties) {

    CONF.setClusterName(
        properties.getProperty(IoTDBConstant.CLUSTER_NAME, CONF.getClusterName()).trim());

    CONF.setDnRpcAddress(
        properties.getProperty(IoTDBConstant.DN_RPC_ADDRESS, CONF.getDnRpcAddress()).trim());

    CONF.setDnRpcThriftCompressionEnable(
        Boolean.parseBoolean(
            properties
                .getProperty(
                    "dn_rpc_thrift_compression_enable",
                    Boolean.toString(CONF.isDnRpcThriftCompressionEnable()))
                .trim()));

    CONF.setDnRpcAdvancedCompressionEnable(
        Boolean.parseBoolean(
            properties
                .getProperty(
                    "dn_rpc_advanced_compression_enable",
                    Boolean.toString(CONF.isDnRpcAdvancedCompressionEnable()))
                .trim()));

    CONF.setDnConnectionTimeoutInMS(
        Integer.parseInt(
            properties
                .getProperty(
                    "dn_connection_timeout_ms", String.valueOf(CONF.getDnConnectionTimeoutInMS()))
                .trim()));

    CONF.setCoreClientNumForEachNode(
        Integer.parseInt(
            properties
                .getProperty(
                    "dn_core_client_count_for_each_node_in_client_manager",
                    String.valueOf(CONF.getCoreClientNumForEachNode()))
                .trim()));

    CONF.setMaxClientNumForEachNode(
        Integer.parseInt(
            properties
                .getProperty(
                    "dn_max_client_count_for_each_node_in_client_manager",
                    String.valueOf(CONF.getMaxClientNumForEachNode()))
                .trim()));

    CONF.setDnSelectorThreadCountOfClientManager(
        Integer.parseInt(
            properties
                .getProperty(
                    "dn_selector_thread_count_of_client_manager",
                    String.valueOf(CONF.getDnSelectorThreadCountOfClientManager()))
                .trim()));

    CONF.setDnRpcPort(
        Integer.parseInt(
            properties
                .getProperty(IoTDBConstant.DN_RPC_PORT, Integer.toString(CONF.getDnRpcPort()))
                .trim()));

    CONF.setEnableInfluxDBRpcService(
        Boolean.parseBoolean(
            properties
                .getProperty(
                    "enable_influxdb_rpc_service",
                    Boolean.toString(CONF.isEnableInfluxDBRpcService()))
                .trim()));

    CONF.setInfluxDBRpcPort(
        Integer.parseInt(
            properties
                .getProperty("influxdb_rpc_port", Integer.toString(CONF.getInfluxDBRpcPort()))
                .trim()));

    CONF.setBufferedArraysMemoryProportion(
        Double.parseDouble(
            properties
                .getProperty(
                    "buffered_arrays_memory_proportion",
                    Double.toString(CONF.getBufferedArraysMemoryProportion()))
                .trim()));

    CONF.setFlushProportion(
        Double.parseDouble(
            properties
                .getProperty("flush_proportion", Double.toString(CONF.getFlushProportion()))
                .trim()));

    CONF.setRejectProportion(
        Double.parseDouble(
            properties
                .getProperty("reject_proportion", Double.toString(CONF.getRejectProportion()))
                .trim()));

    CONF.setWriteMemoryVariationReportProportion(
        Double.parseDouble(
            properties
                .getProperty(
                    "write_memory_variation_report_proportion",
                    Double.toString(CONF.getWriteMemoryVariationReportProportion()))
                .trim()));

    CONF.setMetaDataCacheEnable(
        Boolean.parseBoolean(
            properties
                .getProperty(
                    "meta_data_cache_enable", Boolean.toString(CONF.isMetaDataCacheEnable()))
                .trim()));

    initMemoryAllocate(properties);

    loadWALProps(properties);

    String systemDir = properties.getProperty("dn_system_dir");
    if (systemDir == null) {
      systemDir = properties.getProperty("base_dir");
      if (systemDir != null) {
        systemDir = FilePathUtils.regularizePath(systemDir) + IoTDBConstant.SYSTEM_FOLDER_NAME;
      } else {
        systemDir = CONF.getDnSystemDir();
      }
    }
    CONF.setDnSystemDir(systemDir);

    CONF.setSchemaDir(
        FilePathUtils.regularizePath(CONF.getDnSystemDir()) + IoTDBConstant.SCHEMA_FOLDER_NAME);

    CONF.setQueryDir(
        FilePathUtils.regularizePath(CONF.getDnSystemDir() + IoTDBConstant.QUERY_FOLDER_NAME));

    CONF.setDnTracingDir(properties.getProperty("dn_tracing_dir", CONF.getDnTracingDir()));

    CONF.setDnDataDirs(properties.getProperty("dn_data_dirs", CONF.getDnDataDirs()[0]).split(","));

    CONF.setDnConsensusDir(properties.getProperty("dn_consensus_dir", CONF.getDnConsensusDir()));

    String oldMultiDirStrategyClassName = CONF.getDnMultiDirStrategyClassName();
    CONF.setDnMultiDirStrategyClassName(
        properties.getProperty("dn_multi_dir_strategy", CONF.getDnMultiDirStrategyClassName()));
    try {
      CONF.checkMultiDirStrategyClassName();
    } catch (Exception e) {
      CONF.setDnMultiDirStrategyClassName(oldMultiDirStrategyClassName);
      throw e;
    }

    CONF.setBatchSize(
        Integer.parseInt(
            properties.getProperty("batch_size", Integer.toString(CONF.getBatchSize()))));

    CONF.setEnableMemControl(
        (Boolean.parseBoolean(
            properties.getProperty(
                "enable_mem_control", Boolean.toString(CONF.isEnableMemControl())))));
    logger.info("IoTDB enable memory control: {}", CONF.isEnableMemControl());

    long memTableSizeThreshold =
        Long.parseLong(
            properties
                .getProperty(
                    "memtable_size_threshold", Long.toString(CONF.getMemtableSizeThreshold()))
                .trim());
    if (memTableSizeThreshold > 0) {
      CONF.setMemtableSizeThreshold(memTableSizeThreshold);
    }

    CONF.setCheckPeriodWhenInsertBlocked(
        Integer.parseInt(
            properties.getProperty(
                "check_period_when_insert_blocked",
                Integer.toString(CONF.getCheckPeriodWhenInsertBlocked()))));

    CONF.setIoTaskQueueSizeForFlushing(
        Integer.parseInt(
            properties.getProperty(
                "io_task_queue_size_for_flushing",
                Integer.toString(CONF.getIoTaskQueueSizeForFlushing()))));

    CONF.setCompactionScheduleIntervalInMs(
        Long.parseLong(
            properties.getProperty(
                "compaction_schedule_interval_in_ms",
                Long.toString(CONF.getCompactionScheduleIntervalInMs()))));

    CONF.setCompactionSubmissionIntervalInMs(
        Long.parseLong(
            properties.getProperty(
                "compaction_submission_interval_in_ms",
                Long.toString(CONF.getCompactionSubmissionIntervalInMs()))));

    CONF.setEnableCrossSpaceCompaction(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_cross_space_compaction",
                Boolean.toString(CONF.isEnableCrossSpaceCompaction()))));

    CONF.setEnableSeqSpaceCompaction(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_seq_space_compaction",
                Boolean.toString(CONF.isEnableSeqSpaceCompaction()))));

    CONF.setEnableUnseqSpaceCompaction(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_unseq_space_compaction",
                Boolean.toString(CONF.isEnableUnseqSpaceCompaction()))));

    CONF.setCrossCompactionSelector(
        CrossCompactionSelector.getCrossCompactionSelector(
            properties.getProperty(
                "cross_selector", CONF.getCrossCompactionSelector().toString())));

    CONF.setInnerSequenceCompactionSelector(
        InnerSequenceCompactionSelector.getInnerSequenceCompactionSelector(
            properties.getProperty(
                "inner_seq_selector", CONF.getInnerSequenceCompactionSelector().toString())));

    CONF.setInnerUnsequenceCompactionSelector(
        InnerUnsequenceCompactionSelector.getInnerUnsequenceCompactionSelector(
            properties.getProperty(
                "inner_unseq_selector", CONF.getInnerUnsequenceCompactionSelector().toString())));

    CONF.setInnerSeqCompactionPerformer(
        InnerSeqCompactionPerformer.getInnerSeqCompactionPerformer(
            properties.getProperty(
                "inner_seq_performer", CONF.getInnerSeqCompactionPerformer().toString())));

    CONF.setInnerUnseqCompactionPerformer(
        InnerUnseqCompactionPerformer.getInnerUnseqCompactionPerformer(
            properties.getProperty(
                "inner_unseq_performer", CONF.getInnerUnseqCompactionPerformer().toString())));

    CONF.setCrossCompactionPerformer(
        CrossCompactionPerformer.getCrossCompactionPerformer(
            properties.getProperty(
                "cross_performer", CONF.getCrossCompactionPerformer().toString())));

    CONF.setCompactionPriority(
        CompactionPriority.valueOf(
            properties.getProperty(
                "compaction_priority", CONF.getCompactionPriority().toString())));

    int subtaskNum =
        Integer.parseInt(
            properties.getProperty(
                "sub_compaction_thread_count", Integer.toString(CONF.getSubCompactionTaskNum())));
    subtaskNum = subtaskNum <= 0 ? 1 : subtaskNum;
    CONF.setSubCompactionTaskNum(subtaskNum);

    CONF.setQueryTimeoutThreshold(
        Long.parseLong(
            properties.getProperty(
                "query_timeout_threshold", Long.toString(CONF.getQueryTimeoutThreshold()))));

    CONF.setDnSessionTimeoutThreshold(
        Integer.parseInt(
            properties.getProperty(
                "dn_session_timeout_threshold",
                Integer.toString(CONF.getDnSessionTimeoutThreshold()))));
    CONF.setMaxNumberOfSyncFileRetry(
        Integer.parseInt(
            properties
                .getProperty(
                    "max_number_of_sync_file_retry",
                    Integer.toString(CONF.getMaxNumberOfSyncFileRetry()))
                .trim()));

    CONF.setIpWhiteList(properties.getProperty("ip_white_list", CONF.getIpWhiteList()));

    // start: index parameter setting
    CONF.setIndexRootFolder(properties.getProperty("index_root_dir", CONF.getIndexRootFolder()));

    CONF.setEnableIndex(
        Boolean.parseBoolean(
            properties.getProperty("enable_index", Boolean.toString(CONF.isEnableIndex()))));

    CONF.setConcurrentIndexBuildThread(
        Integer.parseInt(
            properties.getProperty(
                "concurrent_index_build_thread",
                Integer.toString(CONF.getConcurrentIndexBuildThread()))));
    if (CONF.getConcurrentIndexBuildThread() <= 0) {
      CONF.setConcurrentIndexBuildThread(Runtime.getRuntime().availableProcessors());
    }

    CONF.setDefaultIndexWindowRange(
        Integer.parseInt(
            properties.getProperty(
                "default_index_window_range",
                Integer.toString(CONF.getDefaultIndexWindowRange()))));

    CONF.setQueryThreadCount(
        Integer.parseInt(
            properties.getProperty(
                "query_thread_count", Integer.toString(CONF.getQueryThreadCount()))));

    if (CONF.getQueryThreadCount() <= 0) {
      CONF.setQueryThreadCount(Runtime.getRuntime().availableProcessors());
    }

    CONF.setMaxAllowedConcurrentQueries(
        Integer.parseInt(
            properties.getProperty(
                "max_allowed_concurrent_queries",
                Integer.toString(CONF.getMaxAllowedConcurrentQueries()))));

    if (CONF.getMaxAllowedConcurrentQueries() <= 0) {
      CONF.setMaxAllowedConcurrentQueries(1000);
    }

    CONF.setSubRawQueryThreadCount(
        Integer.parseInt(
            properties.getProperty(
                "sub_rawQuery_thread_count", Integer.toString(CONF.getSubRawQueryThreadCount()))));

    if (CONF.getSubRawQueryThreadCount() <= 0) {
      CONF.setSubRawQueryThreadCount(Runtime.getRuntime().availableProcessors());
    }

    CONF.setRawQueryBlockingQueueCapacity(
        Integer.parseInt(
            properties.getProperty(
                "raw_query_blocking_queue_capacity",
                Integer.toString(CONF.getRawQueryBlockingQueueCapacity()))));

    CONF.setmRemoteSchemaCacheSize(
        Integer.parseInt(
            properties
                .getProperty(
                    "remote_schema_cache_size", Integer.toString(CONF.getmRemoteSchemaCacheSize()))
                .trim()));

    CONF.setLanguageVersion(
        properties.getProperty("language_version", CONF.getLanguageVersion()).trim());

    if (properties.containsKey("chunk_buffer_pool_enable")) {
      CONF.setChunkBufferPoolEnable(
          Boolean.parseBoolean(properties.getProperty("chunk_buffer_pool_enable")));
    }

    CONF.setEnableExternalSort(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_external_sort", Boolean.toString(CONF.isEnableExternalSort()))));
    CONF.setExternalSortThreshold(
        Integer.parseInt(
            properties.getProperty(
                "external_sort_threshold", Integer.toString(CONF.getExternalSortThreshold()))));
    CONF.setCrossCompactionFileSelectionTimeBudget(
        Long.parseLong(
            properties.getProperty(
                "cross_compaction_file_selection_time_budget",
                Long.toString(CONF.getCrossCompactionFileSelectionTimeBudget()))));
    CONF.setMergeIntervalSec(
        Long.parseLong(
            properties.getProperty(
                "merge_interval_sec", Long.toString(CONF.getMergeIntervalSec()))));
    CONF.setCompactionThreadCount(
        Integer.parseInt(
            properties.getProperty(
                "compaction_thread_count", Integer.toString(CONF.getCompactionThreadCount()))));
    CONF.setChunkMetadataSizeProportion(
        Double.parseDouble(
            properties.getProperty(
                "chunk_metadata_size_proportion",
                Double.toString(CONF.getChunkMetadataSizeProportion()))));
    CONF.setTargetCompactionFileSize(
        Long.parseLong(
            properties.getProperty(
                "target_compaction_file_size", Long.toString(CONF.getTargetCompactionFileSize()))));
    CONF.setTargetChunkSize(
        Long.parseLong(
            properties.getProperty("target_chunk_size", Long.toString(CONF.getTargetChunkSize()))));
    CONF.setTargetChunkPointNum(
        Long.parseLong(
            properties.getProperty(
                "target_chunk_point_num", Long.toString(CONF.getTargetChunkPointNum()))));
    CONF.setChunkPointNumLowerBoundInCompaction(
        Long.parseLong(
            properties.getProperty(
                "chunk_point_num_lower_bound_in_compaction",
                Long.toString(CONF.getChunkPointNumLowerBoundInCompaction()))));
    CONF.setChunkSizeLowerBoundInCompaction(
        Long.parseLong(
            properties.getProperty(
                "chunk_size_lower_bound_in_compaction",
                Long.toString(CONF.getChunkSizeLowerBoundInCompaction()))));
    CONF.setMaxInnerCompactionCandidateFileNum(
        Integer.parseInt(
            properties.getProperty(
                "max_inner_compaction_candidate_file_num",
                Integer.toString(CONF.getMaxInnerCompactionCandidateFileNum()))));
    CONF.setMaxCrossCompactionCandidateFileNum(
        Integer.parseInt(
            properties.getProperty(
                "max_cross_compaction_candidate_file_num",
                Integer.toString(CONF.getMaxCrossCompactionCandidateFileNum()))));
    CONF.setMaxCrossCompactionCandidateFileSize(
        Long.parseLong(
            properties.getProperty(
                "max_cross_compaction_candidate_file_size",
                Long.toString(CONF.getMaxCrossCompactionCandidateFileSize()))));

    CONF.setCompactionWriteThroughputMbPerSec(
        Integer.parseInt(
            properties.getProperty(
                "compaction_write_throughput_mb_per_sec",
                Integer.toString(CONF.getCompactionWriteThroughputMbPerSec()))));

    CONF.setEnableCompactionValidation(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_compaction_validation",
                Boolean.toString(CONF.isEnableCompactionValidation()))));

    int rpcSelectorThreadNum =
        Integer.parseInt(
            properties.getProperty(
                "dn_rpc_selector_thread_count",
                Integer.toString(CONF.getDnRpcSelectorThreadCount()).trim()));
    if (rpcSelectorThreadNum <= 0) {
      rpcSelectorThreadNum = 1;
    }

    CONF.setDnRpcSelectorThreadCount(rpcSelectorThreadNum);

    int minConcurrentClientNum =
        Integer.parseInt(
            properties.getProperty(
                "dn_rpc_min_concurrent_client_num",
                Integer.toString(CONF.getDnRpcMinConcurrentClientNum()).trim()));
    if (minConcurrentClientNum <= 0) {
      minConcurrentClientNum = Runtime.getRuntime().availableProcessors();
    }

    CONF.setDnRpcMinConcurrentClientNum(minConcurrentClientNum);

    int maxConcurrentClientNum =
        Integer.parseInt(
            properties.getProperty(
                "dn_rpc_max_concurrent_client_num",
                Integer.toString(CONF.getDnRpcMaxConcurrentClientNum()).trim()));
    if (maxConcurrentClientNum <= 0) {
      maxConcurrentClientNum = 65535;
    }

    CONF.setDnRpcMaxConcurrentClientNum(maxConcurrentClientNum);

    CONF.setEnableWatermark(
        Boolean.parseBoolean(
            properties.getProperty(
                "watermark_module_opened", Boolean.toString(CONF.isEnableWatermark()).trim())));
    CONF.setWatermarkSecretKey(
        properties.getProperty("watermark_secret_key", CONF.getWatermarkSecretKey()));
    CONF.setWatermarkBitString(
        properties.getProperty("watermark_bit_string", CONF.getWatermarkBitString()));
    CONF.setWatermarkMethod(properties.getProperty("watermark_method", CONF.getWatermarkMethod()));

    loadAutoCreateSchemaProps(properties);

    CONF.setTsFileStorageFs(
        properties.getProperty("tsfile_storage_fs", CONF.getTsFileStorageFs().toString()));
    CONF.setCoreSitePath(properties.getProperty("core_site_path", CONF.getCoreSitePath()));
    CONF.setHdfsSitePath(properties.getProperty("hdfs_site_path", CONF.getHdfsSitePath()));
    CONF.setHdfsIp(properties.getProperty("hdfs_ip", CONF.getRawHDFSIp()).split(","));
    CONF.setHdfsPort(properties.getProperty("hdfs_port", CONF.getHdfsPort()));
    CONF.setDfsNameServices(properties.getProperty("dfs_nameservices", CONF.getDfsNameServices()));
    CONF.setDfsHaNamenodes(
        properties.getProperty("dfs_ha_namenodes", CONF.getRawDfsHaNamenodes()).split(","));
    CONF.setDfsHaAutomaticFailoverEnabled(
        Boolean.parseBoolean(
            properties.getProperty(
                "dfs_ha_automatic_failover_enabled",
                String.valueOf(CONF.isDfsHaAutomaticFailoverEnabled()))));
    CONF.setDfsClientFailoverProxyProvider(
        properties.getProperty(
            "dfs_client_failover_proxy_provider", CONF.getDfsClientFailoverProxyProvider()));
    CONF.setUseKerberos(
        Boolean.parseBoolean(
            properties.getProperty("hdfs_use_kerberos", String.valueOf(CONF.isUseKerberos()))));
    CONF.setKerberosKeytabFilePath(
        properties.getProperty("kerberos_keytab_file_path", CONF.getKerberosKeytabFilePath()));
    CONF.setKerberosPrincipal(
        properties.getProperty("kerberos_principal", CONF.getKerberosPrincipal()));

    // the num of memtables in each database
    CONF.setConcurrentWritingTimePartition(
        Integer.parseInt(
            properties.getProperty(
                "concurrent_writing_time_partition",
                String.valueOf(CONF.getConcurrentWritingTimePartition()))));

    // the default fill interval in LinearFill and PreviousFill
    CONF.setDefaultFillInterval(
        Integer.parseInt(
            properties.getProperty(
                "default_fill_interval", String.valueOf(CONF.getDefaultFillInterval()))));

    CONF.setPrimitiveArraySize(
        (Integer.parseInt(
            properties.getProperty(
                "primitive_array_size", String.valueOf(CONF.getPrimitiveArraySize())))));

    CONF.setDnThriftMaxFrameSize(
        Integer.parseInt(
            properties.getProperty(
                "dn_thrift_max_frame_size", String.valueOf(CONF.getDnThriftMaxFrameSize()))));

    if (CONF.getDnThriftMaxFrameSize() < IoTDBConstant.LEFT_SIZE_IN_REQUEST * 2) {
      CONF.setDnThriftMaxFrameSize(IoTDBConstant.LEFT_SIZE_IN_REQUEST * 2);
    }

    CONF.setDnThriftInitBufferSize(
        Integer.parseInt(
            properties.getProperty(
                "dn_thrift_init_buffer_size", String.valueOf(CONF.getDnThriftInitBufferSize()))));

    CONF.setFrequencyIntervalInMinute(
        Integer.parseInt(
            properties.getProperty(
                "frequency_interval_in_minute",
                String.valueOf(CONF.getFrequencyIntervalInMinute()))));

    CONF.setSlowQueryThreshold(
        Long.parseLong(
            properties.getProperty(
                "slow_query_threshold", String.valueOf(CONF.getSlowQueryThreshold()))));

    CONF.setWindowEvaluationThreadCount(
        Integer.parseInt(
            properties.getProperty(
                "window_evaluation_thread_count",
                Integer.toString(CONF.getWindowEvaluationThreadCount()))));
    if (CONF.getWindowEvaluationThreadCount() <= 0) {
      CONF.setWindowEvaluationThreadCount(Runtime.getRuntime().availableProcessors());
    }

    CONF.setMaxPendingWindowEvaluationTasks(
        Integer.parseInt(
            properties.getProperty(
                "max_pending_window_evaluation_tasks",
                Integer.toString(CONF.getMaxPendingWindowEvaluationTasks()))));
    if (CONF.getMaxPendingWindowEvaluationTasks() <= 0) {
      CONF.setMaxPendingWindowEvaluationTasks(64);
    }

    // id table related configuration
    CONF.setDeviceIDTransformationMethod(
        properties.getProperty(
            "device_id_transformation_method", CONF.getDeviceIDTransformationMethod()));

    CONF.setEnableIDTable(
        Boolean.parseBoolean(
            properties.getProperty("enable_id_table", String.valueOf(CONF.isEnableIDTable()))));

    CONF.setEnableIDTableLogFile(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_id_table_log_file", String.valueOf(CONF.isEnableIDTableLogFile()))));

    CONF.setSchemaEngineMode(
        properties.getProperty("schema_engine_mode", String.valueOf(CONF.getSchemaEngineMode())));

    CONF.setEnableLastCache(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_last_cache", Boolean.toString(CONF.isLastCacheEnabled()))));

    if (CONF.getSchemaEngineMode().equals("Rocksdb_based")) {
      CONF.setEnableLastCache(false);
    }

    CONF.setCachedMNodeSizeInSchemaFileMode(
        Integer.parseInt(
            properties.getProperty(
                "cached_mnode_size_in_schema_file_mode",
                String.valueOf(CONF.getCachedMNodeSizeInSchemaFileMode()))));

    CONF.setMinimumSegmentInSchemaFile(
        Short.parseShort(
            properties.getProperty(
                "minimum_schema_file_segment_in_bytes",
                String.valueOf(CONF.getMinimumSegmentInSchemaFile()))));

    CONF.setPageCacheSizeInSchemaFile(
        Integer.parseInt(
            properties.getProperty(
                "page_cache_in_schema_file", String.valueOf(CONF.getPageCacheSizeInSchemaFile()))));

    CONF.setSchemaFileLogSize(
        Integer.parseInt(
            properties.getProperty(
                "schema_file_log_size", String.valueOf(CONF.getSchemaFileLogSize()))));

    // mqtt
    loadMqttProps(properties);

    CONF.setSelectIntoInsertTabletPlanRowLimit(
        Integer.parseInt(
            properties.getProperty(
                "select_into_insert_tablet_plan_row_limit",
                String.valueOf(CONF.getSelectIntoInsertTabletPlanRowLimit()))));
    CONF.setIntoOperationExecutionThreadCount(
        Integer.parseInt(
            properties.getProperty(
                "into_operation_execution_thread_count",
                String.valueOf(CONF.getIntoOperationExecutionThreadCount()))));
    if (CONF.getIntoOperationExecutionThreadCount() <= 0) {
      CONF.setIntoOperationExecutionThreadCount(2);
    }

    CONF.setExtPipeDir(properties.getProperty("ext_pipe_dir", CONF.getExtPipeDir()).trim());

    // At the same time, set TSFileConfig
    TSFileDescriptor.getInstance()
        .getConfig()
        .setTSFileStorageFs(
            FSType.valueOf(
                properties.getProperty("tsfile_storage_fs", CONF.getTsFileStorageFs().name())));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setCoreSitePath(properties.getProperty("core_site_path", CONF.getCoreSitePath()));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setHdfsSitePath(properties.getProperty("hdfs_site_path", CONF.getHdfsSitePath()));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setHdfsIp(properties.getProperty("hdfs_ip", CONF.getRawHDFSIp()).split(","));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setHdfsPort(properties.getProperty("hdfs_port", CONF.getHdfsPort()));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setDfsNameServices(properties.getProperty("dfs_nameservices", CONF.getDfsNameServices()));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setDfsHaNamenodes(
            properties.getProperty("dfs_ha_namenodes", CONF.getRawDfsHaNamenodes()).split(","));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setDfsHaAutomaticFailoverEnabled(
            Boolean.parseBoolean(
                properties.getProperty(
                    "dfs_ha_automatic_failover_enabled",
                    String.valueOf(CONF.isDfsHaAutomaticFailoverEnabled()))));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setDfsClientFailoverProxyProvider(
            properties.getProperty(
                "dfs_client_failover_proxy_provider", CONF.getDfsClientFailoverProxyProvider()));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setPatternMatchingThreshold(
            Integer.parseInt(
                properties.getProperty(
                    "pattern_matching_threshold",
                    String.valueOf(CONF.getPatternMatchingThreshold()))));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setUseKerberos(
            Boolean.parseBoolean(
                properties.getProperty("hdfs_use_kerberos", String.valueOf(CONF.isUseKerberos()))));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setKerberosKeytabFilePath(
            properties.getProperty("kerberos_keytab_file_path", CONF.getKerberosKeytabFilePath()));
    TSFileDescriptor.getInstance()
        .getConfig()
        .setKerberosPrincipal(
            properties.getProperty("kerberos_principal", CONF.getKerberosPrincipal()));
    TSFileDescriptor.getInstance().getConfig().setBatchSize(CONF.getBatchSize());

    // commons
    commonDescriptor.loadCommonProps(properties);
    commonDescriptor.initCommonConfigDir(CONF.getDnSystemDir());

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

    CONF.setTimePartitionInterval(
        DateTimeUtils.convertMilliTimeWithPrecision(
            CONF.getTimePartitionInterval(), CONF.getTimestampPrecision()));
  }

  private void loadAuthorCache(Properties properties) {
    CONF.setAuthorCacheSize(
        Integer.parseInt(
            properties.getProperty(
                "author_cache_size", String.valueOf(CONF.getAuthorCacheSize()))));
    CONF.setAuthorCacheExpireTime(
        Integer.parseInt(
            properties.getProperty(
                "author_cache_expire_time", String.valueOf(CONF.getAuthorCacheExpireTime()))));
  }

  private void loadWALProps(Properties properties) {
    CONF.setWalMode(
        WALMode.valueOf((properties.getProperty("wal_mode", CONF.getWalMode().toString()))));

    int maxWalNodesNum =
        Integer.parseInt(
            properties.getProperty(
                "max_wal_nodes_num", Integer.toString(CONF.getMaxWalNodesNum())));
    if (maxWalNodesNum > 0) {
      CONF.setMaxWalNodesNum(maxWalNodesNum);
    }

    int walBufferSize =
        Integer.parseInt(
            properties.getProperty(
                "wal_buffer_size_in_byte", Integer.toString(CONF.getWalBufferSize())));
    if (walBufferSize > 0) {
      CONF.setWalBufferSize(walBufferSize);
    }

    int walBufferEntrySize =
        Integer.parseInt(
            properties.getProperty(
                "wal_buffer_entry_size_in_byte", Integer.toString(CONF.getWalBufferEntrySize())));
    if (walBufferEntrySize > 0) {
      CONF.setWalBufferEntrySize(walBufferEntrySize);
    }

    int walBufferQueueCapacity =
        Integer.parseInt(
            properties.getProperty(
                "wal_buffer_queue_capacity", Integer.toString(CONF.getWalBufferQueueCapacity())));
    if (walBufferQueueCapacity > 0) {
      CONF.setWalBufferQueueCapacity(walBufferQueueCapacity);
    }

    loadWALHotModifiedProps(properties);
  }

  private void loadWALHotModifiedProps(Properties properties) {
    long fsyncWalDelayInMs =
        Long.parseLong(
            properties.getProperty(
                "fsync_wal_delay_in_ms", Long.toString(CONF.getFsyncWalDelayInMs())));
    if (fsyncWalDelayInMs > 0) {
      CONF.setFsyncWalDelayInMs(fsyncWalDelayInMs);
    }

    long walFileSizeThreshold =
        Long.parseLong(
            properties.getProperty(
                "wal_file_size_threshold_in_byte",
                Long.toString(CONF.getWalFileSizeThresholdInByte())));
    if (walFileSizeThreshold > 0) {
      CONF.setWalFileSizeThresholdInByte(walFileSizeThreshold);
    }

    double walMinEffectiveInfoRatio =
        Double.parseDouble(
            properties.getProperty(
                "wal_min_effective_info_ratio",
                Double.toString(CONF.getWalMinEffectiveInfoRatio())));
    if (walMinEffectiveInfoRatio > 0) {
      CONF.setWalMinEffectiveInfoRatio(walMinEffectiveInfoRatio);
    }

    long walMemTableSnapshotThreshold =
        Long.parseLong(
            properties.getProperty(
                "wal_memtable_snapshot_threshold_in_byte",
                Long.toString(CONF.getWalMemTableSnapshotThreshold())));
    if (walMemTableSnapshotThreshold > 0) {
      CONF.setWalMemTableSnapshotThreshold(walMemTableSnapshotThreshold);
    }

    int maxWalMemTableSnapshotNum =
        Integer.parseInt(
            properties.getProperty(
                "max_wal_memtable_snapshot_num",
                Integer.toString(CONF.getMaxWalMemTableSnapshotNum())));
    if (maxWalMemTableSnapshotNum > 0) {
      CONF.setMaxWalMemTableSnapshotNum(maxWalMemTableSnapshotNum);
    }

    long deleteWalFilesPeriod =
        Long.parseLong(
            properties.getProperty(
                "delete_wal_files_period_in_ms",
                Long.toString(CONF.getDeleteWalFilesPeriodInMs())));
    if (deleteWalFilesPeriod > 0) {
      CONF.setDeleteWalFilesPeriodInMs(deleteWalFilesPeriod);
    }

    long throttleDownThresholdInByte =
        Long.parseLong(
            properties.getProperty(
                "iot_consensus_throttle_threshold_in_byte",
                Long.toString(CONF.getThrottleThreshold())));
    if (throttleDownThresholdInByte > 0) {
      CONF.setThrottleThreshold(throttleDownThresholdInByte);
    }

    long cacheWindowInMs =
        Long.parseLong(
            properties.getProperty(
                "iot_consensus_cache_window_time_in_ms",
                Long.toString(CONF.getCacheWindowTimeInMs())));
    if (cacheWindowInMs > 0) {
      CONF.setCacheWindowTimeInMs(cacheWindowInMs);
    }
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
                    CONF.getMaxBytesPerFragmentInstance()));

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
    CONF.setMqttDir(properties.getProperty("mqtt_root_dir", CONF.getMqttDir()));

    if (properties.getProperty(IoTDBConstant.MQTT_HOST_NAME) != null) {
      CONF.setMqttHost(properties.getProperty(IoTDBConstant.MQTT_HOST_NAME));
    }

    if (properties.getProperty(IoTDBConstant.MQTT_PORT_NAME) != null) {
      CONF.setMqttPort(Integer.parseInt(properties.getProperty(IoTDBConstant.MQTT_PORT_NAME)));
    }

    if (properties.getProperty(IoTDBConstant.MQTT_HANDLER_POOL_SIZE_NAME) != null) {
      CONF.setMqttHandlerPoolSize(
          Integer.parseInt(properties.getProperty(IoTDBConstant.MQTT_HANDLER_POOL_SIZE_NAME)));
    }

    if (properties.getProperty(IoTDBConstant.MQTT_PAYLOAD_FORMATTER_NAME) != null) {
      CONF.setMqttPayloadFormatter(
          properties.getProperty(IoTDBConstant.MQTT_PAYLOAD_FORMATTER_NAME));
    }

    if (properties.getProperty(IoTDBConstant.ENABLE_MQTT) != null) {
      CONF.setEnableMQTTService(
          Boolean.parseBoolean(properties.getProperty(IoTDBConstant.ENABLE_MQTT)));
    }

    if (properties.getProperty(IoTDBConstant.MQTT_MAX_MESSAGE_SIZE) != null) {
      CONF.setMqttMaxMessageSize(
          Integer.parseInt(properties.getProperty(IoTDBConstant.MQTT_MAX_MESSAGE_SIZE)));
    }
  }

  public void loadHotModifiedProps(Properties properties) throws QueryProcessException {
    try {
      // update data dirs
      String dataDirs = properties.getProperty("dn_data_dirs", null);
      if (dataDirs != null) {
        CONF.reloadDataDirs(dataDirs.split(","));
      }

      // update dir strategy, must update after data dirs
      String multiDirStrategyClassName = properties.getProperty("dn_multi_dir_strategy", null);
      if (multiDirStrategyClassName != null
          && !multiDirStrategyClassName.equals(CONF.getDnMultiDirStrategyClassName())) {
        CONF.setDnMultiDirStrategyClassName(multiDirStrategyClassName);
        CONF.confirmMultiDirStrategy();
        DirectoryManager.getInstance().updateDirectoryStrategy();
      }

      // update timed flush & close conf
      loadTimedService(properties);
      StorageEngine.getInstance().rebootTimedService();

      // update tsfile-format config
      loadTsFileProps(properties);

      // update max_deduplicated_path_num
      CONF.setMaxQueryDeduplicatedPathNum(
          Integer.parseInt(
              properties.getProperty(
                  "max_deduplicated_path_num",
                  Integer.toString(CONF.getMaxQueryDeduplicatedPathNum()))));
      // update frequency_interval_in_minute
      CONF.setFrequencyIntervalInMinute(
          Integer.parseInt(
              properties.getProperty(
                  "frequency_interval_in_minute",
                  Integer.toString(CONF.getFrequencyIntervalInMinute()))));
      // update slow_query_threshold
      CONF.setSlowQueryThreshold(
          Long.parseLong(
              properties.getProperty(
                  "slow_query_threshold", Long.toString(CONF.getSlowQueryThreshold()))));
      // update merge_write_throughput_mb_per_sec
      CONF.setCompactionWriteThroughputMbPerSec(
          Integer.parseInt(
              properties.getProperty(
                  "merge_write_throughput_mb_per_sec",
                  Integer.toString(CONF.getCompactionWriteThroughputMbPerSec()))));
      // update insert-tablet-plan's row limit for select-into
      CONF.setSelectIntoInsertTabletPlanRowLimit(
          Integer.parseInt(
              properties.getProperty(
                  "select_into_insert_tablet_plan_row_limit",
                  String.valueOf(CONF.getSelectIntoInsertTabletPlanRowLimit()))));

      // update sync config
      CONF.setMaxNumberOfSyncFileRetry(
          Integer.parseInt(
              properties
                  .getProperty(
                      "max_number_of_sync_file_retry",
                      Integer.toString(CONF.getMaxNumberOfSyncFileRetry()))
                  .trim()));
      CONF.setIpWhiteList(properties.getProperty("ip_white_list", CONF.getIpWhiteList()));

      // update enable query memory estimation for memory control
      CONF.setEnableQueryMemoryEstimation(
          Boolean.parseBoolean(
              properties.getProperty(
                  "enable_query_memory_estimation",
                  Boolean.toString(CONF.isEnableQueryMemoryEstimation()))));

      // update wal config
      long prevDeleteWalFilesPeriodInMs = CONF.getDeleteWalFilesPeriodInMs();
      loadWALHotModifiedProps(properties);
      if (prevDeleteWalFilesPeriodInMs != CONF.getDeleteWalFilesPeriodInMs()) {
        WALManager.getInstance().rebootWALDeleteThread();
      }
    } catch (Exception e) {
      throw new QueryProcessException(String.format("Fail to reload configuration because %s", e));
    }
  }

  public void loadHotModifiedProps() throws QueryProcessException {
    URL url = getPropsUrl(CommonConfig.CONF_FILE_NAME);
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
        CONF.setAllocateMemoryForStorageEngine(
            maxMemoryAvailable * Integer.parseInt(proportions[0].trim()) / proportionSum);
        CONF.setAllocateMemoryForRead(
            maxMemoryAvailable * Integer.parseInt(proportions[1].trim()) / proportionSum);
        CONF.setAllocateMemoryForSchema(
            maxMemoryAvailable * Integer.parseInt(proportions[2].trim()) / proportionSum);
        CONF.setAllocateMemoryForConsensus(
            maxMemoryAvailable * Integer.parseInt(proportions[3].trim()) / proportionSum);
      }
    }

    logger.info("initial allocateMemoryForRead = {}", CONF.getAllocateMemoryForRead());
    logger.info("initial allocateMemoryForWrite = {}", CONF.getAllocateMemoryForStorageEngine());
    logger.info("initial allocateMemoryForSchema = {}", CONF.getAllocateMemoryForSchema());
    logger.info("initial allocateMemoryForConsensus = {}", CONF.getAllocateMemoryForConsensus());

    initSchemaMemoryAllocate(properties);
    initStorageEngineAllocate(properties);

    CONF.setMaxQueryDeduplicatedPathNum(
        Integer.parseInt(
            properties.getProperty(
                "max_deduplicated_path_num",
                Integer.toString(CONF.getMaxQueryDeduplicatedPathNum()))));

    CONF.setEnableQueryMemoryEstimation(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_query_memory_estimation",
                Boolean.toString(CONF.isEnableQueryMemoryEstimation()))));

    String queryMemoryAllocateProportion =
        properties.getProperty("chunk_timeseriesmeta_free_memory_proportion");
    if (queryMemoryAllocateProportion != null) {
      String[] proportions = queryMemoryAllocateProportion.split(":");
      int proportionSum = 0;
      for (String proportion : proportions) {
        proportionSum += Integer.parseInt(proportion.trim());
      }
      long maxMemoryAvailable = CONF.getAllocateMemoryForRead();
      if (proportionSum != 0) {
        try {
          CONF.setAllocateMemoryForBloomFilterCache(
              maxMemoryAvailable * Integer.parseInt(proportions[0].trim()) / proportionSum);
          CONF.setAllocateMemoryForChunkCache(
              maxMemoryAvailable * Integer.parseInt(proportions[1].trim()) / proportionSum);
          CONF.setAllocateMemoryForTimeSeriesMetaDataCache(
              maxMemoryAvailable * Integer.parseInt(proportions[2].trim()) / proportionSum);
          CONF.setAllocateMemoryForCoordinator(
              maxMemoryAvailable * Integer.parseInt(proportions[3].trim()) / proportionSum);
          CONF.setAllocateMemoryForOperators(
              maxMemoryAvailable * Integer.parseInt(proportions[4].trim()) / proportionSum);
          CONF.setAllocateMemoryForDataExchange(
              maxMemoryAvailable * Integer.parseInt(proportions[5].trim()) / proportionSum);
          CONF.setAllocateMemoryForTimeIndex(
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
    if (!CONF.isMetaDataCacheEnable()) {
      long sum =
          CONF.getAllocateMemoryForBloomFilterCache()
              + CONF.getAllocateMemoryForChunkCache()
              + CONF.getAllocateMemoryForTimeSeriesMetaDataCache();
      CONF.setAllocateMemoryForBloomFilterCache(0);
      CONF.setAllocateMemoryForChunkCache(0);
      CONF.setAllocateMemoryForTimeSeriesMetaDataCache(0);
      long partForDataExchange = sum / 2;
      long partForOperators = sum - partForDataExchange;
      CONF.setAllocateMemoryForDataExchange(
          CONF.getAllocateMemoryForDataExchange() + partForDataExchange);
      CONF.setAllocateMemoryForOperators(CONF.getAllocateMemoryForOperators() + partForOperators);
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
    Double.parseDouble(properties.getProperty("flush_time_memory_proportion", "0.05"));
    double timePartitionInfoForWrite =
        ((double) (proportionForTimePartitionInfo)
            / (double) (proportionForMemTable + proportionForTimePartitionInfo));
    CONF.setWriteProportionForMemtable(writeProportion * memtableProportionForWrite);

    CONF.setAllocateMemoryForTimePartitionInfo(
        (long)
            ((writeProportion * timePartitionInfoForWrite)
                * CONF.getAllocateMemoryForStorageEngine()));

    CONF.setCompactionProportion(
        ((double) (proportionForCompaction)
            / (double) (proportionForCompaction + proportionForWrite)));
  }

  private void initSchemaMemoryAllocate(Properties properties) {
    long schemaMemoryTotal = CONF.getAllocateMemoryForSchema();

    int proportionSum = 10;
    int schemaRegionProportion = 8;
    int schemaCacheProportion = 1;
    int partitionCacheProportion = 0;
    int lastCacheProportion = 1;

    String schemaMemoryAllocatePortion =
        properties.getProperty("schema_memory_allocate_proportion");
    if (schemaMemoryAllocatePortion != null) {
      CONF.setDefaultSchemaMemoryConfig(false);
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
      CONF.setDefaultSchemaMemoryConfig(true);
    }

    CONF.setAllocateMemoryForSchemaRegion(
        schemaMemoryTotal * schemaRegionProportion / proportionSum);
    logger.info("allocateMemoryForSchemaRegion = {}", CONF.getAllocateMemoryForSchemaRegion());

    CONF.setAllocateMemoryForSchemaCache(schemaMemoryTotal * schemaCacheProportion / proportionSum);
    logger.info("allocateMemoryForSchemaCache = {}", CONF.getAllocateMemoryForSchemaCache());

    CONF.setAllocateMemoryForPartitionCache(
        schemaMemoryTotal * partitionCacheProportion / proportionSum);
    logger.info("allocateMemoryForPartitionCache = {}", CONF.getAllocateMemoryForPartitionCache());

    CONF.setAllocateMemoryForLastCache(schemaMemoryTotal * lastCacheProportion / proportionSum);
    logger.info("allocateMemoryForLastCache = {}", CONF.getAllocateMemoryForLastCache());
  }

  @SuppressWarnings("squid:S3518") // "proportionSum" can't be zero
  private void loadUDFProps(Properties properties) {
    String initialByteArrayLengthForMemoryControl =
        properties.getProperty("udf_initial_byte_array_length_for_memory_control");
    if (initialByteArrayLengthForMemoryControl != null) {
      CONF.setUdfInitialByteArrayLengthForMemoryControl(
          Integer.parseInt(initialByteArrayLengthForMemoryControl));
    }

    CONF.setUdfDir(properties.getProperty("udf_lib_dir", CONF.getUdfDir()));

    String memoryBudgetInMb = properties.getProperty("udf_memory_budget_in_mb");
    if (memoryBudgetInMb != null) {
      CONF.setUdfMemoryBudgetInMB(
          (float)
              Math.min(Float.parseFloat(memoryBudgetInMb), 0.2 * CONF.getAllocateMemoryForRead()));
    }

    String groupByFillCacheSizeInMB = properties.getProperty("group_by_fill_cache_size_in_mb");
    if (groupByFillCacheSizeInMB != null) {
      CONF.setGroupByFillCacheSizeInMB(Float.parseFloat(groupByFillCacheSizeInMB));
    }

    String readerTransformerCollectorMemoryProportion =
        properties.getProperty("udf_reader_transformer_collector_memory_proportion");
    if (readerTransformerCollectorMemoryProportion != null) {
      String[] proportions = readerTransformerCollectorMemoryProportion.split(":");
      int proportionSum = 0;
      for (String proportion : proportions) {
        proportionSum += Integer.parseInt(proportion.trim());
      }
      float maxMemoryAvailable = CONF.getUdfMemoryBudgetInMB();
      try {
        CONF.setUdfReaderMemoryBudgetInMB(
            maxMemoryAvailable * Integer.parseInt(proportions[0].trim()) / proportionSum);
        CONF.setUdfTransformerMemoryBudgetInMB(
            maxMemoryAvailable * Integer.parseInt(proportions[1].trim()) / proportionSum);
        CONF.setUdfCollectorMemoryBudgetInMB(
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
    CONF.setTriggerDir(properties.getProperty("trigger_lib_dir", CONF.getTriggerDir()));
    CONF.setRetryNumToFindStatefulTrigger(
        Integer.parseInt(
            properties.getProperty(
                "stateful_trigger_retry_num_when_not_found",
                Integer.toString(CONF.getRetryNumToFindStatefulTrigger()))));

    int tlogBufferSize =
        Integer.parseInt(
            properties.getProperty("tlog_buffer_size", Integer.toString(CONF.getTlogBufferSize())));
    if (tlogBufferSize > 0) {
      CONF.setTlogBufferSize(tlogBufferSize);
    }

    CONF.setTriggerForwardMaxQueueNumber(
        Integer.parseInt(
            properties.getProperty(
                "trigger_forward_max_queue_number",
                Integer.toString(CONF.getTriggerForwardMaxQueueNumber()))));
    CONF.setTriggerForwardMaxSizePerQueue(
        Integer.parseInt(
            properties.getProperty(
                "trigger_forward_max_size_per_queue",
                Integer.toString(CONF.getTriggerForwardMaxSizePerQueue()))));
    CONF.setTriggerForwardBatchSize(
        Integer.parseInt(
            properties.getProperty(
                "trigger_forward_batch_size",
                Integer.toString(CONF.getTriggerForwardBatchSize()))));
    CONF.setTriggerForwardHTTPPoolSize(
        Integer.parseInt(
            properties.getProperty(
                "trigger_forward_http_pool_size",
                Integer.toString(CONF.getTriggerForwardHTTPPoolSize()))));
    CONF.setTriggerForwardHTTPPOOLMaxPerRoute(
        Integer.parseInt(
            properties.getProperty(
                "trigger_forward_http_pool_max_per_route",
                Integer.toString(CONF.getTriggerForwardHTTPPOOLMaxPerRoute()))));
    CONF.setTriggerForwardMQTTPoolSize(
        Integer.parseInt(
            properties.getProperty(
                "trigger_forward_mqtt_pool_size",
                Integer.toString(CONF.getTriggerForwardMQTTPoolSize()))));
  }

  private void loadCQProps(Properties properties) {
    CONF.setContinuousQueryThreadNum(
        Integer.parseInt(
            properties.getProperty(
                "continuous_query_thread_num",
                Integer.toString(CONF.getContinuousQueryThreadNum()))));
    if (CONF.getContinuousQueryThreadNum() <= 0) {
      CONF.setContinuousQueryThreadNum(Runtime.getRuntime().availableProcessors() / 2);
    }

    CONF.setContinuousQueryMinimumEveryInterval(
        DateTimeUtils.convertDurationStrToLong(
            properties.getProperty("continuous_query_minimum_every_interval", "1s"),
            CONF.getTimestampPrecision()));
  }

  public void loadClusterProps(Properties properties) {
    String configNodeUrls = properties.getProperty(IoTDBConstant.DN_TARGET_CONFIG_NODE_LIST);
    if (configNodeUrls != null) {
      try {
        CONF.setDnTargetConfigNodeList(NodeUrlUtils.parseTEndPointUrls(configNodeUrls));
      } catch (BadNodeUrlException e) {
        logger.error(
            "Config nodes are set in wrong format, please set them like 127.0.0.1:10710,127.0.0.1:10712");
      }
    }

    CONF.setDnInternalAddress(
        properties.getProperty(IoTDBConstant.DN_INTERNAL_ADDRESS, CONF.getDnInternalAddress()));

    CONF.setDnInternalPort(
        Integer.parseInt(
            properties.getProperty(
                IoTDBConstant.DN_INTERNAL_PORT, Integer.toString(CONF.getDnInternalPort()))));

    CONF.setDnDataRegionConsensusPort(
        Integer.parseInt(
            properties.getProperty(
                "dn_data_region_consensus_port",
                Integer.toString(CONF.getDnDataRegionConsensusPort()))));

    CONF.setDnSchemaRegionConsensusPort(
        Integer.parseInt(
            properties.getProperty(
                "dn_schema_region_consensus_port",
                Integer.toString(CONF.getDnSchemaRegionConsensusPort()))));
    CONF.setDnJoinClusterRetryIntervalMs(
        Long.parseLong(
            properties.getProperty(
                "dn_join_cluster_retry_interval_ms",
                Long.toString(CONF.getDnJoinClusterRetryIntervalMs()))));
  }

  public void loadShuffleProps(Properties properties) {
    CONF.setDnMppDataExchangePort(
        Integer.parseInt(
            properties.getProperty(
                "dn_mpp_data_exchange_port", Integer.toString(CONF.getDnMppDataExchangePort()))));
    CONF.setMppDataExchangeCorePoolSize(
        Integer.parseInt(
            properties.getProperty(
                "mpp_data_exchange_core_pool_size",
                Integer.toString(CONF.getMppDataExchangeCorePoolSize()))));
    CONF.setMppDataExchangeMaxPoolSize(
        Integer.parseInt(
            properties.getProperty(
                "mpp_data_exchange_max_pool_size",
                Integer.toString(CONF.getMppDataExchangeMaxPoolSize()))));
    CONF.setMppDataExchangeKeepAliveTimeInMs(
        Integer.parseInt(
            properties.getProperty(
                "mpp_data_exchange_keep_alive_time_in_ms",
                Integer.toString(CONF.getMppDataExchangeKeepAliveTimeInMs()))));

    CONF.setDriverTaskExecutionTimeSliceInMs(
        Integer.parseInt(
            properties.getProperty(
                "driver_task_execution_time_slice_in_ms",
                Integer.toString(CONF.getDriverTaskExecutionTimeSliceInMs()))));
  }

  /** Get default encode algorithm by data type */
  public TSEncoding getDefaultEncodingByType(TSDataType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return CONF.getDefaultBooleanEncoding();
      case INT32:
        return CONF.getDefaultInt32Encoding();
      case INT64:
        return CONF.getDefaultInt64Encoding();
      case FLOAT:
        return CONF.getDefaultFloatEncoding();
      case DOUBLE:
        return CONF.getDefaultDoubleEncoding();
      default:
        return CONF.getDefaultTextEncoding();
    }
  }

  // These configurations are received from config node when registering
  public void loadGlobalConfig(TGlobalConfig globalConfig) {
    CONF.setSeriesPartitionExecutorClass(globalConfig.getSeriesPartitionExecutorClass());
    CONF.setSeriesPartitionSlotNum(globalConfig.getSeriesPartitionSlotNum());
    CONF.setTimePartitionInterval(
        DateTimeUtils.convertMilliTimeWithPrecision(
            globalConfig.timePartitionInterval, CONF.getTimestampPrecision()));
    CONF.setReadConsistencyLevel(globalConfig.getReadConsistencyLevel());
  }

  public void loadRatisConfig(TRatisConfig ratisConfig) {
    CONF.setDataRatisConsensusLogAppenderBufferSizeMax(ratisConfig.getDataAppenderBufferSize());
    CONF.setSchemaRatisConsensusLogAppenderBufferSizeMax(ratisConfig.getSchemaAppenderBufferSize());

    CONF.setDataRatisConsensusSnapshotTriggerThreshold(
        ratisConfig.getDataSnapshotTriggerThreshold());
    CONF.setSchemaRatisConsensusSnapshotTriggerThreshold(
        ratisConfig.getSchemaSnapshotTriggerThreshold());

    CONF.setDataRatisConsensusLogUnsafeFlushEnable(ratisConfig.isDataLogUnsafeFlushEnable());
    CONF.setSchemaRatisConsensusLogUnsafeFlushEnable(ratisConfig.isSchemaLogUnsafeFlushEnable());

    CONF.setDataRatisConsensusLogSegmentSizeMax(ratisConfig.getDataLogSegmentSizeMax());
    CONF.setSchemaRatisConsensusLogSegmentSizeMax(ratisConfig.getSchemaLogSegmentSizeMax());

    CONF.setDataRatisConsensusGrpcFlowControlWindow(ratisConfig.getDataGrpcFlowControlWindow());
    CONF.setSchemaRatisConsensusGrpcFlowControlWindow(ratisConfig.getSchemaGrpcFlowControlWindow());

    CONF.setDataRatisConsensusLeaderElectionTimeoutMinMs(
        ratisConfig.getDataLeaderElectionTimeoutMin());
    CONF.setSchemaRatisConsensusLeaderElectionTimeoutMinMs(
        ratisConfig.getSchemaLeaderElectionTimeoutMin());

    CONF.setDataRatisConsensusLeaderElectionTimeoutMaxMs(
        ratisConfig.getDataLeaderElectionTimeoutMax());
    CONF.setSchemaRatisConsensusLeaderElectionTimeoutMaxMs(
        ratisConfig.getSchemaLeaderElectionTimeoutMax());

    CONF.setDataRatisConsensusRequestTimeoutMs(ratisConfig.getDataRequestTimeout());
    CONF.setSchemaRatisConsensusRequestTimeoutMs(ratisConfig.getSchemaRequestTimeout());

    CONF.setDataRatisConsensusMaxRetryAttempts(ratisConfig.getDataMaxRetryAttempts());
    CONF.setDataRatisConsensusInitialSleepTimeMs(ratisConfig.getDataInitialSleepTime());
    CONF.setDataRatisConsensusMaxSleepTimeMs(ratisConfig.getDataMaxSleepTime());

    CONF.setSchemaRatisConsensusMaxRetryAttempts(ratisConfig.getSchemaMaxRetryAttempts());
    CONF.setSchemaRatisConsensusInitialSleepTimeMs(ratisConfig.getSchemaInitialSleepTime());
    CONF.setSchemaRatisConsensusMaxSleepTimeMs(ratisConfig.getSchemaMaxSleepTime());

    CONF.setDataRatisConsensusPreserveWhenPurge(ratisConfig.getDataPreserveWhenPurge());
    CONF.setSchemaRatisConsensusPreserveWhenPurge(ratisConfig.getSchemaPreserveWhenPurge());

    CONF.setRatisFirstElectionTimeoutMinMs(ratisConfig.getFirstElectionTimeoutMin());
    CONF.setRatisFirstElectionTimeoutMaxMs(ratisConfig.getFirstElectionTimeoutMax());

    CONF.setSchemaRatisLogMax(ratisConfig.getSchemaRegionRatisLogMax());
    CONF.setDataRatisLogMax(ratisConfig.getDataRegionRatisLogMax());
  }

  public void loadCQConfig(TCQConfig cqConfig) {
    CONF.setCqMinEveryIntervalInMs(cqConfig.getCqMinEveryIntervalInMs());
  }

  public void reclaimConsensusMemory() {
    CONF.setAllocateMemoryForStorageEngine(
        CONF.getAllocateMemoryForStorageEngine() + CONF.getAllocateMemoryForConsensus());
    SystemInfo.getInstance().allocateWriteMemory();
  }

  public void initClusterSchemaMemoryAllocate() {
    if (!CONF.isDefaultSchemaMemoryConfig()) {
      // the config has already been updated as user config in properties file
      return;
    }

    // process the default schema memory allocate

    long schemaMemoryTotal = CONF.getAllocateMemoryForSchema();

    int proportionSum = 10;
    int schemaRegionProportion = 5;
    int schemaCacheProportion = 3;
    int partitionCacheProportion = 1;
    int lastCacheProportion = 1;

    CONF.setAllocateMemoryForSchemaRegion(
        schemaMemoryTotal * schemaRegionProportion / proportionSum);
    logger.info(
        "Cluster allocateMemoryForSchemaRegion = {}", CONF.getAllocateMemoryForSchemaRegion());

    CONF.setAllocateMemoryForSchemaCache(schemaMemoryTotal * schemaCacheProportion / proportionSum);
    logger.info(
        "Cluster allocateMemoryForSchemaCache = {}", CONF.getAllocateMemoryForSchemaCache());

    CONF.setAllocateMemoryForPartitionCache(
        schemaMemoryTotal * partitionCacheProportion / proportionSum);
    logger.info(
        "Cluster allocateMemoryForPartitionCache = {}", CONF.getAllocateMemoryForPartitionCache());

    CONF.setAllocateMemoryForLastCache(schemaMemoryTotal * lastCacheProportion / proportionSum);
    logger.info("Cluster allocateMemoryForLastCache = {}", CONF.getAllocateMemoryForLastCache());
  }

  private static class IoTDBDescriptorHolder {

    private static final IoTDBDescriptor INSTANCE = new IoTDBDescriptor();

    private IoTDBDescriptorHolder() {}
  }
}
