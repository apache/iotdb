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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.compaction.constant.CompactionPriority;
import org.apache.iotdb.db.engine.compaction.cross.CrossCompactionStrategy;
import org.apache.iotdb.db.engine.compaction.inner.InnerCompactionStrategy;
import org.apache.iotdb.db.exception.BadNodeUrlFormatException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.utils.DatetimeUtils;
import org.apache.iotdb.db.service.metrics.MetricsService;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class IoTDBDescriptor {

  private static final Logger logger = LoggerFactory.getLogger(IoTDBDescriptor.class);

  private final IoTDBConfig conf = new IoTDBConfig();

  protected IoTDBDescriptor() {
    loadProps();
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
  public URL getPropsUrl() {
    // Check if a config-directory was specified first.
    String urlString = System.getProperty(IoTDBConstant.IOTDB_CONF, null);
    // If it wasn't, check if a home directory was provided (This usually contains a config)
    if (urlString == null) {
      urlString = System.getProperty(IoTDBConstant.IOTDB_HOME, null);
      if (urlString != null) {
        urlString =
            urlString + File.separatorChar + "conf" + File.separatorChar + IoTDBConfig.CONFIG_NAME;
      } else {
        // If this too wasn't provided, try to find a default config in the root of the classpath.
        URL uri = IoTDBConfig.class.getResource("/" + IoTDBConfig.CONFIG_NAME);
        if (uri != null) {
          return uri;
        }
        logger.warn(
            "Cannot find IOTDB_HOME or IOTDB_CONF environment variable when loading "
                + "config file {}, use default configuration",
            IoTDBConfig.CONFIG_NAME);
        // update all data seriesPath
        conf.updatePath();
        return null;
      }
    }
    // If a config location was provided, but it doesn't end with a properties file,
    // append the default location.
    else if (!urlString.endsWith(".properties")) {
      urlString += (File.separatorChar + IoTDBConfig.CONFIG_NAME);
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
    URL url = getPropsUrl();
    if (url == null) {
      logger.warn("Couldn't load the configuration from any of the known sources.");
      return;
    }

    try (InputStream inputStream = url.openStream()) {

      logger.info("Start to read config file {}", url);
      Properties properties = new Properties();
      properties.load(inputStream);

      conf.setRpcAddress(properties.getProperty("rpc_address", conf.getRpcAddress()));

      loadClusterProps(properties);

      // TODO: Use FQDN  to identify our nodes afterwards
      try {
        replaceHostnameWithIP();
      } catch (Exception e) {
        logger.info(String.format("replace hostname with ip failed, %s", e.getMessage()));
      }

      conf.setRpcThriftCompressionEnable(
          Boolean.parseBoolean(
              properties.getProperty(
                  "rpc_thrift_compression_enable",
                  Boolean.toString(conf.isRpcThriftCompressionEnable()))));

      conf.setRpcAdvancedCompressionEnable(
          Boolean.parseBoolean(
              properties.getProperty(
                  "rpc_advanced_compression_enable",
                  Boolean.toString(conf.isRpcAdvancedCompressionEnable()))));

      conf.setRpcPort(
          Integer.parseInt(
              properties.getProperty("rpc_port", Integer.toString(conf.getRpcPort()))));

      conf.setMppPort(
          Integer.parseInt(
              properties.getProperty("mpp_port", Integer.toString(conf.getRpcPort()))));

      conf.setEnableInfluxDBRpcService(
          Boolean.parseBoolean(
              properties.getProperty(
                  "enable_influxdb_rpc_service",
                  Boolean.toString(conf.isEnableInfluxDBRpcService()))));

      conf.setInfluxDBRpcPort(
          Integer.parseInt(
              properties.getProperty(
                  "influxdb_rpc_port", Integer.toString(conf.getInfluxDBRpcPort()))));

      conf.setTimestampPrecision(
          properties.getProperty("timestamp_precision", conf.getTimestampPrecision()));

      conf.setBufferedArraysMemoryProportion(
          Double.parseDouble(
              properties.getProperty(
                  "buffered_arrays_memory_proportion",
                  Double.toString(conf.getBufferedArraysMemoryProportion()))));

      conf.setTimeIndexMemoryProportion(
          Double.parseDouble(
              properties.getProperty(
                  "time_index_memory_proportion",
                  Double.toString(conf.getTimeIndexMemoryProportion()))));

      conf.setFlushProportion(
          Double.parseDouble(
              properties.getProperty(
                  "flush_proportion", Double.toString(conf.getFlushProportion()))));

      conf.setRejectProportion(
          Double.parseDouble(
              properties.getProperty(
                  "reject_proportion", Double.toString(conf.getRejectProportion()))));

      conf.setStorageGroupSizeReportThreshold(
          Long.parseLong(
              properties.getProperty(
                  "storage_group_report_threshold",
                  Long.toString(conf.getStorageGroupSizeReportThreshold()))));

      conf.setMetaDataCacheEnable(
          Boolean.parseBoolean(
              properties.getProperty(
                  "meta_data_cache_enable", Boolean.toString(conf.isMetaDataCacheEnable()))));

      conf.setEnableLastCache(
          Boolean.parseBoolean(
              properties.getProperty(
                  "enable_last_cache", Boolean.toString(conf.isLastCacheEnabled()))));

      initMemoryAllocate(properties);

      loadWALProps(properties);

      String systemDir = properties.getProperty("system_dir");
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

      conf.setTracingDir(properties.getProperty("tracing_dir", conf.getTracingDir()));

      conf.setDataDirs(properties.getProperty("data_dirs", conf.getDataDirs()[0]).split(","));

      conf.setWalDir(properties.getProperty("wal_dir", conf.getWalDir()));

      conf.setSyncDir(properties.getProperty("sync_dir", conf.getSyncDir()));

      conf.setConsensusDir(properties.getProperty("consensus_dir", conf.getConsensusDir()));

      int mlogBufferSize =
          Integer.parseInt(
              properties.getProperty(
                  "mlog_buffer_size", Integer.toString(conf.getMlogBufferSize())));
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

      conf.setMultiDirStrategyClassName(
          properties.getProperty("multi_dir_strategy", conf.getMultiDirStrategyClassName()));

      conf.setBatchSize(
          Integer.parseInt(
              properties.getProperty("batch_size", Integer.toString(conf.getBatchSize()))));

      conf.setEnableMemControl(
          (Boolean.parseBoolean(
              properties.getProperty(
                  "enable_mem_control", Boolean.toString(conf.isEnableMemControl())))));
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

      conf.setEstimatedSeriesSize(
          Integer.parseInt(
              properties.getProperty(
                  "estimated_series_size", Integer.toString(conf.getEstimatedSeriesSize()))));

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

      conf.setCrossCompactionStrategy(
          CrossCompactionStrategy.getCrossCompactionStrategy(
              properties.getProperty(
                  "cross_compaction_strategy", conf.getCrossCompactionStrategy().toString())));

      conf.setInnerCompactionStrategy(
          InnerCompactionStrategy.getInnerCompactionStrategy(
              properties.getProperty(
                  "inner_compaction_strategy", conf.getInnerCompactionStrategy().toString())));

      conf.setCompactionPriority(
          CompactionPriority.valueOf(
              properties.getProperty(
                  "compaction_priority", conf.getCompactionPriority().toString())));

      conf.setQueryTimeoutThreshold(
          Integer.parseInt(
              properties.getProperty(
                  "query_timeout_threshold", Integer.toString(conf.getQueryTimeoutThreshold()))));

      conf.setSessionTimeoutThreshold(
          Integer.parseInt(
              properties.getProperty(
                  "session_timeout_threshold",
                  Integer.toString(conf.getSessionTimeoutThreshold()))));
      conf.setPipeServerPort(
          Integer.parseInt(
              properties
                  .getProperty("pipe_server_port", Integer.toString(conf.getPipeServerPort()))
                  .trim()));
      conf.setMaxNumberOfSyncFileRetry(
          Integer.parseInt(
              properties
                  .getProperty(
                      "max_number_of_sync_file_retry",
                      Integer.toString(conf.getMaxNumberOfSyncFileRetry()))
                  .trim()));

      conf.setIpWhiteList(properties.getProperty("ip_white_list", conf.getIpWhiteList()));

      conf.setConcurrentFlushThread(
          Integer.parseInt(
              properties.getProperty(
                  "concurrent_flush_thread", Integer.toString(conf.getConcurrentFlushThread()))));

      if (conf.getConcurrentFlushThread() <= 0) {
        conf.setConcurrentFlushThread(Runtime.getRuntime().availableProcessors());
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

      conf.setIndexBufferSize(
          Long.parseLong(
              properties.getProperty(
                  "index_buffer_size", Long.toString(conf.getIndexBufferSize()))));
      // end: index parameter setting

      conf.setConcurrentQueryThread(
          Integer.parseInt(
              properties.getProperty(
                  "concurrent_query_thread", Integer.toString(conf.getConcurrentQueryThread()))));

      if (conf.getConcurrentQueryThread() <= 0) {
        conf.setConcurrentQueryThread(Runtime.getRuntime().availableProcessors());
      }

      conf.setConcurrentSubRawQueryThread(
          Integer.parseInt(
              properties.getProperty(
                  "concurrent_sub_rawQuery_thread",
                  Integer.toString(conf.getConcurrentSubRawQueryThread()))));

      if (conf.getConcurrentSubRawQueryThread() <= 0) {
        conf.setConcurrentSubRawQueryThread(Runtime.getRuntime().availableProcessors());
      }

      conf.setRawQueryBlockingQueueCapacity(
          Integer.parseInt(
              properties.getProperty(
                  "raw_query_blocking_queue_capacity",
                  Integer.toString(conf.getRawQueryBlockingQueueCapacity()))));

      conf.setSchemaRegionCacheSize(
          Integer.parseInt(
              properties
                  .getProperty(
                      "metadata_node_cache_size", Integer.toString(conf.getSchemaRegionCacheSize()))
                  .trim()));

      conf.setmRemoteSchemaCacheSize(
          Integer.parseInt(
              properties
                  .getProperty(
                      "remote_schema_cache_size",
                      Integer.toString(conf.getmRemoteSchemaCacheSize()))
                  .trim()));

      conf.setLanguageVersion(
          properties.getProperty("language_version", conf.getLanguageVersion()).trim());

      if (properties.containsKey("chunk_buffer_pool_enable")) {
        conf.setChunkBufferPoolEnable(
            Boolean.parseBoolean(properties.getProperty("chunk_buffer_pool_enable")));
      }

      conf.setEnableExternalSort(
          Boolean.parseBoolean(
              properties.getProperty(
                  "enable_external_sort", Boolean.toString(conf.isEnableExternalSort()))));
      conf.setExternalSortThreshold(
          Integer.parseInt(
              properties.getProperty(
                  "external_sort_threshold", Integer.toString(conf.getExternalSortThreshold()))));
      conf.setUpgradeThreadNum(
          Integer.parseInt(
              properties.getProperty(
                  "upgrade_thread_num", Integer.toString(conf.getUpgradeThreadNum()))));
      conf.setCrossCompactionMemoryBudget(
          Long.parseLong(
              properties.getProperty(
                  "cross_compaction_memory_budget",
                  Long.toString(conf.getCrossCompactionMemoryBudget()))));
      conf.setCrossCompactionFileSelectionTimeBudget(
          Long.parseLong(
              properties.getProperty(
                  "cross_compaction_file_selection_time_budget",
                  Long.toString(conf.getCrossCompactionFileSelectionTimeBudget()))));
      conf.setMergeIntervalSec(
          Long.parseLong(
              properties.getProperty(
                  "merge_interval_sec", Long.toString(conf.getMergeIntervalSec()))));
      conf.setConcurrentCompactionThread(
          Integer.parseInt(
              properties.getProperty(
                  "concurrent_compaction_thread",
                  Integer.toString(conf.getConcurrentCompactionThread()))));
      conf.setTargetCompactionFileSize(
          Long.parseLong(
              properties.getProperty(
                  "target_compaction_file_size",
                  Long.toString(conf.getTargetCompactionFileSize()))));
      conf.setTargetChunkSize(
          Long.parseLong(
              properties.getProperty(
                  "target_chunk_size", Long.toString(conf.getTargetChunkSize()))));
      conf.setTargetChunkPointNum(
          Long.parseLong(
              properties.getProperty(
                  "target_chunk_point_num", Long.toString(conf.getTargetChunkPointNum()))));
      conf.setChunkPointNumLowerBoundInCompaction(
          Long.parseLong(
              properties.getProperty(
                  "chunk_size_lower_bound_in_compaction",
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

      conf.setCompactionWriteThroughputMbPerSec(
          Integer.parseInt(
              properties.getProperty(
                  "compaction_write_throughput_mb_per_sec",
                  Integer.toString(conf.getCompactionWriteThroughputMbPerSec()))));

      conf.setEnablePartialInsert(
          Boolean.parseBoolean(
              properties.getProperty(
                  "enable_partial_insert", String.valueOf(conf.isEnablePartialInsert()))));

      conf.setEnablePerformanceStat(
          Boolean.parseBoolean(
              properties
                  .getProperty(
                      "enable_performance_stat", Boolean.toString(conf.isEnablePerformanceStat()))
                  .trim()));

      int maxConcurrentClientNum =
          Integer.parseInt(
              properties.getProperty(
                  "rpc_max_concurrent_client_num",
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
      conf.setWatermarkMethod(
          properties.getProperty("watermark_method", conf.getWatermarkMethod()));

      loadAutoCreateSchemaProps(properties);

      conf.setTsFileStorageFs(
          properties.getProperty("tsfile_storage_fs", conf.getTsFileStorageFs().toString()));
      conf.setCoreSitePath(properties.getProperty("core_site_path", conf.getCoreSitePath()));
      conf.setHdfsSitePath(properties.getProperty("hdfs_site_path", conf.getHdfsSitePath()));
      conf.setHdfsIp(properties.getProperty("hdfs_ip", conf.getRawHDFSIp()).split(","));
      conf.setHdfsPort(properties.getProperty("hdfs_port", conf.getHdfsPort()));
      conf.setDfsNameServices(
          properties.getProperty("dfs_nameservices", conf.getDfsNameServices()));
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

      conf.setDefaultTTL(
          Long.parseLong(
              properties.getProperty("default_ttl", String.valueOf(conf.getDefaultTTL()))));

      // the num of memtables in each storage group
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
                  "thrift_max_frame_size", String.valueOf(conf.getThriftMaxFrameSize()))));

      if (conf.getThriftMaxFrameSize() < IoTDBConstant.LEFT_SIZE_IN_REQUEST * 2) {
        conf.setThriftMaxFrameSize(IoTDBConstant.LEFT_SIZE_IN_REQUEST * 2);
      }

      conf.setThriftDefaultBufferSize(
          Integer.parseInt(
              properties.getProperty(
                  "thrift_init_buffer_size", String.valueOf(conf.getThriftDefaultBufferSize()))));

      conf.setFrequencyIntervalInMinute(
          Integer.parseInt(
              properties.getProperty(
                  "frequency_interval_in_minute",
                  String.valueOf(conf.getFrequencyIntervalInMinute()))));

      conf.setSlowQueryThreshold(
          Long.parseLong(
              properties.getProperty(
                  "slow_query_threshold", String.valueOf(conf.getSlowQueryThreshold()))));

      conf.setVirtualStorageGroupNum(
          Integer.parseInt(
              properties.getProperty(
                  "virtual_storage_group_num", String.valueOf(conf.getVirtualStorageGroupNum()))));

      conf.setRecoveryLogIntervalInMs(
          Long.parseLong(
              properties.getProperty(
                  "recovery_log_interval_in_ms",
                  String.valueOf(conf.getRecoveryLogIntervalInMs()))));

      conf.setConcurrentWindowEvaluationThread(
          Integer.parseInt(
              properties.getProperty(
                  "concurrent_window_evaluation_thread",
                  Integer.toString(conf.getConcurrentWindowEvaluationThread()))));
      if (conf.getConcurrentWindowEvaluationThread() <= 0) {
        conf.setConcurrentWindowEvaluationThread(Runtime.getRuntime().availableProcessors());
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
      // mqtt
      loadMqttProps(properties);

      conf.setAuthorizerProvider(
          properties.getProperty("authorizer_provider_class", conf.getAuthorizerProvider()));
      // if using org.apache.iotdb.db.auth.authorizer.OpenIdAuthorizer, openID_url is needed.
      conf.setOpenIdProviderUrl(properties.getProperty("openID_url", conf.getOpenIdProviderUrl()));

      conf.setEnablePartition(
          Boolean.parseBoolean(
              properties.getProperty(
                  "enable_partition", String.valueOf(conf.isEnablePartition()))));

      conf.setPartitionInterval(
          Long.parseLong(
              properties.getProperty(
                  "partition_interval", String.valueOf(conf.getPartitionInterval()))));

      conf.setAdminName(properties.getProperty("admin_name", conf.getAdminName()));

      conf.setAdminPassword(properties.getProperty("admin_password", conf.getAdminPassword()));

      conf.setSelectIntoInsertTabletPlanRowLimit(
          Integer.parseInt(
              properties.getProperty(
                  "select_into_insert_tablet_plan_row_limit",
                  String.valueOf(conf.getSelectIntoInsertTabletPlanRowLimit()))));

      conf.setInsertMultiTabletEnableMultithreadingColumnThreshold(
          Integer.parseInt(
              properties.getProperty(
                  "insert_multi_tablet_enable_multithreading_column_threshold",
                  String.valueOf(conf.getInsertMultiTabletEnableMultithreadingColumnThreshold()))));

      conf.setEncryptDecryptProvider(
          properties.getProperty(
              "iotdb_server_encrypt_decrypt_provider", conf.getEncryptDecryptProvider()));

      conf.setEncryptDecryptProviderParameter(
          properties.getProperty(
              "iotdb_server_encrypt_decrypt_provider_parameter",
              conf.getEncryptDecryptProviderParameter()));

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
          .setDfsNameServices(
              properties.getProperty("dfs_nameservices", conf.getDfsNameServices()));
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
          .setUseKerberos(
              Boolean.parseBoolean(
                  properties.getProperty(
                      "hdfs_use_kerberos", String.valueOf(conf.isUseKerberos()))));
      TSFileDescriptor.getInstance()
          .getConfig()
          .setKerberosKeytabFilePath(
              properties.getProperty(
                  "kerberos_keytab_file_path", conf.getKerberosKeytabFilePath()));
      TSFileDescriptor.getInstance()
          .getConfig()
          .setKerberosPrincipal(
              properties.getProperty("kerberos_principal", conf.getKerberosPrincipal()));
      TSFileDescriptor.getInstance().getConfig().setBatchSize(conf.getBatchSize());

      // timed flush memtable, timed close tsfile
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
    } catch (FileNotFoundException e) {
      logger.warn("Fail to find config file {}", url, e);
    } catch (IOException e) {
      logger.warn("Cannot load config file, use default configuration", e);
    } catch (Exception e) {
      logger.warn("Incorrect format in config file, use default configuration", e);
    } finally {
      // update all data seriesPath
      conf.updatePath();
    }
  }

  // to keep consistent with the cluster module.
  private void replaceHostnameWithIP() throws UnknownHostException, BadNodeUrlFormatException {
    boolean isInvalidRpcIp = InetAddresses.isInetAddress(conf.getRpcAddress());
    if (!isInvalidRpcIp) {
      conf.setRpcAddress(InetAddress.getByName(conf.getRpcAddress()).getHostAddress());
    }

    boolean isInvalidInternalIp = InetAddresses.isInetAddress(conf.getInternalIp());
    if (!isInvalidInternalIp) {
      conf.setInternalIp(InetAddress.getByName(conf.getInternalIp()).getHostAddress());
    }

    List<String> newConfigNodeUrls = new ArrayList<>();
    for (String nodeUrl : conf.getConfigNodeUrls()) {
      String[] splits = nodeUrl.split(":");
      if (splits.length != 2) {
        throw new BadNodeUrlFormatException(nodeUrl);
      }
      String nodeIP = splits[0];
      boolean isInvalidNodeIp = InetAddresses.isInetAddress(nodeIP);
      if (!isInvalidNodeIp) {
        String newNodeIP = InetAddress.getByName(nodeIP).getHostAddress();
        newConfigNodeUrls.add(newNodeIP + ":" + splits[1]);
      } else {
        newConfigNodeUrls.add(nodeUrl);
      }
    }
    conf.setConfigNodeUrls(newConfigNodeUrls);
    logger.debug(
        "after replace, the rpcIP={}, internalIP={}, configNodeUrls={}",
        conf.getRpcAddress(),
        conf.getInternalIp(),
        conf.getConfigNodeUrls());
  }

  private void loadWALProps(Properties properties) {
    conf.setEnableWal(
        Boolean.parseBoolean(
            properties.getProperty("enable_wal", Boolean.toString(conf.isEnableWal()))));

    conf.setFlushWalThreshold(
        Integer.parseInt(
            properties.getProperty(
                "flush_wal_threshold", Integer.toString(conf.getFlushWalThreshold()))));

    conf.setForceWalPeriodInMs(
        Long.parseLong(
            properties.getProperty(
                "force_wal_period_in_ms", Long.toString(conf.getForceWalPeriodInMs()))));

    conf.setEnableDiscardOutOfOrderData(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_discard_out_of_order_data",
                Boolean.toString(conf.isEnableDiscardOutOfOrderData()))));

    int walBufferSize =
        Integer.parseInt(
            properties.getProperty("wal_buffer_size", Integer.toString(conf.getWalBufferSize())));
    if (walBufferSize > 0) {
      conf.setWalBufferSize(walBufferSize);
    }

    int maxWalBytebufferNumForEachPartition =
        Integer.parseInt(
            properties.getProperty(
                "max_wal_bytebuffer_num_for_each_partition",
                Integer.toString(conf.getMaxWalBytebufferNumForEachPartition())));
    if (maxWalBytebufferNumForEachPartition > 0) {
      conf.setMaxWalBytebufferNumForEachPartition(maxWalBytebufferNumForEachPartition);
    }

    long poolTrimIntervalInMS =
        Long.parseLong(
            properties.getProperty(
                "wal_pool_trim_interval_ms", Long.toString(conf.getWalPoolTrimIntervalInMS())));
    if (poolTrimIntervalInMS > 0) {
      conf.setWalPoolTrimIntervalInMS(poolTrimIntervalInMS);
    }

    long registerBufferSleepIntervalInMs =
        Long.parseLong(
            properties.getProperty(
                "register_buffer_sleep_interval_in_ms",
                Long.toString(conf.getRegisterBufferSleepIntervalInMs())));
    if (registerBufferSleepIntervalInMs > 0) {
      conf.setRegisterBufferSleepIntervalInMs(registerBufferSleepIntervalInMs);
    }

    long registerBufferRejectThresholdInMs =
        Long.parseLong(
            properties.getProperty(
                "register_buffer_reject_threshold_in_ms",
                Long.toString(conf.getRegisterBufferRejectThresholdInMs())));
    if (registerBufferRejectThresholdInMs > 0) {
      conf.setRegisterBufferRejectThresholdInMs(registerBufferRejectThresholdInMs);
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
  }

  // Mqtt related
  private void loadMqttProps(Properties properties) {
    conf.setMqttDir(properties.getProperty("mqtt_root_dir", conf.getMqttDir()));

    if (properties.getProperty(IoTDBConstant.MQTT_HOST_NAME) != null) {
      conf.setMqttHost(properties.getProperty(IoTDBConstant.MQTT_HOST_NAME));
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

  // timed flush memtable, timed close tsfile
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

    conf.setEnableTimedCloseTsFile(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_timed_close_tsfile", Boolean.toString(conf.isEnableTimedCloseTsFile()))));

    long closeTsFileIntervalAfterFlushing =
        Long.parseLong(
            properties
                .getProperty(
                    "close_tsfile_interval_after_flushing_in_ms",
                    Long.toString(conf.getCloseTsFileIntervalAfterFlushing()))
                .trim());
    if (closeTsFileIntervalAfterFlushing > 0) {
      conf.setCloseTsFileIntervalAfterFlushing(closeTsFileIntervalAfterFlushing);
    }

    long closeTsFileCheckInterval =
        Long.parseLong(
            properties
                .getProperty(
                    "close_tsfile_check_interval_in_ms",
                    Long.toString(conf.getCloseTsFileCheckInterval()))
                .trim());
    if (closeTsFileCheckInterval > 0) {
      conf.setCloseTsFileCheckInterval(closeTsFileCheckInterval);
    }
  }

  public void loadHotModifiedProps(Properties properties) throws QueryProcessException {
    try {
      // update data dirs
      String dataDirs = properties.getProperty("data_dirs", null);
      if (dataDirs != null) {
        conf.reloadDataDirs(dataDirs.split(","));
      }

      // update dir strategy
      String multiDirStrategyClassName = properties.getProperty("multi_dir_strategy", null);
      if (multiDirStrategyClassName != null
          && !multiDirStrategyClassName.equals(conf.getMultiDirStrategyClassName())) {
        conf.setMultiDirStrategyClassName(multiDirStrategyClassName);
        DirectoryManager.getInstance().updateDirectoryStrategy();
      }

      // update WAL conf
      loadWALProps(properties);

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
              properties.getProperty(
                  "max_deduplicated_path_num",
                  Integer.toString(conf.getMaxQueryDeduplicatedPathNum()))));
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
      conf.setPipeServerPort(
          Integer.parseInt(
              properties.getProperty(
                  "pipe_server_port", String.valueOf(conf.getPipeServerPort()))));
      conf.setMaxNumberOfSyncFileRetry(
          Integer.parseInt(
              properties
                  .getProperty(
                      "max_number_of_sync_file_retry",
                      Integer.toString(conf.getMaxNumberOfSyncFileRetry()))
                  .trim()));
      conf.setIpWhiteList(properties.getProperty("ip_white_list", conf.getIpWhiteList()));
    } catch (Exception e) {
      throw new QueryProcessException(String.format("Fail to reload configuration because %s", e));
    }
  }

  public void loadHotModifiedProps() throws QueryProcessException {
    URL url = getPropsUrl();
    if (url == null) {
      logger.warn("Couldn't load the configuration from any of the known sources.");
      return;
    }

    try (InputStream inputStream = url.openStream()) {
      logger.info("Start to reload config file {}", url);
      Properties properties = new Properties();
      properties.load(inputStream);
      loadHotModifiedProps(properties);
    } catch (Exception e) {
      logger.warn("Fail to reload config file {}", url, e);
      throw new QueryProcessException(
          String.format("Fail to reload config file %s because %s", url, e.getMessage()));
    }
    ReloadLevel reloadLevel = MetricConfigDescriptor.getInstance().loadHotProperties();
    MetricsService.getInstance().reloadProperties(reloadLevel);
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
        conf.setAllocateMemoryForWrite(
            maxMemoryAvailable * Integer.parseInt(proportions[0].trim()) / proportionSum);
        conf.setAllocateMemoryForRead(
            maxMemoryAvailable * Integer.parseInt(proportions[1].trim()) / proportionSum);
        conf.setAllocateMemoryForSchema(
            maxMemoryAvailable * Integer.parseInt(proportions[2].trim()) / proportionSum);
      }
    }

    logger.info("allocateMemoryForRead = {}", conf.getAllocateMemoryForRead());
    logger.info("allocateMemoryForWrite = {}", conf.getAllocateMemoryForWrite());
    logger.info("allocateMemoryForSchema = {}", conf.getAllocateMemoryForSchema());

    conf.setMaxQueryDeduplicatedPathNum(
        Integer.parseInt(
            properties.getProperty(
                "max_deduplicated_path_num",
                Integer.toString(conf.getMaxQueryDeduplicatedPathNum()))));

    if (!conf.isMetaDataCacheEnable()) {
      return;
    }

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
          conf.setAllocateMemoryForReadWithoutCache(
              maxMemoryAvailable * Integer.parseInt(proportions[3].trim()) / proportionSum);
        } catch (Exception e) {
          throw new RuntimeException(
              "Each subsection of configuration item chunkmeta_chunk_timeseriesmeta_free_memory_proportion"
                  + " should be an integer, which is "
                  + queryMemoryAllocateProportion);
        }
      }
    }
  }

  @SuppressWarnings("squid:S3518") // "proportionSum" can't be zero
  private void loadUDFProps(Properties properties) {
    String initialByteArrayLengthForMemoryControl =
        properties.getProperty("udf_initial_byte_array_length_for_memory_control");
    if (initialByteArrayLengthForMemoryControl != null) {
      conf.setUdfInitialByteArrayLengthForMemoryControl(
          Integer.parseInt(initialByteArrayLengthForMemoryControl));
    }

    conf.setUdfDir(properties.getProperty("udf_root_dir", conf.getUdfDir()));

    String memoryBudgetInMb = properties.getProperty("udf_memory_budget_in_mb");
    if (memoryBudgetInMb != null) {
      conf.setUdfMemoryBudgetInMB(
          (float)
              Math.min(Float.parseFloat(memoryBudgetInMb), 0.2 * conf.getAllocateMemoryForRead()));
    }

    String groupByFillCacheSizeInMB = properties.getProperty("group_by_fill_cache_size_in_mb");
    if (groupByFillCacheSizeInMB != null) {
      conf.setGroupByFillCacheSizeInMB(Float.parseFloat(groupByFillCacheSizeInMB));
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
    conf.setTriggerDir(properties.getProperty("trigger_root_dir", conf.getTriggerDir()));

    int tlogBufferSize =
        Integer.parseInt(
            properties.getProperty("tlog_buffer_size", Integer.toString(conf.getTlogBufferSize())));
    if (tlogBufferSize > 0) {
      conf.setTlogBufferSize(tlogBufferSize);
    }
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

    conf.setMaxPendingContinuousQueryTasks(
        Integer.parseInt(
            properties.getProperty(
                "max_pending_continuous_query_tasks",
                Integer.toString(conf.getMaxPendingContinuousQueryTasks()))));
    if (conf.getMaxPendingContinuousQueryTasks() <= 0) {
      conf.setMaxPendingContinuousQueryTasks(64);
    }

    conf.setContinuousQueryMinimumEveryInterval(
        DatetimeUtils.convertDurationStrToLong(
            properties.getProperty("continuous_query_minimum_every_interval", "1s"),
            conf.getTimestampPrecision()));

    conf.setCqlogBufferSize(
        Integer.parseInt(
            properties.getProperty(
                "cqlog_buffer_size", Integer.toString(conf.getCqlogBufferSize()))));
  }

  public void loadClusterProps(Properties properties) {
    String configNodeUrls = properties.getProperty("config_nodes");
    if (configNodeUrls != null) {
      List<String> urlList = getNodeUrlList(configNodeUrls);
      conf.setConfigNodeUrls(urlList);
    }

    conf.setInternalIp(properties.getProperty("internal_ip", conf.getInternalIp()));

    conf.setInternalPort(
        Integer.parseInt(
            properties.getProperty("internal_port", Integer.toString(conf.getInternalPort()))));

    conf.setConsensusPort(
        Integer.parseInt(
            properties.getProperty("consensus_port", Integer.toString(conf.getConsensusPort()))));
  }

  public void loadShuffleProps(Properties properties) {
    conf.setDataBlockManagerPort(
        Integer.parseInt(
            properties.getProperty(
                "data_block_manager_port", Integer.toString(conf.getDataBlockManagerPort()))));
    conf.setDataBlockManagerCorePoolSize(
        Integer.parseInt(
            properties.getProperty(
                "data_block_manager_core_pool_size",
                Integer.toString(conf.getDataBlockManagerCorePoolSize()))));
    conf.setDataBlockManagerMaxPoolSize(
        Integer.parseInt(
            properties.getProperty(
                "data_block_manager_max_pool_size",
                Integer.toString(conf.getDataBlockManagerMaxPoolSize()))));
    conf.setDataBlockManagerKeepAliveTimeInMs(
        Integer.parseInt(
            properties.getProperty(
                "data_block_manager_keep_alive_time_in_ms",
                Integer.toString(conf.getDataBlockManagerKeepAliveTimeInMs()))));
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

  /**
   * Split the node urls as one list.
   *
   * @param nodeUrls the config node urls.
   * @return the node urls as a list.
   */
  public static List<String> getNodeUrlList(String nodeUrls) {
    if (nodeUrls == null) {
      return Collections.emptyList();
    }
    List<String> urlList = new ArrayList<>();
    String[] split = nodeUrls.split(",");
    for (String nodeUrl : split) {
      nodeUrl = nodeUrl.trim();
      if ("".equals(nodeUrl)) {
        continue;
      }
      urlList.add(nodeUrl);
    }
    return urlList;
  }

  private static class IoTDBDescriptorHolder {

    private static final IoTDBDescriptor INSTANCE = new IoTDBDescriptor();

    private IoTDBDescriptorHolder() {}
  }
}
