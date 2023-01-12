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

  private static class IoTDBDescriptorHolder {

    private static final IoTDBDescriptor INSTANCE = new IoTDBDescriptor();

    private IoTDBDescriptorHolder() {
      // Empty constructor
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
    /* DataNode RPC Configuration */
    loadDataNodeRPCConfiguration(properties);

    /* Target ConfigNodes */
    loadTargetConfigNodes(properties);

    /* Connection Configuration */
    loadConnectionConfiguration(properties);

    /* Directory Configuration */
    loadDirectoryConfiguration(properties);

    /* Compaction Configurations */
    // TODO: Move to CommonDescriptor
    loadCompactionConfigurations(properties);


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

    // the default fill interval in LinearFill and PreviousFill
    CONF.setDefaultFillInterval(
        Integer.parseInt(
            properties.getProperty(
                "default_fill_interval", String.valueOf(CONF.getDefaultFillInterval()))));

    if (CONF.getDnThriftMaxFrameSize() < IoTDBConstant.LEFT_SIZE_IN_REQUEST * 2) {
      CONF.setDnThriftMaxFrameSize(IoTDBConstant.LEFT_SIZE_IN_REQUEST * 2);
    }

    CONF.setFrequencyIntervalInMinute(
        Integer.parseInt(
            properties.getProperty(
                "frequency_interval_in_minute",
                String.valueOf(CONF.getFrequencyIntervalInMinute()))));

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

  private void loadDataNodeRPCConfiguration(Properties properties) {
    CONF.setDnRpcAddress(
      properties.getProperty(IoTDBConstant.DN_RPC_ADDRESS, CONF.getDnRpcAddress()).trim());

    CONF.setDnRpcPort(
      Integer.parseInt(
        properties
          .getProperty(IoTDBConstant.DN_RPC_PORT, Integer.toString(CONF.getDnRpcPort()))
          .trim()));

    CONF.setDnInternalAddress(
      properties.getProperty(IoTDBConstant.DN_INTERNAL_ADDRESS, CONF.getDnInternalAddress()));

    CONF.setDnInternalPort(
      Integer.parseInt(
        properties.getProperty(
          IoTDBConstant.DN_INTERNAL_PORT, Integer.toString(CONF.getDnInternalPort()))));

    CONF.setDnMppDataExchangePort(
      Integer.parseInt(
        properties.getProperty(
          "dn_mpp_data_exchange_port", Integer.toString(CONF.getDnMppDataExchangePort()))));

    CONF.setDnSchemaRegionConsensusPort(
      Integer.parseInt(
        properties.getProperty(
          "dn_schema_region_consensus_port",
          Integer.toString(CONF.getDnSchemaRegionConsensusPort()))));

    CONF.setDnDataRegionConsensusPort(
      Integer.parseInt(
        properties.getProperty(
          "dn_data_region_consensus_port",
          Integer.toString(CONF.getDnDataRegionConsensusPort()))));

    CONF.setDnJoinClusterRetryIntervalMs(
      Long.parseLong(
        properties.getProperty(
          "dn_join_cluster_retry_interval_ms",
          Long.toString(CONF.getDnJoinClusterRetryIntervalMs()))));
  }

  private void loadTargetConfigNodes(Properties properties) {
    String configNodeUrls = properties.getProperty(IoTDBConstant.DN_TARGET_CONFIG_NODE_LIST);
    if (configNodeUrls != null) {
      try {
        CONF.setDnTargetConfigNodeList(NodeUrlUtils.parseTEndPointUrls(configNodeUrls));
      } catch (BadNodeUrlException e) {
        logger.error(
          "ConfigNodes are set in wrong format, please set them like 127.0.0.1:10710,127.0.0.1:10712");
      }
    }
  }

  private void loadConnectionConfiguration(Properties properties) {
    CONF.setDnSessionTimeoutThreshold(
      Integer.parseInt(
        properties.getProperty(
          "dn_session_timeout_threshold",
          Integer.toString(CONF.getDnSessionTimeoutThreshold()))));

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

    CONF.setDnThriftMaxFrameSize(
      Integer.parseInt(
        properties.getProperty(
          "dn_thrift_max_frame_size", String.valueOf(CONF.getDnThriftMaxFrameSize()))));

    CONF.setDnThriftInitBufferSize(
      Integer.parseInt(
        properties.getProperty(
          "dn_thrift_init_buffer_size", String.valueOf(CONF.getDnThriftInitBufferSize()))));

    CONF.setDnConnectionTimeoutInMS(
      Integer.parseInt(
        properties
          .getProperty(
            "dn_connection_timeout_ms", String.valueOf(CONF.getDnConnectionTimeoutInMS()))
          .trim()));

    CONF.setDnSelectorThreadCountOfClientManager(
      Integer.parseInt(
        properties
          .getProperty(
            "dn_selector_thread_count_of_client_manager",
            String.valueOf(CONF.getDnSelectorThreadCountOfClientManager()))
          .trim()));

    CONF.setDnCoreClientCountForEachNodeInClientManager(
      Integer.parseInt(
        properties.getProperty("dn_core_client_count_for_each_node_in_client_manager",
          String.valueOf(CONF.getDnCoreClientCountForEachNodeInClientManager())).trim()
      )
    );

    CONF.setDnMaxClientCountForEachNodeInClientManager(
      Integer.parseInt(
        properties.getProperty("dn_max_client_count_for_each_node_in_client_manager",
          String.valueOf(CONF.getDnMaxClientCountForEachNodeInClientManager())).trim()
      )
    );
  }

  private void loadDirectoryConfiguration(Properties properties) {
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

    CONF.setDnDataDirs(properties.getProperty("dn_data_dirs", CONF.getDnDataDirs()[0]).split(","));

    String oldMultiDirStrategyClassName = CONF.getDnMultiDirStrategyClassName();
    CONF.setDnMultiDirStrategyClassName(
      properties.getProperty("dn_multi_dir_strategy", CONF.getDnMultiDirStrategyClassName()));
    try {
      CONF.checkMultiDirStrategyClassName();
    } catch (Exception e) {
      CONF.setDnMultiDirStrategyClassName(oldMultiDirStrategyClassName);
      throw e;
    }

    CONF.setDnConsensusDir(properties.getProperty("dn_consensus_dir", CONF.getDnConsensusDir()));

    CONF.setDnWalDirs(
      properties
        .getProperty("dn_wal_dirs", String.join(",", CONF.getDnWalDirs()))
        .trim()
        .split(","));

    CONF.setDnTracingDir(properties.getProperty("dn_tracing_dir", CONF.getDnTracingDir()));

    CONF.setDnSyncDir(properties.getProperty("dn_sync_dir", CONF.getDnSyncDir()).trim());
  }

  private void loadCompactionConfigurations(Properties properties) {
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

    CONF.setEnableCrossSpaceCompaction(
      Boolean.parseBoolean(
        properties.getProperty(
          "enable_cross_space_compaction",
          Boolean.toString(CONF.isEnableCrossSpaceCompaction()))));

    CONF.setCrossCompactionSelector(
      CrossCompactionSelector.getCrossCompactionSelector(
        properties.getProperty(
          "cross_selector", CONF.getCrossCompactionSelector().toString())));

    CONF.setCrossCompactionPerformer(
      CrossCompactionPerformer.getCrossCompactionPerformer(
        properties.getProperty(
          "cross_performer", CONF.getCrossCompactionPerformer().toString())));

    CONF.setInnerSequenceCompactionSelector(
      InnerSequenceCompactionSelector.getInnerSequenceCompactionSelector(
        properties.getProperty(
          "inner_seq_selector", CONF.getInnerSequenceCompactionSelector().toString())));

    CONF.setInnerSeqCompactionPerformer(
      InnerSeqCompactionPerformer.getInnerSeqCompactionPerformer(
        properties.getProperty(
          "inner_seq_performer", CONF.getInnerSeqCompactionPerformer().toString())));

    CONF.setInnerUnsequenceCompactionSelector(
      InnerUnsequenceCompactionSelector.getInnerUnsequenceCompactionSelector(
        properties.getProperty(
          "inner_unseq_selector", CONF.getInnerUnsequenceCompactionSelector().toString())));

    CONF.setInnerUnseqCompactionPerformer(
      InnerUnseqCompactionPerformer.getInnerUnseqCompactionPerformer(
        properties.getProperty(
          "inner_unseq_performer", CONF.getInnerUnseqCompactionPerformer().toString())));

    CONF.setCompactionPriority(
      CompactionPriority.valueOf(
        properties.getProperty(
          "compaction_priority", CONF.getCompactionPriority().toString())));

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

    int subtaskNum =
      Integer.parseInt(
        properties.getProperty(
          "sub_compaction_thread_count", Integer.toString(CONF.getSubCompactionTaskNum())));
    subtaskNum = subtaskNum <= 0 ? 1 : subtaskNum;
    CONF.setSubCompactionTaskNum(subtaskNum);

    CONF.setCompactionThreadCount(
      Integer.parseInt(
        properties.getProperty(
          "compaction_thread_count", Integer.toString(CONF.getCompactionThreadCount()))));

    CONF.setCrossCompactionFileSelectionTimeBudget(
      Long.parseLong(
        properties.getProperty(
          "cross_compaction_file_selection_time_budget",
          Long.toString(CONF.getCrossCompactionFileSelectionTimeBudget()))));
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
}
