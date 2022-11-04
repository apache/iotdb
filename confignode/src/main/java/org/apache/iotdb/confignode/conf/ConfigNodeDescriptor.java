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
package org.apache.iotdb.confignode.conf;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.BadNodeUrlException;
import org.apache.iotdb.commons.utils.NodeUrlUtils;
import org.apache.iotdb.confignode.manager.load.balancer.RegionBalancer;
import org.apache.iotdb.confignode.manager.load.balancer.RouteBalancer;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

public class ConfigNodeDescriptor {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigNodeDescriptor.class);

  private final CommonDescriptor commonDescriptor = CommonDescriptor.getInstance();

  private final ConfigNodeConfig conf = new ConfigNodeConfig();

  private ConfigNodeDescriptor() {
    loadProps();
  }

  public ConfigNodeConfig getConf() {
    return conf;
  }

  /**
   * get props url location
   *
   * @return url object if location exit, otherwise null.
   */
  public URL getPropsUrl(String configFileName) {
    // Check if a config-directory was specified first.
    String urlString = System.getProperty(ConfigNodeConstant.CONFIGNODE_CONF, null);
    // If it wasn't, check if a home directory was provided
    if (urlString == null) {
      urlString = System.getProperty(ConfigNodeConstant.CONFIGNODE_HOME, null);
      if (urlString != null) {
        urlString = urlString + File.separatorChar + "conf" + File.separatorChar + configFileName;
      } else {
        // When start ConfigNode with the script, the environment variables CONFIGNODE_CONF
        // and CONFIGNODE_HOME will be set. But we didn't set these two in developer mode.
        // Thus, just return null and use default Configuration in developer mode.
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

  private void loadProps() {
    URL url = getPropsUrl(CommonConfig.CONFIG_NAME);
    Properties commonProperties = new Properties();
    if (url != null) {
      try (InputStream inputStream = url.openStream()) {

        LOGGER.info("Start to read config file {}", url);
        commonProperties.load(inputStream);

      } catch (FileNotFoundException e) {
        LOGGER.warn("Fail to find config file {}", url, e);
      } catch (IOException e) {
        LOGGER.warn("Cannot load config file, use default configuration", e);
      } catch (Exception e) {
        LOGGER.warn("Incorrect format in config file, use default configuration", e);
      }
    } else {
      LOGGER.warn(
          "Couldn't load the configuration {} from any of the known sources.",
          CommonConfig.CONFIG_NAME);
    }

    url = getPropsUrl(ConfigNodeConstant.CONF_FILE_NAME);
    if (url != null) {
      try (InputStream inputStream = url.openStream()) {
        LOGGER.info("start reading ConfigNode conf file: {}", url);
        Properties properties = new Properties();
        properties.load(inputStream);
        commonProperties.putAll(properties);
        loadProperties(commonProperties);
      } catch (IOException | BadNodeUrlException e) {
        LOGGER.warn("Couldn't load ConfigNode conf file, use default config", e);
      } finally {
        conf.updatePath();
        commonDescriptor
            .getConfig()
            .updatePath(System.getProperty(ConfigNodeConstant.CONFIGNODE_HOME, null));
        MetricConfigDescriptor.getInstance()
            .getMetricConfig()
            .updateRpcInstance(conf.getInternalAddress(), conf.getInternalPort());
      }
    } else {
      LOGGER.warn(
          "Couldn't load the configuration {} from any of the known sources.",
          ConfigNodeConstant.CONF_FILE_NAME);
    }
  }

  private void loadProperties(Properties properties) throws BadNodeUrlException, IOException {
    conf.setInternalAddress(
        properties.getProperty(IoTDBConstant.CN_INTERNAL_ADDRESS, conf.getInternalAddress()));

    conf.setInternalPort(
        Integer.parseInt(
            properties.getProperty(
                IoTDBConstant.CN_INTERNAL_PORT, String.valueOf(conf.getInternalPort()))));

    conf.setConsensusPort(
        Integer.parseInt(
            properties.getProperty(
                IoTDBConstant.CN_CONSENSUS_PORT, String.valueOf(conf.getConsensusPort()))));

    // TODO: Enable multiple target_config_node_list
    String targetConfigNodes =
        properties.getProperty(IoTDBConstant.CN_TARGET_CONFIG_NODE_LIST, null);
    if (targetConfigNodes != null) {
      conf.setTargetConfigNode(NodeUrlUtils.parseTEndPointUrl(targetConfigNodes));
    }

    conf.setSeriesPartitionSlotNum(
        Integer.parseInt(
            properties.getProperty(
                "series_partition_slot_num", String.valueOf(conf.getSeriesPartitionSlotNum()))));

    conf.setSeriesPartitionExecutorClass(
        properties.getProperty(
            "series_partition_executor_class", conf.getSeriesPartitionExecutorClass()));

    conf.setConfigNodeConsensusProtocolClass(
        properties.getProperty(
            "config_node_consensus_protocol_class", conf.getConfigNodeConsensusProtocolClass()));

    conf.setSchemaRegionConsensusProtocolClass(
        properties.getProperty(
            "schema_region_consensus_protocol_class",
            conf.getSchemaRegionConsensusProtocolClass()));

    conf.setSchemaRegionPerDataNode(
        Double.parseDouble(
            properties.getProperty(
                "schema_region_per_data_node", String.valueOf(conf.getSchemaRegionPerDataNode()))));

    conf.setDataRegionConsensusProtocolClass(
        properties.getProperty(
            "data_region_consensus_protocol_class", conf.getDataRegionConsensusProtocolClass()));

    conf.setDataRegionPerProcessor(
        Double.parseDouble(
            properties.getProperty(
                "data_region_per_processor", String.valueOf(conf.getDataRegionPerProcessor()))));

    try {
      conf.setRegionAllocateStrategy(
          RegionBalancer.RegionAllocateStrategy.valueOf(
              properties.getProperty(
                  "region_allocate_strategy", conf.getRegionAllocateStrategy().name())));
    } catch (IllegalArgumentException e) {
      LOGGER.warn(
          "The configured region allocate strategy does not exist, use the default: GREEDY!");
    }

    conf.setCnRpcAdvancedCompressionEnable(
        Boolean.parseBoolean(
            properties.getProperty(
                "cn_rpc_advanced_compression_enable",
                String.valueOf(conf.isCnRpcAdvancedCompressionEnable()))));

    conf.setCnRpcMaxConcurrentClientNum(
        Integer.parseInt(
            properties.getProperty(
                "cn_rpc_max_concurrent_client_num",
                String.valueOf(conf.getCnRpcMaxConcurrentClientNum()))));

    conf.setCnThriftDefaultBufferSize(
        Integer.parseInt(
            properties.getProperty(
                "cn_thrift_init_buffer_size",
                String.valueOf(conf.getCnThriftDefaultBufferSize()))));

    conf.setCnThriftMaxFrameSize(
        Integer.parseInt(
            properties.getProperty(
                "cn_thrift_max_frame_size", String.valueOf(conf.getCnThriftMaxFrameSize()))));

    conf.setSystemDir(properties.getProperty("cn_system_dir", conf.getSystemDir()));

    conf.setConsensusDir(properties.getProperty("cn_consensus_dir", conf.getConsensusDir()));

    conf.setUdfLibDir(properties.getProperty("udf_lib_dir", conf.getUdfLibDir()));

    conf.setTemporaryLibDir(
        properties.getProperty("udf_temporary_lib_dir", conf.getTemporaryLibDir()));

    conf.setTriggerLibDir(properties.getProperty("trigger_lib_dir", conf.getTriggerLibDir()));

    conf.setTimePartitionInterval(
        Long.parseLong(
            properties.getProperty(
                "time_partition_interval_for_routing",
                String.valueOf(conf.getTimePartitionInterval()))));

    conf.setSchemaReplicationFactor(
        Integer.parseInt(
            properties.getProperty(
                "schema_replication_factor", String.valueOf(conf.getSchemaReplicationFactor()))));

    conf.setDataReplicationFactor(
        Integer.parseInt(
            properties.getProperty(
                "data_replication_factor", String.valueOf(conf.getDataReplicationFactor()))));

    conf.setHeartbeatIntervalInMs(
        Long.parseLong(
            properties.getProperty(
                "heartbeat_interval_in_ms", String.valueOf(conf.getHeartbeatIntervalInMs()))));

    String routingPolicy = properties.getProperty("routing_policy", conf.getRoutingPolicy());
    if (routingPolicy.equals(RouteBalancer.GREEDY_POLICY)
        || routingPolicy.equals(RouteBalancer.LEADER_POLICY)) {
      conf.setRoutingPolicy(routingPolicy);
    } else {
      throw new IOException(
          String.format(
              "Unknown routing_policy: %s, please set to \"leader\" or \"greedy\"", routingPolicy));
    }

    String readConsistencyLevel =
        properties.getProperty("read_consistency_level", conf.getReadConsistencyLevel());
    if (readConsistencyLevel.equals("strong") || readConsistencyLevel.equals("weak")) {
      conf.setReadConsistencyLevel(readConsistencyLevel);
    } else {
      throw new IOException(
          String.format(
              "Unknown read_consistency_level: %s, please set to \"strong\" or \"weak\"",
              readConsistencyLevel));
    }

    // commons
    commonDescriptor.loadCommonProps(properties);
    commonDescriptor.initCommonConfigDir(conf.getSystemDir());

    conf.setProcedureCompletedEvictTTL(
        Integer.parseInt(
            properties.getProperty(
                "procedure_completed_evict_ttl",
                String.valueOf(conf.getProcedureCompletedEvictTTL()))));

    conf.setProcedureCompletedCleanInterval(
        Integer.parseInt(
            properties.getProperty(
                "procedure_completed_clean_interval",
                String.valueOf(conf.getProcedureCompletedCleanInterval()))));

    conf.setProcedureCoreWorkerThreadsCount(
        Integer.parseInt(
            properties.getProperty(
                "procedure_core_worker_thread_count",
                String.valueOf(conf.getProcedureCoreWorkerThreadsCount()))));

    loadRatisConsensusConfig(properties);
    loadCQConfig(properties);
  }

  private void loadRatisConsensusConfig(Properties properties) {
    conf.setDataRegionRatisConsensusLogAppenderBufferSize(
        Long.parseLong(
            properties.getProperty(
                "data_region_ratis_log_appender_buffer_size_max",
                String.valueOf(conf.getDataRegionRatisConsensusLogAppenderBufferSize()))));

    conf.setConfigNodeRatisConsensusLogAppenderBufferSize(
        Long.parseLong(
            properties.getProperty(
                "config_node_ratis_log_appender_buffer_size_max",
                String.valueOf(conf.getConfigNodeRatisConsensusLogAppenderBufferSize()))));

    conf.setSchemaRegionRatisConsensusLogAppenderBufferSize(
        Long.parseLong(
            properties.getProperty(
                "schema_region_ratis_log_appender_buffer_size_max",
                String.valueOf(conf.getSchemaRegionRatisConsensusLogAppenderBufferSize()))));

    conf.setDataRegionRatisSnapshotTriggerThreshold(
        Long.parseLong(
            properties.getProperty(
                "data_region_ratis_snapshot_trigger_threshold",
                String.valueOf(conf.getDataRegionRatisSnapshotTriggerThreshold()))));

    conf.setConfigNodeRatisSnapshotTriggerThreshold(
        Long.parseLong(
            properties.getProperty(
                "config_node_ratis_snapshot_trigger_threshold",
                String.valueOf(conf.getConfigNodeRatisSnapshotTriggerThreshold()))));

    conf.setConfigNodeSimpleConsensusSnapshotTriggerThreshold(
        Long.parseLong(
            properties.getProperty(
                "config_node_one_copy_snapshot_trigger_threshold",
                String.valueOf(conf.getConfigNodeSimpleConsensusSnapshotTriggerThreshold()))));

    conf.setSchemaRegionRatisSnapshotTriggerThreshold(
        Long.parseLong(
            properties.getProperty(
                "schema_region_ratis_snapshot_trigger_threshold",
                String.valueOf(conf.getSchemaRegionRatisSnapshotTriggerThreshold()))));

    conf.setDataRegionRatisLogUnsafeFlushEnable(
        Boolean.parseBoolean(
            properties.getProperty(
                "data_region_ratis_log_unsafe_flush_enable",
                String.valueOf(conf.isDataRegionRatisLogUnsafeFlushEnable()))));

    conf.setConfigNodeRatisLogUnsafeFlushEnable(
        Boolean.parseBoolean(
            properties.getProperty(
                "config_node_ratis_log_unsafe_flush_enable",
                String.valueOf(conf.isConfigNodeRatisLogUnsafeFlushEnable()))));

    conf.setSchemaRegionRatisLogUnsafeFlushEnable(
        Boolean.parseBoolean(
            properties.getProperty(
                "schema_region_ratis_log_unsafe_flush_enable",
                String.valueOf(conf.isSchemaRegionRatisLogUnsafeFlushEnable()))));

    conf.setDataRegionRatisLogSegmentSizeMax(
        Long.parseLong(
            properties.getProperty(
                "data_region_ratis_log_segment_size_max_in_byte",
                String.valueOf(conf.getDataRegionRatisLogSegmentSizeMax()))));

    conf.setConfigNodeRatisLogSegmentSizeMax(
        Long.parseLong(
            properties.getProperty(
                "config_node_ratis_log_segment_size_max_in_byte",
                String.valueOf(conf.getConfigNodeRatisLogSegmentSizeMax()))));

    conf.setSchemaRegionRatisLogSegmentSizeMax(
        Long.parseLong(
            properties.getProperty(
                "schema_region_ratis_log_segment_size_max_in_byte",
                String.valueOf(conf.getSchemaRegionRatisLogSegmentSizeMax()))));

    conf.setConfigNodeSimpleConsensusLogSegmentSizeMax(
        Long.parseLong(
            properties.getProperty(
                "config_node_one_copy_log_segment_size_max_in_byte",
                String.valueOf(conf.getConfigNodeSimpleConsensusLogSegmentSizeMax()))));

    conf.setDataRegionRatisGrpcFlowControlWindow(
        Long.parseLong(
            properties.getProperty(
                "data_region_ratis_grpc_flow_control_window",
                String.valueOf(conf.getDataRegionRatisGrpcFlowControlWindow()))));

    conf.setConfigNodeRatisGrpcFlowControlWindow(
        Long.parseLong(
            properties.getProperty(
                "config_node_ratis_grpc_flow_control_window",
                String.valueOf(conf.getConfigNodeRatisGrpcFlowControlWindow()))));

    conf.setSchemaRegionRatisGrpcFlowControlWindow(
        Long.parseLong(
            properties.getProperty(
                "schema_region_ratis_grpc_flow_control_window",
                String.valueOf(conf.getSchemaRegionRatisGrpcFlowControlWindow()))));

    conf.setDataRegionRatisRpcLeaderElectionTimeoutMinMs(
        Long.parseLong(
            properties.getProperty(
                "data_region_ratis_rpc_leader_election_timeout_min_ms",
                String.valueOf(conf.getDataRegionRatisRpcLeaderElectionTimeoutMinMs()))));

    conf.setConfigNodeRatisRpcLeaderElectionTimeoutMinMs(
        Long.parseLong(
            properties.getProperty(
                "config_node_ratis_rpc_leader_election_timeout_min_ms",
                String.valueOf(conf.getConfigNodeRatisRpcLeaderElectionTimeoutMinMs()))));

    conf.setSchemaRegionRatisRpcLeaderElectionTimeoutMinMs(
        Long.parseLong(
            properties.getProperty(
                "schema_region_ratis_rpc_leader_election_timeout_min_ms",
                String.valueOf(conf.getSchemaRegionRatisRpcLeaderElectionTimeoutMinMs()))));

    conf.setDataRegionRatisRpcLeaderElectionTimeoutMaxMs(
        Long.parseLong(
            properties.getProperty(
                "data_region_ratis_rpc_leader_election_timeout_max_ms",
                String.valueOf(conf.getDataRegionRatisRpcLeaderElectionTimeoutMaxMs()))));

    conf.setConfigNodeRatisRpcLeaderElectionTimeoutMaxMs(
        Long.parseLong(
            properties.getProperty(
                "config_node_ratis_rpc_leader_election_timeout_max_ms",
                String.valueOf(conf.getConfigNodeRatisRpcLeaderElectionTimeoutMaxMs()))));

    conf.setSchemaRegionRatisRpcLeaderElectionTimeoutMaxMs(
        Long.parseLong(
            properties.getProperty(
                "schema_region_ratis_rpc_leader_election_timeout_max_ms",
                String.valueOf(conf.getSchemaRegionRatisRpcLeaderElectionTimeoutMaxMs()))));

    conf.setConfigNodeRatisRequestTimeoutMs(
        Long.parseLong(
            properties.getProperty(
                "config_node_ratis_request_timeout_ms",
                String.valueOf(conf.getConfigNodeRatisRequestTimeoutMs()))));
    conf.setSchemaRegionRatisRequestTimeoutMs(
        Long.parseLong(
            properties.getProperty(
                "schema_region_ratis_request_timeout_ms",
                String.valueOf(conf.getSchemaRegionRatisRequestTimeoutMs()))));
    conf.setDataRegionRatisRequestTimeoutMs(
        Long.parseLong(
            properties.getProperty(
                "data_region_ratis_request_timeout_ms",
                String.valueOf(conf.getDataRegionRatisRequestTimeoutMs()))));

    conf.setConfigNodeRatisMaxRetryAttempts(
        Integer.parseInt(
            properties.getProperty(
                "config_node_ratis_max_retry_attempts",
                String.valueOf(conf.getConfigNodeRatisMaxRetryAttempts()))));
    conf.setConfigNodeRatisInitialSleepTimeMs(
        Long.parseLong(
            properties.getProperty(
                "config_node_ratis_initial_sleep_time_ms",
                String.valueOf(conf.getConfigNodeRatisInitialSleepTimeMs()))));
    conf.setConfigNodeRatisMaxSleepTimeMs(
        Long.parseLong(
            properties.getProperty(
                "config_node_ratis_max_sleep_time_ms",
                String.valueOf(conf.getConfigNodeRatisMaxSleepTimeMs()))));

    conf.setDataRegionRatisMaxRetryAttempts(
        Integer.parseInt(
            properties.getProperty(
                "data_region_ratis_max_retry_attempts",
                String.valueOf(conf.getDataRegionRatisMaxRetryAttempts()))));
    conf.setDataRegionRatisInitialSleepTimeMs(
        Long.parseLong(
            properties.getProperty(
                "data_region_ratis_initial_sleep_time_ms",
                String.valueOf(conf.getDataRegionRatisInitialSleepTimeMs()))));
    conf.setDataRegionRatisMaxSleepTimeMs(
        Long.parseLong(
            properties.getProperty(
                "data_region_ratis_max_sleep_time_ms",
                String.valueOf(conf.getDataRegionRatisMaxSleepTimeMs()))));

    conf.setSchemaRegionRatisMaxRetryAttempts(
        Integer.parseInt(
            properties.getProperty(
                "schema_region_ratis_max_retry_attempts",
                String.valueOf(conf.getSchemaRegionRatisMaxRetryAttempts()))));
    conf.setSchemaRegionRatisInitialSleepTimeMs(
        Long.parseLong(
            properties.getProperty(
                "schema_region_ratis_initial_sleep_time_ms",
                String.valueOf(conf.getSchemaRegionRatisInitialSleepTimeMs()))));
    conf.setSchemaRegionRatisMaxSleepTimeMs(
        Long.parseLong(
            properties.getProperty(
                "schema_region_ratis_max_sleep_time_ms",
                String.valueOf(conf.getSchemaRegionRatisMaxSleepTimeMs()))));

    conf.setConfigNodeRatisPreserveLogsWhenPurge(
        Long.parseLong(
            properties.getProperty(
                "config_node_ratis_preserve_logs_num_when_purge",
                String.valueOf(conf.getConfigNodeRatisPreserveLogsWhenPurge()))));

    conf.setSchemaRegionRatisPreserveLogsWhenPurge(
        Long.parseLong(
            properties.getProperty(
                "schema_region_ratis_preserve_logs_num_when_purge",
                String.valueOf(conf.getSchemaRegionRatisPreserveLogsWhenPurge()))));

    conf.setDataRegionRatisPreserveLogsWhenPurge(
        Long.parseLong(
            properties.getProperty(
                "data_region_ratis_preserve_logs_num_when_purge",
                String.valueOf(conf.getDataRegionRatisPreserveLogsWhenPurge()))));

    conf.setRatisFirstElectionTimeoutMinMs(
        Long.parseLong(
            properties.getProperty(
                "ratis_first_election_timeout_min_ms",
                String.valueOf(conf.getRatisFirstElectionTimeoutMinMs()))));

    conf.setRatisFirstElectionTimeoutMaxMs(
        Long.parseLong(
            properties.getProperty(
                "ratis_first_election_timeout_max_ms",
                String.valueOf(conf.getRatisFirstElectionTimeoutMaxMs()))));
  }

  private void loadCQConfig(Properties properties) {
    int cqSubmitThread =
        Integer.parseInt(
            properties.getProperty(
                "continuous_query_submit_thread_count", String.valueOf(conf.getCqSubmitThread())));
    if (cqSubmitThread <= 0) {
      LOGGER.warn(
          "continuous_query_submit_thread should be greater than 0, but current value is {}, ignore that and use the default value {}",
          cqSubmitThread,
          conf.getCqSubmitThread());
      cqSubmitThread = conf.getCqSubmitThread();
    }
    conf.setCqSubmitThread(cqSubmitThread);

    long cqMinEveryIntervalInMs =
        Long.parseLong(
            properties.getProperty(
                "continuous_query_min_every_interval_in_ms",
                String.valueOf(conf.getCqMinEveryIntervalInMs())));
    if (cqMinEveryIntervalInMs <= 0) {
      LOGGER.warn(
          "continuous_query_min_every_interval_in_ms should be greater than 0, but current value is {}, ignore that and use the default value {}",
          cqMinEveryIntervalInMs,
          conf.getCqMinEveryIntervalInMs());
      cqMinEveryIntervalInMs = conf.getCqMinEveryIntervalInMs();
    }

    conf.setCqMinEveryIntervalInMs(cqMinEveryIntervalInMs);
  }

  /**
   * Check if the current ConfigNode is SeedConfigNode.
   *
   * @return True if the target_config_node_list points to itself
   */
  public boolean isSeedConfigNode() {
    return (conf.getInternalAddress().equals(conf.getTargetConfigNode().getIp())
            || (NodeUrlUtils.isLocalAddress(conf.getInternalAddress())
                && NodeUrlUtils.isLocalAddress(conf.getTargetConfigNode().getIp())))
        && conf.getInternalPort() == conf.getTargetConfigNode().getPort();
  }

  public static ConfigNodeDescriptor getInstance() {
    return ConfigNodeDescriptorHolder.INSTANCE;
  }

  private static class ConfigNodeDescriptorHolder {

    private static final ConfigNodeDescriptor INSTANCE = new ConfigNodeDescriptor();

    private ConfigNodeDescriptorHolder() {
      // empty constructor
    }
  }
}
