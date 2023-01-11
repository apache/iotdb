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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.BadNodeUrlException;
import org.apache.iotdb.commons.utils.NodeUrlUtils;
import org.apache.iotdb.confignode.manager.load.balancer.RegionBalancer;
import org.apache.iotdb.confignode.manager.load.balancer.router.priority.IPriorityBalancer;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

public class ConfigNodeDescriptor {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigNodeDescriptor.class);

  private final ConfigNodeConfig CONF = new ConfigNodeConfig();

  private ConfigNodeDescriptor() {
    loadProps();
  }

  public ConfigNodeConfig getConf() {
    return CONF;
  }

  /**
   * Get props url location
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
    Properties properties = new Properties();
    URL url = getPropsUrl(ConfigNodeConstant.CONF_FILE_NAME);
    if (url != null) {
      try (InputStream inputStream = url.openStream()) {
        LOGGER.info("Start reading ConfigNode conf file: {}", url);
        properties.load(inputStream);
        loadProperties(properties);
      } catch (IOException | BadNodeUrlException e) {
        LOGGER.warn("Couldn't load ConfigNode conf file, use default config", e);
      } finally {
        CONF.updatePath();
        MetricConfigDescriptor.getInstance().loadProps(properties);
        MetricConfigDescriptor.getInstance()
            .getMetricConfig()
            .updateRpcInstance(CONF.getCnInternalAddress(), CONF.getCnInternalPort());
      }
    } else {
      LOGGER.warn(
          "Couldn't load the configuration {} from any of the known sources.",
          ConfigNodeConstant.CONF_FILE_NAME);
    }
  }

  private void loadProperties(Properties properties) throws BadNodeUrlException, IOException {

    CONF.setCnInternalAddress(
        properties
            .getProperty(IoTDBConstant.CN_INTERNAL_ADDRESS, CONF.getCnInternalAddress())
            .trim());

    CONF.setCnInternalPort(
        Integer.parseInt(
            properties
                .getProperty(
                    IoTDBConstant.CN_INTERNAL_PORT, String.valueOf(CONF.getCnInternalPort()))
                .trim()));

    CONF.setCnConsensusPort(
        Integer.parseInt(
            properties
                .getProperty(
                    IoTDBConstant.CN_CONSENSUS_PORT, String.valueOf(CONF.getCnConsensusPort()))
                .trim()));

    // TODO: Enable multiple target_config_node_list
    String targetConfigNodes =
        properties.getProperty(IoTDBConstant.CN_TARGET_CONFIG_NODE_LIST, null);
    if (targetConfigNodes != null) {
      CONF.setCnTargetConfigNode(NodeUrlUtils.parseTEndPointUrl(targetConfigNodes.trim()));
    }

    try {
      CONF.setRegionGroupAllocatePolicy(
          RegionBalancer.RegionGroupAllocatePolicy.valueOf(
              properties
                  .getProperty(
                      "region_group_allocate_policy", CONF.getRegionGroupAllocatePolicy().name())
                  .trim()));
    } catch (IllegalArgumentException e) {
      LOGGER.warn(
          "The configured region allocate strategy does not exist, use the default: GREEDY!");
    }

    CONF.setCnRpcThriftCompressionEnable(
      Boolean.parseBoolean(
        properties.getProperty(
          "cn_rpc_thrift_compression_enable", String.valueOf(CONF.isCnRpcThriftCompressionEnable())
        ).trim()
      )
    );

    CONF.setCnRpcAdvancedCompressionEnable(
        Boolean.parseBoolean(
            properties
                .getProperty(
                    "cn_rpc_advanced_compression_enable",
                    String.valueOf(CONF.isCnRpcAdvancedCompressionEnable()))
                .trim()));

    CONF.setCnRpcMaxConcurrentClientNum(
        Integer.parseInt(
            properties
                .getProperty(
                    "cn_rpc_max_concurrent_client_num",
                    String.valueOf(CONF.getCnRpcMaxConcurrentClientNum()))
                .trim()));

    CONF.setCnThriftMaxFrameSize(
      Integer.parseInt(
        properties
          .getProperty(
            "cn_thrift_max_frame_size", String.valueOf(CONF.getCnThriftMaxFrameSize()))
          .trim()));

    CONF.setCnThriftInitBufferSize(
        Integer.parseInt(
            properties
                .getProperty(
                    "cn_thrift_init_buffer_size", String.valueOf(CONF.getCnThriftInitBufferSize()))
                .trim()));

    CONF.setCnConnectionTimeoutMs(
      Integer.parseInt(
        properties
          .getProperty(
            "cn_connection_timeout_ms", String.valueOf(CONF.getCnConnectionTimeoutMs()))
          .trim()));

    CONF.setCnSelectorThreadNumsOfClientManager(
      Integer.parseInt(
        properties
          .getProperty(
            "cn_selector_thread_nums_of_client_manager",
            String.valueOf(CONF.setCnSelectorThreadNumsOfClientManager()))
          .trim()));


    CONF.setCnSystemDir(properties.getProperty("cn_system_dir", CONF.getCnSystemDir()).trim());

    CONF.setCnConsensusDir(properties.getProperty("cn_consensus_dir", CONF.getCnConsensusDir()).trim());

    String routePriorityPolicy =
        properties.getProperty("route_priority_policy", CONF.getRoutePriorityPolicy()).trim();
    if (IPriorityBalancer.GREEDY_POLICY.equals(routePriorityPolicy)
        || IPriorityBalancer.LEADER_POLICY.equals(routePriorityPolicy)) {
      CONF.setRoutePriorityPolicy(routePriorityPolicy);
    } else {
      throw new IOException(
          String.format(
              "Unknown route_priority_policy: %s, please set to \"LEADER\" or \"GREEDY\"",
              routePriorityPolicy));
    }

    CONF.setProcedureCompletedEvictTTL(
        Integer.parseInt(
            properties
                .getProperty(
                    "procedure_completed_evict_ttl",
                    String.valueOf(CONF.getProcedureCompletedEvictTTL()))
                .trim()));

    CONF.setProcedureCompletedCleanInterval(
        Integer.parseInt(
            properties
                .getProperty(
                    "procedure_completed_clean_interval",
                    String.valueOf(CONF.getProcedureCompletedCleanInterval()))
                .trim()));

    CONF.setProcedureCoreWorkerThreadsCount(
        Integer.parseInt(
            properties
                .getProperty(
                    "procedure_core_worker_thread_count",
                    String.valueOf(CONF.getProcedureCoreWorkerThreadsCount()))
                .trim()));

    loadRatisConsensusConfig(properties);
    loadCQConfig(properties);

    loadClientManager: self-config -> common-config
  }

  private void loadRatisConsensusConfig(Properties properties) {
    CONF.setDataRegionRatisConsensusLogAppenderBufferSize(
        Long.parseLong(
            properties
                .getProperty(
                    "data_region_ratis_log_appender_buffer_size_max",
                    String.valueOf(CONF.getDataRegionRatisConsensusLogAppenderBufferSize()))
                .trim()));

    CONF.setConfigNodeRatisConsensusLogAppenderBufferSize(
        Long.parseLong(
            properties
                .getProperty(
                    "config_node_ratis_log_appender_buffer_size_max",
                    String.valueOf(CONF.getConfigNodeRatisConsensusLogAppenderBufferSize()))
                .trim()));

    CONF.setSchemaRegionRatisConsensusLogAppenderBufferSize(
        Long.parseLong(
            properties
                .getProperty(
                    "schema_region_ratis_log_appender_buffer_size_max",
                    String.valueOf(CONF.getSchemaRegionRatisConsensusLogAppenderBufferSize()))
                .trim()));

    CONF.setDataRegionRatisSnapshotTriggerThreshold(
        Long.parseLong(
            properties
                .getProperty(
                    "data_region_ratis_snapshot_trigger_threshold",
                    String.valueOf(CONF.getDataRegionRatisSnapshotTriggerThreshold()))
                .trim()));

    CONF.setConfigNodeRatisSnapshotTriggerThreshold(
        Long.parseLong(
            properties
                .getProperty(
                    "config_node_ratis_snapshot_trigger_threshold",
                    String.valueOf(CONF.getConfigNodeRatisSnapshotTriggerThreshold()))
                .trim()));

    CONF.setSchemaRegionRatisSnapshotTriggerThreshold(
        Long.parseLong(
            properties
                .getProperty(
                    "schema_region_ratis_snapshot_trigger_threshold",
                    String.valueOf(CONF.getSchemaRegionRatisSnapshotTriggerThreshold()))
                .trim()));

    CONF.setDataRegionRatisLogUnsafeFlushEnable(
        Boolean.parseBoolean(
            properties
                .getProperty(
                    "data_region_ratis_log_unsafe_flush_enable",
                    String.valueOf(CONF.isDataRegionRatisLogUnsafeFlushEnable()))
                .trim()));

    CONF.setConfigNodeRatisLogUnsafeFlushEnable(
        Boolean.parseBoolean(
            properties
                .getProperty(
                    "config_node_ratis_log_unsafe_flush_enable",
                    String.valueOf(CONF.isConfigNodeRatisLogUnsafeFlushEnable()))
                .trim()));

    CONF.setSchemaRegionRatisLogUnsafeFlushEnable(
        Boolean.parseBoolean(
            properties
                .getProperty(
                    "schema_region_ratis_log_unsafe_flush_enable",
                    String.valueOf(CONF.isSchemaRegionRatisLogUnsafeFlushEnable()))
                .trim()));

    CONF.setDataRegionRatisLogSegmentSizeMax(
        Long.parseLong(
            properties
                .getProperty(
                    "data_region_ratis_log_segment_size_max_in_byte",
                    String.valueOf(CONF.getDataRegionRatisLogSegmentSizeMax()))
                .trim()));

    CONF.setConfigNodeRatisLogSegmentSizeMax(
        Long.parseLong(
            properties
                .getProperty(
                    "config_node_ratis_log_segment_size_max_in_byte",
                    String.valueOf(CONF.getConfigNodeRatisLogSegmentSizeMax()))
                .trim()));

    CONF.setSchemaRegionRatisLogSegmentSizeMax(
        Long.parseLong(
            properties
                .getProperty(
                    "schema_region_ratis_log_segment_size_max_in_byte",
                    String.valueOf(CONF.getSchemaRegionRatisLogSegmentSizeMax()))
                .trim()));

    CONF.setConfigNodeSimpleConsensusLogSegmentSizeMax(
        Long.parseLong(
            properties
                .getProperty(
                    "config_node_simple_consensus_log_segment_size_max_in_byte",
                    String.valueOf(CONF.getConfigNodeSimpleConsensusLogSegmentSizeMax()))
                .trim()));

    CONF.setDataRegionRatisGrpcFlowControlWindow(
        Long.parseLong(
            properties
                .getProperty(
                    "data_region_ratis_grpc_flow_control_window",
                    String.valueOf(CONF.getDataRegionRatisGrpcFlowControlWindow()))
                .trim()));

    CONF.setConfigNodeRatisGrpcFlowControlWindow(
        Long.parseLong(
            properties
                .getProperty(
                    "config_node_ratis_grpc_flow_control_window",
                    String.valueOf(CONF.getConfigNodeRatisGrpcFlowControlWindow()))
                .trim()));

    CONF.setSchemaRegionRatisGrpcFlowControlWindow(
        Long.parseLong(
            properties
                .getProperty(
                    "schema_region_ratis_grpc_flow_control_window",
                    String.valueOf(CONF.getSchemaRegionRatisGrpcFlowControlWindow()))
                .trim()));

    CONF.setDataRegionRatisRpcLeaderElectionTimeoutMinMs(
        Long.parseLong(
            properties
                .getProperty(
                    "data_region_ratis_rpc_leader_election_timeout_min_ms",
                    String.valueOf(CONF.getDataRegionRatisRpcLeaderElectionTimeoutMinMs()))
                .trim()));

    CONF.setConfigNodeRatisRpcLeaderElectionTimeoutMinMs(
        Long.parseLong(
            properties
                .getProperty(
                    "config_node_ratis_rpc_leader_election_timeout_min_ms",
                    String.valueOf(CONF.getConfigNodeRatisRpcLeaderElectionTimeoutMinMs()))
                .trim()));

    CONF.setSchemaRegionRatisRpcLeaderElectionTimeoutMinMs(
        Long.parseLong(
            properties
                .getProperty(
                    "schema_region_ratis_rpc_leader_election_timeout_min_ms",
                    String.valueOf(CONF.getSchemaRegionRatisRpcLeaderElectionTimeoutMinMs()))
                .trim()));

    CONF.setDataRegionRatisRpcLeaderElectionTimeoutMaxMs(
        Long.parseLong(
            properties
                .getProperty(
                    "data_region_ratis_rpc_leader_election_timeout_max_ms",
                    String.valueOf(CONF.getDataRegionRatisRpcLeaderElectionTimeoutMaxMs()))
                .trim()));

    CONF.setConfigNodeRatisRpcLeaderElectionTimeoutMaxMs(
        Long.parseLong(
            properties
                .getProperty(
                    "config_node_ratis_rpc_leader_election_timeout_max_ms",
                    String.valueOf(CONF.getConfigNodeRatisRpcLeaderElectionTimeoutMaxMs()))
                .trim()));

    CONF.setSchemaRegionRatisRpcLeaderElectionTimeoutMaxMs(
        Long.parseLong(
            properties
                .getProperty(
                    "schema_region_ratis_rpc_leader_election_timeout_max_ms",
                    String.valueOf(CONF.getSchemaRegionRatisRpcLeaderElectionTimeoutMaxMs()))
                .trim()));

    CONF.setConfigNodeRatisRequestTimeoutMs(
        Long.parseLong(
            properties
                .getProperty(
                    "config_node_ratis_request_timeout_ms",
                    String.valueOf(CONF.getConfigNodeRatisRequestTimeoutMs()))
                .trim()));
    CONF.setSchemaRegionRatisRequestTimeoutMs(
        Long.parseLong(
            properties
                .getProperty(
                    "schema_region_ratis_request_timeout_ms",
                    String.valueOf(CONF.getSchemaRegionRatisRequestTimeoutMs()))
                .trim()));
    CONF.setDataRegionRatisRequestTimeoutMs(
        Long.parseLong(
            properties
                .getProperty(
                    "data_region_ratis_request_timeout_ms",
                    String.valueOf(CONF.getDataRegionRatisRequestTimeoutMs()))
                .trim()));

    CONF.setConfigNodeRatisMaxRetryAttempts(
        Integer.parseInt(
            properties
                .getProperty(
                    "config_node_ratis_max_retry_attempts",
                    String.valueOf(CONF.getConfigNodeRatisMaxRetryAttempts()))
                .trim()));
    CONF.setConfigNodeRatisInitialSleepTimeMs(
        Long.parseLong(
            properties
                .getProperty(
                    "config_node_ratis_initial_sleep_time_ms",
                    String.valueOf(CONF.getConfigNodeRatisInitialSleepTimeMs()))
                .trim()));
    CONF.setConfigNodeRatisMaxSleepTimeMs(
        Long.parseLong(
            properties
                .getProperty(
                    "config_node_ratis_max_sleep_time_ms",
                    String.valueOf(CONF.getConfigNodeRatisMaxSleepTimeMs()))
                .trim()));

    CONF.setDataRegionRatisMaxRetryAttempts(
        Integer.parseInt(
            properties
                .getProperty(
                    "data_region_ratis_max_retry_attempts",
                    String.valueOf(CONF.getDataRegionRatisMaxRetryAttempts()))
                .trim()));
    CONF.setDataRegionRatisInitialSleepTimeMs(
        Long.parseLong(
            properties
                .getProperty(
                    "data_region_ratis_initial_sleep_time_ms",
                    String.valueOf(CONF.getDataRegionRatisInitialSleepTimeMs()))
                .trim()));
    CONF.setDataRegionRatisMaxSleepTimeMs(
        Long.parseLong(
            properties
                .getProperty(
                    "data_region_ratis_max_sleep_time_ms",
                    String.valueOf(CONF.getDataRegionRatisMaxSleepTimeMs()))
                .trim()));

    CONF.setSchemaRegionRatisMaxRetryAttempts(
        Integer.parseInt(
            properties
                .getProperty(
                    "schema_region_ratis_max_retry_attempts",
                    String.valueOf(CONF.getSchemaRegionRatisMaxRetryAttempts()))
                .trim()));
    CONF.setSchemaRegionRatisInitialSleepTimeMs(
        Long.parseLong(
            properties
                .getProperty(
                    "schema_region_ratis_initial_sleep_time_ms",
                    String.valueOf(CONF.getSchemaRegionRatisInitialSleepTimeMs()))
                .trim()));
    CONF.setSchemaRegionRatisMaxSleepTimeMs(
        Long.parseLong(
            properties
                .getProperty(
                    "schema_region_ratis_max_sleep_time_ms",
                    String.valueOf(CONF.getSchemaRegionRatisMaxSleepTimeMs()))
                .trim()));

    CONF.setConfigNodeRatisPreserveLogsWhenPurge(
        Long.parseLong(
            properties
                .getProperty(
                    "config_node_ratis_preserve_logs_num_when_purge",
                    String.valueOf(CONF.getConfigNodeRatisPreserveLogsWhenPurge()))
                .trim()));

    CONF.setSchemaRegionRatisPreserveLogsWhenPurge(
        Long.parseLong(
            properties
                .getProperty(
                    "schema_region_ratis_preserve_logs_num_when_purge",
                    String.valueOf(CONF.getSchemaRegionRatisPreserveLogsWhenPurge()))
                .trim()));

    CONF.setDataRegionRatisPreserveLogsWhenPurge(
        Long.parseLong(
            properties
                .getProperty(
                    "data_region_ratis_preserve_logs_num_when_purge",
                    String.valueOf(CONF.getDataRegionRatisPreserveLogsWhenPurge()))
                .trim()));

    CONF.setRatisFirstElectionTimeoutMinMs(
        Long.parseLong(
            properties
                .getProperty(
                    "ratis_first_election_timeout_min_ms",
                    String.valueOf(CONF.getRatisFirstElectionTimeoutMinMs()))
                .trim()));

    CONF.setRatisFirstElectionTimeoutMaxMs(
        Long.parseLong(
            properties
                .getProperty(
                    "ratis_first_election_timeout_max_ms",
                    String.valueOf(CONF.getRatisFirstElectionTimeoutMaxMs()))
                .trim()));

    CONF.setConfigNodeRatisLogMax(
        Long.parseLong(
            properties
                .getProperty(
                    "config_node_ratis_log_max_size",
                    String.valueOf(CONF.getConfigNodeRatisLogMax()))
                .trim()));

    CONF.setSchemaRegionRatisLogMax(
        Long.parseLong(
            properties
                .getProperty(
                    "schema_region_ratis_log_max_size",
                    String.valueOf(CONF.getSchemaRegionRatisLogMax()))
                .trim()));

    CONF.setDataRegionRatisLogMax(
        Long.parseLong(
            properties
                .getProperty(
                    "data_region_ratis_log_max_size",
                    String.valueOf(CONF.getDataRegionRatisLogMax()))
                .trim()));
  }

  private void loadCQConfig(Properties properties) {
    int cqSubmitThread =
        Integer.parseInt(
            properties
                .getProperty(
                    "continuous_query_submit_thread_count",
                    String.valueOf(CONF.getCqSubmitThread()))
                .trim());
    if (cqSubmitThread <= 0) {
      LOGGER.warn(
          "continuous_query_submit_thread should be greater than 0, but current value is {}, ignore that and use the default value {}",
          cqSubmitThread,
          CONF.getCqSubmitThread());
      cqSubmitThread = CONF.getCqSubmitThread();
    }
    CONF.setCqSubmitThread(cqSubmitThread);

    long cqMinEveryIntervalInMs =
        Long.parseLong(
            properties
                .getProperty(
                    "continuous_query_min_every_interval_in_ms",
                    String.valueOf(CONF.getCqMinEveryIntervalInMs()))
                .trim());
    if (cqMinEveryIntervalInMs <= 0) {
      LOGGER.warn(
          "continuous_query_min_every_interval_in_ms should be greater than 0, but current value is {}, ignore that and use the default value {}",
          cqMinEveryIntervalInMs,
          CONF.getCqMinEveryIntervalInMs());
      cqMinEveryIntervalInMs = CONF.getCqMinEveryIntervalInMs();
    }

    CONF.setCqMinEveryIntervalInMs(cqMinEveryIntervalInMs);
  }

  /**
   * Check if the current ConfigNode is SeedConfigNode.
   *
   * <p>Notice: Only invoke this interface when first startup.
   *
   * @return True if the target_config_node_list points to itself
   */
  public boolean isSeedConfigNode() {
    return (CONF.getCnInternalAddress().equals(CONF.getCnTargetConfigNode().getIp())
            || (NodeUrlUtils.isLocalAddress(CONF.getCnInternalAddress())
                && NodeUrlUtils.isLocalAddress(CONF.getCnTargetConfigNode().getIp())))
        && CONF.getCnInternalPort() == CONF.getCnTargetConfigNode().getPort();
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
