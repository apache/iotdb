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
import org.apache.iotdb.commons.conf.ConfigurationFileUtils;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.conf.TrimProperties;
import org.apache.iotdb.commons.exception.BadNodeUrlException;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.utils.NodeUrlUtils;
import org.apache.iotdb.confignode.manager.load.balancer.RegionBalancer;
import org.apache.iotdb.confignode.manager.load.balancer.router.leader.AbstractLeaderBalancer;
import org.apache.iotdb.confignode.manager.load.balancer.router.priority.IPriorityBalancer;
import org.apache.iotdb.confignode.manager.load.cache.IFailureDetector;
import org.apache.iotdb.confignode.manager.partition.RegionGroupExtensionPolicy;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.utils.NodeType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Optional;

public class ConfigNodeDescriptor {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigNodeDescriptor.class);

  private final CommonDescriptor commonDescriptor = CommonDescriptor.getInstance();

  private final ConfigNodeConfig conf = new ConfigNodeConfig();

  static {
    URL systemConfigUrl = getPropsUrl(CommonConfig.SYSTEM_CONFIG_NAME);
    URL configNodeUrl = getPropsUrl(CommonConfig.OLD_CONFIG_NODE_CONFIG_NAME);
    URL dataNodeUrl = getPropsUrl(CommonConfig.OLD_DATA_NODE_CONFIG_NAME);
    URL commonConfigUrl = getPropsUrl(CommonConfig.OLD_COMMON_CONFIG_NAME);
    try {
      ConfigurationFileUtils.checkAndMayUpdate(
          systemConfigUrl, configNodeUrl, dataNodeUrl, commonConfigUrl);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      LOGGER.error("Failed to update config file", e);
    }
  }

  private ConfigNodeDescriptor() {
    loadProps();
  }

  public ConfigNodeConfig getConf() {
    return conf;
  }

  /**
   * Get props url location.
   *
   * @return url object if location exit, otherwise null.
   */
  public static URL getPropsUrl(String configFileName) {
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
      LOGGER.warn("get url failed", e);
      return null;
    }
  }

  private void loadProps() {
    TrimProperties trimProperties = new TrimProperties();
    URL url = getPropsUrl(CommonConfig.SYSTEM_CONFIG_NAME);
    if (url != null) {
      try (InputStream inputStream = url.openStream()) {
        LOGGER.info("start reading ConfigNode conf file: {}", url);
        trimProperties.load(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
        loadProperties(trimProperties);
      } catch (IOException | BadNodeUrlException e) {
        LOGGER.error("Couldn't load ConfigNode conf file, reject ConfigNode startup.", e);
        System.exit(-1);
      } finally {
        conf.updatePath();
        commonDescriptor
            .getConfig()
            .updatePath(System.getProperty(ConfigNodeConstant.CONFIGNODE_HOME, null));
        MetricConfigDescriptor.getInstance().loadProps(trimProperties, true);
        MetricConfigDescriptor.getInstance()
            .getMetricConfig()
            .updateRpcInstance(NodeType.CONFIGNODE, SchemaConstant.SYSTEM_DATABASE);
      }
    } else {
      LOGGER.warn(
          "Couldn't load the configuration {} from any of the known sources.",
          CommonConfig.SYSTEM_CONFIG_NAME);
    }
  }

  private void loadProperties(TrimProperties properties) throws BadNodeUrlException, IOException {
    conf.setClusterName(
        properties.getProperty(IoTDBConstant.CLUSTER_NAME, conf.getClusterName()).trim());

    conf.setInternalAddress(
        properties
            .getProperty(IoTDBConstant.CN_INTERNAL_ADDRESS, conf.getInternalAddress())
            .trim());

    conf.setInternalPort(
        Integer.parseInt(
            properties
                .getProperty(IoTDBConstant.CN_INTERNAL_PORT, String.valueOf(conf.getInternalPort()))
                .trim()));

    conf.setConsensusPort(
        Integer.parseInt(
            properties
                .getProperty(
                    IoTDBConstant.CN_CONSENSUS_PORT, String.valueOf(conf.getConsensusPort()))
                .trim()));

    String seedConfigNode = properties.getProperty(IoTDBConstant.CN_SEED_CONFIG_NODE, null);
    if (seedConfigNode == null) {
      seedConfigNode = properties.getProperty(IoTDBConstant.CN_TARGET_CONFIG_NODE_LIST, null);
      LOGGER.warn(
          "The parameter cn_target_config_node_list has been abandoned, "
              + "only the first ConfigNode address will be used to join in the cluster. "
              + "Please use cn_seed_config_node instead.");
    }
    if (seedConfigNode != null) {
      conf.setSeedConfigNode(NodeUrlUtils.parseTEndPointUrls(seedConfigNode.trim()).get(0));
    }

    conf.setSeriesSlotNum(
        Integer.parseInt(
            properties
                .getProperty("series_slot_num", String.valueOf(conf.getSeriesSlotNum()))
                .trim()));

    conf.setSeriesPartitionExecutorClass(
        properties
            .getProperty("series_partition_executor_class", conf.getSeriesPartitionExecutorClass())
            .trim());

    conf.setConfigNodeConsensusProtocolClass(
        properties
            .getProperty(
                "config_node_consensus_protocol_class", conf.getConfigNodeConsensusProtocolClass())
            .trim());

    conf.setSchemaRegionConsensusProtocolClass(
        properties
            .getProperty(
                "schema_region_consensus_protocol_class",
                conf.getSchemaRegionConsensusProtocolClass())
            .trim());

    conf.setSchemaReplicationFactor(
        Integer.parseInt(
            properties
                .getProperty(
                    "schema_replication_factor", String.valueOf(conf.getSchemaReplicationFactor()))
                .trim()));

    conf.setDataRegionConsensusProtocolClass(
        properties
            .getProperty(
                "data_region_consensus_protocol_class", conf.getDataRegionConsensusProtocolClass())
            .trim());

    conf.setDataReplicationFactor(
        Integer.parseInt(
            properties
                .getProperty(
                    "data_replication_factor", String.valueOf(conf.getDataReplicationFactor()))
                .trim()));

    conf.setSchemaRegionGroupExtensionPolicy(
        RegionGroupExtensionPolicy.parse(
            properties.getProperty(
                "schema_region_group_extension_policy",
                conf.getSchemaRegionGroupExtensionPolicy().getPolicy().trim())));

    conf.setDefaultSchemaRegionGroupNumPerDatabase(
        Integer.parseInt(
            properties
                .getProperty(
                    "default_schema_region_group_num_per_database",
                    String.valueOf(conf.getDefaultSchemaRegionGroupNumPerDatabase()))
                .trim()));

    conf.setSchemaRegionPerDataNode(
        (int)
            Double.parseDouble(
                properties
                    .getProperty(
                        "schema_region_per_data_node",
                        String.valueOf(conf.getSchemaRegionPerDataNode()))
                    .trim()));

    conf.setDataRegionGroupExtensionPolicy(
        RegionGroupExtensionPolicy.parse(
            properties.getProperty(
                "data_region_group_extension_policy",
                conf.getDataRegionGroupExtensionPolicy().getPolicy().trim())));

    conf.setDefaultDataRegionGroupNumPerDatabase(
        Integer.parseInt(
            properties.getProperty(
                "default_data_region_group_num_per_database",
                String.valueOf(conf.getDefaultDataRegionGroupNumPerDatabase()).trim())));

    conf.setDataRegionPerDataNode(
        (int)
            Double.parseDouble(
                properties
                    .getProperty(
                        "data_region_per_data_node",
                        String.valueOf(conf.getDataRegionPerDataNode()))
                    .trim()));

    try {
      conf.setRegionAllocateStrategy(
          RegionBalancer.RegionGroupAllocatePolicy.valueOf(
              properties
                  .getProperty(
                      "region_group_allocate_policy", conf.getRegionGroupAllocatePolicy().name())
                  .trim()));
    } catch (IllegalArgumentException e) {
      throw new IOException(e);
    }

    conf.setCnRpcMaxConcurrentClientNum(
        Integer.parseInt(
            properties
                .getProperty(
                    "cn_rpc_max_concurrent_client_num",
                    String.valueOf(conf.getCnRpcMaxConcurrentClientNum()))
                .trim()));

    conf.setMaxClientNumForEachNode(
        Integer.parseInt(
            properties
                .getProperty(
                    "cn_max_client_count_for_each_node_in_client_manager",
                    String.valueOf(conf.getMaxClientNumForEachNode()))
                .trim()));

    conf.setSystemDir(properties.getProperty("cn_system_dir", conf.getSystemDir()).trim());

    conf.setConsensusDir(properties.getProperty("cn_consensus_dir", conf.getConsensusDir()).trim());

    conf.setUdfDir(properties.getProperty("udf_lib_dir", conf.getUdfDir()).trim());

    conf.setTriggerDir(properties.getProperty("trigger_lib_dir", conf.getTriggerDir()).trim());

    conf.setPipeDir(properties.getProperty("pipe_lib_dir", conf.getPipeDir()).trim());

    conf.setPipeReceiverFileDir(
        Optional.ofNullable(properties.getProperty("cn_pipe_receiver_file_dir"))
            .orElse(
                properties.getProperty(
                    "pipe_receiver_file_dir",
                    conf.getSystemDir() + File.separator + "pipe" + File.separator + "receiver"))
            .trim());

    conf.setHeartbeatIntervalInMs(
        Long.parseLong(
            properties
                .getProperty(
                    "heartbeat_interval_in_ms", String.valueOf(conf.getHeartbeatIntervalInMs()))
                .trim()));

    String failureDetector = properties.getProperty("failure_detector", conf.getFailureDetector());
    if (IFailureDetector.FIXED_DETECTOR.equals(failureDetector)
        || IFailureDetector.PHI_ACCRUAL_DETECTOR.equals(failureDetector)) {
      conf.setFailureDetector(failureDetector);
    } else {
      throw new IOException(
          String.format(
              "Unknown failure_detector: %s, " + "please set to \"fixed\" or \"phi_accrual\"",
              failureDetector));
    }

    conf.setFailureDetectorFixedThresholdInMs(
        Long.parseLong(
            properties.getProperty(
                "failure_detector_fixed_threshold_in_ms",
                String.valueOf(conf.getFailureDetectorFixedThresholdInMs()))));

    conf.setFailureDetectorPhiThreshold(
        Long.parseLong(
            properties.getProperty(
                "failure_detector_phi_threshold",
                String.valueOf(conf.getFailureDetectorPhiThreshold()))));

    conf.setFailureDetectorPhiAcceptablePauseInMs(
        Long.parseLong(
            properties.getProperty(
                "failure_detector_phi_acceptable_pause_in_ms",
                String.valueOf(conf.getFailureDetectorPhiAcceptablePauseInMs()))));

    String leaderDistributionPolicy =
        properties
            .getProperty("leader_distribution_policy", conf.getLeaderDistributionPolicy())
            .trim();
    if (AbstractLeaderBalancer.GREEDY_POLICY.equals(leaderDistributionPolicy)
        || AbstractLeaderBalancer.CFD_POLICY.equals(leaderDistributionPolicy)) {
      conf.setLeaderDistributionPolicy(leaderDistributionPolicy);
    } else {
      throw new IOException(
          String.format(
              "Unknown leader_distribution_policy: %s, " + "please set to \"GREEDY\" or \"CFD\"",
              leaderDistributionPolicy));
    }

    conf.setEnableAutoLeaderBalanceForRatisConsensus(
        Boolean.parseBoolean(
            properties
                .getProperty(
                    "enable_auto_leader_balance_for_ratis_consensus",
                    String.valueOf(conf.isEnableAutoLeaderBalanceForRatisConsensus()))
                .trim()));

    conf.setEnableAutoLeaderBalanceForIoTConsensus(
        Boolean.parseBoolean(
            properties
                .getProperty(
                    "enable_auto_leader_balance_for_iot_consensus",
                    String.valueOf(conf.isEnableAutoLeaderBalanceForIoTConsensus()))
                .trim()));

    String routePriorityPolicy =
        properties.getProperty("route_priority_policy", conf.getRoutePriorityPolicy()).trim();
    if (IPriorityBalancer.GREEDY_POLICY.equals(routePriorityPolicy)
        || IPriorityBalancer.LEADER_POLICY.equals(routePriorityPolicy)) {
      conf.setRoutePriorityPolicy(routePriorityPolicy);
    } else {
      throw new IOException(
          String.format(
              "Unknown route_priority_policy: %s, please set to \"LEADER\" or \"GREEDY\"",
              routePriorityPolicy));
    }

    String readConsistencyLevel =
        properties.getProperty("read_consistency_level", conf.getReadConsistencyLevel()).trim();
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
            properties
                .getProperty(
                    "procedure_completed_evict_ttl",
                    String.valueOf(conf.getProcedureCompletedEvictTTL()))
                .trim()));

    conf.setProcedureCompletedCleanInterval(
        Integer.parseInt(
            properties
                .getProperty(
                    "procedure_completed_clean_interval",
                    String.valueOf(conf.getProcedureCompletedCleanInterval()))
                .trim()));

    conf.setProcedureCoreWorkerThreadsCount(
        Integer.parseInt(
            properties
                .getProperty(
                    "procedure_core_worker_thread_count",
                    String.valueOf(conf.getProcedureCoreWorkerThreadsCount()))
                .trim()));

    loadRatisConsensusConfig(properties);
    loadCQConfig(properties);
  }

  private void loadRatisConsensusConfig(TrimProperties properties) {
    conf.setDataRegionRatisConsensusLogAppenderBufferSize(
        Long.parseLong(
            properties
                .getProperty(
                    "data_region_ratis_log_appender_buffer_size_max",
                    String.valueOf(conf.getDataRegionRatisConsensusLogAppenderBufferSize()))
                .trim()));

    conf.setConfigNodeRatisConsensusLogAppenderBufferSize(
        Long.parseLong(
            properties
                .getProperty(
                    "config_node_ratis_log_appender_buffer_size_max",
                    String.valueOf(conf.getConfigNodeRatisConsensusLogAppenderBufferSize()))
                .trim()));

    conf.setSchemaRegionRatisConsensusLogAppenderBufferSize(
        Long.parseLong(
            properties
                .getProperty(
                    "schema_region_ratis_log_appender_buffer_size_max",
                    String.valueOf(conf.getSchemaRegionRatisConsensusLogAppenderBufferSize()))
                .trim()));

    conf.setDataRegionRatisSnapshotTriggerThreshold(
        Long.parseLong(
            properties
                .getProperty(
                    "data_region_ratis_snapshot_trigger_threshold",
                    String.valueOf(conf.getDataRegionRatisSnapshotTriggerThreshold()))
                .trim()));

    conf.setConfigNodeRatisSnapshotTriggerThreshold(
        Long.parseLong(
            properties
                .getProperty(
                    "config_node_ratis_snapshot_trigger_threshold",
                    String.valueOf(conf.getConfigNodeRatisSnapshotTriggerThreshold()))
                .trim()));

    conf.setSchemaRegionRatisSnapshotTriggerThreshold(
        Long.parseLong(
            properties
                .getProperty(
                    "schema_region_ratis_snapshot_trigger_threshold",
                    String.valueOf(conf.getSchemaRegionRatisSnapshotTriggerThreshold()))
                .trim()));

    conf.setDataRegionRatisLogUnsafeFlushEnable(
        Boolean.parseBoolean(
            properties
                .getProperty(
                    "data_region_ratis_log_unsafe_flush_enable",
                    String.valueOf(conf.isDataRegionRatisLogUnsafeFlushEnable()))
                .trim()));

    conf.setConfigNodeRatisLogUnsafeFlushEnable(
        Boolean.parseBoolean(
            properties
                .getProperty(
                    "config_node_ratis_log_unsafe_flush_enable",
                    String.valueOf(conf.isConfigNodeRatisLogUnsafeFlushEnable()))
                .trim()));

    conf.setSchemaRegionRatisLogUnsafeFlushEnable(
        Boolean.parseBoolean(
            properties
                .getProperty(
                    "schema_region_ratis_log_unsafe_flush_enable",
                    String.valueOf(conf.isSchemaRegionRatisLogUnsafeFlushEnable()))
                .trim()));

    conf.setDataRegionRatisLogSegmentSizeMax(
        Long.parseLong(
            properties
                .getProperty(
                    "data_region_ratis_log_segment_size_max_in_byte",
                    String.valueOf(conf.getDataRegionRatisLogSegmentSizeMax()))
                .trim()));

    conf.setConfigNodeRatisLogSegmentSizeMax(
        Long.parseLong(
            properties
                .getProperty(
                    "config_node_ratis_log_segment_size_max_in_byte",
                    String.valueOf(conf.getConfigNodeRatisLogSegmentSizeMax()))
                .trim()));

    conf.setSchemaRegionRatisLogSegmentSizeMax(
        Long.parseLong(
            properties
                .getProperty(
                    "schema_region_ratis_log_segment_size_max_in_byte",
                    String.valueOf(conf.getSchemaRegionRatisLogSegmentSizeMax()))
                .trim()));

    conf.setConfigNodeSimpleConsensusLogSegmentSizeMax(
        Long.parseLong(
            properties
                .getProperty(
                    "config_node_simple_consensus_log_segment_size_max_in_byte",
                    String.valueOf(conf.getConfigNodeSimpleConsensusLogSegmentSizeMax()))
                .trim()));

    conf.setDataRegionRatisGrpcFlowControlWindow(
        Long.parseLong(
            properties
                .getProperty(
                    "data_region_ratis_grpc_flow_control_window",
                    String.valueOf(conf.getDataRegionRatisGrpcFlowControlWindow()))
                .trim()));

    conf.setConfigNodeRatisGrpcFlowControlWindow(
        Long.parseLong(
            properties
                .getProperty(
                    "config_node_ratis_grpc_flow_control_window",
                    String.valueOf(conf.getConfigNodeRatisGrpcFlowControlWindow()))
                .trim()));

    conf.setSchemaRegionRatisGrpcFlowControlWindow(
        Long.parseLong(
            properties
                .getProperty(
                    "schema_region_ratis_grpc_flow_control_window",
                    String.valueOf(conf.getSchemaRegionRatisGrpcFlowControlWindow()))
                .trim()));

    conf.setDataRegionRatisGrpcLeaderOutstandingAppendsMax(
        Integer.parseInt(
            properties
                .getProperty(
                    "data_region_ratis_grpc_leader_outstanding_appends_max",
                    String.valueOf(conf.getDataRegionRatisGrpcLeaderOutstandingAppendsMax()))
                .trim()));

    conf.setConfigNodeRatisGrpcLeaderOutstandingAppendsMax(
        Integer.parseInt(
            properties
                .getProperty(
                    "config_node_ratis_grpc_leader_outstanding_appends_max",
                    String.valueOf(conf.getConfigNodeRatisGrpcLeaderOutstandingAppendsMax()))
                .trim()));

    conf.setSchemaRegionRatisGrpcLeaderOutstandingAppendsMax(
        Integer.parseInt(
            properties
                .getProperty(
                    "schema_region_ratis_grpc_leader_outstanding_appends_max",
                    String.valueOf(conf.getSchemaRegionRatisGrpcLeaderOutstandingAppendsMax()))
                .trim()));

    conf.setDataRegionRatisLogForceSyncNum(
        Integer.parseInt(
            properties
                .getProperty(
                    "data_region_ratis_log_force_sync_num",
                    String.valueOf(conf.getDataRegionRatisLogForceSyncNum()))
                .trim()));

    conf.setConfigNodeRatisLogForceSyncNum(
        Integer.parseInt(
            properties
                .getProperty(
                    "config_node_ratis_log_force_sync_num",
                    String.valueOf(conf.getConfigNodeRatisLogForceSyncNum()))
                .trim()));

    conf.setSchemaRegionRatisLogForceSyncNum(
        Integer.parseInt(
            properties
                .getProperty(
                    "schema_region_ratis_log_force_sync_num",
                    String.valueOf(conf.getSchemaRegionRatisLogForceSyncNum()))
                .trim()));

    conf.setDataRegionRatisRpcLeaderElectionTimeoutMinMs(
        Long.parseLong(
            properties
                .getProperty(
                    "data_region_ratis_rpc_leader_election_timeout_min_ms",
                    String.valueOf(conf.getDataRegionRatisRpcLeaderElectionTimeoutMinMs()))
                .trim()));

    conf.setConfigNodeRatisRpcLeaderElectionTimeoutMinMs(
        Long.parseLong(
            properties
                .getProperty(
                    "config_node_ratis_rpc_leader_election_timeout_min_ms",
                    String.valueOf(conf.getConfigNodeRatisRpcLeaderElectionTimeoutMinMs()))
                .trim()));

    conf.setSchemaRegionRatisRpcLeaderElectionTimeoutMinMs(
        Long.parseLong(
            properties
                .getProperty(
                    "schema_region_ratis_rpc_leader_election_timeout_min_ms",
                    String.valueOf(conf.getSchemaRegionRatisRpcLeaderElectionTimeoutMinMs()))
                .trim()));

    conf.setDataRegionRatisRpcLeaderElectionTimeoutMaxMs(
        Long.parseLong(
            properties
                .getProperty(
                    "data_region_ratis_rpc_leader_election_timeout_max_ms",
                    String.valueOf(conf.getDataRegionRatisRpcLeaderElectionTimeoutMaxMs()))
                .trim()));

    conf.setConfigNodeRatisRpcLeaderElectionTimeoutMaxMs(
        Long.parseLong(
            properties
                .getProperty(
                    "config_node_ratis_rpc_leader_election_timeout_max_ms",
                    String.valueOf(conf.getConfigNodeRatisRpcLeaderElectionTimeoutMaxMs()))
                .trim()));

    conf.setSchemaRegionRatisRpcLeaderElectionTimeoutMaxMs(
        Long.parseLong(
            properties
                .getProperty(
                    "schema_region_ratis_rpc_leader_election_timeout_max_ms",
                    String.valueOf(conf.getSchemaRegionRatisRpcLeaderElectionTimeoutMaxMs()))
                .trim()));

    conf.setConfigNodeRatisRequestTimeoutMs(
        Long.parseLong(
            properties
                .getProperty(
                    "config_node_ratis_request_timeout_ms",
                    String.valueOf(conf.getConfigNodeRatisRequestTimeoutMs()))
                .trim()));
    conf.setSchemaRegionRatisRequestTimeoutMs(
        Long.parseLong(
            properties
                .getProperty(
                    "schema_region_ratis_request_timeout_ms",
                    String.valueOf(conf.getSchemaRegionRatisRequestTimeoutMs()))
                .trim()));
    conf.setDataRegionRatisRequestTimeoutMs(
        Long.parseLong(
            properties
                .getProperty(
                    "data_region_ratis_request_timeout_ms",
                    String.valueOf(conf.getDataRegionRatisRequestTimeoutMs()))
                .trim()));

    conf.setConfigNodeRatisMaxRetryAttempts(
        Integer.parseInt(
            properties
                .getProperty(
                    "config_node_ratis_max_retry_attempts",
                    String.valueOf(conf.getConfigNodeRatisMaxRetryAttempts()))
                .trim()));
    conf.setConfigNodeRatisInitialSleepTimeMs(
        Long.parseLong(
            properties
                .getProperty(
                    "config_node_ratis_initial_sleep_time_ms",
                    String.valueOf(conf.getConfigNodeRatisInitialSleepTimeMs()))
                .trim()));
    conf.setConfigNodeRatisMaxSleepTimeMs(
        Long.parseLong(
            properties
                .getProperty(
                    "config_node_ratis_max_sleep_time_ms",
                    String.valueOf(conf.getConfigNodeRatisMaxSleepTimeMs()))
                .trim()));

    conf.setDataRegionRatisMaxRetryAttempts(
        Integer.parseInt(
            properties
                .getProperty(
                    "data_region_ratis_max_retry_attempts",
                    String.valueOf(conf.getDataRegionRatisMaxRetryAttempts()))
                .trim()));
    conf.setDataRegionRatisInitialSleepTimeMs(
        Long.parseLong(
            properties
                .getProperty(
                    "data_region_ratis_initial_sleep_time_ms",
                    String.valueOf(conf.getDataRegionRatisInitialSleepTimeMs()))
                .trim()));
    conf.setDataRegionRatisMaxSleepTimeMs(
        Long.parseLong(
            properties
                .getProperty(
                    "data_region_ratis_max_sleep_time_ms",
                    String.valueOf(conf.getDataRegionRatisMaxSleepTimeMs()))
                .trim()));

    conf.setSchemaRegionRatisMaxRetryAttempts(
        Integer.parseInt(
            properties
                .getProperty(
                    "schema_region_ratis_max_retry_attempts",
                    String.valueOf(conf.getSchemaRegionRatisMaxRetryAttempts()))
                .trim()));
    conf.setSchemaRegionRatisInitialSleepTimeMs(
        Long.parseLong(
            properties
                .getProperty(
                    "schema_region_ratis_initial_sleep_time_ms",
                    String.valueOf(conf.getSchemaRegionRatisInitialSleepTimeMs()))
                .trim()));
    conf.setSchemaRegionRatisMaxSleepTimeMs(
        Long.parseLong(
            properties
                .getProperty(
                    "schema_region_ratis_max_sleep_time_ms",
                    String.valueOf(conf.getSchemaRegionRatisMaxSleepTimeMs()))
                .trim()));

    conf.setConfigNodeRatisPreserveLogsWhenPurge(
        Long.parseLong(
            properties
                .getProperty(
                    "config_node_ratis_preserve_logs_num_when_purge",
                    String.valueOf(conf.getConfigNodeRatisPreserveLogsWhenPurge()))
                .trim()));

    conf.setSchemaRegionRatisPreserveLogsWhenPurge(
        Long.parseLong(
            properties
                .getProperty(
                    "schema_region_ratis_preserve_logs_num_when_purge",
                    String.valueOf(conf.getSchemaRegionRatisPreserveLogsWhenPurge()))
                .trim()));

    conf.setDataRegionRatisPreserveLogsWhenPurge(
        Long.parseLong(
            properties
                .getProperty(
                    "data_region_ratis_preserve_logs_num_when_purge",
                    String.valueOf(conf.getDataRegionRatisPreserveLogsWhenPurge()))
                .trim()));

    conf.setRatisFirstElectionTimeoutMinMs(
        Long.parseLong(
            properties
                .getProperty(
                    "ratis_first_election_timeout_min_ms",
                    String.valueOf(conf.getRatisFirstElectionTimeoutMinMs()))
                .trim()));

    conf.setRatisFirstElectionTimeoutMaxMs(
        Long.parseLong(
            properties
                .getProperty(
                    "ratis_first_election_timeout_max_ms",
                    String.valueOf(conf.getRatisFirstElectionTimeoutMaxMs()))
                .trim()));

    conf.setConfigNodeRatisLogMax(
        Long.parseLong(
            properties
                .getProperty(
                    "config_node_ratis_log_max_size",
                    String.valueOf(conf.getConfigNodeRatisLogMax()))
                .trim()));

    conf.setSchemaRegionRatisLogMax(
        Long.parseLong(
            properties
                .getProperty(
                    "schema_region_ratis_log_max_size",
                    String.valueOf(conf.getSchemaRegionRatisLogMax()))
                .trim()));

    conf.setDataRegionRatisLogMax(
        Long.parseLong(
            properties
                .getProperty(
                    "data_region_ratis_log_max_size",
                    String.valueOf(conf.getDataRegionRatisLogMax()))
                .trim()));

    conf.setConfigNodeRatisPeriodicSnapshotInterval(
        Long.parseLong(
            properties.getProperty(
                "config_node_ratis_periodic_snapshot_interval",
                String.valueOf(conf.getConfigNodeRatisPeriodicSnapshotInterval()).trim())));

    conf.setSchemaRegionRatisPeriodicSnapshotInterval(
        Long.parseLong(
            properties.getProperty(
                "schema_region_ratis_periodic_snapshot_interval",
                String.valueOf(conf.getSchemaRegionRatisPeriodicSnapshotInterval()).trim())));

    conf.setDataRegionRatisPeriodicSnapshotInterval(
        Long.parseLong(
            properties.getProperty(
                "data_region_ratis_periodic_snapshot_interval",
                String.valueOf(conf.getDataRegionRatisPeriodicSnapshotInterval()).trim())));

    conf.setEnablePrintingNewlyCreatedPartition(
        Boolean.parseBoolean(
            properties
                .getProperty(
                    "enable_printing_newly_created_partition",
                    String.valueOf(conf.isEnablePrintingNewlyCreatedPartition()))
                .trim()));

    conf.setForceWalPeriodForConfigNodeSimpleInMs(
        Long.parseLong(
            properties
                .getProperty(
                    "force_wal_period_for_confignode_simple_in_ms",
                    String.valueOf(conf.getForceWalPeriodForConfigNodeSimpleInMs()))
                .trim()));
  }

  private void loadCQConfig(TrimProperties properties) {
    int cqSubmitThread =
        Integer.parseInt(
            properties
                .getProperty(
                    "continuous_query_submit_thread_count",
                    String.valueOf(conf.getCqSubmitThread()))
                .trim());
    if (cqSubmitThread <= 0) {
      LOGGER.warn(
          "continuous_query_submit_thread should be greater than 0, "
              + "but current value is {}, ignore that and use the default value {}",
          cqSubmitThread,
          conf.getCqSubmitThread());
      cqSubmitThread = conf.getCqSubmitThread();
    }
    conf.setCqSubmitThread(cqSubmitThread);

    long cqMinEveryIntervalInMs =
        Long.parseLong(
            properties
                .getProperty(
                    "continuous_query_min_every_interval_in_ms",
                    String.valueOf(conf.getCqMinEveryIntervalInMs()))
                .trim());
    if (cqMinEveryIntervalInMs <= 0) {
      LOGGER.warn(
          "continuous_query_min_every_interval_in_ms should be greater than 0, "
              + "but current value is {}, ignore that and use the default value {}",
          cqMinEveryIntervalInMs,
          conf.getCqMinEveryIntervalInMs());
      cqMinEveryIntervalInMs = conf.getCqMinEveryIntervalInMs();
    }

    conf.setCqMinEveryIntervalInMs(cqMinEveryIntervalInMs);
  }

  /**
   * Check if the current ConfigNode is SeedConfigNode.
   *
   * <p>Notice: Only invoke this interface when first startup.
   *
   * @return True if the seed_config_node points to itself
   */
  public boolean isSeedConfigNode() {
    try {
      return (conf.getInternalAddress().equals(conf.getSeedConfigNode().getIp())
              || (NodeUrlUtils.containsLocalAddress(
                      Collections.singletonList(conf.getInternalAddress()))
                  && NodeUrlUtils.containsLocalAddress(
                      Collections.singletonList(conf.getSeedConfigNode().getIp()))))
          && conf.getInternalPort() == conf.getSeedConfigNode().getPort();
    } catch (UnknownHostException e) {
      LOGGER.warn("Unknown host when checking seed configNode IP {}", conf.getInternalAddress(), e);
      return false;
    }
  }

  public void loadHotModifiedProps(TrimProperties properties) {
    Optional.ofNullable(properties.getProperty(IoTDBConstant.CLUSTER_NAME))
        .ifPresent(conf::setClusterName);
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
