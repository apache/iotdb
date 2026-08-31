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
import org.apache.iotdb.commons.log.LoggerPeriodicalLogReducer;
import org.apache.iotdb.commons.pipe.config.PipeDescriptor;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.utils.NodeUrlUtils;
import org.apache.iotdb.confignode.i18n.ConfigNodeMessages;
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
import java.math.BigDecimal;
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

  private final ConfigNodeMemoryConfig memoryConfig = new ConfigNodeMemoryConfig();

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
      LOGGER.error(ConfigNodeMessages.FAILED_TO_UPDATE_CONFIG_FILE, e);
    }
  }

  private ConfigNodeDescriptor() {
    loadProps();
  }

  public ConfigNodeConfig getConf() {
    return conf;
  }

  public ConfigNodeMemoryConfig getMemoryConfig() {
    return memoryConfig;
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
      LOGGER.warn(ConfigNodeMessages.GET_URL_FAILED, e);
      return null;
    }
  }

  private void loadProps() {
    TrimProperties trimProperties = new TrimProperties();
    URL url = getPropsUrl(CommonConfig.SYSTEM_CONFIG_NAME);
    if (url != null) {
      try (InputStream inputStream = url.openStream()) {
        LOGGER.info(ConfigNodeMessages.START_READING_CONFIGNODE_CONF_FILE, url);
        trimProperties.load(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
        loadProperties(trimProperties);
      } catch (IOException | BadNodeUrlException e) {
        LOGGER.error(
            ConfigNodeMessages.COULDN_T_LOAD_CONFIGNODE_CONF_FILE_REJECT_CONFIGNODE_STARTUP, e);
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
          ConfigNodeMessages.COULDN_T_LOAD_THE_CONFIGURATION_FROM_ANY_OF_THE_KNOWN,
          CommonConfig.SYSTEM_CONFIG_NAME);
      memoryConfig.init(trimProperties);
    }
  }

  private void loadProperties(TrimProperties properties) throws BadNodeUrlException, IOException {
    ConfigurationFileUtils.updateAppliedProperties(properties, false);
    memoryConfig.init(properties);

    conf.setClusterName(properties.getProperty(IoTDBConstant.CLUSTER_NAME, conf.getClusterName()));

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

    String seedConfigNode = properties.getProperty(IoTDBConstant.CN_SEED_CONFIG_NODE, null);
    if (seedConfigNode == null) {
      seedConfigNode = properties.getProperty(IoTDBConstant.CN_TARGET_CONFIG_NODE_LIST, null);
      LOGGER.warn(ConfigNodeMessages.THE_PARAMETER_CN_TARGET_CONFIG_NODE_LIST_HAS_BEEN_ABANDONED);
    }
    if (seedConfigNode != null) {
      conf.setSeedConfigNode(NodeUrlUtils.parseTEndPointUrls(seedConfigNode).get(0));
    }

    conf.setSeriesSlotNum(
        Integer.parseInt(
            properties.getProperty("series_slot_num", String.valueOf(conf.getSeriesSlotNum()))));

    conf.setSeriesPartitionExecutorClass(
        properties.getProperty(
            "series_partition_executor_class", conf.getSeriesPartitionExecutorClass()));

    conf.setDataPartitionAllocationStrategy(
        properties.getProperty(
            "data_partition_allocation_strategy", conf.getDataPartitionAllocationStrategy()));

    conf.setConfigNodeConsensusProtocolClass(
        properties.getProperty(
            "config_node_consensus_protocol_class", conf.getConfigNodeConsensusProtocolClass()));

    conf.setSchemaRegionConsensusProtocolClass(
        properties.getProperty(
            "schema_region_consensus_protocol_class",
            conf.getSchemaRegionConsensusProtocolClass()));

    conf.setSchemaReplicationFactor(
        Integer.parseInt(
            properties.getProperty(
                "schema_replication_factor", String.valueOf(conf.getSchemaReplicationFactor()))));

    conf.setDataRegionConsensusProtocolClass(
        properties.getProperty(
            "data_region_consensus_protocol_class", conf.getDataRegionConsensusProtocolClass()));

    conf.setIotConsensusV2Mode(
        properties.getProperty("iot_consensus_v2_mode", conf.getIotConsensusV2Mode()));

    conf.setDataReplicationFactor(
        Integer.parseInt(
            properties.getProperty(
                "data_replication_factor", String.valueOf(conf.getDataReplicationFactor()))));

    loadRegionGroupExtensionConfig(properties);

    try {
      conf.setRegionAllocateStrategy(
          RegionBalancer.RegionGroupAllocatePolicy.valueOf(
              properties.getProperty(
                  "region_group_allocate_policy", conf.getRegionGroupAllocatePolicy().name())));
    } catch (IllegalArgumentException e) {
      throw new IOException(e);
    }

    conf.setCnRpcMaxConcurrentClientNum(
        Integer.parseInt(
            properties.getProperty(
                "cn_rpc_max_concurrent_client_num",
                String.valueOf(conf.getCnRpcMaxConcurrentClientNum()))));

    conf.setMaxClientNumForEachNode(
        Integer.parseInt(
            properties.getProperty(
                "cn_max_client_count_for_each_node_in_client_manager",
                String.valueOf(conf.getMaxClientNumForEachNode()))));

    int cnMaxIdleClientNumForEachNode =
        Integer.parseInt(
            properties.getProperty(
                "cn_max_idle_client_count_for_each_node_in_client_manager",
                String.valueOf(conf.getMaxIdleClientNumForEachNode())));
    if (cnMaxIdleClientNumForEachNode >= 0) {
      conf.setMaxIdleClientNumForEachNode(cnMaxIdleClientNumForEachNode);
    }

    int cnSelectorNumOfClientManager =
        Integer.parseInt(
            properties.getProperty(
                "cn_selector_thread_nums_of_client_manager",
                String.valueOf(conf.getSelectorNumOfClientManager())));
    if (cnSelectorNumOfClientManager > 0) {
      conf.setSelectorNumOfClientManager(cnSelectorNumOfClientManager);
    }

    conf.setSystemDir(properties.getProperty("cn_system_dir", conf.getSystemDir()));

    conf.setConsensusDir(properties.getProperty("cn_consensus_dir", conf.getConsensusDir()));

    conf.setUdfDir(properties.getProperty("udf_lib_dir", conf.getUdfDir()));

    conf.setTriggerDir(properties.getProperty("trigger_lib_dir", conf.getTriggerDir()));

    conf.setPipeDir(properties.getProperty("pipe_lib_dir", conf.getPipeDir()));

    conf.setPipeReceiverFileDir(
        Optional.ofNullable(properties.getProperty("cn_pipe_receiver_file_dir"))
            .orElse(
                properties.getProperty(
                    "pipe_receiver_file_dir",
                    conf.getSystemDir() + File.separator + "pipe" + File.separator + "receiver")));

    conf.setHeartbeatIntervalInMs(loadHeartbeatIntervalInMs(properties));
    conf.setFailureDetectorHeartbeatIntervalInMs(conf.getHeartbeatIntervalInMs());

    String failureDetector = properties.getProperty("failure_detector", conf.getFailureDetector());
    if (IFailureDetector.FIXED_DETECTOR.equals(failureDetector)
        || IFailureDetector.PHI_ACCRUAL_DETECTOR.equals(failureDetector)) {
      conf.setFailureDetector(failureDetector);
    } else {
      throw new IOException(
          String.format(ConfigNodeMessages.UNKNOWN_FAILURE_DETECTOR, failureDetector));
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

    conf.setMetadataLeaseFenceMs(
        Long.parseLong(
            properties.getProperty(
                "metadata_lease_fence_ms", String.valueOf(conf.getMetadataLeaseFenceMs()))));

    conf.setEnableTopologyProbing(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_topology_probing", String.valueOf(conf.isEnableTopologyProbing()))));

    long topologyProbingBaseIntervalInMs =
        Long.parseLong(
            properties.getProperty(
                "topology_probing_base_interval_in_ms",
                String.valueOf(conf.getTopologyProbingBaseIntervalInMs())));
    if (topologyProbingBaseIntervalInMs <= 0) {
      throw new IOException(
          ConfigNodeMessages
                  .EXCEPTION_TOPOLOGY_PROBING_BASE_INTERVAL_MS_MUST_POSITIVE_BUT_GOT_18C9B7A2
              + topologyProbingBaseIntervalInMs);
    }
    conf.setTopologyProbingBaseIntervalInMs(topologyProbingBaseIntervalInMs);

    double topologyProbingTimeoutRatio =
        Double.parseDouble(
            properties.getProperty(
                "topology_probing_timeout_ratio",
                String.valueOf(conf.getTopologyProbingTimeoutRatio())));
    if (topologyProbingTimeoutRatio <= 0 || topologyProbingTimeoutRatio >= 1.0) {
      throw new IOException(
          ConfigNodeMessages.EXCEPTION_TOPOLOGY_PROBING_TIMEOUT_RATIO_MUST_0_1_BUT_GOT_FBD0E28B
              + topologyProbingTimeoutRatio);
    }
    conf.setTopologyProbingTimeoutRatio(topologyProbingTimeoutRatio);

    String leaderDistributionPolicy =
        properties.getProperty("leader_distribution_policy", conf.getLeaderDistributionPolicy());
    if (AbstractLeaderBalancer.GREEDY_POLICY.equals(leaderDistributionPolicy)
        || AbstractLeaderBalancer.CFS_POLICY.equals(leaderDistributionPolicy)
        || AbstractLeaderBalancer.HASH_POLICY.equals(leaderDistributionPolicy)) {
      conf.setLeaderDistributionPolicy(leaderDistributionPolicy);
    } else {
      throw new IOException(
          String.format(
              ConfigNodeMessages.UNKNOWN_LEADER_DISTRIBUTION_POLICY, leaderDistributionPolicy));
    }

    conf.setEnableAutoLeaderBalanceForRatisConsensus(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_auto_leader_balance_for_ratis_consensus",
                String.valueOf(conf.isEnableAutoLeaderBalanceForRatisConsensus()))));

    String routePriorityPolicy =
        properties.getProperty("route_priority_policy", conf.getRoutePriorityPolicy());
    if (IPriorityBalancer.GREEDY_POLICY.equals(routePriorityPolicy)
        || IPriorityBalancer.LEADER_POLICY.equals(routePriorityPolicy)) {
      conf.setRoutePriorityPolicy(routePriorityPolicy);
    } else {
      throw new IOException(
          String.format(
              ConfigNodeMessages.UNKNOWN_ROUTE_PRIORITY_POLICY_PLEASE_SET_TO, routePriorityPolicy));
    }

    conf.setReadConsistencyLevel(loadReadConsistencyLevel(properties));

    // commons
    commonDescriptor.loadCommonProps(properties);
    commonDescriptor.initCommonConfigDir(conf.getSystemDir());
    commonDescriptor.initThriftSSL(properties);

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

  private void loadRatisConsensusConfig(TrimProperties properties) {
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
                "config_node_simple_consensus_log_segment_size_max_in_byte",
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

    conf.setDataRegionRatisGrpcLeaderOutstandingAppendsMax(
        Integer.parseInt(
            properties.getProperty(
                "data_region_ratis_grpc_leader_outstanding_appends_max",
                String.valueOf(conf.getDataRegionRatisGrpcLeaderOutstandingAppendsMax()))));

    conf.setConfigNodeRatisGrpcLeaderOutstandingAppendsMax(
        Integer.parseInt(
            properties.getProperty(
                "config_node_ratis_grpc_leader_outstanding_appends_max",
                String.valueOf(conf.getConfigNodeRatisGrpcLeaderOutstandingAppendsMax()))));

    conf.setSchemaRegionRatisGrpcLeaderOutstandingAppendsMax(
        Integer.parseInt(
            properties.getProperty(
                "schema_region_ratis_grpc_leader_outstanding_appends_max",
                String.valueOf(conf.getSchemaRegionRatisGrpcLeaderOutstandingAppendsMax()))));

    conf.setDataRegionRatisLogForceSyncNum(
        Integer.parseInt(
            properties.getProperty(
                "data_region_ratis_log_force_sync_num",
                String.valueOf(conf.getDataRegionRatisLogForceSyncNum()))));

    conf.setConfigNodeRatisLogForceSyncNum(
        Integer.parseInt(
            properties.getProperty(
                "config_node_ratis_log_force_sync_num",
                String.valueOf(conf.getConfigNodeRatisLogForceSyncNum()))));

    conf.setSchemaRegionRatisLogForceSyncNum(
        Integer.parseInt(
            properties.getProperty(
                "schema_region_ratis_log_force_sync_num",
                String.valueOf(conf.getSchemaRegionRatisLogForceSyncNum()))));

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
    conf.setConfigNodeRatisReconfigurationMaxRetryAttempts(
        Integer.parseInt(
            properties.getProperty(
                "config_node_ratis_reconfiguration_max_retry_attempts",
                String.valueOf(conf.getConfigNodeRatisReconfigurationMaxRetryAttempts()))));
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
    conf.setDataRegionRatisReconfigurationMaxRetryAttempts(
        Integer.parseInt(
            properties.getProperty(
                "data_region_ratis_reconfiguration_max_retry_attempts",
                String.valueOf(conf.getDataRegionRatisReconfigurationMaxRetryAttempts()))));
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
    conf.setSchemaRegionRatisReconfigurationMaxRetryAttempts(
        Integer.parseInt(
            properties.getProperty(
                "schema_region_ratis_reconfiguration_max_retry_attempts",
                String.valueOf(conf.getSchemaRegionRatisReconfigurationMaxRetryAttempts()))));
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

    conf.setRatisTransferLeaderTimeoutMs(
        Integer.parseInt(
            properties.getProperty(
                "ratis_transfer_leader_timeout_ms",
                String.valueOf(conf.getRatisTransferLeaderTimeoutMs()))));

    conf.setConfigNodeRatisLogMax(
        Long.parseLong(
            properties.getProperty(
                "config_node_ratis_log_max_size",
                String.valueOf(conf.getConfigNodeRatisLogMax()))));

    conf.setSchemaRegionRatisLogMax(
        Long.parseLong(
            properties.getProperty(
                "schema_region_ratis_log_max_size",
                String.valueOf(conf.getSchemaRegionRatisLogMax()))));

    conf.setDataRegionRatisLogMax(
        Long.parseLong(
            properties.getProperty(
                "data_region_ratis_log_max_size",
                String.valueOf(conf.getDataRegionRatisLogMax()))));

    conf.setConfigNodeRatisPeriodicSnapshotInterval(
        Long.parseLong(
            properties.getProperty(
                "config_node_ratis_periodic_snapshot_interval",
                String.valueOf(conf.getConfigNodeRatisPeriodicSnapshotInterval()))));

    conf.setSchemaRegionRatisPeriodicSnapshotInterval(
        Long.parseLong(
            properties.getProperty(
                "schema_region_ratis_periodic_snapshot_interval",
                String.valueOf(conf.getSchemaRegionRatisPeriodicSnapshotInterval()))));

    conf.setDataRegionRatisPeriodicSnapshotInterval(
        Long.parseLong(
            properties.getProperty(
                "data_region_ratis_periodic_snapshot_interval",
                String.valueOf(conf.getDataRegionRatisPeriodicSnapshotInterval()))));

    conf.setEnablePrintingNewlyCreatedPartition(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_printing_newly_created_partition",
                String.valueOf(conf.isEnablePrintingNewlyCreatedPartition()))));

    conf.setForceWalPeriodForConfigNodeSimpleInMs(
        Long.parseLong(
            properties.getProperty(
                "force_wal_period_for_confignode_simple_in_ms",
                String.valueOf(conf.getForceWalPeriodForConfigNodeSimpleInMs()))));
  }

  private void loadCQConfig(TrimProperties properties) throws IOException {
    int cqSubmitThread =
        Integer.parseInt(
            properties.getProperty(
                "continuous_query_submit_thread_count", String.valueOf(conf.getCqSubmitThread())));
    if (cqSubmitThread <= 0) {
      LOGGER.warn(
          ConfigNodeMessages.CONTINUOUS_QUERY_SUBMIT_THREAD_SHOULD_BE_GREATER_THAN_0,
          cqSubmitThread,
          conf.getCqSubmitThread());
      cqSubmitThread = conf.getCqSubmitThread();
    }
    conf.setCqSubmitThread(cqSubmitThread);

    conf.setCqMinEveryIntervalInMs(loadCqMinEveryIntervalInMs(properties));
  }

  private long loadHeartbeatIntervalInMs(TrimProperties properties) {
    return Long.parseLong(
        properties.getProperty(
            "heartbeat_interval_in_ms", String.valueOf(conf.getHeartbeatIntervalInMs())));
  }

  private long loadHotReloadHeartbeatIntervalInMs(TrimProperties properties) throws IOException {
    long heartbeatIntervalInMs = loadHeartbeatIntervalInMs(properties);
    if (heartbeatIntervalInMs <= 0) {
      throw new IOException(
          ConfigNodeMessages
                  .EXCEPTION_HEARTBEAT_INTERVAL_IN_MS_SHOULD_BE_GREATER_THAN_0_BUT_WAS_2269997B
              + heartbeatIntervalInMs
              + ".");
    }
    return heartbeatIntervalInMs;
  }

  private long loadCqMinEveryIntervalInMs(TrimProperties properties) {
    long cqMinEveryIntervalInMs =
        Long.parseLong(
            properties.getProperty(
                "continuous_query_min_every_interval_in_ms",
                String.valueOf(conf.getCqMinEveryIntervalInMs())));
    if (cqMinEveryIntervalInMs <= 0) {
      LOGGER.warn(
          ConfigNodeMessages.CONTINUOUS_QUERY_MIN_EVERY_INTERVAL_IN_MS_SHOULD_BE_GREATER,
          cqMinEveryIntervalInMs,
          conf.getCqMinEveryIntervalInMs());
      cqMinEveryIntervalInMs = conf.getCqMinEveryIntervalInMs();
    }

    return cqMinEveryIntervalInMs;
  }

  private long loadHotReloadCqMinEveryIntervalInMs(TrimProperties properties) throws IOException {
    long cqMinEveryIntervalInMs =
        Long.parseLong(
            properties.getProperty(
                "continuous_query_min_every_interval_in_ms",
                String.valueOf(conf.getCqMinEveryIntervalInMs())));
    if (cqMinEveryIntervalInMs <= 0) {
      throw new IOException(
          ConfigNodeMessages
                  .EXCEPTION_CONTINUOUS_QUERY_MIN_EVERY_INTERVAL_IN_MS_SHOULD_BE_GREATER_THAN_0_BUT_CURRENT_VALUE_IS_F9A1BEC4
              + cqMinEveryIntervalInMs
              + ".");
    }

    return cqMinEveryIntervalInMs;
  }

  private void loadRegionGroupExtensionConfig(TrimProperties properties) throws IOException {
    RegionGroupExtensionPolicy schemaRegionGroupExtensionPolicy =
        RegionGroupExtensionPolicy.parse(
            properties.getProperty(
                "schema_region_group_extension_policy",
                conf.getSchemaRegionGroupExtensionPolicy().getPolicy()));
    int defaultSchemaRegionGroupNumPerDatabase =
        Integer.parseInt(
            properties.getProperty(
                "default_schema_region_group_num_per_database",
                String.valueOf(conf.getDefaultSchemaRegionGroupNumPerDatabase())));
    int schemaRegionPerDataNode =
        parseIntegerCompatibleValue(
            properties, "schema_region_per_data_node", conf.getSchemaRegionPerDataNode());
    RegionGroupExtensionPolicy dataRegionGroupExtensionPolicy =
        RegionGroupExtensionPolicy.parse(
            properties.getProperty(
                "data_region_group_extension_policy",
                conf.getDataRegionGroupExtensionPolicy().getPolicy()));
    int defaultDataRegionGroupNumPerDatabase =
        Integer.parseInt(
            properties.getProperty(
                "default_data_region_group_num_per_database",
                String.valueOf(conf.getDefaultDataRegionGroupNumPerDatabase())));
    int dataRegionPerDataNode =
        parseIntegerCompatibleValue(
            properties, "data_region_per_data_node", conf.getDataRegionPerDataNode());

    if (defaultSchemaRegionGroupNumPerDatabase <= 0) {
      throw new IOException(
          ConfigNodeMessages
              .EXCEPTION_DEFAULT_SCHEMA_REGION_GROUP_NUM_PER_DATABASE_SHOULD_BE_POSITIVE_C8F77774);
    }
    if (schemaRegionPerDataNode <= 0) {
      throw new IOException(
          ConfigNodeMessages.EXCEPTION_SCHEMA_REGION_PER_DATA_NODE_SHOULD_BE_POSITIVE_CDEB9FC1);
    }
    if (defaultDataRegionGroupNumPerDatabase <= 0) {
      throw new IOException(
          ConfigNodeMessages
              .EXCEPTION_DEFAULT_DATA_REGION_GROUP_NUM_PER_DATABASE_SHOULD_BE_POSITIVE_8E68B5A0);
    }
    if (dataRegionPerDataNode < 0) {
      throw new IOException(
          ConfigNodeMessages.EXCEPTION_DATA_REGION_PER_DATA_NODE_SHOULD_BE_NON_NEGATIVE_D2960368);
    }

    conf.setSchemaRegionGroupExtensionPolicy(schemaRegionGroupExtensionPolicy);
    conf.setDefaultSchemaRegionGroupNumPerDatabase(defaultSchemaRegionGroupNumPerDatabase);
    conf.setSchemaRegionPerDataNode(schemaRegionPerDataNode);
    conf.setDataRegionGroupExtensionPolicy(dataRegionGroupExtensionPolicy);
    conf.setDefaultDataRegionGroupNumPerDatabase(defaultDataRegionGroupNumPerDatabase);
    conf.setDataRegionPerDataNode(dataRegionPerDataNode);
  }

  private int parseIntegerCompatibleValue(
      TrimProperties properties, String propertyName, int defaultValue) throws IOException {
    String propertyValue = properties.getProperty(propertyName, String.valueOf(defaultValue));
    try {
      return new BigDecimal(propertyValue).stripTrailingZeros().intValueExact();
    } catch (ArithmeticException | NumberFormatException e) {
      throw new IOException(
          String.format(
              ConfigNodeMessages.EXCEPTION_ARG_SHOULD_BE_AN_INTEGER_BUT_WAS_ARG_56B5D91B,
              propertyName,
              propertyValue),
          e);
    }
  }

  private String loadReadConsistencyLevel(TrimProperties properties) throws IOException {
    String readConsistencyLevel =
        properties.getProperty("read_consistency_level", conf.getReadConsistencyLevel());
    if (readConsistencyLevel.equals("strong") || readConsistencyLevel.equals("weak")) {
      return readConsistencyLevel;
    }
    throw new IOException(
        String.format(
            ConfigNodeMessages.UNKNOWN_READ_CONSISTENCY_LEVEL_PLEASE_SET_TO, readConsistencyLevel));
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
      LOGGER.warn(
          ConfigNodeMessages.UNKNOWN_HOST_WHEN_CHECKING_SEED_CONFIGNODE_IP,
          conf.getInternalAddress(),
          e);
      return false;
    }
  }

  public void loadHotModifiedProps(TrimProperties properties) throws IOException {
    Optional.ofNullable(properties.getProperty(IoTDBConstant.CLUSTER_NAME))
        .ifPresent(conf::setClusterName);
    long heartbeatIntervalInMs = loadHotReloadHeartbeatIntervalInMs(properties);
    long cqMinEveryIntervalInMs = loadHotReloadCqMinEveryIntervalInMs(properties);
    String readConsistencyLevel = loadReadConsistencyLevel(properties);
    loadRegionGroupExtensionConfig(properties);
    conf.setHeartbeatIntervalInMs(heartbeatIntervalInMs);
    conf.setCqMinEveryIntervalInMs(cqMinEveryIntervalInMs);
    conf.setReadConsistencyLevel(readConsistencyLevel);
    Optional.ofNullable(properties.getProperty("enable_topology_probing"))
        .ifPresent(v -> conf.setEnableTopologyProbing(Boolean.parseBoolean(v)));
    // Keep the ConfigNode's CommonConfig in sync so that SHOW VARIABLES / cluster-parameter
    // consistency checks report the hot-reloaded value; the DataNode side additionally refreshes
    // JVMCommonUtils where the ReadOnly disk guard actually consumes it.
    commonDescriptor.loadHotModifiedDiskSpaceWarningThreshold(properties);
    loadHotModifiedProcedureConfig(properties);
    loadPipeHotModifiedProp(properties);
    ConfigurationFileUtils.updateAppliedProperties(properties, true);
  }

  private void loadHotModifiedProcedureConfig(TrimProperties properties) throws IOException {
    int procedureCompletedCleanInterval =
        Integer.parseInt(
            properties.getProperty(
                "procedure_completed_clean_interval",
                String.valueOf(conf.getProcedureCompletedCleanInterval())));
    if (procedureCompletedCleanInterval <= 0) {
      throw new IOException(
          ConfigNodeMessages
                  .EXCEPTION_PROCEDURE_COMPLETED_CLEAN_INTERVAL_SHOULD_BE_GREATER_THAN_0_BUT_WAS_8781558E
              + procedureCompletedCleanInterval
              + ".");
    }
    int procedureCompletedEvictTTL =
        Integer.parseInt(
            properties.getProperty(
                "procedure_completed_evict_ttl",
                String.valueOf(conf.getProcedureCompletedEvictTTL())));
    if (procedureCompletedEvictTTL <= 0) {
      throw new IOException(
          ConfigNodeMessages
                  .EXCEPTION_PROCEDURE_COMPLETED_EVICT_TTL_SHOULD_BE_GREATER_THAN_0_BUT_WAS_5A4D0CF6
              + procedureCompletedEvictTTL
              + ".");
    }
    conf.setProcedureCompletedCleanInterval(procedureCompletedCleanInterval);
    conf.setProcedureCompletedEvictTTL(procedureCompletedEvictTTL);
  }

  private void loadPipeHotModifiedProp(TrimProperties properties) throws IOException {
    PipeDescriptor.loadPipeProps(commonDescriptor.getConfig(), properties, true);
    LoggerPeriodicalLogReducer.update();
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
