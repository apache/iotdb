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

    // TODO: Enable multiple target_config_nodes
    String targetConfigNodes =
        properties.getProperty(IoTDBConstant.CN_TARGET_CONFIG_NODES, null).trim();
    if (targetConfigNodes != null) {
      conf.setTargetConfigNode(NodeUrlUtils.parseTEndPointUrl(targetConfigNodes));
    }

    conf.setSeriesPartitionSlotNum(
        Integer.parseInt(
            properties
                .getProperty(
                    "series_partition_slot_num", String.valueOf(conf.getSeriesPartitionSlotNum()))
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

    conf.setSchemaRegionPerDataNode(
        Double.parseDouble(
            properties
                .getProperty(
                    "schema_region_per_data_node",
                    String.valueOf(conf.getSchemaRegionPerDataNode()))
                .trim()));

    conf.setDataRegionConsensusProtocolClass(
        properties
            .getProperty(
                "data_region_consensus_protocol_class", conf.getDataRegionConsensusProtocolClass())
            .trim());

    conf.setDataRegionPerProcessor(
        Double.parseDouble(
            properties
                .getProperty(
                    "data_region_per_processor", String.valueOf(conf.getDataRegionPerProcessor()))
                .trim()));

    try {
      conf.setRegionAllocateStrategy(
          RegionBalancer.RegionAllocateStrategy.valueOf(
              properties
                  .getProperty("region_allocate_strategy", conf.getRegionAllocateStrategy().name())
                  .trim()));
    } catch (IllegalArgumentException e) {
      LOGGER.warn(
          "The configured region allocate strategy does not exist, use the default: GREEDY!");
    }

    conf.setCnRpcAdvancedCompressionEnable(
        Boolean.parseBoolean(
            properties
                .getProperty(
                    "cn_rpc_advanced_compression_enable",
                    String.valueOf(conf.isCnRpcAdvancedCompressionEnable()))
                .trim()));

    conf.setCnRpcMaxConcurrentClientNum(
        Integer.parseInt(
            properties
                .getProperty(
                    "cn_rpc_max_concurrent_client_num",
                    String.valueOf(conf.getCnRpcMaxConcurrentClientNum()))
                .trim()));

    conf.setCnThriftDefaultBufferSize(
        Integer.parseInt(
            properties
                .getProperty(
                    "cn_thrift_init_buffer_size",
                    String.valueOf(conf.getCnThriftDefaultBufferSize()))
                .trim()));

    conf.setCnThriftMaxFrameSize(
        Integer.parseInt(
            properties
                .getProperty(
                    "cn_thrift_max_frame_size", String.valueOf(conf.getCnThriftMaxFrameSize()))
                .trim()));

    conf.setSystemDir(properties.getProperty("cn_system_dir", conf.getSystemDir()).trim());

    conf.setConsensusDir(properties.getProperty("cn_consensus_dir", conf.getConsensusDir()).trim());

    conf.setUdfLibDir(properties.getProperty("udf_lib_dir", conf.getUdfLibDir()).trim());

    conf.setTemporaryLibDir(
        properties.getProperty("udf_temporary_lib_dir", conf.getTemporaryLibDir()).trim());

    conf.setTriggerLibDir(
        properties.getProperty("trigger_lib_dir", conf.getTriggerLibDir()).trim());

    conf.setTimePartitionInterval(
        Long.parseLong(
            properties
                .getProperty(
                    "time_partition_interval_for_routing",
                    String.valueOf(conf.getTimePartitionInterval()))
                .trim()));

    conf.setSchemaReplicationFactor(
        Integer.parseInt(
            properties
                .getProperty(
                    "schema_replication_factor", String.valueOf(conf.getSchemaReplicationFactor()))
                .trim()));

    conf.setDataReplicationFactor(
        Integer.parseInt(
            properties
                .getProperty(
                    "data_replication_factor", String.valueOf(conf.getDataReplicationFactor()))
                .trim()));

    conf.setHeartbeatIntervalInMs(
        Long.parseLong(
            properties
                .getProperty(
                    "heartbeat_interval_in_ms", String.valueOf(conf.getHeartbeatIntervalInMs()))
                .trim()));

    String routingPolicy = properties.getProperty("routing_policy", conf.getRoutingPolicy()).trim();
    if (routingPolicy.equals(RouteBalancer.GREEDY_POLICY)
        || routingPolicy.equals(RouteBalancer.LEADER_POLICY)) {
      conf.setRoutingPolicy(routingPolicy);
    } else {
      throw new IOException(
          String.format(
              "Unknown routing_policy: %s, please set to \"leader\" or \"greedy\"", routingPolicy));
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

  private void loadRatisConsensusConfig(Properties properties) {
    conf.setDataRegionRatisConsensusLogAppenderBufferSize(
        Long.parseLong(
            properties
                .getProperty(
                    "data_region_ratis_log_appender_buffer_size_max",
                    String.valueOf(conf.getDataRegionRatisConsensusLogAppenderBufferSize()))
                .trim()));

    conf.setPartitionRegionRatisConsensusLogAppenderBufferSize(
        Long.parseLong(
            properties
                .getProperty(
                    "partition_region_ratis_log_appender_buffer_size_max",
                    String.valueOf(conf.getPartitionRegionRatisConsensusLogAppenderBufferSize()))
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

    conf.setPartitionRegionRatisSnapshotTriggerThreshold(
        Long.parseLong(
            properties
                .getProperty(
                    "partition_region_ratis_snapshot_trigger_threshold",
                    String.valueOf(conf.getPartitionRegionRatisSnapshotTriggerThreshold()))
                .trim()));

    conf.setPartitionRegionOneCopySnapshotTriggerThreshold(
        Long.parseLong(
            properties
                .getProperty(
                    "partition_region_one_copy_snapshot_trigger_threshold",
                    String.valueOf(conf.getPartitionRegionOneCopySnapshotTriggerThreshold()))
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

    conf.setPartitionRegionRatisLogUnsafeFlushEnable(
        Boolean.parseBoolean(
            properties
                .getProperty(
                    "partition_region_ratis_log_unsafe_flush_enable",
                    String.valueOf(conf.isPartitionRegionRatisLogUnsafeFlushEnable()))
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

    conf.setPartitionRegionRatisLogSegmentSizeMax(
        Long.parseLong(
            properties
                .getProperty(
                    "partition_region_ratis_log_segment_size_max_in_byte",
                    String.valueOf(conf.getPartitionRegionRatisLogSegmentSizeMax()))
                .trim()));

    conf.setSchemaRegionRatisLogSegmentSizeMax(
        Long.parseLong(
            properties
                .getProperty(
                    "schema_region_ratis_log_segment_size_max_in_byte",
                    String.valueOf(conf.getSchemaRegionRatisLogSegmentSizeMax()))
                .trim()));

    conf.setPartitionRegionOneCopyLogSegmentSizeMax(
        Long.parseLong(
            properties
                .getProperty(
                    "partition_region_one_copy_log_segment_size_max_in_byte",
                    String.valueOf(conf.getPartitionRegionOneCopyLogSegmentSizeMax()))
                .trim()));

    conf.setDataRegionRatisGrpcFlowControlWindow(
        Long.parseLong(
            properties
                .getProperty(
                    "data_region_ratis_grpc_flow_control_window",
                    String.valueOf(conf.getDataRegionRatisGrpcFlowControlWindow()))
                .trim()));

    conf.setPartitionRegionRatisGrpcFlowControlWindow(
        Long.parseLong(
            properties
                .getProperty(
                    "partition_region_ratis_grpc_flow_control_window",
                    String.valueOf(conf.getPartitionRegionRatisGrpcFlowControlWindow()))
                .trim()));

    conf.setSchemaRegionRatisGrpcFlowControlWindow(
        Long.parseLong(
            properties
                .getProperty(
                    "schema_region_ratis_grpc_flow_control_window",
                    String.valueOf(conf.getSchemaRegionRatisGrpcFlowControlWindow()))
                .trim()));

    conf.setDataRegionRatisRpcLeaderElectionTimeoutMinMs(
        Long.parseLong(
            properties
                .getProperty(
                    "data_region_ratis_rpc_leader_election_timeout_min_ms",
                    String.valueOf(conf.getDataRegionRatisRpcLeaderElectionTimeoutMinMs()))
                .trim()));

    conf.setPartitionRegionRatisRpcLeaderElectionTimeoutMinMs(
        Long.parseLong(
            properties
                .getProperty(
                    "partition_region_ratis_rpc_leader_election_timeout_min_ms",
                    String.valueOf(conf.getPartitionRegionRatisRpcLeaderElectionTimeoutMinMs()))
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

    conf.setPartitionRegionRatisRpcLeaderElectionTimeoutMaxMs(
        Long.parseLong(
            properties
                .getProperty(
                    "partition_region_ratis_rpc_leader_election_timeout_max_ms",
                    String.valueOf(conf.getPartitionRegionRatisRpcLeaderElectionTimeoutMaxMs()))
                .trim()));

    conf.setSchemaRegionRatisRpcLeaderElectionTimeoutMaxMs(
        Long.parseLong(
            properties
                .getProperty(
                    "schema_region_ratis_rpc_leader_election_timeout_max_ms",
                    String.valueOf(conf.getSchemaRegionRatisRpcLeaderElectionTimeoutMaxMs()))
                .trim()));

    conf.setPartitionRegionRatisRequestTimeoutMs(
        Long.parseLong(
            properties
                .getProperty(
                    "partition_region_ratis_request_timeout_ms",
                    String.valueOf(conf.getPartitionRegionRatisRequestTimeoutMs()))
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

    conf.setPartitionRegionRatisMaxRetryAttempts(
        Integer.parseInt(
            properties
                .getProperty(
                    "partition_region_ratis_max_retry_attempts",
                    String.valueOf(conf.getPartitionRegionRatisMaxRetryAttempts()))
                .trim()));
    conf.setPartitionRegionRatisInitialSleepTimeMs(
        Long.parseLong(
            properties
                .getProperty(
                    "partition_region_ratis_initial_sleep_time_ms",
                    String.valueOf(conf.getPartitionRegionRatisInitialSleepTimeMs()))
                .trim()));
    conf.setPartitionRegionRatisMaxSleepTimeMs(
        Long.parseLong(
            properties
                .getProperty(
                    "partition_region_ratis_max_sleep_time_ms",
                    String.valueOf(conf.getPartitionRegionRatisMaxSleepTimeMs()))
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

    conf.setPartitionRegionRatisPreserveLogsWhenPurge(
        Long.parseLong(
            properties
                .getProperty(
                    "partition_region_ratis_preserve_logs_num_when_purge",
                    String.valueOf(conf.getPartitionRegionRatisPreserveLogsWhenPurge()))
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
  }

  private void loadCQConfig(Properties properties) {
    int cqSubmitThread =
        Integer.parseInt(
            properties
                .getProperty(
                    "continuous_query_submit_thread_count",
                    String.valueOf(conf.getCqSubmitThread()))
                .trim());
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
            properties
                .getProperty(
                    "continuous_query_min_every_interval_in_ms",
                    String.valueOf(conf.getCqMinEveryIntervalInMs()))
                .trim());
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
   * @return True if the target_config_nodes points to itself
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
