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
  public URL getPropsUrl() {
    // Check if a config-directory was specified first.
    String urlString = System.getProperty(ConfigNodeConstant.CONFIGNODE_CONF, null);
    // If it wasn't, check if a home directory was provided
    if (urlString == null) {
      urlString = System.getProperty(ConfigNodeConstant.CONFIGNODE_HOME, null);
      if (urlString != null) {
        urlString =
            urlString
                + File.separatorChar
                + "conf"
                + File.separatorChar
                + ConfigNodeConstant.CONF_FILE_NAME;
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
      urlString += (File.separatorChar + ConfigNodeConstant.CONF_FILE_NAME);
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
    URL url = getPropsUrl();
    if (url == null) {
      LOGGER.warn(
          "Couldn't load the ConfigNode configuration from any of the known sources. Use default configuration.");
      return;
    }

    try (InputStream inputStream = url.openStream()) {

      LOGGER.info("start reading ConfigNode conf file: {}", url);

      Properties properties = new Properties();
      properties.load(inputStream);

      conf.setInternalAddress(
          properties.getProperty(IoTDBConstant.INTERNAL_ADDRESS, conf.getInternalAddress()));

      conf.setInternalPort(
          Integer.parseInt(
              properties.getProperty(
                  IoTDBConstant.INTERNAL_PORT, String.valueOf(conf.getInternalPort()))));

      conf.setConsensusPort(
          Integer.parseInt(
              properties.getProperty(
                  IoTDBConstant.CONSENSUS_PORT, String.valueOf(conf.getConsensusPort()))));

      // TODO: Enable multiple target_config_nodes
      String targetConfigNodes = properties.getProperty(IoTDBConstant.TARGET_CONFIG_NODES, null);
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
                  "schema_region_per_data_node",
                  String.valueOf(conf.getSchemaRegionPerDataNode()))));

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

      conf.setRpcAdvancedCompressionEnable(
          Boolean.parseBoolean(
              properties.getProperty(
                  "rpc_advanced_compression_enable",
                  String.valueOf(conf.isRpcAdvancedCompressionEnable()))));

      conf.setRpcMaxConcurrentClientNum(
          Integer.parseInt(
              properties.getProperty(
                  "rpc_max_concurrent_client_num",
                  String.valueOf(conf.getRpcMaxConcurrentClientNum()))));

      conf.setThriftDefaultBufferSize(
          Integer.parseInt(
              properties.getProperty(
                  "thrift_init_buffer_size", String.valueOf(conf.getThriftDefaultBufferSize()))));

      conf.setThriftMaxFrameSize(
          Integer.parseInt(
              properties.getProperty(
                  "thrift_max_frame_size", String.valueOf(conf.getThriftMaxFrameSize()))));

      conf.setSystemDir(properties.getProperty("system_dir", conf.getSystemDir()));

      conf.setConsensusDir(properties.getProperty("consensus_dir", conf.getConsensusDir()));

      conf.setUdfLibDir(properties.getProperty("udf_lib_dir", conf.getUdfLibDir()));

      conf.setTemporaryLibDir(
          properties.getProperty("temporary_lib_dir", conf.getTemporaryLibDir()));

      conf.setTimePartitionInterval(
          Long.parseLong(
              properties.getProperty(
                  "time_partition_interval", String.valueOf(conf.getTimePartitionInterval()))));

      conf.setSchemaReplicationFactor(
          Integer.parseInt(
              properties.getProperty(
                  "schema_replication_factor", String.valueOf(conf.getSchemaReplicationFactor()))));

      conf.setDataReplicationFactor(
          Integer.parseInt(
              properties.getProperty(
                  "data_replication_factor", String.valueOf(conf.getDataReplicationFactor()))));

      conf.setHeartbeatInterval(
          Long.parseLong(
              properties.getProperty(
                  "heartbeat_interval", String.valueOf(conf.getHeartbeatInterval()))));

      String routingPolicy = properties.getProperty("routing_policy", conf.getRoutingPolicy());
      if (routingPolicy.equals(RouteBalancer.GREEDY_POLICY)
          || routingPolicy.equals(RouteBalancer.LEADER_POLICY)) {
        conf.setRoutingPolicy(routingPolicy);
      } else {
        throw new IOException(
            String.format(
                "Unknown routing_policy: %s, please set to \"leader\" or \"greedy\"",
                routingPolicy));
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

      conf.setProcedureCoreWorkerThreadsSize(
          Integer.parseInt(
              properties.getProperty(
                  "procedure_core_worker_thread_size",
                  String.valueOf(conf.getProcedureCoreWorkerThreadsSize()))));

      loadRatisConsensusConfig(properties);
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
  }

  private void loadRatisConsensusConfig(Properties properties) {
    conf.setRatisConsensusLogAppenderBufferSize(
        Long.parseLong(
            properties.getProperty(
                "ratis_log_appender_buffer_size_max",
                String.valueOf(conf.getRatisConsensusLogAppenderBufferSize()))));
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
