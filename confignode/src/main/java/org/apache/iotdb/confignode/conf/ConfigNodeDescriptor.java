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
  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConf();

  private ConfigNodeDescriptor() {
    loadProps();
  }

  public ConfigNodeConfig getConf() {
    return CONF;
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
        CONF.formulateFolders();

        CommonDescriptor.getInstance().initCommonConfigDir(CONF.getCnSystemDir());

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
    /* ConfigNode RPC Configuration */
    loadConfigNodeRPCConfiguration(properties);

    /* Target ConfigNodes */
    loadTargetConfigNodes(properties);

    /* Directory Configuration */
    loadDirectoryConfiguration(properties);

    /* Thrift RPC Configuration */
    loadThriftRPCConfiguration(properties);

    /* Retain Configurations */
    // Notice: Never read any configuration through loadRetainConfiguration
    // Every parameter in retain configuration will be deleted or moved into other set
    loadRetainConfiguration(properties);
  }

  private void loadConfigNodeRPCConfiguration(Properties properties) {
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
  }

  private void loadTargetConfigNodes(Properties properties) throws BadNodeUrlException {
    // TODO: Enable multiple target_config_node_list
    String targetConfigNodes =
        properties.getProperty(IoTDBConstant.CN_TARGET_CONFIG_NODE_LIST, null);
    if (targetConfigNodes != null) {
      CONF.setCnTargetConfigNode(NodeUrlUtils.parseTEndPointUrl(targetConfigNodes.trim()));
    }
  }

  private void loadDirectoryConfiguration(Properties properties) {
    CONF.setCnSystemDir(properties.getProperty("cn_system_dir", CONF.getCnSystemDir()).trim());

    CONF.setCnConsensusDir(
        properties.getProperty("cn_consensus_dir", CONF.getCnConsensusDir()).trim());
  }

  private void loadThriftRPCConfiguration(Properties properties) {
    CONF.setCnRpcThriftCompressionEnable(
        Boolean.parseBoolean(
            properties
                .getProperty(
                    "cn_rpc_thrift_compression_enable",
                    String.valueOf(CONF.isCnRpcThriftCompressionEnable()))
                .trim()));
    COMMON_CONFIG.setRpcThriftCompressionEnable(CONF.isCnRpcThriftCompressionEnable());

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
    COMMON_CONFIG.setConnectionTimeoutInMS(CONF.getCnConnectionTimeoutMs());

    CONF.setCnSelectorThreadNumsOfClientManager(
        Integer.parseInt(
            properties
                .getProperty(
                    "cn_selector_thread_nums_of_client_manager",
                    String.valueOf(CONF.getCnSelectorThreadNumsOfClientManager()))
                .trim()));
    COMMON_CONFIG.setSelectorThreadCountOfClientManager(
        CONF.getCnSelectorThreadNumsOfClientManager());

    CONF.setCnCoreClientCountForEachNodeInClientManager(
        Integer.parseInt(
            properties
                .getProperty(
                    "cn_core_client_count_for_each_node_in_client_manager",
                    String.valueOf(CONF.getCnCoreClientCountForEachNodeInClientManager()))
                .trim()));
    COMMON_CONFIG.setCoreClientCountForEachNodeInClientManager(
        CONF.getCnCoreClientCountForEachNodeInClientManager());

    CONF.setCnMaxClientCountForEachNodeInClientManager(
        Integer.parseInt(
            properties
                .getProperty(
                    "cn_max_client_count_for_each_node_in_client_manager",
                    String.valueOf(CONF.getCnMaxClientCountForEachNodeInClientManager()))
                .trim()));
    COMMON_CONFIG.setMaxClientCountForEachNodeInClientManager(
        CONF.getCnMaxClientCountForEachNodeInClientManager());
  }

  /**
   * Load retain configuration. Please don't insert any code within this function
   *
   * <p>TODO: Delete this function in the future
   */
  private void loadRetainConfiguration(Properties properties) throws IOException {
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
}
