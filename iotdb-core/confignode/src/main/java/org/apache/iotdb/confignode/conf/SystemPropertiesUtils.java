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

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.BadNodeUrlException;
import org.apache.iotdb.commons.utils.NodeUrlUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import static org.apache.iotdb.commons.conf.IoTDBConstant.CLUSTER_NAME;
import static org.apache.iotdb.commons.conf.IoTDBConstant.DEFAULT_CLUSTER_NAME;

public class SystemPropertiesUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(SystemPropertiesUtils.class);

  private static final File systemPropertiesFile =
      new File(
          ConfigNodeDescriptor.getInstance().getConf().getSystemDir()
              + File.separator
              + ConfigNodeConstant.SYSTEM_FILE_NAME);

  private static final ConfigNodeConfig conf = ConfigNodeDescriptor.getInstance().getConf();
  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();

  private static final String CN_INTERNAL_ADDRESS = "cn_internal_address";
  private static final String CN_INTERNAL_PORT = "cn_internal_port";
  private static final String CN_CONSENSUS_PORT = "cn_consensus_port";
  private static final String CN_CONSENSUS_PROTOCOL = "config_node_consensus_protocol_class";
  private static final String DATA_CONSENSUS_PROTOCOL = "data_region_consensus_protocol_class";
  private static final String SCHEMA_CONSENSUS_PROTOCOL = "schema_region_consensus_protocol_class";
  private static final String SERIES_PARTITION_SLOT_NUM = "series_partition_slot_num";
  private static final String SERIES_PARTITION_EXECUTOR_CLASS = "series_partition_executor_class";
  private static final String TIME_PARTITION_INTERVAL = "time_partition_interval";

  private SystemPropertiesUtils() {
    throw new IllegalStateException("Utility class: SystemPropertiesUtils.");
  }

  /**
   * Check if the ConfigNode is restarted.
   *
   * @return True if confignode-system.properties file exist.
   */
  public static boolean isRestarted() {
    return systemPropertiesFile.exists();
  }

  /**
   * Check whether system parameters are consistent during each restart. We only invoke this
   * interface when restarted.
   *
   * @throws IOException When read the confignode-system.properties file failed
   */
  public static void checkSystemProperties() throws IOException {
    Properties systemProperties = getSystemProperties();
    boolean needReWrite = false;
    final String format =
        "[SystemProperties] The parameter \"{}\" can't be modified after first startup."
            + " Your configuration: {} will be forced update to: {}";

    // Cluster configuration
    String clusterName = systemProperties.getProperty(CLUSTER_NAME, null);
    if (clusterName == null) {
      needReWrite = true;
    } else if (!clusterName.equals(conf.getClusterName())) {
      LOGGER.warn(format, CLUSTER_NAME, conf.getClusterName(), clusterName);
      conf.setClusterName(clusterName);
    }

    String internalAddress = systemProperties.getProperty(CN_INTERNAL_ADDRESS, null);
    if (internalAddress == null) {
      needReWrite = true;
    } else if (!internalAddress.equals(conf.getInternalAddress())) {
      LOGGER.warn(format, CN_INTERNAL_ADDRESS, conf.getInternalAddress(), internalAddress);
      conf.setInternalAddress(internalAddress);
    }

    if (systemProperties.getProperty(CN_INTERNAL_PORT, null) == null) {
      needReWrite = true;
    } else {
      int internalPort = Integer.parseInt(systemProperties.getProperty(CN_INTERNAL_PORT));
      if (internalPort != conf.getInternalPort()) {
        LOGGER.warn(format, CN_INTERNAL_PORT, conf.getInternalPort(), internalPort);
        conf.setInternalPort(internalPort);
      }
    }

    if (systemProperties.getProperty(CN_CONSENSUS_PORT, null) == null) {
      needReWrite = true;
    } else {
      int consensusPort = Integer.parseInt(systemProperties.getProperty(CN_CONSENSUS_PORT));
      if (consensusPort != conf.getConsensusPort()) {
        LOGGER.warn(format, CN_CONSENSUS_PORT, conf.getConsensusPort(), consensusPort);
        conf.setConsensusPort(consensusPort);
      }
    }

    // Consensus protocol configuration
    String configNodeConsensusProtocolClass =
        systemProperties.getProperty(CN_CONSENSUS_PROTOCOL, null);
    if (configNodeConsensusProtocolClass == null) {
      needReWrite = true;
    } else if (!configNodeConsensusProtocolClass.equals(
        conf.getConfigNodeConsensusProtocolClass())) {
      LOGGER.warn(
          format,
          CN_CONSENSUS_PROTOCOL,
          conf.getConfigNodeConsensusProtocolClass(),
          configNodeConsensusProtocolClass);
      conf.setConfigNodeConsensusProtocolClass(configNodeConsensusProtocolClass);
    }

    String dataRegionConsensusProtocolClass =
        systemProperties.getProperty(DATA_CONSENSUS_PROTOCOL, null);
    if (dataRegionConsensusProtocolClass == null) {
      needReWrite = true;
    } else if (!dataRegionConsensusProtocolClass.equals(
        conf.getDataRegionConsensusProtocolClass())) {
      LOGGER.warn(
          format,
          DATA_CONSENSUS_PROTOCOL,
          conf.getDataRegionConsensusProtocolClass(),
          dataRegionConsensusProtocolClass);
      conf.setDataRegionConsensusProtocolClass(dataRegionConsensusProtocolClass);
    }

    String schemaRegionConsensusProtocolClass =
        systemProperties.getProperty(SCHEMA_CONSENSUS_PROTOCOL, null);
    if (schemaRegionConsensusProtocolClass == null) {
      needReWrite = true;
    } else if (!schemaRegionConsensusProtocolClass.equals(
        conf.getSchemaRegionConsensusProtocolClass())) {
      LOGGER.warn(
          format,
          SCHEMA_CONSENSUS_PROTOCOL,
          conf.getSchemaRegionConsensusProtocolClass(),
          schemaRegionConsensusProtocolClass);
      conf.setSchemaRegionConsensusProtocolClass(schemaRegionConsensusProtocolClass);
    }

    // PartitionSlot configuration
    if (systemProperties.getProperty(SERIES_PARTITION_SLOT_NUM, null) == null) {
      needReWrite = true;
    } else {
      int seriesPartitionSlotNum =
          Integer.parseInt(systemProperties.getProperty(SERIES_PARTITION_SLOT_NUM));
      if (seriesPartitionSlotNum != conf.getSeriesSlotNum()) {
        LOGGER.warn(format, "series_slot_num", conf.getSeriesSlotNum(), seriesPartitionSlotNum);
        conf.setSeriesSlotNum(seriesPartitionSlotNum);
      }
    }

    String seriesPartitionSlotExecutorClass =
        systemProperties.getProperty(SERIES_PARTITION_EXECUTOR_CLASS, null);
    if (seriesPartitionSlotExecutorClass == null) {
      needReWrite = true;
    } else if (!Objects.equals(
        seriesPartitionSlotExecutorClass, conf.getSeriesPartitionExecutorClass())) {
      LOGGER.warn(
          format,
          SERIES_PARTITION_EXECUTOR_CLASS,
          conf.getSeriesPartitionExecutorClass(),
          seriesPartitionSlotExecutorClass);
      conf.setSeriesPartitionExecutorClass(seriesPartitionSlotExecutorClass);
    }

    if (systemProperties.getProperty(TIME_PARTITION_INTERVAL, null) == null) {
      needReWrite = true;
    } else {
      long timePartitionInterval =
          Long.parseLong(systemProperties.getProperty(TIME_PARTITION_INTERVAL));
      if (timePartitionInterval != COMMON_CONFIG.getTimePartitionInterval()) {
        LOGGER.warn(
            format,
            TIME_PARTITION_INTERVAL,
            COMMON_CONFIG.getTimePartitionInterval(),
            timePartitionInterval);
        COMMON_CONFIG.setTimePartitionInterval(timePartitionInterval);
      }
    }

    if (needReWrite) {
      // Re-write special parameters if necessary
      storeSystemParameters();
    }
  }

  /**
   * Load the config_node_list in confignode-system.properties file. We only invoke this interface
   * when restarted.
   *
   * @return The property of config_node_list in confignode-system.properties file
   * @throws IOException When load confignode-system.properties file failed
   * @throws BadNodeUrlException When parsing config_node_list failed
   */
  public static List<TConfigNodeLocation> loadConfigNodeList()
      throws IOException, BadNodeUrlException {
    Properties systemProperties = getSystemProperties();
    String addresses = systemProperties.getProperty("config_node_list", null);

    if (addresses != null && !addresses.isEmpty()) {
      return NodeUrlUtils.parseTConfigNodeUrls(addresses);
    } else {
      return new ArrayList<>();
    }
  }

  /**
   * The system parameters can't be changed after the ConfigNode first started. Therefore, store
   * them in confignode-system.properties during the first startup.
   *
   * @throws IOException getSystemProperties()
   */
  public static void storeSystemParameters() throws IOException {
    Properties systemProperties = getSystemProperties();

    systemProperties.setProperty("iotdb_version", IoTDBConstant.VERSION);
    systemProperties.setProperty("commit_id", IoTDBConstant.BUILD_INFO);

    // Cluster configuration
    systemProperties.setProperty("cluster_name", conf.getClusterName());
    LOGGER.info("[SystemProperties] store cluster_name: {}", conf.getClusterName());
    systemProperties.setProperty("config_node_id", String.valueOf(conf.getConfigNodeId()));
    LOGGER.info("[SystemProperties] store config_node_id: {}", conf.getConfigNodeId());
    systemProperties.setProperty(
        "is_seed_config_node",
        String.valueOf(ConfigNodeDescriptor.getInstance().isSeedConfigNode()));
    LOGGER.info(
        "[SystemProperties] store is_seed_config_node: {}",
        ConfigNodeDescriptor.getInstance().isSeedConfigNode());

    // Startup configuration
    systemProperties.setProperty(CN_INTERNAL_ADDRESS, String.valueOf(conf.getInternalAddress()));
    systemProperties.setProperty(CN_INTERNAL_PORT, String.valueOf(conf.getInternalPort()));
    systemProperties.setProperty(CN_CONSENSUS_PORT, String.valueOf(conf.getConsensusPort()));

    // Consensus protocol configuration
    systemProperties.setProperty(CN_CONSENSUS_PROTOCOL, conf.getConfigNodeConsensusProtocolClass());
    systemProperties.setProperty(
        DATA_CONSENSUS_PROTOCOL, conf.getDataRegionConsensusProtocolClass());
    systemProperties.setProperty(
        SCHEMA_CONSENSUS_PROTOCOL, conf.getSchemaRegionConsensusProtocolClass());

    // PartitionSlot configuration
    systemProperties.setProperty(
        SERIES_PARTITION_SLOT_NUM, String.valueOf(conf.getSeriesSlotNum()));
    systemProperties.setProperty(
        SERIES_PARTITION_EXECUTOR_CLASS, conf.getSeriesPartitionExecutorClass());
    systemProperties.setProperty(
        TIME_PARTITION_INTERVAL, String.valueOf(COMMON_CONFIG.getTimePartitionInterval()));
    systemProperties.setProperty("timestamp_precision", COMMON_CONFIG.getTimestampPrecision());

    // DataNode Functions
    systemProperties.setProperty("schema_engine_mode", COMMON_CONFIG.getSchemaEngineMode());
    systemProperties.setProperty(
        "tag_attribute_total_size", String.valueOf(COMMON_CONFIG.getTagAttributeTotalSize()));

    storeSystemProperties(systemProperties);
  }

  /**
   * Store the latest config_node_list in confignode-system.properties file
   *
   * @param configNodes The latest ConfigNodeList
   * @throws IOException When store confignode-system.properties file failed
   */
  public static void storeConfigNodeList(List<TConfigNodeLocation> configNodes) throws IOException {
    if (!systemPropertiesFile.exists()) {
      // Avoid creating confignode-system.properties files during
      // synchronizing the ApplyConfigNode logs from the ConsensusLayer.
      // 1. For the Non-Seed-ConfigNode, We don't need to create confignode-system.properties file
      // until the leader sends the notifyRegisterSuccess request.
      // 2. The leader commits the ApplyConfigNode log at the end of AddConfigNodeProcedure,
      // in which case the latest config_node_list will be updated.
      return;
    }

    Properties systemProperties = getSystemProperties();
    systemProperties.setProperty(
        "config_node_list", NodeUrlUtils.convertTConfigNodeUrls(configNodes));

    storeSystemProperties(systemProperties);
  }

  /**
   * Load the cluster_name in confignode-system.properties file. We only invoke this interface when
   * restarted.
   *
   * @return The property of cluster_name in confignode-system.properties file
   * @throws IOException When load confignode-system.properties file failed
   */
  public static String loadClusterNameWhenRestarted() throws IOException {
    Properties systemProperties = getSystemProperties();
    String clusterName = systemProperties.getProperty(CLUSTER_NAME, null);
    if (clusterName == null) {
      LOGGER.warn(
          "Lack cluster_name field in "
              + "data/confignode/system/confignode-system.properties, set it as defaultCluster");
      systemProperties.setProperty(CLUSTER_NAME, DEFAULT_CLUSTER_NAME);
      return systemProperties.getProperty(CLUSTER_NAME, null);
    }
    return clusterName;
  }

  /**
   * Load the config_node_id in confignode-system.properties file. We only invoke this interface
   * when restarted.
   *
   * @return The property of config_node_id in confignode-system.properties file
   * @throws IOException When load confignode-system.properties file failed
   */
  public static int loadConfigNodeIdWhenRestarted() throws IOException {
    Properties systemProperties = getSystemProperties();
    try {
      return Integer.parseInt(systemProperties.getProperty("config_node_id", null));
    } catch (NumberFormatException e) {
      throw new IOException(
          "The parameter config_node_id doesn't exist in "
              + "data/confignode/system/confignode-system.properties. "
              + "Please delete data dir data/confignode and restart again.");
    }
  }

  /**
   * Check if the current ConfigNode is SeedConfigNode.
   *
   * <p>Notice: Only invoke this interface when restarted.
   *
   * @return True if the is_seed_config_node is set to True in iotdb-confignode.properties file
   */
  public static boolean isSeedConfigNode() {
    try {
      Properties systemProperties = getSystemProperties();
      boolean isSeedConfigNode =
          Boolean.parseBoolean(systemProperties.getProperty("is_seed_config_node", null));
      if (isSeedConfigNode) {
        return true;
      } else {
        return ConfigNodeDescriptor.getInstance().isSeedConfigNode();
      }
    } catch (IOException ignore) {
      return false;
    }
  }

  private static synchronized Properties getSystemProperties() throws IOException {
    // Create confignode-system.properties file if necessary
    if (!systemPropertiesFile.exists()) {
      if (systemPropertiesFile.createNewFile()) {
        LOGGER.info(
            "System properties file {} for ConfigNode is created.",
            systemPropertiesFile.getAbsolutePath());
      } else {
        LOGGER.error(
            "Can't create the system properties file {} for ConfigNode. "
                + "IoTDB-ConfigNode is shutdown.",
            systemPropertiesFile.getAbsolutePath());
        throw new IOException("Can't create system properties file");
      }
    }

    Properties systemProperties = new Properties();
    try (FileInputStream inputStream = new FileInputStream(systemPropertiesFile)) {
      systemProperties.load(inputStream);
    }
    return systemProperties;
  }

  private static synchronized void storeSystemProperties(Properties systemProperties)
      throws IOException {
    try (FileOutputStream fileOutputStream = new FileOutputStream(systemPropertiesFile)) {
      systemProperties.store(fileOutputStream, "");
    }
  }
}
