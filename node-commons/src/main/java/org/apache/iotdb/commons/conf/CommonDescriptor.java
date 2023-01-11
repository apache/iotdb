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

package org.apache.iotdb.commons.conf;

import org.apache.iotdb.commons.consensus.ConsensusProtocolClass;
import org.apache.iotdb.commons.enums.HandleSystemErrorStrategy;
import org.apache.iotdb.commons.loadbalance.LeaderDistributionPolicy;
import org.apache.iotdb.commons.loadbalance.RegionGroupExtensionPolicy;
import org.apache.iotdb.confignode.rpc.thrift.TGlobalConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class CommonDescriptor {

  private static final Logger LOGGER = LoggerFactory.getLogger(CommonDescriptor.class);

  private final CommonConfig CONF = new CommonConfig();

  private CommonDescriptor() {
    // Empty constructor
  }

  public static CommonDescriptor getInstance() {
    return CommonDescriptorHolder.INSTANCE;
  }

  private static class CommonDescriptorHolder {

    private static final CommonDescriptor INSTANCE = new CommonDescriptor();

    private CommonDescriptorHolder() {
      // Empty constructor
    }
  }

  public CommonConfig getConfig() {
    return CONF;
  }

  public void initCommonConfigDir(String systemDir) {
    CONF.setUserFolder(systemDir + File.separator + "users");
    CONF.setRoleFolder(systemDir + File.separator + "roles");
    CONF.setProcedureWalFolder(systemDir + File.separator + "procedure");
  }



  private void loadCommonProps(Properties properties) {
    CONF.setClusterName(
      properties.getProperty(IoTDBConstant.CLUSTER_NAME, CONF.getClusterName()).trim());

    try {
      CONF.setConfigNodeConsensusProtocolClass(
        ConsensusProtocolClass.parse(
          properties
            .getProperty(
              "config_node_consensus_protocol_class", CONF.getConfigNodeConsensusProtocolClass().getProtocol())
            .trim()));
    } catch (IOException e) {
      LOGGER.warn("Unknown config_node_consensus_protocol_class in iotdb-common.properties file, use default config", e);
    }

    CONF.setSchemaReplicationFactor(
      Integer.parseInt(
        properties
          .getProperty(
            "schema_replication_factor", String.valueOf(CONF.getSchemaReplicationFactor()))
          .trim()));

    try {
      CONF.setSchemaRegionConsensusProtocolClass(
        ConsensusProtocolClass.parse(
          properties
            .getProperty(
              "schema_region_consensus_protocol_class", CONF.getSchemaRegionConsensusProtocolClass().getProtocol())
            .trim()));
    } catch (IOException e) {
      LOGGER.warn("Unknown schema_region_consensus_protocol_class in iotdb-common.properties file, use default config", e);
    }

    CONF.setDataReplicationFactor(
      Integer.parseInt(
        properties
          .getProperty(
            "data_replication_factor", String.valueOf(CONF.getDataReplicationFactor()))
          .trim()));

    try {
      CONF.setDataRegionConsensusProtocolClass(
        ConsensusProtocolClass.parse(
          properties
            .getProperty(
              "data_region_consensus_protocol_class", CONF.getDataRegionConsensusProtocolClass().getProtocol())
            .trim()));
    } catch (IOException e) {
      LOGGER.warn("Unknown data_region_consensus_protocol_class in iotdb-common.properties file, use default config", e);
    }

    CONF.setSeriesSlotNum(
      Integer.parseInt(
        properties
          .getProperty("series_slot_num", String.valueOf(CONF.getSeriesSlotNum()))
          .trim()));

    CONF.setSeriesPartitionExecutorClass(
      properties
        .getProperty("series_partition_executor_class", CONF.getSeriesPartitionExecutorClass())
        .trim());

    CONF.setSchemaRegionPerDataNode(
      Double.parseDouble(
        properties
          .getProperty(
            "schema_region_per_data_node",
            String.valueOf(CONF.getSchemaReplicationFactor()))
          .trim()));

    CONF.setDataRegionPerProcessor(
      Double.parseDouble(
        properties
          .getProperty(
            "data_region_per_processor", String.valueOf(CONF.getDataRegionPerProcessor()))
          .trim()));

    try {
      CONF.setSchemaRegionGroupExtensionPolicy(
        RegionGroupExtensionPolicy.parse(
          properties.getProperty(
            "schema_region_group_extension_policy",
            CONF.getSchemaRegionGroupExtensionPolicy().getPolicy().trim())));
    } catch (IOException e) {
      LOGGER.warn("Unknown schema_region_group_extension_policy in iotdb-common.properties file, use default config", e);
    }

    CONF.setSchemaRegionGroupPerDatabase(
      Integer.parseInt(
        properties.getProperty(
          "schema_region_group_per_database",
          String.valueOf(CONF.getSchemaRegionGroupPerDatabase()).trim())));

    try {
      CONF.setDataRegionGroupExtensionPolicy(
        RegionGroupExtensionPolicy.parse(
          properties.getProperty(
            "data_region_group_extension_policy",
            CONF.getDataRegionGroupExtensionPolicy().getPolicy().trim())));
    } catch (IOException e) {
      LOGGER.warn("Unknown data_region_group_extension_policy in iotdb-common.properties file, use default config", e);
    }

    CONF.setDataRegionGroupPerDatabase(
      Integer.parseInt(
        properties.getProperty(
          "data_region_group_per_database",
          String.valueOf(CONF.getDataRegionGroupPerDatabase()).trim())));

    CONF.setLeastDataRegionGroupNum(
      Integer.parseInt(
        properties.getProperty(
          "least_data_region_group_num", String.valueOf(CONF.getLeastDataRegionGroupNum()))));

    CONF.setEnableDataPartitionInheritPolicy(
      Boolean.parseBoolean(
        properties.getProperty(
          "enable_data_partition_inherit_policy",
          String.valueOf(CONF.isEnableDataPartitionInheritPolicy()))));

    try {
      CONF.setLeaderDistributionPolicy(
        LeaderDistributionPolicy.parse(
          properties.getProperty(
            "leader_distribution_policy",
            CONF.getLeaderDistributionPolicy().getPolicy().trim())));
    } catch (IOException e) {
      LOGGER.warn("Unknown leader_distribution_policy in iotdb-common.properties file, use default config", e);
    }

    CONF.setEnableAutoLeaderBalanceForRatisConsensus(
      Boolean.parseBoolean(
        properties
          .getProperty(
            "enable_auto_leader_balance_for_ratis_consensus",
            String.valueOf(CONF.isEnableAutoLeaderBalanceForRatisConsensus()))
          .trim()));

    CONF.setEnableAutoLeaderBalanceForIoTConsensus(
      Boolean.parseBoolean(
        properties
          .getProperty(
            "enable_auto_leader_balance_for_iot_consensus",
            String.valueOf(CONF.isEnableAutoLeaderBalanceForIoTConsensus()))
          .trim()));

    CONF.setTimePartitionInterval(
      Long.parseLong(
        properties
          .getProperty(
            "time_partition_interval", String.valueOf(CONF.getTimePartitionInterval()))
          .trim()));

    CONF.setHeartbeatIntervalInMs(
      Long.parseLong(
        properties
          .getProperty(
            "heartbeat_interval_in_ms", String.valueOf(CONF.getHeartbeatIntervalInMs()))
          .trim()));

    CONF.setDiskSpaceWarningThreshold(
      Double.parseDouble(
        properties
          .getProperty(
            "disk_space_warning_threshold",
            String.valueOf(CONF.getDiskSpaceWarningThreshold()))
          .trim()));

    String readConsistencyLevel =
      properties.getProperty("read_consistency_level", CONF.getReadConsistencyLevel()).trim();
    if (readConsistencyLevel.equals("strong") || readConsistencyLevel.equals("weak")) {
      CONF.setReadConsistencyLevel(readConsistencyLevel);
    } else {
      LOGGER.warn(
        String.format(
          "Unknown read_consistency_level: %s, please set to \"strong\" or \"weak\"",
          readConsistencyLevel));
    }

    CONF.setDefaultTtlInMs(
      Long.parseLong(
        properties
          .getProperty("default_ttl_in_ms", String.valueOf(CONF.getDefaultTtlInMs()))
          .trim()));

    CONF.setAuthorizerProvider(
        properties.getProperty("authorizer_provider_class", CONF.getAuthorizerProvider()).trim());

    // if using org.apache.iotdb.db.auth.authorizer.OpenIdAuthorizer, openID_url is needed.
    CONF.setOpenIdProviderUrl(
        properties.getProperty("openID_url", CONF.getOpenIdProviderUrl()).trim());

    CONF.setAdminName(properties.getProperty("admin_name", CONF.getAdminName()).trim());

    CONF.setAdminPassword(
        properties.getProperty("admin_password", CONF.getAdminPassword()).trim());

    CONF.setEncryptDecryptProvider(
        properties
            .getProperty(
                "iotdb_server_encrypt_decrypt_provider", CONF.getEncryptDecryptProvider())
            .trim());

    CONF.setEncryptDecryptProviderParameter(
        properties.getProperty(
            "iotdb_server_encrypt_decrypt_provider_parameter",
            CONF.getEncryptDecryptProviderParameter()));


    CONF.setUdfDir(properties.getProperty("udf_lib_dir", CONF.getUdfDir()).trim());

    CONF.setTriggerDir(properties.getProperty("trigger_lib_dir", CONF.getTriggerDir()).trim());

    CONF.setSyncDir(properties.getProperty("dn_sync_dir", CONF.getSyncDir()).trim());

    CONF.setWalDirs(
        properties
            .getProperty("dn_wal_dirs", String.join(",", CONF.getWalDirs()))
            .trim()
            .split(","));

    CONF.setRpcThriftCompressionEnabled(
        Boolean.parseBoolean(
            properties
                .getProperty(
                    "cn_rpc_thrift_compression_enable",
                    String.valueOf(CONF.isRpcThriftCompressionEnabled()))
                .trim()));

    CONF.setConnectionTimeoutInMS(
        Integer.parseInt(
            properties
                .getProperty(
                    "dn_connection_timeout_ms", String.valueOf(CONF.getConnectionTimeoutInMS()))
                .trim()));

    CONF.setRpcThriftCompressionEnabled(
        Boolean.parseBoolean(
            properties
                .getProperty(
                    "dn_rpc_thrift_compression_enable",
                    String.valueOf(CONF.isRpcThriftCompressionEnabled()))
                .trim()));

    CONF.setSelectorNumOfClientManager(
        Integer.parseInt(
            properties
                .getProperty(
                    "dn_selector_thread_nums_of_client_manager",
                    String.valueOf(CONF.getSelectorNumOfClientManager()))
                .trim()));

    CONF.setMaxTotalClientForEachNode(
        Integer.parseInt(
            properties
                .getProperty(
                    "dn_max_connection_for_internal_service",
                    String.valueOf(CONF.getMaxTotalClientForEachNode()))
                .trim()));

    CONF.setMaxIdleClientForEachNode(
        Integer.parseInt(
            properties
                .getProperty(
                    "dn_core_connection_for_internal_service",
                    String.valueOf(CONF.getMaxIdleClientForEachNode()))
                .trim()));

    CONF.setHandleSystemErrorStrategy(
        HandleSystemErrorStrategy.valueOf(
            properties
                .getProperty(
                    "handle_system_error", String.valueOf(CONF.getHandleSystemErrorStrategy()))
                .trim()));
  }

  public void loadGlobalConfig(TGlobalConfig globalConfig) {
    CONF.setDiskSpaceWarningThreshold(globalConfig.getDiskSpaceWarningThreshold());
  }
}
