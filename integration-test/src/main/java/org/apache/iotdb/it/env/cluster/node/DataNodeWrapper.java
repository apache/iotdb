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

package org.apache.iotdb.it.env.cluster.node;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.it.env.cluster.EnvUtils;
import org.apache.iotdb.it.env.cluster.config.MppBaseConfig;
import org.apache.iotdb.it.env.cluster.config.MppJVMConfig;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.consensus.ConsensusFactory.SIMPLE_CONSENSUS;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.COMMON_PROPERTIES_FILE;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.CONFIG_NODE_CONSENSUS_PROTOCOL_CLASS;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.DATANODE_INIT_HEAP_SIZE;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.DATANODE_MAX_DIRECT_MEMORY_SIZE;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.DATANODE_MAX_HEAP_SIZE;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.DATA_NODE_NAME;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.DATA_NODE_PROPERTIES_FILE;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.DATA_REGION_CONSENSUS_PROTOCOL_CLASS;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.DATA_REPLICATION_FACTOR;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.DEFAULT_DATA_NODE_COMMON_PROPERTIES;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.DEFAULT_DATA_NODE_PROPERTIES;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.DN_CONNECTION_TIMEOUT_MS;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.DN_CONSENSUS_DIR;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.DN_DATA_DIRS;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.DN_DATA_REGION_CONSENSUS_PORT;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.DN_JOIN_CLUSTER_RETRY_INTERVAL_MS;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.DN_METRIC_INTERNAL_REPORTER_TYPE;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.DN_METRIC_IOTDB_REPORTER_HOST;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.DN_METRIC_PROMETHEUS_REPORTER_PORT;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.DN_MPP_DATA_EXCHANGE_PORT;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.DN_SCHEMA_REGION_CONSENSUS_PORT;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.DN_SYNC_DIR;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.DN_SYSTEM_DIR;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.DN_TRACING_DIR;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.DN_WAL_DIRS;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.MAIN_CLASS_NAME;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.MAX_TSBLOCK_SIZE_IN_BYTES;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.MQTT_HOST;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.MQTT_PORT;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.PAGE_SIZE_IN_BYTE;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.SCHEMA_REPLICATION_FACTOR;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.SYSTEM_PROPERTIES_FILE;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.TARGET;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.USER_DIR;

public class DataNodeWrapper extends AbstractNodeWrapper {
  private int mppDataExchangePort;
  private int internalPort;
  private final String internalAddress;
  private final int dataRegionConsensusPort;
  private final int schemaRegionConsensusPort;
  private final int mqttPort;

  private final String defaultNodePropertiesFile;

  private final String defaultCommonPropertiesFile;

  public DataNodeWrapper(
      String targetConfigNode,
      String testClassName,
      String testMethodName,
      int[] portList,
      int clusterIndex,
      boolean isMultiCluster,
      long startTime) {
    super(testClassName, testMethodName, portList, clusterIndex, isMultiCluster, startTime);
    this.internalAddress = super.getIp();
    this.mppDataExchangePort = portList[1];
    this.internalPort = portList[2];
    this.dataRegionConsensusPort = portList[3];
    this.schemaRegionConsensusPort = portList[4];
    this.mqttPort = portList[5];
    this.defaultNodePropertiesFile =
        EnvUtils.getFilePathFromSysVar(DEFAULT_DATA_NODE_PROPERTIES, clusterIndex);
    this.defaultCommonPropertiesFile =
        EnvUtils.getFilePathFromSysVar(DEFAULT_DATA_NODE_COMMON_PROPERTIES, clusterIndex);
    // Initialize mutable properties
    reloadMutableFields();

    // Initialize immutable properties
    // Override mqtt properties of super class
    immutableCommonProperties.setProperty(MQTT_HOST, super.getIp());
    immutableCommonProperties.setProperty(MQTT_PORT, String.valueOf(this.mqttPort));

    immutableNodeProperties.setProperty(IoTDBConstant.DN_TARGET_CONFIG_NODE_LIST, targetConfigNode);
    immutableNodeProperties.setProperty(DN_SYSTEM_DIR, MppBaseConfig.NULL_VALUE);
    immutableNodeProperties.setProperty(DN_DATA_DIRS, MppBaseConfig.NULL_VALUE);
    immutableNodeProperties.setProperty(DN_CONSENSUS_DIR, MppBaseConfig.NULL_VALUE);
    immutableNodeProperties.setProperty(DN_WAL_DIRS, MppBaseConfig.NULL_VALUE);
    immutableNodeProperties.setProperty(DN_TRACING_DIR, MppBaseConfig.NULL_VALUE);
    immutableNodeProperties.setProperty(DN_SYNC_DIR, MppBaseConfig.NULL_VALUE);
    immutableNodeProperties.setProperty(DN_METRIC_IOTDB_REPORTER_HOST, MppBaseConfig.NULL_VALUE);
    immutableNodeProperties.setProperty(
        DN_METRIC_PROMETHEUS_REPORTER_PORT, MppBaseConfig.NULL_VALUE);
  }

  @Override
  protected String getTargetNodeConfigPath() {
    return workDirFilePath("conf", DATA_NODE_PROPERTIES_FILE);
  }

  @Override
  protected String getTargetCommonConfigPath() {
    return workDirFilePath("conf", COMMON_PROPERTIES_FILE);
  }

  @Override
  protected String getDefaultNodeConfigPath() {
    return defaultNodePropertiesFile;
  }

  @Override
  protected String getDefaultCommonConfigPath() {
    return defaultCommonPropertiesFile;
  }

  @Override
  public String getSystemPropertiesPath() {
    return workDirFilePath("data/datanode/system/schema", SYSTEM_PROPERTIES_FILE);
  }

  @Override
  protected MppJVMConfig initVMConfig() {
    return MppJVMConfig.builder()
        .setInitHeapSize(EnvUtils.getIntFromSysVar(DATANODE_INIT_HEAP_SIZE, 256, clusterIndex))
        .setMaxHeapSize(EnvUtils.getIntFromSysVar(DATANODE_MAX_HEAP_SIZE, 256, clusterIndex))
        .setMaxDirectMemorySize(
            EnvUtils.getIntFromSysVar(DATANODE_MAX_DIRECT_MEMORY_SIZE, 256, clusterIndex))
        .build();
  }

  @Override
  public final String getId() {
    return DATA_NODE_NAME + getPort();
  }

  @Override
  protected void addStartCmdParams(List<String> params) {
    final String workDir = getNodePath();
    final String confDir = workDir + File.separator + "conf";
    params.addAll(
        Arrays.asList(
            "-Dlogback.configurationFile=" + confDir + File.separator + "logback-datanode.xml",
            "-DIOTDB_HOME=" + workDir,
            "-DTSFILE_HOME=" + workDir,
            "-DIOTDB_CONF=" + confDir,
            "-DTSFILE_CONF=" + confDir,
            MAIN_CLASS_NAME,
            "-s"));
  }

  @Override
  protected void reloadMutableFields() {
    mutableCommonProperties.setProperty(CONFIG_NODE_CONSENSUS_PROTOCOL_CLASS, SIMPLE_CONSENSUS);
    mutableCommonProperties.setProperty(SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS, SIMPLE_CONSENSUS);
    mutableCommonProperties.setProperty(DATA_REGION_CONSENSUS_PROTOCOL_CLASS, SIMPLE_CONSENSUS);

    mutableCommonProperties.setProperty(SCHEMA_REPLICATION_FACTOR, "1");
    mutableCommonProperties.setProperty(DATA_REPLICATION_FACTOR, "1");

    mutableCommonProperties.put(MAX_TSBLOCK_SIZE_IN_BYTES, "1024");
    mutableCommonProperties.put(PAGE_SIZE_IN_BYTE, "1024");

    mutableNodeProperties.put(DN_JOIN_CLUSTER_RETRY_INTERVAL_MS, "1000");
    mutableNodeProperties.put(DN_CONNECTION_TIMEOUT_MS, "30000");
    mutableNodeProperties.put(DN_METRIC_INTERNAL_REPORTER_TYPE, "MEMORY");

    mutableNodeProperties.setProperty(IoTDBConstant.DN_RPC_ADDRESS, super.getIp());
    mutableNodeProperties.setProperty(IoTDBConstant.DN_RPC_PORT, String.valueOf(super.getPort()));
    mutableNodeProperties.setProperty(IoTDBConstant.DN_INTERNAL_ADDRESS, this.internalAddress);
    mutableNodeProperties.setProperty(
        IoTDBConstant.DN_INTERNAL_PORT, String.valueOf(this.internalPort));
    mutableNodeProperties.setProperty(
        DN_MPP_DATA_EXCHANGE_PORT, String.valueOf(this.mppDataExchangePort));
    mutableNodeProperties.setProperty(
        DN_DATA_REGION_CONSENSUS_PORT, String.valueOf(this.dataRegionConsensusPort));
    mutableNodeProperties.setProperty(
        DN_SCHEMA_REGION_CONSENSUS_PORT, String.valueOf(this.schemaRegionConsensusPort));
  }

  @Override
  public void renameFile() {
    // Rename log file
    String oldLogFilePath =
        getLogDirPath() + File.separator + DATA_NODE_NAME + portList[0] + ".log";
    String newLogFilePath = getLogDirPath() + File.separator + getId() + ".log";
    File oldLogFile = new File(oldLogFilePath);
    oldLogFile.renameTo(new File(newLogFilePath));

    // Rename node dir
    String oldNodeDirPath =
        System.getProperty(USER_DIR)
            + File.separator
            + TARGET
            + File.separator
            + DATA_NODE_NAME
            + portList[0];
    String newNodeDirPath = getNodePath();
    File oldNodeDir = new File(oldNodeDirPath);
    oldNodeDir.renameTo(new File(newNodeDirPath));
  }

  public int getMppDataExchangePort() {
    return mppDataExchangePort;
  }

  public void setMppDataExchangePort(int mppDataExchangePort) {
    this.mppDataExchangePort = mppDataExchangePort;
  }

  public String getInternalAddress() {
    return internalAddress;
  }

  public int getInternalPort() {
    return internalPort;
  }

  public void setInternalPort(int internalPort) {
    this.internalPort = internalPort;
  }

  public int getDataRegionConsensusPort() {
    return dataRegionConsensusPort;
  }

  public int getSchemaRegionConsensusPort() {
    return schemaRegionConsensusPort;
  }

  public int getMqttPort() {
    return mqttPort;
  }
}
