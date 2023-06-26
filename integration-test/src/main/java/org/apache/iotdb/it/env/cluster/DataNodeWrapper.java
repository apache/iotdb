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

package org.apache.iotdb.it.env.cluster;

import org.apache.iotdb.commons.conf.IoTDBConstant;

import java.io.File;
import java.util.Arrays;
import java.util.List;

public class DataNodeWrapper extends AbstractNodeWrapper {
  private int mppDataExchangePort;
  private int internalPort;
  private final String internalAddress;
  private final int dataRegionConsensusPort;
  private final int schemaRegionConsensusPort;
  private final int mqttPort;

  private final String defaultNodePropertiesFile =
      EnvUtils.getFilePathFromSysVar("DefaultDataNodeProperties");
  private final String defaultCommonPropertiesFile =
      EnvUtils.getFilePathFromSysVar("DefaultDataNodeCommonProperties");

  public DataNodeWrapper(
      String targetConfigNode, String testClassName, String testMethodName, int[] portList) {
    super(testClassName, testMethodName, portList);
    this.internalAddress = super.getIp();
    this.mppDataExchangePort = portList[1];
    this.internalPort = portList[2];
    this.dataRegionConsensusPort = portList[3];
    this.schemaRegionConsensusPort = portList[4];
    this.mqttPort = portList[5];

    // initialize mutable properties
    reloadMutableFields();

    // initialize immutable properties
    // Override mqtt properties of super class
    immutableCommonProperties.setProperty("mqtt_host", super.getIp());
    immutableCommonProperties.setProperty("mqtt_port", String.valueOf(this.mqttPort));

    immutableNodeProperties.setProperty(IoTDBConstant.DN_TARGET_CONFIG_NODE_LIST, targetConfigNode);
    immutableNodeProperties.setProperty("dn_system_dir", MppBaseConfig.NULL_VALUE);
    immutableNodeProperties.setProperty("dn_data_dirs", MppBaseConfig.NULL_VALUE);
    immutableNodeProperties.setProperty("dn_consensus_dir", MppBaseConfig.NULL_VALUE);
    immutableNodeProperties.setProperty("dn_wal_dirs", MppBaseConfig.NULL_VALUE);
    immutableNodeProperties.setProperty("dn_tracing_dir", MppBaseConfig.NULL_VALUE);
    immutableNodeProperties.setProperty("dn_sync_dir", MppBaseConfig.NULL_VALUE);
    immutableNodeProperties.setProperty("dn_metric_iotdb_reporter_host", MppBaseConfig.NULL_VALUE);
    immutableNodeProperties.setProperty(
        "dn_metric_prometheus_reporter_port", MppBaseConfig.NULL_VALUE);
  }

  @Override
  protected String getTargetNodeConfigPath() {
    return workDirFilePath("conf", "iotdb-datanode.properties");
  }

  @Override
  protected String getTargetCommonConfigPath() {
    return workDirFilePath("conf", "iotdb-common.properties");
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
    return workDirFilePath("data/datanode/system/schema", "system.properties");
  }

  @Override
  protected MppJVMConfig initVMConfig() {
    return MppJVMConfig.builder()
        .setInitHeapSize(EnvUtils.getIntFromSysVar("DataNodeInitHeapSize", 256))
        .setMaxHeapSize(EnvUtils.getIntFromSysVar("DataNodeMaxHeapSize", 256))
        .setMaxDirectMemorySize(EnvUtils.getIntFromSysVar("DataNodeMaxDirectMemorySize", 256))
        .build();
  }

  @Override
  public final String getId() {
    return "DataNode" + getPort();
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
            mainClassName(),
            "-s"));
  }

  @Override
  protected void reloadMutableFields() {
    mutableCommonProperties.setProperty(
        propertyKeyConfigNodeConsensusProtocolClass,
        "org.apache.iotdb.consensus.simple.SimpleConsensus");
    mutableCommonProperties.setProperty(
        propertyKeySchemaRegionConsensusProtocolClass,
        "org.apache.iotdb.consensus.simple.SimpleConsensus");
    mutableCommonProperties.setProperty(
        propertyKeyDataRegionConsensusProtocolClass,
        "org.apache.iotdb.consensus.simple.SimpleConsensus");

    mutableCommonProperties.setProperty(propertyKeySchemaReplicationFactor, "1");
    mutableCommonProperties.setProperty(propertyKeyDataReplicationFactor, "1");

    mutableCommonProperties.put("max_tsblock_size_in_bytes", "1024");
    mutableCommonProperties.put("page_size_in_byte", "1024");

    mutableNodeProperties.put("dn_join_cluster_retry_interval_ms", "1000");
    mutableNodeProperties.put("dn_connection_timeout_ms", "30000");
    mutableNodeProperties.put("dn_metric_internal_reporter_type", "MEMORY");

    mutableNodeProperties.setProperty(IoTDBConstant.DN_RPC_ADDRESS, super.getIp());
    mutableNodeProperties.setProperty(IoTDBConstant.DN_RPC_PORT, String.valueOf(super.getPort()));
    mutableNodeProperties.setProperty(IoTDBConstant.DN_INTERNAL_ADDRESS, this.internalAddress);
    mutableNodeProperties.setProperty(
        IoTDBConstant.DN_INTERNAL_PORT, String.valueOf(this.internalPort));
    mutableNodeProperties.setProperty(
        "dn_mpp_data_exchange_port", String.valueOf(this.mppDataExchangePort));
    mutableNodeProperties.setProperty(
        "dn_data_region_consensus_port", String.valueOf(this.dataRegionConsensusPort));
    mutableNodeProperties.setProperty(
        "dn_schema_region_consensus_port", String.valueOf(this.schemaRegionConsensusPort));
  }

  @Override
  public void renameFile() {
    String dataNodeName = "DataNode";
    // rename log file
    String oldLogFilePath = getLogDirPath() + File.separator + dataNodeName + portList[0] + ".log";
    String newLogFilePath = getLogDirPath() + File.separator + getId() + ".log";
    File oldLogFile = new File(oldLogFilePath);
    oldLogFile.renameTo(new File(newLogFilePath));

    // rename node dir
    String oldNodeDirPath =
        System.getProperty("user.dir")
            + File.separator
            + "target"
            + File.separator
            + dataNodeName
            + portList[0];
    String newNodeDirPath = getNodePath();
    File oldNodeDir = new File(oldNodeDirPath);
    oldNodeDir.renameTo(new File(newNodeDirPath));
  }

  protected String mainClassName() {
    return "org.apache.iotdb.db.service.DataNode";
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
