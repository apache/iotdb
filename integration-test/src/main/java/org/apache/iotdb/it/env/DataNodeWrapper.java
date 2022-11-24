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
package org.apache.iotdb.it.env;

import org.apache.iotdb.commons.conf.IoTDBConstant;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class DataNodeWrapper extends AbstractNodeWrapper {

  private final String targetConfigNode;
  private int mppDataExchangePort;
  private int internalPort;
  private final String internalAddress;
  private final int dataRegionConsensusPort;
  private final int schemaRegionConsensusPort;
  private final int mqttPort;

  public DataNodeWrapper(
      String targetConfigNode, String testClassName, String testMethodName, int[] portList) {
    super(testClassName, testMethodName, portList);
    this.targetConfigNode = targetConfigNode;
    this.internalAddress = super.getIp();
    this.mppDataExchangePort = portList[1];
    this.internalPort = portList[2];
    this.dataRegionConsensusPort = portList[3];
    this.schemaRegionConsensusPort = portList[4];
    this.mqttPort = portList[5];
  }

  @Override
  protected void updateConfig(Properties properties) {
    properties.setProperty(IoTDBConstant.DN_RPC_ADDRESS, super.getIp());
    properties.setProperty(IoTDBConstant.DN_RPC_PORT, String.valueOf(super.getPort()));
    properties.setProperty(IoTDBConstant.DN_INTERNAL_ADDRESS, this.internalAddress);
    properties.setProperty(IoTDBConstant.DN_INTERNAL_PORT, String.valueOf(this.internalPort));
    properties.setProperty("dn_mpp_data_exchange_port", String.valueOf(this.mppDataExchangePort));
    properties.setProperty(
        "dn_data_region_consensus_port", String.valueOf(this.dataRegionConsensusPort));
    properties.setProperty(
        "dn_schema_region_consensus_port", String.valueOf(this.schemaRegionConsensusPort));
    properties.setProperty("dn_join_cluster_retry_interval_ms", "1000");
    properties.setProperty("mqtt_host", super.getIp());
    properties.setProperty("mqtt_port", String.valueOf(this.mqttPort));
    properties.setProperty("connection_timeout_ms", "30000");
    if (this.targetConfigNode != null) {
      properties.setProperty(IoTDBConstant.DN_TARGET_CONFIG_NODE_LIST, this.targetConfigNode);
    }
    properties.setProperty("max_tsblock_size_in_bytes", "1024");
    properties.setProperty("page_size_in_byte", "1024");
  }

  @Override
  protected String getConfigPath() {
    return workDirFilePath("conf", "iotdb-datanode.properties");
  }

  @Override
  protected String getCommonConfigPath() {
    return workDirFilePath("conf", "iotdb-common.properties");
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
