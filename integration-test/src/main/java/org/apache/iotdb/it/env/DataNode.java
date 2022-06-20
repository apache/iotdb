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

import org.apache.commons.lang3.SystemUtils;

import java.io.File;
import java.util.Properties;

public class DataNode extends ClusterNodeBase {

  private final String targetConfigNode;

  private final int dataBlockManagerPort;
  private final int internalPort;
  private final int dataRegionConsensusPort;
  private final int schemaRegionConsensusPort;
  private final int[] portList;

  public DataNode(String targetConfigNode, String testName) {
    super(testName);
    this.targetConfigNode = targetConfigNode;
    portList = super.searchAvailablePorts();
    this.dataBlockManagerPort = portList[1];
    this.internalPort = portList[2];
    this.dataRegionConsensusPort = portList[3];
    this.schemaRegionConsensusPort = portList[4];
  }

  @Override
  protected void updateConfig(Properties properties) {
    properties.setProperty("rpc_address", super.getIp());
    properties.setProperty("internal_ip", "127.0.0.1");
    properties.setProperty("rpc_port", String.valueOf(getPort()));
    properties.setProperty("data_block_manager_port", String.valueOf(this.dataBlockManagerPort));
    properties.setProperty("internal_port", String.valueOf(this.internalPort));
    properties.setProperty(
        "data_region_consensus_port", String.valueOf(this.dataRegionConsensusPort));
    properties.setProperty(
        "schema_region_consensus_port", String.valueOf(this.schemaRegionConsensusPort));
    properties.setProperty("connection_timeout_ms", "30000");
    properties.setProperty("config_nodes", this.targetConfigNode);
  }

  @Override
  protected String getConfigPath() {
    return workDirFilePath("datanode" + File.separator + "conf", "iotdb-engine.properties");
  }

  @Override
  protected String getStartScriptPath() {
    String scriptName = "start-datanode.sh";
    if (SystemUtils.IS_OS_WINDOWS) {
      scriptName = "start-datanode.bat";
    }
    return workDirFilePath("datanode" + File.separator + "sbin", scriptName);
  }

  @Override
  protected String getStopScriptPath() {
    String scriptName = "stop-datanode.sh";
    if (SystemUtils.IS_OS_WINDOWS) {
      scriptName = "stop-datanode.bat";
    }
    return workDirFilePath("datanode" + File.separator + "sbin", scriptName);
  }

  @Override
  protected String getLogPath() {
    return System.getProperty("user.dir")
        + File.separator
        + "target"
        + File.separator
        + "cluster-logs"
        + File.separator
        + testName
        + File.separator
        + "Data"
        + super.getId()
        + ".log";
  }

  @Override
  public int getPort() {
    return portList[0];
  }
}
