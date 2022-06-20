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

public class ConfigNode extends ClusterNodeBase {

  private final int consensusPort;
  private final String targetConfigNode;
  private final int[] portList;

  public ConfigNode(boolean isSeed, String targetConfigNode, String testName) {
    super(testName);

    portList = super.searchAvailablePorts();
    this.consensusPort = portList[1];

    if (isSeed) {
      this.targetConfigNode = getIpAndPortString();
    } else {
      this.targetConfigNode = targetConfigNode;
    }
  }

  @Override
  protected void updateConfig(Properties properties) {
    properties.setProperty("rpc_address", super.getIp());
    properties.setProperty("rpc_port", String.valueOf(getPort()));
    properties.setProperty("consensus_port", String.valueOf(this.consensusPort));
    properties.setProperty("target_confignode", this.targetConfigNode);
    properties.setProperty(
        "config_node_consensus_protocol_class",
        "org.apache.iotdb.consensus.standalone.StandAloneConsensus");
    properties.setProperty(
        "schema_region_consensus_protocol_class",
        "org.apache.iotdb.consensus.standalone.StandAloneConsensus");
    properties.setProperty(
        "data_region_consensus_protocol_class",
        "org.apache.iotdb.consensus.standalone.StandAloneConsensus");
    properties.setProperty("schema_replication_factor", "1");
    properties.setProperty("data_replication_factor", "1");
    properties.setProperty("connection_timeout_ms", "30000");
  }

  @Override
  protected String getConfigPath() {
    return workDirFilePath("confignode" + File.separator + "conf", "iotdb-confignode.properties");
  }

  @Override
  protected String getStartScriptPath() {
    String scriptName = "start-confignode.sh";
    if (SystemUtils.IS_OS_WINDOWS) {
      scriptName = "start-confignode.bat";
    }
    return workDirFilePath("confignode" + File.separator + "sbin", scriptName);
  }

  @Override
  protected String getStopScriptPath() {
    String scriptName = "stop-confignode.sh";
    if (SystemUtils.IS_OS_WINDOWS) {
      scriptName = "stop-confignode.bat";
    }
    return workDirFilePath("confignode" + File.separator + "sbin", scriptName);
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
        + "Config"
        + super.getId()
        + ".log";
  }

  @Override
  protected String getNodePath() {
    return System.getProperty("user.dir")
        + File.separator
        + "target"
        + File.separator
        + super.getId();
  }

  @Override
  public int getPort() {
    return portList[0];
  }
}
