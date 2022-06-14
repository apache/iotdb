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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.fail;

public class DataNode extends ClusterNodeBase {

  private final String targetConfignode;
  private final String configPath;

  private final int dataBlockManagerPort;
  private final int internalPort;
  private final int dataRegionConsensusPort;
  private final int schemaRegionConsensusPort;

  public DataNode(String targetConfignode, String testName) {

    this.targetConfignode = targetConfignode;

    int[] portList = super.searchAvailablePorts();
    super.setPort(portList[0]);
    this.dataBlockManagerPort = portList[1];
    this.internalPort = portList[2];
    this.dataRegionConsensusPort = portList[3];
    this.schemaRegionConsensusPort = portList[4];

    super.setId("node" + this.getPort());

    String nodePath =
        System.getProperty("user.dir") + File.separator + "target" + File.separator + super.getId();
    super.setNodePath(nodePath);

    String logPath =
        System.getProperty("user.dir")
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
    super.setLogPath(logPath);

    String scriptPath =
        super.getNodePath()
            + File.separator
            + "template-node"
            + File.separator
            + "datanode"
            + File.separator
            + "sbin"
            + File.separator
            + "start-datanode"
            + ".sh";
    super.setScriptPath(scriptPath);

    this.configPath =
        super.getNodePath()
            + File.separator
            + "template-node"
            + File.separator
            + "datanode"
            + File.separator
            + "conf"
            + File.separator
            + "iotdb-engine.properties";
  }

  @Override
  public void changeConfig(Properties properties) {
    try {
      Properties configProperties = new Properties();
      configProperties.load(new FileInputStream(this.configPath));
      configProperties.setProperty("rpc_address", super.getIp());
      configProperties.setProperty("internal_ip", "127.0.0.1");
      configProperties.setProperty("rpc_port", String.valueOf(super.getPort()));
      configProperties.setProperty(
          "data_block_manager_port", String.valueOf(this.dataBlockManagerPort));
      configProperties.setProperty("internal_port", String.valueOf(this.internalPort));
      configProperties.setProperty(
          "data_region_consensus_port", String.valueOf(this.dataRegionConsensusPort));
      configProperties.setProperty(
          "schema_region_consensus_port", String.valueOf(this.schemaRegionConsensusPort));
      configProperties.setProperty("config_nodes", this.targetConfignode);
      configProperties.setProperty("connection_timeout_ms", "30000");
      if (properties != null && !properties.isEmpty()) {
        configProperties.putAll(properties);
      }
      configProperties.store(new FileWriter(this.configPath), null);
    } catch (IOException ex) {
      fail("Change the config of data node failed. " + ex);
    }
  }
}
