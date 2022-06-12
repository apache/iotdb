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

public class ConfigNode extends ClusterNodeBase {

  private final int consensusPort;
  private final String configPath;
  private final String targetConfignode;

  public ConfigNode(boolean isSeed, String targetConfignode, String testName) {

    int[] portList = super.searchAvailablePorts();
    super.setPort(portList[0]);
    this.consensusPort = portList[1];

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
            + "Config"
            + super.getId()
            + ".log";
    super.setLogPath(logPath);

    String scriptPath =
        super.getNodePath()
            + File.separator
            + "template-node"
            + File.separator
            + "confignode"
            + File.separator
            + "sbin"
            + File.separator
            + "start-confignode"
            + ".sh";
    super.setScriptPath(scriptPath);

    this.configPath =
        super.getNodePath()
            + File.separator
            + "template-node"
            + File.separator
            + "confignode"
            + File.separator
            + "conf"
            + File.separator
            + "iotdb-confignode.properties";

    if (isSeed) {
      this.targetConfignode = getIpAndPortString();
    } else {
      this.targetConfignode = targetConfignode;
    }
  }

  @Override
  public void changeConfig(Properties properties) {
    try {
      Properties configProperties = new Properties();
      configProperties.load(new FileInputStream(this.configPath));
      configProperties.setProperty("rpc_address", super.getIp());
      configProperties.setProperty("rpc_port", String.valueOf(super.getPort()));
      configProperties.setProperty("consensus_port", String.valueOf(this.consensusPort));
      configProperties.setProperty("target_confignode", this.targetConfignode);
      configProperties.setProperty(
          "schema_region_consensus_protocol_class",
          "org.apache.iotdb.consensus.ratis.RatisConsensus");
      configProperties.setProperty(
          "data_region_consensus_protocol_class",
          "org.apache.iotdb.consensus.ratis.RatisConsensus");
      configProperties.setProperty("schema_replication_factor", "2");
      configProperties.setProperty("data_replication_factor", "2");
      if (properties != null && !properties.isEmpty()) {
        configProperties.putAll(properties);
      }
      configProperties.store(new FileWriter(this.configPath), null);
    } catch (IOException ex) {
      fail("Change the config of config node failed. " + ex);
    }
  }
}
