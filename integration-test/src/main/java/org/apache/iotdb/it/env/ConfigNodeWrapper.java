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

public class ConfigNodeWrapper extends AbstractNodeWrapper {

  private final int consensusPort;
  private final String targetConfigNodes;
  private final boolean isSeed;

  public ConfigNodeWrapper(
      boolean isSeed,
      String targetConfigNodes,
      String testClassName,
      String testMethodName,
      int[] portList) {
    super(testClassName, testMethodName, portList);
    this.consensusPort = portList[1];
    this.isSeed = isSeed;
    if (isSeed) {
      this.targetConfigNodes = getIpAndPortString();
    } else {
      this.targetConfigNodes = targetConfigNodes;
    }
  }

  @Override
  protected void updateConfig(Properties properties) {
    properties.setProperty(IoTDBConstant.INTERNAL_ADDRESS, super.getIp());
    properties.setProperty(IoTDBConstant.INTERNAL_PORT, String.valueOf(getPort()));
    properties.setProperty(IoTDBConstant.CONSENSUS_PORT, String.valueOf(this.consensusPort));
    properties.setProperty(IoTDBConstant.TARGET_CONFIG_NODES, this.targetConfigNodes);
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
  public final String getId() {
    if (isSeed) {
      return "SeedConfigNode" + getPort();
    }
    return "ConfigNode" + getPort();
  }

  @Override
  protected void addStartCmdParams(List<String> params) {
    final String workDir = getNodePath() + File.separator + "confignode";
    final String confDir = workDir + File.separator + "conf";
    params.addAll(
        Arrays.asList(
            "-Dlogback.configurationFile=" + confDir + File.separator + "logback-confignode.xml",
            "-DCONFIGNODE_HOME=" + workDir,
            "-DCONFIGNODE_CONF=" + confDir,
            "org.apache.iotdb.confignode.service.ConfigNode",
            "-s"));
  }

  public int getConsensusPort() {
    return consensusPort;
  }
}
