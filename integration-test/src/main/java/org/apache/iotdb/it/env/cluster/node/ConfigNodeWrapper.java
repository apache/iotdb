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
import static org.apache.iotdb.it.env.cluster.ClusterConstant.CN_CONNECTION_TIMEOUT_MS;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.CN_CONSENSUS_DIR;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.CN_METRIC_IOTDB_REPORTER_HOST;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.CN_METRIC_IOTDB_REPORTER_PORT;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.CN_METRIC_PROMETHEUS_REPORTER_PORT;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.CN_SYSTEM_DIR;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.COMMON_PROPERTIES_FILE;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.CONFIG_NODE_CONSENSUS_PROTOCOL_CLASS;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.CONFIG_NODE_INIT_HEAP_SIZE;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.CONFIG_NODE_MAX_DIRECT_MEMORY_SIZE;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.CONFIG_NODE_MAX_HEAP_SIZE;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.CONFIG_NODE_PROPERTIES_FILE;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.CONFIG_NODE_SYSTEM_PROPERTIES_FILE;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.DATA_REGION_CONSENSUS_PROTOCOL_CLASS;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.DATA_REPLICATION_FACTOR;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.DEFAULT_CONFIG_NODE_COMMON_PROPERTIES;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.DEFAULT_CONFIG_NODE_PROPERTIES;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.SCHEMA_REPLICATION_FACTOR;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.TARGET;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.USER_DIR;

public class ConfigNodeWrapper extends AbstractNodeWrapper {

  private int consensusPort;
  private final boolean isSeed;
  private final String defaultNodePropertiesFile;
  private final String defaultCommonPropertiesFile;

  public ConfigNodeWrapper(
      boolean isSeed,
      String targetCNs,
      String testClassName,
      String testMethodName,
      int[] portList,
      int clusterIndex,
      boolean isMultiCluster,
      long startTime) {
    super(testClassName, testMethodName, portList, clusterIndex, isMultiCluster, startTime);
    this.consensusPort = portList[1];
    this.isSeed = isSeed;
    String targetConfigNodes;
    if (isSeed) {
      targetConfigNodes = getIpAndPortString();
    } else {
      targetConfigNodes = targetCNs;
    }
    this.defaultNodePropertiesFile =
        EnvUtils.getFilePathFromSysVar(DEFAULT_CONFIG_NODE_PROPERTIES, clusterIndex);
    this.defaultCommonPropertiesFile =
        EnvUtils.getFilePathFromSysVar(DEFAULT_CONFIG_NODE_COMMON_PROPERTIES, clusterIndex);

    // initialize mutable properties
    reloadMutableFields();

    // initialize immutable properties
    immutableNodeProperties.setProperty(
        IoTDBConstant.CN_TARGET_CONFIG_NODE_LIST, targetConfigNodes);
    immutableNodeProperties.setProperty(CN_SYSTEM_DIR, MppBaseConfig.NULL_VALUE);
    immutableNodeProperties.setProperty(CN_CONSENSUS_DIR, MppBaseConfig.NULL_VALUE);
    immutableNodeProperties.setProperty(
        CN_METRIC_PROMETHEUS_REPORTER_PORT, MppBaseConfig.NULL_VALUE);
    immutableNodeProperties.setProperty(CN_METRIC_IOTDB_REPORTER_HOST, MppBaseConfig.NULL_VALUE);
    immutableNodeProperties.setProperty(CN_METRIC_IOTDB_REPORTER_PORT, MppBaseConfig.NULL_VALUE);
  }

  @Override
  protected String getTargetNodeConfigPath() {
    return workDirFilePath("conf", CONFIG_NODE_PROPERTIES_FILE);
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
    return workDirFilePath("data/confignode/system", CONFIG_NODE_SYSTEM_PROPERTIES_FILE);
  }

  @Override
  protected MppJVMConfig initVMConfig() {
    return MppJVMConfig.builder()
        .setInitHeapSize(EnvUtils.getIntFromSysVar(CONFIG_NODE_INIT_HEAP_SIZE, 256, clusterIndex))
        .setMaxHeapSize(EnvUtils.getIntFromSysVar(CONFIG_NODE_MAX_HEAP_SIZE, 256, clusterIndex))
        .setMaxDirectMemorySize(
            EnvUtils.getIntFromSysVar(CONFIG_NODE_MAX_DIRECT_MEMORY_SIZE, 256, clusterIndex))
        .build();
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
    final String workDir = getNodePath();
    final String confDir = workDir + File.separator + "conf";
    params.addAll(
        Arrays.asList(
            "-Dlogback.configurationFile=" + confDir + File.separator + "logback-confignode.xml",
            "-DCONFIGNODE_HOME=" + workDir,
            "-DCONFIGNODE_CONF=" + confDir,
            "-DIOTDB_HOME=" + workDir,
            "-DIOTDB_CONF=" + confDir,
            "-DTSFILE_CONF=" + confDir,
            "org.apache.iotdb.confignode.service.ConfigNode",
            "-s"));
  }

  @Override
  protected void reloadMutableFields() {
    mutableCommonProperties.setProperty(CONFIG_NODE_CONSENSUS_PROTOCOL_CLASS, SIMPLE_CONSENSUS);
    mutableCommonProperties.setProperty(SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS, SIMPLE_CONSENSUS);
    mutableCommonProperties.setProperty(DATA_REGION_CONSENSUS_PROTOCOL_CLASS, SIMPLE_CONSENSUS);

    mutableCommonProperties.setProperty(SCHEMA_REPLICATION_FACTOR, "1");
    mutableCommonProperties.setProperty(DATA_REPLICATION_FACTOR, "1");

    mutableNodeProperties.put(CN_CONNECTION_TIMEOUT_MS, "30000");

    mutableNodeProperties.setProperty(IoTDBConstant.CN_INTERNAL_ADDRESS, super.getIp());
    mutableNodeProperties.setProperty(IoTDBConstant.CN_INTERNAL_PORT, String.valueOf(getPort()));
    mutableNodeProperties.setProperty(
        IoTDBConstant.CN_CONSENSUS_PORT, String.valueOf(this.consensusPort));
  }

  @Override
  protected void renameFile() {
    String configNodeName = isSeed ? "SeedConfigNode" : "ConfigNode";
    // rename log file
    File oldLogFile =
        new File(getLogDirPath() + File.separator + configNodeName + portList[0] + ".log");
    oldLogFile.renameTo(new File(getLogDirPath() + File.separator + getId() + ".log"));

    // rename node dir
    File oldNodeDir =
        new File(
            System.getProperty(USER_DIR)
                + File.separator
                + TARGET
                + File.separator
                + configNodeName
                + portList[0]);
    oldNodeDir.renameTo(new File(getNodePath()));
  }

  public void setConsensusPort(int consensusPort) {
    this.consensusPort = consensusPort;
  }

  public int getConsensusPort() {
    return consensusPort;
  }
}
