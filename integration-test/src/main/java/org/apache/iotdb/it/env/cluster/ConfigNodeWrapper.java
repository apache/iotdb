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

public class ConfigNodeWrapper extends AbstractNodeWrapper {

  private int consensusPort;
  private final boolean isSeed;
  private final String defaultNodePropertiesFile =
      EnvUtils.getFilePathFromSysVar("DefaultConfigNodeProperties");
  private final String defaultCommonPropertiesFile =
      EnvUtils.getFilePathFromSysVar("DefaultConfigNodeCommonProperties");

  public ConfigNodeWrapper(
      boolean isSeed,
      String targetCNs,
      String testClassName,
      String testMethodName,
      int[] portList) {
    super(testClassName, testMethodName, portList);
    this.consensusPort = portList[1];
    this.isSeed = isSeed;
    String targetConfigNodes;
    if (isSeed) {
      targetConfigNodes = getIpAndPortString();
    } else {
      targetConfigNodes = targetCNs;
    }

    // initialize mutable properties
    reloadMutableFields();

    // initialize immutable properties
    immutableNodeProperties.setProperty(
        IoTDBConstant.CN_TARGET_CONFIG_NODE_LIST, targetConfigNodes);
    immutableNodeProperties.setProperty("cn_system_dir", MppBaseConfig.NULL_VALUE);
    immutableNodeProperties.setProperty("cn_consensus_dir", MppBaseConfig.NULL_VALUE);
    immutableNodeProperties.setProperty(
        "cn_metric_prometheus_reporter_port", MppBaseConfig.NULL_VALUE);
    immutableNodeProperties.setProperty("cn_metric_iotdb_reporter_host", MppBaseConfig.NULL_VALUE);
    immutableNodeProperties.setProperty("cn_metric_iotdb_reporter_port", MppBaseConfig.NULL_VALUE);
  }

  @Override
  protected String getTargetNodeConfigPath() {
    return workDirFilePath("conf", "iotdb-confignode.properties");
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
    return workDirFilePath("data/confignode/system", "confignode-system.properties");
  }

  @Override
  protected MppJVMConfig initVMConfig() {
    return MppJVMConfig.builder()
        .setInitHeapSize(EnvUtils.getIntFromSysVar("ConfigNodeInitHeapSize", 256))
        .setMaxHeapSize(EnvUtils.getIntFromSysVar("ConfigNodeMaxHeapSize", 256))
        .setMaxDirectMemorySize(EnvUtils.getIntFromSysVar("ConfigNodeMaxDirectMemorySize", 256))
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

    mutableNodeProperties.put("cn_connection_timeout_ms", "30000");

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
            System.getProperty("user.dir")
                + File.separator
                + "target"
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
