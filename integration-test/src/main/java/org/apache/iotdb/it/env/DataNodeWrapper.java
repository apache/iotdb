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
  private final int mppDataExchangePort;
  private final int internalPort;
  private final int dataRegionConsensusPort;
  private final int schemaRegionConsensusPort;

  public DataNodeWrapper(
      String targetConfigNode, String testClassName, String testMethodName, int[] portList) {
    super(testClassName, testMethodName, portList);
    this.targetConfigNode = targetConfigNode;
    this.mppDataExchangePort = portList[1];
    this.internalPort = portList[2];
    this.dataRegionConsensusPort = portList[3];
    this.schemaRegionConsensusPort = portList[4];
  }

  @Override
  protected void updateConfig(Properties properties) {
    properties.setProperty(IoTDBConstant.RPC_ADDRESS, super.getIp());
    properties.setProperty(IoTDBConstant.INTERNAL_ADDRESS, super.getIp());
    properties.setProperty(IoTDBConstant.RPC_PORT, String.valueOf(getPort()));
    properties.setProperty("mpp_data_exchange_port", String.valueOf(this.mppDataExchangePort));
    properties.setProperty(IoTDBConstant.INTERNAL_PORT, String.valueOf(this.internalPort));
    properties.setProperty(
        "data_region_consensus_port", String.valueOf(this.dataRegionConsensusPort));
    properties.setProperty(
        "schema_region_consensus_port", String.valueOf(this.schemaRegionConsensusPort));
    properties.setProperty("connection_timeout_ms", "30000");
    if (this.targetConfigNode != null) {
      properties.setProperty(IoTDBConstant.TARGET_CONFIG_NODES, this.targetConfigNode);
    }
    properties.setProperty("max_tsblock_size_in_bytes", "1024");
    properties.setProperty("page_size_in_byte", "1024");
  }

  @Override
  protected String getConfigPath() {
    return workDirFilePath("datanode" + File.separator + "conf", "iotdb-datanode.properties");
  }

  @Override
  public final String getId() {
    return "DataNode" + getPort();
  }

  @Override
  protected void addStartCmdParams(List<String> params) {
    final String workDir = getNodePath() + File.separator + "datanode";
    final String confDir = workDir + File.separator + "conf";
    params.addAll(
        Arrays.asList(
            "-Dlogback.configurationFile=" + confDir + File.separator + "logback.xml",
            "-DIOTDB_HOME=" + workDir,
            "-DTSFILE_HOME=" + workDir,
            "-DIOTDB_CONF=" + confDir,
            "-DTSFILE_CONF=" + confDir,
            mainClassName(),
            "-s"));
  }

  protected String mainClassName() {
    return "org.apache.iotdb.db.service.DataNode";
  }

  public int getMppDataExchangePort() {
    return mppDataExchangePort;
  }

  public int getInternalPort() {
    return internalPort;
  }

  public int getDataRegionConsensusPort() {
    return dataRegionConsensusPort;
  }

  public int getSchemaRegionConsensusPort() {
    return schemaRegionConsensusPort;
  }
}
