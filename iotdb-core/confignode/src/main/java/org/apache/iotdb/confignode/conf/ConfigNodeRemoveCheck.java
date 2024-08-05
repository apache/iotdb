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

package org.apache.iotdb.confignode.conf;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.BadNodeUrlException;
import org.apache.iotdb.confignode.client.CnToCnNodeRequestType;
import org.apache.iotdb.confignode.client.sync.SyncConfigNodeClientPool;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.isNumeric;

public class ConfigNodeRemoveCheck {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigNodeRemoveCheck.class);

  private final SyncConfigNodeClientPool configNodeClientPool;
  private final File systemPropertiesFile;
  private final Properties systemProperties;

  public ConfigNodeRemoveCheck() {
    this(ConfigNodeDescriptor.getInstance().getConf(), SyncConfigNodeClientPool.getInstance());
  }

  public ConfigNodeRemoveCheck(
      ConfigNodeConfig conf, SyncConfigNodeClientPool configNodeClientPool) {
    this.configNodeClientPool = configNodeClientPool;
    this.systemPropertiesFile =
        new File(conf.getSystemDir() + File.separator + ConfigNodeConstant.SYSTEM_FILE_NAME);
    this.systemProperties = new Properties();
  }

  public TConfigNodeLocation removeCheck(String args) {
    if (!systemPropertiesFile.exists()) {
      LOGGER.error("The system properties file is not exists. IoTDB-ConfigNode is shutdown.");
      return null;
    }
    try (FileInputStream inputStream = new FileInputStream(systemPropertiesFile)) {
      systemProperties.load(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
      if (isNumeric(args)) {
        int id = Integer.parseInt(args);
        return getConfigNodeList().stream()
            .filter(e -> e.getConfigNodeId() == id)
            .findFirst()
            .orElse(null);
      } else {
        LOGGER.error("Invalid format. Expected a numeric node id, but got: {}", args);
      }
    } catch (IOException | BadNodeUrlException e) {
      LOGGER.error("Load system properties file failed.", e);
    }

    return null;
  }

  public void removeConfigNode(TConfigNodeLocation removedNode)
      throws BadNodeUrlException, IOException {
    TSStatus status = new TSStatus();
    // Using leader ConfigNode id firstly
    List<TConfigNodeLocation> configNodeList =
        getConfigNodeList().stream()
            .sorted(Comparator.comparing(TConfigNodeLocation::getConfigNodeId))
            .collect(Collectors.toList());
    for (TConfigNodeLocation configNodeLocation : configNodeList) {
      status =
          (TSStatus)
              configNodeClientPool.sendSyncRequestToConfigNodeWithRetry(
                  configNodeLocation.getInternalEndPoint(),
                  removedNode,
                  CnToCnNodeRequestType.REMOVE_CONFIG_NODE);
      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        break;
      }

      if (status.getCode() == TSStatusCode.REMOVE_CONFIGNODE_ERROR.getStatusCode()) {
        LOGGER.warn("Execute removeConfigNode failed for: {}", status.getMessage());
        break;
      }
    }

    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.error(status.getMessage());
      throw new IOException("Remove ConfigNode failed: " + status.getMessage());
    }
  }

  /**
   * config_node_list of confignode-system.properties
   *
   * @throws BadNodeUrlException loadConfigNodeList()
   * @throws IOException loadConfigNodeList()
   */
  public List<TConfigNodeLocation> getConfigNodeList() throws BadNodeUrlException, IOException {
    return SystemPropertiesUtils.loadConfigNodeList();
  }

  public int getConsensusPort() {
    return Integer.parseInt(systemProperties.getProperty(IoTDBConstant.CN_CONSENSUS_PORT));
  }

  private static class ConfigNodeConfRemoveCheckHolder {

    private static final ConfigNodeRemoveCheck INSTANCE = new ConfigNodeRemoveCheck();

    private ConfigNodeConfRemoveCheckHolder() {
      // Empty constructor
    }
  }

  public static ConfigNodeRemoveCheck getInstance() {
    return ConfigNodeRemoveCheck.ConfigNodeConfRemoveCheckHolder.INSTANCE;
  }
}
