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
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.BadNodeUrlException;
import org.apache.iotdb.commons.utils.NodeUrlUtils;
import org.apache.iotdb.confignode.client.ConfigNodeRequestType;
import org.apache.iotdb.confignode.client.sync.SyncConfigNodeClientPool;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.isNumeric;

public class ConfigNodeRemoveCheck {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigNodeStartupCheck.class);

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();

  private final File systemPropertiesFile;
  private final Properties systemProperties;

  public ConfigNodeRemoveCheck() {
    systemPropertiesFile =
        new File(CONF.getSystemDir() + File.separator + ConfigNodeConstant.SYSTEM_FILE_NAME);
    systemProperties = new Properties();
  }

  public TConfigNodeLocation removeCheck(String args) {
    TConfigNodeLocation nodeLocation = new TConfigNodeLocation();
    if (!systemPropertiesFile.exists()) {
      LOGGER.error("The system properties file is not exists. IoTDB-ConfigNode is shutdown.");
      return nodeLocation;
    }
    try (FileInputStream inputStream = new FileInputStream(systemPropertiesFile)) {
      systemProperties.load(inputStream);
      if (isNumeric(args)) {
        int id = Integer.parseInt(args);
        nodeLocation =
            getConfigNodeList().stream()
                .filter(e -> e.getConfigNodeId() == id)
                .findFirst()
                .orElse(null);
      } else {
        try {
          TEndPoint endPoint = NodeUrlUtils.parseTEndPointUrl(args);
          nodeLocation =
              getConfigNodeList().stream()
                  .filter(e -> e.getInternalEndPoint().equals(endPoint))
                  .findFirst()
                  .orElse(null);
        } catch (BadNodeUrlException e2) {
          LOGGER.info(
              "Usage: remove-confignode.sh <confignode-id> or remove-confignode.sh <internal_address>:<internal_port>");
          return nodeLocation;
        }
      }
    } catch (IOException | BadNodeUrlException e) {
      LOGGER.error("Load system properties file failed.", e);
    }

    return nodeLocation;
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
              SyncConfigNodeClientPool.getInstance()
                  .sendSyncRequestToConfigNodeWithRetry(
                      configNodeLocation.getInternalEndPoint(),
                      removedNode,
                      ConfigNodeRequestType.REMOVE_CONFIG_NODE);
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

  /** config_node_list of confignode-system.properties */
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
