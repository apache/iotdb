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
import org.apache.iotdb.commons.exception.BadNodeUrlException;
import org.apache.iotdb.commons.utils.NodeUrlUtils;
import org.apache.iotdb.confignode.client.SyncConfigNodeClientPool;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class ConfigNodeRemoveCheck {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigNodeStartupCheck.class);

  private static final ConfigNodeConf conf = ConfigNodeDescriptor.getInstance().getConf();

  private final File systemPropertiesFile;
  private final Properties systemProperties;

  public ConfigNodeRemoveCheck() {
    systemPropertiesFile =
        new File(conf.getSystemDir() + File.separator + ConfigNodeConstant.SYSTEM_FILE_NAME);
    systemProperties = new Properties();
  }

  public boolean removeCheck(TEndPoint endPoint) throws IOException, BadNodeUrlException {
    if (isContainRemoveNode(endPoint)) {
      removeConfigNode(endPoint);
      return true;
    }
    return false;
  }

  private boolean isContainRemoveNode(TEndPoint endPoint) {
    if (!systemPropertiesFile.exists()) {
      LOGGER.error("The system properties file is not exists. IoTDB-ConfigNode is shutdown.");
      return false;
    }
    try (FileInputStream inputStream = new FileInputStream(systemPropertiesFile)) {
      systemProperties.load(inputStream);
      List<TConfigNodeLocation> configNodeLocations =
          NodeUrlUtils.parseTConfigNodeUrls(systemProperties.getProperty("confignode_list"));
      for (TConfigNodeLocation nodeLocation : configNodeLocations) {
        if (nodeLocation.getInternalEndPoint().equals(endPoint)) {
          return true;
        }
      }
    } catch (IOException | BadNodeUrlException e) {
      LOGGER.error("Load system properties file failed.", e);
    }

    return false;
  }

  private void removeConfigNode(TEndPoint endPoint) throws IOException, BadNodeUrlException {
    LOGGER.info(
        "removeConfigNode confignode_list:{}, consensus_port: {}",
        systemProperties.getProperty("confignode_list"),
        Integer.parseInt(systemProperties.getProperty("consensus_port")));
    TSStatus status =
        SyncConfigNodeClientPool.getInstance()
            .removeConfigNode(
                NodeUrlUtils.parseTConfigNodeUrls(systemProperties.getProperty("confignode_list")),
                new TConfigNodeLocation(
                    endPoint,
                    new TEndPoint(
                        endPoint.getIp(),
                        Integer.parseInt(systemProperties.getProperty("consensus_port")))));
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.error(status.getMessage());
      throw new IOException("Apply ConfigNode failed:");
    }
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
