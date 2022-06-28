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
import org.apache.iotdb.commons.exception.BadNodeUrlException;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.utils.NodeUrlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class SystemPropertiesUtils {

  private static final File systemPropertiesFile =
    new File(
      ConfigNodeDescriptor.getInstance().getConf().getSystemDir()
        + File.separator
        + ConfigNodeConstant.SYSTEM_FILE_NAME);

  /**
   * Store the latest config_node_list in confignode-system.properties file
   *
   * @param configNodes The latest ConfigNodeList
   * @throws IOException When store confignode-system.properties file failed
   */
  public static void storeConfigNodeList(List<TConfigNodeLocation> configNodes) throws IOException {
    Properties systemProperties = getSystemProperties();
    systemProperties.setProperty(
      "config_node_list", NodeUrlUtils.convertTConfigNodeUrls(configNodes));
    try (FileOutputStream fileOutputStream = new FileOutputStream(systemPropertiesFile)) {
      systemProperties.store(fileOutputStream, "");
    }
  }

  /**
   * Load the config_node_list in confignode-system.properties file
   *
   * @return The property of config_node_list in confignode-system.properties file
   * @throws IOException When load confignode-system.properties file failed
   * @throws BadNodeUrlException When parsing config_node_list failed
   */
  private static List<TConfigNodeLocation> loadConfigNodeList() throws IOException, BadNodeUrlException {
    Properties systemProperties = getSystemProperties();
    String addresses = systemProperties.getProperty("config_node_list", null);

    if (addresses != null && !addresses.isEmpty()) {
      return NodeUrlUtils.parseTConfigNodeUrls(addresses);
    } else {
      return new ArrayList<>();
    }
  }



  private static Properties getSystemProperties() throws IOException{
    Properties systemProperties = new Properties();
    try (FileInputStream inputStream = new FileInputStream(systemPropertiesFile)) {
      systemProperties.load(inputStream);
    }
    return systemProperties;
  }
}
