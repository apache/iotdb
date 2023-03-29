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

package org.apache.iotdb.db.client;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.exception.BadNodeUrlException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.utils.NodeUrlUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ConfigNodeInfo {

  private static final Logger logger = LoggerFactory.getLogger(ConfigNodeInfo.class);

  private static final String CONFIG_NODE_LIST = "config_node_list";

  private static final String PROPERTIES_FILE_NAME = "system.properties";

  private final ReentrantReadWriteLock configNodeInfoReadWriteLock;

  /** latest config nodes. */
  private final Set<TEndPoint> onlineConfigNodes;

  public static final ConfigRegionId CONFIG_REGION_ID = new ConfigRegionId(0);

  private final File propertiesFile;

  private ConfigNodeInfo() {
    this.configNodeInfoReadWriteLock = new ReentrantReadWriteLock();
    this.onlineConfigNodes = new HashSet<>();
    propertiesFile =
        SystemFileFactory.INSTANCE.getFile(
            IoTDBDescriptor.getInstance().getConfig().getSchemaDir()
                + File.separator
                + PROPERTIES_FILE_NAME);
  }

  /** Update ConfigNodeList both in memory and system.properties file */
  public void updateConfigNodeList(List<TEndPoint> latestConfigNodes) {
    // check whether the config nodes are latest or not
    configNodeInfoReadWriteLock.readLock().lock();
    try {
      if (onlineConfigNodes.containsAll(latestConfigNodes)
          && new HashSet<>(latestConfigNodes).containsAll(onlineConfigNodes)) {
        return;
      }
    } finally {
      configNodeInfoReadWriteLock.readLock().unlock();
    }

    // update config nodes
    configNodeInfoReadWriteLock.writeLock().lock();
    try {
      onlineConfigNodes.clear();
      onlineConfigNodes.addAll(latestConfigNodes);
      storeConfigNode();

      logger.info("Successfully update ConfigNode: {}.", onlineConfigNodes);
    } catch (IOException e) {
      logger.error("Update ConfigNode failed.", e);
    } finally {
      configNodeInfoReadWriteLock.writeLock().unlock();
    }
  }

  /** call this method to store config node list */
  private void storeConfigNode() throws IOException {
    Properties properties = new Properties();
    try (FileInputStream inputStream = new FileInputStream(propertiesFile)) {
      properties.load(inputStream);
    }
    properties.setProperty(
        CONFIG_NODE_LIST, NodeUrlUtils.convertTEndPointUrls(new ArrayList<>(onlineConfigNodes)));
    try (FileOutputStream fileOutputStream = new FileOutputStream(propertiesFile)) {
      properties.store(fileOutputStream, "");
    }
  }

  public void loadConfigNodeList() {
    // properties contain CONFIG_NODE_LIST only when start as Data node
    try {
      configNodeInfoReadWriteLock.writeLock().lock();
      Properties properties = new Properties();
      try (FileInputStream inputStream = new FileInputStream(propertiesFile)) {
        properties.load(inputStream);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      if (properties.containsKey(CONFIG_NODE_LIST)) {
        onlineConfigNodes.clear();
        onlineConfigNodes.addAll(
            NodeUrlUtils.parseTEndPointUrls(properties.getProperty(CONFIG_NODE_LIST)));
      }
    } catch (BadNodeUrlException e) {
      logger.error("Cannot parse config node list in system.properties");
    } finally {
      configNodeInfoReadWriteLock.writeLock().unlock();
    }
  }

  public List<TEndPoint> getLatestConfigNodes() {
    List<TEndPoint> result;
    configNodeInfoReadWriteLock.readLock().lock();
    try {
      result = new ArrayList<>(onlineConfigNodes);
    } finally {
      configNodeInfoReadWriteLock.readLock().unlock();
    }
    return result;
  }

  private static class ConfigNodeInfoHolder {
    private static final ConfigNodeInfo INSTANCE = new ConfigNodeInfo();

    private ConfigNodeInfoHolder() {
      // empty constructor
    }
  }

  public static ConfigNodeInfo getInstance() {
    return ConfigNodeInfoHolder.INSTANCE;
  }
}
