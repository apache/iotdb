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

package org.apache.iotdb.db.protocol.client;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.exception.BadNodeUrlException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.utils.FileUtils;
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
  private final File propertiesFileTmp;

  private ConfigNodeInfo() {
    this.configNodeInfoReadWriteLock = new ReentrantReadWriteLock();
    this.onlineConfigNodes = new HashSet<>();
    propertiesFile =
        SystemFileFactory.INSTANCE.getFile(
            IoTDBDescriptor.getInstance().getConfig().getSystemDir()
                + File.separator
                + PROPERTIES_FILE_NAME);
    propertiesFileTmp =
        SystemFileFactory.INSTANCE.getFile(propertiesFile.getAbsolutePath() + ".tmp");
  }
  // TODO: This needs removal of statics ...
  public static void reinitializeStatics() {
    ConfigNodeInfoHolder.INSTANCE = new ConfigNodeInfo();
  }

  /** Update ConfigNodeList both in memory and system.properties file */
  public boolean updateConfigNodeList(List<TEndPoint> latestConfigNodes) {
    long startTime = System.currentTimeMillis();
    // Check whether the config nodes are latest or not
    configNodeInfoReadWriteLock.readLock().lock();
    try {
      if (onlineConfigNodes.size() == latestConfigNodes.size()
          && onlineConfigNodes.containsAll(latestConfigNodes)) {
        return true;
      }
    } finally {
      configNodeInfoReadWriteLock.readLock().unlock();
    }

    // Update config nodes
    configNodeInfoReadWriteLock.writeLock().lock();
    try {
      onlineConfigNodes.clear();
      onlineConfigNodes.addAll(latestConfigNodes);
      storeConfigNode();
      long endTime = System.currentTimeMillis();
      logger.info(
          "Update ConfigNode Successfully: {}, which takes {} ms.",
          onlineConfigNodes,
          (endTime - startTime));
    } catch (IOException e) {
      logger.error("Update ConfigNode failed.", e);
      return false;
    } finally {
      configNodeInfoReadWriteLock.writeLock().unlock();
    }
    return true;
  }

  /**
   * Call this method to store config node list.
   *
   * @throws IOException if properties deserialization or configNode list serialization failed.
   */
  private void storeConfigNode() throws IOException {
    Properties properties = new Properties();
    try (FileInputStream inputStream = new FileInputStream(propertiesFile)) {
      properties.load(inputStream);
    }
    properties.setProperty(
        CONFIG_NODE_LIST, NodeUrlUtils.convertTEndPointUrls(new ArrayList<>(onlineConfigNodes)));
    try (FileOutputStream fileOutputStream = new FileOutputStream(propertiesFileTmp)) {
      properties.store(fileOutputStream, "");
    }
    if (!propertiesFile.delete()) {
      String msg =
          String.format(
              "Update %s file fail: %s", PROPERTIES_FILE_NAME, propertiesFile.getAbsoluteFile());
      throw new IOException(msg);
    }
    FileUtils.moveFileSafe(propertiesFileTmp, propertiesFile);
  }

  public void loadConfigNodeList() {
    long startTime = System.currentTimeMillis();
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
      long endTime = System.currentTimeMillis();
      logger.info(
          "Load ConfigNode successfully: {}, which takes {} ms.",
          onlineConfigNodes,
          (endTime - startTime));
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
    private static ConfigNodeInfo INSTANCE = new ConfigNodeInfo();

    private ConfigNodeInfoHolder() {
      // Empty constructor
    }
  }

  public static ConfigNodeInfo getInstance() {
    return ConfigNodeInfoHolder.INSTANCE;
  }
}
