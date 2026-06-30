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

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.exception.BadNodeUrlException;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.file.SystemPropertiesHandler;
import org.apache.iotdb.commons.utils.NodeUrlUtils;
import org.apache.iotdb.db.conf.DataNodeSystemPropertiesHandler;
import org.apache.iotdb.db.i18n.DataNodeMiscMessages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ConfigNodeInfo {
  private static final Logger logger = LoggerFactory.getLogger(ConfigNodeInfo.class);

  private static final String CONFIG_NODE_LIST = "config_node_list";

  private final ReentrantReadWriteLock configNodeInfoReadWriteLock;

  /** latest config nodes. */
  private final Set<TEndPoint> onlineConfigNodes;

  private final Map<TEndPoint, Integer> configNodeIdMap;

  public static final ConfigRegionId CONFIG_REGION_ID = new ConfigRegionId(0);

  SystemPropertiesHandler systemPropertiesHandler = DataNodeSystemPropertiesHandler.getInstance();

  private ConfigNodeInfo() {
    this.configNodeInfoReadWriteLock = new ReentrantReadWriteLock();
    this.onlineConfigNodes = new HashSet<>();
    this.configNodeIdMap = new HashMap<>();
  }

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
      configNodeIdMap.keySet().retainAll(onlineConfigNodes);
      storeConfigNodeList();
      long endTime = System.currentTimeMillis();
      logger.info(
          DataNodeMiscMessages.UPDATE_CONFIG_NODE_SUCCESSFULLY,
          onlineConfigNodes,
          (endTime - startTime));
    } catch (IOException e) {
      logger.error(DataNodeMiscMessages.UPDATE_CONFIG_NODE_FAILED, e);
      return false;
    } finally {
      configNodeInfoReadWriteLock.writeLock().unlock();
    }
    return true;
  }

  public boolean updateConfigNodeLocations(List<TConfigNodeLocation> latestConfigNodeLocations) {
    if (latestConfigNodeLocations == null) {
      return false;
    }
    final List<TEndPoint> latestConfigNodes = new ArrayList<>();
    final Map<TEndPoint, Integer> latestConfigNodeIdMap = new HashMap<>();
    for (TConfigNodeLocation configNodeLocation : latestConfigNodeLocations) {
      if (configNodeLocation == null || configNodeLocation.getInternalEndPoint() == null) {
        continue;
      }
      final TEndPoint internalEndPoint = configNodeLocation.getInternalEndPoint();
      latestConfigNodes.add(internalEndPoint);
      latestConfigNodeIdMap.put(internalEndPoint, configNodeLocation.getConfigNodeId());
    }

    if (!updateConfigNodeList(latestConfigNodes)) {
      return false;
    }

    configNodeInfoReadWriteLock.writeLock().lock();
    try {
      configNodeIdMap.putAll(latestConfigNodeIdMap);
      configNodeIdMap.keySet().retainAll(onlineConfigNodes);
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
  public void storeConfigNodeList() throws IOException {
    if (!systemPropertiesHandler.fileExist()) {
      logger.info(DataNodeMiscMessages.SYSTEM_PROPERTIES_NOT_EXIST);
      return;
    }
    systemPropertiesHandler.put(
        CONFIG_NODE_LIST, NodeUrlUtils.convertTEndPointUrls(new ArrayList<>(onlineConfigNodes)));
  }

  public void loadConfigNodeList() throws StartupException {
    long startTime = System.currentTimeMillis();
    // properties contain CONFIG_NODE_LIST only when start as Data node
    configNodeInfoReadWriteLock.writeLock().lock();
    try {
      Properties properties = systemPropertiesHandler.read();

      if (properties.containsKey(CONFIG_NODE_LIST)) {
        onlineConfigNodes.clear();
        onlineConfigNodes.addAll(
            NodeUrlUtils.parseTEndPointUrls(properties.getProperty(CONFIG_NODE_LIST)));
      }
      if (onlineConfigNodes.isEmpty()) {
        throw new StartupException(
            "Removing is only allowed in an environment where the datanode has been successfully started. "
                + "Please check whether it is removed on the confignode, or if you have deleted the system.properties file by mistake.");
      }
      long endTime = System.currentTimeMillis();
      logger.info(
          DataNodeMiscMessages.LOAD_CONFIG_NODE_SUCCESSFULLY,
          onlineConfigNodes,
          (endTime - startTime));
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (BadNodeUrlException e) {
      logger.error(DataNodeMiscMessages.CANNOT_PARSE_CONFIG_NODE_LIST);
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

  public int getConfigNodeId(TEndPoint internalEndPoint) {
    if (internalEndPoint == null) {
      return -1;
    }
    configNodeInfoReadWriteLock.readLock().lock();
    try {
      return configNodeIdMap.getOrDefault(internalEndPoint, -1);
    } finally {
      configNodeInfoReadWriteLock.readLock().unlock();
    }
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
