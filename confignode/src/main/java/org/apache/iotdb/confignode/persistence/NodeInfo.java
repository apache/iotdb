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
package org.apache.iotdb.confignode.persistence;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.utils.NodeUrlUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.conf.ConfigNodeConstant;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.read.GetDataNodeInfoReq;
import org.apache.iotdb.confignode.consensus.request.write.ApplyConfigNodeReq;
import org.apache.iotdb.confignode.consensus.request.write.RegisterDataNodeReq;
import org.apache.iotdb.confignode.consensus.response.DataNodeLocationsResp;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class NodeInfo {

  private static final Logger LOGGER = LoggerFactory.getLogger(NodeInfo.class);

  private static final File systemPropertiesFile =
      new File(
          ConfigNodeDescriptor.getInstance().getConf().getSystemDir()
              + File.separator
              + ConfigNodeConstant.SYSTEM_FILE_NAME);

  private static final int minimumDataNode =
      Math.max(
          ConfigNodeDescriptor.getInstance().getConf().getSchemaReplicationFactor(),
          ConfigNodeDescriptor.getInstance().getConf().getDataReplicationFactor());

  private final ReentrantReadWriteLock configNodeInfoReadWriteLock;

  // Online ConfigNodes
  private final Set<TConfigNodeLocation> onlineConfigNodes;

  private final ReentrantReadWriteLock dataNodeInfoReadWriteLock;

  // TODO: serialize and deserialize
  private AtomicInteger nextDataNodeId = new AtomicInteger(0);

  // Online DataNodes
  // TODO: serialize and deserialize
  private final ConcurrentNavigableMap<Integer, TDataNodeLocation> onlineDataNodes =
      new ConcurrentSkipListMap();

  // For remove or draining DataNode
  // TODO: implement
  // TODO: serialize and deserialize
  private final Set<TDataNodeLocation> drainingDataNodes = new HashSet<>();

  private NodeInfo() {
    this.dataNodeInfoReadWriteLock = new ReentrantReadWriteLock();
    this.configNodeInfoReadWriteLock = new ReentrantReadWriteLock();
    this.onlineConfigNodes =
        new HashSet<>(ConfigNodeDescriptor.getInstance().getConf().getConfigNodeList());
  }

  public boolean containsValue(TDataNodeLocation info) {
    boolean result = false;
    dataNodeInfoReadWriteLock.readLock().lock();

    try {
      for (Map.Entry<Integer, TDataNodeLocation> entry : onlineDataNodes.entrySet()) {
        info.setDataNodeId(entry.getKey());
        if (entry.getValue().equals(info)) {
          result = true;
          break;
        }
      }
    } finally {
      dataNodeInfoReadWriteLock.readLock().unlock();
    }

    return result;
  }

  public void put(int dataNodeID, TDataNodeLocation info) {
    onlineDataNodes.put(dataNodeID, info);
  }

  /**
   * Persist DataNode info
   *
   * @param registerDataNodeReq RegisterDataNodePlan
   * @return SUCCESS_STATUS
   */
  public TSStatus registerDataNode(RegisterDataNodeReq registerDataNodeReq) {
    TSStatus result;
    TDataNodeLocation info = registerDataNodeReq.getLocation();
    dataNodeInfoReadWriteLock.writeLock().lock();
    try {
      onlineDataNodes.put(info.getDataNodeId(), info);
      if (nextDataNodeId.get() < registerDataNodeReq.getLocation().getDataNodeId()) {
        // In this case, at least one Datanode is registered with the leader node,
        // so the nextDataNodeID of the followers needs to be added
        nextDataNodeId.getAndIncrement();
      }
      result = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      if (nextDataNodeId.get() < minimumDataNode) {
        result.setMessage(
            String.format(
                "To enable IoTDB-Cluster's data service, please register %d more IoTDB-DataNode",
                minimumDataNode - nextDataNodeId.get()));
      } else if (nextDataNodeId.get() == minimumDataNode) {
        result.setMessage("IoTDB-Cluster could provide data service, now enjoy yourself!");
      }

      LOGGER.info(
          "Successfully register DataNode: {}. Current online DataNodes: {}",
          info,
          onlineDataNodes);
    } finally {
      dataNodeInfoReadWriteLock.writeLock().unlock();
    }
    return result;
  }

  /**
   * Get DataNode info
   *
   * @param getDataNodeInfoReq QueryDataNodeInfoPlan
   * @return The specific DataNode's info or all DataNode info if dataNodeId in
   *     QueryDataNodeInfoPlan is -1
   */
  public DataNodeLocationsResp getDataNodeInfo(GetDataNodeInfoReq getDataNodeInfoReq) {
    DataNodeLocationsResp result = new DataNodeLocationsResp();
    result.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));

    int dataNodeId = getDataNodeInfoReq.getDataNodeID();
    dataNodeInfoReadWriteLock.readLock().lock();
    try {
      if (dataNodeId == -1) {
        result.setDataNodeLocations(new HashMap<>(onlineDataNodes));
      } else {

        result.setDataNodeLocations(
            Collections.singletonMap(dataNodeId, onlineDataNodes.get(dataNodeId)));
      }
    } finally {
      dataNodeInfoReadWriteLock.readLock().unlock();
    }

    return result;
  }

  public int getOnlineDataNodeCount() {
    int result;
    dataNodeInfoReadWriteLock.readLock().lock();
    try {
      result = onlineDataNodes.size();
    } finally {
      dataNodeInfoReadWriteLock.readLock().unlock();
    }
    return result;
  }

  public List<TDataNodeLocation> getOnlineDataNodes() {
    List<TDataNodeLocation> result;
    dataNodeInfoReadWriteLock.readLock().lock();
    try {
      result = new ArrayList<>(onlineDataNodes.values());
    } finally {
      dataNodeInfoReadWriteLock.readLock().unlock();
    }
    return result;
  }

  public TDataNodeLocation getOnlineDataNode(int dataNodeId) {
    TDataNodeLocation result;
    dataNodeInfoReadWriteLock.readLock().lock();
    try {
      result = onlineDataNodes.get(dataNodeId);
    } finally {
      dataNodeInfoReadWriteLock.readLock().unlock();
    }
    return result;
  }

  /**
   * Update ConfigNodeList both in memory and confignode-system.properties file
   *
   * @param applyConfigNodeReq ApplyConfigNodeReq
   * @return APPLY_CONFIGNODE_FAILED if update online ConfigNode failed.
   */
  public TSStatus updateConfigNodeList(ApplyConfigNodeReq applyConfigNodeReq) {
    TSStatus status = new TSStatus();
    configNodeInfoReadWriteLock.writeLock().lock();
    try {
      onlineConfigNodes.add(applyConfigNodeReq.getConfigNodeLocation());
      storeConfigNode();
      LOGGER.info(
          "Successfully apply ConfigNode: {}. Current ConfigNodeGroup: {}",
          applyConfigNodeReq.getConfigNodeLocation(),
          onlineConfigNodes);
      status.setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (IOException e) {
      LOGGER.error("Update online ConfigNode failed.", e);
      status.setCode(TSStatusCode.APPLY_CONFIGNODE_FAILED.getStatusCode());
      status.setMessage(
          "Apply new ConfigNode failed because current ConfigNode can't store ConfigNode information.");
    } finally {
      configNodeInfoReadWriteLock.writeLock().unlock();
    }
    return status;
  }

  private void storeConfigNode() throws IOException {
    Properties systemProperties = new Properties();
    try (FileInputStream inputStream = new FileInputStream(systemPropertiesFile)) {
      systemProperties.load(inputStream);
    }
    systemProperties.setProperty(
        "confignode_list", NodeUrlUtils.convertTConfigNodeUrls(new ArrayList<>(onlineConfigNodes)));
    systemProperties.store(new FileOutputStream(systemPropertiesFile), "");
  }

  public List<TConfigNodeLocation> getOnlineConfigNodes() {
    List<TConfigNodeLocation> result;
    configNodeInfoReadWriteLock.readLock().lock();
    try {
      result = new ArrayList<>(onlineConfigNodes);
    } finally {
      configNodeInfoReadWriteLock.readLock().unlock();
    }
    return result;
  }

  public int generateNextDataNodeId() {
    return nextDataNodeId.getAndIncrement();
  }

  public void serialize(ByteBuffer buffer) {
    // TODO: Serialize DataNodeInfo
  }

  public void deserialize(ByteBuffer buffer) {
    // TODO: Deserialize DataNodeInfo
  }

  @TestOnly
  public void clear() {
    nextDataNodeId = new AtomicInteger(0);
    onlineDataNodes.clear();
    drainingDataNodes.clear();
    onlineConfigNodes.clear();
  }

  private static class DataNodeInfoPersistenceHolder {

    private static final NodeInfo INSTANCE = new NodeInfo();

    private DataNodeInfoPersistenceHolder() {
      // empty constructor
    }
  }

  public static NodeInfo getInstance() {
    return NodeInfo.DataNodeInfoPersistenceHolder.INSTANCE;
  }
}
