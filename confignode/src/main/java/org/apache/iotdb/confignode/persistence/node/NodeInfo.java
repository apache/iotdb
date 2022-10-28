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
package org.apache.iotdb.confignode.persistence.node;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.conf.SystemPropertiesUtils;
import org.apache.iotdb.confignode.consensus.request.read.GetDataNodeConfigurationPlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.ApplyConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.RemoveConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.datanode.RegisterDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.datanode.RemoveDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.datanode.UpdateDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.statistics.UpdateLoadStatisticsPlan;
import org.apache.iotdb.confignode.consensus.response.DataNodeConfigurationResp;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.iotdb.confignode.conf.ConfigNodeConstant.REMOVE_DATANODE_PROCESS;

/**
 * The NodeInfo stores cluster node information. The cluster node information including: 1. DataNode
 * information 2. ConfigNode information
 */
public class NodeInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(NodeInfo.class);

  private static final int minimumDataNode =
      Math.max(
          ConfigNodeDescriptor.getInstance().getConf().getSchemaReplicationFactor(),
          ConfigNodeDescriptor.getInstance().getConf().getDataReplicationFactor());

  // Registered ConfigNodes
  private final ReentrantReadWriteLock configNodeInfoReadWriteLock;
  private final Map<Integer, TConfigNodeLocation> registeredConfigNodes;

  // Registered DataNodes
  private final ReentrantReadWriteLock dataNodeInfoReadWriteLock;
  private final AtomicInteger nextNodeId = new AtomicInteger(-1);
  private final Map<Integer, TDataNodeConfiguration> registeredDataNodes;

  // Node Statistics
  private final Map<Integer, NodeStatistics> nodeStatisticsMap;

  private final String snapshotFileName = "node_info.bin";

  public NodeInfo() {
    this.configNodeInfoReadWriteLock = new ReentrantReadWriteLock();
    this.registeredConfigNodes = new ConcurrentHashMap<>();

    this.dataNodeInfoReadWriteLock = new ReentrantReadWriteLock();
    this.registeredDataNodes = new ConcurrentHashMap<>();

    this.nodeStatisticsMap = new ConcurrentHashMap<>();
  }

  /**
   * Only leader use this interface
   *
   * @return True if the specific DataNode already registered, false otherwise
   */
  public boolean isRegisteredDataNode(TDataNodeLocation dataNodeLocation) {
    boolean result = false;
    int originalDataNodeId = dataNodeLocation.getDataNodeId();

    dataNodeInfoReadWriteLock.readLock().lock();
    try {
      for (Map.Entry<Integer, TDataNodeConfiguration> entry : registeredDataNodes.entrySet()) {
        dataNodeLocation.setDataNodeId(entry.getKey());
        if (entry.getValue().getLocation().equals(dataNodeLocation)) {
          result = true;
          break;
        }
      }
    } finally {
      dataNodeInfoReadWriteLock.readLock().unlock();
    }

    dataNodeLocation.setDataNodeId(originalDataNodeId);
    return result;
  }

  /**
   * Persist DataNode info
   *
   * @param registerDataNodePlan RegisterDataNodePlan
   * @return SUCCESS_STATUS
   */
  public TSStatus registerDataNode(RegisterDataNodePlan registerDataNodePlan) {
    TSStatus result;
    TDataNodeConfiguration info = registerDataNodePlan.getDataNodeConfiguration();
    dataNodeInfoReadWriteLock.writeLock().lock();
    try {

      // To ensure that the nextNodeId is updated correctly when
      // the ConfigNode-followers concurrently processes RegisterDataNodePlan,
      // we need to add a synchronization lock here
      synchronized (nextNodeId) {
        if (nextNodeId.get() < info.getLocation().getDataNodeId()) {
          nextNodeId.set(info.getLocation().getDataNodeId());
        }
      }
      registeredDataNodes.put(info.getLocation().getDataNodeId(), info);

      result = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      if (nextNodeId.get() < minimumDataNode) {
        result.setMessage(
            String.format(
                "To enable IoTDB-Cluster's data service, please register %d more IoTDB-DataNode",
                minimumDataNode - nextNodeId.get()));
      } else if (nextNodeId.get() == minimumDataNode) {
        result.setMessage("IoTDB-Cluster could provide data service, now enjoy yourself!");
      }
    } finally {
      dataNodeInfoReadWriteLock.writeLock().unlock();
    }
    return result;
  }

  /**
   * Persist Information about remove dataNode
   *
   * @param req RemoveDataNodePlan
   * @return TSStatus
   */
  public TSStatus removeDataNode(RemoveDataNodePlan req) {
    LOGGER.info(
        "{}, There are {} data node in cluster before executed remove-datanode.sh",
        REMOVE_DATANODE_PROCESS,
        registeredDataNodes.size());
    try {
      dataNodeInfoReadWriteLock.writeLock().lock();
      req.getDataNodeLocations()
          .forEach(
              removeDataNodes -> {
                registeredDataNodes.remove(removeDataNodes.getDataNodeId());
                LOGGER.info("removed the datanode {} from cluster", removeDataNodes);
              });
    } finally {
      dataNodeInfoReadWriteLock.writeLock().unlock();
    }
    LOGGER.info(
        "{}, There are {} data node in cluster after executed remove-datanode.sh",
        REMOVE_DATANODE_PROCESS,
        registeredDataNodes.size());
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  /**
   * Update the specified DataNode‘s location
   *
   * @param updateDataNodePlan UpdateDataNodePlan
   * @return SUCCESS_STATUS if update DataNode info successfully, otherwise return
   *     UPDATE_DATA_NODE_ERROR
   */
  public TSStatus updateDataNode(UpdateDataNodePlan updateDataNodePlan) {
    dataNodeInfoReadWriteLock.writeLock().lock();
    try {
      registeredDataNodes
          .get(updateDataNodePlan.getDataNodeLocation().getDataNodeId())
          .setLocation(updateDataNodePlan.getDataNodeLocation());
    } finally {
      dataNodeInfoReadWriteLock.writeLock().unlock();
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  /**
   * Get DataNodeConfiguration
   *
   * @param getDataNodeConfigurationPlan GetDataNodeConfigurationPlan
   * @return The specific DataNode's configuration or all DataNodes' configuration if dataNodeId in
   *     GetDataNodeConfigurationPlan is -1
   */
  public DataNodeConfigurationResp getDataNodeConfiguration(
      GetDataNodeConfigurationPlan getDataNodeConfigurationPlan) {
    DataNodeConfigurationResp result = new DataNodeConfigurationResp();
    result.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));

    int dataNodeId = getDataNodeConfigurationPlan.getDataNodeId();
    dataNodeInfoReadWriteLock.readLock().lock();
    try {
      if (dataNodeId == -1) {
        result.setDataNodeConfigurationMap(new HashMap<>(registeredDataNodes));
      } else {
        result.setDataNodeConfigurationMap(
            registeredDataNodes.get(dataNodeId) == null
                ? new HashMap<>(0)
                : Collections.singletonMap(dataNodeId, registeredDataNodes.get(dataNodeId)));
      }
    } finally {
      dataNodeInfoReadWriteLock.readLock().unlock();
    }

    return result;
  }

  /** Return the number of registered DataNodes */
  public int getRegisteredDataNodeCount() {
    int result;
    dataNodeInfoReadWriteLock.readLock().lock();
    try {
      result = registeredDataNodes.size();
    } finally {
      dataNodeInfoReadWriteLock.readLock().unlock();
    }
    return result;
  }

  /** Return the number of registered ConfigNodes */
  public int getRegisteredConfigNodeCount() {
    int result;
    configNodeInfoReadWriteLock.readLock().lock();
    try {
      result = registeredConfigNodes.size();
    } finally {
      configNodeInfoReadWriteLock.readLock().unlock();
    }
    return result;
  }

  /** Return the number of total cpu cores in online DataNodes */
  public int getTotalCpuCoreCount() {
    int result = 0;
    dataNodeInfoReadWriteLock.readLock().lock();
    try {
      for (TDataNodeConfiguration dataNodeConfiguration : registeredDataNodes.values()) {
        result += dataNodeConfiguration.getResource().getCpuCoreNum();
      }
    } finally {
      dataNodeInfoReadWriteLock.readLock().unlock();
    }
    return result;
  }

  /** Return All registered DataNodes */
  public List<TDataNodeConfiguration> getRegisteredDataNodes() {
    List<TDataNodeConfiguration> result;
    dataNodeInfoReadWriteLock.readLock().lock();
    try {
      result = new ArrayList<>(registeredDataNodes.values());
    } finally {
      dataNodeInfoReadWriteLock.readLock().unlock();
    }
    return result;
  }

  /**
   * Update ConfigNodeList both in memory and confignode-system.properties file
   *
   * @param applyConfigNodePlan ApplyConfigNodePlan
   * @return APPLY_CONFIGNODE_FAILED if update online ConfigNode failed.
   */
  public TSStatus applyConfigNode(ApplyConfigNodePlan applyConfigNodePlan) {
    TSStatus status = new TSStatus();
    configNodeInfoReadWriteLock.writeLock().lock();
    try {
      // To ensure that the nextNodeId is updated correctly when
      // the ConfigNode-followers concurrently processes ApplyConfigNodePlan,
      // we need to add a synchronization lock here
      synchronized (nextNodeId) {
        if (nextNodeId.get() < applyConfigNodePlan.getConfigNodeLocation().getConfigNodeId()) {
          nextNodeId.set(applyConfigNodePlan.getConfigNodeLocation().getConfigNodeId());
        }
      }

      registeredConfigNodes.put(
          applyConfigNodePlan.getConfigNodeLocation().getConfigNodeId(),
          applyConfigNodePlan.getConfigNodeLocation());
      SystemPropertiesUtils.storeConfigNodeList(new ArrayList<>(registeredConfigNodes.values()));
      LOGGER.info(
          "Successfully apply ConfigNode: {}. Current ConfigNodeGroup: {}",
          applyConfigNodePlan.getConfigNodeLocation(),
          registeredConfigNodes);
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

  /**
   * Update ConfigNodeList both in memory and confignode-system.properties file
   *
   * @param removeConfigNodePlan RemoveConfigNodePlan
   * @return REMOVE_CONFIGNODE_FAILED if remove online ConfigNode failed.
   */
  public TSStatus removeConfigNode(RemoveConfigNodePlan removeConfigNodePlan) {
    TSStatus status = new TSStatus();
    configNodeInfoReadWriteLock.writeLock().lock();
    try {
      registeredConfigNodes.remove(removeConfigNodePlan.getConfigNodeLocation().getConfigNodeId());
      SystemPropertiesUtils.storeConfigNodeList(new ArrayList<>(registeredConfigNodes.values()));
      LOGGER.info(
          "Successfully remove ConfigNode: {}. Current ConfigNodeGroup: {}",
          removeConfigNodePlan.getConfigNodeLocation(),
          registeredConfigNodes);
      status.setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (IOException e) {
      LOGGER.error("Remove online ConfigNode failed.", e);
      status.setCode(TSStatusCode.REMOVE_CONFIGNODE_FAILED.getStatusCode());
      status.setMessage(
          "Remove ConfigNode failed because current ConfigNode can't store ConfigNode information.");
    } finally {
      configNodeInfoReadWriteLock.writeLock().unlock();
    }
    return status;
  }

  public List<TConfigNodeLocation> getRegisteredConfigNodes() {
    List<TConfigNodeLocation> result;
    configNodeInfoReadWriteLock.readLock().lock();
    try {
      result = new ArrayList<>(registeredConfigNodes.values());
    } finally {
      configNodeInfoReadWriteLock.readLock().unlock();
    }
    return result;
  }

  public int generateNextNodeId() {
    return nextNodeId.incrementAndGet();
  }

  /**
   * Update NodeStatistics through consensus-write
   *
   * @param updateLoadStatisticsPlan UpdateLoadStatisticsPlan
   */
  public void updateNodeStatistics(UpdateLoadStatisticsPlan updateLoadStatisticsPlan) {
    nodeStatisticsMap.putAll(updateLoadStatisticsPlan.getNodeStatisticsMap());

    // Log current NodeStatistics
    LOGGER.info("[UpdateLoadStatistics] NodeStatisticsMap: ");
    for (Map.Entry<Integer, NodeStatistics> nodeCacheEntry : nodeStatisticsMap.entrySet()) {
      LOGGER.info(
          "[UpdateLoadStatistics]\t {}={}",
          "nodeId{" + nodeCacheEntry.getKey() + "}",
          nodeCacheEntry.getValue());
    }
  }

  /** Only used when the ConfigNode-Leader is switched */
  public Map<Integer, NodeStatistics> getNodeStatisticsMap() {
    return nodeStatisticsMap;
  }

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws IOException, TException {
    File snapshotFile = new File(snapshotDir, snapshotFileName);
    if (snapshotFile.exists() && snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to take snapshot, because snapshot file [{}] is already exist.",
          snapshotFile.getAbsolutePath());
      return false;
    }

    File tmpFile = new File(snapshotFile.getAbsolutePath() + "-" + UUID.randomUUID());
    configNodeInfoReadWriteLock.readLock().lock();
    dataNodeInfoReadWriteLock.readLock().lock();
    try (FileOutputStream fileOutputStream = new FileOutputStream(tmpFile);
        TIOStreamTransport tioStreamTransport = new TIOStreamTransport(fileOutputStream)) {

      TProtocol protocol = new TBinaryProtocol(tioStreamTransport);

      ReadWriteIOUtils.write(nextNodeId.get(), fileOutputStream);

      serializeRegisteredConfigNode(fileOutputStream, protocol);

      serializeRegisteredDataNode(fileOutputStream, protocol);

      serializeNodeStatistics(fileOutputStream);

      fileOutputStream.flush();

      fileOutputStream.close();

      return tmpFile.renameTo(snapshotFile);

    } finally {
      configNodeInfoReadWriteLock.readLock().unlock();
      dataNodeInfoReadWriteLock.readLock().unlock();
      for (int retry = 0; retry < 5; retry++) {
        if (!tmpFile.exists() || tmpFile.delete()) {
          break;
        } else {
          LOGGER.warn(
              "Can't delete temporary snapshot file: {}, retrying...", tmpFile.getAbsolutePath());
        }
      }
    }
  }

  private void serializeRegisteredConfigNode(OutputStream outputStream, TProtocol protocol)
      throws IOException, TException {
    ReadWriteIOUtils.write(registeredConfigNodes.size(), outputStream);
    for (Entry<Integer, TConfigNodeLocation> entry : registeredConfigNodes.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      entry.getValue().write(protocol);
    }
  }

  private void serializeRegisteredDataNode(OutputStream outputStream, TProtocol protocol)
      throws IOException, TException {
    ReadWriteIOUtils.write(registeredDataNodes.size(), outputStream);
    for (Entry<Integer, TDataNodeConfiguration> entry : registeredDataNodes.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      entry.getValue().write(protocol);
    }
  }

  private void serializeNodeStatistics(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(nodeStatisticsMap.size(), outputStream);
    for (Map.Entry<Integer, NodeStatistics> nodeStatisticsEntry : nodeStatisticsMap.entrySet()) {
      ReadWriteIOUtils.write(nodeStatisticsEntry.getKey(), outputStream);
      nodeStatisticsEntry.getValue().serialize(outputStream);
    }
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws IOException, TException {

    File snapshotFile = new File(snapshotDir, snapshotFileName);
    if (!snapshotFile.exists() || !snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to load snapshot,snapshot file [{}] is not exist.",
          snapshotFile.getAbsolutePath());
      return;
    }

    configNodeInfoReadWriteLock.writeLock().lock();
    dataNodeInfoReadWriteLock.writeLock().lock();

    try (FileInputStream fileInputStream = new FileInputStream(snapshotFile);
        TIOStreamTransport tioStreamTransport = new TIOStreamTransport(fileInputStream)) {
      TProtocol protocol = new TBinaryProtocol(tioStreamTransport);

      clear();

      nextNodeId.set(ReadWriteIOUtils.readInt(fileInputStream));

      deserializeRegisteredConfigNode(fileInputStream, protocol);

      deserializeRegisteredDataNode(fileInputStream, protocol);

      deserializeNodeStatistics(fileInputStream);

    } finally {
      configNodeInfoReadWriteLock.writeLock().unlock();
      dataNodeInfoReadWriteLock.writeLock().unlock();
    }
  }

  private void deserializeRegisteredConfigNode(InputStream inputStream, TProtocol protocol)
      throws IOException, TException {
    int size = ReadWriteIOUtils.readInt(inputStream);
    while (size > 0) {
      int configNodeId = ReadWriteIOUtils.readInt(inputStream);
      TConfigNodeLocation configNodeLocation = new TConfigNodeLocation();
      configNodeLocation.read(protocol);
      registeredConfigNodes.put(configNodeId, configNodeLocation);
      size--;
    }
  }

  private void deserializeRegisteredDataNode(InputStream inputStream, TProtocol protocol)
      throws IOException, TException {
    int size = ReadWriteIOUtils.readInt(inputStream);
    while (size > 0) {
      int dataNodeId = ReadWriteIOUtils.readInt(inputStream);
      TDataNodeConfiguration dataNodeInfo = new TDataNodeConfiguration();
      dataNodeInfo.read(protocol);
      registeredDataNodes.put(dataNodeId, dataNodeInfo);
      size--;
    }
  }

  private void deserializeNodeStatistics(InputStream inputStream) throws IOException {
    int size = ReadWriteIOUtils.readInt(inputStream);
    while (size > 0) {
      int nodeId = ReadWriteIOUtils.readInt(inputStream);
      NodeStatistics nodeStatistics = new NodeStatistics();
      nodeStatistics.deserialize(inputStream);
      nodeStatisticsMap.put(nodeId, nodeStatistics);
      size--;
    }
  }

  public static int getMinimumDataNode() {
    return minimumDataNode;
  }

  public void clear() {
    nextNodeId.set(-1);
    registeredDataNodes.clear();
    registeredConfigNodes.clear();
    nodeStatisticsMap.clear();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    NodeInfo nodeInfo = (NodeInfo) o;
    return registeredConfigNodes.equals(nodeInfo.registeredConfigNodes)
        && nextNodeId.get() == nodeInfo.nextNodeId.get()
        && registeredDataNodes.equals(nodeInfo.registeredDataNodes)
        && nodeStatisticsMap.equals(nodeInfo.nodeStatisticsMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(registeredConfigNodes, nextNodeId, registeredDataNodes, nodeStatisticsMap);
  }
}
