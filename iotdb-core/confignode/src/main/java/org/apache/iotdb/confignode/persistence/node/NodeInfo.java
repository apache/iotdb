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

import org.apache.iotdb.common.rpc.thrift.TAINodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TAINodeLocation;
import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.conf.SystemPropertiesUtils;
import org.apache.iotdb.confignode.consensus.request.read.ainode.GetAINodeConfigurationPlan;
import org.apache.iotdb.confignode.consensus.request.read.datanode.GetDataNodeConfigurationPlan;
import org.apache.iotdb.confignode.consensus.request.write.ainode.RegisterAINodePlan;
import org.apache.iotdb.confignode.consensus.request.write.ainode.RemoveAINodePlan;
import org.apache.iotdb.confignode.consensus.request.write.ainode.UpdateAINodePlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.ApplyConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.RemoveConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.UpdateVersionInfoPlan;
import org.apache.iotdb.confignode.consensus.request.write.datanode.RegisterDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.datanode.RemoveDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.datanode.UpdateDataNodePlan;
import org.apache.iotdb.confignode.consensus.response.ainode.AINodeConfigurationResp;
import org.apache.iotdb.confignode.consensus.response.datanode.DataNodeConfigurationResp;
import org.apache.iotdb.confignode.rpc.thrift.TNodeVersionInfo;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.tsfile.utils.ReadWriteIOUtils;
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

import static org.apache.iotdb.confignode.conf.ConfigNodeConstant.REMOVE_AINODE_PROCESS;
import static org.apache.iotdb.confignode.conf.ConfigNodeConstant.REMOVE_DATANODE_PROCESS;

/**
 * The {@link NodeInfo} stores cluster node information.
 *
 * <p>The cluster node information includes:
 *
 * <p>1. DataNode information
 *
 * <p>2. ConfigNode information
 */
public class NodeInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(NodeInfo.class);

  private static final int MINIMUM_DATANODE =
      Math.max(
          ConfigNodeDescriptor.getInstance().getConf().getSchemaReplicationFactor(),
          ConfigNodeDescriptor.getInstance().getConf().getDataReplicationFactor());

  // Registered ConfigNodes
  private final ReentrantReadWriteLock configNodeInfoReadWriteLock;
  private final Map<Integer, TConfigNodeLocation> registeredConfigNodes;

  // Registered DataNodes
  private final AtomicInteger nextNodeId = new AtomicInteger(-1);
  private final Map<Integer, TDataNodeConfiguration> registeredDataNodes;
  private final ReentrantReadWriteLock dataNodeInfoReadWriteLock;

  private final Map<Integer, TAINodeConfiguration> registeredAINodes;
  private final ReentrantReadWriteLock aiNodeInfoReadWriteLock;

  private final Map<Integer, TNodeVersionInfo> nodeVersionInfo;
  private final ReentrantReadWriteLock versionInfoReadWriteLock;

  private static final String SNAPSHOT_FILENAME = "node_info.bin";

  public NodeInfo() {
    this.configNodeInfoReadWriteLock = new ReentrantReadWriteLock();
    this.registeredConfigNodes = new ConcurrentHashMap<>();

    this.dataNodeInfoReadWriteLock = new ReentrantReadWriteLock();
    this.registeredDataNodes = new ConcurrentHashMap<>();

    this.aiNodeInfoReadWriteLock = new ReentrantReadWriteLock();
    this.registeredAINodes = new ConcurrentHashMap<>();

    this.nodeVersionInfo = new ConcurrentHashMap<>();
    this.versionInfoReadWriteLock = new ReentrantReadWriteLock();
  }

  /**
   * Persist DataNode info.
   *
   * @param registerDataNodePlan RegisterDataNodePlan
   * @return {@link TSStatusCode#SUCCESS_STATUS}
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
      if (nextNodeId.get() < MINIMUM_DATANODE) {
        result.setMessage(
            String.format(
                "To enable IoTDB-Cluster's data service, please register %d more IoTDB-DataNode",
                MINIMUM_DATANODE - nextNodeId.get()));
      } else if (nextNodeId.get() == MINIMUM_DATANODE) {
        result.setMessage("IoTDB-Cluster could provide data service, now enjoy yourself!");
      }
    } finally {
      dataNodeInfoReadWriteLock.writeLock().unlock();
    }
    return result;
  }

  /**
   * Persist Information about remove dataNode.
   *
   * @param req RemoveDataNodePlan
   * @return {@link TSStatus}
   */
  public TSStatus removeDataNode(RemoveDataNodePlan req) {
    LOGGER.info(
        "{}, There are {} data node in cluster before executed RemoveDataNodePlan",
        REMOVE_DATANODE_PROCESS,
        registeredDataNodes.size());

    dataNodeInfoReadWriteLock.writeLock().lock();
    versionInfoReadWriteLock.writeLock().lock();
    try {
      req.getDataNodeLocations()
          .forEach(
              removeDataNodes -> {
                registeredDataNodes.remove(removeDataNodes.getDataNodeId());
                nodeVersionInfo.remove(removeDataNodes.getDataNodeId());
                LOGGER.info("Removed the datanode {} from cluster", removeDataNodes);
              });
    } finally {
      versionInfoReadWriteLock.writeLock().unlock();
      dataNodeInfoReadWriteLock.writeLock().unlock();
    }
    LOGGER.info(
        "{}, There are {} data node in cluster after executed RemoveDataNodePlan",
        REMOVE_DATANODE_PROCESS,
        registeredDataNodes.size());
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  /**
   * Update the specified DataNode‘s location.
   *
   * @param updateDataNodePlan UpdateDataNodePlan
   * @return {@link TSStatusCode#SUCCESS_STATUS} if update DataNode info successfully.
   */
  public TSStatus updateDataNode(UpdateDataNodePlan updateDataNodePlan) {
    dataNodeInfoReadWriteLock.writeLock().lock();
    try {
      TDataNodeConfiguration newConfiguration = updateDataNodePlan.getDataNodeConfiguration();
      registeredDataNodes.replace(newConfiguration.getLocation().getDataNodeId(), newConfiguration);
    } finally {
      dataNodeInfoReadWriteLock.writeLock().unlock();
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  /**
   * Get DataNodeConfiguration.
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

  public AINodeConfigurationResp getAINodeConfiguration(
      GetAINodeConfigurationPlan getAINodeConfigurationPlan) {
    AINodeConfigurationResp result = new AINodeConfigurationResp();
    result.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));

    int aiNodeId = getAINodeConfigurationPlan.getAiNodeId();
    aiNodeInfoReadWriteLock.readLock().lock();
    try {
      if (aiNodeId == -1) {
        result.setAiNodeConfigurationMap(new HashMap<>(registeredAINodes));
      } else {
        result.setAiNodeConfigurationMap(
            registeredAINodes.get(aiNodeId) == null
                ? new HashMap<>(0)
                : Collections.singletonMap(aiNodeId, registeredAINodes.get(aiNodeId)));
      }
    } finally {
      aiNodeInfoReadWriteLock.readLock().unlock();
    }
    return result;
  }

  /** Return the number of registered Nodes. */
  public int getRegisteredNodeCount() {
    int result;
    dataNodeInfoReadWriteLock.readLock().lock();
    try {
      result = registeredDataNodes.size();
    } finally {
      dataNodeInfoReadWriteLock.readLock().unlock();
    }
    configNodeInfoReadWriteLock.readLock().lock();
    try {
      result += registeredConfigNodes.size();
    } finally {
      configNodeInfoReadWriteLock.readLock().unlock();
    }
    return result;
  }

  /** Return the number of registered DataNodes. */
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

  public int getDataNodeCpuCoreCount(int dataNodeId) {
    try {
      return registeredDataNodes.get(dataNodeId).getResource().getCpuCoreNum();
    } catch (Exception e) {
      LOGGER.warn("Get DataNode {} cpu core fail, will be treated as zero.", dataNodeId, e);
      return 0;
    }
  }

  /** Return the number of total cpu cores in online DataNodes. */
  public int getDataNodeTotalCpuCoreCount() {
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

  /**
   * @return All registered DataNodes.
   */
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
   * @return The specified registered DataNode.
   */
  public TDataNodeConfiguration getRegisteredDataNode(int dataNodeId) {
    dataNodeInfoReadWriteLock.readLock().lock();
    try {
      return registeredDataNodes.getOrDefault(dataNodeId, new TDataNodeConfiguration()).deepCopy();
    } finally {
      dataNodeInfoReadWriteLock.readLock().unlock();
    }
  }

  /**
   * @return The specified registered DataNodes.
   */
  public List<TDataNodeConfiguration> getRegisteredDataNodes(List<Integer> dataNodeIds) {
    List<TDataNodeConfiguration> result = new ArrayList<>();
    dataNodeInfoReadWriteLock.readLock().lock();
    try {
      dataNodeIds.forEach(
          dataNodeId -> {
            if (registeredDataNodes.containsKey(dataNodeId)) {
              result.add(registeredDataNodes.get(dataNodeId).deepCopy());
            }
          });
    } finally {
      dataNodeInfoReadWriteLock.readLock().unlock();
    }
    return result;
  }

  public List<TAINodeConfiguration> getRegisteredAINodes() {
    List<TAINodeConfiguration> result;
    aiNodeInfoReadWriteLock.readLock().lock();
    try {
      result = new ArrayList<>(registeredAINodes.values());
    } finally {
      aiNodeInfoReadWriteLock.readLock().unlock();
    }
    return result;
  }

  public TAINodeConfiguration getRegisteredAINode(int aiNodeId) {
    aiNodeInfoReadWriteLock.readLock().lock();
    try {
      return registeredAINodes.getOrDefault(aiNodeId, new TAINodeConfiguration()).deepCopy();
    } finally {
      aiNodeInfoReadWriteLock.readLock().unlock();
    }
  }

  /** Return the number of registered DataNodes. */
  public int getRegisteredAINodeCount() {
    int result;
    aiNodeInfoReadWriteLock.readLock().lock();
    try {
      result = registeredAINodes.size();
    } finally {
      aiNodeInfoReadWriteLock.readLock().unlock();
    }
    return result;
  }

  public boolean containsAINode(int aiNodeId) {
    aiNodeInfoReadWriteLock.readLock().lock();
    try {
      return registeredAINodes.containsKey(aiNodeId);
    } finally {
      aiNodeInfoReadWriteLock.readLock().unlock();
    }
  }

  /**
   * Update ConfigNodeList both in memory and confignode-system{@literal .}properties file.
   *
   * @param applyConfigNodePlan ApplyConfigNodePlan
   * @return {@link TSStatusCode#ADD_CONFIGNODE_ERROR} if update online ConfigNode failed.
   */
  public TSStatus applyConfigNode(ApplyConfigNodePlan applyConfigNodePlan) {
    TSStatus status = new TSStatus();
    configNodeInfoReadWriteLock.writeLock().lock();
    try {
      // To ensure that the nextNodeId is updated correctly when
      // the ConfigNode-followers concurrently processes ApplyConfigNodePlan,
      // We need to add a synchronization lock here
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
      status.setCode(TSStatusCode.ADD_CONFIGNODE_ERROR.getStatusCode());
      status.setMessage(
          "Apply new ConfigNode failed because current ConfigNode can't store ConfigNode information.");
    } finally {
      configNodeInfoReadWriteLock.writeLock().unlock();
    }
    return status;
  }

  /**
   * Update ConfigNodeList both in memory and confignode-system{@literal .}properties file.
   *
   * @param removeConfigNodePlan RemoveConfigNodePlan
   * @return {@link TSStatusCode#REMOVE_CONFIGNODE_ERROR} if remove online ConfigNode failed.
   */
  public TSStatus removeConfigNode(RemoveConfigNodePlan removeConfigNodePlan) {
    TSStatus status = new TSStatus();
    configNodeInfoReadWriteLock.writeLock().lock();
    versionInfoReadWriteLock.writeLock().lock();
    try {
      registeredConfigNodes.remove(removeConfigNodePlan.getConfigNodeLocation().getConfigNodeId());
      nodeVersionInfo.remove(removeConfigNodePlan.getConfigNodeLocation().getConfigNodeId());
      SystemPropertiesUtils.storeConfigNodeList(new ArrayList<>(registeredConfigNodes.values()));
      LOGGER.info(
          "Successfully remove ConfigNode: {}. Current ConfigNodeGroup: {}",
          removeConfigNodePlan.getConfigNodeLocation(),
          registeredConfigNodes);
      status.setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (IOException e) {
      LOGGER.error("Remove online ConfigNode failed.", e);
      status.setCode(TSStatusCode.REMOVE_CONFIGNODE_ERROR.getStatusCode());
      status.setMessage(
          "Remove ConfigNode failed because current ConfigNode can't store ConfigNode information.");
    } finally {
      versionInfoReadWriteLock.writeLock().unlock();
      configNodeInfoReadWriteLock.writeLock().unlock();
    }
    return status;
  }

  /**
   * Persist AINode info.
   *
   * @param registerAINodePlan RegisterAINodePlan
   * @return {@link TSStatusCode#SUCCESS_STATUS}
   */
  public TSStatus registerAINode(RegisterAINodePlan registerAINodePlan) {
    TSStatus result;
    TAINodeConfiguration info = registerAINodePlan.getAINodeConfiguration();
    aiNodeInfoReadWriteLock.writeLock().lock();
    try {
      synchronized (nextNodeId) {
        if (nextNodeId.get() < info.getLocation().getAiNodeId()) {
          nextNodeId.set(info.getLocation().getAiNodeId());
        }
      }
      registeredAINodes.put(info.getLocation().getAiNodeId(), info);
      result = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } finally {
      aiNodeInfoReadWriteLock.writeLock().unlock();
    }
    return result;
  }

  /**
   * Update the specified AINode‘s location.
   *
   * @param updateAINodePlan UpdateAINodePlan
   * @return {@link TSStatusCode#SUCCESS_STATUS} if update AINode info successfully.
   */
  public TSStatus updateAINode(UpdateAINodePlan updateAINodePlan) {
    dataNodeInfoReadWriteLock.writeLock().lock();
    try {
      TAINodeConfiguration newConfiguration = updateAINodePlan.getAINodeConfiguration();
      registeredAINodes.replace(newConfiguration.getLocation().getAiNodeId(), newConfiguration);
    } finally {
      dataNodeInfoReadWriteLock.writeLock().unlock();
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  /**
   * Persist Information about remove dataNode.
   *
   * @param req RemoveDataNodePlan
   * @return {@link TSStatus}
   */
  public TSStatus removeAINode(RemoveAINodePlan req) {
    LOGGER.info(
        "{}, There are {} AI nodes in cluster before executed RemoveAINodePlan",
        REMOVE_AINODE_PROCESS,
        registeredAINodes.size());

    aiNodeInfoReadWriteLock.writeLock().lock();
    versionInfoReadWriteLock.writeLock().lock();
    TAINodeLocation removedAINode = req.getAINodeLocation();
    try {
      registeredAINodes.remove(removedAINode.getAiNodeId());
      nodeVersionInfo.remove(removedAINode.getAiNodeId());
      LOGGER.info("Removed the AINode {} from cluster", removedAINode);
    } finally {
      versionInfoReadWriteLock.writeLock().unlock();
      aiNodeInfoReadWriteLock.writeLock().unlock();
    }
    LOGGER.info(
        "{}, There are {} AI nodes in cluster after executed RemoveAINodePlan",
        REMOVE_AINODE_PROCESS,
        registeredAINodes.size());
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  /**
   * Update the specified Node‘s versionInfo.
   *
   * @param updateVersionInfoPlan UpdateVersionInfoPlan
   * @return {@link TSStatusCode#SUCCESS_STATUS} if update build info successfully.
   */
  public TSStatus updateVersionInfo(UpdateVersionInfoPlan updateVersionInfoPlan) {
    versionInfoReadWriteLock.writeLock().lock();
    try {
      nodeVersionInfo.put(
          updateVersionInfoPlan.getNodeId(), updateVersionInfoPlan.getVersionInfo());
    } finally {
      versionInfoReadWriteLock.writeLock().unlock();
    }
    LOGGER.info("Successfully update Node {} 's version.", updateVersionInfoPlan.getNodeId());
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  /**
   * @return All registered ConfigNodes.
   */
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

  /**
   * @return The specified registered ConfigNode.
   */
  public List<TConfigNodeLocation> getRegisteredConfigNodes(List<Integer> configNodeIds) {
    List<TConfigNodeLocation> result = new ArrayList<>();
    configNodeInfoReadWriteLock.readLock().lock();
    try {
      configNodeIds.forEach(
          configNodeId -> {
            if (registeredConfigNodes.containsKey(configNodeId)) {
              result.add(registeredConfigNodes.get(configNodeId).deepCopy());
            }
          });
    } finally {
      configNodeInfoReadWriteLock.readLock().unlock();
    }
    return result;
  }

  /**
   * @return all nodes buildInfo
   */
  public Map<Integer, TNodeVersionInfo> getNodeVersionInfo() {
    Map<Integer, TNodeVersionInfo> result = new HashMap<>(nodeVersionInfo.size());
    versionInfoReadWriteLock.readLock().lock();
    try {
      result.putAll(nodeVersionInfo);
    } finally {
      versionInfoReadWriteLock.readLock().unlock();
    }
    return result;
  }

  public TNodeVersionInfo getVersionInfo(int nodeId) {
    versionInfoReadWriteLock.readLock().lock();
    try {
      return nodeVersionInfo.getOrDefault(nodeId, new TNodeVersionInfo("Unknown", "Unknown"));
    } finally {
      versionInfoReadWriteLock.readLock().unlock();
    }
  }

  public int generateNextNodeId() {
    return nextNodeId.incrementAndGet();
  }

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws IOException, TException {
    File snapshotFile = new File(snapshotDir, SNAPSHOT_FILENAME);
    if (snapshotFile.exists() && snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to take snapshot, because snapshot file [{}] is already exist.",
          snapshotFile.getAbsolutePath());
      return false;
    }

    File tmpFile = new File(snapshotFile.getAbsolutePath() + "-" + UUID.randomUUID());
    configNodeInfoReadWriteLock.readLock().lock();
    dataNodeInfoReadWriteLock.readLock().lock();
    aiNodeInfoReadWriteLock.readLock().lock();
    versionInfoReadWriteLock.readLock().lock();
    try (FileOutputStream fileOutputStream = new FileOutputStream(tmpFile);
        TIOStreamTransport tioStreamTransport = new TIOStreamTransport(fileOutputStream)) {

      TProtocol protocol = new TBinaryProtocol(tioStreamTransport);

      ReadWriteIOUtils.write(nextNodeId.get(), fileOutputStream);

      serializeRegisteredConfigNode(fileOutputStream, protocol);

      serializeRegisteredDataNode(fileOutputStream, protocol);

      serializeRegisteredAINode(fileOutputStream, protocol);

      serializeVersionInfo(fileOutputStream);

      tioStreamTransport.flush();
      fileOutputStream.getFD().sync();

      // The tmpFile can be renamed only after the stream is closed
      tioStreamTransport.close();

      return tmpFile.renameTo(snapshotFile);
    } finally {
      versionInfoReadWriteLock.readLock().unlock();
      aiNodeInfoReadWriteLock.readLock().unlock();
      dataNodeInfoReadWriteLock.readLock().unlock();
      configNodeInfoReadWriteLock.readLock().unlock();
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

  private void serializeRegisteredAINode(OutputStream outputStream, TProtocol protocol)
      throws IOException, TException {
    ReadWriteIOUtils.write(registeredAINodes.size(), outputStream);
    for (Entry<Integer, TAINodeConfiguration> entry : registeredAINodes.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      entry.getValue().write(protocol);
    }
  }

  private void serializeVersionInfo(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(nodeVersionInfo.size(), outputStream);
    for (Entry<Integer, TNodeVersionInfo> entry : nodeVersionInfo.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      ReadWriteIOUtils.write(entry.getValue().getVersion(), outputStream);
      ReadWriteIOUtils.write(entry.getValue().getBuildInfo(), outputStream);
    }
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws IOException, TException {

    File snapshotFile = new File(snapshotDir, SNAPSHOT_FILENAME);
    if (!snapshotFile.exists() || !snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to load snapshot,snapshot file [{}] is not exist.",
          snapshotFile.getAbsolutePath());
      return;
    }

    configNodeInfoReadWriteLock.writeLock().lock();
    dataNodeInfoReadWriteLock.writeLock().lock();
    aiNodeInfoReadWriteLock.writeLock().lock();
    versionInfoReadWriteLock.writeLock().lock();

    try (FileInputStream fileInputStream = new FileInputStream(snapshotFile);
        TIOStreamTransport tioStreamTransport = new TIOStreamTransport(fileInputStream)) {
      TProtocol protocol = new TBinaryProtocol(tioStreamTransport);

      clear();

      nextNodeId.set(ReadWriteIOUtils.readInt(fileInputStream));

      deserializeRegisteredConfigNode(fileInputStream, protocol);

      deserializeRegisteredDataNode(fileInputStream, protocol);

      deserializeRegisteredAINode(fileInputStream, protocol);

      deserializeBuildInfo(fileInputStream);

    } finally {
      versionInfoReadWriteLock.writeLock().unlock();
      aiNodeInfoReadWriteLock.writeLock().unlock();
      dataNodeInfoReadWriteLock.writeLock().unlock();
      configNodeInfoReadWriteLock.writeLock().unlock();
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

  private void deserializeRegisteredAINode(InputStream inputStream, TProtocol protocol)
      throws IOException, TException {
    int size = ReadWriteIOUtils.readInt(inputStream);
    while (size > 0) {
      int aiNodeId = ReadWriteIOUtils.readInt(inputStream);
      TAINodeConfiguration aiNodeInfo = new TAINodeConfiguration();
      aiNodeInfo.read(protocol);
      registeredAINodes.put(aiNodeId, aiNodeInfo);
      size--;
    }
  }

  private void deserializeBuildInfo(InputStream inputStream) throws IOException {
    // old version may not have build info,
    // thus we need to check inputStream before deserialize.
    if (inputStream.available() != 0) {
      int size = ReadWriteIOUtils.readInt(inputStream);
      while (size > 0) {
        int nodeId = ReadWriteIOUtils.readInt(inputStream);
        String version = ReadWriteIOUtils.readString(inputStream);
        String buildInfo = ReadWriteIOUtils.readString(inputStream);
        nodeVersionInfo.put(nodeId, new TNodeVersionInfo(version, buildInfo));
        size--;
      }
    }
  }

  public static int getMinimumDataNode() {
    return MINIMUM_DATANODE;
  }

  public void clear() {
    nextNodeId.set(-1);
    registeredDataNodes.clear();
    registeredConfigNodes.clear();
    nodeVersionInfo.clear();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NodeInfo nodeInfo = (NodeInfo) o;
    return registeredConfigNodes.equals(nodeInfo.registeredConfigNodes)
        && nextNodeId.get() == nodeInfo.nextNodeId.get()
        && registeredDataNodes.equals(nodeInfo.registeredDataNodes)
        && registeredAINodes.equals(nodeInfo.registeredAINodes)
        && nodeVersionInfo.equals(nodeInfo.nodeVersionInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(registeredConfigNodes, nextNodeId, registeredDataNodes, nodeVersionInfo);
  }
}
