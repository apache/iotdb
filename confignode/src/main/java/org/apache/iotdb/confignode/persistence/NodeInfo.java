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

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.conf.SystemPropertiesUtils;
import org.apache.iotdb.confignode.consensus.request.read.GetDataNodeConfigurationPlan;
import org.apache.iotdb.confignode.consensus.request.write.ApplyConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.RegisterDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.RemoveConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.RemoveDataNodePlan;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
  private final Set<TConfigNodeLocation> registeredConfigNodes;

  // Registered DataNodes
  private final ReentrantReadWriteLock dataNodeInfoReadWriteLock;
  private final AtomicInteger nextNodeId = new AtomicInteger(-1);
  private final ConcurrentNavigableMap<Integer, TDataNodeConfiguration> registeredDataNodes =
      new ConcurrentSkipListMap<>();

  // For remove or draining DataNode
  private final Set<TDataNodeLocation> drainingDataNodes = new HashSet<>();

  private final String snapshotFileName = "node_info.bin";

  public NodeInfo() {
    this.dataNodeInfoReadWriteLock = new ReentrantReadWriteLock();
    this.configNodeInfoReadWriteLock = new ReentrantReadWriteLock();
    this.registeredConfigNodes = new HashSet<>();
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
    TDataNodeConfiguration info = registerDataNodePlan.getInfo();
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
   * @param req RemoveDataNodeReq
   * @return TSStatus
   */
  public TSStatus removeDataNode(RemoveDataNodePlan req) {
    LOGGER.info("there are {} data node in cluster before remove some", registeredDataNodes.size());
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
    LOGGER.info("there are {} data node in cluster after remove some", registeredDataNodes.size());
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

  /** Return the number of registered Nodes */
  public int getRegisteredNodeCount() {
    int result;
    configNodeInfoReadWriteLock.readLock().lock();
    dataNodeInfoReadWriteLock.readLock().lock();
    try {
      result = registeredConfigNodes.size() + registeredDataNodes.size();
    } finally {
      dataNodeInfoReadWriteLock.readLock().unlock();
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

      registeredConfigNodes.add(applyConfigNodePlan.getConfigNodeLocation());
      SystemPropertiesUtils.storeConfigNodeList(new ArrayList<>(registeredConfigNodes));
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
      registeredConfigNodes.remove(removeConfigNodePlan.getConfigNodeLocation());
      SystemPropertiesUtils.storeConfigNodeList(new ArrayList<>(registeredConfigNodes));
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
      result = new ArrayList<>(registeredConfigNodes);
    } finally {
      configNodeInfoReadWriteLock.readLock().unlock();
    }
    return result;
  }

  public int generateNextNodeId() {
    return nextNodeId.incrementAndGet();
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

      serializeDrainingDataNodes(fileOutputStream, protocol);

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
    for (TConfigNodeLocation configNodeLocation : registeredConfigNodes) {
      configNodeLocation.write(protocol);
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

  private void serializeDrainingDataNodes(OutputStream outputStream, TProtocol protocol)
      throws IOException, TException {
    ReadWriteIOUtils.write(drainingDataNodes.size(), outputStream);
    for (TDataNodeLocation tDataNodeLocation : drainingDataNodes) {
      tDataNodeLocation.write(protocol);
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

      deserializeDrainingDataNodes(fileInputStream, protocol);

    } finally {
      configNodeInfoReadWriteLock.writeLock().unlock();
      dataNodeInfoReadWriteLock.writeLock().unlock();
    }
  }

  private void deserializeRegisteredConfigNode(InputStream inputStream, TProtocol protocol)
      throws IOException, TException {
    int size = ReadWriteIOUtils.readInt(inputStream);
    while (size > 0) {
      TConfigNodeLocation configNodeLocation = new TConfigNodeLocation();
      configNodeLocation.read(protocol);
      registeredConfigNodes.add(configNodeLocation);
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

  private void deserializeDrainingDataNodes(InputStream inputStream, TProtocol protocol)
      throws IOException, TException {
    int size = ReadWriteIOUtils.readInt(inputStream);
    while (size > 0) {
      TDataNodeLocation tDataNodeLocation = new TDataNodeLocation();
      tDataNodeLocation.read(protocol);
      drainingDataNodes.add(tDataNodeLocation);
      size--;
    }
  }

  // as drainingDataNodes is not currently implemented, manually set it to validate the test
  @TestOnly
  public void setDrainingDataNodes(Set<TDataNodeLocation> tDataNodeLocations) {
    drainingDataNodes.addAll(tDataNodeLocations);
  }

  @TestOnly
  public int getNextNodeId() {
    return nextNodeId.get();
  }

  public static int getMinimumDataNode() {
    return minimumDataNode;
  }

  @TestOnly
  public Set<TDataNodeLocation> getDrainingDataNodes() {
    return drainingDataNodes;
  }

  public void clear() {
    nextNodeId.set(-1);
    registeredDataNodes.clear();
    drainingDataNodes.clear();
    registeredConfigNodes.clear();
  }
}
