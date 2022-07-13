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
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeInfo;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.enums.DataNodeRemoveState;
import org.apache.iotdb.commons.enums.RegionMigrateState;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.conf.SystemPropertiesUtils;
import org.apache.iotdb.confignode.consensus.request.read.GetDataNodeInfoPlan;
import org.apache.iotdb.confignode.consensus.request.write.ActivateDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.ApplyConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.RegisterDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.RemoveConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.RemoveDataNodePlan;
import org.apache.iotdb.confignode.consensus.response.DataNodeInfosResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRemoveReq;
import org.apache.iotdb.db.service.metrics.MetricsService;
import org.apache.iotdb.db.service.metrics.enums.Metric;
import org.apache.iotdb.db.service.metrics.enums.Tag;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.utils.MetricLevel;
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
import java.util.concurrent.LinkedBlockingQueue;
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
  private final AtomicInteger nextNodeId = new AtomicInteger(0);
  private final ConcurrentNavigableMap<Integer, TDataNodeInfo> registeredDataNodes =
      new ConcurrentSkipListMap<>();

  // For remove or draining DataNode
  // TODO: implement
  private final Set<TDataNodeLocation> drainingDataNodes = new HashSet<>();

  private final RemoveNodeInfo removeNodeInfo;

  private final String snapshotFileName = "node_info.bin";

  public NodeInfo() {
    this.dataNodeInfoReadWriteLock = new ReentrantReadWriteLock();
    this.configNodeInfoReadWriteLock = new ReentrantReadWriteLock();
    this.registeredConfigNodes = new HashSet<>();
    removeNodeInfo = new RemoveNodeInfo();
  }

  public void addMetrics() {
    if (MetricConfigDescriptor.getInstance().getMetricConfig().getEnableMetric()) {
      MetricsService.getInstance()
          .getMetricManager()
          .getOrCreateAutoGauge(
              Metric.CONFIG_NODE.toString(),
              MetricLevel.CORE,
              registeredConfigNodes,
              o -> getRegisteredDataNodeCount(),
              Tag.NAME.toString(),
              "online");
      MetricsService.getInstance()
          .getMetricManager()
          .getOrCreateAutoGauge(
              Metric.DATA_NODE.toString(),
              MetricLevel.CORE,
              registeredDataNodes,
              Map::size,
              Tag.NAME.toString(),
              "online");
    }
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
      for (Map.Entry<Integer, TDataNodeInfo> entry : registeredDataNodes.entrySet()) {
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
    TDataNodeInfo info = registerDataNodePlan.getInfo();
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
   * add dataNode to onlineDataNodes
   *
   * @param activateDataNodePlan ActivateDataNodePlan
   * @return SUCCESS_STATUS
   */
  public TSStatus activateDataNode(ActivateDataNodePlan activateDataNodePlan) {
    TSStatus result = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    result.setMessage("activateDataNode success.");
    TDataNodeInfo info = activateDataNodePlan.getInfo();
    dataNodeInfoReadWriteLock.writeLock().lock();
    try {
      registeredDataNodes.put(info.getLocation().getDataNodeId(), info);
    } finally {
      dataNodeInfoReadWriteLock.writeLock().unlock();
    }
    return result;
  }

  /**
   * Persist Infomation about remove dataNode
   *
   * @param req RemoveDataNodeReq
   * @return TSStatus
   */
  public TSStatus removeDataNode(RemoveDataNodePlan req) {
    TSStatus result = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    removeNodeInfo.removeDataNode(req);
    return result;
  }

  /**
   * Get DataNode info
   *
   * @param getDataNodeInfoPlan QueryDataNodeInfoPlan
   * @return The specific DataNode's info or all DataNode info if dataNodeId in
   *     QueryDataNodeInfoPlan is -1
   */
  public DataNodeInfosResp getDataNodeInfo(GetDataNodeInfoPlan getDataNodeInfoPlan) {
    DataNodeInfosResp result = new DataNodeInfosResp();
    result.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));

    int dataNodeId = getDataNodeInfoPlan.getDataNodeID();
    dataNodeInfoReadWriteLock.readLock().lock();
    try {
      if (dataNodeId == -1) {
        result.setDataNodeInfoMap(new HashMap<>(registeredDataNodes));
      } else {
        result.setDataNodeInfoMap(
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

  /** Return the number of total cpu cores in online DataNodes */
  public int getTotalCpuCoreCount() {
    int result = 0;
    dataNodeInfoReadWriteLock.readLock().lock();
    try {
      for (TDataNodeInfo info : registeredDataNodes.values()) {
        result += info.getCpuCoreNum();
      }
    } finally {
      dataNodeInfoReadWriteLock.readLock().unlock();
    }
    return result;
  }

  /**
   * Return the specific registered DataNode
   *
   * @param dataNodeId Specific DataNodeId
   * @return All registered DataNodes if dataNodeId equals -1. And return the specific DataNode
   *     otherwise.
   */
  public List<TDataNodeInfo> getRegisteredDataNodes(int dataNodeId) {
    List<TDataNodeInfo> result;
    dataNodeInfoReadWriteLock.readLock().lock();
    try {
      if (dataNodeId == -1) {
        result = new ArrayList<>(registeredDataNodes.values());
      } else {
        result = Collections.singletonList(registeredDataNodes.get(dataNodeId));
      }
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
    return nextNodeId.getAndIncrement();
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

      serializeRegisteredDataNode(fileOutputStream, protocol);

      serializeDrainingDataNodes(fileOutputStream, protocol);

      removeNodeInfo.serializeRemoveNodeInfo(fileOutputStream, protocol);

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

  private void serializeRegisteredDataNode(OutputStream outputStream, TProtocol protocol)
      throws IOException, TException {
    ReadWriteIOUtils.write(registeredDataNodes.size(), outputStream);
    for (Entry<Integer, TDataNodeInfo> entry : registeredDataNodes.entrySet()) {
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

      deserializeRegisteredDataNode(fileInputStream, protocol);

      deserializeDrainingDataNodes(fileInputStream, protocol);

      removeNodeInfo.deserializeRemoveNodeInfo(fileInputStream, protocol);

    } finally {
      configNodeInfoReadWriteLock.writeLock().unlock();
      dataNodeInfoReadWriteLock.writeLock().unlock();
    }
  }

  private void deserializeRegisteredDataNode(InputStream inputStream, TProtocol protocol)
      throws IOException, TException {
    int size = ReadWriteIOUtils.readInt(inputStream);
    while (size > 0) {
      int dataNodeId = ReadWriteIOUtils.readInt(inputStream);
      TDataNodeInfo dataNodeInfo = new TDataNodeInfo();
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
    nextNodeId.set(0);
    registeredDataNodes.clear();
    drainingDataNodes.clear();
    registeredConfigNodes.clear();
    removeNodeInfo.clear();
  }

  /**
   * get data node remove request queue
   *
   * @return LinkedBlockingQueue
   */
  public LinkedBlockingQueue<RemoveDataNodePlan> getDataNodeRemoveRequestQueue() {
    return removeNodeInfo.getDataNodeRemoveRequestQueue();
  }

  /**
   * get head data node remove request
   *
   * @return RemoveDataNodeReq
   */
  public RemoveDataNodePlan getHeadRequestForDataNodeRemove() {
    return removeNodeInfo.getHeadRequest();
  }

  /** storage remove Data Node request Info */
  private class RemoveNodeInfo {
    private LinkedBlockingQueue<RemoveDataNodePlan> dataNodeRemoveRequestQueue =
        new LinkedBlockingQueue<>();

    // which request is running
    private RemoveDataNodePlan headRequest = null;

    public RemoveNodeInfo() {}

    private void removeDataNode(RemoveDataNodePlan req) {
      if (!dataNodeRemoveRequestQueue.contains(req)) {
        dataNodeRemoveRequestQueue.add(req);
      } else {
        updateRemoveState(req);
      }
      LOGGER.info("request detail: {}", req);
    }

    private void removeSoppedDDataNode(TDataNodeLocation node) {
      try {
        dataNodeInfoReadWriteLock.writeLock().lock();
        registeredDataNodes.remove(node.getDataNodeId());
      } finally {
        dataNodeInfoReadWriteLock.writeLock().unlock();
      }
    }

    private void updateRemoveState(RemoveDataNodePlan req) {
      if (!req.isUpdate()) {
        LOGGER.warn("request is not in update status: {}", req);
        return;
      }
      this.headRequest = req;

      if (req.getExecDataNodeState() == DataNodeRemoveState.STOP) {
        // headNodeState = DataNodeRemoveState.STOP;
        int headNodeIndex = req.getExecDataNodeIndex();
        TDataNodeLocation stopNode = req.getDataNodeLocations().get(headNodeIndex);
        removeSoppedDDataNode(stopNode);
        LOGGER.info(
            "the Data Node {} remove succeed, now the registered Data Node size: {}",
            stopNode.getInternalEndPoint(),
            registeredDataNodes.size());
      }

      if (req.isFinished()) {
        this.dataNodeRemoveRequestQueue.remove(req);
        this.headRequest = null;
      }
    }

    private void serializeRemoveNodeInfo(OutputStream outputStream, TProtocol protocol)
        throws IOException, TException {
      // request queue
      ReadWriteIOUtils.write(dataNodeRemoveRequestQueue.size(), outputStream);
      for (RemoveDataNodePlan req : dataNodeRemoveRequestQueue) {
        TDataNodeRemoveReq tReq = new TDataNodeRemoveReq(req.getDataNodeLocations());
        tReq.write(protocol);
      }
      // -1 means headRequest is null,  1 means headRequest is not null
      if (headRequest == null) {
        ReadWriteIOUtils.write(-1, outputStream);
        return;
      }

      ReadWriteIOUtils.write(1, outputStream);
      TDataNodeRemoveReq tHeadReq = new TDataNodeRemoveReq(headRequest.getDataNodeLocations());
      tHeadReq.write(protocol);

      ReadWriteIOUtils.write(headRequest.getExecDataNodeIndex(), outputStream);
      ReadWriteIOUtils.write(headRequest.getExecDataNodeState().getCode(), outputStream);

      ReadWriteIOUtils.write(headRequest.getExecDataNodeRegionIds().size(), outputStream);
      for (TConsensusGroupId regionId : headRequest.getExecDataNodeRegionIds()) {
        regionId.write(protocol);
      }
      ReadWriteIOUtils.write(headRequest.getExecRegionIndex(), outputStream);
      ReadWriteIOUtils.write(headRequest.getExecRegionState().getCode(), outputStream);
    }

    private void deserializeRemoveNodeInfo(InputStream inputStream, TProtocol protocol)
        throws IOException, TException {
      int queueSize = ReadWriteIOUtils.readInt(inputStream);
      dataNodeRemoveRequestQueue = new LinkedBlockingQueue<>();
      for (int i = 0; i < queueSize; i++) {
        TDataNodeRemoveReq tReq = new TDataNodeRemoveReq();
        tReq.read(protocol);
        dataNodeRemoveRequestQueue.add(new RemoveDataNodePlan(tReq.getDataNodeLocations()));
      }
      boolean headRequestExist = ReadWriteIOUtils.readInt(inputStream) == 1;
      if (!headRequestExist) {
        headRequest = null;
        return;
      }

      TDataNodeRemoveReq tHeadReq = new TDataNodeRemoveReq();
      tHeadReq.read(protocol);
      headRequest = new RemoveDataNodePlan(tHeadReq.getDataNodeLocations());
      headRequest.setUpdate(true);
      headRequest.setFinished(false);

      int headNodeIndex = ReadWriteIOUtils.readInt(inputStream);
      DataNodeRemoveState headNodeState =
          DataNodeRemoveState.getStateByCode(ReadWriteIOUtils.readInt(inputStream));
      headRequest.setExecDataNodeIndex(headNodeIndex);
      headRequest.setExecDataNodeState(headNodeState);

      int headNodeRegionSize = ReadWriteIOUtils.readInt(inputStream);
      List<TConsensusGroupId> headNodeRegionIds = new ArrayList<>();
      for (int i = 0; i < headNodeRegionSize; i++) {
        TConsensusGroupId regionId = new TConsensusGroupId();
        regionId.read(protocol);
        headNodeRegionIds.add(regionId);
      }
      headRequest.setExecDataNodeRegionIds(headNodeRegionIds);

      int headRegionIndex = ReadWriteIOUtils.readInt(inputStream);
      RegionMigrateState headRegionState =
          RegionMigrateState.getStateByCode(ReadWriteIOUtils.readInt(inputStream));
      headRequest.setExecRegionIndex(headRegionIndex);
      headRequest.setExecRegionState(headRegionState);
    }

    private void clear() {
      dataNodeRemoveRequestQueue.clear();
      headRequest = null;
    }

    public LinkedBlockingQueue<RemoveDataNodePlan> getDataNodeRemoveRequestQueue() {
      return dataNodeRemoveRequestQueue;
    }

    public RemoveDataNodePlan getHeadRequest() {
      return headRequest;
    }
  }
}
