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
import org.apache.iotdb.common.rpc.thrift.TDataNodeInfo;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.BadNodeUrlException;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.commons.utils.NodeUrlUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeConstant;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.read.GetConfigNodeConfigurationReq;
import org.apache.iotdb.confignode.consensus.request.read.GetDataNodeInfoReq;
import org.apache.iotdb.confignode.consensus.request.write.ApplyConfigNodeReq;
import org.apache.iotdb.confignode.consensus.request.write.RegisterDataNodeReq;
import org.apache.iotdb.confignode.consensus.request.write.RemoveConfigNodeReq;
import org.apache.iotdb.confignode.consensus.response.ConfigNodeConfigurationResp;
import org.apache.iotdb.confignode.consensus.response.DataNodeInfosResp;
import org.apache.iotdb.confignode.rpc.thrift.TGlobalConfig;
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
import java.util.Properties;
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

  private static final File systemPropertiesFile =
      new File(
          ConfigNodeDescriptor.getInstance().getConf().getSystemDir()
              + File.separator
              + ConfigNodeConstant.SYSTEM_FILE_NAME);

  private static final int minimumDataNode =
      Math.max(
          ConfigNodeDescriptor.getInstance().getConf().getSchemaReplicationFactor(),
          ConfigNodeDescriptor.getInstance().getConf().getDataReplicationFactor());

  // Online ConfigNodes
  private final ReentrantReadWriteLock configNodeInfoReadWriteLock;
  private final Set<TConfigNodeLocation> onlineConfigNodes;

  // Online DataNodes
  private final ReentrantReadWriteLock dataNodeInfoReadWriteLock;
  private final AtomicInteger nextNodeId = new AtomicInteger(1);
  private final ConcurrentNavigableMap<Integer, TDataNodeInfo> onlineDataNodes =
      new ConcurrentSkipListMap<>();

  // For remove or draining DataNode
  // TODO: implement
  private final Set<TDataNodeLocation> drainingDataNodes = new HashSet<>();

  private final String snapshotFileName = "node_info.bin";

  public NodeInfo() {
    this.dataNodeInfoReadWriteLock = new ReentrantReadWriteLock();
    this.configNodeInfoReadWriteLock = new ReentrantReadWriteLock();
    this.onlineConfigNodes =
        new HashSet<>(ConfigNodeDescriptor.getInstance().getConf().getConfigNodeList());
  }

  public void addMetrics() {
    if (MetricConfigDescriptor.getInstance().getMetricConfig().getEnableMetric()) {
      MetricsService.getInstance()
          .getMetricManager()
          .getOrCreateAutoGauge(
              Metric.CONFIG_NODE.toString(),
              MetricLevel.CORE,
              onlineConfigNodes,
              o -> getOnlineDataNodeCount(),
              Tag.NAME.toString(),
              "online");
      MetricsService.getInstance()
          .getMetricManager()
          .getOrCreateAutoGauge(
              Metric.DATA_NODE.toString(),
              MetricLevel.CORE,
              onlineDataNodes,
              Map::size,
              Tag.NAME.toString(),
              "online");
    }
  }

  /** @return true if the specific DataNode is now online */
  public boolean isOnlineDataNode(TDataNodeLocation info) {
    boolean result = false;
    dataNodeInfoReadWriteLock.readLock().lock();

    try {
      for (Map.Entry<Integer, TDataNodeInfo> entry : onlineDataNodes.entrySet()) {
        info.setDataNodeId(entry.getKey());
        if (entry.getValue().getLocation().equals(info)) {
          result = true;
          break;
        }
      }
    } finally {
      dataNodeInfoReadWriteLock.readLock().unlock();
    }

    return result;
  }

  /**
   * Persist DataNode info
   *
   * @param registerDataNodeReq RegisterDataNodePlan
   * @return SUCCESS_STATUS
   */
  public TSStatus registerDataNode(RegisterDataNodeReq registerDataNodeReq) {
    TSStatus result;
    TDataNodeInfo info = registerDataNodeReq.getInfo();
    dataNodeInfoReadWriteLock.writeLock().lock();
    try {
      onlineDataNodes.put(info.getLocation().getDataNodeId(), info);

      // To ensure that the nextNodeId is updated correctly when
      // the ConfigNode-followers concurrently processes RegisterDataNodeReq,
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
   * Get DataNode info
   *
   * @param getDataNodeInfoReq QueryDataNodeInfoPlan
   * @return The specific DataNode's info or all DataNode info if dataNodeId in
   *     QueryDataNodeInfoPlan is -1
   */
  public DataNodeInfosResp getDataNodeInfo(GetDataNodeInfoReq getDataNodeInfoReq) {
    DataNodeInfosResp result = new DataNodeInfosResp();
    result.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));

    int dataNodeId = getDataNodeInfoReq.getDataNodeID();
    dataNodeInfoReadWriteLock.readLock().lock();
    try {
      if (dataNodeId == -1) {
        result.setDataNodeInfoMap(new HashMap<>(onlineDataNodes));
      } else {
        result.setDataNodeInfoMap(
            Collections.singletonMap(dataNodeId, onlineDataNodes.get(dataNodeId)));
      }
    } finally {
      dataNodeInfoReadWriteLock.readLock().unlock();
    }

    return result;
  }

  /** Return the number of online DataNodes */
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

  /** Return the number of total cpu cores in online DataNodes */
  public int getTotalCpuCoreCount() {
    int result = 0;
    dataNodeInfoReadWriteLock.readLock().lock();
    try {
      for (TDataNodeInfo info : onlineDataNodes.values()) {
        result += info.getCpuCoreNum();
      }
    } finally {
      dataNodeInfoReadWriteLock.readLock().unlock();
    }
    return result;
  }

  /**
   * Return the specific online DataNode
   *
   * @param dataNodeId Specific DataNodeId
   * @return All online DataNodes if dataNodeId equals -1. And return the specific DataNode
   *     otherwise.
   */
  public List<TDataNodeInfo> getOnlineDataNodes(int dataNodeId) {
    List<TDataNodeInfo> result;
    dataNodeInfoReadWriteLock.readLock().lock();
    try {
      // TODO: Check DataNode status, ensure the returned DataNode isn't removed
      if (dataNodeId == -1) {
        result = new ArrayList<>(onlineDataNodes.values());
      } else {
        result = Collections.singletonList(onlineDataNodes.get(dataNodeId));
      }
    } finally {
      dataNodeInfoReadWriteLock.readLock().unlock();
    }
    return result;
  }

  /**
   * Get target ConfigNode configuration
   *
   * @param req GetConfigNodeConfigurationReq
   * @return GET_CONFIGNODE_CONFIGURATION_FAILED if confignode-system.properties not exist
   */
  public ConfigNodeConfigurationResp getConfigNodeConfiguration(GetConfigNodeConfigurationReq req) {
    ConfigNodeConfigurationResp result = new ConfigNodeConfigurationResp();
    Properties systemProperties = new Properties();
    configNodeInfoReadWriteLock.readLock().lock();

    if (!systemPropertiesFile.exists()) {
      LOGGER.info("The system properties file is not exists.");
      result.setStatus(new TSStatus(TSStatusCode.SYSTEM_PROPERTIES_FILE_NOT_EXIST.getStatusCode()));
      return result;
    }

    try (FileInputStream inputStream = new FileInputStream(systemPropertiesFile)) {
      systemProperties.load(inputStream);
      result.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
      result.setConfigNodes(
          NodeUrlUtils.parseTConfigNodeUrls(systemProperties.getProperty("confignode_list")));

      ConfigNodeConfig conf = ConfigNodeDescriptor.getInstance().getConf();

      TGlobalConfig globalConfig = new TGlobalConfig();
      globalConfig.setDataRegionConsensusProtocolClass(
          systemProperties.getProperty("data_region_consensus_protocol_class"));
      globalConfig.setSchemaRegionConsensusProtocolClass(
          systemProperties.getProperty("schema_region_consensus_protocol_class"));
      globalConfig.setSeriesPartitionSlotNum(
          Integer.parseInt(systemProperties.getProperty("series_partition_slot_num")));
      globalConfig.setSeriesPartitionExecutorClass(
          systemProperties.getProperty("series_partition_executor_class"));
      globalConfig.setTimePartitionInterval(conf.getTimePartitionInterval());
      globalConfig.setDefaultTTL(CommonDescriptor.getInstance().getConfig().getDefaultTTL());
      globalConfig.setSchemaReplicationFactor(conf.getSchemaReplicationFactor());
      globalConfig.setDataReplicationFactor(conf.getDataReplicationFactor());
      result.setGlobalConfig(globalConfig);
    } catch (IOException | BadNodeUrlException e) {
      LOGGER.error("Load system properties file failed.", e);
    } finally {
      configNodeInfoReadWriteLock.readLock().unlock();
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
      // To ensure that the nextNodeId is updated correctly when
      // the ConfigNode-followers concurrently processes ApplyConfigNodeReq,
      // we need to add a synchronization lock here
      synchronized (nextNodeId) {
        if (nextNodeId.get() < applyConfigNodeReq.getConfigNodeLocation().getConfigNodeId()) {
          nextNodeId.set(applyConfigNodeReq.getConfigNodeLocation().getConfigNodeId());
        }
      }

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

  /**
   * Update ConfigNodeList both in memory and confignode-system.properties file
   *
   * @param removeConfigNodeReq RemoveConfigNodeReq
   * @return REMOVE_CONFIGNODE_FAILED if remove online ConfigNode failed.
   */
  public TSStatus removeConfigNodeList(RemoveConfigNodeReq removeConfigNodeReq) {
    TSStatus status = new TSStatus();
    configNodeInfoReadWriteLock.writeLock().lock();
    try {
      onlineConfigNodes.remove(removeConfigNodeReq.getConfigNodeLocation());
      storeConfigNode();
      LOGGER.info(
          "Successfully remove ConfigNode: {}. Current ConfigNodeGroup: {}",
          removeConfigNodeReq.getConfigNodeLocation(),
          onlineConfigNodes);
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

  private void storeConfigNode() throws IOException {
    Properties systemProperties = new Properties();
    try (FileInputStream inputStream = new FileInputStream(systemPropertiesFile)) {
      systemProperties.load(inputStream);
    }
    systemProperties.setProperty(
        "confignode_list", NodeUrlUtils.convertTConfigNodeUrls(new ArrayList<>(onlineConfigNodes)));
    try (FileOutputStream fileOutputStream = new FileOutputStream(systemPropertiesFile)) {
      systemProperties.store(fileOutputStream, "");
    }
  }

  public List<TConfigNodeLocation> getOnlineConfigNodes() {
    List<TConfigNodeLocation> result;
    configNodeInfoReadWriteLock.readLock().lock();
    try {
      // TODO: Check ConfigNode status, ensure the returned ConfigNode isn't removed
      result = new ArrayList<>(onlineConfigNodes);
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

      serializeOnlineDataNode(fileOutputStream, protocol);

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

  private void serializeOnlineDataNode(OutputStream outputStream, TProtocol protocol)
      throws IOException, TException {
    ReadWriteIOUtils.write(onlineDataNodes.size(), outputStream);
    for (Entry<Integer, TDataNodeInfo> entry : onlineDataNodes.entrySet()) {
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

      deserializeOnlineDataNode(fileInputStream, protocol);

      deserializeDrainingDataNodes(fileInputStream, protocol);

    } finally {
      configNodeInfoReadWriteLock.writeLock().unlock();
      dataNodeInfoReadWriteLock.writeLock().unlock();
    }
  }

  private void deserializeOnlineDataNode(InputStream inputStream, TProtocol protocol)
      throws IOException, TException {
    int size = ReadWriteIOUtils.readInt(inputStream);
    while (size > 0) {
      int dataNodeId = ReadWriteIOUtils.readInt(inputStream);
      TDataNodeInfo dataNodeInfo = new TDataNodeInfo();
      dataNodeInfo.read(protocol);
      onlineDataNodes.put(dataNodeId, dataNodeInfo);
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

  @TestOnly
  public Set<TDataNodeLocation> getDrainingDataNodes() {
    return drainingDataNodes;
  }

  public void clear() {
    nextNodeId.set(0);
    onlineDataNodes.clear();
    drainingDataNodes.clear();
    onlineConfigNodes.clear();
  }
}
