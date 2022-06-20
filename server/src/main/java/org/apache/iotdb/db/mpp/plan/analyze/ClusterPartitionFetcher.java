/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.mpp.plan.analyze;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TRegionCache;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.consensus.PartitionRegionId;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.partition.SchemaNodeManagementPartition;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.partition.executor.SeriesPartitionExecutor;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionResp;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaNodeManagementReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaNodeManagementResp;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionResp;
import org.apache.iotdb.confignode.rpc.thrift.TSetStorageGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchemaResp;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.client.ConfigNodeInfo;
import org.apache.iotdb.db.client.DataNodeClientPoolFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ClusterPartitionFetcher implements IPartitionFetcher {
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final List<String> ROOT_PATH = Arrays.asList("root", "**");

  private final SeriesPartitionExecutor partitionExecutor;

  private PartitionCache partitionCache;

  private final IClientManager<PartitionRegionId, ConfigNodeClient> configNodeClientManager =
      new IClientManager.Factory<PartitionRegionId, ConfigNodeClient>()
          .createClientManager(new DataNodeClientPoolFactory.ConfigNodeClientPoolFactory());

  private static final class ClusterPartitionFetcherHolder {
    private static final ClusterPartitionFetcher INSTANCE = new ClusterPartitionFetcher();

    private ClusterPartitionFetcherHolder() {}
  }

  public static ClusterPartitionFetcher getInstance() {
    return ClusterPartitionFetcherHolder.INSTANCE;
  }

  private ClusterPartitionFetcher() {
    this.partitionExecutor =
        SeriesPartitionExecutor.getSeriesPartitionExecutor(
            config.getSeriesPartitionExecutorClass(), config.getSeriesPartitionSlotNum());
    this.partitionCache =
        new PartitionCache(
            partitionExecutor,
            config.getSeriesPartitionExecutorClass(),
            config.getSeriesPartitionSlotNum());
  }

  @Override
  public SchemaPartition getSchemaPartition(PathPatternTree patternTree) {
    try (ConfigNodeClient client =
        configNodeClientManager.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      patternTree.constructTree();
      List<String> devicePaths = patternTree.getAllDevicePatterns();
      Map<String, String> deviceToStorageGroupMap = getDeviceToStorageGroup(devicePaths, false);
      SchemaPartition schemaPartition = partitionCache.getSchemaPartition(deviceToStorageGroupMap);
      if (null == schemaPartition) {
        TSchemaPartitionResp schemaPartitionResp =
            client.getSchemaPartition(constructSchemaPartitionReq(patternTree));
        if (schemaPartitionResp.getStatus().getCode()
            == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          schemaPartition = parseSchemaPartitionResp(schemaPartitionResp);
          partitionCache.updateSchemaPartitionCache(
              devicePaths, schemaPartitionResp.getNewSchemaRegionMap());
        }
      }
      return schemaPartition;
    } catch (TException | IOException e) {
      throw new StatementAnalyzeException(
          "An error occurred when executing getSchemaPartition():" + e.getMessage());
    }
  }

  @Override
  public SchemaPartition getOrCreateSchemaPartition(PathPatternTree patternTree) {
    try (ConfigNodeClient client =
        configNodeClientManager.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      patternTree.constructTree();
      List<String> devicePaths = patternTree.getAllDevicePatterns();
      Map<String, String> deviceToStorageGroupMap = getDeviceToStorageGroup(devicePaths, true);
      SchemaPartition schemaPartition = partitionCache.getSchemaPartition(deviceToStorageGroupMap);
      if (null == schemaPartition) {
        TSchemaPartitionResp schemaPartitionResp =
            client.getOrCreateSchemaPartition(constructSchemaPartitionReq(patternTree));
        if (schemaPartitionResp.getStatus().getCode()
            == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          schemaPartition = parseSchemaPartitionResp(schemaPartitionResp);
          partitionCache.updateSchemaPartitionCache(
              devicePaths, schemaPartitionResp.getNewSchemaRegionMap());
        }
      }
      return schemaPartition;
    } catch (TException | IOException e) {
      throw new StatementAnalyzeException(
          "An error occurred when executing getOrCreateSchemaPartition():" + e.getMessage());
    }
  }

  @Override
  public SchemaNodeManagementPartition getSchemaNodeManagementPartitionWithLevel(
      PathPatternTree patternTree, Integer level) {
    try (ConfigNodeClient client =
        configNodeClientManager.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      patternTree.constructTree();
      TSchemaNodeManagementResp schemaNodeManagementResp =
          client.getSchemaNodeManagementPartition(
              constructSchemaNodeManagementPartitionReq(patternTree, level));

      return parseSchemaNodeManagementPartitionResp(schemaNodeManagementResp);
    } catch (TException | IOException e) {
      throw new StatementAnalyzeException(
          "An error occurred when executing getSchemaNodeManagementPartition():" + e.getMessage());
    }
  }

  @Override
  public DataPartition getDataPartition(
      Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap) {
    try (ConfigNodeClient client =
        configNodeClientManager.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      TDataPartitionResp dataPartitionResp =
          client.getDataPartition(constructDataPartitionReq(sgNameToQueryParamsMap));
      if (dataPartitionResp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return parseDataPartitionResp(dataPartitionResp);
      }
    } catch (TException | IOException e) {
      throw new StatementAnalyzeException(
          "An error occurred when executing getDataPartition():" + e.getMessage());
    }
    return null;
  }

  @Override
  public DataPartition getDataPartition(List<DataPartitionQueryParam> dataPartitionQueryParams) {
    try (ConfigNodeClient client =
        configNodeClientManager.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      Map<String, List<DataPartitionQueryParam>> splitDataPartitionQueryParams =
          splitDataPartitionQueryParam(dataPartitionQueryParams, false);
      DataPartition dataPartition = partitionCache.getDataPartition(splitDataPartitionQueryParams);
      if (null == dataPartition) {
        TDataPartitionResp dataPartitionResp =
            client.getDataPartition(constructDataPartitionReq(splitDataPartitionQueryParams));
        if (dataPartitionResp.getStatus().getCode()
            == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          dataPartition = parseDataPartitionResp(dataPartitionResp);
          partitionCache.updateDataPartitionCache(dataPartitionResp.getNewDataPartitionMap());
        }
      }
      return dataPartition;
    } catch (TException | IOException e) {
      throw new StatementAnalyzeException(
          "An error occurred when executing getDataPartition():" + e.getMessage());
    }
  }

  @Override
  public DataPartition getOrCreateDataPartition(
      Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap) {
    // Do not use data partition cache
    try (ConfigNodeClient client =
        configNodeClientManager.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      TDataPartitionResp dataPartitionResp =
          client.getOrCreateDataPartition(constructDataPartitionReq(sgNameToQueryParamsMap));
      if (dataPartitionResp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return parseDataPartitionResp(dataPartitionResp);
      }
    } catch (TException | IOException e) {
      throw new StatementAnalyzeException(
          "An error occurred when executing getOrCreateDataPartition():" + e.getMessage());
    }
    return null;
  }

  @Override
  public DataPartition getOrCreateDataPartition(
      List<DataPartitionQueryParam> dataPartitionQueryParams) {
    try (ConfigNodeClient client =
        configNodeClientManager.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      Map<String, List<DataPartitionQueryParam>> splitDataPartitionQueryParams =
          splitDataPartitionQueryParam(dataPartitionQueryParams, true);
      DataPartition dataPartition = partitionCache.getDataPartition(splitDataPartitionQueryParams);
      if (null == dataPartition) {
        TDataPartitionResp dataPartitionResp =
            client.getOrCreateDataPartition(
                constructDataPartitionReq(splitDataPartitionQueryParams));
        if (dataPartitionResp.getStatus().getCode()
            == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          dataPartition = parseDataPartitionResp(dataPartitionResp);
          partitionCache.updateDataPartitionCache(dataPartitionResp.getNewDataPartitionMap());
        }
      }
      return dataPartition;
    } catch (TException | IOException e) {
      throw new StatementAnalyzeException(
          "An error occurred when executing getOrCreateDataPartition():" + e.getMessage());
    }
  }

  @Override
  public void updateRegionCache(TRegionCache regionCache) {
    partitionCache.updateRegionCache(regionCache);
  }

  @Override
  public void invalidAllCache() {
    partitionCache.invalidAll();
  }

  /** get deviceToStorageGroup map */
  private Map<String, String> getDeviceToStorageGroup(
      List<String> devicePaths, boolean isAutoCreate) {
    Map<String, String> deviceToStorageGroup = new HashMap<>();
    // miss when devicePath contains *
    for (String devicePath : devicePaths) {
      if (devicePath.contains("*")) {
        return deviceToStorageGroup;
      }
    }
    // first try to hit cache
    boolean firstTryResult = partitionCache.getStorageGroup(devicePaths, deviceToStorageGroup);
    if (!firstTryResult) {
      List<String> storageGroupPathPattern = ROOT_PATH;
      try (ConfigNodeClient client =
          configNodeClientManager.borrowClient(ConfigNodeInfo.partitionRegionId)) {
        TStorageGroupSchemaResp storageGroupSchemaResp =
            client.getMatchedStorageGroupSchemas(storageGroupPathPattern);
        if (storageGroupSchemaResp.getStatus().getCode()
            == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          Set<String> storageGroupNames =
              storageGroupSchemaResp.getStorageGroupSchemaMap().keySet();
          // update all storage group into cache
          partitionCache.updateStorageCache(storageGroupNames);
          // second try to hit cache
          deviceToStorageGroup = new HashMap<>();
          boolean secondTryResult =
              partitionCache.getStorageGroup(devicePaths, deviceToStorageGroup);
          if (!secondTryResult && isAutoCreate) {
            // try to auto create storage group
            Set<String> storageGroupNamesNeedCreated = new HashSet<>();
            for (String devicePath : devicePaths) {
              if (!deviceToStorageGroup.containsKey(devicePath)) {
                PartialPath storageGroupNameNeedCreated =
                    MetaUtils.getStorageGroupPathByLevel(
                        new PartialPath(devicePath), config.getDefaultStorageGroupLevel());
                storageGroupNamesNeedCreated.add(storageGroupNameNeedCreated.getFullPath());
              }
            }
            for (String storageGroupName : storageGroupNamesNeedCreated) {
              TStorageGroupSchema storageGroupSchema = new TStorageGroupSchema();
              storageGroupSchema.setName(storageGroupName);
              TSetStorageGroupReq req = new TSetStorageGroupReq(storageGroupSchema);
              client.setStorageGroup(req);
            }
            partitionCache.updateStorageCache(storageGroupNamesNeedCreated);
            // third try to hit cache
            deviceToStorageGroup = new HashMap<>();
            boolean thirdTryResult =
                partitionCache.getStorageGroup(devicePaths, deviceToStorageGroup);
            if (!thirdTryResult) {
              throw new StatementAnalyzeException(
                  "Failed to get Storage Group Map when executing getOrCreateDataPartition()");
            }
          }
        }
      } catch (TException | MetadataException | IOException e) {
        throw new StatementAnalyzeException(
            "An error occurred when executing getOrCreateDataPartition():" + e.getMessage());
      }
    }
    return deviceToStorageGroup;
  }

  /** split data partition query param by storage group */
  private Map<String, List<DataPartitionQueryParam>> splitDataPartitionQueryParam(
      List<DataPartitionQueryParam> dataPartitionQueryParams, boolean isAutoCreate) {
    List<String> devicePaths = new ArrayList<>();
    for (DataPartitionQueryParam dataPartitionQueryParam : dataPartitionQueryParams) {
      devicePaths.add(dataPartitionQueryParam.getDevicePath());
    }
    Map<String, String> deviceToStorageGroup = getDeviceToStorageGroup(devicePaths, isAutoCreate);

    Map<String, List<DataPartitionQueryParam>> result = new HashMap<>();
    for (DataPartitionQueryParam dataPartitionQueryParam : dataPartitionQueryParams) {
      String devicePath = dataPartitionQueryParam.getDevicePath();
      if (deviceToStorageGroup.containsKey(devicePath)) {
        String storageGroup = deviceToStorageGroup.get(devicePath);
        if (!result.containsKey(storageGroup)) {
          result.put(storageGroup, new ArrayList<>());
        }
        result.get(storageGroup).add(dataPartitionQueryParam);
      }
    }
    return result;
  }

  private TSchemaPartitionReq constructSchemaPartitionReq(PathPatternTree patternTree) {
    PublicBAOS baos = new PublicBAOS();
    try {
      patternTree.serialize(baos);
      ByteBuffer serializedPatternTree = ByteBuffer.allocate(baos.size());
      serializedPatternTree.put(baos.getBuf(), 0, baos.size());
      serializedPatternTree.flip();
      return new TSchemaPartitionReq(serializedPatternTree);
    } catch (IOException e) {
      throw new StatementAnalyzeException("An error occurred when serializing pattern tree");
    }
  }

  private TSchemaNodeManagementReq constructSchemaNodeManagementPartitionReq(
      PathPatternTree patternTree, Integer level) {
    PublicBAOS baos = new PublicBAOS();
    try {
      patternTree.serialize(baos);
      ByteBuffer serializedPatternTree = ByteBuffer.allocate(baos.size());
      serializedPatternTree.put(baos.getBuf(), 0, baos.size());
      serializedPatternTree.flip();
      TSchemaNodeManagementReq schemaNodeManagementReq =
          new TSchemaNodeManagementReq(serializedPatternTree);
      if (null == level) {
        schemaNodeManagementReq.setLevel(-1);
      } else {
        schemaNodeManagementReq.setLevel(level);
      }
      return schemaNodeManagementReq;
    } catch (IOException e) {
      throw new StatementAnalyzeException("An error occurred when serializing pattern tree");
    }
  }

  private TDataPartitionReq constructDataPartitionReq(
      Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap) {
    Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>> partitionSlotsMap =
        new HashMap<>();
    for (Map.Entry<String, List<DataPartitionQueryParam>> entry :
        sgNameToQueryParamsMap.entrySet()) {
      // for each sg
      Map<TSeriesPartitionSlot, List<TTimePartitionSlot>> deviceToTimePartitionMap =
          new HashMap<>();
      for (DataPartitionQueryParam queryParam : entry.getValue()) {
        if (queryParam.getSeriesPartitionSlot() == null) {
          queryParam.setSeriesPartitionSlot(
              partitionExecutor.getSeriesPartitionSlot(queryParam.getDevicePath()));
        }
        deviceToTimePartitionMap.put(
            new TSeriesPartitionSlot(queryParam.getSeriesPartitionSlot().getSlotId()),
            queryParam.getTimePartitionSlotList().stream()
                .map(timePartitionSlot -> new TTimePartitionSlot(timePartitionSlot.getStartTime()))
                .collect(Collectors.toList()));
      }
      partitionSlotsMap.put(entry.getKey(), deviceToTimePartitionMap);
    }
    return new TDataPartitionReq(partitionSlotsMap);
  }

  private SchemaPartition parseSchemaPartitionResp(TSchemaPartitionResp schemaPartitionResp) {
    Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> schemaRegionMap = new HashMap<>();
    for (Map.Entry<String, Map<TSeriesPartitionSlot, TConsensusGroupId>> entry1 :
        schemaPartitionResp.getNewSchemaRegionMap().entrySet()) {
      String storageGroup = entry1.getKey();
      if (!schemaRegionMap.containsKey(storageGroup)) {
        schemaRegionMap.put(storageGroup, new HashMap<>());
      }
      Map<TSeriesPartitionSlot, TRegionReplicaSet> map1 = schemaRegionMap.get(storageGroup);
      for (Map.Entry<TSeriesPartitionSlot, TConsensusGroupId> entry2 :
          entry1.getValue().entrySet()) {
        map1.put(entry2.getKey(), partitionCache.getRegionReplicaSet(entry2.getValue()));
      }
    }
    return new SchemaPartition(
        schemaRegionMap,
        IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionExecutorClass(),
        IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionSlotNum());
  }

  private SchemaNodeManagementPartition parseSchemaNodeManagementPartitionResp(
      TSchemaNodeManagementResp schemaNodeManagementResp) {
    return new SchemaNodeManagementPartition(
        schemaNodeManagementResp.getSchemaRegionMap(),
        IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionExecutorClass(),
        IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionSlotNum(),
        schemaNodeManagementResp.getMatchedNode());
  }

  private DataPartition parseDataPartitionResp(TDataPartitionResp dataPartitionResp) {
    Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
        dataPartitionMap = new HashMap<>();
    for (Map.Entry<
            String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>>>
        entry1 : dataPartitionResp.getNewDataPartitionMap().entrySet()) {
      String storageGroup = entry1.getKey();
      if (!dataPartitionMap.containsKey(storageGroup)) {
        dataPartitionMap.put(storageGroup, new HashMap<>());
      }
      Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>> map1 =
          dataPartitionMap.get(storageGroup);
      for (Map.Entry<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>>
          entry2 : entry1.getValue().entrySet()) {
        TSeriesPartitionSlot seriesPartitionSlot = entry2.getKey();
        if (!map1.containsKey(seriesPartitionSlot)) {
          map1.put(seriesPartitionSlot, new HashMap<>());
        }
        Map<TTimePartitionSlot, List<TRegionReplicaSet>> map2 = map1.get(seriesPartitionSlot);
        for (Map.Entry<TTimePartitionSlot, List<TConsensusGroupId>> entry3 :
            entry2.getValue().entrySet()) {
          TTimePartitionSlot timePartitionSlot = entry3.getKey();
          List<TRegionReplicaSet> regionReplicaSets = new LinkedList<>();
          for (TConsensusGroupId consensusGroupId : entry3.getValue()) {
            regionReplicaSets.add(partitionCache.getRegionReplicaSet(consensusGroupId));
          }
          map2.put(timePartitionSlot, regionReplicaSets);
        }
      }
    }
    return new DataPartition(
        dataPartitionResp.getDataPartitionMap(),
        config.getSeriesPartitionExecutorClass(),
        config.getSeriesPartitionSlotNum());
  }
}
