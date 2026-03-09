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

package org.apache.iotdb.db.queryengine.plan.analyze;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.partition.SchemaNodeManagementPartition;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.partition.executor.SeriesPartitionExecutor;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaNodeManagementReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaNodeManagementResp;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TTimeSlotList;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.partition.PartitionCache;
import org.apache.iotdb.mpp.rpc.thrift.TRegionRouteReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.apache.tsfile.file.metadata.IDeviceID;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.schema.table.Audit.TREE_MODEL_AUDIT_DATABASE;

public class ClusterPartitionFetcher implements IPartitionFetcher {

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final SeriesPartitionExecutor partitionExecutor;

  private final PartitionCache partitionCache;

  private final IClientManager<ConfigRegionId, ConfigNodeClient> configNodeClientManager =
      ConfigNodeClientManager.getInstance();

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
    this.partitionCache = new PartitionCache();
  }

  @Override
  public SchemaPartition getSchemaPartition(
      final PathPatternTree patternTree, String userName, boolean needAuditDB) {
    patternTree.constructTree();
    final List<IDeviceID> deviceIDs = patternTree.getAllDevicePatterns();
    final Map<String, List<IDeviceID>> storageGroupToDeviceMap =
        partitionCache.getDatabaseToDevice(deviceIDs, true, false, userName);
    SchemaPartition schemaPartition = partitionCache.getSchemaPartition(storageGroupToDeviceMap);
    if (null == schemaPartition) {
      try (final ConfigNodeClient client =
          configNodeClientManager.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        final TSchemaPartitionTableResp schemaPartitionTableResp =
            client.getSchemaPartitionTable(constructSchemaPartitionReq(patternTree));
        if (schemaPartitionTableResp.getStatus().getCode()
            == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          schemaPartition = parseSchemaPartitionTableResp(schemaPartitionTableResp);
          partitionCache.updateSchemaPartitionCache(
              schemaPartitionTableResp.getSchemaPartitionTable());
        } else {
          throw new IoTDBRuntimeException(
              schemaPartitionTableResp.getStatus().getMessage(),
              schemaPartitionTableResp.getStatus().getCode());
        }
      } catch (final ClientManagerException | TException e) {
        throw new StatementAnalyzeException(
            "An error occurred when executing getSchemaPartition():" + e.getMessage());
      }
    }
    if (!needAuditDB) {
      schemaPartition.removeDB(TREE_MODEL_AUDIT_DATABASE);
    }
    return schemaPartition;
  }

  @Override
  public SchemaPartition getSchemaPartition(final PathPatternTree patternTree) {
    return getSchemaPartition(patternTree, true);
  }

  @Override
  public SchemaPartition getSchemaPartition(PathPatternTree patternTree, boolean needAuditDB) {
    return getSchemaPartition(patternTree, null, needAuditDB);
  }

  @Override
  public SchemaPartition getOrCreateSchemaPartition(
      final PathPatternTree patternTree, final String userName) {
    patternTree.constructTree();
    final List<IDeviceID> deviceIDs = patternTree.getAllDevicePatterns();
    final Map<String, List<IDeviceID>> storageGroupToDeviceMap =
        partitionCache.getDatabaseToDevice(deviceIDs, true, true, userName);
    SchemaPartition schemaPartition = partitionCache.getSchemaPartition(storageGroupToDeviceMap);
    if (null == schemaPartition) {
      try (final ConfigNodeClient client =
          configNodeClientManager.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        final TSchemaPartitionTableResp schemaPartitionTableResp =
            client.getOrCreateSchemaPartitionTable(constructSchemaPartitionReq(patternTree));
        if (schemaPartitionTableResp.getStatus().getCode()
            == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          schemaPartition = parseSchemaPartitionTableResp(schemaPartitionTableResp);
          partitionCache.updateSchemaPartitionCache(
              schemaPartitionTableResp.getSchemaPartitionTable());
        } else {
          throw new IoTDBRuntimeException(
              schemaPartitionTableResp.getStatus().getMessage(),
              schemaPartitionTableResp.getStatus().getCode());
        }
      } catch (final ClientManagerException | TException e) {
        throw new StatementAnalyzeException(
            "An error occurred when executing getOrCreateSchemaPartition():" + e.getMessage());
      }
    }
    return schemaPartition;
  }

  @Override
  public SchemaNodeManagementPartition getSchemaNodeManagementPartitionWithLevel(
      final PathPatternTree patternTree,
      final PathPatternTree scope,
      final Integer level,
      final boolean canSeeAuditDB) {
    try (ConfigNodeClient client =
        configNodeClientManager.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      patternTree.constructTree();
      final TSchemaNodeManagementResp schemaNodeManagementResp =
          client.getSchemaNodeManagementPartition(
              constructSchemaNodeManagementPartitionReq(patternTree, scope, level, canSeeAuditDB));

      return parseSchemaNodeManagementPartitionResp(schemaNodeManagementResp);
    } catch (final ClientManagerException | TException e) {
      throw new StatementAnalyzeException(
          "An error occurred when executing getSchemaNodeManagementPartition():" + e.getMessage());
    }
  }

  @Override
  public DataPartition getDataPartition(
      final Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap) {
    DataPartition dataPartition = partitionCache.getDataPartition(sgNameToQueryParamsMap);
    if (null == dataPartition) {
      try (ConfigNodeClient client =
          configNodeClientManager.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        final TDataPartitionTableResp dataPartitionTableResp =
            client.getDataPartitionTable(constructDataPartitionReqForQuery(sgNameToQueryParamsMap));
        if (dataPartitionTableResp.getStatus().getCode()
            == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          dataPartition = parseDataPartitionResp(dataPartitionTableResp);
          partitionCache.updateDataPartitionCache(dataPartitionTableResp.getDataPartitionTable());
        } else {
          throw new StatementAnalyzeException(
              "An error occurred when executing getDataPartition():"
                  + dataPartitionTableResp.getStatus().getMessage());
        }
      } catch (final ClientManagerException | TException e) {
        throw new StatementAnalyzeException(
            "An error occurred when executing getDataPartition():" + e.getMessage());
      }
    }
    return dataPartition;
  }

  @Override
  public DataPartition getDataPartitionWithUnclosedTimeRange(
      final Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap) {
    // In this method, we must fetch from config node because it contains -oo or +oo
    // and there is no need to update cache because since we will never fetch it from cache, the
    // update operation will be only time waste
    try (final ConfigNodeClient client =
        configNodeClientManager.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      final TDataPartitionTableResp dataPartitionTableResp =
          client.getDataPartitionTable(constructDataPartitionReqForQuery(sgNameToQueryParamsMap));
      if (dataPartitionTableResp.getStatus().getCode()
          == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return parseDataPartitionResp(dataPartitionTableResp);
      } else {
        throw new StatementAnalyzeException(
            "An error occurred when executing getDataPartition():"
                + dataPartitionTableResp.getStatus().getMessage());
      }
    } catch (final ClientManagerException | TException e) {
      throw new StatementAnalyzeException(
          "An error occurred when executing getDataPartition():" + e.getMessage());
    }
  }

  @Override
  public DataPartition getOrCreateDataPartition(
      final Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap) {
    DataPartition dataPartition = partitionCache.getDataPartition(sgNameToQueryParamsMap);
    if (null == dataPartition) {
      // Do not use data partition cache
      try (final ConfigNodeClient client =
          configNodeClientManager.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        final TDataPartitionTableResp dataPartitionTableResp =
            client.getOrCreateDataPartitionTable(constructDataPartitionReq(sgNameToQueryParamsMap));
        if (dataPartitionTableResp.getStatus().getCode()
            == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          dataPartition = parseDataPartitionResp(dataPartitionTableResp);
          partitionCache.updateDataPartitionCache(dataPartitionTableResp.getDataPartitionTable());
        } else {
          throw new StatementAnalyzeException(
              "An error occurred when executing getOrCreateDataPartition():"
                  + dataPartitionTableResp.getStatus().getMessage());
        }
      } catch (final ClientManagerException | TException e) {
        throw new StatementAnalyzeException(
            "An error occurred when executing getOrCreateDataPartition():" + e.getMessage());
      }
    }
    return dataPartition;
  }

  @Override
  public DataPartition getOrCreateDataPartition(
      final List<DataPartitionQueryParam> dataPartitionQueryParams, final String userName) {
    DataPartition dataPartition;
    try (final ConfigNodeClient client =
        configNodeClientManager.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      final Map<String, List<DataPartitionQueryParam>> splitDataPartitionQueryParams =
          splitDataPartitionQueryParam(
              dataPartitionQueryParams, config.isAutoCreateSchemaEnabled(), userName);
      dataPartition = partitionCache.getDataPartition(splitDataPartitionQueryParams);

      if (null == dataPartition) {
        final TDataPartitionReq req = constructDataPartitionReq(splitDataPartitionQueryParams);
        final TDataPartitionTableResp dataPartitionTableResp =
            client.getOrCreateDataPartitionTable(req);

        if (dataPartitionTableResp.getStatus().getCode()
            == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          dataPartition = parseDataPartitionResp(dataPartitionTableResp);
          partitionCache.updateDataPartitionCache(dataPartitionTableResp.getDataPartitionTable());
        } else {
          throw new IoTDBRuntimeException(
              dataPartitionTableResp.getStatus().getMessage(),
              dataPartitionTableResp.getStatus().getCode());
        }
      }
    } catch (final ClientManagerException | TException e) {
      throw new StatementAnalyzeException(
          "An error occurred when executing getOrCreateDataPartition():" + e.getMessage());
    }
    return dataPartition;
  }

  @Override
  public boolean updateRegionCache(final TRegionRouteReq req) {
    return partitionCache.updateGroupIdToReplicaSetMap(req.getTimestamp(), req.getRegionRouteMap());
  }

  public List<TRegionReplicaSet> getRegionReplicaSet(List<TConsensusGroupId> consensusGroupIds) {
    return partitionCache.getRegionReplicaSet(consensusGroupIds);
  }

  @Override
  public void invalidAllCache() {
    partitionCache.invalidAllCache();
  }

  @Override
  public SchemaPartition getOrCreateSchemaPartition(
      final String database, final List<IDeviceID> deviceIDs, final String userName) {
    return getOrCreateSchemaPartition(database, deviceIDs, true, userName);
  }

  // deviceIDs is null if search the whole database, in which case we skip the cache and fetch the
  // configNode directly
  @Override
  public SchemaPartition getSchemaPartition(
      final String database, final @Nullable List<IDeviceID> deviceIDs) {
    return getOrCreateSchemaPartition(database, deviceIDs, false, null);
  }

  private SchemaPartition getOrCreateSchemaPartition(
      final String database,
      final @Nullable List<IDeviceID> deviceIDs,
      final boolean isAutoCreate,
      final String userName) {
    try (final ConfigNodeClient client =
        configNodeClientManager.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      partitionCache.checkAndAutoCreateDatabase(database, isAutoCreate, userName);
      SchemaPartition schemaPartition =
          Objects.nonNull(deviceIDs)
              ? partitionCache.getSchemaPartition(Collections.singletonMap(database, deviceIDs))
              : null;
      if (null == schemaPartition) {
        final List<TSeriesPartitionSlot> partitionSlots =
            Objects.nonNull(deviceIDs)
                ? deviceIDs.stream()
                    .map(partitionExecutor::getSeriesPartitionSlot)
                    .distinct()
                    .collect(Collectors.toList())
                : Collections.emptyList();
        final TSchemaPartitionTableResp schemaPartitionTableResp =
            isAutoCreate
                ? client.getOrCreateSchemaPartitionTableWithSlots(
                    Collections.singletonMap(database, partitionSlots))
                : client.getSchemaPartitionTableWithSlots(
                    Collections.singletonMap(database, partitionSlots));
        if (schemaPartitionTableResp.getStatus().getCode()
            == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          schemaPartition = parseSchemaPartitionTableResp(schemaPartitionTableResp);
          partitionCache.updateSchemaPartitionCache(
              schemaPartitionTableResp.getSchemaPartitionTable());
        } else {
          throw new IoTDBRuntimeException(
              schemaPartitionTableResp.getStatus().getMessage(),
              schemaPartitionTableResp.getStatus().getCode());
        }
      }
      return schemaPartition;
    } catch (final ClientManagerException | TException e) {
      throw new StatementAnalyzeException(
          "An error occurred when executing getSchemaPartition():" + e.getMessage());
    }
  }

  /** split data partition query param by database */
  private Map<String, List<DataPartitionQueryParam>> splitDataPartitionQueryParam(
      final List<DataPartitionQueryParam> dataPartitionQueryParams,
      final boolean isAutoCreate,
      final String userName) {
    final List<IDeviceID> deviceIDs = new ArrayList<>();
    for (final DataPartitionQueryParam dataPartitionQueryParam : dataPartitionQueryParams) {
      deviceIDs.add(dataPartitionQueryParam.getDeviceID());
    }
    Map<IDeviceID, String> deviceToDatabase = null;
    final Map<String, List<DataPartitionQueryParam>> result = new HashMap<>();
    for (final DataPartitionQueryParam dataPartitionQueryParam : dataPartitionQueryParams) {
      final IDeviceID deviceID = dataPartitionQueryParam.getDeviceID();
      String database = null;
      if (dataPartitionQueryParam.getDatabaseName() == null) {
        if (deviceToDatabase == null) {
          deviceToDatabase =
              partitionCache.getDeviceToDatabase(deviceIDs, true, isAutoCreate, userName);
        }
        if (deviceToDatabase.containsKey(deviceID)) {
          database = deviceToDatabase.get(deviceID);
        }
      } else {
        database = dataPartitionQueryParam.getDatabaseName();
      }
      if (database != null) {
        result.computeIfAbsent(database, key -> new ArrayList<>()).add(dataPartitionQueryParam);
      }
    }
    return result;
  }

  private TSchemaPartitionReq constructSchemaPartitionReq(final PathPatternTree patternTree) {
    try {
      return new TSchemaPartitionReq(patternTree.serialize());
    } catch (final IOException e) {
      throw new StatementAnalyzeException("An error occurred when serializing pattern tree");
    }
  }

  private TSchemaNodeManagementReq constructSchemaNodeManagementPartitionReq(
      final PathPatternTree patternTree,
      final PathPatternTree scope,
      final Integer level,
      boolean needAuditDB) {
    try {
      final TSchemaNodeManagementReq schemaNodeManagementReq =
          new TSchemaNodeManagementReq(patternTree.serialize());
      schemaNodeManagementReq.setScopePatternTree(scope.serialize());
      schemaNodeManagementReq.setNeedAuditDB(needAuditDB);
      if (null == level) {
        schemaNodeManagementReq.setLevel(-1);
      } else {
        schemaNodeManagementReq.setLevel(level);
      }
      return schemaNodeManagementReq;
    } catch (final IOException e) {
      throw new StatementAnalyzeException("An error occurred when serializing pattern tree");
    }
  }

  private static class ComplexTimeSlotList {

    Set<TTimePartitionSlot> timeSlotList;
    boolean needLeftAll;
    boolean needRightAll;

    private ComplexTimeSlotList(final boolean needLeftAll, final boolean needRightAll) {
      timeSlotList = new HashSet<>();
      this.needLeftAll = needLeftAll;
      this.needRightAll = needRightAll;
    }

    private void putTimeSlot(final List<TTimePartitionSlot> slotList) {
      timeSlotList.addAll(slotList);
    }
  }

  private TDataPartitionReq constructDataPartitionReq(
      final Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap) {
    final Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> partitionSlotsMap = new HashMap<>();
    for (final Map.Entry<String, List<DataPartitionQueryParam>> entry :
        sgNameToQueryParamsMap.entrySet()) {
      // for each sg
      final Map<TSeriesPartitionSlot, TTimeSlotList> deviceToTimePartitionMap = new HashMap<>();

      final Map<TSeriesPartitionSlot, ComplexTimeSlotList> seriesSlotTimePartitionMap =
          new HashMap<>();

      for (final DataPartitionQueryParam queryParam : entry.getValue()) {
        seriesSlotTimePartitionMap
            .computeIfAbsent(
                partitionExecutor.getSeriesPartitionSlot(queryParam.getDeviceID()),
                k ->
                    new ComplexTimeSlotList(
                        queryParam.isNeedLeftAll(), queryParam.isNeedRightAll()))
            .putTimeSlot(queryParam.getTimePartitionSlotList());
      }
      seriesSlotTimePartitionMap.forEach(
          (k, v) ->
              deviceToTimePartitionMap.put(
                  k,
                  new TTimeSlotList(
                      new ArrayList<>(v.timeSlotList), v.needLeftAll, v.needRightAll)));
      partitionSlotsMap.put(entry.getKey(), deviceToTimePartitionMap);
    }
    return new TDataPartitionReq(partitionSlotsMap);
  }

  /** For query, DataPartitionQueryParam is shared by each device */
  private TDataPartitionReq constructDataPartitionReqForQuery(
      final Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap) {
    final Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> partitionSlotsMap = new HashMap<>();
    TTimeSlotList sharedTTimeSlotList = null;
    for (final Map.Entry<String, List<DataPartitionQueryParam>> entry :
        sgNameToQueryParamsMap.entrySet()) {
      // for each sg
      final Map<TSeriesPartitionSlot, TTimeSlotList> deviceToTimePartitionMap = new HashMap<>();

      for (final DataPartitionQueryParam queryParam : entry.getValue()) {
        if (sharedTTimeSlotList == null) {
          sharedTTimeSlotList =
              new TTimeSlotList(
                  queryParam.getTimePartitionSlotList(),
                  queryParam.isNeedLeftAll(),
                  queryParam.isNeedRightAll());
        }
        deviceToTimePartitionMap.putIfAbsent(
            partitionExecutor.getSeriesPartitionSlot(queryParam.getDeviceID()),
            sharedTTimeSlotList);
      }
      partitionSlotsMap.put(entry.getKey(), deviceToTimePartitionMap);
    }
    return new TDataPartitionReq(partitionSlotsMap);
  }

  private SchemaPartition parseSchemaPartitionTableResp(
      final TSchemaPartitionTableResp schemaPartitionTableResp) {
    final Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> regionReplicaMap =
        new HashMap<>();
    for (final Map.Entry<String, Map<TSeriesPartitionSlot, TConsensusGroupId>> entry1 :
        schemaPartitionTableResp.getSchemaPartitionTable().entrySet()) {
      String database = entry1.getKey();
      final Map<TSeriesPartitionSlot, TRegionReplicaSet> result1 =
          regionReplicaMap.computeIfAbsent(database, k -> new HashMap<>());

      Map<TSeriesPartitionSlot, TConsensusGroupId> orderedMap =
          new LinkedHashMap<>(entry1.getValue());
      List<TConsensusGroupId> orderedGroupIds = new ArrayList<>(orderedMap.values());
      List<TRegionReplicaSet> regionReplicaSets =
          partitionCache.getRegionReplicaSet(orderedGroupIds);

      int index = 0;
      for (Map.Entry<TSeriesPartitionSlot, TConsensusGroupId> entry2 : orderedMap.entrySet()) {
        result1.put(entry2.getKey(), regionReplicaSets.get(index++));
      }
    }
    return new SchemaPartition(
        regionReplicaMap,
        IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionExecutorClass(),
        IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionSlotNum());
  }

  private SchemaNodeManagementPartition parseSchemaNodeManagementPartitionResp(
      final TSchemaNodeManagementResp schemaNodeManagementResp) {
    return new SchemaNodeManagementPartition(
        schemaNodeManagementResp.getSchemaRegionMap(),
        IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionExecutorClass(),
        IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionSlotNum(),
        schemaNodeManagementResp.getMatchedNode());
  }

  private DataPartition parseDataPartitionResp(
      final TDataPartitionTableResp dataPartitionTableResp) {
    final Set<TConsensusGroupId> uniqueConsensusGroupIds = new HashSet<>();
    for (final Map<
            String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>>>
        partitionTable : Collections.singleton(dataPartitionTableResp.getDataPartitionTable())) {
      for (final Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>>
          seriesPartitionMap : partitionTable.values()) {
        for (final Map<TTimePartitionSlot, List<TConsensusGroupId>> timePartitionMap :
            seriesPartitionMap.values()) {
          for (final List<TConsensusGroupId> consensusGroupIds : timePartitionMap.values()) {
            uniqueConsensusGroupIds.addAll(consensusGroupIds);
          }
        }
      }
    }

    final List<TRegionReplicaSet> allRegionReplicaSets =
        partitionCache.getRegionReplicaSet(new ArrayList<>(uniqueConsensusGroupIds));
    final List<TConsensusGroupId> consensusGroupIds = new ArrayList<>(uniqueConsensusGroupIds);
    final Map<TConsensusGroupId, TRegionReplicaSet> regionReplicaSetMap = new HashMap<>();
    for (int i = 0; i < allRegionReplicaSets.size(); i++) {
      regionReplicaSetMap.put(consensusGroupIds.get(i), allRegionReplicaSets.get(i));
    }

    final Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
        regionReplicaSet = new HashMap<>();
    for (final Map.Entry<
            String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>>>
        entry1 : dataPartitionTableResp.getDataPartitionTable().entrySet()) {
      final Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>> result1 =
          regionReplicaSet.computeIfAbsent(entry1.getKey(), k -> new HashMap<>());
      for (final Map.Entry<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>>
          entry2 : entry1.getValue().entrySet()) {
        final Map<TTimePartitionSlot, List<TRegionReplicaSet>> result2 =
            result1.computeIfAbsent(entry2.getKey(), k -> new HashMap<>());
        for (final Map.Entry<TTimePartitionSlot, List<TConsensusGroupId>> entry3 :
            entry2.getValue().entrySet()) {
          final List<TRegionReplicaSet> regionReplicaSets = new ArrayList<>();
          for (TConsensusGroupId groupId : entry3.getValue()) {
            regionReplicaSets.add(regionReplicaSetMap.get(groupId));
          }
          result2.put(entry3.getKey(), regionReplicaSets);
        }
      }
    }

    return new DataPartition(
        regionReplicaSet,
        config.getSeriesPartitionExecutorClass(),
        config.getSeriesPartitionSlotNum());
  }
}
