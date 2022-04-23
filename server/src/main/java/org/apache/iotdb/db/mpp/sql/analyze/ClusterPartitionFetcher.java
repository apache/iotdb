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
package org.apache.iotdb.db.mpp.sql.analyze;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.exception.BadNodeUrlException;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.partition.executor.SeriesPartitionExecutor;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionResp;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionResp;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class ClusterPartitionFetcher implements IPartitionFetcher {
  private static final Logger logger = LoggerFactory.getLogger(ClusterPartitionFetcher.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private final ConfigNodeClient client;

  private SeriesPartitionExecutor partitionExecutor;

  private PartitionCache partitionCache;

  private static final class ClusterPartitionFetcherHolder {
    private static final ClusterPartitionFetcher INSTANCE = new ClusterPartitionFetcher();

    private ClusterPartitionFetcherHolder() {}
  }

  public static ClusterPartitionFetcher getInstance() {
    return ClusterPartitionFetcherHolder.INSTANCE;
  }

  private ClusterPartitionFetcher() {
    try {
      client = new ConfigNodeClient();
    } catch (IoTDBConnectionException | BadNodeUrlException e) {
      throw new StatementAnalyzeException("Couldn't connect config node");
    }
    initPartitionExecutor();
  }

  private void initPartitionExecutor() {
    // TODO: @crz move to node-commons
    try {
      Class<?> executor = Class.forName(config.getSeriesPartitionExecutorClass());
      Constructor<?> executorConstructor = executor.getConstructor(int.class);
      this.partitionExecutor =
          (SeriesPartitionExecutor)
              executorConstructor.newInstance(config.getSeriesPartitionSlotNum());
      this.partitionCache =
          new PartitionCache(
              config.getSeriesPartitionExecutorClass(), config.getSeriesPartitionSlotNum());
    } catch (ClassNotFoundException
        | NoSuchMethodException
        | InstantiationException
        | IllegalAccessException
        | InvocationTargetException e) {
      throw new StatementAnalyzeException(
          String.format(
              "Couldn't Constructor SeriesPartitionExecutor class: %s",
              config.getSeriesPartitionExecutorClass()));
    }
  }

  @Override
  public SchemaPartition getSchemaPartition(PathPatternTree patternTree) {
    try {
      SchemaPartition schemaPartition = partitionCache.getSchemaPartition(patternTree);
      if (null == schemaPartition) {
        TSchemaPartitionResp schemaPartitionResp =
            client.getSchemaPartition(constructSchemaPartitionReq(patternTree));
        if (schemaPartitionResp.getStatus().getCode()
            == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          schemaPartition = parseSchemaPartitionResp(schemaPartitionResp);
          partitionCache.updateSchemaPartitionCache(
              patternTree.findAllDevicePaths(), schemaPartition);
        }
      }
      return schemaPartition;
    } catch (IoTDBConnectionException e) {
      throw new StatementAnalyzeException("An error occurred when executing getSchemaPartition()");
    }
  }

  @Override
  public SchemaPartition getOrCreateSchemaPartition(PathPatternTree patternTree) {
    try {
      SchemaPartition schemaPartition = partitionCache.getSchemaPartition(patternTree);
      if (null == schemaPartition) {
        TSchemaPartitionResp schemaPartitionResp =
            client.getOrCreateSchemaPartition(constructSchemaPartitionReq(patternTree));
        if (schemaPartitionResp.getStatus().getCode()
            == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          schemaPartition = parseSchemaPartitionResp(schemaPartitionResp);
          partitionCache.updateSchemaPartitionCache(
              patternTree.findAllDevicePaths(), schemaPartition);
        }
      }
      return schemaPartition;
    } catch (IoTDBConnectionException e) {
      throw new StatementAnalyzeException(
          "An error occurred when executing getOrCreateSchemaPartition()");
    }
  }

  @Override
  public DataPartition getDataPartition(
      Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap) {
    try {
      DataPartition dataPartition = partitionCache.getDataPartition(sgNameToQueryParamsMap);
      if (null == dataPartition) {
        TDataPartitionResp dataPartitionResp =
            client.getDataPartition(constructDataPartitionReq(sgNameToQueryParamsMap));
        if (dataPartitionResp.getStatus().getCode()
            == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          dataPartition = parseDataPartitionResp(dataPartitionResp);
          partitionCache.updateDataPartitionCache(dataPartition);
        }
      }
      return dataPartition;
    } catch (IoTDBConnectionException e) {
      throw new StatementAnalyzeException("An error occurred when executing getDataPartition()");
    }
  }

  @Override
  public DataPartition getOrCreateDataPartition(
      Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap) {
    try {
      DataPartition dataPartition = partitionCache.getDataPartition(sgNameToQueryParamsMap);
      if (null == dataPartition) {
        TDataPartitionResp dataPartitionResp =
            client.getOrCreateDataPartition(constructDataPartitionReq(sgNameToQueryParamsMap));
        if (dataPartitionResp.getStatus().getCode()
            == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          dataPartition = parseDataPartitionResp(dataPartitionResp);
          partitionCache.updateDataPartitionCache(dataPartition);
        }
      }
      return dataPartition;
    } catch (IoTDBConnectionException e) {
      throw new StatementAnalyzeException(
          "An error occurred when executing getOrCreateDataPartition()");
    }
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
        deviceToTimePartitionMap.put(
            new TSeriesPartitionSlot(
                partitionExecutor.getSeriesPartitionSlot(queryParam.getDevicePath()).getSlotId()),
            queryParam.getTimePartitionSlotList().stream()
                .map(timePartitionSlot -> new TTimePartitionSlot(timePartitionSlot.getStartTime()))
                .collect(Collectors.toList()));
      }
      partitionSlotsMap.put(entry.getKey(), deviceToTimePartitionMap);
    }
    return new TDataPartitionReq(partitionSlotsMap);
  }

  private SchemaPartition parseSchemaPartitionResp(TSchemaPartitionResp schemaPartitionResp) {
    Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> schemaPartitionMap = new HashMap<>();
    for (Map.Entry<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> sgEntry :
        schemaPartitionResp.getSchemaRegionMap().entrySet()) {
      // for each sg
      String storageGroupName = sgEntry.getKey();
      Map<TSeriesPartitionSlot, TRegionReplicaSet> deviceToSchemaRegionMap = new HashMap<>();
      for (Map.Entry<TSeriesPartitionSlot, TRegionReplicaSet> deviceEntry :
          sgEntry.getValue().entrySet()) {
        deviceToSchemaRegionMap.put(
            new TSeriesPartitionSlot(deviceEntry.getKey().getSlotId()),
            new TRegionReplicaSet(deviceEntry.getValue()));
      }
      schemaPartitionMap.put(storageGroupName, deviceToSchemaRegionMap);
    }
    return new SchemaPartition(
        schemaPartitionMap,
        config.getSeriesPartitionExecutorClass(),
        config.getSeriesPartitionSlotNum());
  }

  private DataPartition parseDataPartitionResp(TDataPartitionResp dataPartitionResp) {
    Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
        dataPartitionMap = new HashMap<>();
    for (Map.Entry<
            String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
        sgEntry : dataPartitionResp.getDataPartitionMap().entrySet()) {
      // for each sg
      String storageGroupName = sgEntry.getKey();
      Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>
          respDeviceToRegionsMap = sgEntry.getValue();
      Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>
          deviceToRegionsMap = new HashMap<>();
      for (Map.Entry<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>
          deviceEntry : respDeviceToRegionsMap.entrySet()) {
        // for each device
        Map<TTimePartitionSlot, List<TRegionReplicaSet>> timePartitionToRegionsMap =
            new HashMap<>();
        for (Map.Entry<TTimePartitionSlot, List<TRegionReplicaSet>> timePartitionEntry :
            deviceEntry.getValue().entrySet()) {
          // for each time partition
          timePartitionToRegionsMap.put(
              new TTimePartitionSlot(timePartitionEntry.getKey().getStartTime()),
              timePartitionEntry.getValue().stream()
                  .map(TRegionReplicaSet::new)
                  .collect(Collectors.toList()));
        }
        deviceToRegionsMap.put(
            new TSeriesPartitionSlot(deviceEntry.getKey().getSlotId()), timePartitionToRegionsMap);
      }
      dataPartitionMap.put(storageGroupName, deviceToRegionsMap);
    }
    return new DataPartition(
        dataPartitionMap,
        config.getSeriesPartitionExecutorClass(),
        config.getSeriesPartitionSlotNum());
  }

  private class PartitionCache {
    /** the size of partitionCache */
    private final int cacheSize = config.getPartitionCacheSize();
    /** device -> storage_group, tRegionReplicaSet */
    private final Cache<String, SchemaPartitionCacheValue> schemaPartitionCache;
    /** storage_group, tSeriesPartitionSlot, tTimesereisPartitionSlot -> TRegionReplicaSets * */
    private final Cache<DataPartitionCacheKey, List<TRegionReplicaSet>> dataPartitionCache;
    /** calculate slotId by device */
    private final String seriesSlotExecutorName;

    private final int seriesPartitionSlotNum;

    public PartitionCache(String seriesSlotExecutorName, int seriesPartitionSlotNum) {
      this.seriesSlotExecutorName = seriesSlotExecutorName;
      this.seriesPartitionSlotNum = seriesPartitionSlotNum;
      this.schemaPartitionCache = Caffeine.newBuilder().maximumSize(cacheSize).build();
      this.dataPartitionCache = Caffeine.newBuilder().maximumSize(cacheSize).build();
    }

    /** get schemaPartition by patternTree */
    public SchemaPartition getSchemaPartition(PathPatternTree patternTree) {
      Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> schemaPartitionMap =
          new HashMap<>();
      // check cache for each device
      for (String device : patternTree.findAllDevicePaths()) {
        TSeriesPartitionSlot seriesPartitionSlot = partitionExecutor.getSeriesPartitionSlot(device);
        SchemaPartitionCacheValue schemaPartitionCacheValue =
            schemaPartitionCache.getIfPresent(device);
        if (null == schemaPartitionCacheValue) {
          // if one device not find, then return cache miss.
          return null;
        }
        String storageGroupName = schemaPartitionCacheValue.getStorageGroup();
        Map<TSeriesPartitionSlot, TRegionReplicaSet> regionReplicaSetMap =
            schemaPartitionMap.getOrDefault(storageGroupName, new HashMap<>());
        regionReplicaSetMap.put(
            seriesPartitionSlot, schemaPartitionCacheValue.getRegionReplicaSet());
      }
      // cache hit
      return new SchemaPartition(
          schemaPartitionMap, seriesSlotExecutorName, seriesPartitionSlotNum);
    }

    /** get dataPartition by query param map */
    public DataPartition getDataPartition(
        Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap) {
      Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
          dataPartitionMap = new HashMap<>();
      // check cache for each storage group
      for (Map.Entry<String, List<DataPartitionQueryParam>> entry :
          sgNameToQueryParamsMap.entrySet()) {
        String storageGroupName = entry.getKey();
        Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>
            seriesPartitionSlotMapMap =
                dataPartitionMap.getOrDefault(storageGroupName, new HashMap<>());
        // check cache for each query param
        for (DataPartitionQueryParam dataPartitionQueryParam : entry.getValue()) {
          TSeriesPartitionSlot seriesPartitionSlot =
              partitionExecutor.getSeriesPartitionSlot(dataPartitionQueryParam.getDevicePath());
          Map<TTimePartitionSlot, List<TRegionReplicaSet>> timePartitionSlotListMap =
              seriesPartitionSlotMapMap.getOrDefault(seriesPartitionSlot, new HashMap<>());
          // check cache for each time partition
          for (TTimePartitionSlot timePartitionSlot :
              dataPartitionQueryParam.getTimePartitionSlotList()) {
            DataPartitionCacheKey dataPartitionCacheKey =
                new DataPartitionCacheKey(storageGroupName, seriesPartitionSlot, timePartitionSlot);
            List<TRegionReplicaSet> regionReplicaSets =
                dataPartitionCache.getIfPresent(dataPartitionCacheKey);
            if (null == regionReplicaSets) {
              // if one time partition not find, cache miss
              return null;
            }
            timePartitionSlotListMap.put(timePartitionSlot, regionReplicaSets);
          }
        }
      }
      // cache hit
      return new DataPartition(dataPartitionMap, seriesSlotExecutorName, seriesPartitionSlotNum);
    }

    /** update schemaPartitionCache by schemaPartition. */
    public void updateSchemaPartitionCache(List<String> devices, SchemaPartition schemaPartition) {
      Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> storageGroupPartitionMap =
          schemaPartition.getSchemaPartitionMap();
      Set<String> storageGroupNames = storageGroupPartitionMap.keySet();
      for (String device : devices) {
        String storageGroup = null;
        for (String storageGroupName : storageGroupNames) {
          if (device.startsWith(storageGroupName)) {
            storageGroup = storageGroupName;
            break;
          }
        }
        if (null == storageGroup) {
          logger.error(
              "Failed to get the storage group of {} when update SchemaPartitionCache", device);
          continue;
        }
        TSeriesPartitionSlot seriesPartitionSlot = partitionExecutor.getSeriesPartitionSlot(device);
        TRegionReplicaSet regionReplicaSet =
            storageGroupPartitionMap.get(storageGroup).getOrDefault(seriesPartitionSlot, null);
        if (null == regionReplicaSet) {
          logger.error(
              "Failed to get the regionReplicaSet of {} when update SchemaPartitionCache", device);
          continue;
        }
        SchemaPartitionCacheValue schemaPartitionCacheValue =
            new SchemaPartitionCacheValue(storageGroup, regionReplicaSet);
        schemaPartitionCache.put(device, schemaPartitionCacheValue);
      }
    }

    /** update dataPartitionCache by dataPartition */
    public void updateDataPartitionCache(DataPartition dataPartition) {
      for (Map.Entry<
              String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
          entry1 : dataPartition.getDataPartitionMap().entrySet()) {
        String storageGroup = entry1.getKey();
        for (Map.Entry<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>
            entry2 : entry1.getValue().entrySet()) {
          TSeriesPartitionSlot seriesPartitionSlot = entry2.getKey();
          for (Map.Entry<TTimePartitionSlot, List<TRegionReplicaSet>> entry3 :
              entry2.getValue().entrySet()) {
            DataPartitionCacheKey dataPartitionCacheKey =
                new DataPartitionCacheKey(storageGroup, seriesPartitionSlot, entry3.getKey());
            dataPartitionCache.put(dataPartitionCacheKey, entry3.getValue());
          }
        }
      }
    }

    /**
     * invalid schemaPartitionCache by device TODO this method should be called in two situation: 1.
     * redirect status 2. config node trigger
     */
    public void invalidSchemaPartitionCache(String device) {
      schemaPartitionCache.invalidate(device);
    }

    /**
     * invalid dataPartitionCache by sg, seriesPartitionSlot, timePartitionSlot TODO this method
     * should be called in two situation: 1. redirect status 2. config node trigger
     */
    public void invalidDataPartitionCache(
        String storageGroup,
        TSeriesPartitionSlot seriesPartitionSlot,
        TTimePartitionSlot timePartitionSlot) {
      DataPartitionCacheKey dataPartitionCacheKey =
          new DataPartitionCacheKey(storageGroup, seriesPartitionSlot, timePartitionSlot);
      dataPartitionCache.invalidate(dataPartitionCacheKey);
    }
  }

  private class SchemaPartitionCacheValue {
    private String storageGroup;
    private TRegionReplicaSet regionReplicaSet;

    public SchemaPartitionCacheValue(String storageGroup, TRegionReplicaSet regionReplicaSet) {
      this.storageGroup = storageGroup;
      this.regionReplicaSet = regionReplicaSet;
    }

    public String getStorageGroup() {
      return storageGroup;
    }

    public TRegionReplicaSet getRegionReplicaSet() {
      return regionReplicaSet;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SchemaPartitionCacheValue that = (SchemaPartitionCacheValue) o;
      return Objects.equals(storageGroup, that.storageGroup)
          && Objects.equals(regionReplicaSet, that.regionReplicaSet);
    }

    @Override
    public int hashCode() {
      return Objects.hash(storageGroup, regionReplicaSet);
    }
  }

  private class DataPartitionCacheKey {
    private String storageGroup;
    private TSeriesPartitionSlot seriesPartitionSlot;
    private TTimePartitionSlot timePartitionSlot;

    public DataPartitionCacheKey(
        String storageGroup,
        TSeriesPartitionSlot seriesPartitionSlot,
        TTimePartitionSlot timePartitionSlot) {
      this.storageGroup = storageGroup;
      this.seriesPartitionSlot = seriesPartitionSlot;
      this.timePartitionSlot = timePartitionSlot;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      DataPartitionCacheKey that = (DataPartitionCacheKey) o;
      return Objects.equals(storageGroup, that.storageGroup)
          && Objects.equals(seriesPartitionSlot, that.seriesPartitionSlot)
          && Objects.equals(timePartitionSlot, that.timePartitionSlot);
    }

    @Override
    public int hashCode() {
      return Objects.hash(storageGroup, seriesPartitionSlot, timePartitionSlot);
    }
  }
}
