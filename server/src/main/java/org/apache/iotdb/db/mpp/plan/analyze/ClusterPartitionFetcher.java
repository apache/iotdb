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

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.consensus.PartitionRegionId;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.partition.SchemaNodeManagementPartition;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.partition.executor.SeriesPartitionExecutor;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.PathUtils;
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
import org.apache.iotdb.db.service.metrics.recorder.CacheMetricsRecorder;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class ClusterPartitionFetcher implements IPartitionFetcher {
  private static final Logger logger = LoggerFactory.getLogger(ClusterPartitionFetcher.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final List<String> ROOT_PATH = Arrays.asList("root", "**");

  private final SeriesPartitionExecutor partitionExecutor;

  private final PartitionCache partitionCache;

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
            config.getSeriesPartitionExecutorClass(), config.getSeriesPartitionSlotNum());
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
          partitionCache.updateSchemaPartitionCache(devicePaths, schemaPartition);
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
          partitionCache.updateSchemaPartitionCache(devicePaths, schemaPartition);
        } else {
          throw new RuntimeException(
              new IoTDBException(
                  schemaPartitionResp.getStatus().getMessage(),
                  schemaPartitionResp.getStatus().getCode()));
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
          partitionCache.updateDataPartitionCache(dataPartition);
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
          partitionCache.updateDataPartitionCache(dataPartition);
        }
      }
      return dataPartition;
    } catch (TException | IOException e) {
      throw new StatementAnalyzeException(
          "An error occurred when executing getOrCreateDataPartition():" + e.getMessage());
    }
  }

  @Override
  public void invalidAllCache() {
    logger.debug("Invalidate partition cache");
    partitionCache.storageGroupCache.clear();
    partitionCache.invalidAllDataPartitionCache();
    partitionCache.invalidAllSchemaPartitionCache();
    logger.debug("PartitionCache is invalid:{}", partitionCache);
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
      try (ConfigNodeClient client =
          configNodeClientManager.borrowClient(ConfigNodeInfo.partitionRegionId)) {
        TStorageGroupSchemaResp storageGroupSchemaResp =
            client.getMatchedStorageGroupSchemas(ROOT_PATH);
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
            Set<String> successFullyCreatedStorageGroup = new HashSet<>();
            for (String storageGroupName : storageGroupNamesNeedCreated) {
              TStorageGroupSchema storageGroupSchema = new TStorageGroupSchema();
              storageGroupSchema.setName(storageGroupName);
              TSetStorageGroupReq req = new TSetStorageGroupReq(storageGroupSchema);
              TSStatus tsStatus = client.setStorageGroup(req);
              if (TSStatusCode.SUCCESS_STATUS.getStatusCode() == tsStatus.getCode()) {
                successFullyCreatedStorageGroup.add(storageGroupName);
              } else {
                partitionCache.updateStorageCache(successFullyCreatedStorageGroup);
                throw new RuntimeException(new IoTDBException(tsStatus.message, tsStatus.code));
              }
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
    return new SchemaPartition(
        schemaPartitionResp.getSchemaRegionMap(),
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
    return new DataPartition(
        dataPartitionResp.getDataPartitionMap(),
        config.getSeriesPartitionExecutorClass(),
        config.getSeriesPartitionSlotNum());
  }

  private class PartitionCache {
    /** the size of partitionCache */
    private final int cacheSize = config.getPartitionCacheSize();
    /** the cache of storage group */
    private final Set<String> storageGroupCache = Collections.synchronizedSet(new HashSet<>());
    /** the lock of storage group cache */
    private final ReentrantReadWriteLock storageGroupCacheLock = new ReentrantReadWriteLock();
    /** device -> regionReplicaSet */
    private final Cache<String, TRegionReplicaSet> schemaPartitionCache;
    /** the lock of schemaPartition cache * */
    private final ReentrantReadWriteLock schemaPartitionCacheLock = new ReentrantReadWriteLock();
    /** seriesPartitionSlot, timesereisPartitionSlot -> regionReplicaSets * */
    private final Cache<DataPartitionCacheKey, List<TRegionReplicaSet>> dataPartitionCache;
    /** the lock of dataPartition cache */
    private final ReentrantReadWriteLock dataPartitionCacheLock = new ReentrantReadWriteLock();
    /** calculate slotId by device */
    private final String seriesSlotExecutorName;

    private final int seriesPartitionSlotNum;

    public PartitionCache(String seriesSlotExecutorName, int seriesPartitionSlotNum) {
      this.seriesSlotExecutorName = seriesSlotExecutorName;
      this.seriesPartitionSlotNum = seriesPartitionSlotNum;
      this.schemaPartitionCache = Caffeine.newBuilder().maximumSize(cacheSize).build();
      this.dataPartitionCache = Caffeine.newBuilder().maximumSize(cacheSize).build();
    }

    /** get storage group by cache */
    public boolean getStorageGroup(
        List<String> devicePaths, Map<String, String> deviceToStorageGroupMap) {
      storageGroupCacheLock.readLock().lock();
      try {
        boolean result = true;
        if (storageGroupCache.size() == 0) {
          logger.debug("Failed to get storage group");
          result = false;
        } else {
          for (String devicePath : devicePaths) {
            boolean hit = false;
            synchronized (storageGroupCache) {
              for (String storageGroup : storageGroupCache) {
                if (PathUtils.isStartWith(devicePath, storageGroup)) {
                  deviceToStorageGroupMap.put(devicePath, storageGroup);
                  hit = true;
                  break;
                }
              }
            }
            if (!hit) {
              logger.debug("{} cannot hit storage group cache", devicePath);
              result = false;
              break;
            }
          }
        }
        CacheMetricsRecorder.record(result, "StorageGroup");
        return result;
      } finally {
        storageGroupCacheLock.readLock().unlock();
      }
    }

    /** update the cache of storage group */
    public void updateStorageCache(Set<String> storageGroupNames) {
      storageGroupCacheLock.writeLock().lock();
      try {
        storageGroupCache.addAll(storageGroupNames);
      } finally {
        storageGroupCacheLock.writeLock().unlock();
      }
    }

    /** invalid storage group after delete */
    public void invalidStorageGroupCache(List<String> storageGroupNames) {
      storageGroupCacheLock.writeLock().lock();
      try {
        for (String storageGroupName : storageGroupNames) {
          storageGroupCache.remove(storageGroupName);
        }
      } finally {
        storageGroupCacheLock.writeLock().unlock();
      }
    }

    /** get schemaPartition by patternTree */
    public SchemaPartition getSchemaPartition(Map<String, String> deviceToStorageGroupMap) {
      schemaPartitionCacheLock.readLock().lock();
      try {
        String name = "SchemaPartition";
        if (deviceToStorageGroupMap.size() == 0) {
          CacheMetricsRecorder.record(false, name);
          return null;
        }
        Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> schemaPartitionMap =
            new HashMap<>();
        // check cache for each device
        for (Map.Entry<String, String> entry : deviceToStorageGroupMap.entrySet()) {
          String device = entry.getKey();
          TSeriesPartitionSlot seriesPartitionSlot =
              partitionExecutor.getSeriesPartitionSlot(device);
          TRegionReplicaSet regionReplicaSet = schemaPartitionCache.getIfPresent(device);
          if (null == regionReplicaSet) {
            // if one device not find, then return cache miss.
            logger.debug("Failed to find schema partition");
            CacheMetricsRecorder.record(false, name);
            return null;
          }
          String storageGroupName = deviceToStorageGroupMap.get(device);
          if (!schemaPartitionMap.containsKey(storageGroupName)) {
            schemaPartitionMap.put(storageGroupName, new HashMap<>());
          }
          Map<TSeriesPartitionSlot, TRegionReplicaSet> regionReplicaSetMap =
              schemaPartitionMap.get(storageGroupName);
          regionReplicaSetMap.put(seriesPartitionSlot, regionReplicaSet);
        }
        logger.debug("Hit schema partition");
        // cache hit
        CacheMetricsRecorder.record(true, name);
        return new SchemaPartition(
            schemaPartitionMap, seriesSlotExecutorName, seriesPartitionSlotNum);
      } finally {
        schemaPartitionCacheLock.readLock().unlock();
      }
    }

    /** get dataPartition by query param map */
    public DataPartition getDataPartition(
        Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap) {
      dataPartitionCacheLock.readLock().lock();
      try {
        String name = "DataPartition";
        if (sgNameToQueryParamsMap.size() == 0) {
          CacheMetricsRecorder.record(false, name);
          return null;
        }
        Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
            dataPartitionMap = new HashMap<>();
        // check cache for each storage group
        for (Map.Entry<String, List<DataPartitionQueryParam>> entry :
            sgNameToQueryParamsMap.entrySet()) {
          String storageGroupName = entry.getKey();
          if (!dataPartitionMap.containsKey(storageGroupName)) {
            dataPartitionMap.put(storageGroupName, new HashMap<>());
          }
          Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>
              seriesSlotToTimePartitionMap = dataPartitionMap.get(storageGroupName);
          // check cache for each query param
          for (DataPartitionQueryParam dataPartitionQueryParam : entry.getValue()) {
            if (null == dataPartitionQueryParam.getTimePartitionSlotList()
                || 0 == dataPartitionQueryParam.getTimePartitionSlotList().size()) {
              // if query all data, cache miss
              logger.debug("Failed to find data partition");
              CacheMetricsRecorder.record(false, name);
              return null;
            }
            TSeriesPartitionSlot seriesPartitionSlot =
                partitionExecutor.getSeriesPartitionSlot(dataPartitionQueryParam.getDevicePath());
            if (!seriesSlotToTimePartitionMap.containsKey(seriesPartitionSlot)) {
              seriesSlotToTimePartitionMap.put(seriesPartitionSlot, new HashMap<>());
            }
            Map<TTimePartitionSlot, List<TRegionReplicaSet>> timePartitionSlotListMap =
                seriesSlotToTimePartitionMap.get(seriesPartitionSlot);
            // check cache for each time partition
            for (TTimePartitionSlot timePartitionSlot :
                dataPartitionQueryParam.getTimePartitionSlotList()) {
              DataPartitionCacheKey dataPartitionCacheKey =
                  new DataPartitionCacheKey(seriesPartitionSlot, timePartitionSlot);
              List<TRegionReplicaSet> regionReplicaSets =
                  dataPartitionCache.getIfPresent(dataPartitionCacheKey);
              if (null == regionReplicaSets) {
                // if one time partition not find, cache miss
                logger.debug("Failed to find data partition");
                CacheMetricsRecorder.record(false, name);
                return null;
              }
              timePartitionSlotListMap.put(timePartitionSlot, regionReplicaSets);
            }
          }
        }
        logger.debug("Hit data partition");
        // cache hit
        CacheMetricsRecorder.record(true, name);
        return new DataPartition(dataPartitionMap, seriesSlotExecutorName, seriesPartitionSlotNum);
      } finally {
        dataPartitionCacheLock.readLock().unlock();
      }
    }

    /** update schemaPartitionCache by schemaPartition. */
    public void updateSchemaPartitionCache(List<String> devices, SchemaPartition schemaPartition) {
      schemaPartitionCacheLock.writeLock().lock();
      try {
        Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> storageGroupPartitionMap =
            schemaPartition.getSchemaPartitionMap();
        Set<String> storageGroupNames = storageGroupPartitionMap.keySet();
        for (String device : devices) {
          if (!device.contains("*")) {
            String storageGroup = null;
            for (String storageGroupName : storageGroupNames) {
              if (PathUtils.isStartWith(device, storageGroupName)) {
                storageGroup = storageGroupName;
                break;
              }
            }
            if (null == storageGroup) {
              // device not exist
              continue;
            }
            TSeriesPartitionSlot seriesPartitionSlot =
                partitionExecutor.getSeriesPartitionSlot(device);
            TRegionReplicaSet regionReplicaSet =
                storageGroupPartitionMap.get(storageGroup).getOrDefault(seriesPartitionSlot, null);
            if (null == regionReplicaSet) {
              logger.error(
                  "Failed to get the regionReplicaSet of {} when update SchemaPartitionCache",
                  device);
              continue;
            }
            schemaPartitionCache.put(device, regionReplicaSet);
          }
        }
      } finally {
        schemaPartitionCacheLock.writeLock().unlock();
      }
    }

    /** update dataPartitionCache by dataPartition */
    public void updateDataPartitionCache(DataPartition dataPartition) {
      dataPartitionCacheLock.writeLock().lock();
      try {
        for (Map.Entry<
                String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
            entry1 : dataPartition.getDataPartitionMap().entrySet()) {
          for (Map.Entry<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>
              entry2 : entry1.getValue().entrySet()) {
            TSeriesPartitionSlot seriesPartitionSlot = entry2.getKey();
            for (Map.Entry<TTimePartitionSlot, List<TRegionReplicaSet>> entry3 :
                entry2.getValue().entrySet()) {
              DataPartitionCacheKey dataPartitionCacheKey =
                  new DataPartitionCacheKey(seriesPartitionSlot, entry3.getKey());
              dataPartitionCache.put(dataPartitionCacheKey, entry3.getValue());
            }
          }
        }
      } finally {
        dataPartitionCacheLock.writeLock().unlock();
      }
    }

    /** invalid schemaPartitionCache by device */
    public void invalidSchemaPartitionCache(String device) {
      // TODO should be called in two situation: 1. redirect status 2. config node trigger
      schemaPartitionCacheLock.writeLock().lock();
      try {
        schemaPartitionCache.invalidate(device);
      } finally {
        schemaPartitionCacheLock.writeLock().unlock();
      }
    }

    /** invalid dataPartitionCache by seriesPartitionSlot, timePartitionSlot */
    public void invalidDataPartitionCache(
        TSeriesPartitionSlot seriesPartitionSlot, TTimePartitionSlot timePartitionSlot) {
      dataPartitionCacheLock.writeLock().lock();
      // TODO should be called in two situation: 1. redirect status 2. config node trigger
      try {
        DataPartitionCacheKey dataPartitionCacheKey =
            new DataPartitionCacheKey(seriesPartitionSlot, timePartitionSlot);
        dataPartitionCache.invalidate(dataPartitionCacheKey);
      } finally {
        dataPartitionCacheLock.writeLock().unlock();
      }
    }

    /** invalid schemaPartitionCache by device */
    public void invalidAllSchemaPartitionCache() {
      schemaPartitionCacheLock.writeLock().lock();
      try {
        schemaPartitionCache.invalidateAll();
      } finally {
        schemaPartitionCacheLock.writeLock().unlock();
      }
    }

    /** invalid dataPartitionCache by seriesPartitionSlot, timePartitionSlot */
    public void invalidAllDataPartitionCache() {
      dataPartitionCacheLock.writeLock().lock();
      try {
        dataPartitionCache.invalidateAll();
      } finally {
        dataPartitionCacheLock.writeLock().unlock();
      }
    }

    @Override
    public String toString() {
      return "PartitionCache{"
          + "cacheSize="
          + cacheSize
          + ", storageGroupCache="
          + storageGroupCache
          + ", schemaPartitionCache="
          + schemaPartitionCache
          + ", dataPartitionCache="
          + dataPartitionCache
          + '}';
    }
  }

  private static class DataPartitionCacheKey {
    private TSeriesPartitionSlot seriesPartitionSlot;
    private TTimePartitionSlot timePartitionSlot;

    public DataPartitionCacheKey(
        TSeriesPartitionSlot seriesPartitionSlot, TTimePartitionSlot timePartitionSlot) {
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
      return Objects.equals(seriesPartitionSlot, that.seriesPartitionSlot)
          && Objects.equals(timePartitionSlot, that.timePartitionSlot);
    }

    @Override
    public int hashCode() {
      return Objects.hash(seriesPartitionSlot, timePartitionSlot);
    }
  }
}
