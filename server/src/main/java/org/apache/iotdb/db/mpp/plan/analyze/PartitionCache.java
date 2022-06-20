package org.apache.iotdb.db.mpp.plan.analyze;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TRegionCache;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.consensus.PartitionRegionId;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.partition.executor.SeriesPartitionExecutor;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.confignode.rpc.thrift.TRegionCacheResp;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.client.ConfigNodeInfo;
import org.apache.iotdb.db.client.DataNodeClientPoolFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.db.service.metrics.recorder.CacheMetricsRecorder;
import org.apache.iotdb.rpc.TSStatusCode;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class PartitionCache {
  private static final Logger logger = LoggerFactory.getLogger(PartitionCache.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final String STORAGE_GROUP_CACHE_NAME = "StorageGroupCache";
  private static final String SCHEMA_PARTITION_CACHE_NAME = "SchemaPartitionCache";
  private static final String DATA_PARTITION_CACHE_NAME = "DataPartitionCache";
  private static final String REGION_CACHE_NAME = "RegionCache";

  private final SeriesPartitionExecutor partitionExecutor;

  private final IClientManager<PartitionRegionId, ConfigNodeClient> configNodeClientManager =
      new IClientManager.Factory<PartitionRegionId, ConfigNodeClient>()
          .createClientManager(new DataNodeClientPoolFactory.ConfigNodeClientPoolFactory());

  /** the size of partitionCache */
  private final int cacheSize = config.getPartitionCacheSize();

  /** the cache of storage group */
  private final Set<String> storageGroupCache = Collections.synchronizedSet(new HashSet<>());
  /** device -> ConsensusGroupId */
  private final Cache<String, TConsensusGroupId> schemaPartitionCache;
  /** SeriesPartitionSlot, TimesereisPartitionSlot -> ConsensusGroupId * */
  private final Cache<DataPartitionCacheKey, List<TConsensusGroupId>> dataPartitionCache;
  /** ConsensusGroupId -> RegionReplicaSets */
  private final Cache<TConsensusGroupId, TRegionReplicaSet> regionCache;

  private final AtomicLong regionCacheUpdateTime = new AtomicLong(0);
  /** calculate slotId by device */
  private final String seriesSlotExecutorName;

  private final int seriesPartitionSlotNum;

  public PartitionCache(
      SeriesPartitionExecutor partitionExecutor,
      String seriesSlotExecutorName,
      int seriesPartitionSlotNum) {
    this.partitionExecutor = partitionExecutor;
    this.seriesSlotExecutorName = seriesSlotExecutorName;
    this.seriesPartitionSlotNum = seriesPartitionSlotNum;
    this.schemaPartitionCache = Caffeine.newBuilder().maximumSize(cacheSize).build();
    this.dataPartitionCache = Caffeine.newBuilder().maximumSize(cacheSize).build();
    this.regionCache = Caffeine.newBuilder().maximumSize(cacheSize).build();
  }

  /** get storage group by cache */
  public boolean getStorageGroup(
      List<String> devicePaths, Map<String, String> deviceToStorageGroupMap) {
    boolean result = true;
    if (storageGroupCache.size() == 0) {
      logger.debug("Storage group cache is empty.");
      result = false;
    } else {
      for (String devicePath : devicePaths) {
        boolean hit = false;
        for (String storageGroup : storageGroupCache) {
          if (PathUtils.isStartWith(devicePath, storageGroup)) {
            deviceToStorageGroupMap.put(devicePath, storageGroup);
            hit = true;
            break;
          }
        }
        if (!hit) {
          logger.debug("{} failed to hit storage group cache", devicePath);
          result = false;
          break;
        }
      }
    }
    CacheMetricsRecorder.record(result, STORAGE_GROUP_CACHE_NAME);
    return result;
  }

  /** update the cache of storage group */
  public void updateStorageCache(Set<String> storageGroupNames) {
    storageGroupCache.addAll(storageGroupNames);
  }

  /** invalid storage group when delete */
  public void invalidStorageGroupCache(List<String> storageGroupNames) {
    for (String storageGroupName : storageGroupNames) {
      storageGroupCache.remove(storageGroupName);
    }
  }

  /** invalid all storageGroupCache */
  public void invalidAllStorageGroupCache() {
    storageGroupCache.clear();
  }

  /** get schemaPartition by patternTree */
  public SchemaPartition getSchemaPartition(Map<String, String> deviceToStorageGroupMap) {
    if (deviceToStorageGroupMap.size() == 0) {
      CacheMetricsRecorder.record(false, SCHEMA_PARTITION_CACHE_NAME);
      return null;
    }
    Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> schemaPartitionMap = new HashMap<>();
    // check cache for each device
    for (Map.Entry<String, String> entry : deviceToStorageGroupMap.entrySet()) {
      String device = entry.getKey();
      TSeriesPartitionSlot seriesPartitionSlot = partitionExecutor.getSeriesPartitionSlot(device);
      TConsensusGroupId consensusGroupId = schemaPartitionCache.getIfPresent(device);
      if (null == consensusGroupId) {
        logger.debug("Failed to hit {}", SCHEMA_PARTITION_CACHE_NAME);
        CacheMetricsRecorder.record(false, SCHEMA_PARTITION_CACHE_NAME);
        return null;
      }
      TRegionReplicaSet regionReplicaSet = getRegionReplicaSet(consensusGroupId);
      if (null == regionReplicaSet) {
        throw new RuntimeException("Failed to regionReplicaSet of " + consensusGroupId);
      }
      String storageGroupName = deviceToStorageGroupMap.get(device);
      if (!schemaPartitionMap.containsKey(storageGroupName)) {
        schemaPartitionMap.put(storageGroupName, new HashMap<>());
      }
      Map<TSeriesPartitionSlot, TRegionReplicaSet> regionReplicaSetMap =
          schemaPartitionMap.get(storageGroupName);
      regionReplicaSetMap.put(seriesPartitionSlot, regionReplicaSet);
    }
    logger.debug("Hit {}", SCHEMA_PARTITION_CACHE_NAME);
    // cache hit
    CacheMetricsRecorder.record(true, SCHEMA_PARTITION_CACHE_NAME);
    return new SchemaPartition(schemaPartitionMap, seriesSlotExecutorName, seriesPartitionSlotNum);
  }

  /** update schemaPartitionCache */
  public void updateSchemaPartitionCache(
      List<String> devices,
      Map<String, Map<TSeriesPartitionSlot, TConsensusGroupId>> schemaPartitionMap) {
    Set<String> storageGroupNames = schemaPartitionMap.keySet();
    for (String device : devices) {
      if (!device.contains("*")) {
        String storageGroup = null;
        for (String storageGroupName : storageGroupNames) {
          if (PathUtils.isStartWith(device, storageGroup)) {
            storageGroup = storageGroupName;
            break;
          }
        }
        if (null == storageGroup) {
          // device not exist
          continue;
        }
        TSeriesPartitionSlot seriesPartitionSlot = partitionExecutor.getSeriesPartitionSlot(device);
        TConsensusGroupId consensusGroupId =
            schemaPartitionMap.get(storageGroup).getOrDefault(seriesPartitionSlot, null);
        if (null == consensusGroupId) {
          throw new RuntimeException(
              "Failed to get consensus group id from "
                  + seriesPartitionSlot
                  + " when update schema partition.");
        }
        schemaPartitionCache.put(device, consensusGroupId);
      }
    }
  }

  /** invalid schemaPartitionCache by device */
  public void invalidSchemaPartitionCache(String device) {
    // TODO invalid schema partition cache
    schemaPartitionCache.invalidate(device);
  }

  /** invalid all schemaPartitionCache */
  public void invalidAllSchemaPartitionCache() {
    schemaPartitionCache.invalidateAll();
  }

  /** get dataPartition by query param map */
  public DataPartition getDataPartition(
      Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap) {
    if (sgNameToQueryParamsMap.size() == 0) {
      CacheMetricsRecorder.record(false, DATA_PARTITION_CACHE_NAME);
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
          logger.debug("Failed to hit {}", DATA_PARTITION_CACHE_NAME);
          CacheMetricsRecorder.record(false, DATA_PARTITION_CACHE_NAME);
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
          List<TConsensusGroupId> consensusGroupIds =
              dataPartitionCache.getIfPresent(dataPartitionCacheKey);
          if (null == consensusGroupIds) {
            // if one time partition not find, cache miss
            logger.debug("Failed to hit {}", DATA_PARTITION_CACHE_NAME);
            CacheMetricsRecorder.record(false, DATA_PARTITION_CACHE_NAME);
            return null;
          }
          List<TRegionReplicaSet> regionReplicaSets = new ArrayList<>();
          for (TConsensusGroupId consensusGroupId : consensusGroupIds) {
            regionReplicaSets.add(getRegionReplicaSet(consensusGroupId));
          }
          timePartitionSlotListMap.put(timePartitionSlot, regionReplicaSets);
        }
      }
    }
    logger.debug("Hit {}", DATA_PARTITION_CACHE_NAME);
    // cache hit
    CacheMetricsRecorder.record(true, DATA_PARTITION_CACHE_NAME);
    return new DataPartition(dataPartitionMap, seriesSlotExecutorName, seriesPartitionSlotNum);
  }

  /** update dataPartitionCache */
  public void updateDataPartitionCache(
      Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>>>
          dataPartitionMap) {
    for (Map.Entry<
            String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>>>
        entry1 : dataPartitionMap.entrySet()) {
      for (Map.Entry<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>>
          entry2 : entry1.getValue().entrySet()) {
        TSeriesPartitionSlot seriesPartitionSlot = entry2.getKey();
        for (Map.Entry<TTimePartitionSlot, List<TConsensusGroupId>> entry3 :
            entry2.getValue().entrySet()) {
          DataPartitionCacheKey dataPartitionCacheKey =
              new DataPartitionCacheKey(seriesPartitionSlot, entry3.getKey());
          dataPartitionCache.put(dataPartitionCacheKey, entry3.getValue());
        }
      }
    }
  }

  /** invalid dataPartitionCache by seriesPartitionSlot, timePartitionSlot */
  public void invalidDataPartitionCache(
      TSeriesPartitionSlot seriesPartitionSlot, TTimePartitionSlot timePartitionSlot) {
    // TODO invalid schema partition cache
    DataPartitionCacheKey dataPartitionCacheKey =
        new DataPartitionCacheKey(seriesPartitionSlot, timePartitionSlot);
    dataPartitionCache.invalidate(dataPartitionCacheKey);
  }

  /** invalid dataPartitionCache by seriesPartitionSlot, timePartitionSlot */
  public void invalidAllDataPartitionCache() {
    dataPartitionCache.invalidateAll();
  }

  /** get schemaPartition by patternTree */
  public TRegionReplicaSet getRegionReplicaSet(TConsensusGroupId consensusGroupId) {
    if (null == consensusGroupId) {
      return null;
    }
    TRegionReplicaSet regionReplicaSet = regionCache.getIfPresent(consensusGroupId);
    if (null != regionReplicaSet) {
      return regionReplicaSet;
    }
    logger.debug("Failed to hit {} in {}", consensusGroupId, REGION_CACHE_NAME);
    CacheMetricsRecorder.record(false, REGION_CACHE_NAME);
    if (!tryToUpdateRegionCache()) {
      return null;
    }
    regionReplicaSet = regionCache.getIfPresent(consensusGroupId);
    if (null == regionReplicaSet) {
      throw new RuntimeException("Failed to hit " + consensusGroupId + " in " + REGION_CACHE_NAME);
    }
    // TODO fetch region map and process
    return regionReplicaSet;
  }

  private boolean tryToUpdateRegionCache() {
    // try to get replica set
    try (ConfigNodeClient client =
        configNodeClientManager.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      TRegionCacheResp regionCacheResp = client.getRegionCache();
      if (regionCacheResp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        updateRegionCache(regionCacheResp.getRegionCache());
      } else {
        return false;
      }
    } catch (TException | IOException e) {
      throw new StatementAnalyzeException(
          "An error occurred when executing getRegionCache():" + e.getMessage());
    }
    return true;
  }

  public void updateRegionCache(TRegionCache regionCache) {
    regionCacheUpdateTime.set(regionCache.getTimestamp());
    invalidAllRegionCache();
    this.regionCache.putAll(regionCache.getRegionReplicaMap());
  }

  public void invalidAllRegionCache() {
    regionCache.cleanUp();
  }

  public void invalidAll() {
    logger.debug("Invalidate partition cache");
    invalidAllStorageGroupCache();
    invalidAllDataPartitionCache();
    invalidAllSchemaPartitionCache();
    invalidAllRegionCache();
    logger.debug("PartitionCache is invalid:{}", this);
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

  private static class DataPartitionCacheKey {
    private final TSeriesPartitionSlot seriesPartitionSlot;
    private final TTimePartitionSlot timePartitionSlot;

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
