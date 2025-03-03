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

package org.apache.iotdb.db.conf;

import org.apache.iotdb.commons.conf.TrimProperties;
import org.apache.iotdb.commons.memory.MemoryConfig;
import org.apache.iotdb.commons.memory.MemoryManager;
import org.apache.iotdb.db.utils.MemUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataNodeMemoryConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataNodeMemoryConfig.class);

  /** The memory manager of on heap */
  private MemoryManager onHeapMemoryManager;

  /** Memory manager for the write process */
  private MemoryManager storageEngineMemoryManager;

  /** The memory Manager for write */
  private MemoryManager writeMemoryManager;

  /** The memory manager for memtable */
  private MemoryManager memtableMemoryManager;

  /** The memory manager of memtable memory for WAL queue */
  private MemoryManager walBufferQueueMemoryManager;

  /** The memory manager of memtable memory for device path cache */
  private MemoryManager devicePathCacheMemoryManager;

  /** The memory manager for buffered arrays */
  private MemoryManager bufferedArraysMemoryManager;

  /** Memory manager for time partition info */
  private MemoryManager timePartitionInfoMemoryManager;

  /** The Memory Manager for compaction */
  private MemoryManager compactionMemoryManager;

  /** Memory manager for the read process */
  private MemoryManager queryEngineMemoryManager;

  /** Memory manager for bloomFilter cache in read process */
  private MemoryManager bloomFilterCacheMemoryManager;

  /** Memory manager for timeSeriesMetaData cache in read process */
  private MemoryManager timeSeriesMetaDataCacheMemoryManager;

  /** Memory manager for chunk cache in read process */
  private MemoryManager chunkCacheMemoryManager;

  /** Memory manager for coordinator */
  private MemoryManager coordinatorMemoryManager;

  /** Memory manager for operators */
  private MemoryManager operatorsMemoryManager;

  /** Memory manager for operators */
  private MemoryManager dataExchangeMemoryManager;

  /** Memory manager proportion for timeIndex */
  private MemoryManager timeIndexMemoryManager;

  /** Memory manager for the mtree */
  private MemoryManager schemaEngineMemoryManager;

  /** Memory manager for schemaRegion */
  private MemoryManager schemaRegionMemoryManager;

  /** Memory manager for SchemaCache */
  private MemoryManager schemaCacheMemoryManager;

  /** Memory allocated for PartitionCache */
  private MemoryManager partitionCacheMemoryManager;

  /** Memory manager for the consensus layer */
  private MemoryManager consensusMemoryManager;

  /** Memory allocated for the pipe */
  private MemoryManager pipeMemoryManager;

  /** The memory manager of off heap */
  private MemoryManager offHeapMemoryManager;

  /** The memory manager of direct Buffer */
  private MemoryManager directBufferMemoryManager;

  public void init(TrimProperties properties, IoTDBConfig conf) {
    // on heap memory
    String memoryAllocateProportion = properties.getProperty("datanode_memory_proportion", null);
    // Get global memory manager here
    if (memoryAllocateProportion == null) {
      memoryAllocateProportion =
          properties.getProperty("storage_query_schema_consensus_free_memory_proportion");
      if (memoryAllocateProportion != null) {
        LOGGER.warn(
            "The parameter storage_query_schema_consensus_free_memory_proportion is deprecated since v1.2.3, "
                + "please use datanode_memory_proportion instead.");
      }
    }

    long storageEngineMemorySize = Runtime.getRuntime().maxMemory() * 3 / 10;
    long queryEngineMemorySize = Runtime.getRuntime().maxMemory() * 3 / 10;
    long schemaEngineMemorySize = Runtime.getRuntime().maxMemory() / 10;
    long consensusMemorySize = Runtime.getRuntime().maxMemory() / 10;
    long pipeMemorySize = Runtime.getRuntime().maxMemory() / 10;
    if (memoryAllocateProportion != null) {
      String[] proportions = memoryAllocateProportion.split(":");
      int proportionSum = 0;
      for (String proportion : proportions) {
        proportionSum += Integer.parseInt(proportion.trim());
      }
      long maxMemoryAvailable = Runtime.getRuntime().maxMemory();

      if (proportionSum != 0) {
        storageEngineMemorySize =
            maxMemoryAvailable * Integer.parseInt(proportions[0].trim()) / proportionSum;
        queryEngineMemorySize =
            maxMemoryAvailable * Integer.parseInt(proportions[1].trim()) / proportionSum;
        schemaEngineMemorySize =
            maxMemoryAvailable * Integer.parseInt(proportions[2].trim()) / proportionSum;
        consensusMemorySize =
            maxMemoryAvailable * Integer.parseInt(proportions[3].trim()) / proportionSum;
        // if pipe proportion is set, use it, otherwise use the default value
        if (proportions.length >= 6) {
          pipeMemorySize =
              maxMemoryAvailable * Integer.parseInt(proportions[4].trim()) / proportionSum;
        } else {
          pipeMemorySize =
              (maxMemoryAvailable
                      - storageEngineMemorySize
                      + queryEngineMemorySize
                      + schemaEngineMemorySize
                      + consensusMemorySize)
                  / 2;
        }
      }
    }
    onHeapMemoryManager =
        MemoryConfig.global().getOrCreateMemoryManager("OnHeap", Runtime.getRuntime().maxMemory());
    storageEngineMemoryManager =
        onHeapMemoryManager.getOrCreateMemoryManager("StorageEngine", storageEngineMemorySize);
    queryEngineMemoryManager =
        onHeapMemoryManager.getOrCreateMemoryManager("QueryEngine", queryEngineMemorySize);
    schemaEngineMemoryManager =
        onHeapMemoryManager.getOrCreateMemoryManager("SchemaEngine", schemaEngineMemorySize);
    consensusMemoryManager =
        onHeapMemoryManager.getOrCreateMemoryManager("Consensus", consensusMemorySize);
    pipeMemoryManager = onHeapMemoryManager.getOrCreateMemoryManager("Pipe", pipeMemorySize);
    LOGGER.info(
        "initial allocateMemoryForWrite = {}",
        storageEngineMemoryManager.getTotalMemorySizeInBytes());
    LOGGER.info(
        "initial allocateMemoryForRead = {}", queryEngineMemoryManager.getTotalMemorySizeInBytes());
    LOGGER.info(
        "initial allocateMemoryForSchema = {}",
        schemaEngineMemoryManager.getTotalMemorySizeInBytes());
    LOGGER.info(
        "initial allocateMemoryForConsensus = {}",
        consensusMemoryManager.getTotalMemorySizeInBytes());
    LOGGER.info(
        "initial allocateMemoryForPipe = {}", pipeMemoryManager.getTotalMemorySizeInBytes());

    initSchemaMemoryAllocate(schemaEngineMemoryManager, properties);
    initStorageEngineAllocate(storageEngineMemoryManager, properties, conf);
    initQueryEngineMemoryAllocate(queryEngineMemoryManager, properties, conf);

    String offHeapMemoryStr = System.getProperty("OFF_HEAP_MEMORY");
    offHeapMemoryManager =
        MemoryConfig.global()
            .getOrCreateMemoryManager("OffHeap", MemUtils.strToBytesCnt(offHeapMemoryStr), false);

    // when we can't get the OffHeapMemory variable from environment, it will be 0
    // and the limit should not be effective
    long totalDirectBufferMemorySizeLimit =
        offHeapMemoryManager.getTotalMemorySizeInBytes() == 0
            ? Long.MAX_VALUE
            : (long)
                (offHeapMemoryManager.getTotalMemorySizeInBytes()
                    * conf.getMaxDirectBufferOffHeapMemorySizeProportion());
    directBufferMemoryManager =
        offHeapMemoryManager.getOrCreateMemoryManager(
            "DirectBuffer", totalDirectBufferMemorySizeLimit);
  }

  @SuppressWarnings("squid:S3518")
  private void initSchemaMemoryAllocate(
      MemoryManager schemaEngineMemoryManager, TrimProperties properties) {
    long schemaMemoryTotal = schemaEngineMemoryManager.getTotalMemorySizeInBytes();
    int[] schemaMemoryProportion = new int[] {5, 4, 1};
    String schemaMemoryPortionInput =
        properties.getProperty(
            "schema_memory_proportion",
            properties.getProperty("schema_memory_allocate_proportion"));
    if (schemaMemoryPortionInput != null) {
      String[] proportions = schemaMemoryPortionInput.split(":");
      int loadedProportionSum = 0;
      for (String proportion : proportions) {
        loadedProportionSum += Integer.parseInt(proportion.trim());
      }

      if (loadedProportionSum != 0) {
        for (int i = 0; i < schemaMemoryProportion.length; i++) {
          schemaMemoryProportion[i] = Integer.parseInt(proportions[i].trim());
        }
      }
    }

    int proportionSum = 0;
    for (int proportion : schemaMemoryProportion) {
      proportionSum += proportion;
    }

    schemaRegionMemoryManager =
        schemaEngineMemoryManager.getOrCreateMemoryManager(
            "SchemaRegion", schemaMemoryTotal * schemaMemoryProportion[0] / proportionSum);
    schemaCacheMemoryManager =
        schemaEngineMemoryManager.getOrCreateMemoryManager(
            "SchemaCache", schemaMemoryTotal * schemaMemoryProportion[1] / proportionSum);
    partitionCacheMemoryManager =
        schemaEngineMemoryManager.getOrCreateMemoryManager(
            "PartitionCache", schemaMemoryTotal * schemaMemoryProportion[2] / proportionSum);

    LOGGER.info(
        "allocateMemoryForSchemaRegion = {}",
        schemaRegionMemoryManager.getTotalMemorySizeInBytes());
    LOGGER.info(
        "allocateMemoryForSchemaCache = {}", schemaCacheMemoryManager.getTotalMemorySizeInBytes());
    LOGGER.info(
        "allocateMemoryForPartitionCache = {}",
        partitionCacheMemoryManager.getTotalMemorySizeInBytes());
  }

  @SuppressWarnings("java:S3518")
  private void initStorageEngineAllocate(
      MemoryManager storageEngineMemoryManager, TrimProperties properties, IoTDBConfig conf) {
    long storageMemoryTotal = storageEngineMemoryManager.getTotalMemorySizeInBytes();
    String valueOfStorageEngineMemoryProportion =
        properties.getProperty("storage_engine_memory_proportion");
    long writeMemorySize = storageMemoryTotal * 8 / 10;
    long compactionMemorySize = storageMemoryTotal * 2 / 10;
    long memtableMemorySize = writeMemorySize * 19 / 20;
    long timePartitionInfoMemorySize = writeMemorySize / 20;
    if (valueOfStorageEngineMemoryProportion != null) {
      String[] storageProportionArray = valueOfStorageEngineMemoryProportion.split(":");
      int storageEngineMemoryProportion = 0;
      for (String proportion : storageProportionArray) {
        int proportionValue = Integer.parseInt(proportion.trim());
        if (proportionValue <= 0) {
          LOGGER.warn(
              "The value of storage_engine_memory_proportion is illegal, use default value 8:2 .");
          return;
        }
        storageEngineMemoryProportion += proportionValue;
      }
      writeMemorySize =
          storageMemoryTotal
              * Integer.parseInt(storageProportionArray[0].trim())
              / storageEngineMemoryProportion;
      compactionMemorySize =
          storageMemoryTotal
              * Integer.parseInt(storageProportionArray[1].trim())
              / storageEngineMemoryProportion;

      String valueOfWriteMemoryProportion = properties.getProperty("write_memory_proportion");
      if (valueOfWriteMemoryProportion != null) {
        String[] writeProportionArray = valueOfWriteMemoryProportion.split(":");
        int writeMemoryProportion = 0;
        for (String proportion : writeProportionArray) {
          int proportionValue = Integer.parseInt(proportion.trim());
          writeMemoryProportion += proportionValue;
          if (proportionValue <= 0) {
            LOGGER.warn(
                "The value of write_memory_proportion is illegal, use default value 19:1 .");
            return;
          }
        }

        memtableMemorySize =
            writeMemorySize
                * Integer.parseInt(writeProportionArray[0].trim())
                / writeMemoryProportion;
        timePartitionInfoMemorySize =
            writeMemorySize
                * Integer.parseInt(writeProportionArray[1].trim())
                / writeMemoryProportion;
      }
    }
    writeMemoryManager =
        storageEngineMemoryManager.getOrCreateMemoryManager("Write", writeMemorySize);
    compactionMemoryManager =
        storageEngineMemoryManager.getOrCreateMemoryManager("Compaction", compactionMemorySize);
    memtableMemoryManager =
        writeMemoryManager.getOrCreateMemoryManager("Memtable", memtableMemorySize);
    timePartitionInfoMemoryManager =
        writeMemoryManager.getOrCreateMemoryManager(
            "TimePartitionInfo", timePartitionInfoMemorySize);
    long devicePathCacheMemorySize =
        (long) (memtableMemorySize * conf.getDevicePathCacheProportion());
    devicePathCacheMemoryManager =
        memtableMemoryManager.getOrCreateMemoryManager(
            "DevicePathCache", devicePathCacheMemorySize);
    long bufferedArraysMemorySize =
        (long) (storageMemoryTotal * conf.getBufferedArraysMemoryProportion());
    bufferedArraysMemoryManager =
        memtableMemoryManager.getOrCreateMemoryManager("BufferedArray", bufferedArraysMemorySize);
    long walBufferQueueMemorySize =
        (long) (memtableMemorySize * conf.getWalBufferQueueProportion());
    walBufferQueueMemoryManager =
        memtableMemoryManager.getOrCreateMemoryManager("WalBufferQueue", walBufferQueueMemorySize);
  }

  @SuppressWarnings("squid:S3518")
  private void initQueryEngineMemoryAllocate(
      MemoryManager queryEngineMemoryManager, TrimProperties properties, IoTDBConfig conf) {
    conf.setEnableQueryMemoryEstimation(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_query_memory_estimation",
                Boolean.toString(conf.isEnableQueryMemoryEstimation()))));

    String queryMemoryAllocateProportion =
        properties.getProperty("chunk_timeseriesmeta_free_memory_proportion");
    long maxMemoryAvailable = queryEngineMemoryManager.getTotalMemorySizeInBytes();

    long bloomFilterCacheMemorySize = maxMemoryAvailable / 1001;
    long chunkCacheMemorySize = maxMemoryAvailable * 100 / 1001;
    long timeSeriesMetaDataCacheMemorySize = maxMemoryAvailable * 200 / 1001;
    long coordinatorMemorySize = maxMemoryAvailable * 50 / 1001;
    long operatorsMemorySize = maxMemoryAvailable * 200 / 1001;
    long dataExchangeMemorySize = maxMemoryAvailable * 200 / 1001;
    long timeIndexMemorySize = maxMemoryAvailable * 200 / 1001;
    if (queryMemoryAllocateProportion != null) {
      String[] proportions = queryMemoryAllocateProportion.split(":");
      int proportionSum = 0;
      for (String proportion : proportions) {
        proportionSum += Integer.parseInt(proportion.trim());
      }
      if (proportionSum != 0) {
        try {
          bloomFilterCacheMemorySize =
              maxMemoryAvailable * Integer.parseInt(proportions[0].trim()) / proportionSum;
          chunkCacheMemorySize =
              maxMemoryAvailable * Integer.parseInt(proportions[1].trim()) / proportionSum;
          timeSeriesMetaDataCacheMemorySize =
              maxMemoryAvailable * Integer.parseInt(proportions[2].trim()) / proportionSum;
          coordinatorMemorySize =
              maxMemoryAvailable * Integer.parseInt(proportions[3].trim()) / proportionSum;
          operatorsMemorySize =
              maxMemoryAvailable * Integer.parseInt(proportions[4].trim()) / proportionSum;
          dataExchangeMemorySize =
              maxMemoryAvailable * Integer.parseInt(proportions[5].trim()) / proportionSum;
          timeIndexMemorySize =
              maxMemoryAvailable * Integer.parseInt(proportions[6].trim()) / proportionSum;
        } catch (Exception e) {
          throw new IllegalArgumentException(
              "Each subsection of configuration item chunkmeta_chunk_timeseriesmeta_free_memory_proportion"
                  + " should be an integer, which is "
                  + queryMemoryAllocateProportion,
              e);
        }
      }
    }

    // metadata cache is disabled, we need to move all their allocated memory to other parts
    if (!conf.isMetaDataCacheEnable()) {
      long sum =
          bloomFilterCacheMemoryManager.getTotalMemorySizeInBytes()
              + chunkCacheMemoryManager.getTotalMemorySizeInBytes()
              + timeSeriesMetaDataCacheMemoryManager.getTotalMemorySizeInBytes();
      bloomFilterCacheMemorySize = 0;
      chunkCacheMemorySize = 0;
      timeSeriesMetaDataCacheMemorySize = 0;
      long partForDataExchange = sum / 2;
      long partForOperators = sum - partForDataExchange;
      dataExchangeMemorySize += partForDataExchange;
      operatorsMemorySize += partForOperators;
    }
    // set max bytes per fragment instance
    conf.setMaxBytesPerFragmentInstance(dataExchangeMemorySize / conf.getQueryThreadCount());

    bloomFilterCacheMemoryManager =
        queryEngineMemoryManager.getOrCreateMemoryManager(
            "BloomFilterCache", bloomFilterCacheMemorySize);
    chunkCacheMemoryManager =
        queryEngineMemoryManager.getOrCreateMemoryManager("ChunkCache", chunkCacheMemorySize);
    timeSeriesMetaDataCacheMemoryManager =
        queryEngineMemoryManager.getOrCreateMemoryManager(
            "TimeSeriesMetaDataCache", timeSeriesMetaDataCacheMemorySize);
    coordinatorMemoryManager =
        queryEngineMemoryManager.getOrCreateMemoryManager("Coordinator", coordinatorMemorySize);
    operatorsMemoryManager =
        queryEngineMemoryManager.getOrCreateMemoryManager("Operators", operatorsMemorySize);
    dataExchangeMemoryManager =
        queryEngineMemoryManager.getOrCreateMemoryManager("DataExchange", dataExchangeMemorySize);
    timeIndexMemoryManager =
        queryEngineMemoryManager.getOrCreateMemoryManager("TimeIndex", timeIndexMemorySize);
  }

  public MemoryManager getOnHeapMemoryManager() {
    return onHeapMemoryManager;
  }

  public MemoryManager getStorageEngineMemoryManager() {
    return storageEngineMemoryManager;
  }

  public MemoryManager getWriteMemoryManager() {
    return writeMemoryManager;
  }

  public MemoryManager getMemtableMemoryManager() {
    return memtableMemoryManager;
  }

  public MemoryManager getWalBufferQueueMemoryManager() {
    return walBufferQueueMemoryManager;
  }

  public MemoryManager getDevicePathCacheMemoryManager() {
    return devicePathCacheMemoryManager;
  }

  public MemoryManager getBufferedArraysMemoryManager() {
    return bufferedArraysMemoryManager;
  }

  public MemoryManager getTimePartitionInfoMemoryManager() {
    return timePartitionInfoMemoryManager;
  }

  public MemoryManager getCompactionMemoryManager() {
    return compactionMemoryManager;
  }

  public MemoryManager getQueryEngineMemoryManager() {
    return queryEngineMemoryManager;
  }

  public MemoryManager getBloomFilterCacheMemoryManager() {
    return bloomFilterCacheMemoryManager;
  }

  public MemoryManager getTimeSeriesMetaDataCacheMemoryManager() {
    return timeSeriesMetaDataCacheMemoryManager;
  }

  public MemoryManager getChunkCacheMemoryManager() {
    return chunkCacheMemoryManager;
  }

  public MemoryManager getCoordinatorMemoryManager() {
    return coordinatorMemoryManager;
  }

  public MemoryManager getOperatorsMemoryManager() {
    return operatorsMemoryManager;
  }

  public MemoryManager getDataExchangeMemoryManager() {
    return dataExchangeMemoryManager;
  }

  public MemoryManager getTimeIndexMemoryManager() {
    return timeIndexMemoryManager;
  }

  public MemoryManager getSchemaEngineMemoryManager() {
    return schemaEngineMemoryManager;
  }

  public MemoryManager getSchemaRegionMemoryManager() {
    return schemaRegionMemoryManager;
  }

  public MemoryManager getSchemaCacheMemoryManager() {
    return schemaCacheMemoryManager;
  }

  public void setSchemaCacheMemoryManager(MemoryManager schemaCacheMemoryManager) {
    this.schemaCacheMemoryManager = schemaCacheMemoryManager;
  }

  public MemoryManager getPartitionCacheMemoryManager() {
    return partitionCacheMemoryManager;
  }

  public MemoryManager getConsensusMemoryManager() {
    return consensusMemoryManager;
  }

  public MemoryManager getPipeMemoryManager() {
    return pipeMemoryManager;
  }

  public MemoryManager getOffHeapMemoryManager() {
    return offHeapMemoryManager;
  }

  public MemoryManager getDirectBufferMemoryManager() {
    return directBufferMemoryManager;
  }

  private DataNodeMemoryConfig() {
    // singleton
  }

  public static DataNodeMemoryConfig getInstance() {
    return DataNodeMemoryConfig.DataNodeMemoryConfigHolder.INSTANCE;
  }

  private static class DataNodeMemoryConfigHolder {
    private static final DataNodeMemoryConfig INSTANCE = new DataNodeMemoryConfig();

    private DataNodeMemoryConfigHolder() {}
  }
}
