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

package org.apache.iotdb.commons.memory;

public class MemoryConfig {
  private final MemoryManager globalMemoryManager =
      new MemoryManager("GlobalMemoryManager", null, Runtime.getRuntime().totalMemory());

  /** The memory manager of on heap */
  private MemoryManager onHeapMemoryManager;

  /** Memory manager for the write process */
  private MemoryManager storageEngineMemoryManager;

  /** The memory Manager for write */
  private MemoryManager writeMemoryManager;

  /** The memory manager for memtable */
  private MemoryManager memtableMemoryManager;

  /** The memory manager of memtable memory for WAL queue */
  private MemoryManager walBufferQueueManager;

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
  private MemoryManager ConsensusMemoryManager;

  /** Memory allocated for the pipe */
  private MemoryManager PipeMemoryManager;

  /** The memory manager of off heap */
  private MemoryManager offHeapMemoryManager;

  /** The memory manager of direct Buffer */
  private MemoryManager directBufferMemoryManager;

  public MemoryManager getOnHeapMemoryManager() {
    return onHeapMemoryManager;
  }

  public void setOnHeapMemoryManager(MemoryManager onHeapMemoryManager) {
    this.onHeapMemoryManager = onHeapMemoryManager;
  }

  public MemoryManager getStorageEngineMemoryManager() {
    return storageEngineMemoryManager;
  }

  public void setStorageEngineMemoryManager(MemoryManager storageEngineMemoryManager) {
    this.storageEngineMemoryManager = storageEngineMemoryManager;
  }

  public MemoryManager getWriteMemoryManager() {
    return writeMemoryManager;
  }

  public void setWriteMemoryManager(MemoryManager writeMemoryManager) {
    this.writeMemoryManager = writeMemoryManager;
  }

  public MemoryManager getMemtableMemoryManager() {
    return memtableMemoryManager;
  }

  public void setMemtableMemoryManager(MemoryManager memtableMemoryManager) {
    this.memtableMemoryManager = memtableMemoryManager;
  }

  public MemoryManager getWalBufferQueueManager() {
    return walBufferQueueManager;
  }

  public void setWalBufferQueueManager(MemoryManager walBufferQueueManager) {
    this.walBufferQueueManager = walBufferQueueManager;
  }

  public MemoryManager getDevicePathCacheMemoryManager() {
    return devicePathCacheMemoryManager;
  }

  public void setDevicePathCacheMemoryManager(MemoryManager devicePathCacheMemoryManager) {
    this.devicePathCacheMemoryManager = devicePathCacheMemoryManager;
  }

  public MemoryManager getBufferedArraysMemoryManager() {
    return bufferedArraysMemoryManager;
  }

  public void setBufferedArraysMemoryManager(MemoryManager bufferedArraysMemoryManager) {
    this.bufferedArraysMemoryManager = bufferedArraysMemoryManager;
  }

  public MemoryManager getTimePartitionInfoMemoryManager() {
    return timePartitionInfoMemoryManager;
  }

  public void setTimePartitionInfoMemoryManager(MemoryManager timePartitionInfoMemoryManager) {
    this.timePartitionInfoMemoryManager = timePartitionInfoMemoryManager;
  }

  public MemoryManager getCompactionMemoryManager() {
    return compactionMemoryManager;
  }

  public void setCompactionMemoryManager(MemoryManager compactionMemoryManager) {
    this.compactionMemoryManager = compactionMemoryManager;
  }

  public MemoryManager getQueryEngineMemoryManager() {
    return queryEngineMemoryManager;
  }

  public void setQueryEngineMemoryManager(MemoryManager queryEngineMemoryManager) {
    this.queryEngineMemoryManager = queryEngineMemoryManager;
  }

  public MemoryManager getBloomFilterCacheMemoryManager() {
    return bloomFilterCacheMemoryManager;
  }

  public void setBloomFilterCacheMemoryManager(MemoryManager bloomFilterCacheMemoryManager) {
    this.bloomFilterCacheMemoryManager = bloomFilterCacheMemoryManager;
  }

  public MemoryManager getTimeSeriesMetaDataCacheMemoryManager() {
    return timeSeriesMetaDataCacheMemoryManager;
  }

  public void setTimeSeriesMetaDataCacheMemoryManager(
      MemoryManager timeSeriesMetaDataCacheMemoryManager) {
    this.timeSeriesMetaDataCacheMemoryManager = timeSeriesMetaDataCacheMemoryManager;
  }

  public MemoryManager getChunkCacheMemoryManager() {
    return chunkCacheMemoryManager;
  }

  public void setChunkCacheMemoryManager(MemoryManager chunkCacheMemoryManager) {
    this.chunkCacheMemoryManager = chunkCacheMemoryManager;
  }

  public MemoryManager getCoordinatorMemoryManager() {
    return coordinatorMemoryManager;
  }

  public void setCoordinatorMemoryManager(MemoryManager coordinatorMemoryManager) {
    this.coordinatorMemoryManager = coordinatorMemoryManager;
  }

  public MemoryManager getOperatorsMemoryManager() {
    return operatorsMemoryManager;
  }

  public void setOperatorsMemoryManager(MemoryManager operatorsMemoryManager) {
    this.operatorsMemoryManager = operatorsMemoryManager;
  }

  public MemoryManager getDataExchangeMemoryManager() {
    return dataExchangeMemoryManager;
  }

  public void setDataExchangeMemoryManager(MemoryManager dataExchangeMemoryManager) {
    this.dataExchangeMemoryManager = dataExchangeMemoryManager;
  }

  public MemoryManager getTimeIndexMemoryManager() {
    return timeIndexMemoryManager;
  }

  public void setTimeIndexMemoryManager(MemoryManager timeIndexMemoryManager) {
    this.timeIndexMemoryManager = timeIndexMemoryManager;
  }

  public MemoryManager getSchemaEngineMemoryManager() {
    return schemaEngineMemoryManager;
  }

  public void setSchemaEngineMemoryManager(MemoryManager schemaEngineMemoryManager) {
    this.schemaEngineMemoryManager = schemaEngineMemoryManager;
  }

  public MemoryManager getSchemaRegionMemoryManager() {
    return schemaRegionMemoryManager;
  }

  public void setSchemaRegionMemoryManager(MemoryManager schemaRegionMemoryManager) {
    this.schemaRegionMemoryManager = schemaRegionMemoryManager;
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

  public void setPartitionCacheMemoryManager(MemoryManager partitionCacheMemoryManager) {
    this.partitionCacheMemoryManager = partitionCacheMemoryManager;
  }

  public MemoryManager getConsensusMemoryManager() {
    return ConsensusMemoryManager;
  }

  public void setConsensusMemoryManager(MemoryManager consensusMemoryManager) {
    ConsensusMemoryManager = consensusMemoryManager;
  }

  public MemoryManager getPipeMemoryManager() {
    return PipeMemoryManager;
  }

  public void setPipeMemoryManager(MemoryManager pipeMemoryManager) {
    PipeMemoryManager = pipeMemoryManager;
  }

  public MemoryManager getOffHeapMemoryManager() {
    return offHeapMemoryManager;
  }

  public void setOffHeapMemoryManager(MemoryManager offHeapMemoryManager) {
    this.offHeapMemoryManager = offHeapMemoryManager;
  }

  public MemoryManager getDirectBufferMemoryManager() {
    return directBufferMemoryManager;
  }

  public void setDirectBufferMemoryManager(MemoryManager directBufferMemoryManager) {
    this.directBufferMemoryManager = directBufferMemoryManager;
  }

  public static MemoryManager global() {
    return MemoryConfigHolder.INSTANCE.globalMemoryManager;
  }

  public static MemoryConfig getInstance() {
    return MemoryConfigHolder.INSTANCE;
  }

  private static class MemoryConfigHolder {
    private static final MemoryConfig INSTANCE = new MemoryConfig();

    private MemoryConfigHolder() {}
  }
}
