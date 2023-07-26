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

package org.apache.iotdb.commons.pipe.config;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipeConfig {

  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();

  /////////////////////////////// File ///////////////////////////////

  public String getPipeHardlinkTsFileDirName() {
    return COMMON_CONFIG.getPipeHardlinkTsFileDirName();
  }

  /////////////////////////////// Tablet ///////////////////////////////

  public int getPipeDataStructureTabletRowSize() {
    return COMMON_CONFIG.getPipeDataStructureTabletRowSize();
  }

  /////////////////////////////// Subtask Executor ///////////////////////////////

  public int getPipeSubtaskExecutorMaxThreadNum() {
    return COMMON_CONFIG.getPipeSubtaskExecutorMaxThreadNum();
  }

  public int getPipeSubtaskExecutorBasicCheckPointIntervalByConsumedEventCount() {
    return COMMON_CONFIG.getPipeSubtaskExecutorBasicCheckPointIntervalByConsumedEventCount();
  }

  public long getPipeSubtaskExecutorBasicCheckPointIntervalByTimeDuration() {
    return COMMON_CONFIG.getPipeSubtaskExecutorBasicCheckPointIntervalByTimeDuration();
  }

  public long getPipeSubtaskExecutorPendingQueueMaxBlockingTimeMs() {
    return COMMON_CONFIG.getPipeSubtaskExecutorPendingQueueMaxBlockingTimeMs();
  }

  /////////////////////////////// Extractor ///////////////////////////////

  public int getPipeExtractorAssignerDisruptorRingBufferSize() {
    return COMMON_CONFIG.getPipeExtractorAssignerDisruptorRingBufferSize();
  }

  public int getPipeExtractorMatcherCacheSize() {
    return COMMON_CONFIG.getPipeExtractorMatcherCacheSize();
  }

  public int getPipeExtractorPendingQueueCapacity() {
    return COMMON_CONFIG.getPipeExtractorPendingQueueCapacity();
  }

  public int getPipeExtractorPendingQueueTabletLimit() {
    return COMMON_CONFIG.getPipeExtractorPendingQueueTabletLimit();
  }

  /////////////////////////////// Connector ///////////////////////////////

  public long getPipeConnectorTimeoutMs() {
    return COMMON_CONFIG.getPipeConnectorTimeoutMs();
  }

  public int getPipeConnectorReadFileBufferSize() {
    return COMMON_CONFIG.getPipeConnectorReadFileBufferSize();
  }

  public long getPipeConnectorRetryIntervalMs() {
    return COMMON_CONFIG.getPipeConnectorRetryIntervalMs();
  }

  public int getPipeConnectorPendingQueueSize() {
    return COMMON_CONFIG.getPipeConnectorPendingQueueSize();
  }

  /////////////////////////////// Meta Consistency ///////////////////////////////

  public boolean isSeperatedPipeHeartbeatEnabled() {
    return COMMON_CONFIG.isSeperatedPipeHeartbeatEnabled();
  }

  public int getPipeHeartbeatIntervalSecondsForCollectingPipeMeta() {
    return COMMON_CONFIG.getPipeHeartbeatIntervalSecondsForCollectingPipeMeta();
  }

  public long getPipeMetaSyncerInitialSyncDelayMinutes() {
    return COMMON_CONFIG.getPipeMetaSyncerInitialSyncDelayMinutes();
  }

  public long getPipeMetaSyncerSyncIntervalMinutes() {
    return COMMON_CONFIG.getPipeMetaSyncerSyncIntervalMinutes();
  }

  public long getPipeMetaSyncerAutoRestartPipeCheckIntervalRound() {
    return COMMON_CONFIG.getPipeMetaSyncerAutoRestartPipeCheckIntervalRound();
  }

  public boolean getPipeAutoRestartEnabled() {
    return COMMON_CONFIG.getPipeAutoRestartEnabled();
  }

  /////////////////////////////// Utils ///////////////////////////////

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeConfig.class);

  public void printAllConfigs() {
    LOGGER.info("PipeHardlinkTsFileDirName: {}", getPipeHardlinkTsFileDirName());

    LOGGER.info("PipeSubtaskExecutorMaxThreadNum: {}", getPipeSubtaskExecutorMaxThreadNum());
    LOGGER.info(
        "PipeSubtaskExecutorBasicCheckPointIntervalByConsumedEventCount: {}",
        getPipeSubtaskExecutorBasicCheckPointIntervalByConsumedEventCount());
    LOGGER.info(
        "PipeSubtaskExecutorBasicCheckPointIntervalByTimeDuration: {}",
        getPipeSubtaskExecutorBasicCheckPointIntervalByTimeDuration());
    LOGGER.info(
        "PipeSubtaskExecutorPendingQueueMaxBlockingTimeMs: {}",
        getPipeSubtaskExecutorPendingQueueMaxBlockingTimeMs());

    LOGGER.info(
        "PipeExtractorAssignerDisruptorRingBufferSize: {}",
        getPipeExtractorAssignerDisruptorRingBufferSize());
    LOGGER.info("PipeExtractorMatcherCacheSize: {}", getPipeExtractorMatcherCacheSize());
    LOGGER.info("PipeExtractorPendingQueueCapacity: {}", getPipeExtractorPendingQueueCapacity());
    LOGGER.info(
        "PipeExtractorPendingQueueTabletLimit: {}", getPipeExtractorPendingQueueTabletLimit());

    LOGGER.info("PipeConnectorReadFileBufferSize: {}", getPipeConnectorReadFileBufferSize());
    LOGGER.info("PipeConnectorRetryIntervalMs: {}", getPipeConnectorRetryIntervalMs());
    LOGGER.info("PipeConnectorPendingQueueSize: {}", getPipeConnectorPendingQueueSize());

    LOGGER.info("SeperatedPipeHeartbeatEnabled: {}", isSeperatedPipeHeartbeatEnabled());
    LOGGER.info(
        "PipeHeartbeatIntervalSecondsForCollectingPipeMeta: {}",
        getPipeHeartbeatIntervalSecondsForCollectingPipeMeta());
    LOGGER.info(
        "PipeMetaSyncerInitialSyncDelayMinutes: {}", getPipeMetaSyncerInitialSyncDelayMinutes());
    LOGGER.info("PipeMetaSyncerSyncIntervalMinutes: {}", getPipeMetaSyncerSyncIntervalMinutes());
  }

  /////////////////////////////// Singleton ///////////////////////////////

  private PipeConfig() {}

  public static PipeConfig getInstance() {
    return PipeConfigHolder.INSTANCE;
  }

  private static class PipeConfigHolder {
    private static final PipeConfig INSTANCE = new PipeConfig();
  }
}
