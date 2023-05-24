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

public class PipeConfig {
  final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();

  public String getPipeTsFileDirName() {
    return COMMON_CONFIG.getPipeTsFileDirName();
  }

  public int getDefaultRingBufferSize() {
    return COMMON_CONFIG.getDefaultRingBufferSize();
  }

  public int getMatcherCacheSize() {
    return COMMON_CONFIG.getMatcherCacheSize();
  }

  public int getRealtimeCollectorPendingQueueCapacity() {
    return COMMON_CONFIG.getRealtimeCollectorPendingQueueCapacity();
  }

  public int getRealtimeCollectorPendingQueueTabletLimit() {
    return COMMON_CONFIG.getRealtimeCollectorPendingQueueTabletLimit();
  }

  public int getReadFileBufferSize() {
    return COMMON_CONFIG.getReadFileBufferSize();
  }

  public int getHeartbeatLoopCyclesForCollectingPipeMeta() {
    return COMMON_CONFIG.getHeartbeatLoopCyclesForCollectingPipeMeta();
  }

  public long getInitialSyncDelayMinutes() {
    return COMMON_CONFIG.getInitialSyncDelayMinutes();
  }

  public long getSyncIntervalMinutes() {
    return COMMON_CONFIG.getSyncIntervalMinutes();
  }

  public long getRetryIntervalMs() {
    return COMMON_CONFIG.getRetryIntervalMs();
  }

  public int getConnectorPendingQueueSize() {
    return COMMON_CONFIG.getConnectorPendingQueueSize();
  }

  public int getBasicCheckPointIntervalByConsumedEventCount() {
    return COMMON_CONFIG.getBasicCheckPointIntervalByConsumedEventCount();
  }

  public long getBasicCheckPointIntervalByTimeDuration() {
    return COMMON_CONFIG.getBasicCheckPointIntervalByTimeDuration();
  }

  public int getPipeSubtaskExecutorMaxThreadNum() {
    return COMMON_CONFIG.getPipeSubtaskExecutorMaxThreadNum();
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
