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

package org.apache.iotdb.commons.pipe.task;

import org.apache.iotdb.commons.pipe.task.meta.PipeStaticMeta;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class PipeTaskManager {

  private final Map<PipeStaticMeta, Map<Integer, PipeTask>> pipeMap = new HashMap<>();

  /**
   * Leader region count in this node. We simply update it when adding {@link PipeTask} but not
   * remove it when removing {@link PipeTask}. So it may be larger than the actual leader region
   * count in this node.
   */
  private volatile int leaderRegionCount = 0;

  /** Add {@link PipeTask} by {@link PipeStaticMeta} and consensus group id. */
  public synchronized void addPipeTask(
      PipeStaticMeta pipeStaticMeta, int consensusGroupId, PipeTask pipeTask) {
    final Map<Integer, PipeTask> regionId2PipeTask =
        pipeMap.computeIfAbsent(pipeStaticMeta, k -> new HashMap<>());
    regionId2PipeTask.put(consensusGroupId, pipeTask);

    // update leader region count
    leaderRegionCount = Math.max(leaderRegionCount, regionId2PipeTask.size());
  }

  /** Add {@link PipeTask}s by {@link PipeStaticMeta}. */
  public synchronized void addPipeTasks(
      PipeStaticMeta pipeStaticMeta, Map<Integer, PipeTask> pipeTasks) {
    final Map<Integer, PipeTask> regionId2PipeTask =
        pipeMap.computeIfAbsent(pipeStaticMeta, k -> new HashMap<>());
    regionId2PipeTask.putAll(pipeTasks);

    // update leader region count
    leaderRegionCount = Math.max(leaderRegionCount, regionId2PipeTask.size());
  }

  /**
   * Remove {@link PipeTask} by {@link PipeStaticMeta} and consensus group id.
   *
   * @param pipeStaticMeta {@link PipeStaticMeta}
   * @param consensusGroupId consensus group id
   * @return {@link PipeTask} if exists, {@code null} otherwise
   */
  public synchronized PipeTask removePipeTask(PipeStaticMeta pipeStaticMeta, int consensusGroupId) {
    Map<Integer, PipeTask> consensusGroupIdPipeTaskMap = pipeMap.get(pipeStaticMeta);
    if (consensusGroupIdPipeTaskMap != null) {
      return consensusGroupIdPipeTaskMap.remove(consensusGroupId);
    }
    return null;
  }

  /**
   * Remove {@link PipeTask}s by {@link PipeStaticMeta}.
   *
   * @param pipeStaticMeta {@link PipeStaticMeta}
   * @return {@link PipeTask}s if exists, {@code null} otherwise
   */
  public synchronized Map<Integer, PipeTask> removePipeTasks(PipeStaticMeta pipeStaticMeta) {
    return pipeMap.remove(pipeStaticMeta);
  }

  /**
   * Get {@link PipeTask} by {@link PipeStaticMeta} and consensus group id.
   *
   * @param pipeStaticMeta {@link PipeStaticMeta}
   * @param consensusGroupId consensus group id
   * @return {@link PipeTask} if exists, {@code null} otherwise
   */
  public synchronized PipeTask getPipeTask(PipeStaticMeta pipeStaticMeta, int consensusGroupId) {
    Map<Integer, PipeTask> consensusGroupIdPipeTaskMap = pipeMap.get(pipeStaticMeta);
    if (consensusGroupIdPipeTaskMap != null) {
      return consensusGroupIdPipeTaskMap.get(consensusGroupId);
    }
    return null;
  }

  /**
   * Get {@link PipeTask}s by {@link PipeStaticMeta}.
   *
   * @param pipeStaticMeta {@link PipeStaticMeta}
   * @return {@link PipeTask}s if exists, {@code null} otherwise
   */
  public synchronized Map<Integer, PipeTask> getPipeTasks(PipeStaticMeta pipeStaticMeta) {
    return pipeMap.get(pipeStaticMeta);
  }

  /**
   * Get {@link PipeTask} by consensus group id.
   *
   * @param consensusGroupId consensus group id
   * @return {@link List}
   */
  public synchronized List<PipeTask> getPipeTask(int consensusGroupId) {
    return pipeMap.values().stream()
        .map(integerPipeTaskMap -> integerPipeTaskMap.get(consensusGroupId))
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  /**
   * Get leader region count in this node.
   *
   * @return leader region count
   */
  public int getLeaderRegionCount() {
    return leaderRegionCount;
  }
}
