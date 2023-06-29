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

package org.apache.iotdb.db.pipe.task;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.commons.pipe.task.meta.PipeStaticMeta;

import java.util.HashMap;
import java.util.Map;

public class PipeTaskManager {

  private final Map<PipeStaticMeta, Map<TConsensusGroupId, PipeTask>> pipeMap = new HashMap<>();

  /** Add pipe task by pipe static meta and consensus group id. */
  public synchronized void addPipeTask(
      PipeStaticMeta pipeStaticMeta, TConsensusGroupId consensusGroupId, PipeTask pipeTask) {
    pipeMap.computeIfAbsent(pipeStaticMeta, k -> new HashMap<>()).put(consensusGroupId, pipeTask);
  }

  /** Add pipe tasks by pipe static meta. */
  public synchronized void addPipeTasks(
      PipeStaticMeta pipeStaticMeta, Map<TConsensusGroupId, PipeTask> pipeTasks) {
    pipeMap.computeIfAbsent(pipeStaticMeta, k -> new HashMap<>()).putAll(pipeTasks);
  }

  /**
   * Remove pipe task by pipe static meta and consensus group id.
   *
   * @param pipeStaticMeta pipe static meta
   * @param consensusGroupId consensus group id
   * @return pipe task if exists, null otherwise
   */
  public synchronized PipeTask removePipeTask(
      PipeStaticMeta pipeStaticMeta, TConsensusGroupId consensusGroupId) {
    Map<TConsensusGroupId, PipeTask> consensusGroupIdPipeTaskMap = pipeMap.get(pipeStaticMeta);
    if (consensusGroupIdPipeTaskMap != null) {
      return consensusGroupIdPipeTaskMap.remove(consensusGroupId);
    }
    return null;
  }

  /**
   * Remove pipe tasks by pipe static meta.
   *
   * @param pipeStaticMeta pipe static meta
   * @return pipe tasks if exists, null otherwise
   */
  public synchronized Map<TConsensusGroupId, PipeTask> removePipeTasks(
      PipeStaticMeta pipeStaticMeta) {
    return pipeMap.remove(pipeStaticMeta);
  }

  /**
   * Get pipe task by pipe static meta and consensus group id.
   *
   * @param pipeStaticMeta pipe static meta
   * @param consensusGroupId consensus group id
   * @return pipe task if exists, null otherwise
   */
  public synchronized PipeTask getPipeTask(
      PipeStaticMeta pipeStaticMeta, TConsensusGroupId consensusGroupId) {
    Map<TConsensusGroupId, PipeTask> consensusGroupIdPipeTaskMap = pipeMap.get(pipeStaticMeta);
    if (consensusGroupIdPipeTaskMap != null) {
      return consensusGroupIdPipeTaskMap.get(consensusGroupId);
    }
    return null;
  }

  /**
   * Get pipe tasks by pipe static meta.
   *
   * @param pipeStaticMeta pipe static meta
   * @return pipe tasks if exists, null otherwise
   */
  public synchronized Map<TConsensusGroupId, PipeTask> getPipeTasks(PipeStaticMeta pipeStaticMeta) {
    return pipeMap.get(pipeStaticMeta);
  }
}
