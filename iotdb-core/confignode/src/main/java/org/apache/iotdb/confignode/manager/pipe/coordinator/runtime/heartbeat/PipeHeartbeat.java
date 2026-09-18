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

package org.apache.iotdb.confignode.manager.pipe.coordinator.runtime.heartbeat;

import org.apache.iotdb.common.rpc.thrift.TPipeCompletedDataRegion;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTemporaryMeta;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class PipeHeartbeat {
  private final Map<PipeStaticMeta, PipeMeta> pipeMetaMap = new HashMap<>();
  private final Map<PipeStaticMeta, Long> remainingEventCountMap = new HashMap<>();
  private final Map<PipeStaticMeta, Double> remainingTimeMap = new HashMap<>();
  private final Map<PipeStaticMeta, Boolean> isDegradedMap = new HashMap<>();
  private final Map<PipeStaticMeta, Map<String, Long>> recentFailuresMap = new HashMap<>();
  private final Map<PipeStaticMeta, List<Integer>> completedDataRegionIdsMap = new HashMap<>();

  public PipeHeartbeat(
      final List<ByteBuffer> pipeMetaByteBufferListFromAgent,
      /* @Nullable */ final List<Boolean> pipeCompletedListFromAgent,
      /* @Nullable */ final List<Long> pipeRemainingEventCountListFromAgent,
      /* @Nullable */ final List<Double> pipeRemainingTimeListFromAgent,
      /* @Nullable */ final List<Integer> pipeDegradedStatusListFromAgent) {
    this(
        pipeMetaByteBufferListFromAgent,
        pipeCompletedListFromAgent,
        pipeRemainingEventCountListFromAgent,
        pipeRemainingTimeListFromAgent,
        pipeDegradedStatusListFromAgent,
        null,
        null);
  }

  public PipeHeartbeat(
      final List<ByteBuffer> pipeMetaByteBufferListFromAgent,
      /* @Nullable */ final List<Boolean> pipeCompletedListFromAgent,
      /* @Nullable */ final List<Long> pipeRemainingEventCountListFromAgent,
      /* @Nullable */ final List<Double> pipeRemainingTimeListFromAgent,
      /* @Nullable */ final List<Integer> pipeDegradedStatusListFromAgent,
      /* @Nullable */ final List<Map<String, Long>> pipeRecentFailureListFromAgent) {
    this(
        pipeMetaByteBufferListFromAgent,
        pipeCompletedListFromAgent,
        pipeRemainingEventCountListFromAgent,
        pipeRemainingTimeListFromAgent,
        pipeDegradedStatusListFromAgent,
        pipeRecentFailureListFromAgent,
        null);
  }

  public PipeHeartbeat(
      final List<ByteBuffer> pipeMetaByteBufferListFromAgent,
      /* @Nullable */ final List<Boolean> pipeCompletedListFromAgent,
      /* @Nullable */ final List<Long> pipeRemainingEventCountListFromAgent,
      /* @Nullable */ final List<Double> pipeRemainingTimeListFromAgent,
      /* @Nullable */ final List<Integer> pipeDegradedStatusListFromAgent,
      /* @Nullable */ final List<Map<String, Long>> pipeRecentFailureListFromAgent,
      /* @Nullable */ final List<TPipeCompletedDataRegion> completedDataRegionListFromAgent) {
    // Shall not reach here, just in case
    if (Objects.isNull(pipeMetaByteBufferListFromAgent)) {
      return;
    }
    for (int i = 0; i < pipeMetaByteBufferListFromAgent.size(); ++i) {
      final PipeMeta pipeMeta =
          PipeMeta.deserialize4TaskAgent(pipeMetaByteBufferListFromAgent.get(i));
      pipeMetaMap.put(pipeMeta.getStaticMeta(), pipeMeta);
      // If remaining event count & remaining time can not be got, it implies that the heartbeat is
      // from an ancient version of DataNode. Here we guarantee that "0" will not affect both of
      // the final results and namely these dataNodes are omitted in calculation.
      remainingEventCountMap.put(
          pipeMeta.getStaticMeta(),
          Objects.nonNull(pipeRemainingEventCountListFromAgent)
                  && i < pipeRemainingEventCountListFromAgent.size()
              ? pipeRemainingEventCountListFromAgent.get(i)
              : 0L);
      remainingTimeMap.put(
          pipeMeta.getStaticMeta(),
          Objects.nonNull(pipeRemainingTimeListFromAgent)
                  && i < pipeRemainingTimeListFromAgent.size()
              ? pipeRemainingTimeListFromAgent.get(i)
              : 0d);
      isDegradedMap.put(
          pipeMeta.getStaticMeta(),
          PipeTemporaryMeta.decodeTsFileEpochDegradedStatus(
              Objects.nonNull(pipeDegradedStatusListFromAgent)
                      && i < pipeDegradedStatusListFromAgent.size()
                  ? pipeDegradedStatusListFromAgent.get(i)
                  : null));
      recentFailuresMap.put(
          pipeMeta.getStaticMeta(),
          Objects.nonNull(pipeRecentFailureListFromAgent)
                  && i < pipeRecentFailureListFromAgent.size()
                  && Objects.nonNull(pipeRecentFailureListFromAgent.get(i))
              ? new HashMap<>(pipeRecentFailureListFromAgent.get(i))
              : Collections.emptyMap());
      if (completedDataRegionListFromAgent != null) {
        for (final TPipeCompletedDataRegion completedDataRegion :
            completedDataRegionListFromAgent) {
          if (pipeMeta.getStaticMeta().getPipeName().equals(completedDataRegion.getPipeName())
              && pipeMeta.getStaticMeta().getCreationTime()
                  == completedDataRegion.getCreationTime()) {
            completedDataRegionIdsMap.put(
                pipeMeta.getStaticMeta(), completedDataRegion.getCompletedDataRegionIds());
          }
        }
      }
    }
  }

  public int getPipeMetaSize() {
    return pipeMetaMap.size();
  }

  public PipeMeta getPipeMeta(final PipeStaticMeta pipeStaticMeta) {
    return pipeMetaMap.get(pipeStaticMeta);
  }

  public List<Integer> getCompletedDataRegionIds(final PipeStaticMeta pipeStaticMeta) {
    return completedDataRegionIdsMap.getOrDefault(pipeStaticMeta, Collections.emptyList());
  }

  public boolean hasCompletedDataRegionReport(final PipeStaticMeta pipeStaticMeta) {
    return completedDataRegionIdsMap.containsKey(pipeStaticMeta);
  }

  public Long getRemainingEventCount(final PipeStaticMeta pipeStaticMeta) {
    return remainingEventCountMap.get(pipeStaticMeta);
  }

  public Double getRemainingTime(final PipeStaticMeta pipeStaticMeta) {
    return remainingTimeMap.get(pipeStaticMeta);
  }

  public Boolean getDegraded(final PipeStaticMeta pipeStaticMeta) {
    return isDegradedMap.get(pipeStaticMeta);
  }

  public Map<String, Long> getRecentFailures(final PipeStaticMeta pipeStaticMeta) {
    return recentFailuresMap.get(pipeStaticMeta);
  }

  public boolean isEmpty() {
    return pipeMetaMap.isEmpty();
  }
}
