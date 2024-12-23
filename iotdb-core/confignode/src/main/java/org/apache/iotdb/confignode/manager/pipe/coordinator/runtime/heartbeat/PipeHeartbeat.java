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

import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;

import javax.validation.constraints.NotNull;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class PipeHeartbeat {
  private final Map<PipeStaticMeta, PipeMeta> pipeMetaMap = new HashMap<>();
  private final Map<PipeStaticMeta, Boolean> isCompletedMap = new HashMap<>();
  private final Map<PipeStaticMeta, Long> remainingEventCountMap = new HashMap<>();
  private final Map<PipeStaticMeta, Double> remainingTimeMap = new HashMap<>();

  public PipeHeartbeat(
      @NotNull final List<ByteBuffer> pipeMetaByteBufferListFromAgent,
      /* @Nullable */ final List<Boolean> pipeCompletedListFromAgent,
      /* @Nullable */ final List<Long> pipeRemainingEventCountListFromAgent,
      /* @Nullable */ final List<Double> pipeRemainingTimeListFromAgent) {
    for (int i = 0; i < pipeMetaByteBufferListFromAgent.size(); ++i) {
      final PipeMeta pipeMeta =
          PipeMeta.deserialize4TaskAgent(pipeMetaByteBufferListFromAgent.get(i));
      pipeMetaMap.put(pipeMeta.getStaticMeta(), pipeMeta);
      isCompletedMap.put(
          pipeMeta.getStaticMeta(),
          Objects.nonNull(pipeCompletedListFromAgent) && pipeCompletedListFromAgent.get(i));
      // If remaining event count & remaining time can not be got, it implies that the heartbeat is
      // from an ancient version of DataNode. Here we guarantee that "0" will not affect both of
      // the final results and namely these dataNodes are omitted in calculation.
      remainingEventCountMap.put(
          pipeMeta.getStaticMeta(),
          Objects.nonNull(pipeCompletedListFromAgent)
              ? pipeRemainingEventCountListFromAgent.get(i)
              : 0L);
      remainingTimeMap.put(
          pipeMeta.getStaticMeta(),
          Objects.nonNull(pipeRemainingTimeListFromAgent)
              ? pipeRemainingTimeListFromAgent.get(i)
              : 0d);
    }
  }

  public int getPipeMetaSize() {
    return pipeMetaMap.size();
  }

  public PipeMeta getPipeMeta(final PipeStaticMeta pipeStaticMeta) {
    return pipeMetaMap.get(pipeStaticMeta);
  }

  public Boolean isCompleted(final PipeStaticMeta pipeStaticMeta) {
    return isCompletedMap.get(pipeStaticMeta);
  }

  public Long getRemainingEventCount(final PipeStaticMeta pipeStaticMeta) {
    return remainingEventCountMap.get(pipeStaticMeta);
  }

  public Double getRemainingTime(final PipeStaticMeta pipeStaticMeta) {
    return remainingTimeMap.get(pipeStaticMeta);
  }

  public boolean isEmpty() {
    return pipeMetaMap.isEmpty();
  }
}
