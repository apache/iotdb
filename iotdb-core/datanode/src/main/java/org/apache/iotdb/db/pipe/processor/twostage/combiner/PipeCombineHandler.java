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

package org.apache.iotdb.db.pipe.processor.twostage.combiner;

import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.pipe.processor.twostage.exchange.payload.FetchCombineResultResponse;
import org.apache.iotdb.db.pipe.processor.twostage.operator.Operator;
import org.apache.iotdb.db.pipe.processor.twostage.state.State;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class PipeCombineHandler {

  private final String pipeName;
  private final long creationTime;

  private final Operator operator;

  private final Set<Integer> expectedRegionIdSet;

  private final ConcurrentMap<String, Combiner> combineId2Combiner;

  public PipeCombineHandler(String pipeName, long creationTime, Operator operator) {
    this.pipeName = pipeName;
    this.creationTime = creationTime;

    this.operator = operator;

    expectedRegionIdSet = new HashSet<>();
    fetchExpectedRegionIdSet();

    combineId2Combiner = new ConcurrentHashMap<>();
  }

  public String getPipeName() {
    return pipeName;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public synchronized void combine(int regionId, String combineId, State state) {
    combineId2Combiner
        .computeIfAbsent(combineId, id -> new Combiner(operator, expectedRegionIdSet))
        .combine(regionId, state);
  }

  public synchronized FetchCombineResultResponse fetchCombineResult(List<String> combineIdList)
      throws IOException {
    final Map<String, FetchCombineResultResponse.CombineResultType> combineId2ResultType =
        new HashMap<>();
    for (String combineId : combineIdList) {
      final Combiner combiner = combineId2Combiner.get(combineId);

      if (combiner == null || combiner.isOutdated()) {
        combineId2ResultType.put(combineId, FetchCombineResultResponse.CombineResultType.OUTDATED);
        continue;
      }

      combineId2ResultType.put(
          combineId,
          combiner.isComplete()
              ? FetchCombineResultResponse.CombineResultType.SUCCESS
              : FetchCombineResultResponse.CombineResultType.INCOMPLETE);
    }

    return FetchCombineResultResponse.toTPipeTransferResp(combineId2ResultType);
  }

  public synchronized void fetchExpectedRegionIdSet() {
    expectedRegionIdSet.clear();
    expectedRegionIdSet.addAll(PipeAgent.task().getPipeTaskRegionIdSet(pipeName, creationTime));
  }

  public synchronized void cleanOutdatedCombiner() {
    combineId2Combiner.entrySet().removeIf(entry -> entry.getValue().isOutdated());
  }

  public synchronized void close() {
    combineId2Combiner.clear();
  }
}
