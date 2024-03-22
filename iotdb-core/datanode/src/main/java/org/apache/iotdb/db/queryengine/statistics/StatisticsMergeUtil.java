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

package org.apache.iotdb.db.queryengine.statistics;

import org.apache.iotdb.mpp.rpc.thrift.TOperatorStatistics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StatisticsMergeUtil {

  private StatisticsMergeUtil() {
    // hidden constructor
  }

  public static void mergeOperatorStatisticsIfDuplicate(
      Map<String, TOperatorStatistics> operatorStatisticsMap) {
    List<String> keysToRemove = new ArrayList<>();
    Map<String, TOperatorStatistics> entriesToAdd = new HashMap<>();

    for (Map.Entry<String, TOperatorStatistics> entry : operatorStatisticsMap.entrySet()) {
      String key = entry.getKey();
      if (key.contains("-")) {
        String[] keys = key.split("-");
        String planNodeId = keys[0];
        if (entriesToAdd.containsKey(planNodeId)) {
          // merge the two operatorStatistics and put in entriesToAdd
          merge(entriesToAdd.get(planNodeId), entry.getValue());
          keysToRemove.add(key);
        } else {
          entriesToAdd.put(planNodeId, entry.getValue());
          keysToRemove.add(key);
        }
      }
    }
    for (String key : keysToRemove) {
      operatorStatisticsMap.remove(key);
    }
    operatorStatisticsMap.putAll(entriesToAdd);
  }

  public static void mergeAllOperatorStatistics(
      Map<String, TOperatorStatistics> operatorStatisticsMap,
      Map<String, String> leadOverloadOperators) {
    for (Map.Entry<String, TOperatorStatistics> entry : operatorStatisticsMap.entrySet()) {
      if (leadOverloadOperators.containsKey(entry.getValue().getOperatorType())) {
        merge(
            operatorStatisticsMap.get(
                leadOverloadOperators.get(entry.getValue().getOperatorType())),
            entry.getValue());
      } else {
        TOperatorStatistics operatorStatistics = entry.getValue();
        operatorStatistics.setCount(1);
        // Can't merge specifiedInfo of String-type, so just clear it
        operatorStatistics.getSpecifiedInfo().clear();
        // keep the first one in operatorStatisticsMap as the only-one leadOverloadOperator
        leadOverloadOperators.put(
            operatorStatistics.getOperatorType(), operatorStatistics.getPlanNodeId());
      }
    }
  }

  public static void merge(TOperatorStatistics first, TOperatorStatistics second) {
    first.setTotalExecutionTimeInNanos(
        first.getTotalExecutionTimeInNanos() + second.getTotalExecutionTimeInNanos());
    first.setNextCalledCount(first.getNextCalledCount() + second.getNextCalledCount());
    first.setHasNextCalledCount(first.getHasNextCalledCount() + second.getHasNextCalledCount());
    first.setOutputRows(first.getOutputRows() + second.getOutputRows());
    first.setMemoryUsage(first.getMemoryUsage() + second.getMemoryUsage());
    first.setCount(first.getCount() + 1);
    first.setSpecifiedInfo(
        SpecifiedInfoMergerFactory.getMerger(first.getOperatorType())
            .merge(first.getSpecifiedInfo(), second.getSpecifiedInfo()));
  }
}
