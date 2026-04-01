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

import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceStatisticsResp;
import org.apache.iotdb.mpp.rpc.thrift.TOperatorStatistics;
import org.apache.iotdb.mpp.rpc.thrift.TQueryStatistics;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Produces JSON output for EXPLAIN ANALYZE results, mirroring the same data as {@link
 * FragmentInstanceStatisticsDrawer} but in JSON format.
 */
public class FragmentInstanceStatisticsJsonDrawer {

  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
  private static final double NS_TO_MS_FACTOR = 1.0 / 1000000;
  private static final double EPSILON = 1e-10;

  private final JsonObject planStatistics = new JsonObject();

  public void renderPlanStatistics(MPPQueryContext context) {
    planStatistics.addProperty(
        "analyzeCostMs", formatMs(context.getAnalyzeCost() * NS_TO_MS_FACTOR));
    planStatistics.addProperty(
        "fetchPartitionCostMs", formatMs(context.getFetchPartitionCost() * NS_TO_MS_FACTOR));
    planStatistics.addProperty(
        "fetchSchemaCostMs", formatMs(context.getFetchSchemaCost() * NS_TO_MS_FACTOR));
    planStatistics.addProperty(
        "logicalPlanCostMs", formatMs(context.getLogicalPlanCost() * NS_TO_MS_FACTOR));
    planStatistics.addProperty(
        "logicalOptimizationCostMs",
        formatMs(context.getLogicalOptimizationCost() * NS_TO_MS_FACTOR));
    planStatistics.addProperty(
        "distributionPlanCostMs", formatMs(context.getDistributionPlanCost() * NS_TO_MS_FACTOR));
  }

  public void renderDispatchCost(MPPQueryContext context) {
    planStatistics.addProperty(
        "dispatchCostMs", formatMs(context.getDispatchCost() * NS_TO_MS_FACTOR));
  }

  public String renderFragmentInstancesAsJson(
      List<FragmentInstance> instancesToBeRendered,
      Map<FragmentInstanceId, TFetchFragmentInstanceStatisticsResp> allStatistics,
      boolean verbose) {

    JsonObject root = new JsonObject();
    root.add("planStatistics", planStatistics);

    List<FragmentInstance> validInstances =
        instancesToBeRendered.stream()
            .filter(
                instance -> {
                  TFetchFragmentInstanceStatisticsResp statistics =
                      allStatistics.get(instance.getId());
                  return statistics != null && statistics.getDataRegion() != null;
                })
            .collect(Collectors.toList());

    root.addProperty("fragmentInstancesCount", validInstances.size());

    JsonArray fragmentInstancesArray = new JsonArray();
    for (FragmentInstance instance : validInstances) {
      TFetchFragmentInstanceStatisticsResp statistics = allStatistics.get(instance.getId());
      JsonObject fiJson = new JsonObject();

      fiJson.addProperty("id", instance.getId().toString());
      fiJson.addProperty("ip", statistics.getIp());
      fiJson.addProperty("dataRegion", statistics.getDataRegion());
      fiJson.addProperty("state", statistics.getState());
      fiJson.addProperty(
          "totalWallTimeMs", statistics.getEndTimeInMS() - statistics.getStartTimeInMS());
      fiJson.addProperty(
          "initDataQuerySourceCostMs",
          formatMs(statistics.getInitDataQuerySourceCost() * NS_TO_MS_FACTOR));

      if (statistics.isSetInitDataQuerySourceRetryCount()
          && statistics.getInitDataQuerySourceRetryCount() > 0) {
        fiJson.addProperty(
            "initDataQuerySourceRetryCount", statistics.getInitDataQuerySourceRetryCount());
      }

      fiJson.addProperty("seqFileUnclosed", statistics.getSeqUnclosedNum());
      fiJson.addProperty("seqFileClosed", statistics.getSeqClosednNum());
      fiJson.addProperty("unseqFileUnclosed", statistics.getUnseqUnclosedNum());
      fiJson.addProperty("unseqFileClosed", statistics.getUnseqClosedNum());
      fiJson.addProperty(
          "readyQueuedTimeMs", formatMs(statistics.getReadyQueuedTime() * NS_TO_MS_FACTOR));
      fiJson.addProperty(
          "blockQueuedTimeMs", formatMs(statistics.getBlockQueuedTime() * NS_TO_MS_FACTOR));

      // Query statistics
      JsonObject queryStats = renderQueryStatisticsJson(statistics.getQueryStatistics(), verbose);
      fiJson.add("queryStatistics", queryStats);

      // Operators
      PlanNode planNodeTree = instance.getFragment().getPlanNodeTree();
      JsonObject operatorTree =
          renderOperatorJson(planNodeTree, statistics.getOperatorStatisticsMap());
      if (operatorTree != null) {
        fiJson.add("operators", operatorTree);
      }

      fragmentInstancesArray.add(fiJson);
    }

    root.add("fragmentInstances", fragmentInstancesArray);
    return GSON.toJson(root);
  }

  private JsonObject renderQueryStatisticsJson(TQueryStatistics qs, boolean verbose) {
    JsonObject stats = new JsonObject();

    if (verbose) {
      stats.addProperty("loadBloomFilterFromCacheCount", qs.loadBloomFilterFromCacheCount);
      stats.addProperty("loadBloomFilterFromDiskCount", qs.loadBloomFilterFromDiskCount);
      stats.addProperty("loadBloomFilterActualIOSize", qs.loadBloomFilterActualIOSize);
      stats.addProperty(
          "loadBloomFilterTimeMs", formatMs(qs.loadBloomFilterTime * NS_TO_MS_FACTOR));

      addIfNonZero(
          stats, "loadTimeSeriesMetadataDiskSeqCount", qs.loadTimeSeriesMetadataDiskSeqCount);
      addIfNonZero(
          stats, "loadTimeSeriesMetadataDiskUnSeqCount", qs.loadTimeSeriesMetadataDiskUnSeqCount);
      addIfNonZero(
          stats, "loadTimeSeriesMetadataMemSeqCount", qs.loadTimeSeriesMetadataMemSeqCount);
      addIfNonZero(
          stats, "loadTimeSeriesMetadataMemUnSeqCount", qs.loadTimeSeriesMetadataMemUnSeqCount);
      addIfNonZero(
          stats,
          "loadTimeSeriesMetadataAlignedDiskSeqCount",
          qs.loadTimeSeriesMetadataAlignedDiskSeqCount);
      addIfNonZero(
          stats,
          "loadTimeSeriesMetadataAlignedDiskUnSeqCount",
          qs.loadTimeSeriesMetadataAlignedDiskUnSeqCount);
      addIfNonZero(
          stats,
          "loadTimeSeriesMetadataAlignedMemSeqCount",
          qs.loadTimeSeriesMetadataAlignedMemSeqCount);
      addIfNonZero(
          stats,
          "loadTimeSeriesMetadataAlignedMemUnSeqCount",
          qs.loadTimeSeriesMetadataAlignedMemUnSeqCount);

      addMsIfNonZero(
          stats,
          "loadTimeSeriesMetadataDiskSeqTimeMs",
          qs.loadTimeSeriesMetadataDiskSeqTime * NS_TO_MS_FACTOR);
      addMsIfNonZero(
          stats,
          "loadTimeSeriesMetadataDiskUnSeqTimeMs",
          qs.loadTimeSeriesMetadataDiskUnSeqTime * NS_TO_MS_FACTOR);
      addMsIfNonZero(
          stats,
          "loadTimeSeriesMetadataMemSeqTimeMs",
          qs.loadTimeSeriesMetadataMemSeqTime * NS_TO_MS_FACTOR);
      addMsIfNonZero(
          stats,
          "loadTimeSeriesMetadataMemUnSeqTimeMs",
          qs.loadTimeSeriesMetadataMemUnSeqTime * NS_TO_MS_FACTOR);
      addMsIfNonZero(
          stats,
          "loadTimeSeriesMetadataAlignedDiskSeqTimeMs",
          qs.loadTimeSeriesMetadataAlignedDiskSeqTime * NS_TO_MS_FACTOR);
      addMsIfNonZero(
          stats,
          "loadTimeSeriesMetadataAlignedDiskUnSeqTimeMs",
          qs.loadTimeSeriesMetadataAlignedDiskUnSeqTime * NS_TO_MS_FACTOR);
      addMsIfNonZero(
          stats,
          "loadTimeSeriesMetadataAlignedMemSeqTimeMs",
          qs.loadTimeSeriesMetadataAlignedMemSeqTime * NS_TO_MS_FACTOR);
      addMsIfNonZero(
          stats,
          "loadTimeSeriesMetadataAlignedMemUnSeqTimeMs",
          qs.loadTimeSeriesMetadataAlignedMemUnSeqTime * NS_TO_MS_FACTOR);

      stats.addProperty(
          "loadTimeSeriesMetadataFromCacheCount", qs.loadTimeSeriesMetadataFromCacheCount);
      stats.addProperty(
          "loadTimeSeriesMetadataFromDiskCount", qs.loadTimeSeriesMetadataFromDiskCount);
      stats.addProperty(
          "loadTimeSeriesMetadataActualIOSize", qs.loadTimeSeriesMetadataActualIOSize);

      addIfNonZero(
          stats,
          "alignedTimeSeriesMetadataModificationCount",
          qs.getAlignedTimeSeriesMetadataModificationCount());
      addMsIfNonZero(
          stats,
          "alignedTimeSeriesMetadataModificationTimeMs",
          qs.getAlignedTimeSeriesMetadataModificationTime() * NS_TO_MS_FACTOR);
      addIfNonZero(
          stats,
          "nonAlignedTimeSeriesMetadataModificationCount",
          qs.getNonAlignedTimeSeriesMetadataModificationCount());
      addMsIfNonZero(
          stats,
          "nonAlignedTimeSeriesMetadataModificationTimeMs",
          qs.getNonAlignedTimeSeriesMetadataModificationTime() * NS_TO_MS_FACTOR);

      addIfNonZero(
          stats,
          "constructNonAlignedChunkReadersDiskCount",
          qs.constructNonAlignedChunkReadersDiskCount);
      addIfNonZero(
          stats,
          "constructNonAlignedChunkReadersMemCount",
          qs.constructNonAlignedChunkReadersMemCount);
      addIfNonZero(
          stats, "constructAlignedChunkReadersDiskCount", qs.constructAlignedChunkReadersDiskCount);
      addIfNonZero(
          stats, "constructAlignedChunkReadersMemCount", qs.constructAlignedChunkReadersMemCount);
      addMsIfNonZero(
          stats,
          "constructNonAlignedChunkReadersDiskTimeMs",
          qs.constructNonAlignedChunkReadersDiskTime * NS_TO_MS_FACTOR);
      addMsIfNonZero(
          stats,
          "constructNonAlignedChunkReadersMemTimeMs",
          qs.constructNonAlignedChunkReadersMemTime * NS_TO_MS_FACTOR);
      addMsIfNonZero(
          stats,
          "constructAlignedChunkReadersDiskTimeMs",
          qs.constructAlignedChunkReadersDiskTime * NS_TO_MS_FACTOR);
      addMsIfNonZero(
          stats,
          "constructAlignedChunkReadersMemTimeMs",
          qs.constructAlignedChunkReadersMemTime * NS_TO_MS_FACTOR);

      stats.addProperty("loadChunkFromCacheCount", qs.loadChunkFromCacheCount);
      stats.addProperty("loadChunkFromDiskCount", qs.loadChunkFromDiskCount);
      stats.addProperty("loadChunkActualIOSize", qs.loadChunkActualIOSize);

      addIfNonZero(
          stats, "pageReadersDecodeAlignedDiskCount", qs.pageReadersDecodeAlignedDiskCount);
      addMsIfNonZero(
          stats,
          "pageReadersDecodeAlignedDiskTimeMs",
          qs.pageReadersDecodeAlignedDiskTime * NS_TO_MS_FACTOR);
      addIfNonZero(stats, "pageReadersDecodeAlignedMemCount", qs.pageReadersDecodeAlignedMemCount);
      addMsIfNonZero(
          stats,
          "pageReadersDecodeAlignedMemTimeMs",
          qs.pageReadersDecodeAlignedMemTime * NS_TO_MS_FACTOR);
      addIfNonZero(
          stats, "pageReadersDecodeNonAlignedDiskCount", qs.pageReadersDecodeNonAlignedDiskCount);
      addMsIfNonZero(
          stats,
          "pageReadersDecodeNonAlignedDiskTimeMs",
          qs.pageReadersDecodeNonAlignedDiskTime * NS_TO_MS_FACTOR);
      addIfNonZero(
          stats, "pageReadersDecodeNonAlignedMemCount", qs.pageReadersDecodeNonAlignedMemCount);
      addMsIfNonZero(
          stats,
          "pageReadersDecodeNonAlignedMemTimeMs",
          qs.pageReadersDecodeNonAlignedMemTime * NS_TO_MS_FACTOR);
      addIfNonZero(stats, "pageReaderMaxUsedMemorySize", qs.pageReaderMaxUsedMemorySize);
      addIfNonZero(stats, "chunkWithMetadataErrorsCount", qs.chunkWithMetadataErrorsCount);
    }

    stats.addProperty("timeSeriesIndexFilteredRows", qs.timeSeriesIndexFilteredRows);
    stats.addProperty("chunkIndexFilteredRows", qs.chunkIndexFilteredRows);
    stats.addProperty("pageIndexFilteredRows", qs.pageIndexFilteredRows);

    if (verbose) {
      stats.addProperty("rowScanFilteredRows", qs.rowScanFilteredRows);
    }

    return stats;
  }

  private JsonObject renderOperatorJson(
      PlanNode planNodeTree, Map<String, TOperatorStatistics> operatorStatistics) {
    if (planNodeTree == null) {
      return null;
    }

    JsonObject operatorJson = new JsonObject();
    TOperatorStatistics opStats = operatorStatistics.get(planNodeTree.getPlanNodeId().toString());

    operatorJson.addProperty("planNodeId", planNodeTree.getPlanNodeId().toString());
    operatorJson.addProperty("nodeType", planNodeTree.getClass().getSimpleName());

    if (opStats != null) {
      operatorJson.addProperty("operatorType", opStats.getOperatorType());
      if (opStats.isSetCount()) {
        operatorJson.addProperty("count", opStats.getCount());
      }
      operatorJson.addProperty(
          "cpuTimeMs", formatMs(opStats.getTotalExecutionTimeInNanos() * NS_TO_MS_FACTOR));
      operatorJson.addProperty("outputRows", opStats.getOutputRows());
      operatorJson.addProperty("hasNextCalledCount", opStats.hasNextCalledCount);
      operatorJson.addProperty("nextCalledCount", opStats.nextCalledCount);
      if (opStats.getMemoryUsage() != 0) {
        operatorJson.addProperty("estimatedMemorySize", opStats.getMemoryUsage());
      }

      if (opStats.getSpecifiedInfoSize() != 0) {
        JsonObject specifiedInfo = new JsonObject();
        for (Map.Entry<String, String> entry : opStats.getSpecifiedInfo().entrySet()) {
          specifiedInfo.addProperty(entry.getKey(), entry.getValue());
        }
        operatorJson.add("specifiedInfo", specifiedInfo);
      }
    }

    List<PlanNode> children = planNodeTree.getChildren();
    if (children != null && !children.isEmpty()) {
      JsonArray childrenArray = new JsonArray();
      for (PlanNode child : children) {
        JsonObject childJson = renderOperatorJson(child, operatorStatistics);
        if (childJson != null) {
          childrenArray.add(childJson);
        }
      }
      if (childrenArray.size() > 0) {
        operatorJson.add("children", childrenArray);
      }
    }

    return operatorJson;
  }

  private static double formatMs(double ms) {
    return Math.round(ms * 1000.0) / 1000.0;
  }

  private static void addIfNonZero(JsonObject obj, String key, long value) {
    if (value != 0) {
      obj.addProperty(key, value);
    }
  }

  private static void addMsIfNonZero(JsonObject obj, String key, double value) {
    if (Math.abs(value) > EPSILON) {
      obj.addProperty(key, formatMs(value));
    }
  }
}
