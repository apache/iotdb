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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FragmentInstanceStatisticsDrawer {
  private int maxLineLength = 0;
  private final List<StatisticLine> table = new ArrayList<>();
  private static final double NS_TO_MS_FACTOR = 1.0 / 1000000;

  public void renderPlanStatistics(MPPQueryContext context) {
    addLine(
        table, 0, String.format("Analyze Cost: %s ms", context.getAnalyzeCost() * NS_TO_MS_FACTOR));
    addLine(
        table,
        0,
        String.format(
            "Fetch Partition Cost: %s ms", context.getFetchPartitionCost() * NS_TO_MS_FACTOR));
    addLine(
        table,
        0,
        String.format("Fetch Schema Cost: %s ms", context.getFetchSchemaCost() * NS_TO_MS_FACTOR));
    addLine(
        table,
        0,
        String.format("Logical Plan Cost: %s ms", context.getLogicalPlanCost() * NS_TO_MS_FACTOR));
    addLine(
        table,
        0,
        String.format(
            "Distribution Plan Cost: %s ms", context.getDistributionPlanCost() * NS_TO_MS_FACTOR));
  }

  public List<StatisticLine> renderFragmentInstances(
      List<FragmentInstance> instancesToBeRendered,
      Map<FragmentInstanceId, TFetchFragmentInstanceStatisticsResp> allStatistics,
      boolean verbose) {
    for (FragmentInstance instance : instancesToBeRendered) {
      List<StatisticLine> singleFragmentInstanceArea = new ArrayList<>();
      TFetchFragmentInstanceStatisticsResp statistics = allStatistics.get(instance.getId());
      if (statistics == null || statistics.getDataRegion() == null) {
        continue;
      }
      addLine(
          singleFragmentInstanceArea,
          0,
          String.format(
              "FRAGMENT-INSTANCE[Id: %s][IP: %s][Database: %s][DataRegion: %s]",
              instance.getId().toString(),
              statistics.getIp(),
              statistics.getDatabase(),
              statistics.getDataRegion()));
      addLine(
          singleFragmentInstanceArea,
          1,
          String.format(
              "Total Wall Time: %s ms",
              (statistics.getEndTimeInMS() - statistics.getStartTimeInMS())));
      addLine(
          singleFragmentInstanceArea,
          1,
          String.format(
              "Cost of initDataQuerySource: %s ms",
              statistics.getInitDataQuerySourceCost() * NS_TO_MS_FACTOR));
      addLine(
          singleFragmentInstanceArea,
          1,
          String.format(
              "Seq File(unclosed): %s, Seq File(closed): %s",
              statistics.getSeqUnclosedNum(), statistics.getSeqClosednNum()));
      addLine(
          singleFragmentInstanceArea,
          1,
          String.format(
              "UnSeq File(unclosed): %s, UnSeq File(closed): %s",
              statistics.getUnseqUnclosedNum(), statistics.getUnseqClosedNum()));
      addLine(
          singleFragmentInstanceArea,
          1,
          String.format(
              "ready queued time: %s ms, blocked queued time: %s ms",
              statistics.getReadyQueuedTime() * NS_TO_MS_FACTOR,
              statistics.getBlockQueuedTime() * NS_TO_MS_FACTOR));
      if (verbose) {
        renderQueryStatistics(statistics.getQueryStatistics(), singleFragmentInstanceArea);
      }
      // render operator
      PlanNode planNodeTree = instance.getFragment().getPlanNodeTree();
      renderOperator(
          planNodeTree, statistics.getOperatorStatisticsMap(), singleFragmentInstanceArea, 2);
      table.addAll(singleFragmentInstanceArea);
    }

    return table;
  }

  private void renderQueryStatistics(
      TQueryStatistics queryStatistics, List<StatisticLine> singleFragmentInstanceArea) {
    addLine(singleFragmentInstanceArea, 1, "Query Statistics:");
    addLine(
        singleFragmentInstanceArea,
        2,
        String.format(
            "loadTimeSeriesMetadataDiskSeqCount: %s",
            queryStatistics.loadTimeSeriesMetadataDiskSeqCount));
    addLine(
        singleFragmentInstanceArea,
        2,
        String.format(
            "loadTimeSeriesMetadataDiskUnSeqCount: %s",
            queryStatistics.loadTimeSeriesMetadataDiskUnSeqCount));
    addLine(
        singleFragmentInstanceArea,
        2,
        String.format(
            "loadTimeSeriesMetadataMemSeqCount: %s",
            queryStatistics.loadTimeSeriesMetadataMemSeqCount));
    addLine(
        singleFragmentInstanceArea,
        2,
        String.format(
            "loadTimeSeriesMetadataMemUnSeqCount: %s",
            queryStatistics.loadTimeSeriesMetadataMemUnSeqCount));
    addLine(
        singleFragmentInstanceArea,
        2,
        String.format(
            "loadTimeSeriesMetadataAlignedDiskSeqCount: %s",
            queryStatistics.loadTimeSeriesMetadataAlignedDiskSeqCount));
    addLine(
        singleFragmentInstanceArea,
        2,
        String.format(
            "loadTimeSeriesMetadataAlignedDiskUnSeqCount: %s",
            queryStatistics.loadTimeSeriesMetadataAlignedDiskUnSeqCount));
    addLine(
        singleFragmentInstanceArea,
        2,
        String.format(
            "loadTimeSeriesMetadataAlignedMemSeqCount: %s",
            queryStatistics.loadTimeSeriesMetadataAlignedMemSeqCount));
    addLine(
        singleFragmentInstanceArea,
        2,
        String.format(
            "loadTimeSeriesMetadataAlignedMemUnSeqCount: %s",
            queryStatistics.loadTimeSeriesMetadataAlignedMemUnSeqCount));
    addLine(
        singleFragmentInstanceArea,
        2,
        String.format(
            "loadTimeSeriesMetadataDiskSeqTime: %s ms",
            queryStatistics.loadTimeSeriesMetadataDiskSeqTime * NS_TO_MS_FACTOR));
    addLine(
        singleFragmentInstanceArea,
        2,
        String.format(
            "loadTimeSeriesMetadataDiskUnSeqTime: %s ms",
            queryStatistics.loadTimeSeriesMetadataDiskUnSeqTime * NS_TO_MS_FACTOR));
    addLine(
        singleFragmentInstanceArea,
        2,
        String.format(
            "loadTimeSeriesMetadataMemSeqTime: %s ms",
            queryStatistics.loadTimeSeriesMetadataMemSeqTime * NS_TO_MS_FACTOR));
    addLine(
        singleFragmentInstanceArea,
        2,
        String.format(
            "loadTimeSeriesMetadataMemUnSeqTime: %s ms",
            queryStatistics.loadTimeSeriesMetadataMemUnSeqTime * NS_TO_MS_FACTOR));
    addLine(
        singleFragmentInstanceArea,
        2,
        String.format(
            "loadTimeSeriesMetadataAlignedDiskSeqTime: %s ms",
            queryStatistics.loadTimeSeriesMetadataAlignedDiskSeqTime * NS_TO_MS_FACTOR));
    addLine(
        singleFragmentInstanceArea,
        2,
        String.format(
            "loadTimeSeriesMetadataAlignedDiskUnSeqTime: %s ms",
            queryStatistics.loadTimeSeriesMetadataAlignedDiskUnSeqTime * NS_TO_MS_FACTOR));
    addLine(
        singleFragmentInstanceArea,
        2,
        String.format(
            "loadTimeSeriesMetadataAlignedMemSeqTime: %s ms",
            queryStatistics.loadTimeSeriesMetadataAlignedMemSeqTime * NS_TO_MS_FACTOR));
    addLine(
        singleFragmentInstanceArea,
        2,
        String.format(
            "loadTimeSeriesMetadataAlignedMemUnSeqTime: %s ms",
            queryStatistics.loadTimeSeriesMetadataAlignedMemUnSeqTime * NS_TO_MS_FACTOR));
    addLine(
        singleFragmentInstanceArea,
        2,
        String.format(
            "constructNonAlignedChunkReadersDiskCount: %s",
            queryStatistics.constructNonAlignedChunkReadersDiskCount));
    addLine(
        singleFragmentInstanceArea,
        2,
        String.format(
            "constructNonAlignedChunkReadersMemCount: %s",
            queryStatistics.constructNonAlignedChunkReadersMemCount));
    addLine(
        singleFragmentInstanceArea,
        2,
        String.format(
            "constructAlignedChunkReadersDiskCount: %s",
            queryStatistics.constructAlignedChunkReadersDiskCount));
    addLine(
        singleFragmentInstanceArea,
        2,
        String.format(
            "constructAlignedChunkReadersMemCount: %s",
            queryStatistics.constructAlignedChunkReadersMemCount));
    addLine(
        singleFragmentInstanceArea,
        2,
        String.format(
            "constructNonAlignedChunkReadersDiskTime: %s ms",
            queryStatistics.constructNonAlignedChunkReadersDiskTime * NS_TO_MS_FACTOR));
    addLine(
        singleFragmentInstanceArea,
        2,
        String.format(
            "constructNonAlignedChunkReadersMemTime: %s ms",
            queryStatistics.constructNonAlignedChunkReadersMemTime * NS_TO_MS_FACTOR));
    addLine(
        singleFragmentInstanceArea,
        2,
        String.format(
            "constructAlignedChunkReadersDiskTime: %s ms",
            queryStatistics.constructAlignedChunkReadersDiskTime * NS_TO_MS_FACTOR));
    addLine(
        singleFragmentInstanceArea,
        2,
        String.format(
            "constructAlignedChunkReadersMemTime: %s ms",
            queryStatistics.constructAlignedChunkReadersMemTime * NS_TO_MS_FACTOR));
    addLine(
        singleFragmentInstanceArea,
        2,
        String.format(
            "pageReadersDecodeAlignedDiskCount: %s",
            queryStatistics.pageReadersDecodeAlignedDiskCount));
    addLine(
        singleFragmentInstanceArea,
        2,
        String.format(
            "pageReadersDecodeAlignedDiskTime: %s ms",
            queryStatistics.pageReadersDecodeAlignedDiskTime * NS_TO_MS_FACTOR));
    addLine(
        singleFragmentInstanceArea,
        2,
        String.format(
            "pageReadersDecodeAlignedMemCount: %s",
            queryStatistics.pageReadersDecodeAlignedMemCount));
    addLine(
        singleFragmentInstanceArea,
        2,
        String.format(
            "pageReadersDecodeAlignedMemTime: %s ms",
            queryStatistics.pageReadersDecodeAlignedMemTime * NS_TO_MS_FACTOR));
    addLine(
        singleFragmentInstanceArea,
        2,
        String.format(
            "pageReadersDecodeNonAlignedDiskCount: %s",
            queryStatistics.pageReadersDecodeNonAlignedDiskCount));
    addLine(
        singleFragmentInstanceArea,
        2,
        String.format(
            "pageReadersDecodeNonAlignedDiskTime: %s ms",
            queryStatistics.pageReadersDecodeNonAlignedDiskTime * NS_TO_MS_FACTOR));
    addLine(
        singleFragmentInstanceArea,
        2,
        String.format(
            "pageReadersDecodeNonAlignedMemCount: %s",
            queryStatistics.pageReadersDecodeNonAlignedMemCount));
    addLine(
        singleFragmentInstanceArea,
        2,
        String.format(
            "pageReadersDecodeNonAlignedMemTime: %s ms",
            queryStatistics.pageReadersDecodeNonAlignedMemTime * NS_TO_MS_FACTOR));
  }

  private void addLine(List<StatisticLine> resultForSingleInstance, int level, String value) {
    maxLineLength = Math.max(maxLineLength, value.length());

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < level; i++) {
      sb.append("  ");
    }
    sb.append(value);
    maxLineLength = Math.max(maxLineLength, sb.length());
    resultForSingleInstance.add(new StatisticLine(sb.toString(), level));
  }

  private void renderOperator(
      PlanNode planNodeTree,
      Map<String, TOperatorStatistics> operatorStatistics,
      List<StatisticLine> singleFragmentInstanceArea,
      int indentNum) {
    if (planNodeTree == null) return;
    TOperatorStatistics operatorStatistic =
        operatorStatistics.get(planNodeTree.getPlanNodeId().toString());
    if (operatorStatistic != null) {
      addLine(
          singleFragmentInstanceArea,
          indentNum,
          String.format(
              "[PlanNodeId %s]: %s(%s) %s",
              planNodeTree.getPlanNodeId().toString(),
              planNodeTree.getClass().getSimpleName(),
              operatorStatistic.getOperatorType(),
              operatorStatistic.isSetCount() ? "Count: * " + operatorStatistic.getCount() : ""));
      addLine(
          singleFragmentInstanceArea,
          indentNum + 2,
          String.format(
              "CPU Time: %s ms",
              operatorStatistic.getTotalExecutionTimeInNanos() * NS_TO_MS_FACTOR));
      addLine(
          singleFragmentInstanceArea,
          indentNum + 2,
          String.format("input: %s rows", operatorStatistic.getInputRows()));
      addLine(
          singleFragmentInstanceArea,
          indentNum + 2,
          String.format("HasNext() Called Count: %s", operatorStatistic.hasNextCalledCount));
      addLine(
          singleFragmentInstanceArea,
          indentNum + 2,
          String.format("Next() Called Count: %s", operatorStatistic.nextCalledCount));
      addLine(
          singleFragmentInstanceArea,
          indentNum + 2,
          String.format("Estimated Memory Size: %s", operatorStatistic.getMemoryInMB()));

      if (operatorStatistic.getSpecifiedInfoSize() != 0) {
        for (Map.Entry<String, String> entry : operatorStatistic.getSpecifiedInfo().entrySet()) {
          addLine(
              singleFragmentInstanceArea,
              indentNum + 2,
              String.format("%s: %s", entry.getKey(), entry.getValue()));
        }
      }
    }

    for (PlanNode child : planNodeTree.getChildren()) {
      renderOperator(child, operatorStatistics, singleFragmentInstanceArea, indentNum + 1);
    }
  }

  public int getMaxLineLength() {
    return maxLineLength;
  }
}
