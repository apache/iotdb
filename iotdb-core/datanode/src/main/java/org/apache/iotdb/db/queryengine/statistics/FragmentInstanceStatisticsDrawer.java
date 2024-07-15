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
  private static final double EPSILON = 1e-10;
  private final List<StatisticLine> planHeader = new ArrayList<>();
  private static final double NS_TO_MS_FACTOR = 1.0 / 1000000;

  public void renderPlanStatistics(MPPQueryContext context) {
    addLine(
        planHeader,
        0,
        String.format("Analyze Cost: %.3f ms", context.getAnalyzeCost() * NS_TO_MS_FACTOR));
    addLine(
        planHeader,
        0,
        String.format(
            "Fetch Partition Cost: %.3f ms", context.getFetchPartitionCost() * NS_TO_MS_FACTOR));
    addLine(
        planHeader,
        0,
        String.format(
            "Fetch Schema Cost: %.3f ms", context.getFetchSchemaCost() * NS_TO_MS_FACTOR));
    addLine(
        planHeader,
        0,
        String.format(
            "Logical Plan Cost: %.3f ms", context.getLogicalPlanCost() * NS_TO_MS_FACTOR));
    addLine(
        planHeader,
        0,
        String.format(
            "Logical Optimization Cost: %.3f ms",
            context.getLogicalOptimizationCost() * NS_TO_MS_FACTOR));
    addLine(
        planHeader,
        0,
        String.format(
            "Distribution Plan Cost: %.3f ms",
            context.getDistributionPlanCost() * NS_TO_MS_FACTOR));
  }

  public void renderDispatchCost(MPPQueryContext context) {
    addLine(
        planHeader,
        0,
        String.format("Dispatch Cost: %.3f ms", context.getDispatchCost() * NS_TO_MS_FACTOR));
  }

  public List<StatisticLine> renderFragmentInstances(
      List<FragmentInstance> instancesToBeRendered,
      Map<FragmentInstanceId, TFetchFragmentInstanceStatisticsResp> allStatistics,
      boolean verbose) {
    List<StatisticLine> table = new ArrayList<>(planHeader);
    addLine(
        table, 0, String.format("Fragment Instances Count: %s", instancesToBeRendered.size() - 1));
    for (FragmentInstance instance : instancesToBeRendered) {
      List<StatisticLine> singleFragmentInstanceArea = new ArrayList<>();
      TFetchFragmentInstanceStatisticsResp statistics = allStatistics.get(instance.getId());
      if (statistics == null || statistics.getDataRegion() == null) {
        continue;
      }
      addBlankLine(singleFragmentInstanceArea);
      addLine(
          singleFragmentInstanceArea,
          0,
          String.format(
              "FRAGMENT-INSTANCE[Id: %s][IP: %s][DataRegion: %s][State: %s]",
              instance.getId().toString(),
              statistics.getIp(),
              statistics.getDataRegion(),
              statistics.getState()));
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
              "Cost of initDataQuerySource: %.3f ms",
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
              "ready queued time: %.3f ms, blocked queued time: %.3f ms",
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

  private void addLineWithValueCheck(
      List<StatisticLine> singleFragmentInstanceArea, int level, String valueName, long value) {
    if (value != 0) {
      addLine(singleFragmentInstanceArea, level, valueName + String.format(": %s", value));
    }
  }

  private void addLineWithValueCheck(
      List<StatisticLine> singleFragmentInstanceArea, int level, String valueName, double value) {
    if (Math.abs(value) > EPSILON) {
      addLine(singleFragmentInstanceArea, level, valueName + String.format(": %.3f", value));
    }
  }

  private void addBlankLine(List<StatisticLine> singleFragmentInstanceArea) {
    addLine(singleFragmentInstanceArea, 0, " ");
  }

  private void renderQueryStatistics(
      TQueryStatistics queryStatistics, List<StatisticLine> singleFragmentInstanceArea) {
    addLine(singleFragmentInstanceArea, 1, "Query Statistics:");

    addLineWithValueCheck(
        singleFragmentInstanceArea,
        2,
        "loadTimeSeriesMetadataDiskSeqCount",
        queryStatistics.loadTimeSeriesMetadataDiskSeqCount);
    addLineWithValueCheck(
        singleFragmentInstanceArea,
        2,
        "loadTimeSeriesMetadataDiskUnSeqCount",
        queryStatistics.loadTimeSeriesMetadataDiskUnSeqCount);
    addLineWithValueCheck(
        singleFragmentInstanceArea,
        2,
        "loadTimeSeriesMetadataMemSeqCount",
        queryStatistics.loadTimeSeriesMetadataMemSeqCount);
    addLineWithValueCheck(
        singleFragmentInstanceArea,
        2,
        "loadTimeSeriesMetadataMemUnSeqCount",
        queryStatistics.loadTimeSeriesMetadataMemUnSeqCount);
    addLineWithValueCheck(
        singleFragmentInstanceArea,
        2,
        "loadTimeSeriesMetadataAlignedDiskSeqCount",
        queryStatistics.loadTimeSeriesMetadataAlignedDiskSeqCount);
    addLineWithValueCheck(
        singleFragmentInstanceArea,
        2,
        "loadTimeSeriesMetadataAlignedDiskUnSeqCount",
        queryStatistics.loadTimeSeriesMetadataAlignedDiskUnSeqCount);
    addLineWithValueCheck(
        singleFragmentInstanceArea,
        2,
        "loadTimeSeriesMetadataAlignedMemSeqCount",
        queryStatistics.loadTimeSeriesMetadataAlignedMemSeqCount);
    addLineWithValueCheck(
        singleFragmentInstanceArea,
        2,
        "loadTimeSeriesMetadataAlignedMemUnSeqCount",
        queryStatistics.loadTimeSeriesMetadataAlignedMemUnSeqCount);

    addLineWithValueCheck(
        singleFragmentInstanceArea,
        2,
        "loadTimeSeriesMetadataDiskSeqTime",
        queryStatistics.loadTimeSeriesMetadataDiskSeqTime * NS_TO_MS_FACTOR);
    addLineWithValueCheck(
        singleFragmentInstanceArea,
        2,
        "loadTimeSeriesMetadataDiskUnSeqTime",
        queryStatistics.loadTimeSeriesMetadataDiskUnSeqTime * NS_TO_MS_FACTOR);
    addLineWithValueCheck(
        singleFragmentInstanceArea,
        2,
        "loadTimeSeriesMetadataMemSeqTime",
        queryStatistics.loadTimeSeriesMetadataMemSeqTime * NS_TO_MS_FACTOR);
    addLineWithValueCheck(
        singleFragmentInstanceArea,
        2,
        "loadTimeSeriesMetadataMemUnSeqTime",
        queryStatistics.loadTimeSeriesMetadataMemUnSeqTime * NS_TO_MS_FACTOR);
    addLineWithValueCheck(
        singleFragmentInstanceArea,
        2,
        "loadTimeSeriesMetadataAlignedDiskSeqTime",
        queryStatistics.loadTimeSeriesMetadataAlignedDiskSeqTime * NS_TO_MS_FACTOR);
    addLineWithValueCheck(
        singleFragmentInstanceArea,
        2,
        "loadTimeSeriesMetadataAlignedDiskUnSeqTime",
        queryStatistics.loadTimeSeriesMetadataAlignedDiskUnSeqTime * NS_TO_MS_FACTOR);
    addLineWithValueCheck(
        singleFragmentInstanceArea,
        2,
        "loadTimeSeriesMetadataAlignedMemSeqTime",
        queryStatistics.loadTimeSeriesMetadataAlignedMemSeqTime * NS_TO_MS_FACTOR);
    addLineWithValueCheck(
        singleFragmentInstanceArea,
        2,
        "loadTimeSeriesMetadataAlignedMemUnSeqTime",
        queryStatistics.loadTimeSeriesMetadataAlignedMemUnSeqTime * NS_TO_MS_FACTOR);
    addLineWithValueCheck(
        singleFragmentInstanceArea,
        2,
        "alignedTimeSeriesMetadataModificationCount",
        queryStatistics.getAlignedTimeSeriesMetadataModificationCount());
    addLineWithValueCheck(
        singleFragmentInstanceArea,
        2,
        "alignedTimeSeriesMetadataModificationTime",
        queryStatistics.getAlignedTimeSeriesMetadataModificationTime() * NS_TO_MS_FACTOR);
    addLineWithValueCheck(
        singleFragmentInstanceArea,
        2,
        "nonAlignedTimeSeriesMetadataModificationCount",
        queryStatistics.getNonAlignedTimeSeriesMetadataModificationCount());
    addLineWithValueCheck(
        singleFragmentInstanceArea,
        2,
        "nonAlignedTimeSeriesMetadataModificationTime",
        queryStatistics.getNonAlignedTimeSeriesMetadataModificationTime() * NS_TO_MS_FACTOR);

    addLineWithValueCheck(
        singleFragmentInstanceArea,
        2,
        "constructNonAlignedChunkReadersDiskCount",
        queryStatistics.constructNonAlignedChunkReadersDiskCount);
    addLineWithValueCheck(
        singleFragmentInstanceArea,
        2,
        "constructNonAlignedChunkReadersMemCount",
        queryStatistics.constructNonAlignedChunkReadersMemCount);
    addLineWithValueCheck(
        singleFragmentInstanceArea,
        2,
        "constructAlignedChunkReadersDiskCount",
        queryStatistics.constructAlignedChunkReadersDiskCount);
    addLineWithValueCheck(
        singleFragmentInstanceArea,
        2,
        "constructAlignedChunkReadersMemCount",
        queryStatistics.constructAlignedChunkReadersMemCount);

    addLineWithValueCheck(
        singleFragmentInstanceArea,
        2,
        "constructNonAlignedChunkReadersDiskTime",
        queryStatistics.constructNonAlignedChunkReadersDiskTime * NS_TO_MS_FACTOR);
    addLineWithValueCheck(
        singleFragmentInstanceArea,
        2,
        "constructNonAlignedChunkReadersMemTime",
        queryStatistics.constructNonAlignedChunkReadersMemTime * NS_TO_MS_FACTOR);
    addLineWithValueCheck(
        singleFragmentInstanceArea,
        2,
        "constructAlignedChunkReadersDiskTime",
        queryStatistics.constructAlignedChunkReadersDiskTime * NS_TO_MS_FACTOR);
    addLineWithValueCheck(
        singleFragmentInstanceArea,
        2,
        "constructAlignedChunkReadersMemTime",
        queryStatistics.constructAlignedChunkReadersMemTime * NS_TO_MS_FACTOR);

    addLineWithValueCheck(
        singleFragmentInstanceArea,
        2,
        "pageReadersDecodeAlignedDiskCount",
        queryStatistics.pageReadersDecodeAlignedDiskCount);
    addLineWithValueCheck(
        singleFragmentInstanceArea,
        2,
        "pageReadersDecodeAlignedDiskTime",
        queryStatistics.pageReadersDecodeAlignedDiskTime * NS_TO_MS_FACTOR);
    addLineWithValueCheck(
        singleFragmentInstanceArea,
        2,
        "pageReadersDecodeAlignedMemCount",
        queryStatistics.pageReadersDecodeAlignedMemCount);
    addLineWithValueCheck(
        singleFragmentInstanceArea,
        2,
        "pageReadersDecodeAlignedMemTime",
        queryStatistics.pageReadersDecodeAlignedMemTime * NS_TO_MS_FACTOR);
    addLineWithValueCheck(
        singleFragmentInstanceArea,
        2,
        "pageReadersDecodeNonAlignedDiskCount",
        queryStatistics.pageReadersDecodeNonAlignedDiskCount);
    addLineWithValueCheck(
        singleFragmentInstanceArea,
        2,
        "pageReadersDecodeNonAlignedDiskTime",
        queryStatistics.pageReadersDecodeNonAlignedDiskTime * NS_TO_MS_FACTOR);
    addLineWithValueCheck(
        singleFragmentInstanceArea,
        2,
        "pageReadersDecodeNonAlignedMemCount",
        queryStatistics.pageReadersDecodeNonAlignedMemCount);
    addLineWithValueCheck(
        singleFragmentInstanceArea,
        2,
        "pageReadersDecodeNonAlignedMemTime",
        queryStatistics.pageReadersDecodeNonAlignedMemTime * NS_TO_MS_FACTOR);
    addLineWithValueCheck(
        singleFragmentInstanceArea,
        2,
        "pageReaderMaxUsedMemorySize",
        queryStatistics.pageReaderMaxUsedMemorySize);
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
              "CPU Time: %.3f ms",
              operatorStatistic.getTotalExecutionTimeInNanos() * NS_TO_MS_FACTOR));
      addLine(
          singleFragmentInstanceArea,
          indentNum + 2,
          String.format("output: %s rows", operatorStatistic.getOutputRows()));
      addLine(
          singleFragmentInstanceArea,
          indentNum + 2,
          String.format("HasNext() Called Count: %s", operatorStatistic.hasNextCalledCount));
      addLine(
          singleFragmentInstanceArea,
          indentNum + 2,
          String.format("Next() Called Count: %s", operatorStatistic.nextCalledCount));
      addLineWithValueCheck(
          singleFragmentInstanceArea,
          indentNum + 2,
          "Estimated Memory Size: ",
          operatorStatistic.getMemoryUsage());

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
