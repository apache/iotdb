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

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceStatisticsResp;
import org.apache.iotdb.mpp.rpc.thrift.TOperatorStatistics;
import org.apache.iotdb.mpp.rpc.thrift.TQueryStatistics;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FragmentInstanceStatisticsDrawer {
  private int maxLineLength = 0;
  private static final double EPSILON = 1e-10;
  private final List<StatisticLine> planHeader = new ArrayList<>();
  private static final double NS_TO_MS_FACTOR = 1.0 / 1000000;

  public void renderPlanStatistics(MPPQueryContext context) {
    addLine(
        planHeader,
        0,
        String.format(
            DataNodeQueryMessages.EXCEPTION_ANALYZE_COST_ARG_MS_222A306D,
            context.getAnalyzeCost() * NS_TO_MS_FACTOR));
    addLine(
        planHeader,
        0,
        String.format(
            DataNodeQueryMessages.EXCEPTION_FETCH_PARTITION_COST_ARG_MS_67861DDF,
            context.getFetchPartitionCost() * NS_TO_MS_FACTOR));
    addLine(
        planHeader,
        0,
        String.format(
            DataNodeQueryMessages.EXCEPTION_FETCH_SCHEMA_COST_ARG_MS_67816EA5,
            context.getFetchSchemaCost() * NS_TO_MS_FACTOR));
    addLine(
        planHeader,
        1,
        String.format(
            DataNodeQueryMessages.EXCEPTION_DISK_IO_SIZE_ARG_BYTES_CD6872DC,
            context.getDiskIOSizeForDeviceEntryDuringFetchSchema()));
    addLine(
        planHeader,
        1,
        String.format(
            DataNodeQueryMessages.EXCEPTION_DISK_IO_TIME_COST_ARG_MS_36FC3B10,
            context.getDiskIOTimeCostForDeviceEntryDuringFetchSchema() * NS_TO_MS_FACTOR));
    addLine(
        planHeader,
        1,
        String.format(
            DataNodeQueryMessages.EXCEPTION_DEVICEENTRY_COUNT_ARG_3CF4F4B6,
            context.getDeviceEntryCount()));
    addLine(
        planHeader,
        0,
        String.format(
            DataNodeQueryMessages.EXCEPTION_LOGICAL_PLAN_COST_ARG_MS_1A40B609,
            context.getLogicalPlanCost() * NS_TO_MS_FACTOR));
    addLine(
        planHeader,
        0,
        String.format(
            DataNodeQueryMessages.EXCEPTION_LOGICAL_OPTIMIZATION_COST_ARG_MS_1D48CF38,
            context.getLogicalOptimizationCost() * NS_TO_MS_FACTOR));
    addLine(
        planHeader,
        0,
        String.format(
            DataNodeQueryMessages.EXCEPTION_DISTRIBUTION_PLAN_COST_ARG_MS_EE261BB5,
            context.getDistributionPlanCost() * NS_TO_MS_FACTOR));
    addLine(
        planHeader,
        1,
        String.format(
            DataNodeQueryMessages.EXCEPTION_DISK_IO_SIZE_ARG_BYTES_CD6872DC,
            context.getDiskIOSizeForDeviceEntryDuringDistributionPlan()));
    addLine(
        planHeader,
        1,
        String.format(
            DataNodeQueryMessages.EXCEPTION_DISK_IO_TIME_COST_ARG_MS_36FC3B10,
            context.getDiskIOTimeCostForDeviceEntryDuringDistributionPlan() * NS_TO_MS_FACTOR));
  }

  public void renderDispatchCost(MPPQueryContext context) {
    addLine(
        planHeader,
        0,
        String.format(
            DataNodeQueryMessages.EXCEPTION_DISPATCH_COST_ARG_MS_E2A08F90,
            context.getDispatchCost() * NS_TO_MS_FACTOR));
  }

  public List<StatisticLine> renderFragmentInstances(
      List<FragmentInstance> instancesToBeRendered,
      Map<FragmentInstanceId, TFetchFragmentInstanceStatisticsResp> allStatistics,
      boolean verbose) {
    List<StatisticLine> table = new ArrayList<>(planHeader);
    List<FragmentInstance> validInstances =
        instancesToBeRendered.stream()
            .filter(
                instance -> {
                  TFetchFragmentInstanceStatisticsResp statistics =
                      allStatistics.get(instance.getId());
                  return statistics != null && statistics.getDataRegion() != null;
                })
            .collect(Collectors.toList());

    addLine(
        table,
        0,
        String.format(
            DataNodeQueryMessages.EXCEPTION_FRAGMENT_INSTANCES_COUNT_ARG_2B7FA76E,
            validInstances.size()));
    for (FragmentInstance instance : validInstances) {
      List<StatisticLine> singleFragmentInstanceArea = new ArrayList<>();
      TFetchFragmentInstanceStatisticsResp statistics = allStatistics.get(instance.getId());

      addBlankLine(singleFragmentInstanceArea);
      addLine(
          singleFragmentInstanceArea,
          0,
          String.format(
              DataNodeQueryMessages
                  .EXCEPTION_FRAGMENT_INSTANCE_ID_ARG_IP_ARG_DATAREGION_ARG_STATE_ARG_32F467DE,
              instance.getId().toString(),
              statistics.getIp(),
              statistics.getDataRegion(),
              statistics.getState()));
      addLine(
          singleFragmentInstanceArea,
          1,
          String.format(
              DataNodeQueryMessages.EXCEPTION_TOTAL_WALL_TIME_ARG_MS_F65FF0B0,
              (statistics.getEndTimeInMS() - statistics.getStartTimeInMS())));
      addLine(
          singleFragmentInstanceArea,
          1,
          String.format(
              DataNodeQueryMessages.EXCEPTION_COST_OF_INITDATAQUERYSOURCE_ARG_MS_55A2E155,
              statistics.getInitDataQuerySourceCost() * NS_TO_MS_FACTOR));

      if (statistics.isSetInitDataQuerySourceRetryCount()
          && statistics.getInitDataQuerySourceRetryCount() > 0) {
        addLine(
            singleFragmentInstanceArea,
            1,
            String.format(
                DataNodeQueryMessages.EXCEPTION_RETRY_COUNT_OF_INITDATAQUERYSOURCE_ARG_70E5C772,
                statistics.getInitDataQuerySourceRetryCount()));
      }

      addLine(
          singleFragmentInstanceArea,
          1,
          String.format(
              DataNodeQueryMessages.EXCEPTION_SEQ_FILE_UNCLOSED_ARG_SEQ_FILE_CLOSED_ARG_CBB3422D,
              statistics.getSeqUnclosedNum(),
              statistics.getSeqClosednNum()));
      addLine(
          singleFragmentInstanceArea,
          1,
          String.format(
              DataNodeQueryMessages
                  .EXCEPTION_UNSEQ_FILE_UNCLOSED_ARG_UNSEQ_FILE_CLOSED_ARG_61850A9D,
              statistics.getUnseqUnclosedNum(),
              statistics.getUnseqClosedNum()));
      addLine(
          singleFragmentInstanceArea,
          1,
          String.format(
              DataNodeQueryMessages
                  .EXCEPTION_READY_QUEUED_TIME_ARG_MS_BLOCKED_QUEUED_TIME_ARG_MS_24FB1E4C,
              statistics.getReadyQueuedTime() * NS_TO_MS_FACTOR,
              statistics.getBlockQueuedTime() * NS_TO_MS_FACTOR));
      renderQueryStatistics(statistics.getQueryStatistics(), singleFragmentInstanceArea, verbose);

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

  private void addLineWithoutValueCheck(
      List<StatisticLine> singleFragmentInstanceArea, int level, String valueName, long value) {
    addLine(singleFragmentInstanceArea, level, valueName + String.format(": %s", value));
  }

  private void addLineWithValueCheck(
      List<StatisticLine> singleFragmentInstanceArea, int level, String valueName, double value) {
    if (Math.abs(value) > EPSILON) {
      addLine(singleFragmentInstanceArea, level, valueName + String.format(": %.3f", value));
    }
  }

  private void addLineWithoutValueCheck(
      List<StatisticLine> singleFragmentInstanceArea, int level, String valueName, double value) {
    addLine(singleFragmentInstanceArea, level, valueName + String.format(": %.3f", value));
  }

  private void addBlankLine(List<StatisticLine> singleFragmentInstanceArea) {
    addLine(singleFragmentInstanceArea, 0, " ");
  }

  private void renderQueryStatistics(
      TQueryStatistics queryStatistics,
      List<StatisticLine> singleFragmentInstanceArea,
      boolean verbose) {
    addLine(
        singleFragmentInstanceArea, 1, DataNodeQueryMessages.EXCEPTION_QUERY_STATISTICS_0F0FEAC4);

    if (verbose) {
      addLineWithoutValueCheck(
          singleFragmentInstanceArea,
          2,
          "loadBloomFilterFromCacheCount",
          queryStatistics.loadBloomFilterFromCacheCount);
      addLineWithoutValueCheck(
          singleFragmentInstanceArea,
          2,
          "loadBloomFilterFromDiskCount",
          queryStatistics.loadBloomFilterFromDiskCount);
      addLineWithoutValueCheck(
          singleFragmentInstanceArea,
          2,
          "loadBloomFilterActualIOSize",
          queryStatistics.loadBloomFilterActualIOSize);
      addLineWithoutValueCheck(
          singleFragmentInstanceArea,
          2,
          "loadBloomFilterTime",
          queryStatistics.loadBloomFilterTime * NS_TO_MS_FACTOR);

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

      addLineWithoutValueCheck(
          singleFragmentInstanceArea,
          2,
          "loadTimeSeriesMetadataFromCacheCount",
          queryStatistics.loadTimeSeriesMetadataFromCacheCount);
      addLineWithoutValueCheck(
          singleFragmentInstanceArea,
          2,
          "loadTimeSeriesMetadataFromDiskCount",
          queryStatistics.loadTimeSeriesMetadataFromDiskCount);
      addLineWithoutValueCheck(
          singleFragmentInstanceArea,
          2,
          "loadTimeSeriesMetadataActualIOSize",
          queryStatistics.loadTimeSeriesMetadataActualIOSize);

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

      addLineWithoutValueCheck(
          singleFragmentInstanceArea,
          2,
          "loadChunkFromCacheCount",
          queryStatistics.loadChunkFromCacheCount);
      addLineWithoutValueCheck(
          singleFragmentInstanceArea,
          2,
          "loadChunkFromDiskCount",
          queryStatistics.loadChunkFromDiskCount);
      addLineWithoutValueCheck(
          singleFragmentInstanceArea,
          2,
          "loadChunkActualIOSize",
          queryStatistics.loadChunkActualIOSize);

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

      addLineWithValueCheck(
          singleFragmentInstanceArea,
          2,
          "chunkWithMetadataErrorsCount",
          queryStatistics.chunkWithMetadataErrorsCount);
    }

    addLineWithoutValueCheck(
        singleFragmentInstanceArea,
        2,
        "timeSeriesIndexFilteredRows",
        queryStatistics.timeSeriesIndexFilteredRows);

    addLineWithoutValueCheck(
        singleFragmentInstanceArea,
        2,
        "chunkIndexFilteredRows",
        queryStatistics.chunkIndexFilteredRows);

    addLineWithoutValueCheck(
        singleFragmentInstanceArea,
        2,
        "pageIndexFilteredRows",
        queryStatistics.pageIndexFilteredRows);

    if (verbose) {
      addLineWithoutValueCheck(
          singleFragmentInstanceArea,
          2,
          "rowScanFilteredRows",
          queryStatistics.rowScanFilteredRows);
    }
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
              DataNodeQueryMessages.EXCEPTION_CPU_TIME_ARG_MS_42555E8C,
              operatorStatistic.getTotalExecutionTimeInNanos() * NS_TO_MS_FACTOR));
      addLine(
          singleFragmentInstanceArea,
          indentNum + 2,
          String.format(
              DataNodeQueryMessages.EXCEPTION_OUTPUT_ARG_ROWS_5EE0EF28,
              operatorStatistic.getOutputRows()));
      addLine(
          singleFragmentInstanceArea,
          indentNum + 2,
          String.format(
              DataNodeQueryMessages.EXCEPTION_HASNEXT_CALLED_COUNT_ARG_32D7AEBA,
              operatorStatistic.hasNextCalledCount));
      addLine(
          singleFragmentInstanceArea,
          indentNum + 2,
          String.format(
              DataNodeQueryMessages.EXCEPTION_NEXT_CALLED_COUNT_ARG_1AD71747,
              operatorStatistic.nextCalledCount));
      addLineWithValueCheck(
          singleFragmentInstanceArea,
          indentNum + 2,
          DataNodeQueryMessages.EXCEPTION_ESTIMATED_MEMORY_SIZE_27D7E3B5,
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
