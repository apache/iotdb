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

package org.apache.iotdb.db.queryengine.execution.fragment;

import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.exception.CpuNotEnoughException;
import org.apache.iotdb.db.queryengine.exception.MemoryNotEnoughException;
import org.apache.iotdb.db.queryengine.execution.driver.IDriver;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISink;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.schedule.IDriverScheduler;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.IDataRegionForQuery;
import org.apache.iotdb.db.storageengine.dataregion.VirtualDataRegion;
import org.apache.iotdb.db.utils.SetThreadName;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceStatisticsResp;
import org.apache.iotdb.mpp.rpc.thrift.TOperatorStatistics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.iotdb.db.queryengine.statistics.StatisticsMergeUtil.merge;
import static org.apache.iotdb.db.queryengine.statistics.StatisticsMergeUtil.mergeAllOperatorStatistics;
import static org.apache.iotdb.db.queryengine.statistics.StatisticsMergeUtil.mergeOperatorStatisticsIfDuplicate;

public class FragmentInstanceExecution {

  private static final Logger LOGGER = LoggerFactory.getLogger(FragmentInstanceExecution.class);
  private final FragmentInstanceId instanceId;
  private final FragmentInstanceContext context;

  // It will be set to null while this FI is FINISHED
  private List<IDriver> drivers;

  // It will be set to null while this FI is FINISHED
  private ISink sink;

  private final FragmentInstanceStateMachine stateMachine;

  private final long timeoutInMs;

  private final MPPDataExchangeManager exchangeManager;

  // This lock is used to guarantee the atomicity of buildStatistics() and the set of
  // statisticsRemoved,
  // so that fetchStatistics() in FragmentInstanceManager won't get the statistics which was just
  // cleared.
  private final ReadWriteLock statisticsLock = new ReentrantReadWriteLock();
  private boolean staticsRemoved = false;

  @SuppressWarnings("squid:S107")
  public static FragmentInstanceExecution createFragmentInstanceExecution(
      IDriverScheduler scheduler,
      FragmentInstanceId instanceId,
      FragmentInstanceContext context,
      List<IDriver> drivers,
      ISink sinkHandle,
      FragmentInstanceStateMachine stateMachine,
      long timeOut,
      boolean isExplainAnalyze,
      MPPDataExchangeManager exchangeManager)
      throws CpuNotEnoughException, MemoryNotEnoughException {
    FragmentInstanceExecution execution =
        new FragmentInstanceExecution(
            instanceId, context, drivers, sinkHandle, stateMachine, timeOut, exchangeManager);
    execution.initialize(scheduler, isExplainAnalyze);
    scheduler.submitDrivers(instanceId.getQueryId(), drivers, timeOut, context.getSessionInfo());
    return execution;
  }

  private FragmentInstanceExecution(
      FragmentInstanceId instanceId,
      FragmentInstanceContext context,
      List<IDriver> drivers,
      ISink sink,
      FragmentInstanceStateMachine stateMachine,
      long timeoutInMs,
      MPPDataExchangeManager exchangeManager) {
    this.instanceId = instanceId;
    this.context = context;
    this.drivers = drivers;
    this.sink = sink;
    this.stateMachine = stateMachine;
    this.timeoutInMs = timeoutInMs;
    this.exchangeManager = exchangeManager;
  }

  public FragmentInstanceState getInstanceState() {
    return stateMachine.getState();
  }

  public FragmentInstanceInfo getInstanceInfo() {
    return context
        .getErrorCode()
        .map(
            s ->
                new FragmentInstanceInfo(
                    stateMachine.getState(),
                    context.getEndTime(),
                    context.getFailedCause(),
                    context.getFailureInfoList(),
                    s))
        .orElseGet(
            () ->
                new FragmentInstanceInfo(
                    stateMachine.getState(),
                    context.getEndTime(),
                    context.getFailedCause(),
                    context.getFailureInfoList()));
  }

  public long getStartTime() {
    return context.getStartTime();
  }

  public long getTimeoutInMs() {
    return timeoutInMs;
  }

  public FragmentInstanceContext getFragmentInstanceContext() {
    return context;
  }

  public List<IDriver> getDrivers() {
    return drivers;
  }

  public FragmentInstanceStateMachine getStateMachine() {
    return stateMachine;
  }

  // Fill Fragment-Level info for statistics
  private boolean fillFragmentInstanceStatistics(
      FragmentInstanceContext context, TFetchFragmentInstanceStatisticsResp statistics) {
    statistics.setFragmentInstanceId(context.getId().toThrift());
    statistics.setQueryStatistics(context.getQueryStatistics().toThrift());
    statistics.setState(getInstanceState().toString());
    IDataRegionForQuery dataRegionForQuery = context.getDataRegion();
    if (dataRegionForQuery instanceof VirtualDataRegion) {
      // We don't need to output the region having ExplainAnalyzeOperator only.
      return false;
    }
    statistics.setDataRegion(((DataRegion) context.getDataRegion()).getDataRegionId());
    statistics.setIp(IoTDBDescriptor.getInstance().getConfig().getAddressAndPort().ip);
    statistics.setStartTimeInMS(context.getStartTime());
    statistics.setEndTimeInMS(
        context.isEndTimeUpdate() ? context.getEndTime() : System.currentTimeMillis());

    statistics.setBlockQueuedTime(context.getBlockQueueTime());
    statistics.setReadyQueuedTime(context.getReadyQueueTime());

    statistics.setInitDataQuerySourceCost(context.getInitQueryDataSourceCost());

    statistics.setSeqClosednNum(context.getClosedSeqFileNum());
    statistics.setSeqUnclosedNum(context.getUnclosedSeqFileNum());
    statistics.setUnseqClosedNum(context.getClosedUnseqFileNum());
    statistics.setUnseqUnclosedNum(context.getUnclosedUnseqFileNum());

    return true;
  }

  // Fill Operator-Level info for statistics
  // Return needMerge to indicate if operatorStatistics in current fragmentInstance is merged.
  private boolean fillFragmentInstanceStatistics(
      List<OperatorContext> contexts,
      Map<String, TOperatorStatistics> operatorStatisticsMap,
      Map<String, Integer> operatorCoutMap,
      Map<String, String> leadOverloadOperators,
      boolean needMerge) {
    for (OperatorContext operatorContext : contexts) {
      TOperatorStatistics operatorStatistics = new TOperatorStatistics();
      // some exchange operators don't have planNodeId
      if (operatorContext.getPlanNodeId() == null) continue;

      String operatorType = operatorContext.getOperatorType();
      // If the operatorType is already overloaded, then merge all operatorStatistics with the
      // leadOverloadOperator
      if (needMerge) {
        setOperatorStatistics(operatorStatistics, operatorContext);
        if (leadOverloadOperators.containsKey(operatorType)) {
          merge(
              operatorStatisticsMap.get(leadOverloadOperators.get(operatorType)),
              operatorStatistics);
        } else {
          String planNodeId = operatorContext.getPlanNodeId().toString();
          operatorStatistics.setCount(1);
          operatorStatistics.getSpecifiedInfo().clear();
          leadOverloadOperators.put(operatorType, planNodeId);
          operatorStatisticsMap.put(planNodeId, operatorStatistics);
        }
      } else {
        setOperatorStatistics(operatorStatistics, operatorContext);
        operatorStatisticsMap.put(operatorContext.getPlanNodeId().toString(), operatorStatistics);
        operatorCoutMap.put(operatorType, operatorCoutMap.getOrDefault(operatorType, 0) + 1);
        if (operatorCoutMap.get(operatorType)
            >= IoTDBDescriptor.getInstance().getConfig().getMergeThresholdOfExplainAnalyze()) {
          needMerge = true;
          // merge all the operatorStatistics with the overload type and remain only one in
          // operatorStatisticsMap
          mergeAllOperatorStatistics(operatorStatisticsMap, leadOverloadOperators);
        }
      }
    }
    return needMerge;
  }

  private void setOperatorStatistics(
      TOperatorStatistics operatorStatistics, OperatorContext operatorContext) {
    operatorStatistics.setPlanNodeId(operatorContext.getPlanNodeId().toString());
    operatorStatistics.setOperatorType(operatorContext.getOperatorType());
    operatorStatistics.setTotalExecutionTimeInNanos(operatorContext.getTotalExecutionTimeInNanos());
    operatorStatistics.setNextCalledCount(operatorContext.getNextCalledCount());
    operatorStatistics.setHasNextCalledCount(operatorContext.getHasNextCalledCount());
    operatorStatistics.setOutputRows(operatorContext.getOutputRows());
    operatorStatistics.setSpecifiedInfo(operatorContext.getSpecifiedInfo());
    operatorStatistics.setMemoryUsage(operatorContext.getEstimatedMemorySize());
  }

  // Directly build statistics from FragmentInstanceExecution, which is still running.
  public TFetchFragmentInstanceStatisticsResp buildStatistics() {
    TFetchFragmentInstanceStatisticsResp statistics = new TFetchFragmentInstanceStatisticsResp();

    boolean res = fillFragmentInstanceStatistics(context, statistics);
    if (!res) {
      return statistics;
    }

    Map<String, TOperatorStatistics> operatorStatisticsMap = new HashMap<>();
    Map<String, Integer> operatorCountMap = new HashMap<>();
    Map<String, String> leadOverloadOperators = new HashMap<>();
    boolean merge = false;
    // Currently, they should be the drivers for each pipeline
    for (IDriver driver : drivers) {
      merge =
          fillFragmentInstanceStatistics(
              driver.getDriverContext().getOperatorContexts(),
              operatorStatisticsMap,
              operatorCountMap,
              leadOverloadOperators,
              merge);
    }

    if (merge) {
      Map<String, TOperatorStatistics> newOperatorStatisticsMap = new HashMap<>();
      for (Map.Entry<String, String> entry : leadOverloadOperators.entrySet()) {
        newOperatorStatisticsMap.put(entry.getValue(), operatorStatisticsMap.get(entry.getValue()));
      }
      statistics.setOperatorStatisticsMap(newOperatorStatisticsMap);
    } else {
      statistics.setOperatorStatisticsMap(operatorStatisticsMap);
    }

    mergeOperatorStatisticsIfDuplicate(statistics.getOperatorStatisticsMap());

    return statistics;
  }

  // this is a separate method to ensure that the `this` reference is not leaked during construction
  @SuppressWarnings("squid:S1181")
  private void initialize(IDriverScheduler scheduler, boolean isExplainAnalyze) {
    stateMachine.addStateChangeListener(
        newState -> {
          try (SetThreadName threadName = new SetThreadName(instanceId.getFullId())) {
            if (!newState.isDone()) {
              return;
            }

            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("Enter the stateChangeListener");
            }

            statisticsLock.writeLock().lock();
            // Store statistics in context for EXPLAIN ANALYZE to avoid being released.
            if (isExplainAnalyze) {
              context.setFragmentInstanceStatistics(buildStatistics());
            }
            staticsRemoved = true;
            statisticsLock.writeLock().unlock();

            clearShuffleSinkHandle(newState);

            // delete tmp file if exists
            deleteTmpFile();

            // close the driver after sink is aborted or closed because in driver.close() it
            // will try to call ISink.setNoMoreTsBlocks()
            for (IDriver driver : drivers) {
              driver.close();
            }
            // help for gc
            drivers = null;

            // release file handlers
            context.releaseResourceWhenAllDriversAreClosed();

            // release memory
            exchangeManager.deRegisterFragmentInstanceFromMemoryPool(
                instanceId.getQueryId().getId(), instanceId.getFragmentInstanceId(), true);

            context.releaseMemoryReservationManager();

            if (newState.isFailed()) {
              scheduler.abortFragmentInstance(instanceId);
            }
          } catch (Throwable t) {
            try (SetThreadName threadName = new SetThreadName(instanceId.getFullId())) {
              LOGGER.error(
                  "Errors occurred while attempting to finish the FI process, potentially leading to resource leakage.",
                  t);
            }
          }
        });
  }

  private void clearShuffleSinkHandle(FragmentInstanceState newState) {
    if (newState.isFailed()) {
      sink.abort();
    } else {
      sink.close();
    }
    // help for gc
    sink = null;
  }

  private void deleteTmpFile() {
    if (context.mayHaveTmpFile()) {
      String tmpFilePath =
          IoTDBDescriptor.getInstance().getConfig().getSortTmpDir()
              + File.separator
              + context.getId().getFullId()
              + File.separator;
      File tmpFile = new File(tmpFilePath);
      if (tmpFile.exists()) {
        FileUtils.deleteFileOrDirectory(tmpFile, true);
      }
    }
  }

  public boolean isStaticsRemoved() {
    return staticsRemoved;
  }

  public void lockStatistics() {
    statisticsLock.readLock().lock();
  }

  public void unlockStatistics() {
    statisticsLock.readLock().unlock();
  }
}
