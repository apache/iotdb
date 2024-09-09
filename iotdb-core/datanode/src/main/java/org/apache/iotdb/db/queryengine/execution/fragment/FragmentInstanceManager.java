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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.driver.IDriver;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISink;
import org.apache.iotdb.db.queryengine.execution.schedule.DriverScheduler;
import org.apache.iotdb.db.queryengine.execution.schedule.IDriverScheduler;
import org.apache.iotdb.db.queryengine.metric.QueryExecutionMetricSet;
import org.apache.iotdb.db.queryengine.metric.QueryRelatedResourceMetricSet;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.planner.PipelineDriverFactory;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.storageengine.dataregion.IDataRegionForQuery;
import org.apache.iotdb.db.utils.SetThreadName;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceStatisticsResp;

import io.airlift.units.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceExecution.createFragmentInstanceExecution;
import static org.apache.iotdb.db.queryengine.execution.schedule.queue.IndexedBlockingQueue.TOO_MANY_CONCURRENT_QUERIES_ERROR_MSG;
import static org.apache.iotdb.db.queryengine.metric.QueryExecutionMetricSet.LOCAL_EXECUTION_PLANNER;
import static org.apache.iotdb.rpc.TSStatusCode.TOO_MANY_CONCURRENT_QUERIES_ERROR;

@SuppressWarnings("squid:S6548")
public class FragmentInstanceManager {

  private static final Logger logger = LoggerFactory.getLogger(FragmentInstanceManager.class);

  private final Map<FragmentInstanceId, FragmentInstanceContext> instanceContext;
  private final Map<FragmentInstanceId, FragmentInstanceExecution> instanceExecution;
  private final Map<QueryId, DataNodeQueryContext> dataNodeQueryContextMap;
  private final LocalExecutionPlanner planner = LocalExecutionPlanner.getInstance();
  private final IDriverScheduler scheduler = DriverScheduler.getInstance();

  private final ScheduledExecutorService instanceManagementExecutor;
  public final ExecutorService instanceNotificationExecutor;

  private final Duration infoCacheTime;

  private final ExecutorService intoOperationExecutor;
  private final ExecutorService modelInferenceExecutor;

  private final MPPDataExchangeManager exchangeManager =
      MPPDataExchangeService.getInstance().getMPPDataExchangeManager();

  private static final QueryExecutionMetricSet QUERY_EXECUTION_METRIC_SET =
      QueryExecutionMetricSet.getInstance();

  public static FragmentInstanceManager getInstance() {
    return FragmentInstanceManager.InstanceHolder.INSTANCE;
  }

  private FragmentInstanceManager() {
    this.instanceContext = new ConcurrentHashMap<>();
    this.instanceExecution = new ConcurrentHashMap<>();
    this.dataNodeQueryContextMap = new ConcurrentHashMap<>();
    this.instanceManagementExecutor =
        IoTDBThreadPoolFactory.newScheduledThreadPool(
            1, ThreadName.FRAGMENT_INSTANCE_MANAGEMENT.getName());
    this.instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(
            4, ThreadName.FRAGMENT_INSTANCE_NOTIFICATION.getName());

    this.infoCacheTime = new Duration(5, TimeUnit.MINUTES);

    ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
        instanceManagementExecutor, this::removeOldInstances, 2000, 2000, TimeUnit.MILLISECONDS);
    ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
        instanceManagementExecutor,
        this::cancelTimeoutFlushingInstances,
        2000,
        2000,
        TimeUnit.MILLISECONDS);

    this.intoOperationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(
            IoTDBDescriptor.getInstance().getConfig().getIntoOperationExecutionThreadCount(),
            "into-operation-executor");

    this.modelInferenceExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(
            CommonDescriptor.getInstance().getConfig().getModelInferenceExecutionThreadCount(),
            "model-inference-executor");
  }

  @SuppressWarnings("squid:S1181")
  public FragmentInstanceInfo execDataQueryFragmentInstance(
      FragmentInstance instance, IDataRegionForQuery dataRegion) {
    long startTime = System.nanoTime();
    FragmentInstanceId instanceId = instance.getId();
    AtomicLong driversCount = new AtomicLong();
    try (SetThreadName fragmentInstanceName = new SetThreadName(instanceId.getFullId())) {
      FragmentInstanceExecution execution =
          instanceExecution.computeIfAbsent(
              instanceId,
              id -> {
                FragmentInstanceStateMachine stateMachine =
                    new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);

                int dataNodeFINum = instance.getDataNodeFINum();
                DataNodeQueryContext dataNodeQueryContext =
                    getOrCreateDataNodeQueryContext(instanceId.getQueryId(), dataNodeFINum);

                FragmentInstanceContext context =
                    instanceContext.computeIfAbsent(
                        instanceId,
                        fragmentInstanceId ->
                            createFragmentInstanceContext(
                                fragmentInstanceId,
                                stateMachine,
                                instance.getSessionInfo(),
                                dataRegion,
                                instance.getGlobalTimePredicate(),
                                dataNodeQueryContextMap));

                try {
                  List<PipelineDriverFactory> driverFactories =
                      planner.plan(
                          instance.getFragment().getPlanNodeTree(),
                          instance.getFragment().getTypeProvider(),
                          context,
                          dataNodeQueryContext);

                  List<IDriver> drivers = new ArrayList<>();
                  driverFactories.forEach(factory -> drivers.add(factory.createDriver()));
                  // For ShowQueries related instances, isHighestPriority == true
                  if (instance.isHighestPriority()) {
                    drivers.forEach(driver -> driver.setHighestPriority(true));
                  }

                  context.initializeNumOfDrivers(drivers.size());
                  // get the sink of last driver
                  ISink sink = drivers.get(drivers.size() - 1).getSink();

                  driversCount.addAndGet(drivers.size());
                  return createFragmentInstanceExecution(
                      scheduler,
                      instanceId,
                      context,
                      drivers,
                      sink,
                      stateMachine,
                      instance.getTimeOut(),
                      instance.isExplainAnalyze(),
                      exchangeManager);
                } catch (Throwable t) {
                  clearFIRelatedResources(instanceId);
                  // deal with
                  if (t instanceof IllegalStateException
                      && TOO_MANY_CONCURRENT_QUERIES_ERROR_MSG.equals(t.getMessage())) {
                    logger.warn(TOO_MANY_CONCURRENT_QUERIES_ERROR_MSG);
                    stateMachine.failed(
                        new IoTDBException(
                            TOO_MANY_CONCURRENT_QUERIES_ERROR_MSG,
                            TOO_MANY_CONCURRENT_QUERIES_ERROR.getStatusCode()));
                  } else if (t instanceof IoTDBRuntimeException) {
                    stateMachine.failed(t);
                  } else {
                    logger.warn("error when create FragmentInstanceExecution.", t);
                    stateMachine.failed(t);
                  }
                  return null;
                }
              });

      if (execution != null) {
        execution
            .getStateMachine()
            .addStateChangeListener(
                newState -> {
                  if (newState.isDone()) {
                    instanceExecution.remove(instanceId);
                  }
                });
        return execution.getInstanceInfo();
      } else {
        return createFailedInstanceInfo(instanceId);
      }
    } finally {
      QueryRelatedResourceMetricSet.getInstance()
          .updateFragmentInstanceCount(
              instanceContext.size(), instanceExecution.size(), driversCount.get());
      QUERY_EXECUTION_METRIC_SET.recordExecutionCost(
          LOCAL_EXECUTION_PLANNER, System.nanoTime() - startTime);
    }
  }

  private void clearFIRelatedResources(FragmentInstanceId instanceId) {
    // close and remove all the handles of the fragment instance
    exchangeManager.forceDeregisterFragmentInstance(instanceId.toThrift());
    // clear MemoryPool
    exchangeManager.deRegisterFragmentInstanceFromMemoryPool(
        instanceId.getQueryId().getId(), instanceId.getFragmentInstanceId(), false);
  }

  private DataNodeQueryContext getOrCreateDataNodeQueryContext(QueryId queryId, int dataNodeFINum) {
    return dataNodeQueryContextMap.computeIfAbsent(
        queryId, queryId1 -> new DataNodeQueryContext(dataNodeFINum));
  }

  @SuppressWarnings("squid:S1181")
  public FragmentInstanceInfo execSchemaQueryFragmentInstance(
      FragmentInstance instance, ISchemaRegion schemaRegion) {
    FragmentInstanceId instanceId = instance.getId();
    FragmentInstanceExecution execution =
        instanceExecution.computeIfAbsent(
            instanceId,
            id -> {
              FragmentInstanceStateMachine stateMachine =
                  new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);

              FragmentInstanceContext context =
                  instanceContext.computeIfAbsent(
                      instanceId,
                      fragmentInstanceId ->
                          createFragmentInstanceContext(
                              fragmentInstanceId, stateMachine, instance.getSessionInfo()));

              try {
                List<PipelineDriverFactory> driverFactories =
                    planner.plan(
                        instance.getFragment().getPlanNodeTree(),
                        instance.getFragment().getTypeProvider(),
                        context,
                        schemaRegion);

                List<IDriver> drivers = new ArrayList<>();
                driverFactories.forEach(factory -> drivers.add(factory.createDriver()));
                context.initializeNumOfDrivers(drivers.size());
                // get the sink of last driver
                ISink sink = drivers.get(drivers.size() - 1).getSink();

                return createFragmentInstanceExecution(
                    scheduler,
                    instanceId,
                    context,
                    drivers,
                    sink,
                    stateMachine,
                    instance.getTimeOut(),
                    false,
                    exchangeManager);
              } catch (Throwable t) {
                clearFIRelatedResources(instanceId);
                // deal with
                if (t instanceof IllegalStateException
                    && TOO_MANY_CONCURRENT_QUERIES_ERROR_MSG.equals(t.getMessage())) {
                  logger.warn(TOO_MANY_CONCURRENT_QUERIES_ERROR_MSG);
                  stateMachine.failed(
                      new IoTDBException(
                          TOO_MANY_CONCURRENT_QUERIES_ERROR_MSG,
                          TOO_MANY_CONCURRENT_QUERIES_ERROR.getStatusCode()));
                } else {
                  logger.warn("Execute error caused by ", t);
                  stateMachine.failed(t);
                }
                return null;
              }
            });
    if (execution != null) {
      execution
          .getStateMachine()
          .addStateChangeListener(
              newState -> {
                if (newState.isDone()) {
                  instanceExecution.remove(instanceId);
                }
              });
      return execution.getInstanceInfo();
    } else {
      return createFailedInstanceInfo(instanceId);
    }
  }

  /** Aborts a FragmentInstance. keep FragmentInstanceContext for later state tracking */
  public FragmentInstanceInfo abortFragmentInstance(FragmentInstanceId fragmentInstanceId) {
    instanceExecution.remove(fragmentInstanceId);
    FragmentInstanceContext context = instanceContext.get(fragmentInstanceId);
    if (context != null) {
      context.abort();
      return context.getInstanceInfo();
    }
    return null;
  }

  /** Cancels a FragmentInstance. */
  public FragmentInstanceInfo cancelTask(FragmentInstanceId instanceId, boolean hasThrowable) {
    logger.debug("[CancelFI]");
    requireNonNull(instanceId, "taskId is null");

    FragmentInstanceContext context = instanceContext.remove(instanceId);
    if (context != null) {
      instanceExecution.remove(instanceId);
      if (hasThrowable) {
        context.cancel();
      } else {
        context.finished();
      }
      return context.getInstanceInfo();
    }
    return null;
  }

  /**
   * Gets the info for the specified fragment instance.
   *
   * <p>NOTE: this design assumes that only fragment instances that will eventually exist are
   * queried.
   */
  public FragmentInstanceInfo getInstanceInfo(FragmentInstanceId instanceId) {
    requireNonNull(instanceId, "instanceId is null");
    FragmentInstanceContext context = instanceContext.get(instanceId);
    if (context == null) {
      return null;
    }
    return context.getInstanceInfo();
  }

  public TFetchFragmentInstanceStatisticsResp getFragmentInstanceStatistics(
      FragmentInstanceId instanceId) {
    requireNonNull(instanceId, "instanceId is null");
    // If the instance is still running, we directly get the statistics from instanceExecution
    FragmentInstanceExecution fragmentInstanceExecution = instanceExecution.get(instanceId);
    if (fragmentInstanceExecution != null) {
      try {
        fragmentInstanceExecution.lockStatistics();
        if (!fragmentInstanceExecution.isStaticsRemoved()) {
          return fragmentInstanceExecution.buildStatistics();
        }
      } finally {
        fragmentInstanceExecution.unlockStatistics();
      }
    }
    // If the instance has finished, we get the statistics which was cached in the instanceContext
    // when instanceExecution was removed.
    FragmentInstanceContext context = instanceContext.get(instanceId);
    if (context == null) {
      return new TFetchFragmentInstanceStatisticsResp();
    }
    TFetchFragmentInstanceStatisticsResp statisticsResp = context.getFragmentInstanceStatistics();
    return statisticsResp == null ? new TFetchFragmentInstanceStatisticsResp() : statisticsResp;
  }

  private FragmentInstanceInfo createFailedInstanceInfo(FragmentInstanceId instanceId) {
    FragmentInstanceContext context = instanceContext.get(instanceId);
    Optional<TSStatus> errorCode = context.getErrorCode();
    return errorCode
        .map(
            tsStatus ->
                new FragmentInstanceInfo(
                    FragmentInstanceState.FAILED,
                    context.getEndTime(),
                    context.getFailedCause(),
                    context.getFailureInfoList(),
                    tsStatus))
        .orElseGet(
            () ->
                new FragmentInstanceInfo(
                    FragmentInstanceState.FAILED,
                    context.getEndTime(),
                    context.getFailedCause(),
                    context.getFailureInfoList()));
  }

  private void removeOldInstances() {
    long oldestAllowedInstance = System.currentTimeMillis() - infoCacheTime.toMillis();
    instanceContext
        .entrySet()
        .removeIf(
            entry -> {
              long endTime = entry.getValue().getEndTime();
              return endTime != -1 && endTime <= oldestAllowedInstance;
            });
  }

  private void cancelTimeoutFlushingInstances() {
    long now = System.currentTimeMillis();
    instanceExecution.forEach(
        (key, execution) -> {
          if (execution.getStateMachine().getState() == FragmentInstanceState.FLUSHING
              && (now - execution.getStartTime()) > execution.getTimeoutInMs()) {
            execution
                .getStateMachine()
                .failed(
                    new TimeoutException(
                        "Query has executed more than " + execution.getTimeoutInMs() + "ms"));
          }
        });
  }

  public ExecutorService getIntoOperationExecutor() {
    return intoOperationExecutor;
  }

  public ExecutorService getModelInferenceExecutor() {
    return modelInferenceExecutor;
  }

  private static class InstanceHolder {

    private InstanceHolder() {}

    private static final FragmentInstanceManager INSTANCE = new FragmentInstanceManager();
  }
}
