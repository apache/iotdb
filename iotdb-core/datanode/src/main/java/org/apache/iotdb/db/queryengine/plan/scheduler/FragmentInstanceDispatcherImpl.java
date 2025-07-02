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

package org.apache.iotdb.db.queryengine.plan.scheduler;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.service.metric.PerformanceOverviewMetrics;
import org.apache.iotdb.consensus.exception.ConsensusGroupNotExistException;
import org.apache.iotdb.consensus.exception.RatisReadUnavailableException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.mpp.FragmentInstanceDispatchException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.execution.executor.RegionExecutionResult;
import org.apache.iotdb.db.queryengine.execution.executor.RegionReadExecutor;
import org.apache.iotdb.db.queryengine.execution.executor.RegionWriteExecutor;
import org.apache.iotdb.db.queryengine.metric.QueryExecutionMetricSet;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.SubPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableSchemaQuerySuccessfulCallbackVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableSchemaQueryWriteVisitor;
import org.apache.iotdb.db.utils.SetThreadName;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstance;
import org.apache.iotdb.mpp.rpc.thrift.TPlanNode;
import org.apache.iotdb.mpp.rpc.thrift.TSendBatchPlanNodeReq;
import org.apache.iotdb.mpp.rpc.thrift.TSendFragmentInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TSendFragmentInstanceResp;
import org.apache.iotdb.mpp.rpc.thrift.TSendSinglePlanNodeReq;
import org.apache.iotdb.mpp.rpc.thrift.TSendSinglePlanNodeResp;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.thrift.TException;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static org.apache.iotdb.db.queryengine.metric.QueryExecutionMetricSet.DISPATCH_READ;

public class FragmentInstanceDispatcherImpl implements IFragInstanceDispatcher {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(FragmentInstanceDispatcherImpl.class);

  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();
  private final QueryType type;
  private final MPPQueryContext queryContext;
  private final String localhostIpAddr;
  private final int localhostInternalPort;
  private final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient>
      syncInternalServiceClientManager;
  private final IClientManager<TEndPoint, AsyncDataNodeInternalServiceClient>
      asyncInternalServiceClientManager;

  private static final QueryExecutionMetricSet QUERY_EXECUTION_METRIC_SET =
      QueryExecutionMetricSet.getInstance();
  private static final PerformanceOverviewMetrics PERFORMANCE_OVERVIEW_METRICS =
      PerformanceOverviewMetrics.getInstance();

  private static final String DISPATCH_FAILED = "[DispatchFailed]";

  private static final String UNEXPECTED_ERRORS = "Unexpected errors: ";

  private final long maxRetryDurationInNs;

  public FragmentInstanceDispatcherImpl(
      QueryType type,
      MPPQueryContext queryContext,
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> syncInternalServiceClientManager,
      IClientManager<TEndPoint, AsyncDataNodeInternalServiceClient>
          asyncInternalServiceClientManager) {
    this.type = type;
    this.queryContext = queryContext;
    this.syncInternalServiceClientManager = syncInternalServiceClientManager;
    this.asyncInternalServiceClientManager = asyncInternalServiceClientManager;
    this.localhostIpAddr = IoTDBDescriptor.getInstance().getConfig().getInternalAddress();
    this.localhostInternalPort = IoTDBDescriptor.getInstance().getConfig().getInternalPort();
    this.maxRetryDurationInNs =
        COMMON_CONFIG.getRemoteWriteMaxRetryDurationInMs() > 0
            ? COMMON_CONFIG.getRemoteWriteMaxRetryDurationInMs() * 1_000_000L
            : 0;
  }

  @Override
  public Future<FragInstanceDispatchResult> dispatch(
      SubPlan root, List<FragmentInstance> instances) {
    if (type == QueryType.READ) {
      return instances.size() == 1 || root == null
          ? dispatchRead(instances)
          : topologicalParallelDispatchRead(root, instances);
    } else {
      return dispatchWrite(instances);
    }
  }

  private Future<FragInstanceDispatchResult> topologicalParallelDispatchRead(
      SubPlan root, List<FragmentInstance> instances) {
    long startTime = System.nanoTime();
    LinkedBlockingQueue<Pair<SubPlan, Boolean>> queue = new LinkedBlockingQueue<>(instances.size());
    List<Future<FragInstanceDispatchResult>> futures = new ArrayList<>(instances.size());
    queue.add(new Pair<>(root, true));
    try {
      while (futures.size() < instances.size()) {
        Pair<SubPlan, Boolean> pair = queue.take();
        SubPlan next = pair.getLeft();
        boolean previousSuccess = pair.getRight();
        if (!previousSuccess) {
          break;
        }
        FragmentInstance fragmentInstance =
            instances.get(next.getPlanFragment().getIndexInFragmentInstanceList());
        futures.add(asyncDispatchOneInstance(next, fragmentInstance, queue));
      }
      for (Future<FragInstanceDispatchResult> future : futures) {
        FragInstanceDispatchResult result = future.get();
        if (!result.isSuccessful()) {
          return immediateFuture(result);
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn("Interrupted when dispatching read async", e);
      return immediateFuture(
          new FragInstanceDispatchResult(
              RpcUtils.getStatus(
                  TSStatusCode.INTERNAL_SERVER_ERROR, "Interrupted errors: " + e.getMessage())));
    } catch (Throwable t) {
      LOGGER.warn(DISPATCH_FAILED, t);
      return immediateFuture(
          new FragInstanceDispatchResult(
              RpcUtils.getStatus(
                  TSStatusCode.INTERNAL_SERVER_ERROR, UNEXPECTED_ERRORS + t.getMessage())));
    } finally {
      long queryDispatchReadTime = System.nanoTime() - startTime;
      QUERY_EXECUTION_METRIC_SET.recordExecutionCost(DISPATCH_READ, queryDispatchReadTime);
      queryContext.recordDispatchCost(queryDispatchReadTime);
    }
    return immediateFuture(new FragInstanceDispatchResult(true));
  }

  private Future<FragInstanceDispatchResult> asyncDispatchOneInstance(
      SubPlan plan, FragmentInstance instance, LinkedBlockingQueue<Pair<SubPlan, Boolean>> queue) {
    return Coordinator.getInstance()
        .getDispatchExecutor()
        .submit(
            () -> {
              boolean success = false;
              try (SetThreadName threadName = new SetThreadName(instance.getId().getFullId())) {
                dispatchOneInstance(instance);
                success = true;
              } catch (FragmentInstanceDispatchException e) {
                return new FragInstanceDispatchResult(e.getFailureStatus());
              } catch (Throwable t) {
                LOGGER.warn(DISPATCH_FAILED, t);
                return new FragInstanceDispatchResult(
                    RpcUtils.getStatus(
                        TSStatusCode.INTERNAL_SERVER_ERROR, UNEXPECTED_ERRORS + t.getMessage()));
              } finally {
                if (!success) {
                  // In case of failure, only one notification needs to be sent to the outer layer.
                  // If the failure is not notified and the child FI is not sent, the outer loop
                  // won't be able to exit.
                  queue.add(new Pair<>(null, false));
                } else {
                  for (SubPlan child : plan.getChildren()) {
                    queue.add(new Pair<>(child, true));
                  }
                }
                // friendly for gc, clear the plan node tree, for some queries select all devices,
                // it will release lots of memory
                if (!queryContext.isExplainAnalyze()) {
                  // EXPLAIN ANALYZE will use these instances, so we can't clear them
                  instance.getFragment().clearUselessField();
                } else {
                  // TypeProvider is not used in EXPLAIN ANALYZE, so we can clear it
                  instance.getFragment().clearTypeProvider();
                }
              }
              return new FragInstanceDispatchResult(true);
            });
  }

  private Future<FragInstanceDispatchResult> dispatchRead(List<FragmentInstance> instances) {
    long startTime = System.nanoTime();

    try {
      for (FragmentInstance instance : instances) {
        try (SetThreadName threadName = new SetThreadName(instance.getId().getFullId())) {
          dispatchOneInstance(instance);
        } catch (FragmentInstanceDispatchException e) {
          return immediateFuture(new FragInstanceDispatchResult(e.getFailureStatus()));
        } catch (Throwable t) {
          LOGGER.warn(DISPATCH_FAILED, t);
          return immediateFuture(
              new FragInstanceDispatchResult(
                  RpcUtils.getStatus(
                      TSStatusCode.INTERNAL_SERVER_ERROR, UNEXPECTED_ERRORS + t.getMessage())));
        } finally {
          // friendly for gc, clear the plan node tree, for some queries select all devices, it will
          // release lots of memory
          if (!queryContext.isExplainAnalyze()) {
            // EXPLAIN ANALYZE will use these instances, so we can't clear them
            instance.getFragment().clearUselessField();
          } else {
            // TypeProvider is not used in EXPLAIN ANALYZE, so we can clear it
            instance.getFragment().clearTypeProvider();
          }
        }
      }

      return immediateFuture(new FragInstanceDispatchResult(true));
    } finally {
      long queryDispatchReadTime = System.nanoTime() - startTime;
      QUERY_EXECUTION_METRIC_SET.recordExecutionCost(DISPATCH_READ, queryDispatchReadTime);
      queryContext.recordDispatchCost(queryDispatchReadTime);
    }
  }

  /** Entrypoint for dispatching write fragment instances. */
  private Future<FragInstanceDispatchResult> dispatchWrite(List<FragmentInstance> instances) {
    final List<TSStatus> dispatchFailures = new ArrayList<>();
    int replicaNum = 0;

    // 1. do not dispatch if the RegionReplicaSet is empty
    final List<FragmentInstance> shouldDispatch = new ArrayList<>();
    for (final FragmentInstance instance : instances) {
      if (instance.getHostDataNode() == null
          || Optional.ofNullable(instance.getRegionReplicaSet())
                  .map(TRegionReplicaSet::getDataNodeLocationsSize)
                  .orElse(0)
              == 0) {
        dispatchFailures.add(
            new TSStatus(TSStatusCode.PLAN_FAILED_NETWORK_PARTITION.getStatusCode()));
      } else {
        replicaNum =
            Math.max(replicaNum, instance.getRegionReplicaSet().getDataNodeLocationsSize());
        shouldDispatch.add(instance);
      }
    }

    try {
      // 2. try the dispatch
      final List<FailedFragmentInstanceWithStatus> failedInstances =
          dispatchWriteOnce(shouldDispatch);

      // 3. decide if we need retry (we may decide the retry condition instance-wise, if needed)
      final boolean shouldRetry =
          !failedInstances.isEmpty() && maxRetryDurationInNs > 0 && replicaNum > 1;
      if (!shouldRetry) {
        failedInstances.forEach(fi -> dispatchFailures.add(fi.getFailureStatus()));
      } else {
        // 4. retry the instance on other replicas
        final List<FragmentInstance> retryInstances =
            failedInstances.stream()
                .map(FailedFragmentInstanceWithStatus::getInstance)
                .collect(Collectors.toList());
        // here we only retry over each replica once
        final List<FailedFragmentInstanceWithStatus> failedAfterRetry =
            dispatchRetryWrite(retryInstances, replicaNum);
        failedAfterRetry.forEach(fi -> dispatchFailures.add(fi.getFailureStatus()));
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.error("Interrupted when dispatching write async", e);
      return immediateFuture(
          new FragInstanceDispatchResult(
              RpcUtils.getStatus(
                  TSStatusCode.INTERNAL_SERVER_ERROR, "Interrupted errors: " + e.getMessage())));
    }

    if (dispatchFailures.isEmpty()) {
      return immediateFuture(new FragInstanceDispatchResult(true));
    }
    if (instances.size() == 1) {
      return immediateFuture(new FragInstanceDispatchResult(dispatchFailures.get(0)));
    } else {
      List<TSStatus> failureStatusList = new ArrayList<>();
      for (TSStatus dataNodeFailure : dispatchFailures) {
        if (dataNodeFailure.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
          failureStatusList.addAll(dataNodeFailure.getSubStatus());
        } else {
          failureStatusList.add(dataNodeFailure);
        }
      }
      return immediateFuture(new FragInstanceDispatchResult(RpcUtils.getStatus(failureStatusList)));
    }
  }

  /**
   * Dispatch the given write instances once. It will dispatch the given instances locally or
   * remotely, give the host datanode.
   */
  private List<FailedFragmentInstanceWithStatus> dispatchWriteOnce(List<FragmentInstance> instances)
      throws InterruptedException {
    if (instances.isEmpty()) {
      return Collections.emptyList();
    }

    final List<FragmentInstance> localInstances = new ArrayList<>();
    final List<FragmentInstance> remoteInstances = new ArrayList<>();
    for (FragmentInstance instance : instances) {
      if (isDispatchedToLocal(instance.getHostDataNode().getInternalEndPoint())) {
        localInstances.add(instance);
      } else {
        remoteInstances.add(instance);
      }
    }

    final List<FailedFragmentInstanceWithStatus> failedFragmentInstanceWithStatuses =
        new ArrayList<>();

    // 1. async dispatch to remote
    final AsyncPlanNodeSender asyncPlanNodeSender =
        new AsyncPlanNodeSender(asyncInternalServiceClientManager, remoteInstances);
    asyncPlanNodeSender.sendAll();

    // 2. sync dispatch to local
    if (!localInstances.isEmpty()) {
      long localScheduleStartTime = System.nanoTime();
      for (FragmentInstance localInstance : localInstances) {
        try (SetThreadName ignored = new SetThreadName(localInstance.getId().getFullId())) {
          dispatchLocally(localInstance);
        } catch (FragmentInstanceDispatchException e) {
          failedFragmentInstanceWithStatuses.add(
              new FailedFragmentInstanceWithStatus(localInstance, e.getFailureStatus()));
        } catch (Throwable t) {
          LOGGER.warn(DISPATCH_FAILED, t);
          failedFragmentInstanceWithStatuses.add(
              new FailedFragmentInstanceWithStatus(
                  localInstance,
                  RpcUtils.getStatus(
                      TSStatusCode.INTERNAL_SERVER_ERROR, UNEXPECTED_ERRORS + t.getMessage())));
        }
      }
      PERFORMANCE_OVERVIEW_METRICS.recordScheduleLocalCost(
          System.nanoTime() - localScheduleStartTime);
    }

    // 3. wait for remote dispatch results
    asyncPlanNodeSender.waitUntilCompleted();

    // 4. collect remote dispatch results
    failedFragmentInstanceWithStatuses.addAll(asyncPlanNodeSender.getFailedInstancesWithStatuses());

    return failedFragmentInstanceWithStatuses;
  }

  private List<FailedFragmentInstanceWithStatus> dispatchRetryWrite(
      List<FragmentInstance> retriedInstances, int maxRetryAttempts) throws InterruptedException {
    Preconditions.checkArgument(maxRetryAttempts > 0);

    final long retryStartTime = System.nanoTime();
    int retryAttempt = 0;
    List<FragmentInstance> nextDispatch = new ArrayList<>(retriedInstances);
    List<FailedFragmentInstanceWithStatus> failedFragmentInstanceWithStatuses =
        Collections.emptyList();

    while (retryAttempt < maxRetryAttempts) {
      // 1. let's retry on next replica location
      nextDispatch.forEach(FragmentInstance::getNextRetriedHostDataNode);

      // 2. dispatch the instances
      failedFragmentInstanceWithStatuses = dispatchWriteOnce(nextDispatch);

      // 3. decide if to continue the retry
      final long waitMillis = getRetrySleepTime(retryAttempt);
      if (failedFragmentInstanceWithStatuses.isEmpty()
          || waitMillis + System.nanoTime() >= retryStartTime + maxRetryDurationInNs) {
        break;
      }

      // 4. sleep and do the next retry
      Thread.sleep(waitMillis);
      PERFORMANCE_OVERVIEW_METRICS.recordRemoteRetrySleepCost(waitMillis * 1_000_000L);
      retryAttempt++;
      nextDispatch =
          failedFragmentInstanceWithStatuses.stream()
              .map(FailedFragmentInstanceWithStatus::getInstance)
              .collect(Collectors.toList());
    }

    return failedFragmentInstanceWithStatuses;
  }

  private long getRetrySleepTime(int retryTimes) {
    return Math.min(
        (long) (TimeUnit.MILLISECONDS.toMillis(100) * Math.pow(2, retryTimes)),
        TimeUnit.SECONDS.toMillis(20));
  }

  private void dispatchOneInstance(FragmentInstance instance)
      throws FragmentInstanceDispatchException {
    TEndPoint endPoint = instance.getHostDataNode().getInternalEndPoint();
    if (isDispatchedToLocal(endPoint)) {
      dispatchLocally(instance);
    } else {
      dispatchRemote(instance, endPoint);
    }
  }

  private boolean isDispatchedToLocal(TEndPoint endPoint) {
    return this.localhostIpAddr.equals(endPoint.getIp()) && localhostInternalPort == endPoint.port;
  }

  private void dispatchRemoteHelper(final FragmentInstance instance, final TEndPoint endPoint)
      throws FragmentInstanceDispatchException,
          TException,
          ClientManagerException,
          RatisReadUnavailableException,
          ConsensusGroupNotExistException {
    try (final SyncDataNodeInternalServiceClient client =
        syncInternalServiceClientManager.borrowClient(endPoint)) {
      switch (instance.getType()) {
        case READ:
          final TSendFragmentInstanceReq sendFragmentInstanceReq =
              new TSendFragmentInstanceReq(new TFragmentInstance(instance.serializeToByteBuffer()));
          if (instance.getExecutorType().isStorageExecutor()) {
            sendFragmentInstanceReq.setConsensusGroupId(
                instance.getRegionReplicaSet().getRegionId());
          }
          final TSendFragmentInstanceResp sendFragmentInstanceResp =
              client.sendFragmentInstance(sendFragmentInstanceReq);
          if (!sendFragmentInstanceResp.accepted) {
            LOGGER.warn(sendFragmentInstanceResp.message);
            if (sendFragmentInstanceResp.isSetNeedRetry()
                && sendFragmentInstanceResp.isNeedRetry()
                && sendFragmentInstanceResp.status != null) {
              if (sendFragmentInstanceResp.status.getCode()
                  == TSStatusCode.RATIS_READ_UNAVAILABLE.getStatusCode()) {
                throw new RatisReadUnavailableException(sendFragmentInstanceResp.message);
              } else if (sendFragmentInstanceResp.status.getCode()
                  == TSStatusCode.CONSENSUS_GROUP_NOT_EXIST.getStatusCode()) {
                throw new ConsensusGroupNotExistException(sendFragmentInstanceResp.message);
              } else {
                throw new FragmentInstanceDispatchException(sendFragmentInstanceResp.status);
              }
            } else if (sendFragmentInstanceResp.status != null) {
              throw new FragmentInstanceDispatchException(sendFragmentInstanceResp.status);
            }
            throw new FragmentInstanceDispatchException(
                RpcUtils.getStatus(
                    TSStatusCode.EXECUTE_STATEMENT_ERROR, sendFragmentInstanceResp.message));
          } else if (Objects.nonNull(sendFragmentInstanceReq.getConsensusGroupId())) {
            // Assume the consensus group casting will not generate any exceptions
            new TableSchemaQuerySuccessfulCallbackVisitor()
                .processFragment(
                    instance,
                    ConsensusGroupId.Factory.createFromTConsensusGroupId(
                        sendFragmentInstanceReq.getConsensusGroupId()));
          }
          break;
        case WRITE:
          final TSendBatchPlanNodeReq sendPlanNodeReq =
              new TSendBatchPlanNodeReq(
                  Collections.singletonList(
                      new TSendSinglePlanNodeReq(
                          new TPlanNode(
                              instance.getFragment().getPlanNodeTree().serializeToByteBuffer()),
                          instance.getRegionReplicaSet().getRegionId())));
          final TSendSinglePlanNodeResp sendPlanNodeResp =
              client.sendBatchPlanNode(sendPlanNodeReq).getResponses().get(0);
          if (!sendPlanNodeResp.accepted) {
            if (sendPlanNodeResp.getStatus() == null) {
              throw new FragmentInstanceDispatchException(
                  RpcUtils.getStatus(
                      TSStatusCode.WRITE_PROCESS_ERROR, sendPlanNodeResp.getMessage()));
            } else {
              // DO NOT LOG READ_ONLY ERROR
              if (sendPlanNodeResp.getStatus().getCode()
                  != TSStatusCode.SYSTEM_READ_ONLY.getStatusCode()) {
                LOGGER.warn(
                    "Dispatch write failed. status: {}, code: {}, message: {}, node {}",
                    sendPlanNodeResp.status,
                    TSStatusCode.representOf(sendPlanNodeResp.status.code),
                    sendPlanNodeResp.message,
                    endPoint);
              }
              throw new FragmentInstanceDispatchException(sendPlanNodeResp.getStatus());
            }
          } else {
            // some expected and accepted status except SUCCESS_STATUS need to be returned
            final TSStatus status = sendPlanNodeResp.getStatus();
            if (status != null && status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
              throw new FragmentInstanceDispatchException(status);
            }
          }
          break;
        default:
          throw new FragmentInstanceDispatchException(
              RpcUtils.getStatus(
                  TSStatusCode.EXECUTE_STATEMENT_ERROR,
                  String.format("unknown read type [%s]", instance.getType())));
      }
    }
  }

  private void dispatchRemoteFailed(TEndPoint endPoint, Exception e)
      throws FragmentInstanceDispatchException {
    LOGGER.warn(
        "can't execute request on node  {} in second try, error msg is {}.",
        endPoint,
        ExceptionUtils.getRootCause(e).toString());
    TSStatus status = new TSStatus();
    status.setCode(TSStatusCode.DISPATCH_ERROR.getStatusCode());
    status.setMessage("can't connect to node " + endPoint);
    // If the DataNode cannot be connected, its endPoint will be put into black list
    // so that the following retry will avoid dispatching instance towards this DataNode.
    queryContext.addFailedEndPoint(endPoint);
    throw new FragmentInstanceDispatchException(status);
  }

  private void dispatchRemote(FragmentInstance instance, TEndPoint endPoint)
      throws FragmentInstanceDispatchException {

    try {
      dispatchRemoteHelper(instance, endPoint);
    } catch (ClientManagerException | TException | RatisReadUnavailableException e) {
      LOGGER.warn(
          "can't execute request on node {}, error msg is {}, and we try to reconnect this node.",
          endPoint,
          ExceptionUtils.getRootCause(e).toString());
      // we just retry once to clear stale connection for a restart node.
      try {
        dispatchRemoteHelper(instance, endPoint);
      } catch (ClientManagerException
          | TException
          | RatisReadUnavailableException
          | ConsensusGroupNotExistException e1) {
        dispatchRemoteFailed(endPoint, e1);
      }
    } catch (ConsensusGroupNotExistException e) {
      dispatchRemoteFailed(endPoint, e);
    }
  }

  private void dispatchLocally(final FragmentInstance instance)
      throws FragmentInstanceDispatchException {
    // deserialize ConsensusGroupId
    ConsensusGroupId groupId = null;
    if (instance.getExecutorType().isStorageExecutor()) {
      try {
        groupId =
            ConsensusGroupId.Factory.createFromTConsensusGroupId(
                instance.getRegionReplicaSet().getRegionId());
      } catch (final Throwable t) {
        LOGGER.warn("Deserialize ConsensusGroupId failed. ", t);
        throw new FragmentInstanceDispatchException(
            RpcUtils.getStatus(
                TSStatusCode.EXECUTE_STATEMENT_ERROR,
                "Deserialize ConsensusGroupId failed: " + t.getMessage()));
      }
    }

    switch (instance.getType()) {
      case READ:
        RegionExecutionResult executionResult = null;
        if (Objects.nonNull(groupId)) {
          // For schema query, there may be a "write" before "read"
          // the result is not null iff the "write" has failed
          executionResult = new TableSchemaQueryWriteVisitor().processFragment(instance, groupId);
        }
        if (Objects.isNull(executionResult)) {
          final RegionReadExecutor readExecutor = new RegionReadExecutor();
          executionResult =
              groupId == null
                  ? readExecutor.execute(instance)
                  : readExecutor.execute(groupId, instance);
        }
        if (!executionResult.isAccepted()) {
          LOGGER.warn(executionResult.getMessage());
          if (executionResult.isReadNeedRetry()) {
            if (executionResult.getStatus() != null) {
              throw new FragmentInstanceDispatchException(executionResult.getStatus());
            }
            throw new FragmentInstanceDispatchException(
                RpcUtils.getStatus(TSStatusCode.DISPATCH_ERROR, executionResult.getMessage()));
          } else if (executionResult.getStatus() != null) {
            throw new FragmentInstanceDispatchException(executionResult.getStatus());
          } else {
            throw new FragmentInstanceDispatchException(
                RpcUtils.getStatus(
                    TSStatusCode.EXECUTE_STATEMENT_ERROR, executionResult.getMessage()));
          }
        } else if (Objects.nonNull(groupId)) {
          // Assume the consensus group casting will not generate any exceptions
          new TableSchemaQuerySuccessfulCallbackVisitor().processFragment(instance, groupId);
        }
        break;
      case WRITE:
        final PlanNode planNode = instance.getFragment().getPlanNodeTree();
        final RegionWriteExecutor writeExecutor = new RegionWriteExecutor();
        final RegionExecutionResult writeResult = writeExecutor.execute(groupId, planNode);
        if (!writeResult.isAccepted()) {
          if (writeResult.getStatus() == null) {
            throw new FragmentInstanceDispatchException(
                RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, writeResult.getMessage()));
          } else {
            // DO NOT LOG READ_ONLY ERROR
            if (writeResult.getStatus().getCode()
                != TSStatusCode.SYSTEM_READ_ONLY.getStatusCode()) {
              LOGGER.warn(
                  "write locally failed. TSStatus: {}, message: {}",
                  writeResult.getStatus(),
                  writeResult.getMessage());
            }
            throw new FragmentInstanceDispatchException(writeResult.getStatus());
          }
        } else {
          // Some expected and accepted status except SUCCESS_STATUS need to be returned
          final TSStatus status = writeResult.getStatus();
          if (status != null && status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            throw new FragmentInstanceDispatchException(status);
          }
        }
        break;
      default:
        throw new FragmentInstanceDispatchException(
            RpcUtils.getStatus(
                TSStatusCode.EXECUTE_STATEMENT_ERROR,
                String.format("unknown read type [%s]", instance.getType())));
    }
  }

  @Override
  public void abort() {}
}
