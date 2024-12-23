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
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static org.apache.iotdb.db.queryengine.metric.QueryExecutionMetricSet.DISPATCH_READ;

public class FragmentInstanceDispatcherImpl implements IFragInstanceDispatcher {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(FragmentInstanceDispatcherImpl.class);

  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();

  private final ExecutorService executor;
  private final ExecutorService writeOperationExecutor;
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

  public FragmentInstanceDispatcherImpl(
      QueryType type,
      MPPQueryContext queryContext,
      ExecutorService executor,
      ExecutorService writeOperationExecutor,
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> syncInternalServiceClientManager,
      IClientManager<TEndPoint, AsyncDataNodeInternalServiceClient>
          asyncInternalServiceClientManager) {
    this.type = type;
    this.queryContext = queryContext;
    this.executor = executor;
    this.writeOperationExecutor = writeOperationExecutor;
    this.syncInternalServiceClientManager = syncInternalServiceClientManager;
    this.asyncInternalServiceClientManager = asyncInternalServiceClientManager;
    this.localhostIpAddr = IoTDBDescriptor.getInstance().getConfig().getInternalAddress();
    this.localhostInternalPort = IoTDBDescriptor.getInstance().getConfig().getInternalPort();
  }

  @Override
  public Future<FragInstanceDispatchResult> dispatch(List<FragmentInstance> instances) {
    if (type == QueryType.READ) {
      return dispatchRead(instances);
    } else {
      return dispatchWriteAsync(instances);
    }
  }

  // TODO: (xingtanzjr) currently we use a sequential dispatch policy for READ, which is
  //  unsafe for current FragmentInstance scheduler framework. We need to implement the
  //  topological dispatch according to dependency relations between FragmentInstances
  private Future<FragInstanceDispatchResult> dispatchRead(List<FragmentInstance> instances) {
    long startTime = System.nanoTime();
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

        long dispatchReadTime = System.nanoTime() - startTime;
        QUERY_EXECUTION_METRIC_SET.recordExecutionCost(DISPATCH_READ, dispatchReadTime);
        queryContext.recordDispatchCost(dispatchReadTime);
      }
    }
    return immediateFuture(new FragInstanceDispatchResult(true));
  }

  private Future<FragInstanceDispatchResult> dispatchWriteSync(List<FragmentInstance> instances) {
    List<TSStatus> failureStatusList = new ArrayList<>();
    for (FragmentInstance instance : instances) {
      try (SetThreadName threadName = new SetThreadName(instance.getId().getFullId())) {
        dispatchOneInstance(instance);
      } catch (FragmentInstanceDispatchException e) {
        TSStatus failureStatus = e.getFailureStatus();
        if (instances.size() == 1) {
          failureStatusList.add(failureStatus);
        } else {
          if (failureStatus.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
            failureStatusList.addAll(failureStatus.getSubStatus());
          } else {
            failureStatusList.add(failureStatus);
          }
        }
      } catch (Throwable t) {
        LOGGER.warn(DISPATCH_FAILED, t);
        failureStatusList.add(
            RpcUtils.getStatus(
                TSStatusCode.INTERNAL_SERVER_ERROR, UNEXPECTED_ERRORS + t.getMessage()));
      }
    }
    if (failureStatusList.isEmpty()) {
      return immediateFuture(new FragInstanceDispatchResult(true));
    } else {
      if (instances.size() == 1) {
        return immediateFuture(new FragInstanceDispatchResult(failureStatusList.get(0)));
      } else {
        return immediateFuture(
            new FragInstanceDispatchResult(RpcUtils.getStatus(failureStatusList)));
      }
    }
  }

  private Future<FragInstanceDispatchResult> dispatchWriteAsync(List<FragmentInstance> instances) {
    List<TSStatus> dataNodeFailureList = new ArrayList<>();
    // split local and remote instances
    List<FragmentInstance> localInstances = new ArrayList<>();
    List<FragmentInstance> remoteInstances = new ArrayList<>();
    for (FragmentInstance instance : instances) {
      TEndPoint endPoint = instance.getHostDataNode().getInternalEndPoint();
      if (isDispatchedToLocal(endPoint)) {
        localInstances.add(instance);
      } else {
        remoteInstances.add(instance);
      }
    }
    // async dispatch to remote
    AsyncPlanNodeSender asyncPlanNodeSender =
        new AsyncPlanNodeSender(asyncInternalServiceClientManager, remoteInstances);
    asyncPlanNodeSender.sendAll();

    if (!localInstances.isEmpty()) {
      // sync dispatch to local
      long localScheduleStartTime = System.nanoTime();
      for (FragmentInstance localInstance : localInstances) {
        try (SetThreadName threadName = new SetThreadName(localInstance.getId().getFullId())) {
          dispatchLocally(localInstance);
        } catch (FragmentInstanceDispatchException e) {
          dataNodeFailureList.add(e.getFailureStatus());
        } catch (Throwable t) {
          LOGGER.warn(DISPATCH_FAILED, t);
          dataNodeFailureList.add(
              RpcUtils.getStatus(
                  TSStatusCode.INTERNAL_SERVER_ERROR, UNEXPECTED_ERRORS + t.getMessage()));
        }
      }
      PERFORMANCE_OVERVIEW_METRICS.recordScheduleLocalCost(
          System.nanoTime() - localScheduleStartTime);
    }
    // wait until remote dispatch done
    try {
      asyncPlanNodeSender.waitUntilCompleted();
      final long maxRetryDurationInNs =
          COMMON_CONFIG.getRemoteWriteMaxRetryDurationInMs() > 0
              ? COMMON_CONFIG.getRemoteWriteMaxRetryDurationInMs() * 1_000_000L
              : 0;
      if (maxRetryDurationInNs > 0 && asyncPlanNodeSender.needRetry()) {
        // retry failed remote FIs
        int retryCount = 0;
        long waitMillis = getRetrySleepTime(retryCount);
        long retryStartTime = System.nanoTime();

        while (asyncPlanNodeSender.needRetry()) {
          retryCount++;
          asyncPlanNodeSender.retry();
          // if !(still need retry and current time + next sleep time < maxRetryDurationInNs)
          if (!(asyncPlanNodeSender.needRetry()
              && (System.nanoTime() - retryStartTime + waitMillis * 1_000_000L)
                  < maxRetryDurationInNs)) {
            break;
          }
          // still need to retry, sleep some time before make another retry.
          Thread.sleep(waitMillis);
          PERFORMANCE_OVERVIEW_METRICS.recordRemoteRetrySleepCost(waitMillis * 1_000_000L);
          waitMillis = getRetrySleepTime(retryCount);
        }
      }

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.error("Interrupted when dispatching write async", e);
      return immediateFuture(
          new FragInstanceDispatchResult(
              RpcUtils.getStatus(
                  TSStatusCode.INTERNAL_SERVER_ERROR, "Interrupted errors: " + e.getMessage())));
    }

    dataNodeFailureList.addAll(asyncPlanNodeSender.getFailureStatusList());

    if (dataNodeFailureList.isEmpty()) {
      return immediateFuture(new FragInstanceDispatchResult(true));
    }
    if (instances.size() == 1) {
      return immediateFuture(new FragInstanceDispatchResult(dataNodeFailureList.get(0)));
    } else {
      List<TSStatus> failureStatusList = new ArrayList<>();
      for (TSStatus dataNodeFailure : dataNodeFailureList) {
        if (dataNodeFailure.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
          failureStatusList.addAll(dataNodeFailure.getSubStatus());
        } else {
          failureStatusList.add(dataNodeFailure);
        }
      }
      return immediateFuture(new FragInstanceDispatchResult(RpcUtils.getStatus(failureStatusList)));
    }
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
              } else if (sendFragmentInstanceResp.status.getCode()
                  == TSStatusCode.TOO_MANY_CONCURRENT_QUERIES_ERROR.getStatusCode()) {
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
            if (executionResult.getStatus() != null
                && executionResult.getStatus().getCode()
                    == TSStatusCode.TOO_MANY_CONCURRENT_QUERIES_ERROR.getStatusCode()) {
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
