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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.service.metric.PerformanceOverviewMetrics;
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

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static org.apache.iotdb.db.queryengine.metric.QueryExecutionMetricSet.DISPATCH_READ;

public class FragmentInstanceDispatcherImpl implements IFragInstanceDispatcher {

  private static final Logger logger =
      LoggerFactory.getLogger(FragmentInstanceDispatcherImpl.class);
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

  private static final Lock QUEUE_LOCK = new ReentrantLock();

  private static final Map<Integer, ExecutorService> DATA_REGION_QUEUE_MAP = new HashMap<>();

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
    for (FragmentInstance instance : instances) {
      long startTime = System.nanoTime();
      try (SetThreadName threadName = new SetThreadName(instance.getId().getFullId())) {
        dispatchOneInstance(instance);
      } catch (FragmentInstanceDispatchException e) {
        return immediateFuture(new FragInstanceDispatchResult(e.getFailureStatus()));
      } catch (Throwable t) {
        logger.warn(DISPATCH_FAILED, t);
        return immediateFuture(
            new FragInstanceDispatchResult(
                RpcUtils.getStatus(
                    TSStatusCode.INTERNAL_SERVER_ERROR, UNEXPECTED_ERRORS + t.getMessage())));
      } finally {
        QUERY_EXECUTION_METRIC_SET.recordExecutionCost(
            DISPATCH_READ, System.nanoTime() - startTime);
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
        logger.warn(DISPATCH_FAILED, t);
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
      if (localInstances.get(0).getRegionReplicaSet().regionId.type
          == TConsensusGroupType.DataRegion) {
        List<Future<Void>> resList = new ArrayList<>(localInstances.size());
        QUEUE_LOCK.lock();
        try {
          for (FragmentInstance localInstance : localInstances) {
            try {
              int regionId = localInstance.getRegionReplicaSet().getRegionId().id;
              ExecutorService executorService = DATA_REGION_QUEUE_MAP.get(regionId);
              if (executorService == null) {
                executorService =
                    IoTDBThreadPoolFactory.newFixedThreadPool(
                        1, ThreadName.DATA_REGION_CONSUMER.getName() + "-" + regionId);
                DATA_REGION_QUEUE_MAP.put(regionId, executorService);
              }
              resList.add(executorService.submit(new writeFITask(localInstance)));
            } catch (Throwable t) {
              logger.warn(DISPATCH_FAILED, t);
              dataNodeFailureList.add(
                  RpcUtils.getStatus(
                      TSStatusCode.INTERNAL_SERVER_ERROR, UNEXPECTED_ERRORS + t.getMessage()));
            }
          }
        } finally {
          QUEUE_LOCK.unlock();
        }

        for (Future<Void> future : resList) {
          try {
            future.get();
          } catch (ExecutionException e) {
            if (e.getCause() instanceof FragmentInstanceDispatchException) {
              FragmentInstanceDispatchException cause =
                  (FragmentInstanceDispatchException) e.getCause();
              dataNodeFailureList.add(cause.getFailureStatus());
            } else {
              logger.warn(DISPATCH_FAILED, e);
              dataNodeFailureList.add(
                  RpcUtils.getStatus(
                      TSStatusCode.INTERNAL_SERVER_ERROR, UNEXPECTED_ERRORS + e.getMessage()));
            }
          } catch (Throwable e) {
            logger.warn(DISPATCH_FAILED, e);
            dataNodeFailureList.add(
                RpcUtils.getStatus(
                    TSStatusCode.INTERNAL_SERVER_ERROR, UNEXPECTED_ERRORS + e.getMessage()));
          }
        }
      } else {
        for (FragmentInstance localInstance : localInstances) {
          try (SetThreadName threadName = new SetThreadName(localInstance.getId().getFullId())) {
            dispatchLocally(localInstance);
          } catch (FragmentInstanceDispatchException e) {
            dataNodeFailureList.add(e.getFailureStatus());
          } catch (Throwable t) {
            logger.warn(DISPATCH_FAILED, t);
            dataNodeFailureList.add(
                RpcUtils.getStatus(
                    TSStatusCode.INTERNAL_SERVER_ERROR, UNEXPECTED_ERRORS + t.getMessage()));
          }
        }
      }

      PERFORMANCE_OVERVIEW_METRICS.recordScheduleLocalCost(
          System.nanoTime() - localScheduleStartTime);
    }
    // wait until remote dispatch done
    try {
      asyncPlanNodeSender.waitUntilCompleted();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.error("Interrupted when dispatching write async", e);
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

  private static class writeFITask implements Callable<Void> {

    private final FragmentInstance instance;

    public writeFITask(FragmentInstance instance) {
      this.instance = instance;
    }

    @Override
    public Void call() throws Exception {
      // deserialize ConsensusGroupId
      ConsensusGroupId groupId = null;
      if (instance.getExecutorType().isStorageExecutor()) {
        try {
          groupId =
              ConsensusGroupId.Factory.createFromTConsensusGroupId(
                  instance.getRegionReplicaSet().getRegionId());
        } catch (Throwable t) {
          logger.warn("Deserialize ConsensusGroupId failed. ", t);
          throw new FragmentInstanceDispatchException(
              RpcUtils.getStatus(
                  TSStatusCode.EXECUTE_STATEMENT_ERROR,
                  "Deserialize ConsensusGroupId failed: " + t.getMessage()));
        }
      }

      PlanNode planNode = instance.getFragment().getPlanNodeTree();
      RegionWriteExecutor writeExecutor = new RegionWriteExecutor();
      RegionExecutionResult writeResult = writeExecutor.execute(groupId, planNode);
      if (!writeResult.isAccepted()) {
        logger.warn(
            "write locally failed. TSStatus: {}, message: {}",
            writeResult.getStatus(),
            writeResult.getMessage());
        if (writeResult.getStatus() == null) {
          throw new FragmentInstanceDispatchException(
              RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, writeResult.getMessage()));
        } else {
          throw new FragmentInstanceDispatchException(writeResult.getStatus());
        }
      } else {
        // some expected and accepted status except SUCCESS_STATUS need to be returned
        TSStatus status = writeResult.getStatus();
        if (status != null && status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          throw new FragmentInstanceDispatchException(status);
        }
      }
      return null;
    }
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

  private void dispatchRemote(FragmentInstance instance, TEndPoint endPoint)
      throws FragmentInstanceDispatchException {
    try (SyncDataNodeInternalServiceClient client =
        syncInternalServiceClientManager.borrowClient(endPoint)) {
      switch (instance.getType()) {
        case READ:
          TSendFragmentInstanceReq sendFragmentInstanceReq =
              new TSendFragmentInstanceReq(new TFragmentInstance(instance.serializeToByteBuffer()));
          if (instance.getExecutorType().isStorageExecutor()) {
            sendFragmentInstanceReq.setConsensusGroupId(
                instance.getRegionReplicaSet().getRegionId());
          }
          TSendFragmentInstanceResp sendFragmentInstanceResp =
              client.sendFragmentInstance(sendFragmentInstanceReq);
          if (!sendFragmentInstanceResp.accepted) {
            logger.warn(sendFragmentInstanceResp.message);
            throw new FragmentInstanceDispatchException(
                RpcUtils.getStatus(
                    TSStatusCode.EXECUTE_STATEMENT_ERROR, sendFragmentInstanceResp.message));
          }
          break;
        case WRITE:
          TSendBatchPlanNodeReq sendPlanNodeReq =
              new TSendBatchPlanNodeReq(
                  Collections.singletonList(
                      new TSendSinglePlanNodeReq(
                          new TPlanNode(
                              instance.getFragment().getPlanNodeTree().serializeToByteBuffer()),
                          instance.getRegionReplicaSet().getRegionId())));
          TSendSinglePlanNodeResp sendPlanNodeResp =
              client.sendBatchPlanNode(sendPlanNodeReq).getResponses().get(0);
          if (!sendPlanNodeResp.accepted) {
            logger.warn(
                "dispatch write failed. status: {}, code: {}, message: {}, node {}",
                sendPlanNodeResp.status,
                TSStatusCode.representOf(sendPlanNodeResp.status.code),
                sendPlanNodeResp.message,
                endPoint);
            if (sendPlanNodeResp.getStatus() == null) {
              throw new FragmentInstanceDispatchException(
                  RpcUtils.getStatus(
                      TSStatusCode.WRITE_PROCESS_ERROR, sendPlanNodeResp.getMessage()));
            } else {
              throw new FragmentInstanceDispatchException(sendPlanNodeResp.getStatus());
            }
          } else {
            // some expected and accepted status except SUCCESS_STATUS need to be returned
            TSStatus status = sendPlanNodeResp.getStatus();
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
    } catch (ClientManagerException | TException e) {
      logger.warn("can't connect to node {}", endPoint, e);
      TSStatus status = new TSStatus();
      status.setCode(TSStatusCode.DISPATCH_ERROR.getStatusCode());
      status.setMessage("can't connect to node " + endPoint);
      // If the DataNode cannot be connected, its endPoint will be put into black list
      // so that the following retry will avoid dispatching instance towards this DataNode.
      queryContext.addFailedEndPoint(endPoint);
      throw new FragmentInstanceDispatchException(status);
    }
  }

  private void dispatchLocally(FragmentInstance instance) throws FragmentInstanceDispatchException {
    // deserialize ConsensusGroupId
    ConsensusGroupId groupId = null;
    if (instance.getExecutorType().isStorageExecutor()) {
      try {
        groupId =
            ConsensusGroupId.Factory.createFromTConsensusGroupId(
                instance.getRegionReplicaSet().getRegionId());
      } catch (Throwable t) {
        logger.warn("Deserialize ConsensusGroupId failed. ", t);
        throw new FragmentInstanceDispatchException(
            RpcUtils.getStatus(
                TSStatusCode.EXECUTE_STATEMENT_ERROR,
                "Deserialize ConsensusGroupId failed: " + t.getMessage()));
      }
    }

    switch (instance.getType()) {
      case READ:
        RegionReadExecutor readExecutor = new RegionReadExecutor();
        RegionExecutionResult readResult =
            groupId == null
                ? readExecutor.execute(instance)
                : readExecutor.execute(groupId, instance);
        if (!readResult.isAccepted()) {
          logger.warn(readResult.getMessage());
          throw new FragmentInstanceDispatchException(
              RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, readResult.getMessage()));
        }
        break;
      case WRITE:
        PlanNode planNode = instance.getFragment().getPlanNodeTree();
        RegionWriteExecutor writeExecutor = new RegionWriteExecutor();
        RegionExecutionResult writeResult = writeExecutor.execute(groupId, planNode);
        if (!writeResult.isAccepted()) {
          logger.warn(
              "write locally failed. TSStatus: {}, message: {}",
              writeResult.getStatus(),
              writeResult.getMessage());
          if (writeResult.getStatus() == null) {
            throw new FragmentInstanceDispatchException(
                RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, writeResult.getMessage()));
          } else {
            throw new FragmentInstanceDispatchException(writeResult.getStatus());
          }
        } else {
          // some expected and accepted status except SUCCESS_STATUS need to be returned
          TSStatus status = writeResult.getStatus();
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
