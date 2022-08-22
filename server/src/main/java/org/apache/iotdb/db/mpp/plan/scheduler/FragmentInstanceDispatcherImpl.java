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

package org.apache.iotdb.db.mpp.plan.scheduler;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.consensus.SchemaRegionConsensusImpl;
import org.apache.iotdb.db.exception.mpp.FragmentInstanceDispatchException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceInfo;
import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.analyze.SchemaValidator;
import org.apache.iotdb.db.mpp.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstance;
import org.apache.iotdb.mpp.rpc.thrift.TPlanNode;
import org.apache.iotdb.mpp.rpc.thrift.TSendFragmentInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TSendFragmentInstanceResp;
import org.apache.iotdb.mpp.rpc.thrift.TSendPlanNodeReq;
import org.apache.iotdb.mpp.rpc.thrift.TSendPlanNodeResp;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import io.airlift.concurrent.SetThreadName;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static com.google.common.util.concurrent.Futures.immediateFuture;

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
      internalServiceClientManager;

  public FragmentInstanceDispatcherImpl(
      QueryType type,
      MPPQueryContext queryContext,
      ExecutorService executor,
      ExecutorService writeOperationExecutor,
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> internalServiceClientManager) {
    this.type = type;
    this.queryContext = queryContext;
    this.executor = executor;
    this.writeOperationExecutor = writeOperationExecutor;
    this.internalServiceClientManager = internalServiceClientManager;
    this.localhostIpAddr = IoTDBDescriptor.getInstance().getConfig().getInternalAddress();
    this.localhostInternalPort = IoTDBDescriptor.getInstance().getConfig().getInternalPort();
  }

  @Override
  public Future<FragInstanceDispatchResult> dispatch(List<FragmentInstance> instances) {
    if (type == QueryType.READ) {
      return dispatchRead(instances);
    } else {
      return dispatchWriteSync(instances);
    }
  }

  // TODO: (xingtanzjr) currently we use a sequential dispatch policy for READ, which is
  //  unsafe for current FragmentInstance scheduler framework. We need to implement the
  //  topological dispatch according to dependency relations between FragmentInstances
  private Future<FragInstanceDispatchResult> dispatchRead(List<FragmentInstance> instances) {
    return executor.submit(
        () -> {
          for (FragmentInstance instance : instances) {
            try (SetThreadName threadName = new SetThreadName(instance.getId().getFullId())) {
              dispatchOneInstance(instance);
            } catch (FragmentInstanceDispatchException e) {
              return new FragInstanceDispatchResult(e.getFailureStatus());
            } catch (Throwable t) {
              logger.error("cannot dispatch FI for read operation", t);
              return new FragInstanceDispatchResult(
                  RpcUtils.getStatus(
                      TSStatusCode.INTERNAL_SERVER_ERROR, "Unexpected errors: " + t.getMessage()));
            }
          }
          return new FragInstanceDispatchResult(true);
        });
  }

  private Future<FragInstanceDispatchResult> dispatchWriteSync(List<FragmentInstance> instances) {
    for (FragmentInstance instance : instances) {
      try (SetThreadName threadName = new SetThreadName(instance.getId().getFullId())) {
        dispatchOneInstance(instance);
      } catch (FragmentInstanceDispatchException e) {
        return immediateFuture(new FragInstanceDispatchResult(e.getFailureStatus()));
      } catch (Throwable t) {
        logger.error("cannot dispatch FI for write operation", t);
        return immediateFuture(
            new FragInstanceDispatchResult(
                RpcUtils.getStatus(
                    TSStatusCode.INTERNAL_SERVER_ERROR, "Unexpected errors: " + t.getMessage())));
      }
    }
    return immediateFuture(new FragInstanceDispatchResult(true));
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
        internalServiceClientManager.borrowClient(endPoint)) {
      switch (instance.getType()) {
        case READ:
          TSendFragmentInstanceReq sendFragmentInstanceReq =
              new TSendFragmentInstanceReq(
                  new TFragmentInstance(instance.serializeToByteBuffer()),
                  instance.getRegionReplicaSet().getRegionId());
          TSendFragmentInstanceResp sendFragmentInstanceResp =
              client.sendFragmentInstance(sendFragmentInstanceReq);
          if (!sendFragmentInstanceResp.accepted) {
            logger.error(sendFragmentInstanceResp.message);
            throw new FragmentInstanceDispatchException(
                RpcUtils.getStatus(
                    TSStatusCode.EXECUTE_STATEMENT_ERROR, sendFragmentInstanceResp.message));
          }
          break;
        case WRITE:
          TSendPlanNodeReq sendPlanNodeReq =
              new TSendPlanNodeReq(
                  new TPlanNode(instance.getFragment().getRoot().serializeToByteBuffer()),
                  instance.getRegionReplicaSet().getRegionId());
          TSendPlanNodeResp sendPlanNodeResp = client.sendPlanNode(sendPlanNodeReq);
          if (!sendPlanNodeResp.accepted) {
            logger.error(sendPlanNodeResp.getStatus().message);
            throw new FragmentInstanceDispatchException(sendPlanNodeResp.getStatus());
          }
          break;
        default:
          throw new FragmentInstanceDispatchException(
              RpcUtils.getStatus(
                  TSStatusCode.EXECUTE_STATEMENT_ERROR,
                  String.format("unknown query type [%s]", instance.getType())));
      }
    } catch (IOException | TException e) {
      logger.error("can't connect to node {}", endPoint, e);
      TSStatus status = new TSStatus();
      status.setCode(TSStatusCode.SYNC_CONNECTION_EXCEPTION.getStatusCode());
      status.setMessage("can't connect to node {}" + endPoint);
      // If the DataNode cannot be connected, its endPoint will be put into black list
      // so that the following retry will avoid dispatching instance towards this DataNode.
      queryContext.addFailedEndPoint(endPoint);
      throw new FragmentInstanceDispatchException(status);
    }
  }

  private void dispatchLocally(FragmentInstance instance) throws FragmentInstanceDispatchException {
    // deserialize ConsensusGroupId
    ConsensusGroupId groupId;
    try {
      groupId =
          ConsensusGroupId.Factory.createFromTConsensusGroupId(
              instance.getRegionReplicaSet().getRegionId());
    } catch (Throwable t) {
      logger.error("Deserialize ConsensusGroupId failed. ", t);
      throw new FragmentInstanceDispatchException(
          RpcUtils.getStatus(
              TSStatusCode.EXECUTE_STATEMENT_ERROR,
              "Deserialize ConsensusGroupId failed: " + t.getMessage()));
    }

    switch (instance.getType()) {
      case READ:
        // execute fragment instance in state machine
        ConsensusReadResponse readResponse;
        try {
          if (groupId instanceof DataRegionId) {
            readResponse = DataRegionConsensusImpl.getInstance().read(groupId, instance);
          } else {
            readResponse = SchemaRegionConsensusImpl.getInstance().read(groupId, instance);
          }
          if (readResponse == null) {
            logger.error("ReadResponse is null");
            throw new FragmentInstanceDispatchException(
                RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, "ReadResponse is null"));
          }
        } catch (Throwable t) {
          logger.error("Execute FragmentInstance in ConsensusGroup {} failed.", groupId, t);
          throw new FragmentInstanceDispatchException(
              RpcUtils.getStatus(
                  TSStatusCode.EXECUTE_STATEMENT_ERROR,
                  "Execute FragmentInstance failed. " + t.getMessage()));
        }
        if (!readResponse.isSuccess()) {
          logger.error(
              "dispatch FragmentInstance {} locally failed. ",
              instance,
              readResponse.getException());
          throw new FragmentInstanceDispatchException(
              RpcUtils.getStatus(
                  TSStatusCode.EXECUTE_STATEMENT_ERROR,
                  "Execute FragmentInstance failed: "
                      + (readResponse.getException() == null
                          ? ""
                          : readResponse.getException().getMessage())));
        } else {
          FragmentInstanceInfo info = (FragmentInstanceInfo) readResponse.getDataset();
          if (info.getState().isFailed()) {
            throw new FragmentInstanceDispatchException(
                RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, info.getMessage()));
          }
        }
        break;
      case WRITE:
        PlanNode planNode = instance.getFragment().getRoot();
        boolean hasFailedMeasurement = false;
        String partialInsertMessage = null;
        if (planNode instanceof InsertNode) {
          InsertNode insertNode = (InsertNode) planNode;
          try {
            SchemaValidator.validate(insertNode);
          } catch (SemanticException e) {
            throw new FragmentInstanceDispatchException(e);
          }
          hasFailedMeasurement = insertNode.hasFailedMeasurements();
          if (hasFailedMeasurement) {
            partialInsertMessage =
                String.format(
                    "Fail to insert measurements %s caused by %s",
                    insertNode.getFailedMeasurements(), insertNode.getFailedMessages());
            logger.warn(partialInsertMessage);
          }
        }
        ConsensusWriteResponse writeResponse;
        if (groupId instanceof DataRegionId) {
          writeResponse = DataRegionConsensusImpl.getInstance().write(groupId, planNode);
        } else {
          writeResponse = SchemaRegionConsensusImpl.getInstance().write(groupId, planNode);
        }

        if (!writeResponse.isSuccessful()) {
          logger.error(writeResponse.getErrorMessage());
          TSStatus failureStatus =
              writeResponse.getStatus() != null
                  ? writeResponse.getStatus()
                  : RpcUtils.getStatus(
                      TSStatusCode.EXECUTE_STATEMENT_ERROR, writeResponse.getErrorMessage());
          throw new FragmentInstanceDispatchException(failureStatus);
        } else if (hasFailedMeasurement) {
          throw new FragmentInstanceDispatchException(
              RpcUtils.getStatus(
                  TSStatusCode.METADATA_ERROR.getStatusCode(), partialInsertMessage));
        }
        break;
      default:
        throw new FragmentInstanceDispatchException(
            RpcUtils.getStatus(
                TSStatusCode.EXECUTE_STATEMENT_ERROR,
                String.format("unknown query type [%s]", instance.getType())));
    }
  }

  @Override
  public void abort() {}
}
