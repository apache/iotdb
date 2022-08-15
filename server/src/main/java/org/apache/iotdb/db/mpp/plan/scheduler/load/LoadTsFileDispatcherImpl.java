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

package org.apache.iotdb.db.mpp.plan.scheduler.load;

import io.airlift.concurrent.SetThreadName;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.mpp.FragmentInstanceDispatchException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.mpp.plan.analyze.SchemaValidator;
import org.apache.iotdb.db.mpp.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.mpp.plan.scheduler.FragInstanceDispatchResult;
import org.apache.iotdb.db.mpp.plan.scheduler.IFragInstanceDispatcher;
import org.apache.iotdb.mpp.rpc.thrift.TLoadCommandReq;
import org.apache.iotdb.mpp.rpc.thrift.TLoadResp;
import org.apache.iotdb.mpp.rpc.thrift.TLoadTsFileReq;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

import static com.google.common.util.concurrent.Futures.immediateFuture;

public class LoadTsFileDispatcherImpl implements IFragInstanceDispatcher {
  private static final Logger logger = LoggerFactory.getLogger(LoadTsFileDispatcherImpl.class);

  private final String uuid;
  private final String localhostIpAddr;
  private final int localhostInternalPort;
  private final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient>
      internalServiceClientManager;

  public LoadTsFileDispatcherImpl(
      String uuid,
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> internalServiceClientManager) {
    this.uuid = uuid;
    this.internalServiceClientManager = internalServiceClientManager;
    this.localhostIpAddr = IoTDBDescriptor.getInstance().getConfig().getInternalAddress();
    this.localhostInternalPort = IoTDBDescriptor.getInstance().getConfig().getInternalPort();
  }

  @Override
  public Future<FragInstanceDispatchResult> dispatch(List<FragmentInstance> instances) {
    for (FragmentInstance instance : instances) {
      try (SetThreadName threadName =
          new SetThreadName(LoadTsFileScheduler.class.getName() + instance.getId().getFullId())) {
        dispatchOneInstance(instance);
      } catch (FragmentInstanceDispatchException e) {
        return immediateFuture(new FragInstanceDispatchResult(e.getFailureStatus()));
      } catch (Throwable t) {
        logger.error("cannot dispatch FI for load operation", t);
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
    for (TDataNodeLocation dataNodeLocation : instance.getDataRegionId().getDataNodeLocations()) {
      TEndPoint endPoint = dataNodeLocation.getInternalEndPoint();
      if (isDispatchedToLocal(endPoint)) {
        dispatchLocally(instance);
      } else {
        dispatchRemote(instance, endPoint);
      }
    }
  }

  private boolean isDispatchedToLocal(TEndPoint endPoint) {
    return this.localhostIpAddr.equals(endPoint.getIp()) && localhostInternalPort == endPoint.port;
  }

  private void dispatchRemote(FragmentInstance instance, TEndPoint endPoint)
      throws FragmentInstanceDispatchException {
    try (SyncDataNodeInternalServiceClient client =
        internalServiceClientManager.borrowClient(endPoint)) {
      TLoadTsFileReq loadTsFileReq =
          new TLoadTsFileReq(instance.getFragment().getRoot().serializeToByteBuffer(), uuid);
      TLoadResp loadResp = client.sendLoadNode(loadTsFileReq);
      if (!LoadTsFileScheduler.LoadResult.SUCCESS.equals(
          LoadTsFileScheduler.LoadResult.values()[loadResp.loadRespStatus])) {
        logger.error(loadResp.info);
        TSStatus status = new TSStatus();
        status.setCode(TSStatusCode.LOAD_TSFILE_ERROR.getStatusCode());
        status.setMessage(loadResp.info);
        throw new FragmentInstanceDispatchException(status);
      }
    } catch (IOException | TException e) {
      logger.error("can't connect to node {}", endPoint, e);
      TSStatus status = new TSStatus();
      status.setCode(TSStatusCode.SYNC_CONNECTION_EXCEPTION.getStatusCode());
      status.setMessage("can't connect to node {}" + endPoint);
      throw new FragmentInstanceDispatchException(status);
    }
  }

  private void dispatchLocally(FragmentInstance instance) throws FragmentInstanceDispatchException {
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
    // TODO: write to local interface

    //    ConsensusWriteResponse writeResponse;
    //    if (groupId instanceof DataRegionId) {
    //      writeResponse = DataRegionConsensusImpl.getInstance().write(groupId, planNode);
    //    } else {
    //      writeResponse = SchemaRegionConsensusImpl.getInstance().write(groupId, planNode);
    //    }
    //
    //    if (writeResponse.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
    //      logger.error(writeResponse.getStatus().message);
    //      throw new FragmentInstanceDispatchException(writeResponse.getStatus());
    //    } else if (hasFailedMeasurement) {
    //      throw new FragmentInstanceDispatchException(
    //          RpcUtils.getStatus(TSStatusCode.METADATA_ERROR.getStatusCode(),
    // partialInsertMessage));
    //    }
  }

  public Future<FragInstanceDispatchResult> dispatchCommand(
      TLoadCommandReq loadCommandReq, Set<TRegionReplicaSet> replicaSets) {
    Set<TEndPoint> allEndPoint = new HashSet<>();
    for (TRegionReplicaSet replicaSet : replicaSets) {
      for (TDataNodeLocation dataNodeLocation : replicaSet.getDataNodeLocations()) {
        allEndPoint.add(dataNodeLocation.getInternalEndPoint());
      }
    }

    for (TEndPoint endPoint : allEndPoint) {
      try (SetThreadName threadName =
          new SetThreadName(
              LoadTsFileScheduler.class.getName() + "-" + loadCommandReq.commandType)) {
        if (isDispatchedToLocal(endPoint)) {
          dispatchLocally(loadCommandReq);
        } else {
          dispatchRemote(loadCommandReq, endPoint);
        }
      } catch (FragmentInstanceDispatchException e) {
        return immediateFuture(new FragInstanceDispatchResult(e.getFailureStatus()));
      } catch (Throwable t) {
        logger.error("cannot dispatch LoadCommand for load operation", t);
        return immediateFuture(
                new FragInstanceDispatchResult(
                        RpcUtils.getStatus(
                                TSStatusCode.INTERNAL_SERVER_ERROR, "Unexpected errors: " + t.getMessage())));
      }
    }
    return immediateFuture(new FragInstanceDispatchResult(true));
  }

  private void dispatchRemote(TLoadCommandReq loadCommandReq, TEndPoint endPoint)
      throws FragmentInstanceDispatchException {
    try (SyncDataNodeInternalServiceClient client =
        internalServiceClientManager.borrowClient(endPoint)) {
      TLoadResp loadResp = client.sendLoadCommand(loadCommandReq);
      if (!LoadTsFileScheduler.LoadResult.SUCCESS.equals(
          LoadTsFileScheduler.LoadResult.values()[loadResp.loadRespStatus])) {
        logger.error(loadResp.info);
        TSStatus status = new TSStatus();
        status.setCode(TSStatusCode.LOAD_TSFILE_ERROR.getStatusCode());
        status.setMessage(loadResp.info);
        throw new FragmentInstanceDispatchException(status);
      }
    } catch (IOException | TException e) {
      logger.error("can't connect to node {}", endPoint, e);
      TSStatus status = new TSStatus();
      status.setCode(TSStatusCode.SYNC_CONNECTION_EXCEPTION.getStatusCode());
      status.setMessage("can't connect to node {}" + endPoint);
      throw new FragmentInstanceDispatchException(status);
    }
  }

  private void dispatchLocally(TLoadCommandReq loadCommandReq) throws FragmentInstanceDispatchException {
    // TODO: use local interface
  }

  @Override
  public void abort() {}
}
