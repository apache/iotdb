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
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.execution.QueryStateMachine;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceFailureInfo;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceInfo;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceManager;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceState;
import org.apache.iotdb.db.mpp.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceInfoReq;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceInfoResp;

import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

public abstract class AbstractFragInsStateTracker implements IFragInstanceStateTracker {

  protected QueryStateMachine stateMachine;
  protected ScheduledExecutorService scheduledExecutor;
  protected List<FragmentInstance> instances;
  protected final String localhostIpAddr;
  protected final int localhostInternalPort;

  private final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient>
      internalServiceClientManager;

  protected AbstractFragInsStateTracker(
      QueryStateMachine stateMachine,
      ScheduledExecutorService scheduledExecutor,
      List<FragmentInstance> instances,
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> internalServiceClientManager) {
    this.stateMachine = stateMachine;
    this.scheduledExecutor = scheduledExecutor;
    this.instances = instances;
    this.internalServiceClientManager = internalServiceClientManager;
    this.localhostIpAddr = IoTDBDescriptor.getInstance().getConfig().getInternalAddress();
    this.localhostInternalPort = IoTDBDescriptor.getInstance().getConfig().getInternalPort();
  }

  public abstract void start();

  public abstract void abort();

  protected FragmentInstanceInfo fetchInstanceInfo(FragmentInstance instance)
      throws ClientManagerException, TException {
    TEndPoint endPoint = instance.getHostDataNode().internalEndPoint;
    if (isInstanceRunningLocally(endPoint)) {
      FragmentInstanceInfo info =
          FragmentInstanceManager.getInstance().getInstanceInfo(instance.getId());
      if (info != null) {
        return info;
      } else {
        return new FragmentInstanceInfo(FragmentInstanceState.NO_SUCH_INSTANCE);
      }
    } else {
      try (SyncDataNodeInternalServiceClient client =
          internalServiceClientManager.borrowClient(endPoint)) {
        TFragmentInstanceInfoResp resp =
            client.fetchFragmentInstanceInfo(new TFetchFragmentInstanceInfoReq(getTId(instance)));
        String failedMessage = "";
        if (resp.getFailedMessages() != null) {
          failedMessage = String.join(";", resp.getFailedMessages());
        }
        List<FragmentInstanceFailureInfo> failureInfoList = new ArrayList<>();
        if (resp.getFailureInfoList() != null) {
          for (ByteBuffer buffer : resp.getFailureInfoList()) {
            failureInfoList.add(FragmentInstanceFailureInfo.deserialize(buffer));
          }
        }
        return new FragmentInstanceInfo(
            FragmentInstanceState.valueOf(resp.getState()),
            resp.getEndTime(),
            failedMessage,
            failureInfoList);
      }
    }
  }

  private boolean isInstanceRunningLocally(TEndPoint endPoint) {
    return this.localhostIpAddr.equals(endPoint.getIp()) && localhostInternalPort == endPoint.port;
  }

  private TFragmentInstanceId getTId(FragmentInstance instance) {
    return new TFragmentInstanceId(
        instance.getId().getQueryId().getId(),
        instance.getId().getFragmentId().getId(),
        instance.getId().getInstanceId());
  }
}
