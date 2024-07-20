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
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceManager;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.mpp.rpc.thrift.TCancelQueryReq;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class SimpleQueryTerminator implements IQueryTerminator {
  private static final Logger logger = LoggerFactory.getLogger(SimpleQueryTerminator.class);
  private static final long TERMINATION_GRACE_PERIOD_IN_MS = 1000L;
  protected ScheduledExecutorService scheduledExecutor;
  private final QueryId queryId;
  private final MPPQueryContext queryContext;

  private final IFragInstanceStateTracker stateTracker;
  private List<TEndPoint> relatedHost;
  private Map<TEndPoint, List<FragmentInstanceId>> ownedFragmentInstance;

  private final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient>
      internalServiceClientManager;

  private boolean hasThrowable;

  public SimpleQueryTerminator(
      final ScheduledExecutorService scheduledExecutor,
      final MPPQueryContext queryContext,
      final List<FragmentInstance> fragmentInstances,
      final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient>
          internalServiceClientManager,
      final IFragInstanceStateTracker stateTracker) {
    this.scheduledExecutor = scheduledExecutor;
    this.queryId = queryContext.getQueryId();
    this.queryContext = queryContext;
    this.internalServiceClientManager = internalServiceClientManager;
    this.stateTracker = stateTracker;
    calculateParameter(fragmentInstances);
  }

  private void calculateParameter(final List<FragmentInstance> instances) {
    this.relatedHost = getRelatedHost(instances);
    this.ownedFragmentInstance = new HashMap<>();
    for (final TEndPoint endPoint : relatedHost) {
      ownedFragmentInstance.put(endPoint, getRelatedFragmentInstances(endPoint, instances));
    }
  }

  @Override
  public Future<Boolean> terminate(final Throwable t) {
    // For the failure dispatch, the termination should not be triggered because of connection issue
    this.relatedHost =
        this.relatedHost.stream()
            .filter(endPoint -> !queryContext.getEndPointBlackList().contains(endPoint))
            .collect(Collectors.toList());
    this.hasThrowable = Objects.nonNull(t);

    return scheduledExecutor.schedule(
        this::syncTerminate, TERMINATION_GRACE_PERIOD_IN_MS, TimeUnit.MILLISECONDS);
  }

  public Boolean syncTerminate() {
    boolean succeed = true;
    for (final TEndPoint endPoint : relatedHost) {
      // We only send cancel query request if there is remaining unfinished FI in that node
      final List<FragmentInstanceId> unfinishedFIs =
          stateTracker.filterUnFinishedFIs(ownedFragmentInstance.get(endPoint));
      if (unfinishedFIs.isEmpty()) {
        continue;
      }

      final String internalAddress = IoTDBDescriptor.getInstance().getConfig().getInternalAddress();
      final int internalPort = IoTDBDescriptor.getInstance().getConfig().getInternalPort();
      if (internalAddress.equalsIgnoreCase(endPoint.getIp())
          && internalPort == endPoint.getPort()) {
        for (final FragmentInstanceId insId : unfinishedFIs) {
          FragmentInstanceManager.getInstance().cancelTask(insId, hasThrowable);
        }
        continue;
      }

      try (final SyncDataNodeInternalServiceClient client =
          internalServiceClientManager.borrowClient(endPoint)) {
        client.cancelQuery(
            new TCancelQueryReq(
                queryId.getId(),
                unfinishedFIs.stream()
                    .map(FragmentInstanceId::toThrift)
                    .collect(Collectors.toList()),
                hasThrowable));
      } catch (final ClientManagerException e) {
        logger.warn("can't connect to node {}", endPoint, e);
        // we shouldn't return here and need to cancel queryTasks in other nodes
        succeed = false;
      } catch (final TException t) {
        logger.warn("cancel query {} on node {} failed.", queryId.getId(), endPoint, t);
        // we shouldn't return here and need to cancel queryTasks in other nodes
        succeed = false;
      }
    }
    return succeed;
  }

  private List<TEndPoint> getRelatedHost(final List<FragmentInstance> fragmentInstances) {
    return fragmentInstances.stream()
        .map(instance -> instance.getHostDataNode().internalEndPoint)
        .distinct()
        .collect(Collectors.toList());
  }

  private List<FragmentInstanceId> getRelatedFragmentInstances(
      final TEndPoint endPoint, final List<FragmentInstance> fragmentInstances) {
    return fragmentInstances.stream()
        .filter(instance -> instance.getHostDataNode().internalEndPoint.equals(endPoint))
        .map(FragmentInstance::getId)
        .collect(Collectors.toList());
  }
}
