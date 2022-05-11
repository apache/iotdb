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
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.mpp.rpc.thrift.TCancelQueryReq;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class SimpleQueryTerminator implements IQueryTerminator {
  private static final Logger logger = LoggerFactory.getLogger(SimpleQueryTerminator.class);
  private static final long TERMINATION_GRACE_PERIOD_IN_MS = 1000L;
  private final ExecutorService executor;
  private final QueryId queryId;
  private final List<FragmentInstance> fragmentInstances;

  private final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient>
      internalServiceClientManager;

  public SimpleQueryTerminator(
      ExecutorService executor,
      QueryId queryId,
      List<FragmentInstance> fragmentInstances,
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> internalServiceClientManager) {
    this.executor = executor;
    this.queryId = queryId;
    this.fragmentInstances = fragmentInstances;
    this.internalServiceClientManager = internalServiceClientManager;
  }

  @Override
  public Future<Boolean> terminate() {
    List<TEndPoint> relatedHost = getRelatedHost();
    try {
      Thread.sleep(TERMINATION_GRACE_PERIOD_IN_MS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return executor.submit(
        () -> {
          for (TEndPoint endPoint : relatedHost) {
            // TODO (jackie tien) change the port
            try (SyncDataNodeInternalServiceClient client =
                internalServiceClientManager.borrowClient(endPoint)) {
              client.cancelQuery(
                  new TCancelQueryReq(queryId.getId(), getRelatedFragmentInstances(endPoint)));
            } catch (IOException e) {
              logger.error("can't connect to node {}", endPoint, e);
              return false;
            } catch (TException e) {
              return false;
            }
          }
          return true;
        });
  }

  private List<TEndPoint> getRelatedHost() {
    return fragmentInstances.stream()
        .map(instance -> instance.getHostDataNode().internalEndPoint)
        .distinct()
        .collect(Collectors.toList());
  }

  private List<TFragmentInstanceId> getRelatedFragmentInstances(TEndPoint endPoint) {
    return fragmentInstances.stream()
        .filter(instance -> instance.getHostDataNode().internalEndPoint.equals(endPoint))
        .map(instance -> instance.getId().toThrift())
        .collect(Collectors.toList());
  }
}
