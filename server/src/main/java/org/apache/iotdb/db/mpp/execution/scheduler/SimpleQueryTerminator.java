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

package org.apache.iotdb.db.mpp.execution.scheduler;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.sql.planner.plan.FragmentInstance;
import org.apache.iotdb.mpp.rpc.thrift.TCancelQueryReq;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class SimpleQueryTerminator implements IQueryTerminator {
  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleQueryTerminator.class);
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
    List<TEndPoint> relatedHost = getRelatedHost(fragmentInstances);

    return executor.submit(
        () -> {
          for (TEndPoint endPoint : relatedHost) {
            // TODO (jackie tien) change the port
            SyncDataNodeInternalServiceClient client = null;
            try {
              client = internalServiceClientManager.borrowClient(endPoint);
              if (client == null) {
                throw new TException("Can't get client for node " + endPoint);
              }
              client.cancelQuery(new TCancelQueryReq(queryId.getId()));
            } catch (IOException e) {
              LOGGER.error("can't connect to node {}", endPoint, e);
              return false;
            } catch (TException e) {
              LOGGER.error("cancelQuery failed for node {}", endPoint, e);
              if (client != null) {
                client.close();
              }
              return false;
            } finally {
              if (client != null) {
                client.returnSelf();
              }
            }
          }
          return true;
        });
  }

  private List<TEndPoint> getRelatedHost(List<FragmentInstance> instances) {
    return instances.stream()
        .map(instance -> instance.getHostDataNode().internalEndPoint)
        .distinct()
        .collect(Collectors.toList());
  }
}
