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
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.sql.planner.plan.FragmentInstance;
import org.apache.iotdb.mpp.rpc.thrift.InternalService;
import org.apache.iotdb.mpp.rpc.thrift.TCancelQueryReq;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class SimpleQueryTerminator implements IQueryTerminator {
  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleQueryTerminator.class);
  private final ExecutorService executor;
  private final QueryId queryId;
  private final List<FragmentInstance> fragmentInstances;

  public SimpleQueryTerminator(
      ExecutorService executor, QueryId queryId, List<FragmentInstance> fragmentInstances) {
    this.executor = executor;
    this.queryId = queryId;
    this.fragmentInstances = fragmentInstances;
  }

  @Override
  public Future<Boolean> terminate() {
    List<TEndPoint> relatedHost = getRelatedHost(fragmentInstances);

    return executor.submit(
        () -> {
          try {
            for (TEndPoint endpoint : relatedHost) {
              // TODO (jackie tien) change the port
              InternalService.Iface client =
                  InternalServiceClientFactory.getInternalServiceClient(
                      new TEndPoint(
                          endpoint.getIp(),
                          IoTDBDescriptor.getInstance().getConfig().getInternalPort()));
              client.cancelQuery(new TCancelQueryReq(queryId.getId()));
            }
          } catch (TException e) {
            return false;
          }
          return true;
        });
  }

  private List<TEndPoint> getRelatedHost(List<FragmentInstance> instances) {
    return instances.stream()
        .map(FragmentInstance::getHostEndpoint)
        .distinct()
        .collect(Collectors.toList());
  }
}
