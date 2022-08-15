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
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.mpp.FragmentInstanceDispatchException;
import org.apache.iotdb.db.mpp.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.mpp.plan.scheduler.FragInstanceDispatchResult;
import org.apache.iotdb.db.mpp.plan.scheduler.IFragInstanceDispatcher;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Future;

import static com.google.common.util.concurrent.Futures.immediateFuture;

public class LoadTsFileDispatcherImpl implements IFragInstanceDispatcher {
  private static final Logger logger = LoggerFactory.getLogger(LoadTsFileDispatcherImpl.class);

  private final String localhostIpAddr;
  private final int localhostInternalPort;
  private final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient>
      internalServiceClientManager;

  public LoadTsFileDispatcherImpl(
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> internalServiceClientManager) {
    this.internalServiceClientManager = internalServiceClientManager;
    this.localhostIpAddr = IoTDBDescriptor.getInstance().getConfig().getInternalAddress();
    this.localhostInternalPort = IoTDBDescriptor.getInstance().getConfig().getInternalPort();
  }

  @Override
  public Future<FragInstanceDispatchResult> dispatch(List<FragmentInstance> instances) {
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

  @Override
  public void abort() {}
}
