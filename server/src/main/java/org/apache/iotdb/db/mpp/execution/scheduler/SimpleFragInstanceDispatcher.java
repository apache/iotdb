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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.sql.planner.plan.FragmentInstance;
import org.apache.iotdb.mpp.rpc.thrift.InternalService;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstance;
import org.apache.iotdb.mpp.rpc.thrift.TSendFragmentInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TSendFragmentInstanceResp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class SimpleFragInstanceDispatcher implements IFragInstanceDispatcher {
  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleFragInstanceDispatcher.class);
  private final ExecutorService executor;

  public SimpleFragInstanceDispatcher(ExecutorService exeutor) {
    this.executor = exeutor;
  }

  @Override
  public Future<FragInstanceDispatchResult> dispatch(List<FragmentInstance> instances) {
    return executor.submit(
        () -> {
          TSendFragmentInstanceResp resp = new TSendFragmentInstanceResp(false);
          for (FragmentInstance instance : instances) {
            // TODO: (jackie tien) change the port
            InternalService.Iface client =
                InternalServiceClientFactory.getInternalServiceClient(
                    new TEndPoint(
                        instance.getHostEndpoint().getIp(),
                        IoTDBDescriptor.getInstance().getConfig().getInternalPort()));
            // TODO: (xingtanzjr) consider how to handle the buffer here
            ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);
            instance.serializeRequest(buffer);
            buffer.flip();
            TConsensusGroupId groupId = instance.getRegionReplicaSet().getRegionId();
            TSendFragmentInstanceReq req =
                new TSendFragmentInstanceReq(
                    new TFragmentInstance(buffer), groupId, instance.getType().toString());
            resp = client.sendFragmentInstance(req);
            if (!resp.accepted) {
              break;
            }
          }
          return new FragInstanceDispatchResult(resp.accepted);
        });
  }

  @Override
  public void abort() {}
}
