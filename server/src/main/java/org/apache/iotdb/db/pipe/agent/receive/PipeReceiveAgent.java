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

package org.apache.iotdb.db.pipe.agent.receive;

import org.apache.iotdb.db.mpp.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.pipe.config.PipeConfig;
import org.apache.iotdb.db.pipe.receive.PipeExecuteThriftReqDirectlyHandler;
import org.apache.iotdb.db.pipe.receive.PipeThriftReqHandler;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeHandshakeReq;
import org.apache.iotdb.service.rpc.thrift.TPipeHandshakeResp;
import org.apache.iotdb.service.rpc.thrift.TPipeHeartbeatReq;
import org.apache.iotdb.service.rpc.thrift.TPipeHeartbeatResp;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

public class PipeReceiveAgent {
  private ThreadLocal<PipeThriftReqHandler> reqHandler = new ThreadLocal<>();

  private boolean validatePipeVersion(String pipeVersion) {
    return pipeVersion.equals(PipeConfig.getInstance().getPipeVersion());
  }

  private ThreadLocal<PipeThriftReqHandler> getReqHandler(String pipeVersion) {
    if (reqHandler.get() == null) {
      reqHandler.set(new PipeExecuteThriftReqDirectlyHandler());
    }
    return reqHandler;
  }

  public TPipeHandshakeResp handshake(TPipeHandshakeReq req) {
    if (!validatePipeVersion(req.getPipeVersion())) {
      return new TPipeHandshakeResp(
          RpcUtils.getStatus(
              TSStatusCode.PIPE_VERSION_ERROR,
              String.format("Pipe version %s is not supported.", req.getPipeVersion())));
    }
    return getReqHandler(req.getPipeVersion()).get().handleHandshakeReq(req);
  }

  public TPipeHeartbeatResp heartbeat(TPipeHeartbeatReq req) {
    if (!validatePipeVersion(req.getPipeVersion())) {
      return new TPipeHeartbeatResp(
          RpcUtils.getStatus(
              TSStatusCode.PIPE_VERSION_ERROR,
              String.format("Pipe version %s is not supported.", req.getPipeVersion())));
    }
    return getReqHandler(req.getPipeVersion()).get().handleHeartbeatReq(req);
  }

  public TPipeTransferResp transfer(
      TPipeTransferReq req, IPartitionFetcher partitionFetcher, ISchemaFetcher schemaFetcher) {
    if (!validatePipeVersion(req.getPipeVersion())) {
      return new TPipeTransferResp(
          RpcUtils.getStatus(
              TSStatusCode.PIPE_HANDSHAKE_ERROR,
              String.format("Pipe version %s is not supported.", req.getPipeVersion())));
    }
    return getReqHandler(req.getPipeVersion())
        .get()
        .handleTransferReq(req, partitionFetcher, schemaFetcher);
  }
}
