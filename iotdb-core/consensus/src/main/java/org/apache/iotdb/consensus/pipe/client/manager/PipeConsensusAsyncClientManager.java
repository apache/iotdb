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

package org.apache.iotdb.consensus.pipe.client.manager;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.consensus.pipe.client.AsyncPipeConsensusServiceClient;
import org.apache.iotdb.consensus.pipe.client.PipeConsensusClientPool.AsyncPipeConsensusServiceClientPoolFactory;
import org.apache.iotdb.consensus.pipe.client.PipeConsensusClientPool.PipeConsensusRPCConfig;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Note: This class is shared by all pipeConsensusTasks of one leader to its peers in a consensus
 * group. And unlike pipe engine, PipeConsensus doesn't need to handshake before establish RPC
 * connection between leader and follower.
 *
 * <p>Usage: you can invoke borrowClient method in this class to get your RPC asyncClient
 */
public class PipeConsensusAsyncClientManager {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeConsensusAsyncClientManager.class);

  private final IClientManager<TEndPoint, AsyncPipeConsensusServiceClient> ASYNC_CLIENT_MANAGER =
      new IClientManager.Factory<TEndPoint, AsyncPipeConsensusServiceClient>()
          .createClientManager(
              new AsyncPipeConsensusServiceClientPoolFactory(new PipeConsensusRPCConfig()));

  private PipeConsensusAsyncClientManager() {
    // do nothing
  }

  public AsyncPipeConsensusServiceClient borrowClient(final TEndPoint endPoint)
      throws PipeException {
    if (endPoint == null) {
      if (LOGGER.isWarnEnabled()) {
        LOGGER.warn("PipeConsensus: borrowAsyncClient endPoint is null");
      }
      throw new PipeException(
          "PipeConsensus: async client manager can't borrow clients for a null TEndPoint. Please set the url of receiver correctly!");
    }

    try {
      return ASYNC_CLIENT_MANAGER.borrowClient(endPoint);
    } catch (Exception e) {
      if (LOGGER.isWarnEnabled()) {
        LOGGER.warn(
            "PipeConsensus: borrowAsyncClient {}:{} failed", endPoint.getIp(), endPoint.getPort());
      }
      throw new PipeConnectionException(
          String.format(
              PipeConnectionException.CONNECTION_ERROR_FORMATTER,
              endPoint.getIp(),
              endPoint.getPort()),
          e);
    }
  }

  //////////////////////////// singleton ////////////////////////////

  private static class PipeConsensusAsyncClientManagerHolder {
    private static final PipeConsensusAsyncClientManager INSTANCE =
        new PipeConsensusAsyncClientManager();

    private PipeConsensusAsyncClientManagerHolder() {}
  }

  public static PipeConsensusAsyncClientManager getInstance() {
    return PipeConsensusAsyncClientManagerHolder.INSTANCE;
  }
}
