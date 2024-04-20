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

package org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.client;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipeConsensusAsyncClientManager {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeConsensusAsyncClientManager.class);

  private final IClientManager<TEndPoint, AsyncDataNodeInternalServiceClient>
      ASYNC_TRANSFER_CLIENT_MGR =
          new IClientManager.Factory<TEndPoint, AsyncDataNodeInternalServiceClient>()
              .createClientManager(
                  new ClientPoolFactory.AsyncDataNodeInternalServiceClientPoolFactory());

  private PipeConsensusAsyncClientManager() {
    // do nothing
  }

  // TODO: Since each peer will have a sync handshake in advance, do we need to handshake the async
  // client again?
  public AsyncDataNodeInternalServiceClient borrowClient(final TEndPoint endPoint)
      throws PipeException {
    if (endPoint == null) {
      throw new PipeException(
          "PipeConsensus: sync client manager can't borrow clients for a null TEndPoint. Please set the url of receiver correctly!");
    }

    try {
      return ASYNC_TRANSFER_CLIENT_MGR.borrowClient(endPoint);
    } catch (ClientManagerException e) {
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
