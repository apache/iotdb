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

package org.apache.iotdb.db.client;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class ConsensusClient {
  private static final Logger logger = LoggerFactory.getLogger(ConsensusClient.class);

  private static final int TIMEOUT_MS = 10000;

  private static final int RETRY_NUM = 5;

  private static final String MSG_RECONNECTION_FAIL =
      "Fail to connect to any config node. Please check server it";

  protected TTransport transport;

  TEndPoint consensusLeader;

  protected int regionId;

  protected List<TEndPoint> regionMemberList;

  private ClientManager clientManager;

  private int cursor = 0;

  protected ConsensusClient(List<TEndPoint> nodes) {
    this.regionMemberList = nodes;
  }

  public void connect(TEndPoint endpoint) throws IoTDBConnectionException {
    try {
      transport =
          RpcTransportFactory.INSTANCE.getTransport(
              // as there is a try-catch already, we do not need to use TSocket.wrap
              endpoint.getIp(), endpoint.getPort(), TIMEOUT_MS);
      transport.open();
    } catch (TTransportException e) {
      throw new IoTDBConnectionException(e);
    }
  }

  protected void reconnect() throws IoTDBConnectionException {
    if (consensusLeader != null) {
      try {
        connect(consensusLeader);
        return;
      } catch (IoTDBConnectionException e) {
        logger.warn("The current node may have been down {},try next node", consensusLeader);
        consensusLeader = null;
      }
    }

    if (transport != null) {
      transport.close();
    }

    for (int tryHostNum = 0; tryHostNum < regionMemberList.size(); tryHostNum++) {
      cursor = (cursor + 1) % regionMemberList.size();
      TEndPoint tryEndpoint = regionMemberList.get(cursor);

      try {
        connect(tryEndpoint);
        return;
      } catch (IoTDBConnectionException e) {
        logger.warn("The current node may have been down {},try next node", tryEndpoint);
      }
    }

    throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
  }

  public void close() {
    transport.close();
  }

  protected boolean processResponse(TSStatus status) {
    if (status.getCode() == TSStatusCode.NEED_REDIRECTION.getStatusCode()) {
      if (status.isSetRedirectNode()) {
        consensusLeader =
            new TEndPoint(status.getRedirectNode().getIp(), status.getRedirectNode().getPort());
      } else {
        consensusLeader = null;
      }
      return true;
    }
    return false;
  }
}
