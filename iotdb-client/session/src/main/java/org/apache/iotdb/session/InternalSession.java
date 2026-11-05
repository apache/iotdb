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

package org.apache.iotdb.session;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.isession.INodeSupplier;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RpcServiceConstants;
import org.apache.iotdb.session.i18n.SessionMessages;

import java.time.ZoneId;
import java.util.List;
import java.util.Objects;

/** Session using caller-supplied internal endpoints and the caller's username. */
public class InternalSession extends Session {

  private final INodeSupplier nodeSupplier;

  public InternalSession(AbstractSessionBuilder builder, INodeSupplier nodeSupplier) {
    super(builder);
    this.nodeSupplier =
        Objects.requireNonNull(
            nodeSupplier,
            SessionMessages.EXCEPTION_INTERNAL_NODE_SUPPLIER_MUST_NOT_BE_NULL_C5CAF392);
    password = null;
    useEncryptedPassword = false;
    enableAutoFetch = false;
    enableRedirection = false;
    enableQueryRedirection = false;
    rpcServiceName = RpcServiceConstants.INTERNAL_CLIENT_RPC_SERVICE;
  }

  @Override
  public synchronized void open(boolean enableRPCCompaction, int connectionTimeoutInMs)
      throws IoTDBConnectionException {
    super.open(enableRPCCompaction, connectionTimeoutInMs, null, null, nodeSupplier);
  }

  @Override
  public SessionConnection constructSessionConnection(
      Session session, TEndPoint endpoint, ZoneId zoneId) throws IoTDBConnectionException {
    final List<TEndPoint> candidates = nodeSupplier.get();
    if (candidates.isEmpty()) {
      throw new IoTDBConnectionException(
          SessionMessages.EXCEPTION_NO_DATANODE_INTERNAL_ENDPOINT_IS_AVAILABLE_249D1081);
    }
    defaultEndPoint =
        candidates.contains(endpoint)
            ? endpoint.deepCopy()
            : nodeSupplier
                .getQueryEndPoint()
                .filter(candidates::contains)
                .orElse(candidates.get(0))
                .deepCopy();
    return super.constructSessionConnection(session, defaultEndPoint, zoneId);
  }
}
