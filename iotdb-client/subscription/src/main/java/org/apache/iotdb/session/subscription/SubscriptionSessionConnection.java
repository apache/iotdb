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

package org.apache.iotdb.session.subscription;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeReq;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeResp;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionConnection;

import org.apache.thrift.TException;

import java.time.ZoneId;
import java.util.List;
import java.util.function.Supplier;

public class SubscriptionSessionConnection extends SessionConnection {

  public SubscriptionSessionConnection(
      final Session session,
      final TEndPoint endPoint,
      final ZoneId zoneId,
      final Supplier<List<TEndPoint>> availableNodes,
      final int maxRetryCount,
      final long retryIntervalInMs,
      final String sqlDialect,
      final String database)
      throws IoTDBConnectionException {
    super(
        session,
        endPoint,
        zoneId,
        availableNodes,
        maxRetryCount,
        retryIntervalInMs,
        sqlDialect,
        database);
  }

  public TPipeSubscribeResp pipeSubscribe(final TPipeSubscribeReq req) throws TException {
    return client.pipeSubscribe(req);
  }
}
