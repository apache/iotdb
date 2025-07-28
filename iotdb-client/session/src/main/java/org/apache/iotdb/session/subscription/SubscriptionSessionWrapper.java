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
import org.apache.iotdb.rpc.subscription.exception.SubscriptionParameterNotValidException;
import org.apache.iotdb.session.AbstractSessionBuilder;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionConnection;

import java.time.ZoneId;
import java.util.Objects;

public final class SubscriptionSessionWrapper extends Session {

  public SubscriptionSessionWrapper(final AbstractSessionBuilder builder) {
    super(builder);
  }

  public void open() throws IoTDBConnectionException {
    super.open();
  }

  public void close() throws IoTDBConnectionException {
    super.close();
  }

  @Override
  public SessionConnection constructSessionConnection(
      final Session session, final TEndPoint endpoint, final ZoneId zoneId)
      throws IoTDBConnectionException {
    if (Objects.isNull(endpoint)) {
      throw new SubscriptionParameterNotValidException(
          "Subscription session must be configured with an endpoint.");
    }
    return new SubscriptionSessionConnection(
        session,
        endpoint,
        zoneId,
        availableNodes,
        maxRetryCount,
        retryIntervalInMs,
        sqlDialect,
        database);
  }

  public SubscriptionSessionConnection getSessionConnection() throws IoTDBConnectionException {
    return (SubscriptionSessionConnection) getDefaultSessionConnection();
  }

  public int getThriftMaxFrameSize() {
    return thriftMaxFrameSize;
  }
}
