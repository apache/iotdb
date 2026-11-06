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
import org.apache.iotdb.rpc.subscription.i18n.SubscriptionMessages;
import org.apache.iotdb.session.AbstractSessionBuilder;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionConnection;

import java.time.ZoneId;
import java.util.Objects;

public class SubscriptionSessionWrapper extends Session {

  private String rpcServiceName;

  public SubscriptionSessionWrapper(final AbstractSessionBuilder builder) {
    super(builder);
  }

  public void open() throws IoTDBConnectionException {
    open(enableThriftRpcCompaction, connectionTimeoutInMs);
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
          SubscriptionMessages.EXCEPTION_SUBSCRIPTION_SESSION_MUST_CONFIGURED_ENDPOINT_2CAD53A9);
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

  public void setRpcServiceName(final String rpcServiceName) {
    this.rpcServiceName = rpcServiceName;
  }

  @Override
  protected String getRpcServiceName() {
    return rpcServiceName;
  }

  public void setSslConfig(
      final boolean useSSL,
      final String trustStore,
      final String trustStorePwd,
      final String keyStore,
      final String keyStorePwd,
      final String sslProtocol) {
    this.useSSL = useSSL;
    this.trustStore = trustStore;
    this.trustStorePwd = trustStorePwd;
    this.keyStore = keyStore;
    this.keyStorePwd = keyStorePwd;
    this.sslProtocol = sslProtocol;
  }
}
