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

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.AbstractSessionBuilder;

public class SubscriptionTableSessionBuilder extends AbstractSessionBuilder {

  public SubscriptionTableSessionBuilder() {
    // use table model
    super.sqlDialect = "table";
    // disable auto fetch
    super.enableAutoFetch = false;
    // disable redirection
    super.enableRedirection = false;
  }

  public SubscriptionTableSessionBuilder host(final String host) {
    super.host = host;
    return this;
  }

  public SubscriptionTableSessionBuilder port(final int port) {
    super.rpcPort = port;
    return this;
  }

  public SubscriptionTableSessionBuilder username(final String username) {
    super.username = username;
    return this;
  }

  public SubscriptionTableSessionBuilder password(final String password) {
    super.pw = password;
    return this;
  }

  public SubscriptionTableSessionBuilder thriftMaxFrameSize(final int thriftMaxFrameSize) {
    super.thriftMaxFrameSize = thriftMaxFrameSize;
    return this;
  }

  public ISubscriptionTableSession build() throws IoTDBConnectionException {
    final ISubscriptionTableSession session = new SubscriptionTableSession(this);
    session.open();
    return session;
  }
}
