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

package org.apache.iotdb.postgresql.server;

import org.apache.iotdb.postgresql.config.PostgreSqlServiceConfig;
import org.apache.iotdb.postgresql.session.CancelRegistry;
import org.apache.iotdb.postgresql.session.PgSession;
import org.apache.iotdb.postgresql.session.QueryProtocol;

import javax.net.ssl.SSLContext;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/** What every connection of one server shares. */
public final class PgServerContext {

  final PostgreSqlServiceConfig config;
  final SSLContext sslContext;
  final boolean loopback;
  final CancelRegistry cancels = new CancelRegistry();
  final Executor workers;
  final Function<PgSession, QueryProtocol> protocols;
  final String serverVersion;

  final java.util.concurrent.atomic.AtomicInteger active =
      new java.util.concurrent.atomic.AtomicInteger();
  final AtomicLong refusedConnections = new AtomicLong();
  final AtomicLong refusedUnencrypted = new AtomicLong();
  final AtomicLong failedLogins = new AtomicLong();

  /**
   * @param sslContext the TLS context, or {@code null} when no key store is configured (only
   *     allowed on loopback)
   * @param protocols creates the query layer of a logged-in connection
   * @param serverVersion the reported {@code server_version}
   */
  public PgServerContext(
      PostgreSqlServiceConfig config,
      SSLContext sslContext,
      Executor workers,
      Function<PgSession, QueryProtocol> protocols,
      String serverVersion) {
    this.config = config;
    this.sslContext = sslContext;
    this.loopback = config.isLoopback();
    this.workers = workers;
    this.protocols = protocols;
    this.serverVersion = serverVersion;
  }

  public int openConnections() {
    return active.get();
  }

  public long refusedConnections() {
    return refusedConnections.get();
  }

  public long refusedUnencrypted() {
    return refusedUnencrypted.get();
  }

  public long failedLogins() {
    return failedLogins.get();
  }
}
