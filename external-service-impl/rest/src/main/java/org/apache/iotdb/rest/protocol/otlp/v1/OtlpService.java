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

package org.apache.iotdb.rest.protocol.otlp.v1;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.rest.IoTDBRestServiceConfig;
import org.apache.iotdb.db.conf.rest.IoTDBRestServiceDescriptor;
import org.apache.iotdb.db.protocol.basic.BasicOpenSessionResp;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.RestClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;

import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Singleton glue between the HTTP OTLP resources and the IoTDB coordinator.
 *
 * <p>There is no single "OTLP database": the receiver routes each OTLP resource group to a database
 * derived from its {@code service.name} resource attribute (see {@link
 * OtlpConverter#deriveDatabaseName}), so traffic from {@code claude-code}, {@code codex}, {@code
 * gemini} etc. lands in separate databases automatically.
 *
 * <p>Each database gets its own pinned {@link IClientSession} cached in {@link #sessionByDatabase},
 * created lazily on first use along with its {@code traces / metrics / logs} tables. Pinning the
 * database per session avoids races between concurrent requests that would otherwise share one
 * session's {@code databaseName} field.
 */
public final class OtlpService {

  private static final Logger LOGGER = LoggerFactory.getLogger(OtlpService.class);

  private static final OtlpService INSTANCE = new OtlpService();

  private final IoTDBRestServiceConfig config =
      IoTDBRestServiceDescriptor.getInstance().getConfig();

  private final ConcurrentHashMap<String, IClientSession> sessionByDatabase =
      new ConcurrentHashMap<>();

  private OtlpService() {}

  public static OtlpService getInstance() {
    return INSTANCE;
  }

  /**
   * Returns a session pinned to {@code database}, creating it and its OTLP tables on first call.
   * Safe to invoke from any request thread; the expensive login + DDL run at most once per database
   * for the lifetime of the process.
   */
  IClientSession sessionFor(final String database) {
    final IClientSession existing = sessionByDatabase.get(database);
    if (existing != null) {
      return existing;
    }
    return sessionByDatabase.computeIfAbsent(database, this::openDatabaseSession);
  }

  private IClientSession openDatabaseSession(final String database) {
    final IClientSession session = login();
    if (session == null) {
      throw new IllegalStateException(
          "OTLP receiver failed to log in as user '"
              + config.getOtlpUsername()
              + "'. Check otlp_username / otlp_password in iotdb-system.properties.");
    }
    OtlpSchemaInitializer.initialize(database, session);
    // After schema init the session's databaseName is pinned to `database`; subsequent inserts
    // on this session all target it.
    session.setDatabaseName(database);
    session.setSqlDialect(IClientSession.SqlDialect.TABLE);
    LOGGER.info(
        "OTLP receiver ready for database={} (user={})", database, config.getOtlpUsername());
    return session;
  }

  private IClientSession login() {
    final SessionManager sm = SessionManager.getInstance();
    // Use 127.0.0.1 as the client address so LoginLockManager's localhost check succeeds without
    // triggering a DNS lookup on a synthetic UUID (which logs a spurious UnknownHostException).
    final RestClientSession session = new RestClientSession("127.0.0.1");
    session.setUsername(config.getOtlpUsername());
    final BasicOpenSessionResp resp =
        sm.login(
            session,
            config.getOtlpUsername(),
            config.getOtlpPassword(),
            ZoneId.systemDefault().toString(),
            TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3,
            IoTDBConstant.ClientVersion.V_1_0,
            IClientSession.SqlDialect.TABLE);
    if (resp.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn(
          "OTLP login failed: user={}, code={}, message={}",
          config.getOtlpUsername(),
          resp.getCode(),
          resp.getMessage());
      return null;
    }
    return session;
  }

  /** Ingests an OTLP trace export request. Returns true on all-success. */
  public boolean ingestTraces(final ExportTraceServiceRequest request) {
    return OtlpTracesConverter.convertAndInsert(this, request);
  }

  /** Ingests an OTLP metrics export request. Returns true on all-success. */
  public boolean ingestMetrics(final ExportMetricsServiceRequest request) {
    return OtlpMetricsConverter.convertAndInsert(this, request);
  }

  /** Ingests an OTLP logs export request. Returns true on all-success. */
  public boolean ingestLogs(final ExportLogsServiceRequest request) {
    return OtlpLogsConverter.convertAndInsert(this, request);
  }
}
