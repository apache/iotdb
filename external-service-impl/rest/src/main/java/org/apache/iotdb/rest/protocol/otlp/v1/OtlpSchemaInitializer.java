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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ensures the {@code claude_code} database and the {@code traces}, {@code metrics}, {@code logs}
 * tables exist before the OTLP receiver writes its first batch. Idempotent via {@code CREATE ... IF
 * NOT EXISTS}, so running it once per ingest call is cheap after the initial DDL has been
 * replicated across the cluster.
 */
final class OtlpSchemaInitializer {

  private static final Logger LOGGER = LoggerFactory.getLogger(OtlpSchemaInitializer.class);

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private OtlpSchemaInitializer() {}

  static void initialize(final String database, final IClientSession session) {
    runDdl(session, null, "CREATE DATABASE IF NOT EXISTS " + database);

    runDdl(
        session,
        database,
        "CREATE TABLE IF NOT EXISTS metrics ("
            + "user_id STRING TAG, "
            + "session_id STRING TAG, "
            + "metric_name STRING TAG, "
            + "model STRING TAG, "
            + "type STRING TAG, "
            + "terminal_type STRING ATTRIBUTE, "
            + "service_version STRING ATTRIBUTE, "
            + "os_type STRING ATTRIBUTE, "
            + "os_version STRING ATTRIBUTE, "
            + "host_arch STRING ATTRIBUTE, "
            + "unit STRING ATTRIBUTE, "
            + "metric_type STRING ATTRIBUTE, "
            + "description STRING ATTRIBUTE, "
            + "value DOUBLE FIELD)");

    runDdl(
        session,
        database,
        "CREATE TABLE IF NOT EXISTS logs ("
            + "user_id STRING TAG, "
            + "session_id STRING TAG, "
            + "event_name STRING TAG, "
            + "terminal_type STRING ATTRIBUTE, "
            + "service_version STRING ATTRIBUTE, "
            + "os_type STRING ATTRIBUTE, "
            + "host_arch STRING ATTRIBUTE, "
            + "prompt_id STRING FIELD, "
            + "event_sequence INT32 FIELD, "
            + "body STRING FIELD, "
            + "prompt_length INT32 FIELD, "
            + "prompt STRING FIELD, "
            + "model STRING FIELD, "
            + "cost_usd DOUBLE FIELD, "
            + "duration_ms INT64 FIELD, "
            + "input_tokens INT64 FIELD, "
            + "output_tokens INT64 FIELD, "
            + "cache_read_tokens INT64 FIELD, "
            + "cache_creation_tokens INT64 FIELD, "
            + "request_id STRING FIELD, "
            + "speed STRING FIELD, "
            + "error STRING FIELD, "
            + "status_code STRING FIELD, "
            + "attempt INT32 FIELD, "
            + "tool_name STRING FIELD, "
            + "success STRING FIELD, "
            + "tool_duration_ms INT64 FIELD, "
            + "decision STRING FIELD, "
            + "decision_source STRING FIELD, "
            + "tool_result_size_bytes INT64 FIELD)");

    runDdl(
        session,
        database,
        "CREATE TABLE IF NOT EXISTS traces ("
            + "service_name STRING TAG, "
            + "span_name STRING TAG, "
            + "service_version STRING ATTRIBUTE, "
            + "os_type STRING ATTRIBUTE, "
            + "host_arch STRING ATTRIBUTE, "
            + "trace_id STRING FIELD, "
            + "span_id STRING FIELD, "
            + "parent_span_id STRING FIELD, "
            + "span_kind STRING FIELD, "
            + "start_time_unix_nano INT64 FIELD, "
            + "end_time_unix_nano INT64 FIELD, "
            + "duration_nano INT64 FIELD, "
            + "status_code STRING FIELD, "
            + "status_message STRING FIELD, "
            + "attributes STRING FIELD, "
            + "resource_attributes STRING FIELD, "
            + "scope_name STRING FIELD, "
            + "scope_version STRING FIELD)");
  }

  private static void runDdl(
      final IClientSession session, final String database, final String sql) {
    final SessionManager sessionManager = SessionManager.getInstance();
    final Long queryId = sessionManager.requestQueryId();
    try {
      if (database == null) {
        // CREATE DATABASE must not set a current database; clear it so the parser parses the name
        // as-is rather than prepending the current database.
        session.setDatabaseName(null);
      } else {
        session.setDatabaseName(database);
      }
      session.setSqlDialect(IClientSession.SqlDialect.TABLE);

      final SqlParser parser = new SqlParser();
      final Statement statement = parser.createStatement(sql, session.getZoneId(), session);
      final Metadata metadata = LocalExecutionPlanner.getInstance().metadata;

      final ExecutionResult result =
          Coordinator.getInstance()
              .executeForTableModel(
                  statement,
                  parser,
                  session,
                  queryId,
                  sessionManager.getSessionInfo(session),
                  sql,
                  metadata,
                  CONFIG.getQueryTimeoutThreshold(),
                  false,
                  false);
      final TSStatus status = result.status;
      final int code = status.getCode();
      if (code != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && code != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()
          // 602 = DATABASE_ALREADY_EXISTS, 507 = TABLE_ALREADY_EXISTS in IF NOT EXISTS races
          && code != TSStatusCode.DATABASE_ALREADY_EXISTS.getStatusCode()
          && code != TSStatusCode.TABLE_ALREADY_EXISTS.getStatusCode()) {
        LOGGER.warn(
            "OTLP schema init DDL failed: sql=[{}], code={}, message={}",
            sql,
            code,
            status.getMessage());
      }
    } catch (final Exception e) {
      LOGGER.warn("OTLP schema init DDL threw: sql=[{}]", sql, e);
    } finally {
      Coordinator.getInstance().cleanupQueryExecution(queryId);
    }
  }
}
