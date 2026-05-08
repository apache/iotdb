/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.metricscrape;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.IoTDBConstant.ClientVersion;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.common.SqlDialect;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.InternalClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MetricTableWriter {

  public static final String METRIC_NAME_COLUMN = "metric_name";
  public static final String VALUE_COLUMN = "value";
  private static final String TIME_COLUMN = "time";

  private static final Logger LOGGER = LoggerFactory.getLogger(MetricTableWriter.class);

  private static final Coordinator COORDINATOR = Coordinator.getInstance();
  private static final SessionManager SESSION_MANAGER = SessionManager.getInstance();
  private static final IoTDBConfig IOTDB_CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private final String database;
  private final String table;
  private final IClientSession session;
  private final SqlParser sqlParser = new SqlParser();
  private final Metadata metadata = LocalExecutionPlanner.getInstance().metadata;

  public MetricTableWriter(String database, String table) {
    this.database = database;
    this.table = table;
    this.session = new InternalClientSession("METRIC_SCRAPE");
    this.session.setDatabaseName(database);
    this.session.setSqlDialect(SqlDialect.TABLE);
    SESSION_MANAGER.supplySession(
        session,
        AuthorityChecker.SUPER_USER_ID,
        AuthorityChecker.SUPER_USER,
        ZoneId.systemDefault(),
        ClientVersion.V_1_0);
  }

  public synchronized void initializeSchema() {
    executeStatement("CREATE DATABASE IF NOT EXISTS " + quoteIdentifier(database));
    session.setDatabaseName(database);
    executeStatement(
        "CREATE TABLE IF NOT EXISTS "
            + quoteIdentifier(table)
            + " ("
            + METRIC_NAME_COLUMN
            + " STRING TAG, "
            + VALUE_COLUMN
            + " DOUBLE FIELD)");
  }

  public synchronized void write(List<PrometheusSample> samples) {
    if (samples.isEmpty()) {
      return;
    }
    Map<List<String>, TabletBuilder> builders = new LinkedHashMap<>();
    for (PrometheusSample sample : samples) {
      List<String> labelKeys = new ArrayList<>(sample.getLabels().keySet());
      Collections.sort(labelKeys);
      validateLabelKeys(labelKeys);
      TabletBuilder builder = builders.get(labelKeys);
      if (builder == null) {
        builder = new TabletBuilder(labelKeys);
        builders.put(labelKeys, builder);
      }
      builder.add(sample);
    }

    for (TabletBuilder builder : builders.values()) {
      executeInsert(builder.build());
    }
  }

  public void close() {
    SESSION_MANAGER.closeSession(session, COORDINATOR::cleanupQueryExecution, false);
  }

  private void validateLabelKeys(List<String> labelKeys) {
    for (String labelKey : labelKeys) {
      if (METRIC_NAME_COLUMN.equalsIgnoreCase(labelKey)
          || VALUE_COLUMN.equalsIgnoreCase(labelKey)
          || TIME_COLUMN.equalsIgnoreCase(labelKey)) {
        throw new IllegalArgumentException(
            "Prometheus label conflicts with reserved IoTDB table column name: " + labelKey);
      }
    }
  }

  private void executeInsert(InsertTabletStatement statement) {
    long queryId = SESSION_MANAGER.requestQueryId();
    try {
      ExecutionResult result =
          COORDINATOR.executeForTableModel(
              statement,
              sqlParser,
              session,
              queryId,
              SESSION_MANAGER.getSessionInfo(session),
              "",
              metadata,
              IOTDB_CONFIG.getQueryTimeoutThreshold());
      warnIfFailed(result.status);
    } finally {
      COORDINATOR.cleanupQueryExecution(queryId);
    }
  }

  private void executeStatement(String sql) {
    long queryId = SESSION_MANAGER.requestQueryId();
    try {
      Statement statement = sqlParser.createStatement(sql, session.getZoneId(), session);
      ExecutionResult result =
          COORDINATOR.executeForTableModel(
              statement,
              sqlParser,
              session,
              queryId,
              SESSION_MANAGER.getSessionInfo(session),
              sql,
              metadata,
              IOTDB_CONFIG.getQueryTimeoutThreshold(),
              false);
      TSStatus status = result.status;
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && status.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
        throw new IllegalStateException(
            "Failed to execute metric scrape schema statement: " + sql + ", status: " + status);
      }
    } finally {
      COORDINATOR.cleanupQueryExecution(queryId);
    }
  }

  private void warnIfFailed(TSStatus status) {
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        || status.getCode() == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
      return;
    }
    LOGGER.warn(
        "Metric scrape write failed, code={}, message={}", status.getCode(), status.getMessage());
  }

  private static String quoteIdentifier(String identifier) {
    return "\"" + identifier.replace("\"", "\"\"") + "\"";
  }

  private class TabletBuilder {
    private final List<String> labelKeys;
    private final List<PrometheusSample> samples = new ArrayList<>();

    private TabletBuilder(List<String> labelKeys) {
      this.labelKeys = new ArrayList<>(labelKeys);
    }

    private void add(PrometheusSample sample) {
      samples.add(sample);
    }

    private InsertTabletStatement build() {
      InsertTabletStatement statement = new InsertTabletStatement();
      statement.setDevicePath(new PartialPath(table, false));
      statement.setAligned(false);
      statement.setWriteToTable(true);
      statement.setDatabaseName(database);
      statement.setRowCount(samples.size());

      List<String> measurements = new ArrayList<>(labelKeys.size() + 2);
      measurements.add(METRIC_NAME_COLUMN);
      measurements.addAll(labelKeys);
      measurements.add(VALUE_COLUMN);
      statement.setMeasurements(measurements.toArray(new String[0]));
      statement.setTimes(buildTimes());
      statement.setDataTypes(buildDataTypes(measurements.size()));
      statement.setColumnCategories(buildColumnCategories(measurements.size()));
      statement.setColumns(buildColumns());
      statement.setBitMaps(new BitMap[measurements.size()]);
      return statement;
    }

    private long[] buildTimes() {
      long[] times = new long[samples.size()];
      for (int i = 0; i < samples.size(); i++) {
        times[i] = samples.get(i).getTimestamp();
      }
      return times;
    }

    private TSDataType[] buildDataTypes(int columnSize) {
      TSDataType[] dataTypes = new TSDataType[columnSize];
      for (int i = 0; i < columnSize - 1; i++) {
        dataTypes[i] = TSDataType.STRING;
      }
      dataTypes[columnSize - 1] = TSDataType.DOUBLE;
      return dataTypes;
    }

    private TsTableColumnCategory[] buildColumnCategories(int columnSize) {
      TsTableColumnCategory[] columnCategories = new TsTableColumnCategory[columnSize];
      for (int i = 0; i < columnSize - 1; i++) {
        columnCategories[i] = TsTableColumnCategory.TAG;
      }
      columnCategories[columnSize - 1] = TsTableColumnCategory.FIELD;
      return columnCategories;
    }

    private Object[] buildColumns() {
      Object[] columns = new Object[labelKeys.size() + 2];
      columns[0] = buildStringColumn(METRIC_NAME_COLUMN);
      for (int i = 0; i < labelKeys.size(); i++) {
        columns[i + 1] = buildStringColumn(labelKeys.get(i));
      }
      double[] values = new double[samples.size()];
      for (int i = 0; i < samples.size(); i++) {
        values[i] = samples.get(i).getValue();
      }
      columns[columns.length - 1] = values;
      return columns;
    }

    private Binary[] buildStringColumn(String columnName) {
      Binary[] values = new Binary[samples.size()];
      for (int i = 0; i < samples.size(); i++) {
        String value =
            METRIC_NAME_COLUMN.equals(columnName)
                ? samples.get(i).getMetricName()
                : samples.get(i).getLabels().get(columnName);
        values[i] = new Binary(value.getBytes(StandardCharsets.UTF_8));
      }
      return values;
    }
  }
}
