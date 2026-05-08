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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class MetricTableWriter {

  private static final String TIME_COLUMN = "time";

  private static final Logger LOGGER = LoggerFactory.getLogger(MetricTableWriter.class);

  private static final Coordinator COORDINATOR = Coordinator.getInstance();
  private static final SessionManager SESSION_MANAGER = SessionManager.getInstance();
  private static final IoTDBConfig IOTDB_CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private final IClientSession session;
  private final SqlParser sqlParser = new SqlParser();
  private final Metadata metadata = LocalExecutionPlanner.getInstance().metadata;

  public MetricTableWriter() {
    this.session = new InternalClientSession("METRIC_SCRAPE");
    this.session.setSqlDialect(SqlDialect.TABLE);
    SESSION_MANAGER.supplySession(
        session,
        AuthorityChecker.SUPER_USER_ID,
        AuthorityChecker.SUPER_USER,
        ZoneId.systemDefault(),
        ClientVersion.V_1_0);
  }

  public synchronized void initializeDatabases(List<MetricScrapeTarget> targets) {
    Set<String> databases = new LinkedHashSet<>();
    for (MetricScrapeTarget target : targets) {
      databases.add(target.getDatabase());
    }
    for (String database : databases) {
      executeStatement("CREATE DATABASE IF NOT EXISTS " + quoteIdentifier(database));
    }
  }

  public synchronized void write(String database, List<PrometheusSample> samples) {
    if (samples.isEmpty()) {
      return;
    }
    Map<TabletKey, TabletBuilder> builders = new LinkedHashMap<>();
    for (PrometheusSample sample : samples) {
      List<String> labelKeys = new ArrayList<>(sample.getLabels().keySet());
      Collections.sort(labelKeys);
      validateColumns(sample, labelKeys);
      TabletKey key =
          new TabletKey(sample.getMetricFamilyName(), sample.getMetricName(), labelKeys);
      TabletBuilder builder = builders.get(key);
      if (builder == null) {
        builder = new TabletBuilder(database, key);
        builders.put(key, builder);
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

  private void validateColumns(PrometheusSample sample, List<String> labelKeys) {
    if (sample.getMetricFamilyName() == null || sample.getMetricFamilyName().trim().isEmpty()) {
      throw new IllegalArgumentException("Prometheus metric family name should not be empty");
    }
    if (sample.getMetricName() == null || sample.getMetricName().trim().isEmpty()) {
      throw new IllegalArgumentException("Prometheus metric name should not be empty");
    }
    for (String labelKey : labelKeys) {
      if (TIME_COLUMN.equalsIgnoreCase(labelKey)
          || sample.getMetricName().equalsIgnoreCase(labelKey)) {
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

  private static class TabletKey {
    private final String table;
    private final String field;
    private final List<String> labelKeys;

    private TabletKey(String table, String field, List<String> labelKeys) {
      this.table = table;
      this.field = field;
      this.labelKeys = new ArrayList<>(labelKeys);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof TabletKey)) {
        return false;
      }
      TabletKey tabletKey = (TabletKey) o;
      return Objects.equals(table, tabletKey.table)
          && Objects.equals(field, tabletKey.field)
          && Objects.equals(labelKeys, tabletKey.labelKeys);
    }

    @Override
    public int hashCode() {
      return Objects.hash(table, field, labelKeys);
    }
  }

  private class TabletBuilder {
    private final String database;
    private final String table;
    private final String field;
    private final List<String> labelKeys;
    private final List<PrometheusSample> samples = new ArrayList<>();

    private TabletBuilder(String database, TabletKey key) {
      this.database = database;
      this.table = key.table;
      this.field = key.field;
      this.labelKeys = new ArrayList<>(key.labelKeys);
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

      List<String> measurements = new ArrayList<>(labelKeys.size() + 1);
      measurements.addAll(labelKeys);
      measurements.add(field);
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
      Object[] columns = new Object[labelKeys.size() + 1];
      for (int i = 0; i < labelKeys.size(); i++) {
        columns[i] = buildStringColumn(labelKeys.get(i));
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
        String value = samples.get(i).getLabels().get(columnName);
        values[i] = new Binary(value.getBytes(StandardCharsets.UTF_8));
      }
      return values;
    }
  }
}
