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

package org.apache.iotdb.pipe.it.dual.tablemodel.manual.enhanced;

import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTableManualEnhanced;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.pipe.it.dual.TypeConversionSemanticCase;
import org.apache.iotdb.pipe.it.dual.tablemodel.manual.AbstractPipeTableModelDualManualIT;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTableManualEnhanced.class})
public class IoTDBPipeTypeConversionSemanticIT extends AbstractPipeTableModelDualManualIT {

  private static final String DATABASE = "pipe_type_conversion";
  private static final String TABLE = "semantic_conversion";
  private static final String STREAM_TABLE = "semantic_stream_conversion";
  private static final List<TypeConversionSemanticCase> STREAM_CASES =
      getCases(
          "bool_to_int32",
          "bool_to_int64",
          "bool_to_float",
          "bool_to_double",
          "bool_to_blob",
          "bool_to_date",
          "bool_to_timestamp",
          "int32_to_boolean",
          "int32_to_timestamp",
          "int32_to_date");

  @Override
  @Before
  public void setUp() {
    MultiEnvFactory.createEnv(2);
    senderEnv = MultiEnvFactory.getEnv(0);
    receiverEnv = MultiEnvFactory.getEnv(1);
    setupConfig();
    senderEnv.initClusterEnvironment(1, 1);
    receiverEnv.initClusterEnvironment(1, 1);
  }

  @Override
  protected void setupConfig() {
    super.setupConfig();
    senderEnv
        .getConfig()
        .getCommonConfig()
        .setDataReplicationFactor(1)
        .setSchemaReplicationFactor(1);
    receiverEnv
        .getConfig()
        .getCommonConfig()
        .setDataReplicationFactor(1)
        .setSchemaReplicationFactor(1);
  }

  @Test
  public void testPipeReceiverTypeConversionSemantics() {
    createDatabaseAndTable(senderEnv, TABLE, TypeConversionSemanticCase.CASES, true);
    createDatabaseAndTable(receiverEnv, TABLE, TypeConversionSemanticCase.CASES, false);
    createPipe();

    TestUtils.executeNonQueries(
        DATABASE,
        BaseEnv.TABLE_SQL_DIALECT,
        senderEnv,
        createInsertStatements(TABLE, TypeConversionSemanticCase.CASES),
        null);

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        createQuerySql(TABLE, TypeConversionSemanticCase.CASES),
        createExpectedHeader(TypeConversionSemanticCase.CASES),
        new HashSet<>(createExpectedRows(TypeConversionSemanticCase.CASES)),
        60,
        DATABASE,
        null);
  }

  @Test
  public void testStreamPipeReceiverTypeConversionSemantics() {
    createDatabaseAndTable(senderEnv, STREAM_TABLE, STREAM_CASES, true);
    createDatabaseAndTable(receiverEnv, STREAM_TABLE, STREAM_CASES, false);
    createStreamPipe();

    TestUtils.executeNonQueries(
        DATABASE,
        BaseEnv.TABLE_SQL_DIALECT,
        senderEnv,
        createInsertStatements(STREAM_TABLE, STREAM_CASES),
        null);

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        createQuerySql(STREAM_TABLE, STREAM_CASES),
        createExpectedHeader(STREAM_CASES),
        new HashSet<>(createExpectedRows(STREAM_CASES)),
        60,
        DATABASE,
        null);
  }

  private static void createDatabaseAndTable(
      final BaseEnv env,
      final String table,
      final List<TypeConversionSemanticCase> conversionCases,
      final boolean useSourceType) {
    final List<String> sqls = new ArrayList<>();
    sqls.add("create database if not exists " + DATABASE);
    sqls.add("use " + DATABASE);
    final List<String> columns = new ArrayList<>();
    columns.add("tag_id string tag");
    for (final TypeConversionSemanticCase conversionCase : conversionCases) {
      columns.add(
          String.format(
              "%s %s field",
              conversionCase.measurement,
              useSourceType ? conversionCase.sourceType : conversionCase.targetType));
    }
    sqls.add(String.format("create table %s (%s)", table, String.join(",", columns)));
    TestUtils.executeNonQueries(null, BaseEnv.TABLE_SQL_DIALECT, env, sqls, null);
  }

  private void createPipe() {
    TestUtils.executeNonQuery(
        DATABASE,
        BaseEnv.TABLE_SQL_DIALECT,
        senderEnv,
        String.format(
            "create pipe type_conversion_semantic"
                + " with source ('source'='iotdb-source','history.enable'='false','realtime.enable'='true','realtime.mode'='forced-log')"
                + " with processor ('processor'='do-nothing-processor')"
                + " with sink ('node-urls'='%s','batch.enable'='false','sink.format'='tablet')",
            receiverEnv.getDataNodeWrapperList().get(0).getIpAndPortString()),
        null);
  }

  private void createStreamPipe() {
    TestUtils.executeNonQuery(
        DATABASE,
        BaseEnv.TABLE_SQL_DIALECT,
        senderEnv,
        String.format(
            "create pipe stream_type_conversion_semantic"
                + " with source ('source'='iotdb-source','history.enable'='false','realtime.enable'='true','realtime.mode'='stream')"
                + " with processor ('processor'='do-nothing-processor')"
                + " with sink ('sink'='iotdb-thrift-sink','sink.node-urls'='%s')",
            receiverEnv.getDataNodeWrapperList().get(0).getIpAndPortString()),
        null);
  }

  private static List<String> createInsertStatements(
      final String table, final List<TypeConversionSemanticCase> conversionCases) {
    final List<String> sqls = new ArrayList<>();
    final String measurements =
        String.join(
            ",",
            conversionCases.stream()
                .map(conversionCase -> conversionCase.measurement)
                .toArray(String[]::new));
    for (int row = 0; row < TypeConversionSemanticCase.ROW_COUNT; row++) {
      final List<String> values = new ArrayList<>();
      for (final TypeConversionSemanticCase conversionCase : conversionCases) {
        values.add(conversionCase.sourceSqlValues[row]);
      }
      sqls.add(
          String.format(
              "insert into %s(time,tag_id,%s) values (%d,'d',%s)",
              table, measurements, row + 1, String.join(",", values)));
    }
    sqls.add("flush");
    return sqls;
  }

  private static String createQuerySql(
      final String table, final List<TypeConversionSemanticCase> conversionCases) {
    return String.format(
        "select %s,time from %s where tag_id='d'",
        String.join(
            ",",
            conversionCases.stream()
                .map(conversionCase -> conversionCase.measurement)
                .toArray(String[]::new)),
        table);
  }

  private static String createExpectedHeader(
      final List<TypeConversionSemanticCase> conversionCases) {
    final List<String> columns = new ArrayList<>();
    for (final TypeConversionSemanticCase conversionCase : conversionCases) {
      columns.add(conversionCase.measurement);
    }
    columns.add("time");
    return String.join(",", columns) + ",";
  }

  private static List<String> createExpectedRows(
      final List<TypeConversionSemanticCase> conversionCases) {
    final List<String> rows = new ArrayList<>();
    for (int row = 0; row < TypeConversionSemanticCase.ROW_COUNT; row++) {
      final List<String> values = new ArrayList<>();
      for (final TypeConversionSemanticCase conversionCase : conversionCases) {
        values.add(conversionCase.expectedValues[row]);
      }
      values.add(TypeConversionSemanticCase.timestampValue(row + 1));
      rows.add(String.join(",", values) + ",");
    }
    return rows;
  }

  private static List<TypeConversionSemanticCase> getCases(final String... measurements) {
    final List<TypeConversionSemanticCase> cases = new ArrayList<>();
    for (final String measurement : measurements) {
      cases.add(getCase(measurement));
    }
    return cases;
  }

  private static TypeConversionSemanticCase getCase(final String measurement) {
    for (final TypeConversionSemanticCase conversionCase : TypeConversionSemanticCase.CASES) {
      if (conversionCase.measurement.equals(measurement)) {
        return conversionCase;
      }
    }
    throw new IllegalArgumentException("Unknown type conversion semantic case: " + measurement);
  }
}
