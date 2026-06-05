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
    createDatabaseAndTable(senderEnv, true);
    createDatabaseAndTable(receiverEnv, false);
    createPipe();

    TestUtils.executeNonQueries(
        DATABASE, BaseEnv.TABLE_SQL_DIALECT, senderEnv, createInsertStatements(), null);

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        createQuerySql(),
        createExpectedHeader(),
        new HashSet<>(createExpectedRows()),
        60,
        DATABASE,
        null);
  }

  private static void createDatabaseAndTable(final BaseEnv env, final boolean useSourceType) {
    final List<String> sqls = new ArrayList<>();
    sqls.add("create database if not exists " + DATABASE);
    sqls.add("use " + DATABASE);
    final List<String> columns = new ArrayList<>();
    columns.add("tag_id string tag");
    for (final TypeConversionSemanticCase conversionCase : TypeConversionSemanticCase.CASES) {
      columns.add(
          String.format(
              "%s %s field",
              conversionCase.measurement,
              useSourceType ? conversionCase.sourceType : conversionCase.targetType));
    }
    sqls.add(String.format("create table %s (%s)", TABLE, String.join(",", columns)));
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

  private static List<String> createInsertStatements() {
    final List<String> sqls = new ArrayList<>();
    final String measurements =
        String.join(
            ",",
            TypeConversionSemanticCase.CASES.stream()
                .map(conversionCase -> conversionCase.measurement)
                .toArray(String[]::new));
    for (int row = 0; row < TypeConversionSemanticCase.ROW_COUNT; row++) {
      final List<String> values = new ArrayList<>();
      for (final TypeConversionSemanticCase conversionCase : TypeConversionSemanticCase.CASES) {
        values.add(conversionCase.sourceSqlValues[row]);
      }
      sqls.add(
          String.format(
              "insert into %s(time,tag_id,%s) values (%d,'d',%s)",
              TABLE, measurements, row + 1, String.join(",", values)));
    }
    sqls.add("flush");
    return sqls;
  }

  private static String createQuerySql() {
    return String.format(
        "select %s,time from %s where tag_id='d'",
        String.join(
            ",",
            TypeConversionSemanticCase.CASES.stream()
                .map(conversionCase -> conversionCase.measurement)
                .toArray(String[]::new)),
        TABLE);
  }

  private static String createExpectedHeader() {
    final List<String> columns = new ArrayList<>();
    for (final TypeConversionSemanticCase conversionCase : TypeConversionSemanticCase.CASES) {
      columns.add(conversionCase.measurement);
    }
    columns.add("time");
    return String.join(",", columns) + ",";
  }

  private static List<String> createExpectedRows() {
    final List<String> rows = new ArrayList<>();
    for (int row = 0; row < TypeConversionSemanticCase.ROW_COUNT; row++) {
      final List<String> values = new ArrayList<>();
      for (final TypeConversionSemanticCase conversionCase : TypeConversionSemanticCase.CASES) {
        values.add(conversionCase.expectedValues[row]);
      }
      values.add(TypeConversionSemanticCase.timestampValue(row + 1));
      rows.add(String.join(",", values) + ",");
    }
    return rows;
  }
}
