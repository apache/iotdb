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

package org.apache.iotdb.pipe.it.dual.treemodel.auto.enhanced;

import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTreeAutoEnhanced;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.pipe.it.dual.TypeConversionSemanticCase;
import org.apache.iotdb.pipe.it.dual.treemodel.auto.AbstractPipeDualTreeModelAutoIT;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTreeAutoEnhanced.class})
public class IoTDBPipeTypeConversionSemanticIT extends AbstractPipeDualTreeModelAutoIT {

  private static final String DEVICE = "root.pipe_type_conversion.d";

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
    createTimeseries(senderEnv, true);
    createTimeseries(receiverEnv, false);
    createPipe();

    TestUtils.executeNonQueries(senderEnv, createInsertStatements(), null);

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        createQuerySql(),
        createExpectedHeader(),
        new HashSet<>(createExpectedRows()),
        60);
  }

  private static void createTimeseries(final BaseEnv env, final boolean useSourceType) {
    final List<String> sqls = new ArrayList<>();
    for (final TypeConversionSemanticCase conversionCase : TypeConversionSemanticCase.CASES) {
      sqls.add(
          String.format(
              "create timeseries %s.%s with datatype=%s,encoding=PLAIN",
              DEVICE,
              conversionCase.measurement,
              useSourceType ? conversionCase.sourceType : conversionCase.targetType));
    }
    TestUtils.executeNonQueries(env, sqls, null);
  }

  private void createPipe() {
    TestUtils.executeNonQuery(
        senderEnv,
        String.format(
            "create pipe type_conversion_semantic"
                + " with source ('source'='iotdb-source','source.path'='%s.**','history.enable'='false','realtime.mode'='forced-log')"
                + " with processor ('processor'='do-nothing-processor')"
                + " with sink ('node-urls'='%s','batch.enable'='false','sink.format'='tablet')",
            DEVICE, receiverEnv.getDataNodeWrapperList().get(0).getIpAndPortString()),
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
              "insert into %s(time,%s) values (%d,%s)",
              DEVICE, measurements, row + 1, String.join(",", values)));
    }
    sqls.add("flush");
    return sqls;
  }

  private static String createQuerySql() {
    return String.format(
        "select %s from %s",
        String.join(
            ",",
            TypeConversionSemanticCase.CASES.stream()
                .map(conversionCase -> conversionCase.measurement)
                .toArray(String[]::new)),
        DEVICE);
  }

  private static String createExpectedHeader() {
    final List<String> columns = new ArrayList<>();
    columns.add("Time");
    for (final TypeConversionSemanticCase conversionCase : TypeConversionSemanticCase.CASES) {
      columns.add(DEVICE + "." + conversionCase.measurement);
    }
    return String.join(",", columns) + ",";
  }

  private static List<String> createExpectedRows() {
    final List<String> rows = new ArrayList<>();
    for (int row = 0; row < TypeConversionSemanticCase.ROW_COUNT; row++) {
      final List<String> values = new ArrayList<>();
      values.add(Integer.toString(row + 1));
      for (final TypeConversionSemanticCase conversionCase : TypeConversionSemanticCase.CASES) {
        values.add(conversionCase.expectedValues[row]);
      }
      rows.add(String.join(",", values) + ",");
    }
    return rows;
  }
}
