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
  private static final String ALIGNED_DEVICE = "root.pipe_type_conversion.aligned_d";
  private static final List<TypeConversionSemanticCase> ALIGNED_STREAM_CASES =
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
    createTimeseries(senderEnv, DEVICE, TypeConversionSemanticCase.CASES, true);
    createTimeseries(receiverEnv, DEVICE, TypeConversionSemanticCase.CASES, false);
    createPipe();

    TestUtils.executeNonQueries(
        senderEnv, createInsertStatements(DEVICE, TypeConversionSemanticCase.CASES, false), null);

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        createQuerySql(DEVICE, TypeConversionSemanticCase.CASES),
        createExpectedHeader(DEVICE, TypeConversionSemanticCase.CASES),
        new HashSet<>(createExpectedRows(TypeConversionSemanticCase.CASES)),
        60);
  }

  @Test
  public void testAlignedStreamPipeReceiverTypeConversionSemantics() {
    createAlignedTimeseries(senderEnv, ALIGNED_DEVICE, ALIGNED_STREAM_CASES, true);
    createAlignedTimeseries(receiverEnv, ALIGNED_DEVICE, ALIGNED_STREAM_CASES, false);
    createStreamPipe(ALIGNED_DEVICE);

    TestUtils.executeNonQueries(
        senderEnv, createInsertStatements(ALIGNED_DEVICE, ALIGNED_STREAM_CASES, true), null);

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        createQuerySql(ALIGNED_DEVICE, ALIGNED_STREAM_CASES),
        createExpectedHeader(ALIGNED_DEVICE, ALIGNED_STREAM_CASES),
        new HashSet<>(createExpectedRows(ALIGNED_STREAM_CASES)),
        60);
  }

  private static void createTimeseries(
      final BaseEnv env,
      final String device,
      final List<TypeConversionSemanticCase> conversionCases,
      final boolean useSourceType) {
    final List<String> sqls = new ArrayList<>();
    for (final TypeConversionSemanticCase conversionCase : conversionCases) {
      sqls.add(
          String.format(
              "create timeseries %s.%s with datatype=%s,encoding=PLAIN",
              device,
              conversionCase.measurement,
              useSourceType ? conversionCase.sourceType : conversionCase.targetType));
    }
    TestUtils.executeNonQueries(env, sqls, null);
  }

  private static void createAlignedTimeseries(
      final BaseEnv env,
      final String device,
      final List<TypeConversionSemanticCase> conversionCases,
      final boolean useSourceType) {
    final List<String> measurements = new ArrayList<>();
    for (final TypeConversionSemanticCase conversionCase : conversionCases) {
      measurements.add(
          String.format(
              "%s %s encoding=PLAIN",
              conversionCase.measurement,
              useSourceType ? conversionCase.sourceType : conversionCase.targetType));
    }
    TestUtils.executeNonQuery(
        env,
        String.format("create aligned timeseries %s(%s)", device, String.join(",", measurements)),
        null);
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

  private void createStreamPipe(final String device) {
    TestUtils.executeNonQuery(
        senderEnv,
        String.format(
            "create pipe aligned_type_conversion_semantic"
                + " with source ('source'='iotdb-source','source.path'='%s.**','history.enable'='false','realtime.mode'='stream')"
                + " with processor ('processor'='do-nothing-processor')"
                + " with sink ('sink'='iotdb-thrift-sink','sink.node-urls'='%s')",
            device, receiverEnv.getDataNodeWrapperList().get(0).getIpAndPortString()),
        null);
  }

  private static List<String> createInsertStatements(
      final String device,
      final List<TypeConversionSemanticCase> conversionCases,
      final boolean isAligned) {
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
              "insert into %s(time,%s)%s values (%d,%s)",
              device,
              measurements,
              isAligned ? " aligned" : "",
              row + 1,
              String.join(",", values)));
    }
    sqls.add("flush");
    return sqls;
  }

  private static String createQuerySql(
      final String device, final List<TypeConversionSemanticCase> conversionCases) {
    return String.format(
        "select %s from %s",
        String.join(
            ",",
            conversionCases.stream()
                .map(conversionCase -> conversionCase.measurement)
                .toArray(String[]::new)),
        device);
  }

  private static String createExpectedHeader(
      final String device, final List<TypeConversionSemanticCase> conversionCases) {
    final List<String> columns = new ArrayList<>();
    columns.add("Time");
    for (final TypeConversionSemanticCase conversionCase : conversionCases) {
      columns.add(device + "." + conversionCase.measurement);
    }
    return String.join(",", columns) + ",";
  }

  private static List<String> createExpectedRows(
      final List<TypeConversionSemanticCase> conversionCases) {
    final List<String> rows = new ArrayList<>();
    for (int row = 0; row < TypeConversionSemanticCase.ROW_COUNT; row++) {
      final List<String> values = new ArrayList<>();
      values.add(Integer.toString(row + 1));
      for (final TypeConversionSemanticCase conversionCase : conversionCases) {
        values.add(conversionCase.expectedValues[row]);
      }
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
