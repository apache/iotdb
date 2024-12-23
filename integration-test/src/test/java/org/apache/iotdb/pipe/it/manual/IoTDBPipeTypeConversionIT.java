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

package org.apache.iotdb.pipe.it.manual;

import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.db.pipe.receiver.transform.converter.ValueConverter;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2ManualCreateSchema;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.RpcUtils;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.utils.Pair;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2ManualCreateSchema.class})
public class IoTDBPipeTypeConversionIT extends AbstractPipeDualManualIT {

  private static final int generateDataSize = 100;

  // Test for converting BOOLEAN to OtherType
  @Test
  public void testBooleanToOtherTypeConversion() {
    createDataPipe();
    executeAndVerifyTypeConversion(TSDataType.BOOLEAN, TSDataType.INT32);
    executeAndVerifyTypeConversion(TSDataType.BOOLEAN, TSDataType.INT64);
    executeAndVerifyTypeConversion(TSDataType.BOOLEAN, TSDataType.FLOAT);
    executeAndVerifyTypeConversion(TSDataType.BOOLEAN, TSDataType.DOUBLE);
    executeAndVerifyTypeConversion(TSDataType.BOOLEAN, TSDataType.TEXT);
    executeAndVerifyTypeConversion(TSDataType.BOOLEAN, TSDataType.TIMESTAMP);
    executeAndVerifyTypeConversion(TSDataType.BOOLEAN, TSDataType.BLOB);
    executeAndVerifyTypeConversion(TSDataType.BOOLEAN, TSDataType.STRING);
    executeAndVerifyTypeConversion(TSDataType.BOOLEAN, TSDataType.DATE);
  }

  // Test for converting INT32 to OtherType
  @Test
  public void testInt32ToOtherTypeConversion() {
    createDataPipe();
    executeAndVerifyTypeConversion(TSDataType.INT32, TSDataType.BOOLEAN);
    executeAndVerifyTypeConversion(TSDataType.INT32, TSDataType.INT64);
    executeAndVerifyTypeConversion(TSDataType.INT32, TSDataType.FLOAT);
    executeAndVerifyTypeConversion(TSDataType.INT32, TSDataType.DOUBLE);
    executeAndVerifyTypeConversion(TSDataType.INT32, TSDataType.TEXT);
    executeAndVerifyTypeConversion(TSDataType.INT32, TSDataType.TIMESTAMP);
    executeAndVerifyTypeConversion(TSDataType.INT32, TSDataType.BLOB);
    executeAndVerifyTypeConversion(TSDataType.INT32, TSDataType.STRING);
    executeAndVerifyTypeConversion(TSDataType.INT32, TSDataType.DATE);
  }

  // Test for converting INT64 to OtherType
  @Test
  public void testInt64ToOtherTypeConversion() {
    createDataPipe();
    executeAndVerifyTypeConversion(TSDataType.INT64, TSDataType.BOOLEAN);
    executeAndVerifyTypeConversion(TSDataType.INT64, TSDataType.INT32);
    executeAndVerifyTypeConversion(TSDataType.INT64, TSDataType.FLOAT);
    executeAndVerifyTypeConversion(TSDataType.INT64, TSDataType.DOUBLE);
    executeAndVerifyTypeConversion(TSDataType.INT64, TSDataType.TEXT);
    executeAndVerifyTypeConversion(TSDataType.INT64, TSDataType.TIMESTAMP);
    executeAndVerifyTypeConversion(TSDataType.INT64, TSDataType.BLOB);
    executeAndVerifyTypeConversion(TSDataType.INT64, TSDataType.STRING);
    executeAndVerifyTypeConversion(TSDataType.INT64, TSDataType.DATE);
  }

  // Test for converting FLOAT to OtherType
  @Test
  public void testFloatToOtherTypeConversion() {
    createDataPipe();
    executeAndVerifyTypeConversion(TSDataType.FLOAT, TSDataType.BOOLEAN);
    executeAndVerifyTypeConversion(TSDataType.FLOAT, TSDataType.INT32);
    executeAndVerifyTypeConversion(TSDataType.FLOAT, TSDataType.INT64);
    executeAndVerifyTypeConversion(TSDataType.FLOAT, TSDataType.DOUBLE);
    executeAndVerifyTypeConversion(TSDataType.FLOAT, TSDataType.TEXT);
    executeAndVerifyTypeConversion(TSDataType.FLOAT, TSDataType.TIMESTAMP);
    executeAndVerifyTypeConversion(TSDataType.FLOAT, TSDataType.BLOB);
    executeAndVerifyTypeConversion(TSDataType.FLOAT, TSDataType.STRING);
    executeAndVerifyTypeConversion(TSDataType.FLOAT, TSDataType.DATE);
  }

  // Test for converting DOUBLE to OtherType
  @Test
  public void testDoubleToOtherTypeConversion() {
    createDataPipe();
    executeAndVerifyTypeConversion(TSDataType.DOUBLE, TSDataType.BOOLEAN);
    executeAndVerifyTypeConversion(TSDataType.DOUBLE, TSDataType.INT32);
    executeAndVerifyTypeConversion(TSDataType.DOUBLE, TSDataType.INT64);
    executeAndVerifyTypeConversion(TSDataType.DOUBLE, TSDataType.FLOAT);
    executeAndVerifyTypeConversion(TSDataType.DOUBLE, TSDataType.TEXT);
    executeAndVerifyTypeConversion(TSDataType.DOUBLE, TSDataType.TIMESTAMP);
    executeAndVerifyTypeConversion(TSDataType.DOUBLE, TSDataType.BLOB);
    executeAndVerifyTypeConversion(TSDataType.DOUBLE, TSDataType.STRING);
    executeAndVerifyTypeConversion(TSDataType.DOUBLE, TSDataType.DATE);
  }

  // Test for converting TEXT to OtherType
  @Test
  public void testTextToOtherTypeConversion() {
    createDataPipe();
    executeAndVerifyTypeConversion(TSDataType.TEXT, TSDataType.BLOB);
    executeAndVerifyTypeConversion(TSDataType.TEXT, TSDataType.STRING);
    executeAndVerifyTypeConversion(TSDataType.TEXT, TSDataType.BOOLEAN);
    executeAndVerifyTypeConversion(TSDataType.TEXT, TSDataType.INT32);
    executeAndVerifyTypeConversion(TSDataType.TEXT, TSDataType.INT64);
    executeAndVerifyTypeConversion(TSDataType.TEXT, TSDataType.FLOAT);
    executeAndVerifyTypeConversion(TSDataType.TEXT, TSDataType.DOUBLE);
    executeAndVerifyTypeConversion(TSDataType.TEXT, TSDataType.TIMESTAMP);
    executeAndVerifyTypeConversion(TSDataType.TEXT, TSDataType.DATE);
  }

  // Test for converting TIMESTAMP to OtherType
  @Test
  public void testTimestampToOtherTypeConversion() {
    createDataPipe();
    executeAndVerifyTypeConversion(TSDataType.TIMESTAMP, TSDataType.BOOLEAN);
    executeAndVerifyTypeConversion(TSDataType.TIMESTAMP, TSDataType.INT32);
    executeAndVerifyTypeConversion(TSDataType.TIMESTAMP, TSDataType.INT64);
    executeAndVerifyTypeConversion(TSDataType.TIMESTAMP, TSDataType.FLOAT);
    executeAndVerifyTypeConversion(TSDataType.TIMESTAMP, TSDataType.DOUBLE);
    executeAndVerifyTypeConversion(TSDataType.TIMESTAMP, TSDataType.TEXT);
    executeAndVerifyTypeConversion(TSDataType.TIMESTAMP, TSDataType.BLOB);
    executeAndVerifyTypeConversion(TSDataType.TIMESTAMP, TSDataType.STRING);
    executeAndVerifyTypeConversion(TSDataType.TIMESTAMP, TSDataType.DATE);
  }

  // Test for converting DATE to OtherType
  @Test
  public void testDateToOtherTypeConversion() {
    createDataPipe();
    executeAndVerifyTypeConversion(TSDataType.DATE, TSDataType.BOOLEAN);
    executeAndVerifyTypeConversion(TSDataType.DATE, TSDataType.INT32);
    executeAndVerifyTypeConversion(TSDataType.DATE, TSDataType.INT64);
    executeAndVerifyTypeConversion(TSDataType.DATE, TSDataType.FLOAT);
    executeAndVerifyTypeConversion(TSDataType.DATE, TSDataType.DOUBLE);
    executeAndVerifyTypeConversion(TSDataType.DATE, TSDataType.TEXT);
    executeAndVerifyTypeConversion(TSDataType.DATE, TSDataType.STRING);
    executeAndVerifyTypeConversion(TSDataType.DATE, TSDataType.TIMESTAMP);
  }

  // Test for converting BLOB to OtherType
  @Test
  public void testBlobToOtherTypeConversion() {
    createDataPipe();
    executeAndVerifyTypeConversion(TSDataType.BLOB, TSDataType.TEXT);
    executeAndVerifyTypeConversion(TSDataType.BLOB, TSDataType.STRING);
    executeAndVerifyTypeConversion(TSDataType.BLOB, TSDataType.BOOLEAN);
    executeAndVerifyTypeConversion(TSDataType.BLOB, TSDataType.INT32);
    executeAndVerifyTypeConversion(TSDataType.BLOB, TSDataType.INT64);
    executeAndVerifyTypeConversion(TSDataType.BLOB, TSDataType.FLOAT);
    executeAndVerifyTypeConversion(TSDataType.BLOB, TSDataType.DOUBLE);
    executeAndVerifyTypeConversion(TSDataType.BLOB, TSDataType.TIMESTAMP);
    executeAndVerifyTypeConversion(TSDataType.BLOB, TSDataType.DATE);
  }

  // Test for converting STRING to OtherType
  @Test
  public void testStringToOtherTypeConversion() {
    createDataPipe();
    executeAndVerifyTypeConversion(TSDataType.STRING, TSDataType.TEXT);
    executeAndVerifyTypeConversion(TSDataType.STRING, TSDataType.BLOB);
    executeAndVerifyTypeConversion(TSDataType.STRING, TSDataType.BOOLEAN);
    executeAndVerifyTypeConversion(TSDataType.STRING, TSDataType.INT32);
    executeAndVerifyTypeConversion(TSDataType.STRING, TSDataType.INT64);
    executeAndVerifyTypeConversion(TSDataType.STRING, TSDataType.FLOAT);
    executeAndVerifyTypeConversion(TSDataType.STRING, TSDataType.DOUBLE);
    executeAndVerifyTypeConversion(TSDataType.STRING, TSDataType.TIMESTAMP);
  }

  private void executeAndVerifyTypeConversion(TSDataType source, TSDataType target) {
    List<Pair> pairs = prepareTypeConversionTest(source, target);
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        String.format("select status from root.test.%s2%s", source.name(), target.name()),
        String.format("Time,root.test.%s2%s.status,", source.name(), target.name()),
        createExpectedResultSet(pairs, source, target),
        30);
  }

  private List<Pair> prepareTypeConversionTest(TSDataType sourceType, TSDataType targetType) {
    String sourceTypeName = sourceType.name();
    String targetTypeName = targetType.name();

    createTimeSeries(sourceTypeName, targetTypeName, sourceTypeName, senderEnv);
    createTimeSeries(sourceTypeName, targetTypeName, targetTypeName, receiverEnv);

    List<Pair> pairs = createTestDataForType(sourceTypeName);

    executeDataInsertions(pairs, sourceType, targetType);
    return pairs;
  }

  private void createTimeSeries(
      String sourceTypeName, String targetTypeName, String dataType, BaseEnv env) {
    String timeSeriesCreationQuery =
        String.format(
            "create timeseries root.test.%s2%s.status with datatype=%s,encoding=PLAIN",
            sourceTypeName, targetTypeName, dataType);
    TestUtils.tryExecuteNonQueriesWithRetry(
        env, Collections.singletonList(timeSeriesCreationQuery));
  }

  private void createDataPipe() {
    String sql =
        String.format(
            "create pipe test"
                + " with source ('source'='iotdb-source','source.path'='root.test.**','realtime.mode'='forced-log','realtime.enable'='true','history.enable'='false')"
                + " with processor ('processor'='do-nothing-processor')"
                + " with sink ('node-urls'='%s:%s','batch.enable'='false','sink.format'='tablet')",
            receiverEnv.getIP(), receiverEnv.getPort());
    TestUtils.tryExecuteNonQueriesWithRetry(senderEnv, Collections.singletonList(sql));
  }

  private List<Pair> createTestDataForType(String sourceType) {
    switch (sourceType) {
      case "BOOLEAN":
        return createTestDataForBoolean();
      case "INT32":
        return createTestDataForInt32();
      case "INT64":
        return createTestDataForInt64();
      case "FLOAT":
        return createTestDataForFloat();
      case "DOUBLE":
        return createTestDataForDouble();
      case "TEXT":
        return createTestDataForText();
      case "TIMESTAMP":
        return createTestDataForTimestamp();
      case "DATE":
        return createTestDataForDate();
      case "BLOB":
        return createTestDataForBlob();
      case "STRING":
        return createTestDataForString();
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + sourceType);
    }
  }

  private void executeDataInsertions(
      List<Pair> testData, TSDataType sourceType, TSDataType targetType) {
    switch (sourceType) {
      case STRING:
      case TEXT:
        TestUtils.tryExecuteNonQueriesWithRetry(
            senderEnv,
            createInsertStatementsForString(testData, sourceType.name(), targetType.name()));
        return;
      case TIMESTAMP:
        TestUtils.tryExecuteNonQueriesWithRetry(
            senderEnv,
            createInsertStatementsForTimestamp(testData, sourceType.name(), targetType.name()));
        return;
      case DATE:
        TestUtils.tryExecuteNonQueriesWithRetry(
            senderEnv,
            createInsertStatementsForLocalDate(testData, sourceType.name(), targetType.name()));
        return;
      case BLOB:
        TestUtils.tryExecuteNonQueriesWithRetry(
            senderEnv,
            createInsertStatementsForBlob(testData, sourceType.name(), targetType.name()));
        return;
      default:
        TestUtils.tryExecuteNonQueriesWithRetry(
            senderEnv,
            createInsertStatementsForNumeric(testData, sourceType.name(), targetType.name()));
    }
  }

  private List<String> createInsertStatementsForString(
      List<Pair> testData, String sourceType, String targetType) {
    List<String> executes = new ArrayList<>();
    for (Pair pair : testData) {
      executes.add(
          String.format(
              "insert into root.test.%s2%s(timestamp,status) values (%s,'%s');",
              sourceType,
              targetType,
              pair.left,
              new String(((Binary) (pair.right)).getValues(), StandardCharsets.UTF_8)));
    }
    executes.add("flush");
    return executes;
  }

  private List<String> createInsertStatementsForNumeric(
      List<Pair> testData, String sourceType, String targetType) {
    List<String> executes = new ArrayList<>();
    for (Pair pair : testData) {
      executes.add(
          String.format(
              "insert into root.test.%s2%s(timestamp,status) values (%s,%s);",
              sourceType, targetType, pair.left, pair.right));
    }
    executes.add("flush");
    return executes;
  }

  private List<String> createInsertStatementsForTimestamp(
      List<Pair> testData, String sourceType, String targetType) {
    List<String> executes = new ArrayList<>();
    for (Pair pair : testData) {
      executes.add(
          String.format(
              "insert into root.test.%s2%s(timestamp,status) values (%s,%s);",
              sourceType, targetType, pair.left, pair.right));
    }
    executes.add("flush");
    return executes;
  }

  private List<String> createInsertStatementsForLocalDate(
      List<Pair> testData, String sourceType, String targetType) {
    List<String> executes = new ArrayList<>();
    for (Pair pair : testData) {
      executes.add(
          String.format(
              "insert into root.test.%s2%s(timestamp,status) values (%s,'%s');",
              sourceType, targetType, pair.left, DateUtils.formatDate((Integer) pair.right)));
    }
    executes.add("flush");
    return executes;
  }

  private List<String> createInsertStatementsForBlob(
      List<Pair> testData, String sourceType, String targetType) {
    List<String> executes = new ArrayList<>();
    for (Pair pair : testData) {
      String value = BytesUtils.parseBlobByteArrayToString(((Binary) pair.right).getValues());
      executes.add(
          String.format(
              "insert into root.test.%s2%s(timestamp,status) values (%s,X'%s');",
              sourceType, targetType, pair.left, value.substring(2)));
    }
    executes.add("flush");
    return executes;
  }

  private Set<String> createExpectedResultSet(
      List<Pair> pairs, TSDataType sourceType, TSDataType targetType) {
    switch (targetType) {
      case TIMESTAMP:
        return generateTimestampResultSet(pairs, sourceType, targetType);
      case DATE:
        return generateLocalDateResultSet(pairs, sourceType, targetType);
      case BLOB:
        return generateBlobResultSet(pairs, sourceType, targetType);
      case TEXT:
      case STRING:
        return generateStringResultSet(pairs, sourceType, targetType);
      default:
        HashSet<String> resultSet = new HashSet<>();
        for (Pair pair : pairs) {
          resultSet.add(
              String.format(
                  "%s,%s,", pair.left, ValueConverter.convert(sourceType, targetType, pair.right)));
        }
        return resultSet;
    }
  }

  private Set<String> generateTimestampResultSet(
      List<Pair> pairs, TSDataType sourceType, TSDataType targetType) {
    HashSet<String> resultSet = new HashSet<>();
    for (Pair pair : pairs) {
      resultSet.add(
          String.format(
              "%s,%s,",
              pair.left,
              RpcUtils.formatDatetime(
                  "default",
                  "ms",
                  (long) ValueConverter.convert(sourceType, targetType, pair.right),
                  ZoneOffset.UTC)));
    }
    return resultSet;
  }

  private Set<String> generateLocalDateResultSet(
      List<Pair> pairs, TSDataType sourceType, TSDataType targetType) {
    HashSet<String> resultSet = new HashSet<>();
    for (Pair pair : pairs) {
      resultSet.add(
          String.format(
              "%s,%s,",
              pair.left,
              DateUtils.formatDate(
                  (Integer) ValueConverter.convert(sourceType, targetType, pair.right))));
    }
    return resultSet;
  }

  private Set<String> generateBlobResultSet(
      List<Pair> pairs, TSDataType sourceType, TSDataType targetType) {
    HashSet<String> resultSet = new HashSet<>();
    for (Pair pair : pairs) {
      resultSet.add(
          String.format(
              "%s,%s,",
              pair.left,
              BytesUtils.parseBlobByteArrayToString(
                  ((Binary) ValueConverter.convert(sourceType, targetType, pair.right))
                      .getValues())));
    }
    return resultSet;
  }

  private Set<String> generateStringResultSet(
      List<Pair> pairs, TSDataType sourceType, TSDataType targetType) {
    HashSet<String> resultSet = new HashSet<>();
    for (Pair pair : pairs) {
      resultSet.add(
          String.format(
              "%s,%s,",
              pair.left,
              new String(
                  ((Binary) ValueConverter.convert(sourceType, targetType, pair.right)).getValues(),
                  StandardCharsets.UTF_8)));
    }
    return resultSet;
  }

  private List<Pair> createTestDataForBoolean() {
    List<Pair> pairs = new java.util.ArrayList<>();
    Random random = new Random();
    for (long i = 0; i < generateDataSize; i++) {
      pairs.add(new Pair<>(i, random.nextBoolean()));
    }
    return pairs;
  }

  private List<Pair> createTestDataForInt32() {
    List<Pair> pairs = new ArrayList<>();
    Random random = new Random();
    for (long i = 0; i < generateDataSize; i++) {
      pairs.add(new Pair<>(i, random.nextInt()));
    }
    pairs.add(new Pair<>(generateDataSize + 1, -1));
    pairs.add(new Pair<>(generateDataSize + 2, -2));
    pairs.add(new Pair<>(generateDataSize + 3, -3));
    return pairs;
  }

  private List<Pair> createTestDataForInt64() {
    List<Pair> pairs = new ArrayList<>();
    Random random = new Random();
    for (long i = 0; i < generateDataSize; i++) {
      pairs.add(new Pair<>(i, random.nextLong()));
    }
    pairs.add(new Pair<>(generateDataSize + 1, -1L));
    pairs.add(new Pair<>(generateDataSize + 2, -2L));
    pairs.add(new Pair<>(generateDataSize + 3, -3L));
    return pairs;
  }

  private List<Pair> createTestDataForFloat() {
    List<Pair> pairs = new ArrayList<>();
    Random random = new Random();
    for (long i = 0; i < generateDataSize; i++) {
      pairs.add(new Pair<>(i, random.nextFloat()));
    }
    return pairs;
  }

  private List<Pair> createTestDataForDouble() {
    List<Pair> pairs = new ArrayList<>();
    Random random = new Random();
    for (long i = 0; i < generateDataSize; i++) {
      pairs.add(new Pair<>(i, random.nextDouble()));
    }
    return pairs;
  }

  private List<Pair> createTestDataForText() {
    List<Pair> pairs = new ArrayList<>();
    for (long i = 0; i < generateDataSize; i++) {
      pairs.add(new Pair<>(i, new Binary((String.valueOf(i)).getBytes(StandardCharsets.UTF_8))));
    }
    pairs.add(new Pair(generateDataSize + 1, new Binary("Hello".getBytes(StandardCharsets.UTF_8))));
    pairs.add(
        new Pair(
            generateDataSize + 2, new Binary("Hello World!".getBytes(StandardCharsets.UTF_8))));
    pairs.add(
        new Pair(
            generateDataSize + 3, new Binary("This is a test.".getBytes(StandardCharsets.UTF_8))));
    pairs.add(
        new Pair(
            generateDataSize + 4,
            new Binary("IoTDB Hello World!!!!".getBytes(StandardCharsets.UTF_8))));
    pairs.add(
        new Pair(
            generateDataSize + 5,
            new Binary(
                "IoTDB is an excellent time series database!!!!!!!!!"
                    .getBytes(StandardCharsets.UTF_8))));
    return pairs;
  }

  private List<Pair> createTestDataForTimestamp() {
    List<Pair> pairs = new ArrayList<>();
    for (long i = 0; i < generateDataSize; i++) {
      pairs.add(new Pair<>(i, new Date().getTime() + i));
    }
    return pairs;
  }

  private List<Pair> createTestDataForDate() {
    List<Pair> pairs = new ArrayList<>();
    int year = 2023;
    int month = 1;
    int day = 1;
    for (long i = 0; i < generateDataSize; i++) {
      pairs.add(new Pair<>(i, year * 10000 + (month * 100) + day));

      // update
      day++;
      if (day > 28) {
        day = 1;
        month++;
        if (month > 12) {
          month = 1;
          year++;
        }
      }
    }
    return pairs;
  }

  private List<Pair> createTestDataForBlob() {
    List<Pair> pairs = new ArrayList<>();
    for (long i = 0; i < generateDataSize; i++) {
      pairs.add(new Pair<>(i, new Binary((String.valueOf(i)).getBytes(StandardCharsets.UTF_8))));
    }
    pairs.add(new Pair(generateDataSize + 1, new Binary("Hello".getBytes(StandardCharsets.UTF_8))));
    pairs.add(
        new Pair(
            generateDataSize + 2, new Binary("Hello World!".getBytes(StandardCharsets.UTF_8))));
    pairs.add(
        new Pair(
            generateDataSize + 3, new Binary("This is a test.".getBytes(StandardCharsets.UTF_8))));
    pairs.add(
        new Pair(
            generateDataSize + 4,
            new Binary("IoTDB Hello World!!!!".getBytes(StandardCharsets.UTF_8))));
    pairs.add(
        new Pair(
            generateDataSize + 5,
            new Binary(
                "IoTDB is an excellent time series database!!!!!!!!!"
                    .getBytes(StandardCharsets.UTF_8))));
    return pairs;
  }

  private List<Pair> createTestDataForString() {
    List<Pair> pairs = new ArrayList<>();
    for (long i = 0; i < generateDataSize; i++) {
      pairs.add(new Pair<>(i, new Binary((String.valueOf(i)).getBytes(StandardCharsets.UTF_8))));
    }
    pairs.add(new Pair(generateDataSize + 1, new Binary("Hello".getBytes(StandardCharsets.UTF_8))));
    pairs.add(
        new Pair(
            generateDataSize + 2, new Binary("Hello World!".getBytes(StandardCharsets.UTF_8))));
    pairs.add(
        new Pair(
            generateDataSize + 3, new Binary("This is a test.".getBytes(StandardCharsets.UTF_8))));
    pairs.add(
        new Pair(
            generateDataSize + 4,
            new Binary("IoTDB Hello World!!!!".getBytes(StandardCharsets.UTF_8))));
    pairs.add(
        new Pair(
            generateDataSize + 5,
            new Binary(
                "IoTDB is an excellent time series database!!!!!!!!!"
                    .getBytes(StandardCharsets.UTF_8))));
    return pairs;
  }
}
