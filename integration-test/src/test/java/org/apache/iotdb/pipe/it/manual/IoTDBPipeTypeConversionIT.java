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
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2ManualCreateSchema.class})
public class IoTDBPipeTypeConversionIT extends AbstractPipeDualManualIT {

  private static final long generateDataSize = 100;

  // Test for converting BOOLEAN to OtherType
  @Test
  public void testBooleanToOtherTypeConversion() {
    encapsulateAndAssertConversion(TSDataType.BOOLEAN, TSDataType.INT32);
    encapsulateAndAssertConversion(TSDataType.BOOLEAN, TSDataType.INT64);
    encapsulateAndAssertConversion(TSDataType.BOOLEAN, TSDataType.FLOAT);
    encapsulateAndAssertConversion(TSDataType.BOOLEAN, TSDataType.DOUBLE);
    encapsulateAndAssertConversion(TSDataType.BOOLEAN, TSDataType.TEXT);
    encapsulateAndAssertConversion(TSDataType.BOOLEAN, TSDataType.TIMESTAMP);
    encapsulateAndAssertConversion(TSDataType.BOOLEAN, TSDataType.BLOB);
    encapsulateAndAssertConversion(TSDataType.BOOLEAN, TSDataType.STRING);
  }

  // Test for converting INT32 to OtherType
  @Test
  public void testInt32ToOtherTypeConversion() {
    encapsulateAndAssertConversion(TSDataType.INT32, TSDataType.BOOLEAN);
    encapsulateAndAssertConversion(TSDataType.INT32, TSDataType.INT64);
    encapsulateAndAssertConversion(TSDataType.INT32, TSDataType.FLOAT);
    encapsulateAndAssertConversion(TSDataType.INT32, TSDataType.DOUBLE);
    encapsulateAndAssertConversion(TSDataType.INT32, TSDataType.TEXT);
    encapsulateAndAssertConversion(TSDataType.INT32, TSDataType.TIMESTAMP);
    encapsulateAndAssertConversion(TSDataType.INT32, TSDataType.BLOB);
    encapsulateAndAssertConversion(TSDataType.INT32, TSDataType.STRING);
  }

  // Test for converting INT64 to OtherType
  @Test
  public void testInt64ToOtherTypeConversion() {
    encapsulateAndAssertConversion(TSDataType.INT64, TSDataType.BOOLEAN);
    encapsulateAndAssertConversion(TSDataType.INT64, TSDataType.INT32);
    encapsulateAndAssertConversion(TSDataType.INT64, TSDataType.FLOAT);
    encapsulateAndAssertConversion(TSDataType.INT64, TSDataType.DOUBLE);
    encapsulateAndAssertConversion(TSDataType.INT64, TSDataType.TEXT);
    encapsulateAndAssertConversion(TSDataType.INT64, TSDataType.TIMESTAMP);
    encapsulateAndAssertConversion(TSDataType.INT64, TSDataType.BLOB);
    encapsulateAndAssertConversion(TSDataType.INT64, TSDataType.STRING);
  }

  // Test for converting FLOAT to OtherType
  @Test
  public void testFloatToOtherTypeConversion() {
    encapsulateAndAssertConversion(TSDataType.FLOAT, TSDataType.BOOLEAN);
    encapsulateAndAssertConversion(TSDataType.FLOAT, TSDataType.INT32);
    encapsulateAndAssertConversion(TSDataType.FLOAT, TSDataType.INT64);
    encapsulateAndAssertConversion(TSDataType.FLOAT, TSDataType.DOUBLE);
    encapsulateAndAssertConversion(TSDataType.FLOAT, TSDataType.TEXT);
    encapsulateAndAssertConversion(TSDataType.FLOAT, TSDataType.TIMESTAMP);
    encapsulateAndAssertConversion(TSDataType.FLOAT, TSDataType.BLOB);
    encapsulateAndAssertConversion(TSDataType.FLOAT, TSDataType.STRING);
  }

  // Test for converting DOUBLE to OtherType
  @Test
  public void testDoubleToOtherTypeConversion() {
    encapsulateAndAssertConversion(TSDataType.DOUBLE, TSDataType.BOOLEAN);
    encapsulateAndAssertConversion(TSDataType.DOUBLE, TSDataType.INT32);
    encapsulateAndAssertConversion(TSDataType.DOUBLE, TSDataType.INT64);
    encapsulateAndAssertConversion(TSDataType.DOUBLE, TSDataType.FLOAT);
    encapsulateAndAssertConversion(TSDataType.DOUBLE, TSDataType.TEXT);
    encapsulateAndAssertConversion(TSDataType.DOUBLE, TSDataType.TIMESTAMP);
    encapsulateAndAssertConversion(TSDataType.DOUBLE, TSDataType.BLOB);
    encapsulateAndAssertConversion(TSDataType.DOUBLE, TSDataType.STRING);
  }

  // Test for converting TEXT to OtherType
  @Test
  public void testTextToOtherTypeConversion() {
    encapsulateAndAssertConversion(TSDataType.TEXT, TSDataType.BLOB);
    encapsulateAndAssertConversion(TSDataType.TEXT, TSDataType.STRING);
    encapsulateAndAssertConversion(TSDataType.TEXT, TSDataType.BOOLEAN);
    encapsulateAndAssertConversion(TSDataType.TEXT, TSDataType.INT32);
    encapsulateAndAssertConversion(TSDataType.TEXT, TSDataType.INT64);
    encapsulateAndAssertConversion(TSDataType.TEXT, TSDataType.FLOAT);
    encapsulateAndAssertConversion(TSDataType.TEXT, TSDataType.DOUBLE);
    encapsulateAndAssertConversion(TSDataType.TEXT, TSDataType.TIMESTAMP);
  }

  // Test for converting TIMESTAMP to OtherType
  @Test
  public void testTimestampToOtherTypeConversion() {
    encapsulateAndAssertConversion(TSDataType.TIMESTAMP, TSDataType.BOOLEAN);
    encapsulateAndAssertConversion(TSDataType.TIMESTAMP, TSDataType.INT32);
    encapsulateAndAssertConversion(TSDataType.TIMESTAMP, TSDataType.INT64);
    encapsulateAndAssertConversion(TSDataType.TIMESTAMP, TSDataType.FLOAT);
    encapsulateAndAssertConversion(TSDataType.TIMESTAMP, TSDataType.DOUBLE);
    encapsulateAndAssertConversion(TSDataType.TIMESTAMP, TSDataType.TEXT);
    encapsulateAndAssertConversion(TSDataType.TIMESTAMP, TSDataType.BLOB);
    encapsulateAndAssertConversion(TSDataType.TIMESTAMP, TSDataType.STRING);
  }

  // Test for converting DATE to OtherType
  @Test
  public void testDateToOtherTypeConversion() {
    encapsulateAndAssertConversion(TSDataType.DATE, TSDataType.BOOLEAN);
    encapsulateAndAssertConversion(TSDataType.DATE, TSDataType.INT32);
    encapsulateAndAssertConversion(TSDataType.DATE, TSDataType.INT64);
    encapsulateAndAssertConversion(TSDataType.DATE, TSDataType.FLOAT);
    encapsulateAndAssertConversion(TSDataType.DATE, TSDataType.DOUBLE);
    encapsulateAndAssertConversion(TSDataType.DATE, TSDataType.TEXT);
    encapsulateAndAssertConversion(TSDataType.DATE, TSDataType.STRING);
    encapsulateAndAssertConversion(TSDataType.DATE, TSDataType.TIMESTAMP);
  }

  // Test for converting BLOB to OtherType
  @Test
  public void testBlobToOtherTypeConversion() {
    encapsulateAndAssertConversion(TSDataType.BLOB, TSDataType.TEXT);
    encapsulateAndAssertConversion(TSDataType.BLOB, TSDataType.STRING);
    encapsulateAndAssertConversion(TSDataType.BLOB, TSDataType.BOOLEAN);
    encapsulateAndAssertConversion(TSDataType.BLOB, TSDataType.INT32);
    encapsulateAndAssertConversion(TSDataType.BLOB, TSDataType.INT64);
    encapsulateAndAssertConversion(TSDataType.BLOB, TSDataType.FLOAT);
    encapsulateAndAssertConversion(TSDataType.BLOB, TSDataType.DOUBLE);
    encapsulateAndAssertConversion(TSDataType.BLOB, TSDataType.TIMESTAMP);
  }

  // Test for converting STRING to OtherType
  @Test
  public void testStringToOtherTypeConversion() {
    encapsulateAndAssertConversion(TSDataType.STRING, TSDataType.TEXT);
    encapsulateAndAssertConversion(TSDataType.STRING, TSDataType.BLOB);
    encapsulateAndAssertConversion(TSDataType.STRING, TSDataType.BOOLEAN);
    encapsulateAndAssertConversion(TSDataType.STRING, TSDataType.INT32);
    encapsulateAndAssertConversion(TSDataType.STRING, TSDataType.INT64);
    encapsulateAndAssertConversion(TSDataType.STRING, TSDataType.FLOAT);
    encapsulateAndAssertConversion(TSDataType.STRING, TSDataType.DOUBLE);
    encapsulateAndAssertConversion(TSDataType.STRING, TSDataType.TIMESTAMP);
  }

  private void encapsulateAndAssertConversion(TSDataType source, TSDataType target) {
    List<Pair> pairs = encapsulateConversion(source, target);
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        String.format("select * from root.%s2%s.**", source.name(), target.name()),
        String.format("Time,root.%s2%s.test.status,", source.name(), target.name()),
        generateResultSet(pairs, source, target),
        20);
  }

  private List<Pair> encapsulateConversion(TSDataType sourceType, TSDataType targetType) {
    String sourceTypeName = sourceType.name();
    String targetTypeName = targetType.name();
    TestUtils.tryExecuteNonQueriesWithRetry(
        senderEnv,
        Collections.singletonList(
            String.format(
                "create timeseries root.%s2%s.test.status with datatype=%s,encoding=PLAIN",
                sourceTypeName, targetTypeName, sourceTypeName)));

    TestUtils.tryExecuteNonQueriesWithRetry(
        receiverEnv,
        Collections.singletonList(
            String.format(
                "create timeseries root.%s2%s.test.status with datatype=%s,encoding=PLAIN",
                sourceTypeName, targetTypeName, targetTypeName)));

    String sql =
        String.format(
            "create pipe %s2%s"
                + " with source ('source'='iotdb-source','source.path'='root.%s2%s.**','realtime.mode'='forced-log','realtime.enable'='true','history.enable'='false')"
                + " with processor ('processor'='do-nothing-processor')"
                + " with sink ('node-urls'='%s:%s','batch.enable'='false','sink.format'='tablet')",
            sourceTypeName,
            targetTypeName,
            sourceTypeName,
            targetTypeName,
            receiverEnv.getIP(),
            receiverEnv.getPort());

    TestUtils.tryExecuteNonQueriesWithRetry(senderEnv, Collections.singletonList(sql));

    List<Pair> pairs = generateTestData(sourceTypeName);

    // wait pipe start
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    executeInserts(pairs, sourceType, targetType);
    return pairs;
  }

  private List<Pair> generateTestData(String sourceType) {
    switch (sourceType) {
      case "BOOLEAN":
        return generateBooleanTestData();
      case "INT32":
        return generateInt32TestData();
      case "INT64":
        return generateInt64TestData();
      case "FLOAT":
        return generateFloatTestData();
      case "DOUBLE":
        return generateDoubleTestData();
      case "TEXT":
        return generateTextTestData();
      case "TIMESTAMP":
        return generateTimestampTestData();
      case "DATE":
        return generateDateTestData();
      case "BLOB":
        return generateBlobTestData();
      case "STRING":
        return generateStringTestData();
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + sourceType);
    }
  }

  private void executeInserts(List<Pair> testData, TSDataType sourceType, TSDataType targetType) {
    switch (sourceType) {
      case STRING:
      case TEXT:
        TestUtils.tryExecuteNonQueriesWithRetry(
            senderEnv, generateInsertsStringData(testData, sourceType.name(), targetType.name()));
        return;
      case TIMESTAMP:
        TestUtils.tryExecuteNonQueriesWithRetry(
            senderEnv,
            generateInsertsTimestampData(testData, sourceType.name(), targetType.name()));
        return;
      case DATE:
        TestUtils.tryExecuteNonQueriesWithRetry(
            senderEnv,
            generateInsertsLocalDateData(testData, sourceType.name(), targetType.name()));
        return;
      case BLOB:
        TestUtils.tryExecuteNonQueriesWithRetry(
            senderEnv, generateInsertsBlobData(testData, sourceType.name(), targetType.name()));
        return;
      default:
        TestUtils.tryExecuteNonQueriesWithRetry(
            senderEnv, generateInsertsNumericData(testData, sourceType.name(), targetType.name()));
    }
  }

  private List<String> generateInsertsStringData(
      List<Pair> testData, String sourceType, String targetType) {
    List<String> executes = new ArrayList<>();
    for (Pair pair : testData) {
      executes.add(
          String.format(
              "insert into root.%s2%s.test(timestamp,status) values (%s,'%s');",
              sourceType,
              targetType,
              pair.left,
              new String(((Binary) (pair.right)).getValues(), StandardCharsets.UTF_8)));
    }
    executes.add("flush");
    return executes;
  }

  private List<String> generateInsertsNumericData(
      List<Pair> testData, String sourceType, String targetType) {
    List<String> executes = new ArrayList<>();
    for (Pair pair : testData) {
      executes.add(
          String.format(
              "insert into root.%s2%s.test(timestamp,status) values (%s,%s);",
              sourceType, targetType, pair.left, pair.right));
    }
    executes.add("flush");
    return executes;
  }

  private List<String> generateInsertsTimestampData(
      List<Pair> testData, String sourceType, String targetType) {
    List<String> executes = new ArrayList<>();
    DateTimeFormatter formatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
    for (Pair pair : testData) {
      executes.add(
          String.format(
              "insert into root.%s2%s.test(timestamp,status) values (%s,%s);",
              sourceType, targetType, pair.left, pair.right));
    }
    executes.add("flush");
    return executes;
  }

  private List<String> generateInsertsLocalDateData(
      List<Pair> testData, String sourceType, String targetType) {
    List<String> executes = new ArrayList<>();
    for (Pair pair : testData) {
      executes.add(
          String.format(
              "insert into root.%s2%s.test(timestamp,status) values (%s,'%s');",
              sourceType, targetType, pair.left, DateUtils.formatDate((Integer) pair.right)));
    }
    executes.add("flush");
    return executes;
  }

  private List<String> generateInsertsBlobData(
      List<Pair> testData, String sourceType, String targetType) {
    List<String> executes = new ArrayList<>();
    for (Pair pair : testData) {
      String value = BytesUtils.parseBlobByteArrayToString(((Binary) pair.right).getValues());
      executes.add(
          String.format(
              "insert into root.%s2%s.test(timestamp,status) values (%s,X'%s');",
              sourceType, targetType, pair.left, value.substring(2)));
    }
    executes.add("flush");
    return executes;
  }

  private Set<String> generateResultSet(
      List<Pair> pairs, TSDataType sourceType, TSDataType targetType) {
    switch (targetType) {
      case TIMESTAMP:
        return generateTimestampResultSet(pairs, sourceType, targetType);
      case DATE:
        return generateLocalDateResultSet(pairs, sourceType, targetType);
      case BLOB:
        return generateBlobDateResultSet(pairs, sourceType, targetType);
      case TEXT:
      case STRING:
        return generateStringDateResultSet(pairs, sourceType, targetType);
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
    DateTimeFormatter formatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
    for (Pair pair : pairs) {
      resultSet.add(
          String.format(
              "%s,%s,",
              pair.left,
              RpcUtils.parseLongToDateWithPrecision(
                  formatter,
                  (Long) ValueConverter.convert(sourceType, targetType, pair.right),
                  ZoneId.of("+0000"),
                  "ms")));
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

  private Set<String> generateBlobDateResultSet(
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

  private Set<String> generateStringDateResultSet(
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

  private List<Pair> generateBooleanTestData() {
    List<Pair> pairs = new java.util.ArrayList<>();
    for (long i = 0; i < generateDataSize; i++) {
      pairs.add(new Pair<>(i, Math.random() > 0.5));
    }
    return pairs;
  }

  private List<Pair> generateInt32TestData() {
    List<Pair> pairs = new ArrayList<>();
    for (long i = 0; i < generateDataSize; i++) {
      pairs.add(new Pair<>(i, (int) i));
    }
    return pairs;
  }

  private List<Pair> generateInt64TestData() {
    List<Pair> pairs = new ArrayList<>();
    for (long i = 0; i < generateDataSize; i++) {
      pairs.add(new Pair<>(i, i));
    }
    return pairs;
  }

  private List<Pair> generateFloatTestData() {
    List<Pair> pairs = new ArrayList<>();
    for (long i = 0; i < generateDataSize; i++) {
      pairs.add(new Pair<>(i, ((float) Math.random() * 100)));
    }
    return pairs;
  }

  private List<Pair> generateDoubleTestData() {
    List<Pair> pairs = new ArrayList<>();
    for (long i = 0; i < generateDataSize; i++) {
      pairs.add(new Pair<>(i, Math.random() * 1000000));
    }
    return pairs;
  }

  private List<Pair> generateTextTestData() {
    List<Pair> pairs = new ArrayList<>();
    for (long i = 0; i < generateDataSize; i++) {
      pairs.add(new Pair<>(i, new Binary((String.valueOf(i)).getBytes(StandardCharsets.UTF_8))));
    }
    return pairs;
  }

  private List<Pair> generateTimestampTestData() {
    List<Pair> pairs = new ArrayList<>();
    for (long i = 0; i < generateDataSize; i++) {
      long timestamp = new Date().getTime();
      pairs.add(new Pair<>(i, timestamp));
    }
    return pairs;
  }

  private List<Pair> generateDateTestData() {
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

  private List<Pair> generateBlobTestData() {
    List<Pair> pairs = new ArrayList<>();
    for (long i = 0; i < generateDataSize; i++) {
      pairs.add(new Pair<>(i, new Binary((String.valueOf(i)).getBytes(StandardCharsets.UTF_8))));
    }
    return pairs;
  }

  private List<Pair> generateStringTestData() {
    List<Pair> pairs = new ArrayList<>();
    for (long i = 0; i < generateDataSize; i++) {
      pairs.add(new Pair<>(i, new Binary((String.valueOf(i)).getBytes(StandardCharsets.UTF_8))));
    }
    return pairs;
  }
}
