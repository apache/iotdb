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

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.utils.Pair;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

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
    encapsulateAndAssertConversion(TSDataType.BOOLEAN, TSDataType.VECTOR);
    encapsulateAndAssertConversion(TSDataType.BOOLEAN, TSDataType.TIMESTAMP);
    encapsulateAndAssertConversion(TSDataType.BOOLEAN, TSDataType.DATE);
    encapsulateAndAssertConversion(TSDataType.BOOLEAN, TSDataType.BLOB);
    encapsulateAndAssertConversion(TSDataType.BOOLEAN, TSDataType.STRING);
  }

  // Test for converting INT32 to OtherType
  @Test
  public void testInt32ToOtherTypeConversion() {
    //    encapsulateAndAssertConversion(TSDataType.INT32, TSDataType.BOOLEAN);
    encapsulateAndAssertConversion(TSDataType.INT32, TSDataType.INT32);
    //    encapsulateAndAssertConversion(TSDataType.INT32, TSDataType.FLOAT);
    //    encapsulateAndAssertConversion(TSDataType.INT32, TSDataType.DOUBLE);
    //    encapsulateAndAssertConversion(TSDataType.INT32, TSDataType.TEXT);
    //    encapsulateAndAssertConversion(TSDataType.INT32, TSDataType.VECTOR);
    //    encapsulateAndAssertConversion(TSDataType.INT32, TSDataType.TIMESTAMP);
    //    encapsulateAndAssertConversion(TSDataType.INT32, TSDataType.DATE);
    //    encapsulateAndAssertConversion(TSDataType.INT32, TSDataType.BLOB);
    //    encapsulateAndAssertConversion(TSDataType.INT32, TSDataType.STRING);
  }

  // Test for converting INT64 to OtherType
  @Test
  public void testInt64ToOtherTypeConversion() {
    encapsulateAndAssertConversion(TSDataType.INT64, TSDataType.BOOLEAN);
    encapsulateAndAssertConversion(TSDataType.INT64, TSDataType.INT32);
    encapsulateAndAssertConversion(TSDataType.INT64, TSDataType.FLOAT);
    encapsulateAndAssertConversion(TSDataType.INT64, TSDataType.DOUBLE);
    encapsulateAndAssertConversion(TSDataType.INT64, TSDataType.TEXT);
    encapsulateAndAssertConversion(TSDataType.INT64, TSDataType.VECTOR);
    encapsulateAndAssertConversion(TSDataType.INT64, TSDataType.UNKNOWN);
    encapsulateAndAssertConversion(TSDataType.INT64, TSDataType.TIMESTAMP);
    encapsulateAndAssertConversion(TSDataType.INT64, TSDataType.DATE);
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
    encapsulateAndAssertConversion(TSDataType.FLOAT, TSDataType.VECTOR);
    encapsulateAndAssertConversion(TSDataType.FLOAT, TSDataType.TIMESTAMP);
    encapsulateAndAssertConversion(TSDataType.FLOAT, TSDataType.DATE);
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
    encapsulateAndAssertConversion(TSDataType.DOUBLE, TSDataType.VECTOR);
    encapsulateAndAssertConversion(TSDataType.DOUBLE, TSDataType.TIMESTAMP);
    encapsulateAndAssertConversion(TSDataType.DOUBLE, TSDataType.DATE);
    encapsulateAndAssertConversion(TSDataType.DOUBLE, TSDataType.BLOB);
    encapsulateAndAssertConversion(TSDataType.DOUBLE, TSDataType.STRING);
  }

  // Test for converting TEXT to OtherType
  @Test
  public void testTextToOtherTypeConversion() {
    encapsulateAndAssertConversion(TSDataType.TEXT, TSDataType.VECTOR);
    encapsulateAndAssertConversion(TSDataType.TEXT, TSDataType.BLOB);
    encapsulateAndAssertConversion(TSDataType.TEXT, TSDataType.STRING);
  }

  // Test for converting VECTOR to OtherType
  @Test
  public void testVectorToOtherTypeConversion() {}

  // Test for converting TIMESTAMP to OtherType
  @Test
  public void testTimestampToOtherTypeConversion() {
    encapsulateAndAssertConversion(TSDataType.TIMESTAMP, TSDataType.BOOLEAN);
    encapsulateAndAssertConversion(TSDataType.TIMESTAMP, TSDataType.INT32);
    encapsulateAndAssertConversion(TSDataType.TIMESTAMP, TSDataType.INT64);
    encapsulateAndAssertConversion(TSDataType.TIMESTAMP, TSDataType.FLOAT);
    encapsulateAndAssertConversion(TSDataType.TIMESTAMP, TSDataType.DOUBLE);
    encapsulateAndAssertConversion(TSDataType.TIMESTAMP, TSDataType.TEXT);
    encapsulateAndAssertConversion(TSDataType.TIMESTAMP, TSDataType.VECTOR);
    encapsulateAndAssertConversion(TSDataType.TIMESTAMP, TSDataType.DATE);
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
    encapsulateAndAssertConversion(TSDataType.BLOB, TSDataType.VECTOR);
    encapsulateAndAssertConversion(TSDataType.BLOB, TSDataType.TEXT);
    encapsulateAndAssertConversion(TSDataType.BLOB, TSDataType.STRING);
  }

  // Test for converting STRING to OtherType
  @Test
  public void testStringToOtherTypeConversion() {
    encapsulateAndAssertConversion(TSDataType.STRING, TSDataType.VECTOR);
    encapsulateAndAssertConversion(TSDataType.STRING, TSDataType.TEXT);
    encapsulateAndAssertConversion(TSDataType.STRING, TSDataType.BLOB);
  }

  private void encapsulateAndAssertConversion(TSDataType source, TSDataType target) {
    List<Pair> pairs = encapsulateConversion(source.name(), target.name());
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "select * from root.test.**",
        String.format("Time,root.test.%s2%s.status,", source.name(), target.name()),
        generateResultSet(pairs, source, target),
        10);
  }

  private List<Pair> encapsulateConversion(String sourceType, String targetType) {
    TestUtils.tryExecuteNonQueriesWithRetry(
        senderEnv,
        Collections.singletonList(
            String.format(
                "create timeseries root.test.%s2%s.status with datatype=%s,encoding=PLAIN",
                sourceType, targetType, sourceType)));

    TestUtils.tryExecuteNonQueriesWithRetry(
        receiverEnv,
        Collections.singletonList(
            String.format(
                "create timeseries root.test.%s2%s.status with datatype=%s,encoding=PLAIN",
                sourceType, targetType, targetType)));

    String sql =
        String.format(
            "create pipe %s2%s"
                + " with source ('source'='iotdb-source', 'source.path'='root.test.**')"
                + " with processor ('processor'='do-nothing-processor')"
                + " with sink ('node-urls'='%s:%s')",
            sourceType, targetType, receiverEnv.getIP(), receiverEnv.getPort());

    TestUtils.tryExecuteNonQueriesWithRetry(senderEnv, Collections.singletonList(sql));

    // wait pipe start
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    List<Pair> testData = null;
    switch (sourceType) {
      case "BOOLEAN":
        testData = generateBooleanTestData();
        break;
      case "INT32":
        testData = generateInt32TestData();
        break;
      case "INT64":
        testData = generateInt64TestData();
        break;
      case "FLOAT":
        testData = generateFloatTestData();
        break;
      case "DOUBLE":
        testData = generateDoubleTestData();
        break;
      case "TEXT":
        testData = generateTextTestData();
        break;
      case "TIMESTAMP":
        testData = generateTimestampTestData();
        break;
      case "DATE":
        testData = generateDateTestData();
        break;
      case "BLOB":
        testData = generateBlobTestData();
        break;
      case "STRING":
        testData = generateStringTestData();
        break;
    }
    List<String> executes = new ArrayList<>((int) generateDataSize);
    for (Pair pair : testData) {
      executes.add(
          String.format(
              "insert into root.test.%s2%s(timestamp,status) values (%s,%s)",
              sourceType, targetType, pair.left, pair.right));
    }
    executes.add("flush");
    TestUtils.tryExecuteNonQueriesWithRetry(senderEnv, executes);
    return testData;
  }

  private Set<String> generateResultSet(
      List<Pair> pairs, TSDataType sourceType, TSDataType targetType) {
    HashSet<String> resultSet = new HashSet<>();
    for (Pair pair : pairs) {
      resultSet.add(
          String.format(
              "%s,%s,", pair.left, ValueConverter.convert(sourceType, targetType, pair.right)));
    }
    return resultSet;
  }

  private List<Pair> generateBooleanTestData() {
    List<Pair> pairs = new java.util.ArrayList<>();
    for (long i = 1; i < generateDataSize; i++) {
      pairs.add(new Pair<>(i, Math.random() > 0.5));
    }
    return pairs;
  }

  private List<Pair> generateInt32TestData() {
    List<Pair> pairs = new ArrayList<>();
    for (long i = 1; i < generateDataSize; i++) {
      pairs.add(new Pair<>(i, (int) i));
    }
    return pairs;
  }

  private List<Pair> generateInt64TestData() {
    List<Pair> pairs = new ArrayList<>();
    for (long i = 1; i < generateDataSize; i++) {
      pairs.add(new Pair<>(i, i));
    }
    return pairs;
  }

  private List<Pair> generateFloatTestData() {
    List<Pair> pairs = new ArrayList<>();
    for (long i = 1; i < generateDataSize; i++) {
      pairs.add(new Pair<>(i, ((float) Math.random() * 100)));
    }
    return pairs;
  }

  private List<Pair> generateDoubleTestData() {
    List<Pair> pairs = new ArrayList<>();
    for (long i = 1; i < generateDataSize; i++) {
      pairs.add(new Pair<>(i, Math.random() * 1000000));
    }
    return pairs;
  }

  private List<Pair> generateTextTestData() {
    List<Pair> pairs = new ArrayList<>();
    for (long i = 1; i < generateDataSize; i++) {
      pairs.add(new Pair<>(i, String.valueOf(i)));
    }
    return pairs;
  }

  private List<Pair> generateTimestampTestData() {
    List<Pair> pairs = new ArrayList<>();
    for (long i = 1; i < generateDataSize; i++) {
      long timestamp = new Date().getTime();
      pairs.add(new Pair<>(i, timestamp));
    }
    return pairs;
  }

  private List<Pair> generateDateTestData() {
    List<Pair> pairs = new ArrayList<>();
    for (long i = 1; i < generateDataSize; i++) {
      Date date = DateUtils.parseIntToDate((int) i);
      pairs.add(new Pair<>(i, date));
    }
    return pairs;
  }

  private List<Pair> generateBlobTestData() {
    List<Pair> pairs = new ArrayList<>();
    for (long i = 1; i < generateDataSize; i++) {
      pairs.add(new Pair<>(i, String.valueOf(i)));
    }
    return pairs;
  }

  private List<Pair> generateStringTestData() {
    List<Pair> pairs = new ArrayList<>();
    for (long i = 1; i < generateDataSize; i++) {
      pairs.add(new Pair<>(i, String.valueOf(i)));
    }
    return pairs;
  }
}
