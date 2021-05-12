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
package org.apache.zeppelin.iotdb;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

import static org.apache.zeppelin.iotdb.IoTDBInterpreter.DEFAULT_ENABLE_RPC_COMPRESSION;
import static org.apache.zeppelin.iotdb.IoTDBInterpreter.DEFAULT_FETCH_SIZE;
import static org.apache.zeppelin.iotdb.IoTDBInterpreter.DEFAULT_HOST;
import static org.apache.zeppelin.iotdb.IoTDBInterpreter.DEFAULT_PORT;
import static org.apache.zeppelin.iotdb.IoTDBInterpreter.DEFAULT_TIME_DISPLAY_TYPE;
import static org.apache.zeppelin.iotdb.IoTDBInterpreter.DEFAULT_ZONE_ID;
import static org.apache.zeppelin.iotdb.IoTDBInterpreter.IOTDB_ENABLE_RPC_COMPRESSION;
import static org.apache.zeppelin.iotdb.IoTDBInterpreter.IOTDB_FETCH_SIZE;
import static org.apache.zeppelin.iotdb.IoTDBInterpreter.IOTDB_HOST;
import static org.apache.zeppelin.iotdb.IoTDBInterpreter.IOTDB_PASSWORD;
import static org.apache.zeppelin.iotdb.IoTDBInterpreter.IOTDB_PORT;
import static org.apache.zeppelin.iotdb.IoTDBInterpreter.IOTDB_TIME_DISPLAY_TYPE;
import static org.apache.zeppelin.iotdb.IoTDBInterpreter.IOTDB_USERNAME;
import static org.apache.zeppelin.iotdb.IoTDBInterpreter.IOTDB_ZONE_ID;
import static org.apache.zeppelin.iotdb.IoTDBInterpreter.SET_TIMESTAMP_DISPLAY;

public class IoTDBInterpreterTest {

  private IoTDBInterpreter interpreter;

  @Before
  public void open() {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    Properties properties = new Properties();
    properties.put(IOTDB_HOST, DEFAULT_HOST);
    properties.put(IOTDB_PORT, DEFAULT_PORT);
    properties.put(IOTDB_USERNAME, "root");
    properties.put(IOTDB_PASSWORD, "root");
    properties.put(IOTDB_FETCH_SIZE, DEFAULT_FETCH_SIZE);
    properties.put(IOTDB_ZONE_ID, DEFAULT_ZONE_ID);
    properties.put(IOTDB_ENABLE_RPC_COMPRESSION, DEFAULT_ENABLE_RPC_COMPRESSION);
    properties.put(IOTDB_TIME_DISPLAY_TYPE, DEFAULT_TIME_DISPLAY_TYPE);
    interpreter = new IoTDBInterpreter(properties);
    interpreter.open();
    initInsert();
  }

  private void initInsert() {
    interpreter.internalInterpret("set storage group to root.test.wf01", null);
    interpreter.internalInterpret(
        "INSERT INTO root.test.wf01.wt01 (timestamp, temperature, status, hardware) VALUES (1, 1.1, false, 11)",
        null);
    interpreter.internalInterpret(
        "INSERT INTO root.test.wf01.wt01 (timestamp, temperature, status, hardware) VALUES (2, 2.2, true, 22)",
        null);
    interpreter.internalInterpret(
        "INSERT INTO root.test.wf01.wt01 (timestamp, temperature, status, hardware) VALUES (3, 3.3, false, 33)",
        null);
    interpreter.internalInterpret(
        "INSERT INTO root.test.wf01.wt01 (timestamp, temperature, status, hardware) VALUES (4, 4.4, false, 44)",
        null);
    interpreter.internalInterpret(
        "INSERT INTO root.test.wf01.wt01 (timestamp, temperature, status, hardware) VALUES (5, 5.5, false, 55)",
        null);

    interpreter.internalInterpret("set storage group to root.test.wf02", null);
    interpreter.internalInterpret(
        "INSERT INTO root.test.wf02.wt02 (timestamp, temperature, status, hardware) VALUES (44, 4.4, false, 44)",
        null);
    interpreter.internalInterpret(
        "INSERT INTO root.test.wf02.wt02 (timestamp, temperature, status, hardware) VALUES (54, 5.5, false, 55)",
        null);
  }

  @After
  public void close() throws IOException, StorageEngineException {
    interpreter.close();
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testNonQuery() {
    for (int i = 0; i < 100; i++) {
      String script =
          String.format(
              "INSERT INTO root.test.wf02(timestamp,temperature) VALUES(%d,%f)",
              i, Math.random() * 10);
      InterpreterResult actual = interpreter.internalInterpret(script, null);
      Assert.assertNotNull(actual);
      Assert.assertEquals(Code.SUCCESS, actual.code());
      Assert.assertEquals("Sql executed.", actual.message().get(0).getData());
    }
  }

  @Test
  public void testSelectColumnStatement() {
    InterpreterResult actual =
        interpreter.internalInterpret("select status from root.test.wf01.wt01", null);
    String gt =
        "Time\troot.test.wf01.wt01.status\n"
            + "1\tfalse\n"
            + "2\ttrue\n"
            + "3\tfalse\n"
            + "4\tfalse\n"
            + "5\tfalse";
    Assert.assertNotNull(actual);
    Assert.assertEquals(Code.SUCCESS, actual.code());
    Assert.assertEquals(gt, actual.message().get(0).getData());
  }

  @Test
  public void testSetTimeDisplay() {
    String longGT =
        "Time\troot.test.wf01.wt01.status\n"
            + "1\tfalse\n"
            + "2\ttrue\n"
            + "3\tfalse\n"
            + "4\tfalse\n"
            + "5\tfalse";
    String isoGT =
        "Time\troot.test.wf01.wt01.status\n"
            + "1970-01-01T00:00:00.001Z\tfalse\n"
            + "1970-01-01T00:00:00.002Z\ttrue\n"
            + "1970-01-01T00:00:00.003Z\tfalse\n"
            + "1970-01-01T00:00:00.004Z\tfalse\n"
            + "1970-01-01T00:00:00.005Z\tfalse";
    String specialGT =
        "Time\troot.test.wf01.wt01.status\n"
            + "1970-01-01 00:00:00.001\tfalse\n"
            + "1970-01-01 00:00:00.002\ttrue\n"
            + "1970-01-01 00:00:00.003\tfalse\n"
            + "1970-01-01 00:00:00.004\tfalse\n"
            + "1970-01-01 00:00:00.005\tfalse";
    String specialGT2 =
        "Time\troot.test.wf01.wt01.status\n"
            + "1970-01 00:00\tfalse\n"
            + "1970-01 00:00\ttrue\n"
            + "1970-01 00:00\tfalse\n"
            + "1970-01 00:00\tfalse\n"
            + "1970-01 00:00\tfalse";

    testSetTimeDisplay("yyyy-MM-dd HH:mm:ss.SSS", specialGT);
    testSetTimeDisplay("yyyy-dd mm:ss", specialGT2);
    testSetTimeDisplay("iso8601", isoGT);
    testSetTimeDisplay("default", isoGT);
    testSetTimeDisplay("long", longGT);
    testSetTimeDisplay("number", longGT);
  }

  private void testSetTimeDisplay(String timeDisplay, String gt) {
    InterpreterResult actual =
        interpreter.internalInterpret(SET_TIMESTAMP_DISPLAY + "=" + timeDisplay, null);
    Assert.assertNotNull(actual);
    Assert.assertEquals(Code.SUCCESS, actual.code());
    Assert.assertEquals(
        "Time display type has set to " + timeDisplay, actual.message().get(0).getData());
    actual = interpreter.internalInterpret("select status from root.test.wf01.wt01", null);
    Assert.assertNotNull(actual);
    Assert.assertEquals(Code.SUCCESS, actual.code());
    Assert.assertEquals(gt, actual.message().get(0).getData());
  }

  @Test
  public void testSelectColumnStatementWithTimeFilter() {
    InterpreterResult actual =
        interpreter.internalInterpret(
            "select * from root.test.wf01.wt01 where time > 2 and time < 6", null);
    String gt =
        "Time\troot.test.wf01.wt01.temperature\troot.test.wf01.wt01.status\troot.test.wf01.wt01.hardware\n"
            + "3\t3.3\tfalse\t33.0\n"
            + "4\t4.4\tfalse\t44.0\n"
            + "5\t5.5\tfalse\t55.0";
    Assert.assertNotNull(actual);
    Assert.assertEquals(Code.SUCCESS, actual.code());
    Assert.assertEquals(gt, actual.message().get(0).getData());
  }

  @Test
  public void testException() {
    InterpreterResult actual;
    String wrongSql;

    wrongSql = "select * from";
    actual = interpreter.internalInterpret(wrongSql, null);
    Assert.assertNotNull(actual);
    Assert.assertEquals(Code.ERROR, actual.code());
    Assert.assertEquals(
        "SQLException: 401: Error occurred while parsing SQL to physical plan: line 1:13 missing ROOT at '<EOF>'",
        actual.message().get(0).getData());

    wrongSql = "select * from a";
    actual = interpreter.internalInterpret(wrongSql, null);
    Assert.assertNotNull(actual);
    Assert.assertEquals(Code.ERROR, actual.code());
    Assert.assertEquals(
        "SQLException: 401: Error occurred while parsing SQL to physical plan: line 1:14 mismatched input 'a' expecting {FROM, ',', '.'}",
        actual.message().get(0).getData());

    wrongSql = "select * from root a";
    actual = interpreter.internalInterpret(wrongSql, null);
    Assert.assertNotNull(actual);
    Assert.assertEquals(Code.ERROR, actual.code());
    Assert.assertEquals(
        "SQLException: 401: Error occurred while parsing SQL to physical plan: line 1:19 extraneous input 'a' expecting {<EOF>, ';'}",
        actual.message().get(0).getData());
  }

  @Test
  public void TestMultiLines() {
    String insert =
        "SET STORAGE GROUP TO root.test.wf01.wt01;\n"
            + "CREATE TIMESERIES root.test.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN;\n"
            + "CREATE TIMESERIES root.test.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=PLAIN;\n"
            + "CREATE TIMESERIES root.test.wf01.wt01.hardware WITH DATATYPE=INT32, ENCODING=PLAIN;\n"
            + "\n"
            + "INSERT INTO root.test.wf01.wt01 (timestamp, temperature, status, hardware)\n"
            + "VALUES (1, 1.1, false, 11);\n"
            + "\n"
            + "INSERT INTO root.test.wf01.wt01 (timestamp, temperature, status, hardware)\n"
            + "VALUES (2, 2.2, true, 22);\n"
            + "\n"
            + "INSERT INTO root.test.wf01.wt01 (timestamp, temperature, status, hardware)\n"
            + "VALUES (3, 3.3, false, 33);\n"
            + "\n"
            + "INSERT INTO root.test.wf01.wt01 (timestamp, temperature, status, hardware)\n"
            + "VALUES (4, 4.4, false, 44);\n"
            + "\n"
            + "INSERT INTO root.test.wf01.wt01 (timestamp, temperature, status, hardware)\n"
            + "VALUES (5, 5.5, false, 55);\n"
            + "\n"
            + "\n";
    String[] gt =
        new String[] {
          "SET STORAGE GROUP TO root.test.wf01.wt01",
          "CREATE TIMESERIES root.test.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
          "CREATE TIMESERIES root.test.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=PLAIN",
          "CREATE TIMESERIES root.test.wf01.wt01.hardware WITH DATATYPE=INT32, ENCODING=PLAIN",
          "INSERT INTO root.test.wf01.wt01 (timestamp, temperature, status, hardware) VALUES (1, 1.1, false, 11)",
          "INSERT INTO root.test.wf01.wt01 (timestamp, temperature, status, hardware) VALUES (2, 2.2, true, 22)",
          "INSERT INTO root.test.wf01.wt01 (timestamp, temperature, status, hardware) VALUES (3, 3.3, false, 33)",
          "INSERT INTO root.test.wf01.wt01 (timestamp, temperature, status, hardware) VALUES (4, 4.4, false, 44)",
          "INSERT INTO root.test.wf01.wt01 (timestamp, temperature, status, hardware) VALUES (5, 5.5, false, 55)",
        };
    Assert.assertArrayEquals(gt, IoTDBInterpreter.parseMultiLinesSQL(insert));
  }

  @Test
  public void TestMultiLines2() {
    String query =
        "INSERT INTO root.test.wf01.wt01 (timestamp, temperature, status, hardware)\n"
            + "VALUES (4, 4.4, false, 44);\n"
            + "\n"
            + "INSERT INTO root.test.wf01.wt01 (timestamp, temperature, status, hardware)\n"
            + "VALUES (5, 5.5, false, 55);\n"
            + "\n"
            + "\n"
            + "SELECT *\n"
            + "FROM root.test.wf01.wt01\n"
            + "WHERE time >= 1\n"
            + "\tAND time <= 6;";

    String[] gt =
        new String[] {
          "INSERT INTO root.test.wf01.wt01 (timestamp, temperature, status, hardware) VALUES (4, 4.4, false, 44)",
          "INSERT INTO root.test.wf01.wt01 (timestamp, temperature, status, hardware) VALUES (5, 5.5, false, 55)",
          "SELECT * FROM root.test.wf01.wt01 WHERE time >= 1  AND time <= 6",
        };
    Assert.assertArrayEquals(gt, IoTDBInterpreter.parseMultiLinesSQL(query));
  }

  @Test
  public void testShowVersion() {
    InterpreterResult actual = interpreter.internalInterpret("SHOW VERSION", null);
    String gt = "version\n" + IoTDBConstant.VERSION;
    Assert.assertNotNull(actual);
    Assert.assertEquals(Code.SUCCESS, actual.code());
    Assert.assertEquals(gt, actual.message().get(0).getData());
  }

  @Test
  public void testShowTimeseries() {
    InterpreterResult actual = interpreter.internalInterpret("show timeseries", null);
    String gt =
        "timeseries\talias\tstorage group\tdataType\tencoding\tcompression\ttags\tattributes\n"
            + "root.test.wf02.wt02.temperature\tnull\troot.test.wf02\tFLOAT\tGORILLA\tSNAPPY\tnull\tnull\n"
            + "root.test.wf02.wt02.status\tnull\troot.test.wf02\tBOOLEAN\tRLE\tSNAPPY\tnull\tnull\n"
            + "root.test.wf02.wt02.hardware\tnull\troot.test.wf02\tFLOAT\tGORILLA\tSNAPPY\tnull\tnull\n"
            + "root.test.wf01.wt01.temperature\tnull\troot.test.wf01\tFLOAT\tGORILLA\tSNAPPY\tnull\tnull\n"
            + "root.test.wf01.wt01.status\tnull\troot.test.wf01\tBOOLEAN\tRLE\tSNAPPY\tnull\tnull\n"
            + "root.test.wf01.wt01.hardware\tnull\troot.test.wf01\tFLOAT\tGORILLA\tSNAPPY\tnull\tnull";
    Assert.assertNotNull(actual);
    Assert.assertEquals(Code.SUCCESS, actual.code());
    Assert.assertEquals(gt, actual.message().get(0).getData());
  }

  @Test
  public void testShowDevices() {
    InterpreterResult actual = interpreter.internalInterpret("show devices", null);
    String gt = "devices\n" + "root.test.wf01.wt01\n" + "root.test.wf02.wt02";
    Assert.assertNotNull(actual);
    Assert.assertEquals(Code.SUCCESS, actual.code());
    Assert.assertEquals(gt, actual.message().get(0).getData());
  }

  @Test
  public void testShowDevicesWithSg() {
    InterpreterResult actual =
        interpreter.internalInterpret("show devices with storage group", null);
    String gt =
        "devices\tstorage group\n"
            + "root.test.wf01.wt01\troot.test.wf01\n"
            + "root.test.wf02.wt02\troot.test.wf02";
    Assert.assertNotNull(actual);
    Assert.assertEquals(Code.SUCCESS, actual.code());
    System.out.println(actual.message().get(0).getData());
    Assert.assertEquals(gt, actual.message().get(0).getData());
  }

  @Test
  public void testShowAllTTL() {
    interpreter.internalInterpret("SET TTL TO root.test.wf01 12345", null);
    InterpreterResult actual = interpreter.internalInterpret("SHOW ALL TTL", null);
    String gt = "storage group\tttl\n" + "root.test.wf02\tnull\n" + "root.test.wf01\t12345";
    Assert.assertNotNull(actual);
    Assert.assertEquals(Code.SUCCESS, actual.code());
    Assert.assertEquals(gt, actual.message().get(0).getData());
  }

  @Test
  public void testShowTTL() {
    interpreter.internalInterpret("SET TTL TO root.test.wf01 12345", null);
    InterpreterResult actual = interpreter.internalInterpret("SHOW TTL ON root.test.wf01", null);
    String gt = "storage group\tttl\n" + "root.test.wf01\t12345";
    Assert.assertNotNull(actual);
    Assert.assertEquals(Code.SUCCESS, actual.code());
    Assert.assertEquals(gt, actual.message().get(0).getData());
  }

  @Test
  public void testShowStorageGroup() {
    InterpreterResult actual = interpreter.internalInterpret("SHOW STORAGE GROUP", null);
    String gt = "storage group\n" + "root.test.wf02\n" + "root.test.wf01";
    Assert.assertNotNull(actual);
    Assert.assertEquals(Code.SUCCESS, actual.code());
    Assert.assertEquals(gt, actual.message().get(0).getData());
  }

  @Test
  public void testListUser() {
    interpreter.internalInterpret("CREATE USER user1 'password1'", null);
    InterpreterResult actual = interpreter.internalInterpret("LIST USER", null);
    String gt = "user\n" + "root\n" + "user1";
    Assert.assertNotNull(actual);
    Assert.assertEquals(Code.SUCCESS, actual.code());
    Assert.assertEquals(gt, actual.message().get(0).getData());
  }
}
