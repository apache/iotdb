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

import java.util.Arrays;
import java.util.Properties;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class IoTDBInterpreterTest {

  private IoTDBInterpreter interpreter;

  @Before
  public void open() {
    Properties properties = new Properties();
    interpreter = new IoTDBInterpreter(properties);
    interpreter.open();
  }

  @After
  public void close() {
    interpreter.close();
  }

  @Test
  public void TestMultiLines() {
    String insert = "SET STORAGE GROUP TO root.ln.wf01.wt01;\n"
        + "CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN;\n"
        + "CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=PLAIN;\n"
        + "CREATE TIMESERIES root.ln.wf01.wt01.hardware WITH DATATYPE=INT32, ENCODING=PLAIN;\n"
        + "\n"
        + "INSERT INTO root.ln.wf01.wt01 (timestamp, temperature, status, hardware)\n"
        + "VALUES (1, 1.1, false, 11);\n"
        + "\n"
        + "INSERT INTO root.ln.wf01.wt01 (timestamp, temperature, status, hardware)\n"
        + "VALUES (2, 2.2, true, 22);\n"
        + "\n"
        + "INSERT INTO root.ln.wf01.wt01 (timestamp, temperature, status, hardware)\n"
        + "VALUES (3, 3.3, false, 33);\n"
        + "\n"
        + "INSERT INTO root.ln.wf01.wt01 (timestamp, temperature, status, hardware)\n"
        + "VALUES (4, 4.4, false, 44);\n"
        + "\n"
        + "INSERT INTO root.ln.wf01.wt01 (timestamp, temperature, status, hardware)\n"
        + "VALUES (5, 5.5, false, 55);\n"
        + "\n"
        + "\n";
    String[] gt = new String[]{
        "SET STORAGE GROUP TO root.ln.wf01.wt01",
        "CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf01.wt01.hardware WITH DATATYPE=INT32, ENCODING=PLAIN",
        "INSERT INTO root.ln.wf01.wt01 (timestamp, temperature, status, hardware) VALUES (1, 1.1, false, 11)",
        "INSERT INTO root.ln.wf01.wt01 (timestamp, temperature, status, hardware) VALUES (2, 2.2, true, 22)",
        "INSERT INTO root.ln.wf01.wt01 (timestamp, temperature, status, hardware) VALUES (3, 3.3, false, 33)",
        "INSERT INTO root.ln.wf01.wt01 (timestamp, temperature, status, hardware) VALUES (4, 4.4, false, 44)",
        "INSERT INTO root.ln.wf01.wt01 (timestamp, temperature, status, hardware) VALUES (5, 5.5, false, 55)",
    };
    Assert.assertArrayEquals(gt, IoTDBInterpreter.parseMultiLinesSQL(insert));
  }

  @Test
  public void TestMultiLines2() {
    String query = "INSERT INTO root.ln.wf01.wt01 (timestamp, temperature, status, hardware)\n"
        + "VALUES (4, 4.4, false, 44);\n"
        + "\n"
        + "INSERT INTO root.ln.wf01.wt01 (timestamp, temperature, status, hardware)\n"
        + "VALUES (5, 5.5, false, 55);\n"
        + "\n"
        + "\n"
        + "SELECT *\n"
        + "FROM root.ln.wf01.wt01\n"
        + "WHERE time >= 1\n"
        + "\tAND time <= 6;";

    String[] gt = new String[]{
        "INSERT INTO root.ln.wf01.wt01 (timestamp, temperature, status, hardware) VALUES (4, 4.4, false, 44)",
        "INSERT INTO root.ln.wf01.wt01 (timestamp, temperature, status, hardware) VALUES (5, 5.5, false, 55)",
        "SELECT * FROM root.ln.wf01.wt01 WHERE time >= 1  AND time <= 6",
    };
    Assert.assertArrayEquals(gt, IoTDBInterpreter.parseMultiLinesSQL(query));
  }

  @Test
  public void testNonQuery() {
    for (int i = 0; i < 100; i++) {
      String script = String
          .format("INSERT INTO root.sg1.d1.test(timestamp,temperature) VALUES(%d,%f)", i,
              Math.random() * 10);
      InterpreterResult interpreterResult = interpreter.interpret(script, null);
      System.out.println(interpreterResult.message());
    }
  }

  @Test
  public void testQuery() {
    InterpreterResult interpreterResult = interpreter
        .interpret("select * from root.sg1.d1.test", null);
    System.out.print(interpreterResult.message().get(0).getData());
  }

  @Test
  public void testException() {
    InterpreterResult interpreterResult;
    String wrongSql;

    wrongSql = "select * from";
    System.out.println("input: " + wrongSql);
    interpreterResult = interpreter.interpret(wrongSql, null);
    System.out.println(interpreterResult.message().get(0).getData());

    wrongSql = "select * from a";
    System.out.println("input: " + wrongSql);
    interpreterResult = interpreter.interpret(wrongSql, null);
    System.out.println(interpreterResult.message().get(0).getData());

    wrongSql = "select * from root a";
    System.out.println("input: " + wrongSql);
    interpreterResult = interpreter.interpret(wrongSql, null);
    System.out.println(interpreterResult.message().get(0).getData());
  }
}