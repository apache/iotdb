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

package org.apache.iotdb.db.it.trigger;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBTriggerExecutionIT {

  private static final String TRIGGER_COUNTER_PREFIX =
      System.getProperty("user.dir")
          + File.separator
          + "target"
          + File.separator
          + "test-classes"
          + File.separator;

  private static final String TRIGGER_JAR_PREFIX =
      new File(TRIGGER_COUNTER_PREFIX).toURI().toString();

  private static final String STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX =
      "statelessTriggerBeforeInsertion_";

  private static final String STATELESS_TRIGGER_AFTER_INSERTION_PREFIX =
      "statelessTriggerAfterInsertion_";

  private static final String STATEFUL_TRIGGER_BEFORE_INSERTION_PREFIX =
      "statefulTriggerBeforeInsertion_";

  private static final String STATEFUL_TRIGGER_AFTER_INSERTION_PREFIX =
      "statefulTriggerAfterInsertion_";

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    createTimeSeries();
    createTriggers();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    dropTriggers();
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void createTimeSeries() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.test");
      statement.execute(
          "CREATE TIMESERIES root.test.stateless.a with datatype=INT32,encoding=PLAIN");
      statement.execute(
          "CREATE TIMESERIES root.test.stateless.b with datatype=INT32,encoding=PLAIN");
      statement.execute(
          "CREATE TIMESERIES root.test.stateless.c with datatype=INT32,encoding=PLAIN");
      statement.execute(
          "CREATE TIMESERIES root.test.stateful.a with datatype=INT32,encoding=PLAIN");
      statement.execute(
          "CREATE TIMESERIES root.test.stateful.b with datatype=INT32,encoding=PLAIN");
      statement.execute(
          "CREATE TIMESERIES root.test.stateful.c with datatype=INT32,encoding=PLAIN");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void createTriggers() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // create stateless triggers before insertion
      statement.execute(
          String.format(
              "create stateless trigger %s before insert on root.test.stateless.* as 'org.apache.iotdb.db.trigger.example.TriggerFireTimesCounter' using URI '%s' with (\"name\"=\"%s\")",
              STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX + "all",
              TRIGGER_JAR_PREFIX + "TriggerFireTimesCounter.jar",
              STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX + "all"));
      statement.execute(
          String.format(
              "create stateless trigger %s before insert on root.test.stateless.a as 'org.apache.iotdb.db.trigger.example.TriggerFireTimesCounter' using URI '%s' with (\"name\"=\"%s\")",
              STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX + "a",
              TRIGGER_JAR_PREFIX + "TriggerFireTimesCounter.jar",
              STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX + "a"));

      // create stateless triggers after insertion
      statement.execute(
          String.format(
              "create stateless trigger %s after insert on root.test.stateless.* as 'org.apache.iotdb.db.trigger.example.TriggerFireTimesCounter' using URI '%s' with (\"name\"=\"%s\")",
              STATELESS_TRIGGER_AFTER_INSERTION_PREFIX + "all",
              TRIGGER_JAR_PREFIX + "TriggerFireTimesCounter.jar",
              STATELESS_TRIGGER_AFTER_INSERTION_PREFIX + "all"));
      statement.execute(
          String.format(
              "create stateless trigger %s after insert on root.test.stateless.a as 'org.apache.iotdb.db.trigger.example.TriggerFireTimesCounter' using URI '%s' with (\"name\"=\"%s\")",
              STATELESS_TRIGGER_AFTER_INSERTION_PREFIX + "a",
              TRIGGER_JAR_PREFIX + "TriggerFireTimesCounter.jar",
              STATELESS_TRIGGER_AFTER_INSERTION_PREFIX + "a"));

      // create stateful triggers before insertion
      statement.execute(
          String.format(
              "create stateful trigger %s before insert on root.test.stateful.* as 'org.apache.iotdb.db.trigger.example.TriggerFireTimesCounter' using URI '%s' with (\"name\"=\"%s\")",
              STATEFUL_TRIGGER_BEFORE_INSERTION_PREFIX + "all",
              TRIGGER_JAR_PREFIX + "TriggerFireTimesCounter.jar",
              STATEFUL_TRIGGER_BEFORE_INSERTION_PREFIX + "all"));
      statement.execute(
          String.format(
              "create stateful trigger %s before insert on root.test.stateful.a as 'org.apache.iotdb.db.trigger.example.TriggerFireTimesCounter' using URI '%s' with (\"name\"=\"%s\")",
              STATEFUL_TRIGGER_BEFORE_INSERTION_PREFIX + "a",
              TRIGGER_JAR_PREFIX + "TriggerFireTimesCounter.jar",
              STATEFUL_TRIGGER_BEFORE_INSERTION_PREFIX + "a"));

      // create stateful triggers after insertion
      statement.execute(
          String.format(
              "create stateful trigger %s after insert on root.test.stateful.* as 'org.apache.iotdb.db.trigger.example.TriggerFireTimesCounter' using URI '%s' with (\"name\"=\"%s\")",
              STATEFUL_TRIGGER_AFTER_INSERTION_PREFIX + "all",
              TRIGGER_JAR_PREFIX + "TriggerFireTimesCounter.jar",
              STATEFUL_TRIGGER_AFTER_INSERTION_PREFIX + "all"));
      statement.execute(
          String.format(
              "create stateful trigger %s after insert on root.test.stateful.a as 'org.apache.iotdb.db.trigger.example.TriggerFireTimesCounter' using URI '%s' with (\"name\"=\"%s\")",
              STATEFUL_TRIGGER_AFTER_INSERTION_PREFIX + "a",
              TRIGGER_JAR_PREFIX + "TriggerFireTimesCounter.jar",
              STATEFUL_TRIGGER_AFTER_INSERTION_PREFIX + "a"));
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  private static void dropTriggers() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          String.format("drop trigger %s", STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX + "all"));
      statement.execute(
          String.format("drop trigger %s", STATELESS_TRIGGER_AFTER_INSERTION_PREFIX + "all"));
      statement.execute(
          String.format("drop trigger %s", STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX + "a"));
      statement.execute(
          String.format("drop trigger %s", STATELESS_TRIGGER_AFTER_INSERTION_PREFIX + "a"));
      statement.execute(
          String.format("drop trigger %s", STATEFUL_TRIGGER_BEFORE_INSERTION_PREFIX + "all"));
      statement.execute(
          String.format("drop trigger %s", STATEFUL_TRIGGER_AFTER_INSERTION_PREFIX + "all"));
      statement.execute(
          String.format("drop trigger %s", STATEFUL_TRIGGER_BEFORE_INSERTION_PREFIX + "a"));
      statement.execute(
          String.format("drop trigger %s", STATEFUL_TRIGGER_AFTER_INSERTION_PREFIX + "a"));
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testFireTimesOfStatelessTrigger() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // 10 times is enough for checking whether fire times is correct
      int rows = 10;
      for (int i = 0; i < rows; i++) {
        statement.execute(
            String.format(
                "insert into root.test.stateless(time,a,b,c) values (%d,%d,%d,%d)", i, i, i, i));
        statement.execute(
            String.format("insert into root.test.stateless(time,a) values (%d,%d)", i, i));
        statement.execute(
            String.format("insert into root.test.stateless(time,b) values (%d,%d)", i, i));
        statement.execute(
            String.format("insert into root.test.stateless(time,c) values (%d,%d)", i, i));
      }

      // fire times of statelessTrigger_all = (1 + 3) * rows
      Assert.assertEquals(rows * 4, getCounter(STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX + "all"));
      Assert.assertEquals(rows * 4, getCounter(STATELESS_TRIGGER_AFTER_INSERTION_PREFIX + "all"));
      // fire times of statelessTrigger_a = (1 + 1) * rows
      Assert.assertEquals(rows * 2, getCounter(STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX + "a"));
      Assert.assertEquals(rows * 2, getCounter(STATELESS_TRIGGER_AFTER_INSERTION_PREFIX + "a"));
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testFireTimesOfStatefulTrigger() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // 10 times is enough for checking whether fire times is correct
      int rows = 10;
      for (int i = 0; i < rows; i++) {
        statement.execute(
            String.format(
                "insert into root.test.stateful(time,a,b,c) values (%d,%d,%d,%d)", i, i, i, i));
        statement.execute(
            String.format("insert into root.test.stateful(time,a) values (%d,%d)", i, i));
        statement.execute(
            String.format("insert into root.test.stateful(time,b) values (%d,%d)", i, i));
        statement.execute(
            String.format("insert into root.test.stateful(time,c) values (%d,%d)", i, i));
      }

      // fire times of statefulTrigger_all = (1 + 3) * rows
      Assert.assertEquals(rows * 4, getCounter(STATEFUL_TRIGGER_BEFORE_INSERTION_PREFIX + "all"));
      Assert.assertEquals(rows * 4, getCounter(STATEFUL_TRIGGER_AFTER_INSERTION_PREFIX + "all"));
      // fire times of statefulTrigger_a = (1 + 1) * rows
      Assert.assertEquals(rows * 2, getCounter(STATEFUL_TRIGGER_BEFORE_INSERTION_PREFIX + "a"));
      Assert.assertEquals(rows * 2, getCounter(STATEFUL_TRIGGER_AFTER_INSERTION_PREFIX + "a"));
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  private int getCounter(String counterName) throws IOException {
    String counterFilePath = TRIGGER_COUNTER_PREFIX + counterName + ".txt";
    int counter = 0;
    try (InputStreamReader Reader =
            new InputStreamReader(
                Files.newInputStream(new File(counterFilePath).toPath()), StandardCharsets.UTF_8);
        BufferedReader bufferedReader = new BufferedReader(Reader)) {
      String lineTxt;
      while ((lineTxt = bufferedReader.readLine()) != null) {
        if (!lineTxt.equals(System.lineSeparator())) {
          counter += Integer.parseInt(lineTxt);
        }
      }
      return counter;
    }
  }
}
