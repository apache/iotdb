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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
// todo: add StandaloneIT.class when supporting trigger on Standalone
@Category({ClusterIT.class})
public class IoTDBTriggerManagementIT {
  private static final String TRIGGER_JAR_PREFIX =
      System.getProperty("user.dir")
          + File.separator
          + "target"
          + File.separator
          + "test-classes"
          + File.separator;

  private static final String TRIGGER_FILE_TIMES_COUNTER =
      "org.apache.iotdb.db.trigger.example.TriggerFireTimesCounter";

  private static final String STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX =
      "statelessTriggerBeforeInsertion_";

  private static final String STATELESS_TRIGGER_AFTER_INSERTION_PREFIX =
      "statelessTriggerAfterInsertion_";

  private static final String STATEFUL_TRIGGER_BEFORE_INSERTION_PREFIX =
      "statefulTriggerBeforeInsertion_";

  private static final String STATEFUL_TRIGGER_AFTER_INSERTION_PREFIX =
      "statefulTriggerAfterInsertion_";

  private static final String STATEFUL = "STATEFUL";

  private static final String STATELESS = "STATELESS";

  private static final String BEFORE_INSERT = "BEFORE_INSERT";

  private static final String AFTER_INSERT = "AFTER_INSERT";

  private static final String ACTIVE = "ACTIVE";

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initBeforeClass();
    createTimeSeries();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterClass();
  }

  private static void createTimeSeries() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.test");
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

  @Test
  public void testCreateTriggersNormally() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      Map<String, String[]> result =
          new HashMap<String, String[]>() {
            {
              put(
                  STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX + "a",
                  new String[] {
                    STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX + "a",
                    BEFORE_INSERT,
                    STATELESS,
                    ACTIVE,
                    "root.test.stateless.a",
                    TRIGGER_FILE_TIMES_COUNTER
                  });
              put(
                  STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX + "all",
                  new String[] {
                    STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX + "all",
                    BEFORE_INSERT,
                    STATELESS,
                    ACTIVE,
                    "root.test.stateless.*",
                    TRIGGER_FILE_TIMES_COUNTER
                  });
              put(
                  STATELESS_TRIGGER_AFTER_INSERTION_PREFIX + "a",
                  new String[] {
                    STATELESS_TRIGGER_AFTER_INSERTION_PREFIX + "a",
                    AFTER_INSERT,
                    STATELESS,
                    ACTIVE,
                    "root.test.stateless.a",
                    TRIGGER_FILE_TIMES_COUNTER
                  });
              put(
                  STATELESS_TRIGGER_AFTER_INSERTION_PREFIX + "all",
                  new String[] {
                    STATELESS_TRIGGER_AFTER_INSERTION_PREFIX + "all",
                    AFTER_INSERT,
                    STATELESS,
                    ACTIVE,
                    "root.test.stateless.*",
                    TRIGGER_FILE_TIMES_COUNTER
                  });
              put(
                  STATEFUL_TRIGGER_BEFORE_INSERTION_PREFIX + "a",
                  new String[] {
                    STATEFUL_TRIGGER_BEFORE_INSERTION_PREFIX + "a",
                    BEFORE_INSERT,
                    STATEFUL,
                    ACTIVE,
                    "root.test.stateful.a",
                    TRIGGER_FILE_TIMES_COUNTER
                  });
              put(
                  STATEFUL_TRIGGER_BEFORE_INSERTION_PREFIX + "all",
                  new String[] {
                    STATEFUL_TRIGGER_BEFORE_INSERTION_PREFIX + "all",
                    BEFORE_INSERT,
                    STATEFUL,
                    ACTIVE,
                    "root.test.stateful.*",
                    TRIGGER_FILE_TIMES_COUNTER
                  });
              put(
                  STATEFUL_TRIGGER_AFTER_INSERTION_PREFIX + "a",
                  new String[] {
                    STATEFUL_TRIGGER_AFTER_INSERTION_PREFIX + "a",
                    AFTER_INSERT,
                    STATEFUL,
                    ACTIVE,
                    "root.test.stateful.a",
                    TRIGGER_FILE_TIMES_COUNTER
                  });
              put(
                  STATEFUL_TRIGGER_AFTER_INSERTION_PREFIX + "all",
                  new String[] {
                    STATEFUL_TRIGGER_AFTER_INSERTION_PREFIX + "all",
                    AFTER_INSERT,
                    STATEFUL,
                    ACTIVE,
                    "root.test.stateful.*",
                    TRIGGER_FILE_TIMES_COUNTER
                  });
            }
          };

      // create stateless triggers before insertion
      statement.execute(
          String.format(
              "create stateless trigger %s before insert on root.test.stateless.a as '%s' using file '%s' with (\"name\"=\"%s\")",
              STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX + "a",
              TRIGGER_FILE_TIMES_COUNTER,
              TRIGGER_JAR_PREFIX + "TriggerFireTimesCounter.jar",
              STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX + "a"));
      statement.execute(
          String.format(
              "create stateless trigger %s before insert on root.test.stateless.* as '%s' using file '%s' with (\"name\"=\"%s\")",
              STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX + "all",
              TRIGGER_FILE_TIMES_COUNTER,
              TRIGGER_JAR_PREFIX + "TriggerFireTimesCounter.jar",
              STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX + "all"));

      // create stateless triggers after insertion
      statement.execute(
          String.format(
              "create stateless trigger %s after insert on root.test.stateless.a as '%s' using file '%s' with (\"name\"=\"%s\")",
              STATELESS_TRIGGER_AFTER_INSERTION_PREFIX + "a",
              TRIGGER_FILE_TIMES_COUNTER,
              TRIGGER_JAR_PREFIX + "TriggerFireTimesCounter.jar",
              STATELESS_TRIGGER_AFTER_INSERTION_PREFIX + "a"));
      statement.execute(
          String.format(
              "create stateless trigger %s after insert on root.test.stateless.* as '%s' using file '%s' with (\"name\"=\"%s\")",
              STATELESS_TRIGGER_AFTER_INSERTION_PREFIX + "all",
              TRIGGER_FILE_TIMES_COUNTER,
              TRIGGER_JAR_PREFIX + "TriggerFireTimesCounter.jar",
              STATELESS_TRIGGER_AFTER_INSERTION_PREFIX + "all"));

      // create stateful triggers before insertion
      statement.execute(
          String.format(
              "create stateful trigger %s before insert on root.test.stateful.a as '%s' using file '%s' with (\"name\"=\"%s\")",
              STATEFUL_TRIGGER_BEFORE_INSERTION_PREFIX + "a",
              TRIGGER_FILE_TIMES_COUNTER,
              TRIGGER_JAR_PREFIX + "TriggerFireTimesCounter.jar",
              STATEFUL_TRIGGER_BEFORE_INSERTION_PREFIX + "a"));
      statement.execute(
          String.format(
              "create stateful trigger %s before insert on root.test.stateful.* as '%s' using file '%s' with (\"name\"=\"%s\")",
              STATEFUL_TRIGGER_BEFORE_INSERTION_PREFIX + "all",
              TRIGGER_FILE_TIMES_COUNTER,
              TRIGGER_JAR_PREFIX + "TriggerFireTimesCounter.jar",
              STATEFUL_TRIGGER_BEFORE_INSERTION_PREFIX + "all"));

      // create stateful triggers after insertion
      statement.execute(
          String.format(
              "create stateful trigger %s after insert on root.test.stateful.a as '%s' using file '%s' with (\"name\"=\"%s\")",
              STATEFUL_TRIGGER_AFTER_INSERTION_PREFIX + "a",
              TRIGGER_FILE_TIMES_COUNTER,
              TRIGGER_JAR_PREFIX + "TriggerFireTimesCounter.jar",
              STATEFUL_TRIGGER_AFTER_INSERTION_PREFIX + "a"));
      statement.execute(
          String.format(
              "create stateful trigger %s after insert on root.test.stateful.* as '%s' using file '%s' with (\"name\"=\"%s\")",
              STATEFUL_TRIGGER_AFTER_INSERTION_PREFIX + "all",
              TRIGGER_FILE_TIMES_COUNTER,
              TRIGGER_JAR_PREFIX + "TriggerFireTimesCounter.jar",
              STATEFUL_TRIGGER_AFTER_INSERTION_PREFIX + "all"));
      ResultSet resultSet = statement.executeQuery("show triggers");
      int cnt = 0;
      while (resultSet.next()) {
        cnt++;
        String triggerName = resultSet.getString("TriggerName");
        String[] triggerInformation = result.get(triggerName);
        assertEquals(triggerInformation[0], triggerName);
        assertEquals(triggerInformation[1], resultSet.getString("Event"));
        assertEquals(triggerInformation[2], resultSet.getString("Type"));
        assertEquals(triggerInformation[3], resultSet.getString("State"));
        assertEquals(triggerInformation[4], resultSet.getString("PathPattern"));
        assertEquals(triggerInformation[5], resultSet.getString("ClassName"));
      }
      assertEquals(cnt, result.size());
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testCreateAndDropMultipleTimes() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create stateless trigger %s before insert on root.test.stateless.* as '%s' using file '%s' with (\"name\"=\"%s\")",
              STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX + "all",
              TRIGGER_FILE_TIMES_COUNTER,
              TRIGGER_JAR_PREFIX + "TriggerFireTimesCounter.jar",
              STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX + "all"));
      statement.execute(
          String.format(
              "create stateful trigger %s after insert on root.test.stateful.a as '%s' using file '%s' with (\"name\"=\"%s\")",
              STATEFUL_TRIGGER_AFTER_INSERTION_PREFIX + "a",
              TRIGGER_FILE_TIMES_COUNTER,
              TRIGGER_JAR_PREFIX + "TriggerFireTimesCounter.jar",
              STATEFUL_TRIGGER_AFTER_INSERTION_PREFIX + "a"));

      // drop triggers
      statement.execute(
          String.format("drop trigger %s", STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX + "all"));
      statement.execute(
          String.format("drop trigger %s", STATEFUL_TRIGGER_AFTER_INSERTION_PREFIX + "a"));
      ResultSet resultSet = statement.executeQuery("show triggers");
      assertFalse(resultSet.next());

      statement.execute(
          String.format(
              "create stateless trigger %s before insert on root.test.stateless.* as '%s' using file '%s' with (\"name\"=\"%s\")",
              STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX + "all",
              TRIGGER_FILE_TIMES_COUNTER,
              TRIGGER_JAR_PREFIX + "TriggerFireTimesCounter.jar",
              STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX + "all"));
      statement.execute(
          String.format(
              "create stateful trigger %s after insert on root.test.stateful.a as '%s' using file '%s' with (\"name\"=\"%s\")",
              STATEFUL_TRIGGER_AFTER_INSERTION_PREFIX + "a",
              TRIGGER_FILE_TIMES_COUNTER,
              TRIGGER_JAR_PREFIX + "TriggerFireTimesCounter.jar",
              STATEFUL_TRIGGER_AFTER_INSERTION_PREFIX + "a"));

      // drop triggers
      statement.execute(
          String.format("drop trigger %s", STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX + "all"));
      statement.execute(
          String.format("drop trigger %s", STATEFUL_TRIGGER_AFTER_INSERTION_PREFIX + "a"));
      resultSet = statement.executeQuery("show triggers");
      assertFalse(resultSet.next());

      statement.execute(
          String.format(
              "create stateless trigger %s before insert on root.test.stateless.* as '%s' using file '%s' with (\"name\"=\"%s\")",
              STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX + "all",
              TRIGGER_FILE_TIMES_COUNTER,
              TRIGGER_JAR_PREFIX + "TriggerFireTimesCounter.jar",
              STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX + "all"));
      statement.execute(
          String.format(
              "create stateful trigger %s after insert on root.test.stateful.a as '%s' using file '%s' with (\"name\"=\"%s\")",
              STATEFUL_TRIGGER_AFTER_INSERTION_PREFIX + "a",
              TRIGGER_FILE_TIMES_COUNTER,
              TRIGGER_JAR_PREFIX + "TriggerFireTimesCounter.jar",
              STATEFUL_TRIGGER_AFTER_INSERTION_PREFIX + "a"));
      resultSet = statement.executeQuery("show triggers");
      int cnt = 0;
      while (resultSet.next()) {
        cnt++;
      }
      assertEquals(cnt, 2);

    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testCreateTriggerWithSameName() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create stateless trigger %s before insert on root.test.stateless.* as '%s' using file '%s' with (\"name\"=\"%s\")",
              STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX + "all",
              TRIGGER_FILE_TIMES_COUNTER,
              TRIGGER_JAR_PREFIX + "TriggerFireTimesCounter.jar",
              STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX + "all"));

      try {
        statement.execute(
            String.format(
                "create stateless trigger %s before insert on root.test.stateless.* as '%s' using file '%s' with (\"name\"=\"%s\")",
                STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX + "all",
                TRIGGER_FILE_TIMES_COUNTER,
                TRIGGER_JAR_PREFIX + "TriggerFireTimesCounter.jar",
                STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX + "all"));
      } catch (SQLException e) {
        assertTrue(
            e.getMessage().contains("same name") && e.getMessage().contains("has been created"));
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testTriggerNameCaseSensitivity() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create stateless trigger %s before insert on root.test.stateless.* as '%s' using file '%s' with (\"name\"=\"%s\")",
              "test",
              TRIGGER_FILE_TIMES_COUNTER,
              TRIGGER_JAR_PREFIX + "TriggerFireTimesCounter.jar",
              STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX + "all"));

      statement.execute(
          String.format(
              "create stateless trigger %s before insert on root.test.stateless.* as '%s' using file '%s' with (\"name\"=\"%s\")",
              "Test",
              TRIGGER_FILE_TIMES_COUNTER,
              TRIGGER_JAR_PREFIX + "TriggerFireTimesCounter.jar",
              STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX + "all"));

      statement.execute(
          String.format(
              "create stateless trigger %s before insert on root.test.stateless.* as '%s' using file '%s' with (\"name\"=\"%s\")",
              "TEST",
              TRIGGER_FILE_TIMES_COUNTER,
              TRIGGER_JAR_PREFIX + "TriggerFireTimesCounter.jar",
              STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX + "all"));

    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testDropTriggersAfterCreationNormally() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // create first
      statement.execute(
          String.format(
              "create stateless trigger %s before insert on root.test.stateless.* as '%s' using file '%s' with (\"name\"=\"%s\")",
              STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX + "all",
              TRIGGER_FILE_TIMES_COUNTER,
              TRIGGER_JAR_PREFIX + "TriggerFireTimesCounter.jar",
              STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX + "all"));
      statement.execute(
          String.format(
              "create stateless trigger %s after insert on root.test.stateless.* as '%s' using file '%s' with (\"name\"=\"%s\")",
              STATELESS_TRIGGER_AFTER_INSERTION_PREFIX + "all",
              TRIGGER_FILE_TIMES_COUNTER,
              TRIGGER_JAR_PREFIX + "TriggerFireTimesCounter.jar",
              STATELESS_TRIGGER_AFTER_INSERTION_PREFIX + "all"));
      statement.execute(
          String.format(
              "create stateful trigger %s before insert on root.test.stateful.* as '%s' using file '%s' with (\"name\"=\"%s\")",
              STATEFUL_TRIGGER_BEFORE_INSERTION_PREFIX + "all",
              TRIGGER_FILE_TIMES_COUNTER,
              TRIGGER_JAR_PREFIX + "TriggerFireTimesCounter.jar",
              STATEFUL_TRIGGER_BEFORE_INSERTION_PREFIX + "all"));
      statement.execute(
          String.format(
              "create stateful trigger %s after insert on root.test.stateful.* as '%s' using file '%s' with (\"name\"=\"%s\")",
              STATEFUL_TRIGGER_AFTER_INSERTION_PREFIX + "all",
              TRIGGER_FILE_TIMES_COUNTER,
              TRIGGER_JAR_PREFIX + "TriggerFireTimesCounter.jar",
              STATEFUL_TRIGGER_AFTER_INSERTION_PREFIX + "all"));

      // drop triggers
      statement.execute(
          String.format("drop trigger %s", STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX + "all"));
      statement.execute(
          String.format("drop trigger %s", STATELESS_TRIGGER_AFTER_INSERTION_PREFIX + "all"));
      statement.execute(
          String.format("drop trigger %s", STATEFUL_TRIGGER_BEFORE_INSERTION_PREFIX + "all"));
      statement.execute(
          String.format("drop trigger %s", STATEFUL_TRIGGER_AFTER_INSERTION_PREFIX + "all"));

      ResultSet resultSet = statement.executeQuery("show triggers");
      assertFalse(resultSet.next());
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testDropBeforeCreation() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // drop trigger before creation
      statement.execute(
          String.format("drop trigger %s", STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX + "all"));
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("has not been created"));
    }
  }
}
