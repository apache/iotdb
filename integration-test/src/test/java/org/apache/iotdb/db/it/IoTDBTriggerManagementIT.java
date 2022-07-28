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

package org.apache.iotdb.db.it;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBTriggerManagementIT {

  private static final String TRIGGER_NAME = "trigger name";
  private static final String STATUS = "status";
  private static final String EVENT = "event";
  private static final String PATH = "path";
  private static final String CLASS_NAME = "class name";
  private static final String ATTRIBUTES = "attributes";

  private static final String STATUS_STARTED = "started";
  private static final String STATUS_STOPPED = "stopped";
  private static final String EVENT_BEFORE = "before insert";
  private static final String EVENT_AFTER = "after insert";

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initBeforeTest();
    createTimeseries();
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanAfterTest();
  }

  private void createTimeseries() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE TIMESERIES root.vehicle.d1.s1 WITH DATATYPE=FLOAT, ENCODING=PLAIN, COMPRESSION=UNCOMPRESSED");
      statement.execute(
          "CREATE TIMESERIES root.vehicle.d1.s2 WITH DATATYPE=DOUBLE, ENCODING=GORILLA, COMPRESSION=LZ4");
      statement.execute(
          "CREATE TIMESERIES root.vehicle.d1.s3 WITH DATATYPE=INT32, ENCODING=RLE, COMPRESSION=LZ4");
      statement.execute(
          "CREATE TIMESERIES root.vehicle.d1.s4 WITH DATATYPE=INT64, ENCODING=RLE, COMPRESSION=LZ4");
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testManageTriggersNormally() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // show
      ResultSet resultSet = statement.executeQuery("show triggers");
      assertFalse(resultSet.next());

      // create trigger
      statement.execute(
          "create trigger trigger_1 before insert on root.vehicle.d1.s1 as 'org.apache.iotdb.db.engine.trigger.example.Accumulator'");
      statement.execute(
          "create trigger trigger_2 after insert on root.vehicle.d1.s2 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");

      // show
      resultSet = statement.executeQuery("show triggers");

      assertTrue(resultSet.next());
      assertEquals("trigger_1", resultSet.getObject(TRIGGER_NAME));
      assertEquals(STATUS_STARTED, resultSet.getObject(STATUS));
      assertEquals(EVENT_BEFORE, resultSet.getObject(EVENT));

      assertTrue(resultSet.next());
      assertEquals("trigger_2", resultSet.getObject(TRIGGER_NAME));
      assertEquals(STATUS_STARTED, resultSet.getObject(STATUS));
      assertEquals(EVENT_AFTER, resultSet.getObject(EVENT));

      assertFalse(resultSet.next());

      // stop trigger
      statement.execute("stop trigger trigger_1");

      // show
      resultSet = statement.executeQuery("show triggers");

      assertTrue(resultSet.next());
      assertEquals("trigger_1", resultSet.getObject(TRIGGER_NAME));
      assertEquals(STATUS_STOPPED, resultSet.getObject(STATUS));
      assertEquals(EVENT_BEFORE, resultSet.getObject(EVENT));

      assertTrue(resultSet.next());
      assertEquals("trigger_2", resultSet.getObject(TRIGGER_NAME));
      assertEquals(STATUS_STARTED, resultSet.getObject(STATUS));
      assertEquals(EVENT_AFTER, resultSet.getObject(EVENT));

      assertFalse(resultSet.next());

      // start trigger
      statement.execute("start trigger trigger_1");

      // show
      resultSet = statement.executeQuery("show triggers");

      assertTrue(resultSet.next());
      assertEquals("trigger_1", resultSet.getObject(TRIGGER_NAME));
      assertEquals(STATUS_STARTED, resultSet.getObject(STATUS));
      assertEquals(EVENT_BEFORE, resultSet.getObject(EVENT));

      assertTrue(resultSet.next());
      assertEquals("trigger_2", resultSet.getObject(TRIGGER_NAME));
      assertEquals(STATUS_STARTED, resultSet.getObject(STATUS));
      assertEquals(EVENT_AFTER, resultSet.getObject(EVENT));

      assertFalse(resultSet.next());

      // drop trigger
      statement.execute("drop trigger trigger_1");
      statement.execute("drop trigger trigger_2");

      // show
      resultSet = statement.executeQuery("show triggers");
      assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testRegisterOnNonMeasurementMNode() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create trigger trigger_2 before insert on root.vehicle.d1 as 'org.apache.iotdb.db.engine.trigger.example.Accumulator'");
    } catch (SQLException throwable) {
      assertTrue(
          throwable.getMessage().contains("MNode [root.vehicle.d1] is not a MeasurementMNode."));
    }
  }

  @Test
  public void testRegisterOnNonExistentMNode() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create trigger trigger_2 before insert on root.nonexistent.d1 as 'org.apache.iotdb.db.engine.trigger.example.Accumulator'");
    } catch (SQLException throwable) {
      assertTrue(throwable.getMessage().contains("Path [root.nonexistent.d1] does not exist"));
    }
  }

  @Test
  public void testRegisterInvalidClass() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create trigger trigger_2 before insert on root.vehicle.d1.s1 as 'org.apache.iotdb.db.engine.trigger.example.Nonexistent'");
    } catch (SQLException throwable) {
      assertTrue(throwable.getMessage().contains("Failed to reflect Trigger trigger_2"));
    }
  }

  @Test
  public void testRegisterSameTriggers() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create trigger trigger_1 before insert on root.vehicle.d1.s1 as 'org.apache.iotdb.db.engine.trigger.example.Accumulator'");
      statement.execute(
          "create trigger trigger_1 after insert on root.vehicle.d1.s2 as 'org.apache.iotdb.db.engine.trigger.example.Accumulator'");
    } catch (SQLException throwable) {
      assertTrue(
          throwable
              .getMessage()
              .contains(
                  "a trigger with the same trigger name and the class name has already been registered"));
    }
  }

  @Test
  public void testRegisterOnSameTimeseries() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create trigger trigger_1 before insert on root.vehicle.d1.s1 as 'org.apache.iotdb.db.engine.trigger.example.Accumulator'");

      try {
        statement.execute(
            "create trigger trigger_2 before insert on root.vehicle.d1.s1 as 'org.apache.iotdb.db.engine.trigger.example.Accumulator'");
      } catch (SQLException throwable) {
        assertTrue(
            throwable
                .getMessage()
                .contains(
                    "because a trigger trigger_1(org.apache.iotdb.db.engine.trigger.example.Accumulator) has already been registered on the timeseries root.vehicle.d1.s1"));
      }

      try {
        statement.execute(
            "create trigger trigger_3 after insert on root.vehicle.d1.s1 as 'org.apache.iotdb.db.engine.trigger.example.Accumulator'");
      } catch (SQLException throwable) {
        assertTrue(
            throwable
                .getMessage()
                .contains(
                    "because a trigger trigger_1(org.apache.iotdb.db.engine.trigger.example.Accumulator) has already been registered on the timeseries root.vehicle.d1.s1"));
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testRegisterTriggersWithSameNameButDifferentClasses() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create trigger trigger_1 before insert on root.vehicle.d1.s1 as 'org.apache.iotdb.db.engine.trigger.example.Accumulator'");
      statement.execute(
          "create trigger trigger_1 after insert on root.vehicle.d1.s2 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
    } catch (SQLException throwable) {
      assertTrue(
          throwable
              .getMessage()
              .contains(
                  "with the same trigger name but a different class name has already been registered"));
    }
  }

  @Test
  public void testCreateAndDropSeveralTimes() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create trigger trigger_1 before insert on root.vehicle.d1.s1 as 'org.apache.iotdb.db.engine.trigger.example.Accumulator'");
      statement.execute(
          "create trigger trigger_2 after insert on root.vehicle.d1.s2 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");

      statement.execute("drop trigger trigger_1");
      statement.execute("drop trigger trigger_2");

      statement.execute(
          "create trigger trigger_1 before insert on root.vehicle.d1.s1 as 'org.apache.iotdb.db.engine.trigger.example.Accumulator'");
      statement.execute(
          "create trigger trigger_2 after insert on root.vehicle.d1.s2 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");

      statement.execute("drop trigger trigger_1");
      statement.execute("drop trigger trigger_2");

      statement.execute(
          "create trigger trigger_1 before insert on root.vehicle.d1.s1 as 'org.apache.iotdb.db.engine.trigger.example.Accumulator'");
      statement.execute(
          "create trigger trigger_2 after insert on root.vehicle.d1.s2 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");

      // show
      ResultSet resultSet = statement.executeQuery("show triggers");

      assertTrue(resultSet.next());
      assertEquals("trigger_1", resultSet.getObject(TRIGGER_NAME));
      assertEquals(STATUS_STARTED, resultSet.getObject(STATUS));
      assertEquals(EVENT_BEFORE, resultSet.getObject(EVENT));

      assertTrue(resultSet.next());
      assertEquals("trigger_2", resultSet.getObject(TRIGGER_NAME));
      assertEquals(STATUS_STARTED, resultSet.getObject(STATUS));
      assertEquals(EVENT_AFTER, resultSet.getObject(EVENT));

      assertFalse(resultSet.next());

      statement.execute("drop trigger trigger_1");
      statement.execute("drop trigger trigger_2");

      resultSet = statement.executeQuery("show triggers");
      assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testDropNonExistentTrigger() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("drop trigger trigger_1");
    } catch (SQLException throwable) {
      assertTrue(throwable.getMessage().contains("Trigger trigger_1 does not exist"));
    }
  }

  @Test
  public void testStartNonExistentTrigger() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("start trigger trigger_1");
    } catch (SQLException throwable) {
      assertTrue(throwable.getMessage().contains("Trigger trigger_1 does not exist"));
    }
  }

  @Test
  public void testStartStartedTrigger() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create trigger trigger_1 before insert on root.vehicle.d1.s1 as 'org.apache.iotdb.db.engine.trigger.example.Accumulator'");
      statement.execute("start trigger trigger_1");
    } catch (SQLException throwable) {
      assertTrue(throwable.getMessage().contains("Trigger trigger_1 has already been started"));
    }
  }

  @Test
  public void testStopNonExistentTrigger() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("stop trigger trigger_1");
    } catch (SQLException throwable) {
      assertTrue(throwable.getMessage().contains("Trigger trigger_1 does not exist"));
    }
  }

  @Test
  public void testStopStoppedTrigger() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create trigger trigger_1 before insert on root.vehicle.d1.s1 as 'org.apache.iotdb.db.engine.trigger.example.Accumulator'");
      statement.execute("stop trigger trigger_1");
    } catch (SQLException throwable) {
      assertTrue(throwable.getMessage().contains("Trigger trigger_1 has already been stopped"));
    }
  }

  @Test
  public void testStopAndStartTriggerMultipleTimesNormally() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create trigger trigger_1 before insert on root.vehicle.d1.s1 as 'org.apache.iotdb.db.engine.trigger.example.Accumulator'");
      // show
      ResultSet resultSet = statement.executeQuery("show triggers");
      assertTrue(resultSet.next());
      assertEquals("trigger_1", resultSet.getObject(TRIGGER_NAME));
      assertEquals(STATUS_STARTED, resultSet.getObject(STATUS));

      statement.execute("stop trigger trigger_1");
      // show
      resultSet = statement.executeQuery("show triggers");
      assertTrue(resultSet.next());
      assertEquals("trigger_1", resultSet.getObject(TRIGGER_NAME));
      assertEquals(STATUS_STOPPED, resultSet.getObject(STATUS));

      statement.execute("start trigger trigger_1");
      // show
      resultSet = statement.executeQuery("show triggers");
      assertTrue(resultSet.next());
      assertEquals("trigger_1", resultSet.getObject(TRIGGER_NAME));
      assertEquals(STATUS_STARTED, resultSet.getObject(STATUS));

      statement.execute("stop trigger trigger_1");
      // show
      resultSet = statement.executeQuery("show triggers");
      assertTrue(resultSet.next());
      assertEquals("trigger_1", resultSet.getObject(TRIGGER_NAME));
      assertEquals(STATUS_STOPPED, resultSet.getObject(STATUS));

      statement.execute("start trigger trigger_1");
      // show
      resultSet = statement.executeQuery("show triggers");
      assertTrue(resultSet.next());
      assertEquals("trigger_1", resultSet.getObject(TRIGGER_NAME));
      assertEquals(STATUS_STARTED, resultSet.getObject(STATUS));
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  @Ignore
  @SuppressWarnings("squid:S5961")
  public void testRecovery() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create trigger trigger_1 before insert on root.vehicle.d1.s1 as 'org.apache.iotdb.db.engine.trigger.example.Accumulator' with ('k1'='v1', 'k2'='v2')");
      statement.execute("stop trigger trigger_1");
      statement.execute("start trigger trigger_1");
      statement.execute("drop trigger trigger_1");

      statement.execute(
          "create trigger trigger_1 before insert on root.vehicle.d1.s2 as 'org.apache.iotdb.db.engine.trigger.example.Counter' with ('k3'='v3')");
      statement.execute("stop trigger trigger_1");
      statement.execute("start trigger trigger_1");
      statement.execute("drop trigger trigger_1");

      statement.execute(
          "create trigger trigger_1 after insert on root.vehicle.d1.s3 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute(
          "create trigger trigger_2 before insert on root.vehicle.d1.s4 as 'org.apache.iotdb.db.engine.trigger.example.Accumulator' with ('k4'='v4')");
      statement.execute("stop trigger trigger_1");

    } catch (Exception e) {
      fail(e.getMessage());
    }

    //    EnvironmentUtils.restartDaemon();

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // show
      ResultSet resultSet = statement.executeQuery("show triggers");

      assertTrue(resultSet.next());
      assertEquals("trigger_1", resultSet.getObject(TRIGGER_NAME));
      assertEquals(STATUS_STOPPED, resultSet.getObject(STATUS));
      assertEquals(EVENT_AFTER, resultSet.getObject(EVENT));
      assertEquals("root.vehicle.d1.s3", resultSet.getObject(PATH));
      assertEquals(
          "org.apache.iotdb.db.engine.trigger.example.Counter", resultSet.getObject(CLASS_NAME));
      assertEquals("{}", resultSet.getObject(ATTRIBUTES));

      assertTrue(resultSet.next());
      assertEquals("trigger_2", resultSet.getObject(TRIGGER_NAME));
      assertEquals(STATUS_STARTED, resultSet.getObject(STATUS));
      assertEquals(EVENT_BEFORE, resultSet.getObject(EVENT));
      assertEquals("root.vehicle.d1.s4", resultSet.getObject(PATH));
      assertEquals(
          "org.apache.iotdb.db.engine.trigger.example.Accumulator",
          resultSet.getObject(CLASS_NAME));
      assertEquals("{k4=v4}", resultSet.getObject(ATTRIBUTES));

      assertFalse(resultSet.next());

      statement.execute("drop trigger trigger_2");
      statement.execute("drop trigger trigger_1");
      statement.execute(
          "create trigger trigger_2 after insert on root.vehicle.d1.s3 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute(
          "create trigger trigger_1 before insert on root.vehicle.d1.s4 as 'org.apache.iotdb.db.engine.trigger.example.Accumulator' with ('k5'='v5')");
      statement.execute("stop trigger trigger_2");
      statement.execute(
          "create trigger trigger_3 before insert on root.vehicle.d1.s2 as 'org.apache.iotdb.db.engine.trigger.example.Accumulator'");
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    //    EnvironmentUtils.restartDaemon();

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // show
      ResultSet resultSet = statement.executeQuery("show triggers");

      assertTrue(resultSet.next());
      assertEquals("trigger_3", resultSet.getObject(TRIGGER_NAME));
      assertEquals(STATUS_STARTED, resultSet.getObject(STATUS));
      assertEquals(EVENT_BEFORE, resultSet.getObject(EVENT));
      assertEquals("root.vehicle.d1.s2", resultSet.getObject(PATH));
      assertEquals(
          "org.apache.iotdb.db.engine.trigger.example.Accumulator",
          resultSet.getObject(CLASS_NAME));
      assertEquals("{}", resultSet.getObject(ATTRIBUTES));

      assertTrue(resultSet.next());
      assertEquals("trigger_1", resultSet.getObject(TRIGGER_NAME));
      assertEquals(STATUS_STARTED, resultSet.getObject(STATUS));
      assertEquals(EVENT_BEFORE, resultSet.getObject(EVENT));
      assertEquals("root.vehicle.d1.s4", resultSet.getObject(PATH));
      assertEquals(
          "org.apache.iotdb.db.engine.trigger.example.Accumulator",
          resultSet.getObject(CLASS_NAME));
      assertEquals("{k5=v5}", resultSet.getObject(ATTRIBUTES));

      assertTrue(resultSet.next());
      assertEquals("trigger_2", resultSet.getObject(TRIGGER_NAME));
      assertEquals(STATUS_STOPPED, resultSet.getObject(STATUS));
      assertEquals(EVENT_AFTER, resultSet.getObject(EVENT));
      assertEquals("root.vehicle.d1.s3", resultSet.getObject(PATH));
      assertEquals(
          "org.apache.iotdb.db.engine.trigger.example.Counter", resultSet.getObject(CLASS_NAME));
      assertEquals("{}", resultSet.getObject(ATTRIBUTES));

      assertFalse(resultSet.next());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
