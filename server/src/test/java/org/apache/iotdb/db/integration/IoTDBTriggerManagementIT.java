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

package org.apache.iotdb.db.integration;

import org.apache.iotdb.db.engine.trigger.example.Accumulator;
import org.apache.iotdb.db.engine.trigger.example.Counter;
import org.apache.iotdb.db.engine.trigger.executor.TriggerEvent;
import org.apache.iotdb.db.engine.trigger.service.TriggerRegistrationInformation;
import org.apache.iotdb.db.engine.trigger.service.TriggerRegistrationService;
import org.apache.iotdb.db.exception.TriggerManagementException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IoTDBTriggerManagementIT {

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d1.s1"),
        TSDataType.FLOAT,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d1.s2"),
        TSDataType.DOUBLE,
        TSEncoding.GORILLA,
        CompressionType.LZ4,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d1.s3"),
        TSDataType.INT32,
        TSEncoding.RLE,
        CompressionType.LZ4,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d1.s4"),
        TSDataType.INT64,
        TSEncoding.RLE,
        CompressionType.LZ4,
        null);
    Class.forName(Config.JDBC_DRIVER_NAME);
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testManageTriggersNormally() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      // show
      ResultSet resultSet = statement.executeQuery("show triggers");
      assertFalse(resultSet.next());

      // create trigger
      statement.execute(
          "create trigger trigger-1 before insert on root.vehicle.d1.s1 as 'org.apache.iotdb.db.engine.trigger.example.Accumulator'");
      statement.execute(
          "create trigger trigger-2 after insert on root.vehicle.d1.s2 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      assertFalse(
          ((Accumulator) TriggerRegistrationService.getInstance().getTriggerInstance("trigger-1"))
              .isStopped());
      assertFalse(
          ((Counter) TriggerRegistrationService.getInstance().getTriggerInstance("trigger-2"))
              .isStopped());

      // show
      resultSet = statement.executeQuery("show triggers");
      assertTrue(resultSet.next());
      assertTrue(resultSet.next());
      assertFalse(resultSet.next());

      // stop trigger
      statement.execute("stop trigger trigger-1");
      assertTrue(
          ((Accumulator) TriggerRegistrationService.getInstance().getTriggerInstance("trigger-1"))
              .isStopped());

      // show
      resultSet = statement.executeQuery("show triggers");
      assertTrue(resultSet.next());
      assertTrue(resultSet.next());
      assertFalse(resultSet.next());

      // start trigger
      statement.execute("start trigger trigger-1");
      assertFalse(
          ((Accumulator) TriggerRegistrationService.getInstance().getTriggerInstance("trigger-1"))
              .isStopped());

      // drop trigger
      statement.execute("drop trigger trigger-1");
      statement.execute("drop trigger trigger-2");

      // show
      resultSet = statement.executeQuery("show triggers");
      assertFalse(resultSet.next());
    } catch (SQLException | TriggerManagementException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testRegisterOnNonMeasurementMNode() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create trigger trigger-2 before insert on root.vehicle.d1 as 'org.apache.iotdb.db.engine.trigger.example.Accumulator'");
    } catch (SQLException throwable) {
      assertTrue(
          throwable.getMessage().contains("MNode [root.vehicle.d1] is not a MeasurementMNode."));
    }
  }

  @Test
  public void testRegisterOnNonExistentMNode() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create trigger trigger-2 before insert on root.nonexistent.d1 as 'org.apache.iotdb.db.engine.trigger.example.Accumulator'");
    } catch (SQLException throwable) {
      assertTrue(throwable.getMessage().contains("Path [root.nonexistent.d1] does not exist"));
    }
  }

  @Test
  public void testRegisterInvalidClass() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create trigger trigger-2 before insert on root.vehicle.d1.s1 as 'org.apache.iotdb.db.engine.trigger.example.Nonexistent'");
    } catch (SQLException throwable) {
      assertTrue(throwable.getMessage().contains("Failed to reflect Trigger trigger-2"));
    }
  }

  @Test
  public void testRegisterSameTriggers() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create trigger trigger-1 before insert on root.vehicle.d1.s1 as 'org.apache.iotdb.db.engine.trigger.example.Accumulator'");
      statement.execute(
          "create trigger trigger-1 after insert on root.vehicle.d1.s1 as 'org.apache.iotdb.db.engine.trigger.example.Accumulator'");
    } catch (SQLException throwable) {
      assertTrue(
          throwable
              .getMessage()
              .contains(
                  "a trigger with the same trigger name and the class name has already been registered"));
    }
  }

  @Test
  public void testRegisterTriggersWithSameNameButDifferentClasses() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create trigger trigger-1 before insert on root.vehicle.d1.s1 as 'org.apache.iotdb.db.engine.trigger.example.Accumulator'");
      statement.execute(
          "create trigger trigger-1 after insert on root.vehicle.d1.s2 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
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
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create trigger trigger-1 before insert on root.vehicle.d1.s1 as 'org.apache.iotdb.db.engine.trigger.example.Accumulator'");
      statement.execute(
          "create trigger trigger-2 after insert on root.vehicle.d1.s2 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");

      statement.execute("drop trigger trigger-1");
      statement.execute("drop trigger trigger-2");

      statement.execute(
          "create trigger trigger-1 before insert on root.vehicle.d1.s1 as 'org.apache.iotdb.db.engine.trigger.example.Accumulator'");
      statement.execute(
          "create trigger trigger-2 after insert on root.vehicle.d1.s2 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");

      statement.execute("drop trigger trigger-1");
      statement.execute("drop trigger trigger-2");

      statement.execute(
          "create trigger trigger-1 before insert on root.vehicle.d1.s1 as 'org.apache.iotdb.db.engine.trigger.example.Accumulator'");
      statement.execute(
          "create trigger trigger-2 after insert on root.vehicle.d1.s2 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");

      ResultSet resultSet = statement.executeQuery("show triggers");
      assertTrue(resultSet.next());
      assertTrue(resultSet.next());
      assertFalse(resultSet.next());

      statement.execute("drop trigger trigger-1");
      statement.execute("drop trigger trigger-2");

      resultSet = statement.executeQuery("show triggers");
      assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testCreateSeveralTimes() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create trigger trigger-1 before insert on root.vehicle.d1.s1 as 'org.apache.iotdb.db.engine.trigger.example.Accumulator'");
      ((Accumulator) TriggerRegistrationService.getInstance().getTriggerInstance("trigger-1"))
          .setAccumulator(1234);

      statement.execute(
          "create trigger trigger-2 after insert on root.vehicle.d1.s2 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute(
          "create trigger trigger-3 before insert on root.vehicle.d1.s3 as 'org.apache.iotdb.db.engine.trigger.example.Accumulator'");
      statement.execute(
          "create trigger trigger-4 after insert on root.vehicle.d1.s4 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");

      assertEquals(
          1234,
          ((Accumulator) TriggerRegistrationService.getInstance().getTriggerInstance("trigger-1"))
              .getAccumulator(),
          0);
    } catch (SQLException | TriggerManagementException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testDropNonExistentTrigger() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("drop trigger trigger-1");
    } catch (SQLException throwable) {
      assertTrue(throwable.getMessage().contains("Trigger trigger-1 does not exist"));
    }
  }

  @Test
  public void testStartNonExistentTrigger() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("start trigger trigger-1");
    } catch (SQLException throwable) {
      assertTrue(throwable.getMessage().contains("Trigger trigger-1 does not exist"));
    }
  }

  @Test
  public void testStartStartedTrigger() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create trigger trigger-1 before insert on root.vehicle.d1.s1 as 'org.apache.iotdb.db.engine.trigger.example.Accumulator'");
      statement.execute("start trigger trigger-1");
    } catch (SQLException throwable) {
      assertTrue(throwable.getMessage().contains("Trigger trigger-1 has already been started"));
    }
  }

  @Test
  public void testStopNonExistentTrigger() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("stop trigger trigger-1");
    } catch (SQLException throwable) {
      assertTrue(throwable.getMessage().contains("Trigger trigger-1 does not exist"));
    }
  }

  @Test
  public void testStopStoppedTrigger() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create trigger trigger-1 before insert on root.vehicle.d1.s1 as 'org.apache.iotdb.db.engine.trigger.example.Accumulator'");
      statement.execute("stop trigger trigger-1");
      statement.execute("stop trigger trigger-1");
    } catch (SQLException throwable) {
      assertTrue(throwable.getMessage().contains("Trigger trigger-1 has already been stopped"));
    }
  }

  @Test
  public void testStopAndStartTriggerMultipleTimesNormally() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create trigger trigger-1 before insert on root.vehicle.d1.s1 as 'org.apache.iotdb.db.engine.trigger.example.Accumulator'");
      assertFalse(
          TriggerRegistrationService.getInstance()
              .getRegistrationInformation("trigger-1")
              .isStopped());

      statement.execute("stop trigger trigger-1");
      assertTrue(
          TriggerRegistrationService.getInstance()
              .getRegistrationInformation("trigger-1")
              .isStopped());

      statement.execute("start trigger trigger-1");
      assertFalse(
          TriggerRegistrationService.getInstance()
              .getRegistrationInformation("trigger-1")
              .isStopped());

      statement.execute("stop trigger trigger-1");
      assertTrue(
          TriggerRegistrationService.getInstance()
              .getRegistrationInformation("trigger-1")
              .isStopped());

      statement.execute("start trigger trigger-1");
      assertFalse(
          TriggerRegistrationService.getInstance()
              .getRegistrationInformation("trigger-1")
              .isStopped());
    } catch (SQLException | TriggerManagementException e) {
      fail(e.getMessage());
    }
  }

  @Test
  @SuppressWarnings("squid:S5961")
  public void testRecovery() throws Exception {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create trigger trigger-1 before insert on root.vehicle.d1.s1 as 'org.apache.iotdb.db.engine.trigger.example.Accumulator' with ('k1'='v1', 'k2'='v2')");
      statement.execute("stop trigger trigger-1");
      statement.execute("start trigger trigger-1");
      statement.execute("drop trigger trigger-1");

      statement.execute(
          "create trigger trigger-1 before insert on root.vehicle.d1.s2 as 'org.apache.iotdb.db.engine.trigger.example.Counter' with ('k3'='v3')");
      statement.execute("stop trigger trigger-1");
      statement.execute("start trigger trigger-1");
      statement.execute("drop trigger trigger-1");

      statement.execute(
          "create trigger trigger-1 after insert on root.vehicle.d1.s3 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute(
          "create trigger trigger-2 before insert on root.vehicle.d1.s4 as 'org.apache.iotdb.db.engine.trigger.example.Accumulator' with ('k4'='v4')");
      statement.execute("stop trigger trigger-1");

    } catch (Exception e) {
      fail(e.getMessage());
    }

    EnvironmentUtils.restartDaemon();

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      TriggerRegistrationInformation trigger1Info =
          TriggerRegistrationService.getInstance().getRegistrationInformation("trigger-1");
      assertEquals("trigger-1", trigger1Info.getTriggerName());
      assertEquals("root.vehicle.d1.s3", trigger1Info.getFullPath().getFullPath());
      assertEquals(TriggerEvent.AFTER_INSERT, trigger1Info.getEvent());
      assertEquals(
          "org.apache.iotdb.db.engine.trigger.example.Counter", trigger1Info.getClassName());
      assertEquals("{}", trigger1Info.getAttributes().toString());
      assertTrue(trigger1Info.isStopped());

      TriggerRegistrationInformation trigger2Info =
          TriggerRegistrationService.getInstance().getRegistrationInformation("trigger-2");
      assertEquals("trigger-2", trigger2Info.getTriggerName());
      assertEquals("root.vehicle.d1.s4", trigger2Info.getFullPath().getFullPath());
      assertEquals(TriggerEvent.BEFORE_INSERT, trigger2Info.getEvent());
      assertEquals(
          "org.apache.iotdb.db.engine.trigger.example.Accumulator", trigger2Info.getClassName());
      assertEquals("{k4=v4}", trigger2Info.getAttributes().toString());
      assertFalse(trigger2Info.isStopped());

      statement.execute("drop trigger trigger-2");
      statement.execute(
          "create trigger trigger-2 after insert on root.vehicle.d1.s3 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute("drop trigger trigger-1");
      statement.execute(
          "create trigger trigger-1 before insert on root.vehicle.d1.s4 as 'org.apache.iotdb.db.engine.trigger.example.Accumulator' with ('k5'='v5')");
      statement.execute("stop trigger trigger-2");
      statement.execute(
          "create trigger trigger-3 before insert on root.vehicle.d1.s2 as 'org.apache.iotdb.db.engine.trigger.example.Accumulator'");
    } catch (Exception e) {
      fail(e.getMessage());
    }

    EnvironmentUtils.restartDaemon();

    TriggerRegistrationInformation trigger1Info =
        TriggerRegistrationService.getInstance().getRegistrationInformation("trigger-1");
    assertEquals("trigger-1", trigger1Info.getTriggerName());
    assertEquals("root.vehicle.d1.s4", trigger1Info.getFullPath().getFullPath());
    assertEquals(TriggerEvent.BEFORE_INSERT, trigger1Info.getEvent());
    assertEquals(
        "org.apache.iotdb.db.engine.trigger.example.Accumulator", trigger1Info.getClassName());
    assertEquals("{k5=v5}", trigger1Info.getAttributes().toString());
    assertFalse(trigger1Info.isStopped());

    TriggerRegistrationInformation trigger2Info =
        TriggerRegistrationService.getInstance().getRegistrationInformation("trigger-2");
    assertEquals("trigger-2", trigger2Info.getTriggerName());
    assertEquals("root.vehicle.d1.s3", trigger2Info.getFullPath().getFullPath());
    assertEquals(TriggerEvent.AFTER_INSERT, trigger2Info.getEvent());
    assertEquals("org.apache.iotdb.db.engine.trigger.example.Counter", trigger2Info.getClassName());
    assertEquals("{}", trigger2Info.getAttributes().toString());
    assertTrue(trigger2Info.isStopped());

    TriggerRegistrationInformation trigger3Info =
        TriggerRegistrationService.getInstance().getRegistrationInformation("trigger-3");
    assertEquals("trigger-3", trigger3Info.getTriggerName());
    assertEquals("root.vehicle.d1.s2", trigger3Info.getFullPath().getFullPath());
    assertEquals(TriggerEvent.BEFORE_INSERT, trigger3Info.getEvent());
    assertEquals(
        "org.apache.iotdb.db.engine.trigger.example.Accumulator", trigger3Info.getClassName());
    assertEquals("{}", trigger3Info.getAttributes().toString());
    assertFalse(trigger3Info.isStopped());
  }
}
