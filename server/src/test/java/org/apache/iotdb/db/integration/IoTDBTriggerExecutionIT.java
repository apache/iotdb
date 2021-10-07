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

import org.apache.iotdb.db.engine.trigger.example.Counter;
import org.apache.iotdb.db.engine.trigger.service.TriggerRegistrationService;
import org.apache.iotdb.db.exception.TriggerManagementException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings("squid:S2925") // enable to use Thread.sleep(long) without warnings
public class IoTDBTriggerExecutionIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBTriggerExecutionIT.class);

  private volatile Exception exception = null;

  private final Thread dataGenerator =
      new Thread() {

        @Override
        public void run() {
          try (Connection connection =
                  DriverManager.getConnection(
                      Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
              Statement statement = connection.createStatement()) {

            long count = 0;
            do {
              ++count;
              boolean isSuccessful = false;
              while (!isSuccessful) {
                try {
                  statement.execute(
                      String.format(
                          "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4,s5,s6) values(%d,%d,%d,%d,%d,%s,\'%d\')",
                          count,
                          count,
                          count,
                          count,
                          count,
                          count % 2 == 0 ? "true" : "false",
                          count));
                  isSuccessful = true;
                } catch (SQLException throwable) {
                  fail(throwable.getMessage());
                  LOGGER.error(throwable.getMessage());
                }
              }
            } while (!isInterrupted());
          } catch (Exception e) {
            exception = e;
          }
        }
      };

  private void startDataGenerator() {
    dataGenerator.start();
  }

  private void stopDataGenerator() throws InterruptedException {
    dataGenerator.interrupt();
    dataGenerator.join();
    if (exception != null) {
      fail(exception.getMessage());
    }
  }

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    createTimeseries();
    Class.forName(Config.JDBC_DRIVER_NAME);
  }

  private void createTimeseries() throws MetadataException {
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d1.s1"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d1.s2"),
        TSDataType.INT64,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d1.s3"),
        TSDataType.FLOAT,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d1.s4"),
        TSDataType.DOUBLE,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d1.s5"),
        TSDataType.BOOLEAN,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d1.s6"),
        TSDataType.TEXT,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void checkFireTimes() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create trigger trigger_1 before insert on root.vehicle.d1.s1 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");
      statement.execute(
          "create trigger trigger_2 after insert on root.vehicle.d1.s2 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");
      statement.execute(
          "create trigger trigger_3 before insert on root.vehicle.d1.s3 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");
      statement.execute(
          "create trigger trigger_4 after insert on root.vehicle.d1.s4 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");
      statement.execute(
          "create trigger trigger_5 before insert on root.vehicle.d1.s5 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");
      statement.execute(
          "create trigger trigger_6 after insert on root.vehicle.d1.s6 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");

      int[] counters1 = getCounters(6);
      LOGGER.info(Arrays.toString(counters1));
      for (int i = 1; i < 6; ++i) {
        assertEquals(Counter.BASE, counters1[i]);
      }

      startDataGenerator();
      Thread.sleep(500);
      stopDataGenerator();

      int[] counters2 = getCounters(6);
      LOGGER.info(Arrays.toString(counters2));
      int expectedTimes = counters2[0] - counters1[0];
      for (int i = 1; i < 6; ++i) {
        assertEquals(expectedTimes, counters2[i] - counters1[i]);
      }
    } catch (SQLException | TriggerManagementException | InterruptedException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testCreateTriggersMultipleTimesWhileInserting() throws InterruptedException {
    startDataGenerator();

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      Thread.sleep(500);

      statement.execute(
          "create trigger trigger_1 before insert on root.vehicle.d1.s1 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");
      statement.execute(
          "create trigger trigger_2 after insert on root.vehicle.d1.s2 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");
      statement.execute(
          "create trigger trigger_3 before insert on root.vehicle.d1.s3 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");

      Thread.sleep(500);

      int[] counters1 = getCounters(3);
      LOGGER.info(Arrays.toString(counters1));
      for (int i = 0; i < 3; ++i) {
        assertTrue(Counter.BASE < counters1[i]);
      }

      Thread.sleep(500);

      statement.execute(
          "create trigger trigger_4 after insert on root.vehicle.d1.s4 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");
      statement.execute(
          "create trigger trigger_5 before insert on root.vehicle.d1.s5 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");
      statement.execute(
          "create trigger trigger_6 after insert on root.vehicle.d1.s6 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");

      int[] counters2 = getCounters(3);
      LOGGER.info(Arrays.toString(counters2));
      for (int i = 0; i < 3; ++i) {
        assertTrue(counters1[i] < counters2[i]);
      }
    } catch (SQLException | TriggerManagementException e) {
      fail(e.getMessage());
    }

    stopDataGenerator();
  }

  @Test
  public void testCreateAndDropTriggersMultipleTimesWhileInserting() throws InterruptedException {
    startDataGenerator();

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      Thread.sleep(500);

      statement.execute(
          "create trigger trigger_1 before insert on root.vehicle.d1.s1 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");
      statement.execute(
          "create trigger trigger_2 after insert on root.vehicle.d1.s2 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");
      statement.execute(
          "create trigger trigger_3 before insert on root.vehicle.d1.s3 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");

      Thread.sleep(500);

      int[] counters1 = getCounters(3);
      LOGGER.info(Arrays.toString(counters1));
      for (int i = 0; i < 3; ++i) {
        assertTrue(Counter.BASE < counters1[i]);
      }

      Thread.sleep(100);
      statement.execute("drop trigger trigger_1");
      statement.execute("drop trigger trigger_2");
      statement.execute("drop trigger trigger_3");
      Thread.sleep(100);
      statement.execute(
          "create trigger trigger_1 before insert on root.vehicle.d1.s1 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");
      statement.execute(
          "create trigger trigger_2 after insert on root.vehicle.d1.s2 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");
      statement.execute(
          "create trigger trigger_3 before insert on root.vehicle.d1.s3 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");
      Thread.sleep(100);
      statement.execute("drop trigger trigger_1");
      statement.execute("drop trigger trigger_2");
      statement.execute("drop trigger trigger_3");
      Thread.sleep(100);
      statement.execute(
          "create trigger trigger_1 before insert on root.vehicle.d1.s1 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");
      statement.execute(
          "create trigger trigger_2 after insert on root.vehicle.d1.s2 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");
      statement.execute(
          "create trigger trigger_3 before insert on root.vehicle.d1.s3 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");

      Thread.sleep(500);

      int[] counters2 = getCounters(3);
      LOGGER.info(Arrays.toString(counters2));
      for (int i = 0; i < 3; ++i) {
        assertTrue(Counter.BASE < counters1[i]);
      }
    } catch (SQLException | TriggerManagementException e) {
      fail(e.getMessage());
    }

    stopDataGenerator();
  }

  @Test
  public void testStopAndStartTriggersWhileInserting() throws InterruptedException {
    startDataGenerator();

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      Thread.sleep(500);

      statement.execute(
          "create trigger trigger_1 before insert on root.vehicle.d1.s1 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");
      statement.execute(
          "create trigger trigger_2 after insert on root.vehicle.d1.s2 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");
      statement.execute(
          "create trigger trigger_3 before insert on root.vehicle.d1.s3 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");
      statement.execute(
          "create trigger trigger_4 after insert on root.vehicle.d1.s4 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");
      statement.execute(
          "create trigger trigger_5 before insert on root.vehicle.d1.s5 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");
      statement.execute(
          "create trigger trigger_6 after insert on root.vehicle.d1.s6 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");

      Thread.sleep(500);

      statement.execute("stop trigger trigger_1");
      statement.execute("stop trigger trigger_2");
      statement.execute("stop trigger trigger_3");

      int[] counters1 = getCounters(6);
      LOGGER.info(Arrays.toString(counters1));

      Thread.sleep(500);

      statement.execute("stop trigger trigger_4");
      statement.execute("stop trigger trigger_5");
      statement.execute("stop trigger trigger_6");

      int[] counters2 = getCounters(6);
      LOGGER.info(Arrays.toString(counters2));
      for (int i = 0; i < 3; ++i) {
        assertEquals(counters1[i], counters2[i]);
      }
      for (int i = 3; i < 6; ++i) {
        assertTrue(counters1[i] < counters2[i]);
      }

      statement.execute("start trigger trigger_1");
      statement.execute("start trigger trigger_2");
      statement.execute("start trigger trigger_3");

      Thread.sleep(500);

      int[] counters3 = getCounters(6);
      LOGGER.info(Arrays.toString(counters3));
      for (int i = 0; i < 3; ++i) {
        assertTrue(counters2[i] < counters3[i]);
      }
      for (int i = 3; i < 6; ++i) {
        assertEquals(counters2[i], counters3[i]);
      }

      statement.execute("start trigger trigger_4");
      statement.execute("start trigger trigger_5");
      statement.execute("start trigger trigger_6");

      Thread.sleep(500);

      int[] counters4 = getCounters(6);
      LOGGER.info(Arrays.toString(counters4));
      for (int i = 0; i < 6; ++i) {
        assertTrue(counters3[i] < counters4[i]);
      }

      statement.execute("stop trigger trigger_1");
      statement.execute("stop trigger trigger_2");
      statement.execute("stop trigger trigger_3");
      statement.execute("stop trigger trigger_4");
      statement.execute("stop trigger trigger_5");
      statement.execute("stop trigger trigger_6");

      Thread.sleep(500);
      int[] counters5 = getCounters(6);
      Thread.sleep(500);
      int[] counters6 = getCounters(6);
      assertEquals(Arrays.toString(counters5), Arrays.toString(counters6));
    } catch (SQLException | TriggerManagementException e) {
      fail(e.getMessage());
    }

    stopDataGenerator();
  }

  private static int[] getCounters(int limit) throws TriggerManagementException {
    int[] counters = new int[limit];
    for (int i = 1; i <= limit; ++i) {
      counters[i - 1] =
          ((Counter) TriggerRegistrationService.getInstance().getTriggerInstance("trigger_" + i))
              .getCounter();
    }
    return counters;
  }

  @Test
  public void testInsertAndRemoveTimeseriesWithTriggers() throws InterruptedException {
    startDataGenerator();

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      Thread.sleep(500);

      statement.execute(
          "create trigger trigger_1 before insert on root.vehicle.d1.s1 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");
      statement.execute(
          "create trigger trigger_2 after insert on root.vehicle.d1.s2 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");
      statement.execute(
          "create trigger trigger_3 before insert on root.vehicle.d1.s3 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");
      statement.execute(
          "create trigger trigger_4 after insert on root.vehicle.d1.s4 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");
      statement.execute(
          "create trigger trigger_5 before insert on root.vehicle.d1.s5 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");
      statement.execute(
          "create trigger trigger_6 after insert on root.vehicle.d1.s6 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");

      Thread.sleep(500);

      int[] counters1 = getCounters(6);
      LOGGER.info(Arrays.toString(counters1));
      for (int i = 0; i < 6; ++i) {
        assertTrue(Counter.BASE < counters1[i]);
      }

      stopDataGenerator();

      IoTDB.metaManager.deleteTimeseries(new PartialPath("root.vehicle.d1.s1"));
      IoTDB.metaManager.deleteTimeseries(new PartialPath("root.vehicle.d1.s2"));
      IoTDB.metaManager.deleteTimeseries(new PartialPath("root.vehicle.d1.s3"));
      IoTDB.metaManager.deleteTimeseries(new PartialPath("root.vehicle.d1.s4"));
      IoTDB.metaManager.deleteTimeseries(new PartialPath("root.vehicle.d1.s5"));
      IoTDB.metaManager.deleteTimeseries(new PartialPath("root.vehicle.d1.s6"));

      for (int i = 0; i < 6; ++i) {
        try {
          TriggerRegistrationService.getInstance().getTriggerInstance("trigger_" + i);
          fail();
        } catch (TriggerManagementException e) {
          assertTrue(e.getMessage().contains("does not exist"));
        }
      }

      createTimeseries();

      for (int i = 0; i < 6; ++i) {
        try {
          TriggerRegistrationService.getInstance().getTriggerInstance("trigger_" + i);
          fail();
        } catch (TriggerManagementException e) {
          assertTrue(e.getMessage().contains("does not exist"));
        }
      }

      statement.execute(
          "create trigger trigger_1 before insert on root.vehicle.d1.s1 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");
      statement.execute(
          "create trigger trigger_2 after insert on root.vehicle.d1.s2 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");
      statement.execute(
          "create trigger trigger_3 before insert on root.vehicle.d1.s3 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");
      statement.execute(
          "create trigger trigger_4 after insert on root.vehicle.d1.s4 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");
      statement.execute(
          "create trigger trigger_5 before insert on root.vehicle.d1.s5 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");
      statement.execute(
          "create trigger trigger_6 after insert on root.vehicle.d1.s6 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");

      Thread.sleep(500);

      int[] counters2 = getCounters(6);
      LOGGER.info(Arrays.toString(counters2));
      for (int i = 0; i < 6; ++i) {
        assertEquals(Counter.BASE, counters2[2]);
      }
    } catch (SQLException | TriggerManagementException | MetadataException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testInsertAndRemoveStorageGroupWithTriggers() throws InterruptedException {
    startDataGenerator();

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create trigger trigger_1 before insert on root.vehicle.d1.s1 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");
      statement.execute(
          "create trigger trigger_2 after insert on root.vehicle.d1.s2 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");
      statement.execute(
          "create trigger trigger_3 before insert on root.vehicle.d1.s3 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");
      statement.execute(
          "create trigger trigger_4 after insert on root.vehicle.d1.s4 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");
      statement.execute(
          "create trigger trigger_5 before insert on root.vehicle.d1.s5 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");
      statement.execute(
          "create trigger trigger_6 after insert on root.vehicle.d1.s6 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");

      Thread.sleep(500);

      stopDataGenerator();

      IoTDB.metaManager.deleteStorageGroups(
          Collections.singletonList(new PartialPath("root.vehicle")));

      for (int i = 0; i < 6; ++i) {
        try {
          TriggerRegistrationService.getInstance().getTriggerInstance("trigger_" + i);
          fail();
        } catch (TriggerManagementException e) {
          assertTrue(e.getMessage().contains("does not exist"));
        }
      }

      createTimeseries();

      for (int i = 0; i < 6; ++i) {
        try {
          TriggerRegistrationService.getInstance().getTriggerInstance("trigger_" + i);
          fail();
        } catch (TriggerManagementException e) {
          assertTrue(e.getMessage().contains("does not exist"));
        }
      }

      statement.execute(
          "create trigger trigger_1 before insert on root.vehicle.d1.s1 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");
      statement.execute(
          "create trigger trigger_2 after insert on root.vehicle.d1.s2 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");
      statement.execute(
          "create trigger trigger_3 before insert on root.vehicle.d1.s3 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");
      statement.execute(
          "create trigger trigger_4 after insert on root.vehicle.d1.s4 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");
      statement.execute(
          "create trigger trigger_5 before insert on root.vehicle.d1.s5 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");
      statement.execute(
          "create trigger trigger_6 after insert on root.vehicle.d1.s6 as \'org.apache.iotdb.db.engine.trigger.example.Counter\'");

      Thread.sleep(500);

      int[] counters1 = getCounters(6);
      LOGGER.info(Arrays.toString(counters1));
      for (int i = 0; i < 6; ++i) {
        assertEquals(Counter.BASE, counters1[2]);
      }
    } catch (SQLException | TriggerManagementException | MetadataException e) {
      fail(e.getMessage());
    }
  }
}
