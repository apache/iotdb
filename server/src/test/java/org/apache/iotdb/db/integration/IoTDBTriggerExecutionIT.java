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

public class IoTDBTriggerExecutionIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBTriggerExecutionIT.class);

  private volatile long count = 0;
  private volatile Exception exception = null;

  private final Thread dataGenerator =
      new Thread() {

        @Override
        public void run() {
          try (Connection connection =
                  DriverManager.getConnection(
                      Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
              Statement statement = connection.createStatement()) {

            do {
              ++count;
              statement.execute(
                  String.format(
                      "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4,s5,s6) values(%d,%d,%d,%d,%d,%s,'%d')",
                      count, count, count, count, count, count % 2 == 0 ? "true" : "false", count));
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
    if (!dataGenerator.isInterrupted()) {
      dataGenerator.interrupt();
    }
    dataGenerator.join();
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

  private void waitCountIncreaseBy(final long increment) throws InterruptedException {
    final long previous = count;
    while (count - previous < increment) {
      Thread.sleep(100);
    }
  }

  @Test
  public void checkFireTimes() throws InterruptedException {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create trigger trigger-1 before insert on root.vehicle.d1.s1 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute(
          "create trigger trigger-2 after insert on root.vehicle.d1.s2 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute(
          "create trigger trigger-3 before insert on root.vehicle.d1.s3 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute(
          "create trigger trigger-4 after insert on root.vehicle.d1.s4 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute(
          "create trigger trigger-5 before insert on root.vehicle.d1.s5 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute(
          "create trigger trigger-6 after insert on root.vehicle.d1.s6 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");

      int[] counters1 = getCounters(6);
      LOGGER.info(Arrays.toString(counters1));
      for (int i = 1; i < 6; ++i) {
        assertEquals(Counter.BASE, counters1[i]);
      }

      startDataGenerator();
      waitCountIncreaseBy(500);
      stopDataGenerator();

      // IOTDB-1825: if the background data generator's connection is closed, the following checks
      // will be meaningless, in which case we ignore the checks
      if (exception != null) {
        return;
      }
      int[] counters2 = getCounters(6);
      LOGGER.info(Arrays.toString(counters2));
      int expectedTimes = counters2[0] - counters1[0];
      for (int i = 1; i < 6; ++i) {
        assertEquals(expectedTimes, counters2[i] - counters1[i]);
      }
    } catch (SQLException | TriggerManagementException | InterruptedException e) {
      fail(e.getMessage());
    } finally {
      stopDataGenerator();
    }
  }

  @Test
  public void testCreateTriggersMultipleTimesWhileInserting() throws InterruptedException {
    startDataGenerator();

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      waitCountIncreaseBy(500);

      statement.execute(
          "create trigger trigger-1 before insert on root.vehicle.d1.s1 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute(
          "create trigger trigger-2 after insert on root.vehicle.d1.s2 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute(
          "create trigger trigger-3 before insert on root.vehicle.d1.s3 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");

      waitCountIncreaseBy(500);

      // IOTDB-1825: if the background data generator's connection is closed, the following checks
      // will be meaningless, in which case we ignore the checks
      if (exception != null) {
        return;
      }
      int[] counters1 = getCounters(3);
      for (int i = 0; i < 3; ++i) {
        assertTrue(Counter.BASE < counters1[i]);
      }

      waitCountIncreaseBy(500);

      statement.execute(
          "create trigger trigger-4 after insert on root.vehicle.d1.s4 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute(
          "create trigger trigger-5 before insert on root.vehicle.d1.s5 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute(
          "create trigger trigger-6 after insert on root.vehicle.d1.s6 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");

      // IOTDB-1825: if the background data generator's connection is closed, the following checks
      // will be meaningless, in which case we ignore the checks
      if (exception != null) {
        return;
      }
      int[] counters2 = getCounters(3);
      LOGGER.info(Arrays.toString(counters2));
      for (int i = 0; i < 3; ++i) {
        assertTrue(counters1[i] < counters2[i]);
      }
    } catch (SQLException | TriggerManagementException e) {
      fail(e.getMessage());
    } finally {
      stopDataGenerator();
    }
  }

  @Test
  public void testCreateAndDropTriggersMultipleTimesWhileInserting() throws InterruptedException {
    startDataGenerator();

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      waitCountIncreaseBy(500);

      statement.execute(
          "create trigger trigger-1 before insert on root.vehicle.d1.s1 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute(
          "create trigger trigger-2 after insert on root.vehicle.d1.s2 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute(
          "create trigger trigger-3 before insert on root.vehicle.d1.s3 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");

      waitCountIncreaseBy(500);

      // IOTDB-1825: if the background data generator's connection is closed, the following checks
      // will be meaningless, in which case we ignore the checks
      if (exception != null) {
        return;
      }
      int[] counters1 = getCounters(3);
      LOGGER.info(Arrays.toString(counters1));
      for (int i = 0; i < 3; ++i) {
        assertTrue(Counter.BASE < counters1[i]);
      }

      waitCountIncreaseBy(100);
      statement.execute("drop trigger trigger-1");
      statement.execute("drop trigger trigger-2");
      statement.execute("drop trigger trigger-3");
      waitCountIncreaseBy(100);
      statement.execute(
          "create trigger trigger-1 before insert on root.vehicle.d1.s1 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute(
          "create trigger trigger-2 after insert on root.vehicle.d1.s2 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute(
          "create trigger trigger-3 before insert on root.vehicle.d1.s3 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      waitCountIncreaseBy(100);
      statement.execute("drop trigger trigger-1");
      statement.execute("drop trigger trigger-2");
      statement.execute("drop trigger trigger-3");
      waitCountIncreaseBy(100);
      statement.execute(
          "create trigger trigger-1 before insert on root.vehicle.d1.s1 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute(
          "create trigger trigger-2 after insert on root.vehicle.d1.s2 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute(
          "create trigger trigger-3 before insert on root.vehicle.d1.s3 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");

      waitCountIncreaseBy(500);

      // IOTDB-1825: if the background data generator's connection is closed, the following checks
      // will be meaningless, in which case we ignore the checks
      if (exception != null) {
        return;
      }
      int[] counters2 = getCounters(3);
      LOGGER.info(Arrays.toString(counters2));
      for (int i = 0; i < 3; ++i) {
        assertTrue(Counter.BASE < counters1[i]);
      }
    } catch (SQLException | TriggerManagementException e) {
      fail(e.getMessage());
    } finally {
      stopDataGenerator();
    }
  }

  @Test
  public void testStopAndStartTriggersWhileInserting() throws InterruptedException {
    startDataGenerator();

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      waitCountIncreaseBy(500);

      statement.execute(
          "create trigger trigger-1 before insert on root.vehicle.d1.s1 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute(
          "create trigger trigger-2 after insert on root.vehicle.d1.s2 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute(
          "create trigger trigger-3 before insert on root.vehicle.d1.s3 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute(
          "create trigger trigger-4 after insert on root.vehicle.d1.s4 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute(
          "create trigger trigger-5 before insert on root.vehicle.d1.s5 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute(
          "create trigger trigger-6 after insert on root.vehicle.d1.s6 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");

      waitCountIncreaseBy(500);

      statement.execute("stop trigger trigger-1");
      statement.execute("stop trigger trigger-2");
      statement.execute("stop trigger trigger-3");

      int[] counters1 = getCounters(6);
      LOGGER.info(Arrays.toString(counters1));

      waitCountIncreaseBy(500);

      statement.execute("stop trigger trigger-4");
      statement.execute("stop trigger trigger-5");
      statement.execute("stop trigger trigger-6");

      int[] counters2 = getCounters(6);
      LOGGER.info(Arrays.toString(counters2));

      // IOTDB-1825: if the background data generator's connection is closed, the following checks
      // will be meaningless, in which case we ignore the checks
      if (exception != null) {
        return;
      }
      for (int i = 0; i < 3; ++i) {
        assertEquals(counters1[i], counters2[i]);
      }
      for (int i = 3; i < 6; ++i) {
        assertTrue(counters1[i] < counters2[i]);
      }

      statement.execute("start trigger trigger-1");
      statement.execute("start trigger trigger-2");
      statement.execute("start trigger trigger-3");

      waitCountIncreaseBy(500);

      // IOTDB-1825: if the background data generator's connection is closed, the following checks
      // will be meaningless, in which case we ignore the checks
      if (exception != null) {
        return;
      }
      int[] counters3 = getCounters(6);
      LOGGER.info(Arrays.toString(counters3));
      for (int i = 0; i < 3; ++i) {
        assertTrue(counters2[i] < counters3[i]);
      }
      for (int i = 3; i < 6; ++i) {
        assertEquals(counters2[i], counters3[i]);
      }

      statement.execute("start trigger trigger-4");
      statement.execute("start trigger trigger-5");
      statement.execute("start trigger trigger-6");

      waitCountIncreaseBy(500);

      // IOTDB-1825: if the background data generator's connection is closed, the following checks
      // will be meaningless, in which case we ignore the checks
      if (exception != null) {
        return;
      }
      int[] counters4 = getCounters(6);
      LOGGER.info(Arrays.toString(counters4));
      for (int i = 0; i < 6; ++i) {
        assertTrue(counters3[i] < counters4[i]);
      }

      statement.execute("stop trigger trigger-1");
      statement.execute("stop trigger trigger-2");
      statement.execute("stop trigger trigger-3");
      statement.execute("stop trigger trigger-4");
      statement.execute("stop trigger trigger-5");
      statement.execute("stop trigger trigger-6");

      waitCountIncreaseBy(500);
      int[] counters5 = getCounters(6);
      waitCountIncreaseBy(500);
      int[] counters6 = getCounters(6);
      // IOTDB-1825: if the background data generator's connection is closed, the following checks
      // will be meaningless, in which case we ignore the checks
      if (exception != null) {
        return;
      }
      assertEquals(Arrays.toString(counters5), Arrays.toString(counters6));
    } catch (SQLException | TriggerManagementException e) {
      fail(e.getMessage());
    } finally {
      stopDataGenerator();
    }
  }

  private static int[] getCounters(int limit) throws TriggerManagementException {
    int[] counters = new int[limit];
    for (int i = 1; i <= limit; ++i) {
      counters[i - 1] =
          ((Counter) TriggerRegistrationService.getInstance().getTriggerInstance("trigger-" + i))
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
      waitCountIncreaseBy(500);

      statement.execute(
          "create trigger trigger-1 before insert on root.vehicle.d1.s1 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute(
          "create trigger trigger-2 after insert on root.vehicle.d1.s2 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute(
          "create trigger trigger-3 before insert on root.vehicle.d1.s3 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute(
          "create trigger trigger-4 after insert on root.vehicle.d1.s4 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute(
          "create trigger trigger-5 before insert on root.vehicle.d1.s5 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute(
          "create trigger trigger-6 after insert on root.vehicle.d1.s6 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");

      waitCountIncreaseBy(500);

      // IOTDB-1825: if the background data generator's connection is closed, the following checks
      // will be meaningless, in which case we ignore the checks
      if (exception != null) {
        return;
      }
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
          TriggerRegistrationService.getInstance().getTriggerInstance("trigger-" + i);
          fail();
        } catch (TriggerManagementException e) {
          assertTrue(e.getMessage().contains("does not exist"));
        }
      }

      createTimeseries();

      for (int i = 0; i < 6; ++i) {
        try {
          TriggerRegistrationService.getInstance().getTriggerInstance("trigger-" + i);
          fail();
        } catch (TriggerManagementException e) {
          assertTrue(e.getMessage().contains("does not exist"));
        }
      }

      statement.execute(
          "create trigger trigger-1 before insert on root.vehicle.d1.s1 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute(
          "create trigger trigger-2 after insert on root.vehicle.d1.s2 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute(
          "create trigger trigger-3 before insert on root.vehicle.d1.s3 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute(
          "create trigger trigger-4 after insert on root.vehicle.d1.s4 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute(
          "create trigger trigger-5 before insert on root.vehicle.d1.s5 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute(
          "create trigger trigger-6 after insert on root.vehicle.d1.s6 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");

      Thread.sleep(500);

      int[] counters2 = getCounters(6);
      LOGGER.info(Arrays.toString(counters2));
      for (int i = 0; i < 6; ++i) {
        assertEquals(Counter.BASE, counters2[2]);
      }
    } catch (SQLException | TriggerManagementException | MetadataException e) {
      fail(e.getMessage());
    } finally {
      stopDataGenerator();
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
          "create trigger trigger-1 before insert on root.vehicle.d1.s1 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute(
          "create trigger trigger-2 after insert on root.vehicle.d1.s2 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute(
          "create trigger trigger-3 before insert on root.vehicle.d1.s3 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute(
          "create trigger trigger-4 after insert on root.vehicle.d1.s4 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute(
          "create trigger trigger-5 before insert on root.vehicle.d1.s5 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute(
          "create trigger trigger-6 after insert on root.vehicle.d1.s6 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");

      waitCountIncreaseBy(500);

      stopDataGenerator();

      IoTDB.metaManager.deleteStorageGroups(
          Collections.singletonList(new PartialPath("root.vehicle")));

      for (int i = 0; i < 6; ++i) {
        try {
          TriggerRegistrationService.getInstance().getTriggerInstance("trigger-" + i);
          fail();
        } catch (TriggerManagementException e) {
          assertTrue(e.getMessage().contains("does not exist"));
        }
      }

      createTimeseries();

      for (int i = 0; i < 6; ++i) {
        try {
          TriggerRegistrationService.getInstance().getTriggerInstance("trigger-" + i);
          fail();
        } catch (TriggerManagementException e) {
          assertTrue(e.getMessage().contains("does not exist"));
        }
      }

      statement.execute(
          "create trigger trigger-1 before insert on root.vehicle.d1.s1 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute(
          "create trigger trigger-2 after insert on root.vehicle.d1.s2 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute(
          "create trigger trigger-3 before insert on root.vehicle.d1.s3 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute(
          "create trigger trigger-4 after insert on root.vehicle.d1.s4 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute(
          "create trigger trigger-5 before insert on root.vehicle.d1.s5 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
      statement.execute(
          "create trigger trigger-6 after insert on root.vehicle.d1.s6 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");

      Thread.sleep(500);

      int[] counters1 = getCounters(6);
      LOGGER.info(Arrays.toString(counters1));
      for (int i = 0; i < 6; ++i) {
        assertEquals(Counter.BASE, counters1[2]);
      }
    } catch (SQLException | TriggerManagementException | MetadataException e) {
      fail(e.getMessage());
    } finally {
      stopDataGenerator();
    }
  }
}
