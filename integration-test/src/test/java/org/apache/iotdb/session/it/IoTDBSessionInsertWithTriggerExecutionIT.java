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

package org.apache.iotdb.session.it;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

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
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBSessionInsertWithTriggerExecutionIT {
  private static final String TRIGGER_COUNTER_PREFIX =
      System.getProperty("user.dir")
          + File.separator
          + "target"
          + File.separator
          + "test-classes"
          + File.separator;

  private static final String TRIGGER_JAR_PREFIX =
      new File(TRIGGER_COUNTER_PREFIX).toURI().toString();

  // row num of tablet
  private final int rows = 10;

  private static final String STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX =
      "statelessTriggerBeforeInsertionSession_";

  private static final String STATELESS_TRIGGER_AFTER_INSERTION_PREFIX =
      "statelessTriggerAfterInsertionSession_";

  private static final String STATEFUL_TRIGGER_BEFORE_INSERTION_PREFIX =
      "statefulTriggerBeforeInsertionSession_";

  private static final String STATEFUL_TRIGGER_AFTER_INSERTION_PREFIX =
      "statefulTriggerAfterInsertionSession_";

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
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.createDatabase("root.test");
      session.createTimeseries(
          "root.test.stateless.a", TSDataType.INT32, TSEncoding.PLAIN, CompressionType.SNAPPY);
      session.createTimeseries(
          "root.test.stateless.b", TSDataType.INT32, TSEncoding.PLAIN, CompressionType.SNAPPY);
      session.createTimeseries(
          "root.test.stateless.c", TSDataType.INT32, TSEncoding.PLAIN, CompressionType.SNAPPY);
      session.createTimeseries(
          "root.test.stateful.a", TSDataType.INT32, TSEncoding.PLAIN, CompressionType.SNAPPY);
      session.createTimeseries(
          "root.test.stateful.b", TSDataType.INT32, TSEncoding.PLAIN, CompressionType.SNAPPY);
      session.createTimeseries(
          "root.test.stateful.c", TSDataType.INT32, TSEncoding.PLAIN, CompressionType.SNAPPY);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private static void createTriggers() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      // create stateless triggers before insertion
      session.executeNonQueryStatement(
          String.format(
              "create stateless trigger %s before insert on root.test.stateless.* as 'org.apache.iotdb.db.trigger.example.TriggerFireTimesCounter' using URI '%s' with (\"name\"=\"%s\")",
              STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX + "all",
              TRIGGER_JAR_PREFIX + "TriggerFireTimesCounter.jar",
              STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX + "all"));
      session.executeNonQueryStatement(
          String.format(
              "create stateless trigger %s before insert on root.test.stateless.a as 'org.apache.iotdb.db.trigger.example.TriggerFireTimesCounter' using URI '%s' with (\"name\"=\"%s\")",
              STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX + "a",
              TRIGGER_JAR_PREFIX + "TriggerFireTimesCounter.jar",
              STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX + "a"));

      // create stateless triggers after insertion
      session.executeNonQueryStatement(
          String.format(
              "create stateless trigger %s after insert on root.test.stateless.* as 'org.apache.iotdb.db.trigger.example.TriggerFireTimesCounter' using URI '%s' with (\"name\"=\"%s\")",
              STATELESS_TRIGGER_AFTER_INSERTION_PREFIX + "all",
              TRIGGER_JAR_PREFIX + "TriggerFireTimesCounter.jar",
              STATELESS_TRIGGER_AFTER_INSERTION_PREFIX + "all"));
      session.executeNonQueryStatement(
          String.format(
              "create stateless trigger %s after insert on root.test.stateless.a as 'org.apache.iotdb.db.trigger.example.TriggerFireTimesCounter' using URI '%s' with (\"name\"=\"%s\")",
              STATELESS_TRIGGER_AFTER_INSERTION_PREFIX + "a",
              TRIGGER_JAR_PREFIX + "TriggerFireTimesCounter.jar",
              STATELESS_TRIGGER_AFTER_INSERTION_PREFIX + "a"));

      // create stateful triggers before insertion
      session.executeNonQueryStatement(
          String.format(
              "create stateful trigger %s before insert on root.test.stateful.* as 'org.apache.iotdb.db.trigger.example.TriggerFireTimesCounter' using URI '%s' with (\"name\"=\"%s\")",
              STATEFUL_TRIGGER_BEFORE_INSERTION_PREFIX + "all",
              TRIGGER_JAR_PREFIX + "TriggerFireTimesCounter.jar",
              STATEFUL_TRIGGER_BEFORE_INSERTION_PREFIX + "all"));
      session.executeNonQueryStatement(
          String.format(
              "create stateful trigger %s before insert on root.test.stateful.a as 'org.apache.iotdb.db.trigger.example.TriggerFireTimesCounter' using URI '%s' with (\"name\"=\"%s\")",
              STATEFUL_TRIGGER_BEFORE_INSERTION_PREFIX + "a",
              TRIGGER_JAR_PREFIX + "TriggerFireTimesCounter.jar",
              STATEFUL_TRIGGER_BEFORE_INSERTION_PREFIX + "a"));

      // create stateful triggers after insertion
      session.executeNonQueryStatement(
          String.format(
              "create stateful trigger %s after insert on root.test.stateful.* as 'org.apache.iotdb.db.trigger.example.TriggerFireTimesCounter' using URI '%s' with (\"name\"=\"%s\")",
              STATEFUL_TRIGGER_AFTER_INSERTION_PREFIX + "all",
              TRIGGER_JAR_PREFIX + "TriggerFireTimesCounter.jar",
              STATEFUL_TRIGGER_AFTER_INSERTION_PREFIX + "all"));
      session.executeNonQueryStatement(
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
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.executeNonQueryStatement(
          String.format("drop trigger %s", STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX + "all"));
      session.executeNonQueryStatement(
          String.format("drop trigger %s", STATELESS_TRIGGER_AFTER_INSERTION_PREFIX + "all"));
      session.executeNonQueryStatement(
          String.format("drop trigger %s", STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX + "a"));
      session.executeNonQueryStatement(
          String.format("drop trigger %s", STATELESS_TRIGGER_AFTER_INSERTION_PREFIX + "a"));
      session.executeNonQueryStatement(
          String.format("drop trigger %s", STATEFUL_TRIGGER_BEFORE_INSERTION_PREFIX + "all"));
      session.executeNonQueryStatement(
          String.format("drop trigger %s", STATEFUL_TRIGGER_AFTER_INSERTION_PREFIX + "all"));
      session.executeNonQueryStatement(
          String.format("drop trigger %s", STATEFUL_TRIGGER_BEFORE_INSERTION_PREFIX + "a"));
      session.executeNonQueryStatement(
          String.format("drop trigger %s", STATEFUL_TRIGGER_AFTER_INSERTION_PREFIX + "a"));
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testFireTimesOfStatelessTrigger() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      insertTablet(
          session,
          "root.test.stateless",
          new ArrayList<String>() {
            {
              add("a");
              add("b");
              add("c");
            }
          });
      insertTablet(
          session,
          "root.test.stateless",
          new ArrayList<String>() {
            {
              add("a");
            }
          });
      insertTablet(
          session,
          "root.test.stateless",
          new ArrayList<String>() {
            {
              add("b");
            }
          });
      insertTablet(
          session,
          "root.test.stateless",
          new ArrayList<String>() {
            {
              add("c");
            }
          });

      Assert.assertEquals(4 * rows, getCounter(STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX + "all"));
      Assert.assertEquals(4 * rows, getCounter(STATELESS_TRIGGER_AFTER_INSERTION_PREFIX + "all"));

      Assert.assertEquals(2 * rows, getCounter(STATELESS_TRIGGER_BEFORE_INSERTION_PREFIX + "a"));
      Assert.assertEquals(2 * rows, getCounter(STATELESS_TRIGGER_AFTER_INSERTION_PREFIX + "a"));
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testFireTimesOfStatefulTrigger() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      insertTablet(
          session,
          "root.test.stateful",
          new ArrayList<String>() {
            {
              add("a");
              add("b");
              add("c");
            }
          });
      insertTablet(
          session,
          "root.test.stateful",
          new ArrayList<String>() {
            {
              add("a");
            }
          });
      insertTablet(
          session,
          "root.test.stateful",
          new ArrayList<String>() {
            {
              add("b");
            }
          });
      insertTablet(
          session,
          "root.test.stateful",
          new ArrayList<String>() {
            {
              add("c");
            }
          });

      Assert.assertEquals(4 * rows, getCounter(STATEFUL_TRIGGER_BEFORE_INSERTION_PREFIX + "all"));
      Assert.assertEquals(4 * rows, getCounter(STATEFUL_TRIGGER_AFTER_INSERTION_PREFIX + "all"));

      Assert.assertEquals(2 * rows, getCounter(STATEFUL_TRIGGER_BEFORE_INSERTION_PREFIX + "a"));
      Assert.assertEquals(2 * rows, getCounter(STATEFUL_TRIGGER_AFTER_INSERTION_PREFIX + "a"));
    } catch (Exception e) {
      System.out.println(e);
      fail(e.getMessage());
    }
  }

  private void insertTablet(ISession session, String device, List<String> measurementList)
      throws IoTDBConnectionException, StatementExecutionException {
    List<MeasurementSchema> schemaList = new ArrayList<>();
    measurementList.forEach(
        measurement -> schemaList.add(new MeasurementSchema(measurement, TSDataType.INT32)));

    Tablet tablet = new Tablet(device, schemaList, 10);

    long timestamp = 1;
    for (int i = 0; i < rows; i++) {
      int rowIndex = tablet.rowSize++;
      tablet.addTimestamp(rowIndex, timestamp);
      measurementList.forEach(measurement -> tablet.addValue(measurement, rowIndex, 1));
      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        session.insertTablet(tablet, true);
        tablet.reset();
      }
      timestamp++;
    }

    if (tablet.rowSize != 0) {
      session.insertTablet(tablet);
      tablet.reset();
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
