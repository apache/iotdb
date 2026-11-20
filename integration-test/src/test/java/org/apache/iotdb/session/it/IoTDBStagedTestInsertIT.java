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
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.TestInsertStage;
import org.apache.iotdb.session.TableSessionBuilder;
import org.apache.iotdb.session.pool.TableSessionPoolBuilder;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBStagedTestInsertIT {
  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testStagesAndPayloadsLeaveDataUnchanged() throws Exception {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.executeNonQueryStatement(
          "CREATE TIMESERIES root.staged.d.`a.b` WITH DATATYPE=DOUBLE, ENCODING=PLAIN");
      session.executeNonQueryStatement("INSERT INTO root.staged.d(time, `a.b`) VALUES(1, 1)");
      for (TestInsertStage stage : TestInsertStage.values()) {
        session.executeNonQueryStatement("CLEAR CACHE");
        session.testInsertRecord(
            "root.staged.d",
            2,
            Collections.singletonList("`a.b`"),
            Collections.singletonList(TSDataType.DOUBLE),
            Collections.singletonList(2.0),
            stage);
        session.testInsertRecord(
            "root.staged.d",
            2,
            Collections.singletonList("`a.b`"),
            Collections.singletonList("2"),
            stage);
        session.testInsertRecords(
            Collections.singletonList("root.staged.d"),
            Collections.singletonList(2L),
            Collections.singletonList(Collections.singletonList("`a.b`")),
            Collections.singletonList(Collections.singletonList(TSDataType.DOUBLE)),
            Collections.singletonList(Collections.singletonList(2.0)),
            stage);
        session.testInsertRecords(
            Collections.singletonList("root.staged.d"),
            Collections.singletonList(2L),
            Collections.singletonList(Collections.singletonList("`a.b`")),
            Collections.singletonList(Collections.singletonList("2")),
            stage);
        Tablet tablet =
            new Tablet(
                "root.staged.d",
                Collections.singletonList(new MeasurementSchema("`a.b`", TSDataType.DOUBLE)));
        tablet.addTimestamp(0, 2);
        tablet.addValue("`a.b`", 0, 2.0);
        session.testInsertTablet(tablet, stage);
        session.testInsertTablets(Collections.singletonMap("root.staged.d", tablet), stage);
      }
      try (SessionDataSet data =
          session.executeQueryStatement("SELECT count(`a.b`) FROM root.staged.d")) {
        assertEquals(1, data.next().getFields().get(0).getLongV());
      }
      session.executeNonQueryStatement(
          "CREATE TIMESERIES root.staged_other.d.s WITH DATATYPE=INT64, ENCODING=PLAIN");
      session.executeNonQueryStatement("INSERT INTO root.staged_other.d(time,s) VALUES(1,1)");
      // Each ingress node tests the same multi-region request, exercising local and remote
      // reception.
      for (DataNodeWrapper node : EnvFactory.getEnv().getDataNodeWrapperList()) {
        try (ISession ingress =
            EnvFactory.getEnv()
                .getSessionConnection(Collections.singletonList(node.getIpAndPortString()))) {
          ingress.testInsertRecords(
              Arrays.asList("root.staged.d", "root.staged_other.d"),
              Arrays.asList(2L, 2L),
              Arrays.asList(Collections.singletonList("`a.b`"), Collections.singletonList("s")),
              Arrays.asList(Collections.singletonList("2"), Collections.singletonList("2")),
              TestInsertStage.UNTIL_WRITE_TARGET_RECEIVED);
        }
      }
      try (SessionDataSet data =
          session.executeQueryStatement("SELECT count(s) FROM root.staged_other.d")) {
        assertEquals(1, data.next().getFields().get(0).getLongV());
      }
      try (SessionDataSet data =
          session.executeQueryStatement("SELECT count(`a.b`) FROM root.staged.d")) {
        assertEquals(1, data.next().getFields().get(0).getLongV());
      }
      // A normal insertion after the tests proves schema locks/resources were released.
      session.executeNonQueryStatement("INSERT INTO root.staged.d(time, `a.b`) VALUES(2, 2)");
      try (SessionDataSet data =
          session.executeQueryStatement("SELECT count(`a.b`) FROM root.staged.d")) {
        assertEquals(2, data.next().getFields().get(0).getLongV());
      }
    }
  }

  @Test
  public void testMissingMetadataAndPartitionAreNotCreated() throws Exception {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.testInsertRecord(
          "root.staged_missing.d",
          1,
          Collections.singletonList("s"),
          Collections.singletonList("1"));
      session.testInsertRecord(
          "root.staged_missing.d",
          1,
          Collections.singletonList("s"),
          Collections.singletonList("1"),
          TestInsertStage.UNTIL_DECODED);
      assertThrows(
          StatementExecutionException.class,
          () ->
              session.testInsertRecord(
                  "root.staged_missing.d",
                  1,
                  Collections.singletonList("s"),
                  Collections.singletonList("1"),
                  TestInsertStage.UNTIL_ANALYZED));
      try (SessionDataSet data =
          session.executeQueryStatement("SHOW DATABASES root.staged_missing")) {
        assertFalse(data.hasNext());
      }
      session.executeNonQueryStatement(
          "CREATE TIMESERIES root.staged_empty.d.s WITH DATATYPE=INT64, ENCODING=PLAIN");
      assertThrows(
          StatementExecutionException.class,
          () ->
              session.testInsertRecord(
                  "root.staged_empty.d",
                  1,
                  Collections.singletonList("s"),
                  Collections.singletonList("1"),
                  TestInsertStage.UNTIL_PLANNED));
      session.executeNonQueryStatement("INSERT INTO root.staged_empty.d(time,s) VALUES(1,1)");
      session.testInsertRecord(
          "root.staged_empty.d",
          2,
          Collections.singletonList("s"),
          Collections.singletonList("2"),
          TestInsertStage.UNTIL_WRITE_TARGET_RECEIVED);
      try (SessionDataSet data =
          session.executeQueryStatement("SELECT count(s) FROM root.staged_empty.d")) {
        assertEquals(1, data.next().getFields().get(0).getLongV());
      }
    }
  }

  @Test
  public void testTemplateIsNotActivatedOrExtended() throws Exception {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.executeNonQueryStatement(
          "CREATE SCHEMA TEMPLATE staged_template (s INT64 ENCODING=PLAIN)");
      session.executeNonQueryStatement("CREATE DATABASE root.staged_template");
      session.executeNonQueryStatement(
          "SET SCHEMA TEMPLATE staged_template TO root.staged_template");
      assertThrows(
          StatementExecutionException.class,
          () ->
              session.testInsertRecord(
                  "root.staged_template.d",
                  1,
                  Collections.singletonList("s"),
                  Collections.singletonList("1"),
                  TestInsertStage.UNTIL_ANALYZED));
      assertTrue(session.showPathsTemplateUsingOn("staged_template").isEmpty());
      session.executeNonQueryStatement("INSERT INTO root.staged_template.d(time,s) VALUES(1,1)");
      assertThrows(
          StatementExecutionException.class,
          () ->
              session.testInsertRecord(
                  "root.staged_template.d",
                  2,
                  Collections.singletonList("extra"),
                  Collections.singletonList("1"),
                  TestInsertStage.UNTIL_ANALYZED));
      try (SessionDataSet data =
          session.executeQueryStatement("SHOW TIMESERIES root.staged_template.d.extra")) {
        assertFalse(data.hasNext());
      }
      session.testInsertRecord(
          "root.staged_template.d",
          2,
          Collections.singletonList("s"),
          Collections.singletonList("2"),
          TestInsertStage.UNTIL_WRITE_TARGET_RECEIVED);
    }
  }

  @Test
  public void testTableDeviceAndAttributeRemainUnchanged() throws Exception {
    try (ITableSession session =
        new TableSessionBuilder()
            .nodeUrls(
                Collections.singletonList(
                    EnvFactory.getEnv().getIP() + ":" + EnvFactory.getEnv().getPort()))
            .build()) {
      session.executeNonQueryStatement("CREATE DATABASE staged_table");
      session.executeNonQueryStatement("USE staged_table");
      session.executeNonQueryStatement(
          "CREATE TABLE t(id STRING TAG, attr STRING ATTRIBUTE, v INT64 FIELD)");
      session.executeNonQueryStatement("INSERT INTO t(time,id,attr,v) VALUES(1,'d','original',1)");
      session.testInsert(tableTablet("d", "original"));
      for (TestInsertStage stage : TestInsertStage.values()) {
        session.testInsert(tableTablet("d", "original"), stage);
      }
      try (ITableSessionPool pool =
          new TableSessionPoolBuilder()
              .nodeUrls(
                  Collections.singletonList(
                      EnvFactory.getEnv().getIP() + ":" + EnvFactory.getEnv().getPort()))
              .database("staged_table")
              .maxSize(1)
              .build()) {
        try (ITableSession borrowed = pool.getSession()) {
          borrowed.testInsert(tableTablet("d", "original"));
          for (TestInsertStage stage : TestInsertStage.values()) {
            borrowed.testInsert(tableTablet("d", "original"), stage);
          }
          assertThrows(
              StatementExecutionException.class,
              () ->
                  borrowed.testInsert(tableTablet("d", "changed"), TestInsertStage.UNTIL_ANALYZED));
        }
        // A statement error leaves the connection usable and returns it to the pool on close.
        try (ITableSession borrowed = pool.getSession();
            SessionDataSet data = borrowed.executeQueryStatement("SELECT count(*) FROM t")) {
          assertEquals(1, data.next().getFields().get(0).getLongV());
        }
      }

      assertThrows(
          StatementExecutionException.class,
          () ->
              session.testInsert(
                  tableTablet("missing", "original"), TestInsertStage.UNTIL_ANALYZED));
      assertThrows(
          StatementExecutionException.class,
          () -> session.testInsert(tableTablet("d", "changed"), TestInsertStage.UNTIL_ANALYZED));
      try (SessionDataSet data = session.executeQueryStatement("SELECT id,attr,v FROM t")) {
        assertTrue(data.hasNext());
        assertEquals("original", data.next().getFields().get(1).getStringValue());
        assertFalse(data.hasNext());
      }
    }
  }

  private Tablet tableTablet(String device, String attribute) {
    Tablet tablet =
        new Tablet(
            "t",
            Arrays.asList("id", "attr", "v"),
            Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.INT64),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.ATTRIBUTE, ColumnCategory.FIELD));
    tablet.addTimestamp(0, 2);
    tablet.addValue("id", 0, device);
    tablet.addValue("attr", 0, attribute);
    tablet.addValue("v", 0, 2L);
    return tablet;
  }
}
