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

package org.apache.iotdb.relational.it.schema;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBTreeToTableViewAutoSyncIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  // ==================== the property and its index (Trie) ====================

  /**
   * The property defaults to false: when the view is created, only the series which exist become
   * columns. An explicit false behaves the same, and SHOW CREATE VIEW only shows the property when
   * it is stored.
   */
  @Test
  @Ignore
  public void testAutoSyncSchemaDisabledByDefault() throws Exception {
    final String treeDb = "root.as01";
    final String viewDb = "as01";
    try (Connection tree = treeConnection();
        Connection table = tableConnection()) {
      execute(tree, "CREATE DATABASE " + treeDb);
      execute(tree, "CREATE TIMESERIES " + treeDb + ".d1.s1 INT32");

      execute(table, "CREATE DATABASE " + viewDb);
      execute(table, "USE " + viewDb);
      execute(table, "CREATE VIEW v1 (dev STRING TAG) AS " + treeDb + ".**");
      // the creation always scans the series which already exist
      awaitFields(table, viewDb, "v1", fields("s1", "INT32"));
      Assert.assertFalse(showCreateView(table, viewDb, "v1").contains("auto_sync_schema"));

      // the default is false, so a new measurement is not added
      execute(tree, "CREATE TIMESERIES " + treeDb + ".d1.s2 INT32");
      awaitAbsent(table, viewDb, "v1", "s2");

      execute(
          table,
          "CREATE VIEW v2 (dev STRING TAG) WITH (auto_sync_schema=false) AS " + treeDb + ".**");
      awaitFields(table, viewDb, "v2", fields("s1", "INT32", "s2", "INT32"));
      Assert.assertTrue(showCreateView(table, viewDb, "v2").contains("auto_sync_schema=false"));
      execute(tree, "CREATE TIMESERIES " + treeDb + ".d1.s3 INT32");
      awaitAbsent(table, viewDb, "v2", "s3");

      // an explicit true follows the new measurements, both the existing and the later ones
      execute(
          table,
          "CREATE VIEW v3 (dev STRING TAG) WITH (auto_sync_schema=true) AS " + treeDb + ".**");
      awaitFields(table, viewDb, "v3", fields("s1", "INT32", "s2", "INT32", "s3", "INT32"));
      Assert.assertTrue(showCreateView(table, viewDb, "v3").contains("auto_sync_schema=true"));
      execute(tree, "CREATE TIMESERIES " + treeDb + ".d1.s4 INT32");
      awaitFields(
          table, viewDb, "v3", fields("s1", "INT32", "s2", "INT32", "s3", "INT32", "s4", "INT32"));
    }
  }

  /** The property is only accepted by the views created on a tree path, not by the tables. */
  @Test
  @Ignore
  public void testAutoSyncSchemaOnlyForTreeToTableViews() throws Exception {
    final String viewDb = "as02";
    try (Connection table = tableConnection()) {
      execute(table, "CREATE DATABASE " + viewDb);
      execute(table, "USE " + viewDb);
      execute(table, "CREATE TABLE t1 (tag1 STRING TAG, s1 INT32 FIELD)");

      assertSqlError(
          table,
          "CREATE TABLE t2 (tag1 STRING TAG, s1 INT32 FIELD) WITH (auto_sync_schema=true)",
          "auto_sync_schema");
      assertSqlError(
          table, "ALTER TABLE t1 SET PROPERTIES auto_sync_schema=true", "auto_sync_schema");
    }
  }

  /**
   * A measurement whose name only differs in case from an existing column, or whose name is the
   * same but whose type is different, does not change the view; the other measurements of the same
   * write are still added.
   */
  @Test
  @Ignore
  public void testConflictingMeasurementsDoNotChangeTheView() throws Exception {
    final String treeDb = "root.as03";
    final String viewDb = "as03";
    try (Connection tree = treeConnection();
        Connection table = tableConnection()) {
      execute(tree, "CREATE DATABASE " + treeDb);
      execute(tree, "CREATE TIMESERIES " + treeDb + ".d1.s1 DOUBLE");
      execute(table, "CREATE DATABASE " + viewDb);
      execute(table, "USE " + viewDb);
      execute(
          table,
          "CREATE VIEW v (dev STRING TAG) WITH (auto_sync_schema=true) AS " + treeDb + ".**");
      awaitFields(table, viewDb, "v", fields("s1", "DOUBLE"));

      // the same name with another type: the series is created, the column is not touched
      execute(tree, "CREATE TIMESERIES " + treeDb + ".d2.s1 INT32");
      // a new measurement of the same write is still added
      execute(tree, "CREATE TIMESERIES " + treeDb + ".d2.s2 INT32");
      awaitFields(table, viewDb, "v", fields("s1", "DOUBLE", "s2", "INT32"));

      // the same name in another case is a conflict as well: no column is added
      execute(tree, "CREATE TIMESERIES " + treeDb + ".d3.S2 INT32");
      awaitFields(table, viewDb, "v", fields("s1", "DOUBLE", "s2", "INT32"));
    }
  }

  /**
   * A restrict view matches a device with exactly numOfTags levels under its prefix, a non-restrict
   * view matches every device with at most numOfTags levels. A device may be covered by several
   * views at once.
   */
  @Test
  @Ignore
  public void testTrieMatchingOfRestrictAndNonRestrictViews() throws Exception {
    final String treeDb = "root.as04";
    final String viewDb = "as04";
    try (Connection tree = treeConnection();
        Connection table = tableConnection()) {
      execute(tree, "CREATE DATABASE " + treeDb);
      execute(table, "CREATE DATABASE " + viewDb);
      execute(table, "USE " + viewDb);
      execute(
          table,
          "CREATE VIEW v_restrict (a STRING TAG, b STRING TAG) RESTRICT "
              + "WITH (auto_sync_schema=true) AS "
              + treeDb
              + ".**");
      execute(
          table,
          "CREATE VIEW v_non_restrict (a STRING TAG) "
              + "WITH (auto_sync_schema=true) AS "
              + treeDb
              + ".**");

      // one level under the prefix: only the non-restrict view matches
      execute(tree, "CREATE TIMESERIES " + treeDb + ".p.s1 INT32");
      awaitFields(table, viewDb, "v_non_restrict", fields("s1", "INT32"));
      awaitAbsent(table, viewDb, "v_restrict", "s1");

      // two levels: only the restrict view matches
      execute(tree, "CREATE TIMESERIES " + treeDb + ".p.q.s2 INT32");
      awaitFields(table, viewDb, "v_restrict", fields("s2", "INT32"));
      awaitFields(table, viewDb, "v_non_restrict", fields("s1", "INT32"));

      // three levels: neither view matches
      execute(tree, "CREATE TIMESERIES " + treeDb + ".p.q.r.s3 INT32");
      awaitAbsent(table, viewDb, "v_restrict", "s3");
      awaitAbsent(table, viewDb, "v_non_restrict", "s3");
    }
  }

  /** Renaming a view keeps its place in the index; the property can be switched on and off. */
  @Test
  @Ignore
  public void testTrieRenameAndToggleAutoSyncSchema() throws Exception {
    final String treeDb = "root.as05";
    final String viewDb = "as05";
    try (Connection tree = treeConnection();
        Connection table = tableConnection()) {
      execute(tree, "CREATE DATABASE " + treeDb);
      execute(table, "CREATE DATABASE " + viewDb);
      execute(table, "USE " + viewDb);
      execute(
          table,
          "CREATE VIEW v (dev STRING TAG) WITH (auto_sync_schema=true) AS " + treeDb + ".**");
      execute(tree, "CREATE TIMESERIES " + treeDb + ".d1.s1 INT32");
      awaitFields(table, viewDb, "v", fields("s1", "INT32"));

      execute(table, "ALTER VIEW v RENAME TO v2");
      execute(tree, "CREATE TIMESERIES " + treeDb + ".d2.s2 INT32");
      awaitFields(table, viewDb, "v2", fields("s1", "INT32", "s2", "INT32"));
      assertSqlError(table, "DESC v", "does not exist");

      // turned off: the new measurements are ignored
      execute(table, "ALTER VIEW v2 SET PROPERTIES auto_sync_schema=false");
      execute(tree, "CREATE TIMESERIES " + treeDb + ".d3.s3 INT32");
      awaitAbsent(table, viewDb, "v2", "s3");

      // turned on again: the missing columns are backfilled, and the later ones are followed
      execute(table, "ALTER VIEW v2 SET PROPERTIES auto_sync_schema=true");
      awaitFields(table, viewDb, "v2", fields("s1", "INT32", "s2", "INT32", "s3", "INT32"));
      execute(tree, "CREATE TIMESERIES " + treeDb + ".d4.s4 INT32");
      awaitFields(
          table, viewDb, "v2", fields("s1", "INT32", "s2", "INT32", "s3", "INT32", "s4", "INT32"));
    }
  }

  /**
   * CREATE OR REPLACE registers the new definition of the view: the old path stops being followed,
   * and the new matching attributes take effect.
   */
  @Test
  @Ignore
  public void testTrieReplaceView() throws Exception {
    final String treeDb = "root.as06";
    final String viewDb = "as06";
    try (Connection tree = treeConnection();
        Connection table = tableConnection()) {
      execute(tree, "CREATE DATABASE " + treeDb);
      execute(table, "CREATE DATABASE " + viewDb);
      execute(table, "USE " + viewDb);
      execute(
          table,
          "CREATE VIEW v (dev STRING TAG) WITH (auto_sync_schema=true) AS " + treeDb + ".old.**");
      execute(tree, "CREATE TIMESERIES " + treeDb + ".old.a.s1 INT32");
      awaitFields(table, viewDb, "v", fields("s1", "INT32"));

      // another source path: the new definition replaces the old one
      execute(
          table,
          "CREATE OR REPLACE VIEW v (dev STRING TAG) WITH (auto_sync_schema=true) AS "
              + treeDb
              + ".new.**");
      awaitFields(table, viewDb, "v", fields());
      execute(tree, "CREATE TIMESERIES " + treeDb + ".old.b.s2 INT32");
      awaitAbsent(table, viewDb, "v", "s2");
      execute(tree, "CREATE TIMESERIES " + treeDb + ".new.c.s3 INT32");
      awaitFields(table, viewDb, "v", fields("s3", "INT32"));

      // the same path with another tag count: the new matching attributes replace the old ones
      execute(
          table,
          "CREATE VIEW w (a STRING TAG) RESTRICT WITH (auto_sync_schema=true) AS "
              + treeDb
              + ".replace.**");
      execute(tree, "CREATE TIMESERIES " + treeDb + ".replace.p.s1 INT32");
      awaitFields(table, viewDb, "w", fields("s1", "INT32"));
      execute(
          table,
          "CREATE OR REPLACE VIEW w (a STRING TAG, b STRING TAG) RESTRICT "
              + "WITH (auto_sync_schema=true) AS "
              + treeDb
              + ".replace.**");
      awaitFields(table, viewDb, "w", fields());
      execute(tree, "CREATE TIMESERIES " + treeDb + ".replace.p.q.s2 INT32");
      awaitFields(table, viewDb, "w", fields("s2", "INT32"));
      execute(tree, "CREATE TIMESERIES " + treeDb + ".replace.m.s3 INT32");
      awaitAbsent(table, viewDb, "w", "s3");
    }
  }

  /** Dropping a view or its database removes the entry from the index of the DataNodes. */
  @Test
  @Ignore
  public void testTrieDropViewAndDropDatabase() throws Exception {
    final String treeDb = "root.as07";
    final String viewDb = "as07";
    try (Connection tree = treeConnection();
        Connection table = tableConnection()) {
      execute(tree, "CREATE DATABASE " + treeDb);
      execute(table, "CREATE DATABASE " + viewDb);
      execute(table, "USE " + viewDb);
      execute(
          table,
          "CREATE VIEW v (dev STRING TAG) WITH (auto_sync_schema=true) AS " + treeDb + ".old.**");
      execute(tree, "CREATE TIMESERIES " + treeDb + ".old.a.s1 INT32");
      awaitFields(table, viewDb, "v", fields("s1", "INT32"));

      execute(table, "DROP VIEW v");
      assertSqlError(table, "DESC v", "does not exist");
      // a view of another path follows only its own path
      execute(
          table,
          "CREATE VIEW v (dev STRING TAG) WITH (auto_sync_schema=true) AS " + treeDb + ".new.**");
      execute(tree, "CREATE TIMESERIES " + treeDb + ".old.b.s2 INT32");
      awaitAbsent(table, viewDb, "v", "s2");
      execute(tree, "CREATE TIMESERIES " + treeDb + ".new.c.s3 INT32");
      awaitFields(table, viewDb, "v", fields("s3", "INT32"));
    }

    // dropping the database removes its views from the index: a view of the same name created
    // again with another source path is not polluted by the old one
    final String replacedTreeDb = "root.as07b";
    final String replacedViewDb = "as07b";
    try (Connection tree = treeConnection();
        Connection table = tableConnection()) {
      execute(tree, "CREATE DATABASE " + replacedTreeDb);
      execute(tree, "CREATE TIMESERIES " + replacedTreeDb + ".old.d1.s1 INT32");
      execute(table, "CREATE DATABASE " + replacedViewDb);
      execute(table, "USE " + replacedViewDb);
      execute(
          table,
          "CREATE VIEW v (dev STRING TAG) WITH (auto_sync_schema=true) AS "
              + replacedTreeDb
              + ".old.**");
      awaitFields(table, replacedViewDb, "v", fields("s1", "INT32"));

      execute(table, "DROP DATABASE " + replacedViewDb);
      execute(table, "CREATE DATABASE " + replacedViewDb);
      execute(table, "USE " + replacedViewDb);
      execute(
          table,
          "CREATE VIEW v (dev STRING TAG) WITH (auto_sync_schema=true) AS "
              + replacedTreeDb
              + ".new.**");

      execute(tree, "CREATE TIMESERIES " + replacedTreeDb + ".old.d2.s2 INT32");
      awaitAbsent(table, replacedViewDb, "v", "s2");
      execute(tree, "CREATE TIMESERIES " + replacedTreeDb + ".new.d3.s3 INT32");
      awaitFields(table, replacedViewDb, "v", fields("s3", "INT32"));
    }
  }

  /** Adding a tag column changes the tag count the view matches devices with. */
  @Test
  @Ignore
  public void testTrieAddingTagColumnUpdatesTheTagCount() throws Exception {
    final String treeDb = "root.as08";
    final String viewDb = "as08";
    try (Connection tree = treeConnection();
        Connection table = tableConnection()) {
      execute(tree, "CREATE DATABASE " + treeDb);
      execute(table, "CREATE DATABASE " + viewDb);
      execute(table, "USE " + viewDb);
      execute(
          table,
          "CREATE VIEW v (a STRING TAG) RESTRICT WITH (auto_sync_schema=true) AS "
              + treeDb
              + ".**");

      // the tag count of the view, hence the devices it matches, changes with the new tag column
      execute(table, "ALTER VIEW v ADD COLUMN b STRING TAG");

      // two levels under the prefix now match the restrict view
      execute(tree, "CREATE TIMESERIES " + treeDb + ".p.q.s2 INT32");
      awaitFields(table, viewDb, "v", fields("s2", "INT32"));
      // one level does not match any more
      execute(tree, "CREATE TIMESERIES " + treeDb + ".p.s1 INT32");
      awaitAbsent(table, viewDb, "v", "s1");
      // three levels do not match either
      execute(tree, "CREATE TIMESERIES " + treeDb + ".p.q.r.s3 INT32");
      awaitAbsent(table, viewDb, "v", "s3");
    }
  }

  // ==================== the interfaces which create measurements ====================

  @Test
  @Ignore
  public void testCreateTimeSeriesAddsColumn() throws Exception {
    try (Connection tree = treeConnection();
        Connection table = tableConnection()) {
      final String treeDb = "root.as10";
      final String viewDb = "as10";
      prepareView(tree, table, treeDb, viewDb);
      execute(tree, "CREATE TIMESERIES " + treeDb + ".d1.s1 INT32");
      awaitFields(table, viewDb, "v", fields("s1", "INT32"));
    }
  }

  @Test
  @Ignore
  public void testCreateAlignedTimeSeriesAddsColumns() throws Exception {
    try (Connection tree = treeConnection();
        Connection table = tableConnection()) {
      final String treeDb = "root.as11";
      final String viewDb = "as11";
      prepareView(tree, table, treeDb, viewDb);
      execute(tree, "CREATE ALIGNED TIMESERIES " + treeDb + ".d1 (s1 INT32, s2 DOUBLE)");
      awaitFields(table, viewDb, "v", fields("s1", "INT32", "s2", "DOUBLE"));
    }
  }

  @Test
  @Ignore
  public void testCreateMultiTimeseriesAddsColumns() throws Exception {
    try (Connection tree = treeConnection();
        Connection table = tableConnection();
        ISession session = EnvFactory.getEnv().getSessionConnection()) {
      final String treeDb = "root.as12";
      final String viewDb = "as12";
      prepareView(tree, table, treeDb, viewDb);
      session.createMultiTimeseries(
          Arrays.asList(treeDb + ".d1.s1", treeDb + ".d1.s2"),
          Arrays.asList(TSDataType.INT32, TSDataType.DOUBLE),
          Arrays.asList(TSEncoding.PLAIN, TSEncoding.PLAIN),
          Arrays.asList(CompressionType.LZ4, CompressionType.LZ4),
          null,
          null,
          null,
          null);
      awaitFields(table, viewDb, "v", fields("s1", "INT32", "s2", "DOUBLE"));
    }
  }

  @Test
  @Ignore
  public void testInsertAutoCreatesColumn() throws Exception {
    try (Connection tree = treeConnection();
        Connection table = tableConnection()) {
      final String treeDb = "root.as13";
      final String viewDb = "as13";
      prepareView(tree, table, treeDb, viewDb);
      execute(tree, "INSERT INTO " + treeDb + ".d1(time, s1) VALUES (1, 1.0)");
      awaitFields(table, viewDb, "v", fields("s1", "DOUBLE"));
    }
  }

  @Test
  @Ignore
  public void testInsertMultipleDevicesAutoCreatesColumns() throws Exception {
    try (Connection tree = treeConnection();
        Connection table = tableConnection();
        ISession session = EnvFactory.getEnv().getSessionConnection()) {
      final String treeDb = "root.as14";
      final String viewDb = "as14";
      prepareView(tree, table, treeDb, viewDb);
      final List<Object> valuesOfD1 = Collections.singletonList(1);
      final List<Object> valuesOfD2 = Collections.singletonList(2.0);
      session.insertRecords(
          Arrays.asList(treeDb + ".d1", treeDb + ".d2"),
          Arrays.asList(1L, 1L),
          Arrays.asList(Collections.singletonList("s1"), Collections.singletonList("s2")),
          Arrays.asList(
              Collections.singletonList(TSDataType.INT32),
              Collections.singletonList(TSDataType.DOUBLE)),
          Arrays.asList(valuesOfD1, valuesOfD2));
      awaitFields(table, viewDb, "v", fields("s1", "INT32", "s2", "DOUBLE"));
    }
  }

  @Test
  @Ignore
  public void testActivateTemplateAddsColumns() throws Exception {
    try (Connection tree = treeConnection();
        Connection table = tableConnection()) {
      final String treeDb = "root.as15";
      final String viewDb = "as15";
      execute(tree, "CREATE DATABASE " + treeDb);
      execute(tree, "CREATE DEVICE TEMPLATE t_as15 (s1 FLOAT, s2 DOUBLE)");
      execute(tree, "SET DEVICE TEMPLATE t_as15 TO " + treeDb);
      createView(table, viewDb, treeDb);

      execute(tree, "CREATE TIMESERIES USING DEVICE TEMPLATE ON " + treeDb + ".d1");
      awaitFields(table, viewDb, "v", fields("s1", "FLOAT", "s2", "DOUBLE"));
    }
  }

  @Test
  @Ignore
  public void testBatchActivateTemplateAddsColumns() throws Exception {
    try (Connection tree = treeConnection();
        Connection table = tableConnection();
        ISession session = EnvFactory.getEnv().getSessionConnection()) {
      final String treeDb = "root.as16";
      final String viewDb = "as16";
      execute(tree, "CREATE DATABASE " + treeDb);
      execute(tree, "CREATE DEVICE TEMPLATE t_as16 (s1 FLOAT, s2 DOUBLE)");
      execute(tree, "SET DEVICE TEMPLATE t_as16 TO " + treeDb);
      createView(table, viewDb, treeDb);

      session.createTimeseriesUsingSchemaTemplate(Arrays.asList(treeDb + ".d1", treeDb + ".d2"));
      awaitFields(table, viewDb, "v", fields("s1", "FLOAT", "s2", "DOUBLE"));
    }
  }

  @Test
  @Ignore
  public void testInternalBatchActivateTemplateAddsColumns() throws Exception {
    try (Connection tree = treeConnection();
        Connection table = tableConnection();
        ISession session = EnvFactory.getEnv().getSessionConnection()) {
      final String treeDb = "root.as17";
      final String viewDb = "as17";
      execute(tree, "CREATE DATABASE " + treeDb);
      execute(tree, "CREATE DEVICE TEMPLATE t_as17 (s1 FLOAT, s2 DOUBLE)");
      execute(tree, "SET DEVICE TEMPLATE t_as17 TO " + treeDb);
      createView(table, viewDb, treeDb);

      // the insert activates the template on both devices and creates the template measurements
      final List<Object> valuesOfD1 = Arrays.asList(1.0f, 1.0);
      final List<Object> valuesOfD2 = Arrays.asList(2.0f, 2.0);
      session.insertRecords(
          Arrays.asList(treeDb + ".d1", treeDb + ".d2"),
          Arrays.asList(1L, 1L),
          Arrays.asList(Arrays.asList("s1", "s2"), Arrays.asList("s1", "s2")),
          Arrays.asList(
              Arrays.asList(TSDataType.FLOAT, TSDataType.DOUBLE),
              Arrays.asList(TSDataType.FLOAT, TSDataType.DOUBLE)),
          Arrays.asList(valuesOfD1, valuesOfD2));
      awaitFields(table, viewDb, "v", fields("s1", "FLOAT", "s2", "DOUBLE"));
    }
  }

  @Test
  @Ignore
  public void testAlterTemplateAddsColumns() throws Exception {
    try (Connection tree = treeConnection();
        Connection table = tableConnection()) {
      final String treeDb = "root.as18";
      final String viewDb = "as18";
      execute(tree, "CREATE DATABASE " + treeDb);
      execute(tree, "CREATE DEVICE TEMPLATE t_as18 (s1 FLOAT)");
      execute(tree, "SET DEVICE TEMPLATE t_as18 TO " + treeDb);
      createView(table, viewDb, treeDb);
      execute(tree, "CREATE TIMESERIES USING DEVICE TEMPLATE ON " + treeDb + ".d1");
      awaitFields(table, viewDb, "v", fields("s1", "FLOAT"));

      execute(tree, "ALTER DEVICE TEMPLATE t_as18 ADD (s2 DOUBLE)");
      awaitFields(table, viewDb, "v", fields("s1", "FLOAT", "s2", "DOUBLE"));
    }
  }

  /** A device which is exactly the path the template is set on uses the template as well. */
  @Test
  @Ignore
  public void testAlterTemplateOnDevicePathAddsColumns() throws Exception {
    try (Connection tree = treeConnection();
        Connection table = tableConnection()) {
      final String treeDb = "root.as19";
      final String viewDb = "as19";
      execute(tree, "CREATE DATABASE " + treeDb);
      execute(tree, "CREATE DEVICE TEMPLATE t_as19 (s1 FLOAT)");
      execute(tree, "SET DEVICE TEMPLATE t_as19 TO " + treeDb + ".exact.d");
      createView(table, viewDb, treeDb, "a STRING TAG, b STRING TAG, c STRING TAG");

      execute(tree, "CREATE TIMESERIES USING DEVICE TEMPLATE ON " + treeDb + ".exact.d");
      awaitFields(table, viewDb, "v", fields("s1", "FLOAT"));

      execute(tree, "ALTER DEVICE TEMPLATE t_as19 ADD (s2 DOUBLE)");
      awaitFields(table, viewDb, "v", fields("s1", "FLOAT", "s2", "DOUBLE"));
    }
  }

  /** The template may be set above the database which holds the activated devices. */
  @Test
  @Ignore
  public void testAlterTemplateOnAncestorPathAddsColumns() throws Exception {
    try (Connection tree = treeConnection();
        Connection table = tableConnection()) {
      final String treeDb = "root.as20.db";
      final String viewDb = "as20";
      execute(tree, "CREATE DATABASE " + treeDb);
      execute(tree, "CREATE DEVICE TEMPLATE t_as20 (s1 FLOAT)");
      execute(tree, "SET DEVICE TEMPLATE t_as20 TO root.as20");
      createView(table, viewDb, "root.as20", "a STRING TAG, b STRING TAG");

      execute(tree, "CREATE TIMESERIES USING DEVICE TEMPLATE ON " + treeDb + ".d1");
      awaitFields(table, viewDb, "v", fields("s1", "FLOAT"));

      execute(tree, "ALTER DEVICE TEMPLATE t_as20 ADD (s2 DOUBLE)");
      awaitFields(table, viewDb, "v", fields("s1", "FLOAT", "s2", "DOUBLE"));
    }
  }

  /**
   * A TAG column name is not a source measurement. It must not prevent an explicit FROM column from
   * referencing a tree measurement with the same name.
   */
  @Test
  public void testTagColumnNameDoesNotBlockFromSource() throws Exception {
    final String treeDb = "root.as21";
    final String viewDb = "as21";
    try (Connection tree = treeConnection();
        Connection table = tableConnection()) {
      execute(tree, "CREATE DATABASE " + treeDb);
      execute(tree, "CREATE TIMESERIES " + treeDb + ".d1.s1 DOUBLE");

      execute(table, "CREATE DATABASE " + viewDb);
      execute(table, "USE " + viewDb);
      execute(table, "CREATE VIEW v (dev STRING TAG, dev_s1 STRING TAG) AS " + treeDb + ".**");

      // Create the measurement after the view so that it is not auto-detected as a column.
      execute(tree, "CREATE TIMESERIES " + treeDb + ".d1.dev_s1 DOUBLE");

      execute(table, "ALTER VIEW v ADD COLUMN test DOUBLE FIELD FROM dev_s1");

      Assert.assertEquals("DOUBLE", fieldColumns(table, viewDb, "v").get("test"));
    }
  }

  // ==================== helpers ====================

  private static Connection treeConnection() throws SQLException {
    return EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
  }

  private static Connection tableConnection() throws SQLException {
    return EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
  }

  private static void execute(final Connection connection, final String sql) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }

  private static void prepareView(
      final Connection tree, final Connection table, final String treeDb, final String viewDb)
      throws SQLException {
    execute(tree, "CREATE DATABASE " + treeDb);
    createView(table, viewDb, treeDb);
  }

  private static void createView(final Connection table, final String viewDb, final String treeDb)
      throws SQLException {
    createView(table, viewDb, treeDb, "dev STRING TAG");
  }

  private static void createView(
      final Connection table,
      final String viewDb,
      final String treeDb,
      final String columnDefinitions)
      throws SQLException {
    execute(table, "CREATE DATABASE " + viewDb);
    execute(table, "USE " + viewDb);
    execute(
        table,
        "CREATE VIEW v ("
            + columnDefinitions
            + ") WITH (auto_sync_schema=true) AS "
            + treeDb
            + ".**");
  }

  /** The FIELD columns of the view, column name to data type. */
  private static Map<String, String> fieldColumns(
      final Connection table, final String viewDb, final String view) throws SQLException {
    execute(table, "USE " + viewDb);
    final Map<String, String> fields = new HashMap<>();
    try (Statement statement = table.createStatement();
        ResultSet resultSet = statement.executeQuery("DESC " + view)) {
      while (resultSet.next()) {
        if ("FIELD".equalsIgnoreCase(resultSet.getString("Category"))) {
          fields.put(resultSet.getString("ColumnName"), resultSet.getString("DataType"));
        }
      }
    }
    return fields;
  }

  private static String showCreateView(
      final Connection table, final String viewDb, final String view) throws SQLException {
    execute(table, "USE " + viewDb);
    try (Statement statement = table.createStatement();
        ResultSet resultSet = statement.executeQuery("SHOW CREATE VIEW " + view)) {
      Assert.assertTrue(resultSet.next());
      return resultSet.getString(2);
    }
  }

  private static void assertSqlError(
      final Connection connection, final String sql, final String expectedMessage) {
    try (Statement statement = connection.createStatement()) {
      statement.execute(sql);
      Assert.fail("Expected an error for: " + sql);
    } catch (final SQLException e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains(expectedMessage));
    }
  }

  private static Map<String, String> fields(final String... nameTypePairs) {
    final Map<String, String> fields = new HashMap<>();
    for (int i = 0; i < nameTypePairs.length; i += 2) {
      fields.put(nameTypePairs[i], nameTypePairs[i + 1]);
    }
    return fields;
  }

  /** The column addition runs on the DataNode and the procedure may finish a bit later. */
  private static void awaitFields(
      final Connection table,
      final String viewDb,
      final String view,
      final Map<String, String> expected)
      throws Exception {
    final long deadline = System.currentTimeMillis() + 10_000L;
    Map<String, String> actual = fieldColumns(table, viewDb, view);
    while (!expected.equals(actual) && System.currentTimeMillis() < deadline) {
      Thread.sleep(100L);
      actual = fieldColumns(table, viewDb, view);
    }
    Assert.assertEquals(expected, actual);
  }

  /** A column which must not be added; waits a little so that a late addition is caught too. */
  private static void awaitAbsent(
      final Connection table, final String viewDb, final String view, final String columnName)
      throws Exception {
    Thread.sleep(500L);
    Assert.assertFalse(
        "Column " + columnName + " should not be added to " + view,
        fieldColumns(table, viewDb, view).containsKey(columnName));
  }
}
