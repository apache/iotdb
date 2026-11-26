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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.relational.it.schema;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category({TableClusterIT.class})
public class IoTDBTableTagSchemaMigrationIT {

  private static final String DATABASE = "metadata_tag_key_migration";
  private static final String TABLE = "tag_key";

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setSchemaEngineMode("Memory")
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaReplicationFactor(1)
        .setDataReplicationFactor(1)
        .setSchemaRegionGroupExtensionPolicy("CUSTOM")
        .setDataRegionGroupExtensionPolicy("CUSTOM")
        .setDefaultSchemaRegionGroupNumPerDatabase(1)
        .setDefaultDataRegionGroupNumPerDatabase(1);
    EnvFactory.getEnv().initClusterEnvironment(1, 2);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testAddedTagDeviceKeysAfterSchemaRegionMigration() throws Exception {
    try (ITableSession setupSession = EnvFactory.getEnv().getTableSessionConnection()) {
      setupSession.executeNonQueryStatement("CREATE DATABASE " + DATABASE);
      setupSession.executeNonQueryStatement("USE " + DATABASE);
      setupSession.executeNonQueryStatement(
          "CREATE TABLE "
              + TABLE
              + " (site STRING TAG, device STRING TAG, model STRING ATTRIBUTE, reading INT32 FIELD)");
      insertTablet(setupSession, false, "legacy", null, "legacy", 100, 10);
      setupSession.executeNonQueryStatement("FLUSH");
    }

    Map<Integer, Set<Integer>> placements = getSchemaRegionPlacements();
    assertEquals("The test database should use one SchemaRegion", 1, placements.size());
    Map.Entry<Integer, Set<Integer>> placement = placements.entrySet().iterator().next();
    int regionId = placement.getKey();
    Set<Integer> originalDataNodes = placement.getValue();
    // Moving the sole Ratis replica makes every subsequent schema operation use restored state.
    assertEquals("The SchemaRegion must have one source replica", 1, originalDataNodes.size());

    int sourceDataNodeId = originalDataNodes.iterator().next();
    int destinationDataNodeId =
        getDataNodeIds().stream()
            .filter(dataNodeId -> !originalDataNodes.contains(dataNodeId))
            .findFirst()
            .orElseThrow(() -> new AssertionError("No non-replica DataNode is available"));
    DataNodeWrapper source = getDataNodeWrapper(sourceDataNodeId);
    DataNodeWrapper destination = getDataNodeWrapper(destinationDataNodeId);
    Set<Integer> migratedDataNodes = new HashSet<>(originalDataNodes);
    migratedDataNodes.remove(sourceDataNodeId);
    migratedDataNodes.add(destinationDataNodeId);

    try (ITableSession longLivedSession =
        EnvFactory.getEnv()
            .getTableSessionConnection(Collections.singletonList(source.getIpAndPortString()))) {
      longLivedSession.executeNonQueryStatement("USE " + DATABASE);
      try (SessionDataSet warmed =
          longLivedSession.executeQueryStatement(
              "SELECT site, device, model, reading FROM "
                  + TABLE
                  + " WHERE site = 'north' AND device = 'legacy'")) {
        // Only an exact-key fetch fills the attribute cache; an unfiltered SHOW DEVICES does not.
        assertEquals(Arrays.asList("site", "device", "model", "reading"), warmed.getColumnNames());
        assertEquals(Arrays.asList("STRING", "STRING", "STRING", "INT32"), warmed.getColumnTypes());
        assertTrue("The old device must be cached before the TAG change", warmed.hasNext());
        assertEquals("legacy", warmed.next().getFields().get(2).getStringValue());
        assertFalse(warmed.hasNext());
      }

      longLivedSession.executeNonQueryStatement(
          "ALTER TABLE " + TABLE + " ADD COLUMN zone STRING TAG");
      longLivedSession.executeNonQueryStatement(
          "INSERT INTO "
              + TABLE
              + " (time, site, device, reading) VALUES (101, 'north', 'legacy', 11)");
      longLivedSession.executeNonQueryStatement(
          "INSERT INTO "
              + TABLE
              + " (time, site, device, zone, reading) VALUES (102, 'north', 'legacy', NULL, 12)");
      longLivedSession.executeNonQueryStatement(
          "INSERT INTO "
              + TABLE
              + " (time, site, device, zone, model, reading) "
              + "VALUES (103, 'north', 'legacy', '', 'empty', 13)");
      insertTablet(longLivedSession, true, "legacy", "z1", "z1", 104, 14);
      longLivedSession.executeNonQueryStatement("FLUSH");
      // Only the appended TAG differs: using different original TAGs would hide key collisions.
      assertDevices("legacy", "empty", "z1");
      assertRows("zone IS NULL", "100,legacy,10,", "101,legacy,11,", "102,legacy,12,");
      assertRows("zone = ''", "103,empty,13,");
      assertRows("zone = 'z1'", "104,z1,14,");

      longLivedSession.executeNonQueryStatement(
          String.format(
              "MIGRATE REGION %d FROM %d TO %d",
              regionId, sourceDataNodeId, destinationDataNodeId));
      Awaitility.await()
          .atMost(2, TimeUnit.MINUTES)
          .pollInterval(1, TimeUnit.SECONDS)
          .untilAsserted(
              () -> assertEquals(migratedDataNodes, getSchemaRegionDataNodeIds(regionId)));

      try (ITableSession destinationSession =
          EnvFactory.getEnv()
              .getTableSessionConnection(
                  Collections.singletonList(destination.getIpAndPortString()))) {
        destinationSession.executeNonQueryStatement("USE " + DATABASE);
        assertDevices("legacy", "empty", "z1");
        assertRows("zone IS NULL", "100,legacy,10,", "101,legacy,11,", "102,legacy,12,");
        assertRows("zone = ''", "103,empty,13,");
        assertRows("zone = 'z1'", "104,z1,14,");
        insertTablet(destinationSession, true, "legacy", "z1", "z1", 111, 15);
        longLivedSession.executeNonQueryStatement(
            "INSERT INTO "
                + TABLE
                + " (time, site, device, reading) VALUES (110, 'north', 'legacy', 15)");
        longLivedSession.executeNonQueryStatement(
            "INSERT INTO "
                + TABLE
                + " (time, site, device, zone, reading) VALUES (120, 'north', 'legacy', '', 16)");

        longLivedSession.executeNonQueryStatement(
            "UPDATE "
                + TABLE
                + " SET model = 'legacy-updated' "
                + "WHERE site = 'north' AND device = 'legacy' AND zone IS NULL");
        longLivedSession.executeNonQueryStatement(
            "UPDATE "
                + TABLE
                + " SET model = 'empty-updated' "
                + "WHERE site = 'north' AND device = 'legacy' AND zone = ''");
        longLivedSession.executeNonQueryStatement(
            "UPDATE "
                + TABLE
                + " SET model = 'tagged-updated' "
                + "WHERE site = 'north' AND device = 'legacy' AND zone = 'z1'");

        Awaitility.await()
            .atMost(30, TimeUnit.SECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .untilAsserted(
                () -> assertDevices("legacy-updated", "empty-updated", "tagged-updated"));
        assertRows(
            "zone IS NULL",
            "100,legacy-updated,10,",
            "101,legacy-updated,11,",
            "102,legacy-updated,12,",
            "110,legacy-updated,15,");
        assertRows("zone = ''", "103,empty-updated,13,", "120,empty-updated,16,");
        assertRows("zone = 'z1'", "104,tagged-updated,14,", "111,tagged-updated,15,");

        longLivedSession.executeNonQueryStatement(
            "DELETE DEVICES FROM "
                + TABLE
                + " WHERE site = 'north' AND device = 'legacy' AND zone = 'z1'");
      }

      TestUtils.tableResultSetEqualTestUnordered(
          "SHOW DEVICES FROM " + TABLE,
          new String[] {"site", "device", "model", "zone"},
          new String[] {"north,legacy,legacy-updated,null,", "north,legacy,empty-updated,,"},
          DATABASE);
      assertRows("zone = 'z1'");
      TestUtils.tableResultSetEqualTestUnordered(
          "COUNT DEVICES FROM " + TABLE,
          new String[] {"count(devices)"},
          new String[] {"2,"},
          DATABASE);
      TestUtils.tableResultSetEqualTestUnordered(
          "SELECT CAST(time AS INT64) AS time, site, device, zone, model, reading FROM " + TABLE,
          new String[] {"time", "site", "device", "zone", "model", "reading"},
          new String[] {
            "100,north,legacy,null,legacy-updated,10,",
            "101,north,legacy,null,legacy-updated,11,",
            "102,north,legacy,null,legacy-updated,12,",
            "110,north,legacy,null,legacy-updated,15,",
            "103,north,legacy,,empty-updated,13,",
            "120,north,legacy,,empty-updated,16,"
          },
          DATABASE);
      longLivedSession.executeNonQueryStatement(
          "DELETE DEVICES FROM "
              + TABLE
              + " WHERE site = 'north' AND device = 'legacy' AND zone IS NULL");
      TestUtils.tableResultSetEqualTestUnordered(
          "SHOW DEVICES FROM " + TABLE,
          new String[] {"site", "device", "model", "zone"},
          new String[] {"north,legacy,empty-updated,,"},
          DATABASE);
      TestUtils.tableResultSetEqualTestUnordered(
          "COUNT DEVICES FROM " + TABLE,
          new String[] {"count(devices)"},
          new String[] {"1,"},
          DATABASE);
      assertRows("zone IS NULL");
      assertRows("zone = ''", "103,empty-updated,13,", "120,empty-updated,16,");
    }
  }

  private static void assertDevices(String nullModel, String emptyModel, String taggedModel) {
    TestUtils.tableResultSetEqualTest(
        "DESCRIBE " + TABLE,
        new String[] {"ColumnName", "DataType", "Category"},
        new String[] {
          "time,TIMESTAMP,TIME,", "site,STRING,TAG,", "device,STRING,TAG,",
          "model,STRING,ATTRIBUTE,", "reading,INT32,FIELD,", "zone,STRING,TAG,"
        },
        DATABASE);
    TestUtils.tableResultSetEqualTestUnordered(
        "SHOW DEVICES FROM " + TABLE,
        new String[] {"site", "device", "model", "zone"},
        new String[] {
          "north,legacy," + nullModel + ",null,",
          "north,legacy," + emptyModel + ",,",
          "north,legacy," + taggedModel + ",z1,"
        },
        DATABASE);
    TestUtils.tableResultSetEqualTestUnordered(
        "COUNT DEVICES FROM " + TABLE,
        new String[] {"count(devices)"},
        new String[] {"3,"},
        DATABASE);
  }

  private static void assertRows(String zonePredicate, String... expectedRows) {
    TestUtils.tableResultSetEqualTestUnordered(
        "SELECT CAST(time AS INT64) AS time, model, reading FROM "
            + TABLE
            + " WHERE site = 'north' AND device = 'legacy' AND "
            + zonePredicate,
        new String[] {"time", "model", "reading"},
        expectedRows,
        DATABASE);
  }

  private static void insertTablet(
      ITableSession session,
      boolean includeZone,
      String device,
      String zone,
      String model,
      long time,
      int reading)
      throws Exception {
    List<IMeasurementSchema> schemas = new ArrayList<>();
    schemas.add(new MeasurementSchema("site", TSDataType.STRING));
    schemas.add(new MeasurementSchema("device", TSDataType.STRING));
    List<ColumnCategory> categories =
        new ArrayList<>(Arrays.asList(ColumnCategory.TAG, ColumnCategory.TAG));
    if (includeZone) {
      schemas.add(new MeasurementSchema("zone", TSDataType.STRING));
      categories.add(ColumnCategory.TAG);
    }
    schemas.add(new MeasurementSchema("model", TSDataType.STRING));
    schemas.add(new MeasurementSchema("reading", TSDataType.INT32));
    categories.add(ColumnCategory.ATTRIBUTE);
    categories.add(ColumnCategory.FIELD);

    Tablet tablet =
        new Tablet(
            TABLE,
            IMeasurementSchema.getMeasurementNameList(schemas),
            IMeasurementSchema.getDataTypeList(schemas),
            categories,
            1);
    tablet.addTimestamp(0, time);
    tablet.addValue("site", 0, "north");
    tablet.addValue("device", 0, device);
    if (includeZone) {
      tablet.addValue("zone", 0, zone);
    }
    tablet.addValue("model", 0, model);
    tablet.addValue("reading", 0, reading);
    session.insert(tablet);
  }

  private static Map<Integer, Set<Integer>> getSchemaRegionPlacements() throws SQLException {
    Map<Integer, Set<Integer>> placements = new HashMap<>();
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SHOW REGIONS")) {
      while (resultSet.next()) {
        if (String.valueOf(TConsensusGroupType.SchemaRegion)
                .equals(resultSet.getString(ColumnHeaderConstant.TYPE))
            && DATABASE.equalsIgnoreCase(resultSet.getString(ColumnHeaderConstant.DATABASE))) {
          int regionId = resultSet.getInt(ColumnHeaderConstant.REGION_ID);
          int dataNodeId = resultSet.getInt(ColumnHeaderConstant.DATA_NODE_ID);
          placements.computeIfAbsent(regionId, unused -> new HashSet<>()).add(dataNodeId);
        }
      }
    }
    return placements;
  }

  private static Set<Integer> getSchemaRegionDataNodeIds(int regionId) throws SQLException {
    return getSchemaRegionPlacements().getOrDefault(regionId, Collections.emptySet());
  }

  private static DataNodeWrapper getDataNodeWrapper(int dataNodeId) {
    return EnvFactory.getEnv()
        .dataNodeIdToWrapper(dataNodeId)
        .orElseThrow(() -> new AssertionError("DataNode " + dataNodeId + " is not running"));
  }

  private static Set<Integer> getDataNodeIds() throws SQLException {
    Set<Integer> dataNodes = new HashSet<>();
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SHOW DATANODES")) {
      while (resultSet.next()) {
        dataNodes.add(resultSet.getInt(ColumnHeaderConstant.NODE_ID));
      }
    }
    return dataNodes;
  }
}
