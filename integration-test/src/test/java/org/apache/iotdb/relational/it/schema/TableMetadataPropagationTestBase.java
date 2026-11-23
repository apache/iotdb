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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.config.MppCommonConfig;
import org.apache.iotdb.it.env.cluster.config.MppJVMConfig;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.relational.it.schema.fault.MetadataFaultController;
import org.apache.iotdb.relational.it.schema.fault.MetadataFaultController.Node;
import org.apache.iotdb.rpc.TSStatusCode;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Real remote attribute pulls, selective invalidation failures and delayed propagation batches. The
 * two DataNodes share one Memory cluster; the non-replica is always the cache under test.
 */
@RunWith(IoTDBTestRunner.class)
@Category(TableClusterIT.class)
public abstract class TableMetadataPropagationTestBase {
  protected static final String TABLE = "sensor";
  protected static final String FILL = "putAttributes:enter";
  protected static final String UPDATE = "updateAttribute:enter";
  protected static final String INVALIDATE = "invalidateColumnCache:enter";
  protected static final String ACK = "invalidateColumnCache:exit";
  protected static final String DELETE = "deleteColumnData:enter";
  protected static final String ACCEPT = "handleAttributeUpdate:exit";
  protected static final String CONTAINER = "handleContainer:enter";
  protected static final String OLD_COMMIT = "sendUpdateRequestAndMayShrink:exit";
  protected static final Map<DataNodeWrapper, Node> NODES = new HashMap<>();
  protected static MetadataFaultController faults;
  protected String database;

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
        .setDefaultDataRegionGroupNumPerDatabase(1)
        .setMetadataLeaseFenceMs(8_000)
        // At a fixed 512 MiB heap this gives SchemaRegion about 256 KiB. Cache memory remains
        // sufficient for exact-device reads, so cache eviction cannot hide the propagation race.
        .setSchemaMemoryAllocate("1:199:1");
    EnvFactory.getEnv().getConfig().getDataNodeJVMConfig().setMaxHeapSize(512);
    Properties properties = new Properties();
    properties.setProperty("general_region_attribute_security_service_interval_seconds", "1");
    properties.setProperty("general_region_attribute_security_service_timeout_seconds", "2");
    properties.setProperty("detail_container_min_degrade_memory_in_bytes", "8192");
    ((MppCommonConfig) EnvFactory.getEnv().getConfig().getDataNodeCommonConfig())
        .updateProperties(properties);
    faults =
        new MetadataFaultController(
            (MppJVMConfig) EnvFactory.getEnv().getConfig().getDataNodeJVMConfig());
    try {
      EnvFactory.getEnv().initClusterEnvironment(1, 2);
      for (DataNodeWrapper wrapper : EnvFactory.getEnv().getDataNodeWrapperList()) {
        NODES.put(wrapper, faults.connect(wrapper));
      }
    } finally {
      faults.restoreLaunchEnvironment();
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    try {
      EnvFactory.getEnv().cleanClusterEnvironment();
    } finally {
      if (faults != null) {
        faults.close();
      }
      NODES.clear();
    }
  }

  @After
  public void cleanUp() throws Exception {
    faults.reset();
    if (database != null) {
      try (Connection connection = EnvFactory.getEnv().getTableConnection();
          Statement statement = connection.createStatement()) {
        statement.execute("DROP DATABASE " + database);
      }
    }
  }

  protected void create(String name, int devices) throws Exception {
    database = name;
    try (Connection connection = connection(EnvFactory.getEnv().getDataNodeWrapperList().get(0));
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + database);
      statement.execute("USE " + database);
      statement.execute(
          "CREATE TABLE sensor (device STRING TAG, changing STRING ATTRIBUTE, kept STRING ATTRIBUTE, reading INT32 FIELD)");
      statement.execute(
          "INSERT INTO sensor(time,device,changing,kept,reading) VALUES(1,'d0','old-0','keep-0',10)");
    }
    DataNodeWrapper remote = remote();
    DataNodeWrapper owner =
        EnvFactory.getEnv().getDataNodeWrapperList().stream()
            .filter(wrapper -> wrapper != remote)
            .findFirst()
            .get();
    // The remaining devices have never been fetched by the remote coordinator before its gate.
    try (Connection connection = connection(owner);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + database);
      for (int i = 1; i < devices; i++) {
        statement.execute(
            "INSERT INTO sensor(time,device,changing,kept,reading) VALUES("
                + (i + 1)
                + ",'d"
                + i
                + "','old-"
                + i
                + "','keep-"
                + i
                + "',"
                + (i + 10)
                + ")");
      }
      statement.execute("FLUSH");
    }
  }

  protected DataNodeWrapper remote() throws SQLException {
    DataNodeWrapper replica = null;
    int regions = 0;
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery("SHOW REGIONS")) {
      while (result.next()) {
        if (TConsensusGroupType.SchemaRegion.toString()
                .equals(result.getString(ColumnHeaderConstant.TYPE))
            && database.equals(result.getString(ColumnHeaderConstant.DATABASE))) {
          regions++;
          replica =
              EnvFactory.getEnv()
                  .dataNodeIdToWrapper(result.getInt(ColumnHeaderConstant.DATA_NODE_ID))
                  .get();
        }
      }
    }
    assertEquals("One sole SchemaRegion replica is required", 1, regions);
    assertNotNull(replica);
    for (DataNodeWrapper wrapper : EnvFactory.getEnv().getDataNodeWrapperList()) {
      if (wrapper != replica) {
        return wrapper;
      }
    }
    throw new AssertionError("No remote cache node");
  }

  protected static Connection connection(DataNodeWrapper wrapper) throws SQLException {
    return EnvFactory.getEnv()
        .getConnection(
            wrapper,
            SessionConfig.DEFAULT_USER,
            SessionConfig.DEFAULT_PASSWORD,
            BaseEnv.TABLE_SQL_DIALECT);
  }

  protected void warm(DataNodeWrapper wrapper, int devices) throws Exception {
    Node node = NODES.get(wrapper);
    node.arm(FILL, "OBSERVE", database);
    try (Connection connection = connection(wrapper);
        Statement statement = connection.createStatement()) {
      for (int i = 0; i < devices; i++) {
        try (ResultSet result =
            statement.executeQuery(
                "SELECT changing, kept, reading FROM "
                    + database
                    + ".sensor WHERE device='d"
                    + i
                    + "'")) {
          TestUtils.assertResultSetEqualUnordered(
              result,
              "changing,kept,reading,",
              java.util.Collections.singletonList(
                  "old-" + i + ",keep-" + i + "," + (i + 10) + ","));
        }
      }
    }
    long fills = Long.parseLong(node.status(FILL).get("matches"));
    assertTrue("The remote queries must actually fetch their attributes", fills >= devices - 1);
    // The same complete keys must now hit the cache whose staleness is under test.
    try (Connection connection = connection(wrapper);
        Statement statement = connection.createStatement()) {
      for (int i = 0; i < devices; i++) {
        try (ResultSet result =
            statement.executeQuery(
                "SELECT changing FROM " + database + ".sensor WHERE device='d" + i + "'")) {
          assertTrue(result.next());
          assertEquals("old-" + i, result.getString(1));
          assertFalse(result.next());
        }
      }
    }
    assertEquals(Long.toString(fills), node.status(FILL).get("matches"));
  }

  protected void update(String sql) throws Exception {
    try (Connection connection = connection(EnvFactory.getEnv().getDataNodeWrapperList().get(0));
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + database);
      statement.execute(sql);
    }
  }

  protected void assertTwoDevices(String first, String second, boolean readded) {
    List<String> attributes = new ArrayList<>();
    attributes.add(first);
    attributes.add(second);
    assertDevices(attributes, readded);
  }

  protected void awaitTwoDevices(String first, String second, boolean readded) {
    Awaitility.await()
        .atMost(20, TimeUnit.SECONDS)
        .untilAsserted(() -> assertTwoDevices(first, second, readded));
  }

  protected void assertDevices(List<String> attributes, boolean readded) {
    List<String> data = new ArrayList<>();
    List<String> devices = new ArrayList<>();
    int nulls = 0;
    for (int i = 0; i < attributes.size(); i++) {
      String attribute = attributes.get(i);
      if (attribute == null) {
        nulls++;
      }
      data.add((i + 1) + ",d" + i + "," + attribute + ",keep-" + i + "," + (i + 10) + ",");
      devices.add(
          "d"
              + i
              + ","
              + (readded ? "keep-" + i + "," + attribute : attribute + ",keep-" + i)
              + ",");
      TestUtils.tableResultSetEqualTestUnordered(
          "SELECT device, changing, kept, reading FROM sensor WHERE device='d" + i + "'",
          new String[] {"device", "changing", "kept", "reading"},
          new String[] {"d" + i + "," + attribute + ",keep-" + i + "," + (i + 10) + ","},
          database);
    }
    TestUtils.tableResultSetEqualTestUnordered(
        "SELECT CAST(time AS INT64) AS time, device, changing, kept, reading FROM sensor",
        new String[] {"time", "device", "changing", "kept", "reading"},
        data.toArray(new String[0]),
        database);
    TestUtils.tableResultSetEqualTestUnordered(
        "SHOW DEVICES FROM sensor",
        readded
            ? new String[] {"device", "kept", "changing"}
            : new String[] {"device", "changing", "kept"},
        devices.toArray(new String[0]),
        database);
    TestUtils.tableResultSetEqualTestUnordered(
        "COUNT DEVICES FROM sensor",
        new String[] {"count(devices)"},
        new String[] {attributes.size() + ","},
        database);
    TestUtils.tableResultSetEqualTestUnordered(
        "COUNT DEVICES FROM sensor WHERE changing IS NULL",
        new String[] {"count(devices)"},
        new String[] {nulls + ","},
        database);
    if (attributes.get(0) != null) {
      assertAttributeFilter(0, attributes.get(0));
    }
    TestUtils.tableResultSetEqualTestUnordered(
        "COUNT DEVICES FROM sensor WHERE changing='late-old'",
        new String[] {"count(devices)"},
        new String[] {"0,"},
        database);
    TestUtils.tableResultSetEqualTest(
        "DESCRIBE sensor",
        new String[] {"ColumnName", "DataType", "Category"},
        readded
            ? new String[] {
              "time,TIMESTAMP,TIME,",
              "device,STRING,TAG,",
              "kept,STRING,ATTRIBUTE,",
              "reading,INT32,FIELD,",
              "changing,STRING,ATTRIBUTE,"
            }
            : new String[] {
              "time,TIMESTAMP,TIME,",
              "device,STRING,TAG,",
              "changing,STRING,ATTRIBUTE,",
              "kept,STRING,ATTRIBUTE,",
              "reading,INT32,FIELD,"
            },
        database);
  }

  protected void assertInvalidationFailure(SQLException failure) {
    assertNotNull("DDL crossed the live node without an ACK", failure);
    assertEquals(
        failure.toString(), TSStatusCode.METADATA_ERROR.getStatusCode(), failure.getErrorCode());
    assertTrue(
        failure.toString(),
        failure
            .getMessage()
            .contains(
                String.format(
                    ProcedureMessages.INVALIDATE_COLUMN_CACHE_FAILED_FOR_TABLE,
                    "changing",
                    database,
                    TABLE)));
  }

  protected void assertAttributeFilter(int device, String attribute) {
    assertAttributePredicate(
        device, attribute == null ? "changing IS NULL" : "changing='" + attribute + "'", true);
    if (attribute == null) {
      for (String predicate :
          new String[] {
            "changing <> 'unmatched'",
            "changing > 'unmatched'",
            "changing IN ('unmatched')",
            "changing NOT IN ('unmatched')",
            "changing LIKE 'unmatched%'",
            "changing NOT LIKE 'unmatched%'",
            "changing <> 'unmatched' OR kept='absent'"
          }) {
        assertAttributePredicate(device, predicate, false);
      }
      assertAttributePredicate(
          device, "changing <> 'unmatched' OR kept='keep-" + device + "'", true);
    }
  }

  private void assertAttributePredicate(int device, String attributePredicate, boolean matches) {
    String predicate = "device='d" + device + "' AND (" + attributePredicate + ")";
    TestUtils.tableResultSetEqualTestUnordered(
        "COUNT DEVICES FROM sensor WHERE " + predicate,
        new String[] {"count(devices)"},
        new String[] {matches ? "1," : "0,"},
        database);
    // Check nullable data predicates after the complete fault/recovery sequence. A query failure
    // must still fail the test, without preventing the delayed batch or later DDL from being
    // tested.
    TestUtils.tableResultSetEqualTestUnordered(
        "SELECT device, kept, reading FROM sensor WHERE " + predicate,
        new String[] {"device", "kept", "reading"},
        matches
            ? new String[] {"d" + device + ",keep-" + device + "," + (device + 10) + ","}
            : new String[0],
        database);
  }

  protected static void assertLease(Node node, Map<String, String> initial) throws Exception {
    Map<String, String> current = node.lease();
    assertEquals(current.toString(), "false", current.get("fenced"));
    assertEquals(
        "Lease recovery must not clear the cache", initial.get("clears"), current.get("clears"));
    assertTrue(
        current.toString(),
        Long.parseLong(current.get("elapsed")) < Long.parseLong(current.get("threshold")));
    assertTrue(
        current.toString(),
        Long.parseLong(current.get("heartbeats")) >= Long.parseLong(initial.get("heartbeats")));
  }

  protected static void awaitMatches(Node node, String hook) {
    Awaitility.await()
        .atMost(20, TimeUnit.SECONDS)
        .until(() -> Boolean.parseBoolean(node.status(hook).get("captured")));
  }

  protected static void awaitFinished(Node node, String hook) {
    Awaitility.await()
        .atMost(10, TimeUnit.SECONDS)
        .until(() -> Long.parseLong(node.status(hook).get("completed")) > 0);
  }

  protected void awaitEmpty(Node sender) {
    Awaitility.await()
        .atMost(20, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                assertEquals(
                    sender.updaters(database).toString(),
                    "0",
                    sender.updaters(database).get("pending")));
  }

  protected static void finish(ExecutorService executor) throws InterruptedException {
    executor.shutdown();
    if (!executor.awaitTermination(35, TimeUnit.SECONDS)) {
      executor.shutdownNow();
      assertTrue("Test executor did not stop", executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }
}
