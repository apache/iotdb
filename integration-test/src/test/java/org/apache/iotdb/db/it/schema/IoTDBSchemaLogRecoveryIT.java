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

package org.apache.iotdb.db.it.schema;

import org.apache.iotdb.commons.utils.KillPoint.MetadataTestKillPoints;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.util.AbstractSchemaIT;
import org.apache.iotdb.util.MetadataRecoveryTestUtils.Gate;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.iotdb.util.MetadataRecoveryTestUtils.*;
import static org.junit.Assert.*;

/** P1-3: schema records, tag indexes and aliases after a forced restart. */
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBSchemaLogRecoveryIT extends AbstractSchemaIT {
  @Parameterized.Parameters(name = "SchemaEngineMode={0}")
  public static Iterable<SchemaTestMode> data() {
    return Collections.singletonList(SchemaTestMode.Memory);
  }

  public IoTDBSchemaLogRecoveryIT(SchemaTestMode mode) {
    super(mode);
  }

  @Parameterized.BeforeParam
  public static void before() throws Exception {
    setUpEnvironment();
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.SIMPLE_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.SIMPLE_CONSENSUS)
        .setSchemaReplicationFactor(1)
        .setDataReplicationFactor(1);
    EnvFactory.getEnv().initClusterEnvironment(1, 1);
  }

  @Parameterized.AfterParam
  public static void after() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
    tearDownEnvironment();
  }

  @After
  public void cleanup() throws Exception {
    if (!node().isAlive()) {
      node().start();
      EnvFactory.getAbstractEnv().checkClusterStatusWithoutUnknown();
    }
    try (Connection cleanup = connection(node(), BaseEnv.TREE_SQL_DIALECT)) {
      List<String> databases = new ArrayList<>();
      try (Statement statement = cleanup.createStatement();
          ResultSet result = statement.executeQuery("SHOW DATABASES")) {
        while (result.next()) {
          if (result.getString(1).startsWith("root.p1_")) {
            databases.add(result.getString(1));
          }
        }
      }
      for (String database : databases) {
        execute(cleanup, "DELETE DATABASE " + database);
      }
      List<String> templates = new ArrayList<>();
      try (Statement statement = cleanup.createStatement();
          ResultSet result = statement.executeQuery("SHOW DEVICE TEMPLATES")) {
        while (result.next()) {
          templates.add(result.getString(1));
        }
      }
      for (String template : templates) {
        execute(cleanup, "DROP DEVICE TEMPLATE " + template);
      }
    }
  }

  @Test
  public void testTagAndAliasIndexesAfterSchemaLogRecovery() throws Exception {
    String database = "root.p1_tags";
    String device = database + ".d";
    try (Connection control = connection(node(), BaseEnv.TREE_SQL_DIALECT)) {
      execute(
          control,
          "CREATE DATABASE " + database,
          "CREATE TIMESERIES "
              + device
              + ".s(old_alias) INT64 ENCODING=PLAIN COMPRESSION=SNAPPY TAGS(old_key='old',removed='gone') ATTRIBUTES(owner='alice',temporary='gone')",
          "CREATE TIMESERIES "
              + device
              + ".reused(dead_alias) TEXT ENCODING=PLAIN COMPRESSION=SNAPPY TAGS(site='deleted')",
          "CREATE TIMESERIES "
              + database
              + ".control.s INT64 ENCODING=PLAIN COMPRESSION=SNAPPY TAGS(site='control') ATTRIBUTES(owner='keep')",
          "INSERT INTO " + device + "(time,s,reused) VALUES(1,10,'old_data')",
          "INSERT INTO " + database + ".control(time,s) VALUES(1,99)",
          "ALTER TIMESERIES " + device + ".s RENAME old_key TO site",
          "ALTER TIMESERIES " + device + ".s SET site='middle',owner='bob'",
          "ALTER TIMESERIES " + device + ".s DROP removed,temporary",
          "ALTER TIMESERIES "
              + device
              + ".s UPSERT ALIAS=new_alias TAGS(site='new') ATTRIBUTES(owner='carol')",
          "DELETE TIMESERIES " + device + ".reused",
          "CREATE TIMESERIES "
              + device
              + ".reused(fresh_alias) DOUBLE ENCODING=PLAIN COMPRESSION=SNAPPY TAGS(site='rebuilt') ATTRIBUTES(owner='dave')",
          "INSERT INTO " + device + "(time,reused) VALUES(3,3.5)",
          "FLUSH");
      assertTagState(control, database);
    }
    // This case deliberately tests the mlog path in both modes, rather than claiming that a
    // small PBTree budget alone proves eviction. A nonempty log and the child-JVM recovery hook
    // establish that the acknowledged mutations went through schema-log recovery.
    try (Stream<Path> files = Files.walk(Paths.get(node().getNodePath()))) {
      assertTrue(
          files.anyMatch(
              path ->
                  path.getFileName().toString().equals("mlog.bin")
                      && path.toString().contains(database)
                      && path.toFile().length() > 0));
    }
    node().stopForcibly();
    try (Gate recovery =
        new Gate(node(), MetadataTestKillPoints.AFTER_SCHEMA_LOG_RECOVERY, false)) {
      node().start();
      recovery.await();
      EnvFactory.getAbstractEnv().checkClusterStatusWithoutUnknown();
    }
    try (Connection recovered = connection(node(), BaseEnv.TREE_SQL_DIALECT)) {
      assertTagState(recovered, database);
      execute(
          recovered,
          "ALTER TIMESERIES " + device + ".s SET site='after_recovery'",
          "INSERT INTO " + device + "(time,new_alias) VALUES(4,40)");
      assertRows(
          recovered,
          "SELECT new_alias FROM " + device,
          "Time," + device + ".new_alias,",
          "1,10,",
          "4,40,");
      assertRows(
          recovered,
          "SHOW TIMESERIES " + database + ".** WHERE TAGS(site)='new'",
          TIMESERIES_HEADER);
      assertRows(
          recovered,
          "SHOW TIMESERIES " + database + ".** WHERE TAGS(site)='after_recovery'",
          TIMESERIES_HEADER,
          device
              + ".s,new_alias,"
              + database
              + ",INT64,PLAIN,SNAPPY,{\"site\":\"after_recovery\"},{\"owner\":\"carol\"},null,null,BASE,");
      execute(recovered, "DELETE TIMESERIES " + device + ".s");
      assertRows(
          recovered,
          "SHOW TIMESERIES " + database + ".** WHERE TAGS(site)='after_recovery'",
          TIMESERIES_HEADER);
      assertRows(
          recovered,
          "SELECT s FROM " + database + ".control",
          "Time," + database + ".control.s,",
          "1,99,");
    }
  }

  private void assertTagState(Connection read, String database) throws Exception {
    String device = database + ".d";
    assertRows(
        read,
        "SHOW TIMESERIES " + database + ".**",
        TIMESERIES_HEADER,
        device
            + ".s,new_alias,"
            + database
            + ",INT64,PLAIN,SNAPPY,{\"site\":\"new\"},{\"owner\":\"carol\"},null,null,BASE,",
        device
            + ".reused,fresh_alias,"
            + database
            + ",DOUBLE,PLAIN,SNAPPY,{\"site\":\"rebuilt\"},{\"owner\":\"dave\"},null,null,BASE,",
        database
            + ".control.s,null,"
            + database
            + ",INT64,PLAIN,SNAPPY,{\"site\":\"control\"},{\"owner\":\"keep\"},null,null,BASE,");
    for (String filter :
        new String[] {"old_key='old'", "site='middle'", "site='deleted'", "removed='gone'"}) {
      String[] pair = filter.split("=", 2);
      assertRows(
          read,
          "SHOW TIMESERIES " + database + ".** WHERE TAGS(" + pair[0] + ")=" + pair[1],
          TIMESERIES_HEADER);
    }
    assertRows(
        read,
        "SHOW TIMESERIES " + database + ".** WHERE TAGS(site)='new'",
        TIMESERIES_HEADER,
        device
            + ".s,new_alias,"
            + database
            + ",INT64,PLAIN,SNAPPY,{\"site\":\"new\"},{\"owner\":\"carol\"},null,null,BASE,");
    assertRows(read, "COUNT TIMESERIES " + database + ".**", "count(timeseries),", "3,");
    assertRows(
        read,
        "SELECT new_alias,fresh_alias FROM " + device,
        "Time," + device + ".new_alias," + device + ".fresh_alias,",
        "1,10,null,",
        "3,null,3.5,");
    assertRows(read, "SELECT old_alias,dead_alias FROM " + device, "Time,");
    assertRows(
        read,
        "SELECT s FROM " + database + ".control",
        "Time," + database + ".control.s,",
        "1,99,");
  }

  private static DataNodeWrapper node() {
    return EnvFactory.getEnv().getDataNodeWrapper(0);
  }
}
