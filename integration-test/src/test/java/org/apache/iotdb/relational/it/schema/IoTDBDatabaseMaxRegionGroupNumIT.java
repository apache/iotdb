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

import org.apache.iotdb.commons.partition.executor.hash.BKDRHashExecutor;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBDatabaseMaxRegionGroupNumIT {

  private static final int SERIES_SLOT_NUM = 8;
  private static final int DEFAULT_SCHEMA_REGION_GROUP_NUM = 1;
  private static final int DEFAULT_DATA_REGION_GROUP_NUM = 2;
  private static final int MAX_SCHEMA_REGION_GROUP_NUM = 3;
  private static final int MAX_DATA_REGION_GROUP_NUM = 4;
  private static final String TABLE_NAME = "table1";
  private static final BKDRHashExecutor PARTITION_EXECUTOR = new BKDRHashExecutor(SERIES_SLOT_NUM);

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setSchemaRegionGroupExtensionPolicy("CUSTOM")
        .setDataRegionGroupExtensionPolicy("CUSTOM")
        .setDefaultSchemaRegionGroupNumPerDatabase(DEFAULT_SCHEMA_REGION_GROUP_NUM)
        .setDefaultDataRegionGroupNumPerDatabase(DEFAULT_DATA_REGION_GROUP_NUM)
        .setSeriesSlotNum(SERIES_SLOT_NUM)
        .setSeriesPartitionExecutorClass(BKDRHashExecutor.class.getName())
        .setTimePartitionInterval(10);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testCreateAndAlterMaxRegionGroupNum() throws SQLException {
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          "create database test_create with(max_schema_region_group_num=3, max_data_region_group_num=4)");
      TestUtils.assertResultSetEqual(
          statement.executeQuery(
              "select database, max_schema_region_group_num, max_data_region_group_num "
                  + "from information_schema.databases where database = 'test_create'"),
          "database,max_schema_region_group_num,max_data_region_group_num,",
          Collections.singleton("test_create,3,4,"));

      statement.execute("create database test_create_schema with(max_schema_region_group_num=3)");
      TestUtils.assertResultSetEqual(
          statement.executeQuery(
              "select database, max_schema_region_group_num, max_data_region_group_num "
                  + "from information_schema.databases where database = 'test_create_schema'"),
          "database,max_schema_region_group_num,max_data_region_group_num,",
          Collections.singleton("test_create_schema,3," + DEFAULT_DATA_REGION_GROUP_NUM + ","));

      statement.execute("create database test_create_data with(max_data_region_group_num=4)");
      TestUtils.assertResultSetEqual(
          statement.executeQuery(
              "select database, max_schema_region_group_num, max_data_region_group_num "
                  + "from information_schema.databases where database = 'test_create_data'"),
          "database,max_schema_region_group_num,max_data_region_group_num,",
          Collections.singleton("test_create_data," + DEFAULT_SCHEMA_REGION_GROUP_NUM + ",4,"));

      statement.execute("create database test_alter");
      statement.execute(
          "alter database test_alter set properties max_schema_region_group_num=3, max_data_region_group_num=4");
      try (final ResultSet resultSet = statement.executeQuery("show databases details")) {
        boolean found = false;
        while (resultSet.next()) {
          if (!"test_alter".equals(resultSet.getString("Database"))) {
            continue;
          }
          found = true;
          org.junit.Assert.assertEquals(3, resultSet.getInt("MaxSchemaRegionGroupNum"));
          org.junit.Assert.assertEquals(4, resultSet.getInt("MaxDataRegionGroupNum"));
        }
        org.junit.Assert.assertTrue(found);
      }

      Assert.assertThrows(
          SQLException.class,
          () ->
              statement.execute(
                  "create database test_deprecated with(schema_region_group_num=4, data_region_group_num=5)"));
    }
  }

  @Test
  public void testAlterMaxRegionGroupNumCannotDecrease() throws SQLException {
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("create database test_decrease_data with(max_data_region_group_num=8)");
      statement.execute(
          "alter database test_decrease_data set properties max_data_region_group_num=16");

      final SQLException exception =
          Assert.assertThrows(
              SQLException.class,
              () ->
                  statement.execute(
                      "alter database test_decrease_data set properties max_data_region_group_num=12"));
      Assert.assertTrue(
          exception.getMessage(),
          exception
              .getMessage()
              .contains(
                  "MaxDataRegionGroupNum should be greater than or equal to current max "
                      + "DataRegionGroupNum: 16."));

      TestUtils.assertResultSetEqual(
          statement.executeQuery(
              "select database, max_data_region_group_num from information_schema.databases "
                  + "where database = 'test_decrease_data'"),
          "database,max_data_region_group_num,",
          Collections.singleton("test_decrease_data,16,"));

      statement.execute("create database test_decrease_schema with(max_schema_region_group_num=4)");
      statement.execute(
          "alter database test_decrease_schema set properties max_schema_region_group_num=5");

      final SQLException schemaException =
          Assert.assertThrows(
              SQLException.class,
              () ->
                  statement.execute(
                      "alter database test_decrease_schema set properties max_schema_region_group_num=3"));
      Assert.assertTrue(
          schemaException.getMessage(),
          schemaException
              .getMessage()
              .contains(
                  "MaxSchemaRegionGroupNum should be greater than or equal to current max "
                      + "SchemaRegionGroupNum: 5."));

      TestUtils.assertResultSetEqual(
          statement.executeQuery(
              "select database, max_schema_region_group_num from information_schema.databases "
                  + "where database = 'test_decrease_schema'"),
          "database,max_schema_region_group_num,",
          Collections.singleton("test_decrease_schema,5,"));
    }
  }

  @Test
  public void testAllocatedRegionGroupNumEqualsQuotaAfterAlterAndWrite() throws SQLException {
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("create database test_partition");
      statement.execute("use test_partition");
      statement.execute("create table " + TABLE_NAME + "(device string tag, s1 int32 field)");

      final Set<Integer> usedSeriesSlots = new HashSet<>();
      final String firstDevice = getDeviceInNewSeriesSlot(usedSeriesSlots);
      final String secondDevice = getDeviceInNewSeriesSlot(usedSeriesSlots);

      insertData(statement, firstDevice, 0, 0);
      assertRegionGroupNum(
          statement,
          "test_partition",
          DEFAULT_SCHEMA_REGION_GROUP_NUM,
          DEFAULT_SCHEMA_REGION_GROUP_NUM,
          DEFAULT_DATA_REGION_GROUP_NUM,
          DEFAULT_DATA_REGION_GROUP_NUM);

      statement.execute(
          "alter database test_partition set properties max_schema_region_group_num="
              + MAX_SCHEMA_REGION_GROUP_NUM);
      insertData(statement, secondDevice, 0, 1);
      assertAllocatedRegionGroupNumEqualsQuota(
          statement, "test_partition", MAX_SCHEMA_REGION_GROUP_NUM, DEFAULT_DATA_REGION_GROUP_NUM);

      statement.execute(
          "alter database test_partition set properties max_data_region_group_num="
              + MAX_DATA_REGION_GROUP_NUM);
      insertData(statement, secondDevice, 20, 2);
      assertAllocatedRegionGroupNumEqualsQuota(
          statement, "test_partition", MAX_SCHEMA_REGION_GROUP_NUM, MAX_DATA_REGION_GROUP_NUM);

      TestUtils.assertResultSetEqual(
          statement.executeQuery("select count(*) from " + TABLE_NAME),
          "_col0,",
          Collections.singleton("3,"));
    }
  }

  private static void insertData(
      final Statement statement, final String device, final int time, final int value)
      throws SQLException {
    statement.execute(
        "insert into "
            + TABLE_NAME
            + "(time, device, s1) values("
            + time
            + ", '"
            + device
            + "', "
            + value
            + ")");
  }

  private static String getDeviceInNewSeriesSlot(final Set<Integer> usedSeriesSlots) {
    for (int i = 0; i < 1_000; i++) {
      final String device = "d" + i;
      if (usedSeriesSlots.add(getSeriesSlot(device))) {
        return device;
      }
    }
    throw new AssertionError("Failed to find a device in a new series partition slot");
  }

  private static int getSeriesSlot(final String device) {
    return PARTITION_EXECUTOR
        .getSeriesPartitionSlot(
            IDeviceID.Factory.DEFAULT_FACTORY.create(new String[] {TABLE_NAME, device}))
        .getSlotId();
  }

  private static void assertAllocatedRegionGroupNumEqualsQuota(
      final Statement statement,
      final String database,
      final int schemaRegionGroupQuota,
      final int dataRegionGroupQuota)
      throws SQLException {
    assertRegionGroupNum(
        statement,
        database,
        schemaRegionGroupQuota,
        schemaRegionGroupQuota,
        dataRegionGroupQuota,
        dataRegionGroupQuota);
  }

  private static void assertRegionGroupNum(
      final Statement statement,
      final String database,
      final int schemaRegionGroupNum,
      final int maxSchemaRegionGroupNum,
      final int dataRegionGroupNum,
      final int maxDataRegionGroupNum)
      throws SQLException {
    try (final ResultSet resultSet =
        statement.executeQuery(
            "select schema_region_group_num, max_schema_region_group_num, "
                + "data_region_group_num, max_data_region_group_num "
                + "from information_schema.databases where database = '"
                + database
                + "'")) {
      Assert.assertTrue(resultSet.next());
      Assert.assertEquals(schemaRegionGroupNum, resultSet.getInt("schema_region_group_num"));
      Assert.assertEquals(maxSchemaRegionGroupNum, resultSet.getInt("max_schema_region_group_num"));
      Assert.assertEquals(dataRegionGroupNum, resultSet.getInt("data_region_group_num"));
      Assert.assertEquals(maxDataRegionGroupNum, resultSet.getInt("max_data_region_group_num"));
      Assert.assertFalse(resultSet.next());
    }
  }
}
