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

package org.apache.iotdb.db.it;

import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.it.utils.TsFileGenerator;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.jdbc.IoTDBSQLException;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.it.utils.TestUtils.assertNonQueryTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.createUser;
import static org.apache.iotdb.db.it.utils.TestUtils.executeNonQuery;
import static org.apache.iotdb.db.it.utils.TestUtils.grantUserSeriesPrivilege;
import static org.apache.iotdb.db.it.utils.TestUtils.grantUserSystemPrivileges;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBLoadTsFileIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBLoadTsFileIT.class);
  private static final long PARTITION_INTERVAL = 10 * 1000L;
  private static final int connectionTimeoutInMS = (int) TimeUnit.SECONDS.toMillis(300);
  private static final long loadTsFileAnalyzeSchemaMemorySizeInBytes = 10 * 1024L;

  private File tmpDir;

  @Before
  public void setUp() throws Exception {
    tmpDir = new File(Files.createTempDirectory("load").toUri());
    EnvFactory.getEnv().getConfig().getCommonConfig().setTimePartitionInterval(PARTITION_INTERVAL);
    EnvFactory.getEnv()
        .getConfig()
        .getDataNodeConfig()
        .setConnectionTimeoutInMS(connectionTimeoutInMS)
        .setLoadTsFileAnalyzeSchemaMemorySizeInBytes(loadTsFileAnalyzeSchemaMemorySizeInBytes);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    deleteSG();
    EnvFactory.getEnv().cleanClusterEnvironment();

    if (!deleteDir()) {
      LOGGER.error("Can not delete tmp dir for loading tsfile.");
    }
  }

  private void registerSchema() throws SQLException {
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {

      statement.execute("CREATE DATABASE " + SchemaConfig.STORAGE_GROUP_0);
      statement.execute("CREATE DATABASE " + SchemaConfig.STORAGE_GROUP_1);

      statement.execute(convert2SQL(SchemaConfig.DEVICE_0, SchemaConfig.MEASUREMENT_00));
      statement.execute(convert2SQL(SchemaConfig.DEVICE_0, SchemaConfig.MEASUREMENT_01));
      statement.execute(convert2SQL(SchemaConfig.DEVICE_0, SchemaConfig.MEASUREMENT_02));
      statement.execute(convert2SQL(SchemaConfig.DEVICE_0, SchemaConfig.MEASUREMENT_03));

      statement.execute(
          convert2AlignedSQL(
              SchemaConfig.DEVICE_1,
              Arrays.asList(
                  SchemaConfig.MEASUREMENT_10,
                  SchemaConfig.MEASUREMENT_11,
                  SchemaConfig.MEASUREMENT_12,
                  SchemaConfig.MEASUREMENT_13,
                  SchemaConfig.MEASUREMENT_14,
                  SchemaConfig.MEASUREMENT_15,
                  SchemaConfig.MEASUREMENT_16,
                  SchemaConfig.MEASUREMENT_17)));

      statement.execute(convert2SQL(SchemaConfig.DEVICE_2, SchemaConfig.MEASUREMENT_20));

      statement.execute(convert2SQL(SchemaConfig.DEVICE_3, SchemaConfig.MEASUREMENT_30));

      statement.execute(
          convert2AlignedSQL(
              SchemaConfig.DEVICE_4, Collections.singletonList(SchemaConfig.MEASUREMENT_40)));
    }
  }

  private String convert2SQL(final String device, final MeasurementSchema schema) {
    final String sql =
        String.format(
            "create timeseries %s %s",
            new Path(device, schema.getMeasurementId(), true).getFullPath(),
            schema.getType().name());
    LOGGER.info("schema execute: {}", sql);
    return sql;
  }

  private String convert2AlignedSQL(final String device, final List<IMeasurementSchema> schemas) {
    String sql = String.format("create aligned timeseries %s(", device);
    for (int i = 0; i < schemas.size(); i++) {
      final IMeasurementSchema schema = schemas.get(i);
      sql += (String.format("%s %s", schema.getMeasurementId(), schema.getType().name()));
      sql += (i == schemas.size() - 1 ? ")" : ",");
    }
    LOGGER.info("schema execute: {}.", sql);
    return sql;
  }

  private void deleteSG() throws SQLException {
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {

      statement.execute(String.format("delete database %s", SchemaConfig.STORAGE_GROUP_0));
      statement.execute(String.format("delete database %s", SchemaConfig.STORAGE_GROUP_1));
    } catch (final IoTDBSQLException ignored) {
    }
  }

  private boolean deleteDir() {
    for (final File file : tmpDir.listFiles()) {
      if (!file.delete()) {
        return false;
      }
    }
    return tmpDir.delete();
  }

  @Test
  public void testLoad() throws Exception {
    registerSchema();

    final long writtenPoint1;
    // device 0, device 1, sg 0
    try (final TsFileGenerator generator =
        new TsFileGenerator(new File(tmpDir, "1-0-0-0.tsfile"))) {
      generator.registerTimeseries(
          SchemaConfig.DEVICE_0,
          Arrays.asList(
              SchemaConfig.MEASUREMENT_00,
              SchemaConfig.MEASUREMENT_01,
              SchemaConfig.MEASUREMENT_02,
              SchemaConfig.MEASUREMENT_03,
              SchemaConfig.MEASUREMENT_04,
              SchemaConfig.MEASUREMENT_05,
              SchemaConfig.MEASUREMENT_06,
              SchemaConfig.MEASUREMENT_07));
      generator.registerAlignedTimeseries(
          SchemaConfig.DEVICE_1,
          Arrays.asList(
              SchemaConfig.MEASUREMENT_10,
              SchemaConfig.MEASUREMENT_11,
              SchemaConfig.MEASUREMENT_12,
              SchemaConfig.MEASUREMENT_13,
              SchemaConfig.MEASUREMENT_14,
              SchemaConfig.MEASUREMENT_15,
              SchemaConfig.MEASUREMENT_16,
              SchemaConfig.MEASUREMENT_17));
      generator.generateData(SchemaConfig.DEVICE_0, 100000, PARTITION_INTERVAL / 10_000, false);
      generator.generateData(SchemaConfig.DEVICE_1, 100000, PARTITION_INTERVAL / 10_000, true);
      writtenPoint1 = generator.getTotalNumber();
    }

    final long writtenPoint2;
    // device 2, device 3, device4, sg 1
    try (final TsFileGenerator generator =
        new TsFileGenerator(new File(tmpDir, "2-0-0-0.tsfile"))) {
      generator.registerTimeseries(
          SchemaConfig.DEVICE_2, Collections.singletonList(SchemaConfig.MEASUREMENT_20));
      generator.registerTimeseries(
          SchemaConfig.DEVICE_3, Collections.singletonList(SchemaConfig.MEASUREMENT_30));
      generator.registerAlignedTimeseries(
          SchemaConfig.DEVICE_4, Collections.singletonList(SchemaConfig.MEASUREMENT_40));
      generator.generateData(SchemaConfig.DEVICE_2, 10000, PARTITION_INTERVAL / 10_000, false);
      generator.generateData(SchemaConfig.DEVICE_3, 10000, PARTITION_INTERVAL / 10_000, false);
      generator.generateData(SchemaConfig.DEVICE_4, 10000, PARTITION_INTERVAL / 10_000, true);
      writtenPoint2 = generator.getTotalNumber();
    }

    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {

      statement.execute(String.format("load \"%s\" sglevel=2", tmpDir.getAbsolutePath()));

      try (final ResultSet resultSet =
          statement.executeQuery("select count(*) from root.** group by level=1,2")) {
        if (resultSet.next()) {
          long sg1Count = resultSet.getLong("count(root.sg.test_0.*.*)");
          Assert.assertEquals(writtenPoint1, sg1Count);
          long sg2Count = resultSet.getLong("count(root.sg.test_1.*.*)");
          Assert.assertEquals(writtenPoint2, sg2Count);
        } else {
          Assert.fail("This ResultSet is empty.");
        }
      }
    }

    // Try to delete after loading. Expect no deadlock
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "delete timeseries %s.%s",
              SchemaConfig.DEVICE_0, SchemaConfig.MEASUREMENT_00.getMeasurementId()));
    }
  }

  @Test
  public void testLoadWithExtendTemplate() throws Exception {
    final long writtenPoint1;
    // device 0, device 1, sg 0
    try (final TsFileGenerator generator =
        new TsFileGenerator(new File(tmpDir, "1-0-0-0.tsfile"))) {
      generator.registerTimeseries(
          SchemaConfig.DEVICE_0,
          Arrays.asList(
              SchemaConfig.MEASUREMENT_00,
              SchemaConfig.MEASUREMENT_01,
              SchemaConfig.MEASUREMENT_02,
              SchemaConfig.MEASUREMENT_03,
              SchemaConfig.MEASUREMENT_04,
              SchemaConfig.MEASUREMENT_05,
              SchemaConfig.MEASUREMENT_06,
              SchemaConfig.MEASUREMENT_07));
      generator.registerAlignedTimeseries(
          SchemaConfig.DEVICE_1,
          Arrays.asList(
              SchemaConfig.MEASUREMENT_10,
              SchemaConfig.MEASUREMENT_11,
              SchemaConfig.MEASUREMENT_12,
              SchemaConfig.MEASUREMENT_13,
              SchemaConfig.MEASUREMENT_14,
              SchemaConfig.MEASUREMENT_15,
              SchemaConfig.MEASUREMENT_16,
              SchemaConfig.MEASUREMENT_17));
      generator.generateData(SchemaConfig.DEVICE_0, 10000, PARTITION_INTERVAL / 10_000, false);
      generator.generateData(SchemaConfig.DEVICE_1, 10000, PARTITION_INTERVAL / 10_000, true);
      writtenPoint1 = generator.getTotalNumber();
    }
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {

      statement.execute("create database root.sg.test_0");
      statement.execute(
          "create device template t1 (lat FLOAT encoding=Gorilla, lon FLOAT encoding=Gorilla)");
      statement.execute(" set device template t1 to root.sg.test_0.d_0");

      statement.execute(String.format("load \"%s\" sglevel=2", tmpDir.getAbsolutePath()));

      try (final ResultSet resultSet =
          statement.executeQuery("select count(*) from root.** group by level=1,2")) {
        if (resultSet.next()) {
          final long sg1Count = resultSet.getLong("count(root.sg.test_0.*.*)");
          Assert.assertEquals(writtenPoint1, sg1Count);
        } else {
          Assert.fail("This ResultSet is empty.");
        }
      }

      final Set<String> nodes =
          new HashSet<>(
              Arrays.asList(
                  "lat",
                  "lon",
                  SchemaConfig.MEASUREMENT_00.getMeasurementId(),
                  SchemaConfig.MEASUREMENT_01.getMeasurementId(),
                  SchemaConfig.MEASUREMENT_02.getMeasurementId(),
                  SchemaConfig.MEASUREMENT_03.getMeasurementId(),
                  SchemaConfig.MEASUREMENT_04.getMeasurementId(),
                  SchemaConfig.MEASUREMENT_05.getMeasurementId(),
                  SchemaConfig.MEASUREMENT_06.getMeasurementId(),
                  SchemaConfig.MEASUREMENT_07.getMeasurementId()));
      try (final ResultSet resultSet = statement.executeQuery("show nodes in schema template t1")) {
        while (resultSet.next()) {
          String device = resultSet.getString(ColumnHeaderConstant.CHILD_NODES);
          Assert.assertTrue(nodes.remove(device));
        }
        Assert.assertTrue(nodes.isEmpty());
      } catch (final Exception e) {
        e.printStackTrace();
        Assert.fail("Parse result set error.");
      }
    }
  }

  @Test
  public void testLoadWithAutoRegister() throws Exception {
    final long writtenPoint1;
    // device 0, device 1, sg 0
    try (final TsFileGenerator generator =
        new TsFileGenerator(new File(tmpDir, "1-0-0-0.tsfile"))) {
      generator.registerTimeseries(
          SchemaConfig.DEVICE_0,
          Arrays.asList(
              SchemaConfig.MEASUREMENT_00,
              SchemaConfig.MEASUREMENT_01,
              SchemaConfig.MEASUREMENT_02,
              SchemaConfig.MEASUREMENT_03,
              SchemaConfig.MEASUREMENT_04,
              SchemaConfig.MEASUREMENT_05,
              SchemaConfig.MEASUREMENT_06,
              SchemaConfig.MEASUREMENT_07));
      generator.registerAlignedTimeseries(
          SchemaConfig.DEVICE_1,
          Arrays.asList(
              SchemaConfig.MEASUREMENT_10,
              SchemaConfig.MEASUREMENT_11,
              SchemaConfig.MEASUREMENT_12,
              SchemaConfig.MEASUREMENT_13,
              SchemaConfig.MEASUREMENT_14,
              SchemaConfig.MEASUREMENT_15,
              SchemaConfig.MEASUREMENT_16,
              SchemaConfig.MEASUREMENT_17));
      generator.generateData(SchemaConfig.DEVICE_0, 10000, PARTITION_INTERVAL / 10_000, false);
      generator.generateData(SchemaConfig.DEVICE_1, 10000, PARTITION_INTERVAL / 10_000, true);
      writtenPoint1 = generator.getTotalNumber();
    }

    final long writtenPoint2;
    // device 2, device 3, device4, sg 1
    try (final TsFileGenerator generator =
        new TsFileGenerator(new File(tmpDir, "2-0-0-0.tsfile"))) {
      generator.registerTimeseries(
          SchemaConfig.DEVICE_2, Collections.singletonList(SchemaConfig.MEASUREMENT_20));
      generator.registerTimeseries(
          SchemaConfig.DEVICE_3, Collections.singletonList(SchemaConfig.MEASUREMENT_30));
      generator.registerAlignedTimeseries(
          SchemaConfig.DEVICE_4, Collections.singletonList(SchemaConfig.MEASUREMENT_40));
      generator.generateData(SchemaConfig.DEVICE_2, 10000, PARTITION_INTERVAL / 10_000, false);
      generator.generateData(SchemaConfig.DEVICE_3, 10000, PARTITION_INTERVAL / 10_000, false);
      generator.generateData(SchemaConfig.DEVICE_4, 10000, PARTITION_INTERVAL / 10_000, true);
      writtenPoint2 = generator.getTotalNumber();
    }

    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {

      statement.execute(String.format("load \"%s\" sglevel=2", tmpDir.getAbsolutePath()));

      try (final ResultSet resultSet =
          statement.executeQuery("select count(*) from root.** group by level=1,2")) {
        if (resultSet.next()) {
          final long sg1Count = resultSet.getLong("count(root.sg.test_0.*.*)");
          Assert.assertEquals(writtenPoint1, sg1Count);
          final long sg2Count = resultSet.getLong("count(root.sg.test_1.*.*)");
          Assert.assertEquals(writtenPoint2, sg2Count);
        } else {
          Assert.fail("This ResultSet is empty.");
        }
      }

      final Map<String, String> isAligned = new HashMap<>();
      isAligned.put(SchemaConfig.DEVICE_0, "false");
      isAligned.put(SchemaConfig.DEVICE_1, "true");
      isAligned.put(SchemaConfig.DEVICE_2, "false");
      isAligned.put(SchemaConfig.DEVICE_3, "false");
      isAligned.put(SchemaConfig.DEVICE_4, "true");
      try (final ResultSet resultSet = statement.executeQuery("show devices")) {
        int size = 0;
        while (resultSet.next()) {
          size += 1;
          String device = resultSet.getString(ColumnHeaderConstant.DEVICE);
          Assert.assertEquals(
              isAligned.get(device), resultSet.getString(ColumnHeaderConstant.IS_ALIGNED));
        }
        Assert.assertEquals(isAligned.size(), size);
      } catch (final Exception e) {
        e.printStackTrace();
        Assert.fail("Parse result set error.");
      }
    }
  }

  @Test
  public void testAuth() throws Exception {
    createUser("test", "test123");

    // device 0, device 1, sg 0
    try (final TsFileGenerator generator =
        new TsFileGenerator(new File(tmpDir, "test1-0-0-0.tsfile"))) {
      generator.registerTimeseries(
          SchemaConfig.DEVICE_0,
          Arrays.asList(
              SchemaConfig.MEASUREMENT_00,
              SchemaConfig.MEASUREMENT_01,
              SchemaConfig.MEASUREMENT_02,
              SchemaConfig.MEASUREMENT_03,
              SchemaConfig.MEASUREMENT_04,
              SchemaConfig.MEASUREMENT_05,
              SchemaConfig.MEASUREMENT_06,
              SchemaConfig.MEASUREMENT_07));
      generator.registerAlignedTimeseries(
          SchemaConfig.DEVICE_1,
          Arrays.asList(
              SchemaConfig.MEASUREMENT_10,
              SchemaConfig.MEASUREMENT_11,
              SchemaConfig.MEASUREMENT_12,
              SchemaConfig.MEASUREMENT_13,
              SchemaConfig.MEASUREMENT_14,
              SchemaConfig.MEASUREMENT_15,
              SchemaConfig.MEASUREMENT_16,
              SchemaConfig.MEASUREMENT_17));
      generator.generateData(SchemaConfig.DEVICE_0, 10000, PARTITION_INTERVAL / 10_000, false);
      generator.generateData(SchemaConfig.DEVICE_1, 10000, PARTITION_INTERVAL / 10_000, true);
    }

    // device 2, device 3, device4, sg 1
    try (final TsFileGenerator generator =
        new TsFileGenerator(new File(tmpDir, "test2-0-0-0.tsfile"))) {
      generator.registerTimeseries(
          SchemaConfig.DEVICE_2, Collections.singletonList(SchemaConfig.MEASUREMENT_20));
      generator.registerTimeseries(
          SchemaConfig.DEVICE_3, Collections.singletonList(SchemaConfig.MEASUREMENT_30));
      generator.registerAlignedTimeseries(
          SchemaConfig.DEVICE_4, Collections.singletonList(SchemaConfig.MEASUREMENT_40));
      generator.generateData(SchemaConfig.DEVICE_2, 10000, PARTITION_INTERVAL / 10_000, false);
      generator.generateData(SchemaConfig.DEVICE_3, 10000, PARTITION_INTERVAL / 10_000, false);
      generator.generateData(SchemaConfig.DEVICE_4, 10000, PARTITION_INTERVAL / 10_000, true);
    }

    assertNonQueryTestFail(
        String.format("load \"%s\" sgLevel=2", tmpDir.getAbsolutePath()),
        "No permissions for this operation, please add privilege WRITE_DATA",
        "test",
        "test123");

    grantUserSeriesPrivilege("test", PrivilegeType.WRITE_DATA, "root.**");

    assertNonQueryTestFail(
        String.format("load \"%s\" sgLevel=2", tmpDir.getAbsolutePath()),
        "No permissions for this operation, please add privilege MANAGE_DATABASE",
        "test",
        "test123");

    grantUserSystemPrivileges("test", PrivilegeType.MANAGE_DATABASE);

    assertNonQueryTestFail(
        String.format("load \"%s\" sgLevel=2", tmpDir.getAbsolutePath()),
        "Auto create or verify schema error when executing statement LoadTsFileStatement",
        "test",
        "test123");

    grantUserSystemPrivileges("test", PrivilegeType.WRITE_SCHEMA);

    executeNonQuery(
        String.format("load \"%s\" sgLevel=2", tmpDir.getAbsolutePath()), "test", "test123");
  }

  @Test
  public void testLoadWithOnSuccess() throws Exception {
    final File file1 = new File(tmpDir, "1-0-0-0.tsfile");
    final File file2 = new File(tmpDir, "2-0-0-0.tsfile");
    long writtenPoint1 = 0;
    // device 0, device 1, sg 0
    try (final TsFileGenerator generator = new TsFileGenerator(file1)) {
      generator.registerTimeseries(
          SchemaConfig.DEVICE_0,
          Arrays.asList(
              SchemaConfig.MEASUREMENT_00,
              SchemaConfig.MEASUREMENT_01,
              SchemaConfig.MEASUREMENT_02,
              SchemaConfig.MEASUREMENT_03,
              SchemaConfig.MEASUREMENT_04,
              SchemaConfig.MEASUREMENT_05,
              SchemaConfig.MEASUREMENT_06,
              SchemaConfig.MEASUREMENT_07));
      generator.registerAlignedTimeseries(
          SchemaConfig.DEVICE_1,
          Arrays.asList(
              SchemaConfig.MEASUREMENT_10,
              SchemaConfig.MEASUREMENT_11,
              SchemaConfig.MEASUREMENT_12,
              SchemaConfig.MEASUREMENT_13,
              SchemaConfig.MEASUREMENT_14,
              SchemaConfig.MEASUREMENT_15,
              SchemaConfig.MEASUREMENT_16,
              SchemaConfig.MEASUREMENT_17));
      generator.generateData(SchemaConfig.DEVICE_0, 10000, PARTITION_INTERVAL / 10_000, false);
      generator.generateData(SchemaConfig.DEVICE_1, 10000, PARTITION_INTERVAL / 10_000, true);
      writtenPoint1 = generator.getTotalNumber();
    }

    final long writtenPoint2;
    // device 2, device 3, device4, sg 1
    try (final TsFileGenerator generator = new TsFileGenerator(file2)) {
      generator.registerTimeseries(
          SchemaConfig.DEVICE_2, Arrays.asList(SchemaConfig.MEASUREMENT_20));
      generator.registerTimeseries(
          SchemaConfig.DEVICE_3, Arrays.asList(SchemaConfig.MEASUREMENT_30));
      generator.registerAlignedTimeseries(
          SchemaConfig.DEVICE_4, Arrays.asList(SchemaConfig.MEASUREMENT_40));
      generator.generateData(SchemaConfig.DEVICE_2, 10000, PARTITION_INTERVAL / 10_000, false);
      generator.generateData(SchemaConfig.DEVICE_3, 10000, PARTITION_INTERVAL / 10_000, false);
      generator.generateData(SchemaConfig.DEVICE_4, 10000, PARTITION_INTERVAL / 10_000, true);
      writtenPoint2 = generator.getTotalNumber();
    }

    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {

      statement.execute(
          String.format("load \"%s\" sglevel=2 onSuccess=none", file1.getAbsolutePath()));

      try (final ResultSet resultSet =
          statement.executeQuery("select count(*) from root.** group by level=1,2")) {
        if (resultSet.next()) {
          final long sg1Count = resultSet.getLong("count(root.sg.test_0.*.*)");
          Assert.assertEquals(writtenPoint1, sg1Count);
        } else {
          Assert.fail("This ResultSet is empty.");
        }
      }
      Assert.assertTrue(file1.exists());
    }

    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {

      statement.execute(
          String.format("load \"%s\" sglevel=2 onSuccess=delete", file2.getAbsolutePath()));

      try (final ResultSet resultSet =
          statement.executeQuery("select count(*) from root.** group by level=1,2")) {
        if (resultSet.next()) {
          long sg1Count = resultSet.getLong("count(root.sg.test_0.*.*)");
          Assert.assertEquals(writtenPoint1, sg1Count);
          long sg2Count = resultSet.getLong("count(root.sg.test_1.*.*)");
          Assert.assertEquals(writtenPoint2, sg2Count);
        } else {
          Assert.fail("This ResultSet is empty.");
        }
      }
      Assert.assertFalse(file2.exists());
    }
  }

  @Test
  public void testLoadWithLastCache() throws Exception {
    registerSchema();

    final String device = SchemaConfig.DEVICE_0;
    final String measurement = SchemaConfig.MEASUREMENT_00.getMeasurementId();

    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {

      statement.execute(
          String.format("insert into %s(timestamp, %s) values(100, 100)", device, measurement));

      try (final ResultSet resultSet =
          statement.executeQuery(String.format("select last %s from %s", measurement, device))) {
        if (resultSet.next()) {
          final String lastValue = resultSet.getString(ColumnHeaderConstant.VALUE);
          Assert.assertEquals("100", lastValue);
        } else {
          Assert.fail("This ResultSet is empty.");
        }
      }
    }

    final File file1 = new File(tmpDir, "1-0-0-0.tsfile");
    final File file2 = new File(tmpDir, "2-0-0-0.tsfile");
    // device 0, device 1, sg 0
    try (final TsFileGenerator generator = new TsFileGenerator(file1)) {
      generator.registerTimeseries(
          SchemaConfig.DEVICE_0,
          Arrays.asList(
              SchemaConfig.MEASUREMENT_00,
              SchemaConfig.MEASUREMENT_01,
              SchemaConfig.MEASUREMENT_02,
              SchemaConfig.MEASUREMENT_03,
              SchemaConfig.MEASUREMENT_04,
              SchemaConfig.MEASUREMENT_05,
              SchemaConfig.MEASUREMENT_06,
              SchemaConfig.MEASUREMENT_07));
      generator.registerAlignedTimeseries(
          SchemaConfig.DEVICE_1,
          Arrays.asList(
              SchemaConfig.MEASUREMENT_10,
              SchemaConfig.MEASUREMENT_11,
              SchemaConfig.MEASUREMENT_12,
              SchemaConfig.MEASUREMENT_13,
              SchemaConfig.MEASUREMENT_14,
              SchemaConfig.MEASUREMENT_15,
              SchemaConfig.MEASUREMENT_16,
              SchemaConfig.MEASUREMENT_17));
      generator.generateData(SchemaConfig.DEVICE_0, 10000, PARTITION_INTERVAL / 10_000, false);
      generator.generateData(SchemaConfig.DEVICE_1, 10000, PARTITION_INTERVAL / 10_000, true);
    }

    // device 2, device 3, device4, sg 1
    try (final TsFileGenerator generator = new TsFileGenerator(file2)) {
      generator.registerTimeseries(
          SchemaConfig.DEVICE_2, Collections.singletonList(SchemaConfig.MEASUREMENT_20));
      generator.registerTimeseries(
          SchemaConfig.DEVICE_3, Collections.singletonList(SchemaConfig.MEASUREMENT_30));
      generator.registerAlignedTimeseries(
          SchemaConfig.DEVICE_4, Collections.singletonList(SchemaConfig.MEASUREMENT_40));
      generator.generateData(SchemaConfig.DEVICE_2, 10000, PARTITION_INTERVAL / 10_000, false);
      generator.generateData(SchemaConfig.DEVICE_3, 10000, PARTITION_INTERVAL / 10_000, false);
      generator.generateData(SchemaConfig.DEVICE_4, 10000, PARTITION_INTERVAL / 10_000, true);
    }

    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {

      statement.execute(String.format("load \"%s\" sglevel=2", tmpDir.getAbsolutePath()));

      try (final ResultSet resultSet =
          statement.executeQuery(String.format("select last %s from %s", measurement, device))) {
        if (resultSet.next()) {
          final String lastTime = resultSet.getString(ColumnHeaderConstant.TIME);
          Assert.assertEquals(String.valueOf(PARTITION_INTERVAL), lastTime);
        } else {
          Assert.fail("This ResultSet is empty.");
        }
      }
    }
  }

  @Test
  public void testLoadWithOnNonStandardTsFileName() throws Exception {
    final File file1 = new File(tmpDir, "1-0-0-0.tsfile");
    final File file2 = new File(tmpDir, "a-1.tsfile");
    final long writtenPoint1;
    // device 0, device 1, sg 0
    try (final TsFileGenerator generator = new TsFileGenerator(file1)) {
      generator.registerTimeseries(
          SchemaConfig.DEVICE_0,
          Arrays.asList(
              SchemaConfig.MEASUREMENT_00,
              SchemaConfig.MEASUREMENT_01,
              SchemaConfig.MEASUREMENT_02,
              SchemaConfig.MEASUREMENT_03,
              SchemaConfig.MEASUREMENT_04,
              SchemaConfig.MEASUREMENT_05,
              SchemaConfig.MEASUREMENT_06,
              SchemaConfig.MEASUREMENT_07));
      generator.registerAlignedTimeseries(
          SchemaConfig.DEVICE_1,
          Arrays.asList(
              SchemaConfig.MEASUREMENT_10,
              SchemaConfig.MEASUREMENT_11,
              SchemaConfig.MEASUREMENT_12,
              SchemaConfig.MEASUREMENT_13,
              SchemaConfig.MEASUREMENT_14,
              SchemaConfig.MEASUREMENT_15,
              SchemaConfig.MEASUREMENT_16,
              SchemaConfig.MEASUREMENT_17));
      generator.generateData(SchemaConfig.DEVICE_0, 10000, PARTITION_INTERVAL / 10_000, false);
      generator.generateData(SchemaConfig.DEVICE_1, 10000, PARTITION_INTERVAL / 10_000, true);
      writtenPoint1 = generator.getTotalNumber();
    }

    final long writtenPoint2;
    // device 2, device 3, device4, sg 1
    try (final TsFileGenerator generator = new TsFileGenerator(file2)) {
      generator.registerTimeseries(
          SchemaConfig.DEVICE_2, Collections.singletonList(SchemaConfig.MEASUREMENT_20));
      generator.registerTimeseries(
          SchemaConfig.DEVICE_3, Collections.singletonList(SchemaConfig.MEASUREMENT_30));
      generator.registerAlignedTimeseries(
          SchemaConfig.DEVICE_4, Collections.singletonList(SchemaConfig.MEASUREMENT_40));
      generator.generateData(SchemaConfig.DEVICE_2, 10000, PARTITION_INTERVAL / 10_000, false);
      generator.generateData(SchemaConfig.DEVICE_3, 10000, PARTITION_INTERVAL / 10_000, false);
      generator.generateData(SchemaConfig.DEVICE_4, 10000, PARTITION_INTERVAL / 10_000, true);
      writtenPoint2 = generator.getTotalNumber();
    }

    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {

      statement.execute(String.format("load \"%s\" sglevel=2", tmpDir.getAbsolutePath()));

      try (final ResultSet resultSet =
          statement.executeQuery("select count(*) from root.** group by level=1,2")) {
        if (resultSet.next()) {
          final long sg1Count = resultSet.getLong("count(root.sg.test_0.*.*)");
          Assert.assertEquals(writtenPoint1, sg1Count);
          final long sg2Count = resultSet.getLong("count(root.sg.test_1.*.*)");
          Assert.assertEquals(writtenPoint2, sg2Count);
        } else {
          Assert.fail("This ResultSet is empty.");
        }
      }
    }
  }

  @Test
  public void testLoadWithMods() throws Exception {
    final long writtenPoint1;
    // device 0, device 1, sg 0
    try (final TsFileGenerator generator =
        new TsFileGenerator(new File(tmpDir, "1-0-0-0.tsfile"))) {
      generator.registerTimeseries(
          SchemaConfig.DEVICE_0,
          Arrays.asList(
              SchemaConfig.MEASUREMENT_00,
              SchemaConfig.MEASUREMENT_01,
              SchemaConfig.MEASUREMENT_02,
              SchemaConfig.MEASUREMENT_03,
              SchemaConfig.MEASUREMENT_04,
              SchemaConfig.MEASUREMENT_05,
              SchemaConfig.MEASUREMENT_06,
              SchemaConfig.MEASUREMENT_07));
      generator.registerAlignedTimeseries(
          SchemaConfig.DEVICE_1,
          Arrays.asList(
              SchemaConfig.MEASUREMENT_10,
              SchemaConfig.MEASUREMENT_11,
              SchemaConfig.MEASUREMENT_12,
              SchemaConfig.MEASUREMENT_13,
              SchemaConfig.MEASUREMENT_14,
              SchemaConfig.MEASUREMENT_15,
              SchemaConfig.MEASUREMENT_16,
              SchemaConfig.MEASUREMENT_17));
      generator.generateData(SchemaConfig.DEVICE_0, 100000, PARTITION_INTERVAL / 10_000, false);
      generator.generateData(SchemaConfig.DEVICE_1, 100000, PARTITION_INTERVAL / 10_000, true);
      generator.generateDeletion(SchemaConfig.DEVICE_0, 10);
      writtenPoint1 = generator.getTotalNumber();
    }

    final long writtenPoint2;
    // device 2, device 3, device4, sg 1
    try (final TsFileGenerator generator =
        new TsFileGenerator(new File(tmpDir, "2-0-0-0.tsfile"))) {
      generator.resetRandom(1000);
      generator.registerTimeseries(
          SchemaConfig.DEVICE_2, Collections.singletonList(SchemaConfig.MEASUREMENT_20));
      generator.registerTimeseries(
          SchemaConfig.DEVICE_3, Collections.singletonList(SchemaConfig.MEASUREMENT_30));
      generator.registerAlignedTimeseries(
          SchemaConfig.DEVICE_4, Collections.singletonList(SchemaConfig.MEASUREMENT_40));
      generator.generateData(SchemaConfig.DEVICE_2, 100, PARTITION_INTERVAL / 10_000, false);
      generator.generateData(SchemaConfig.DEVICE_3, 100, PARTITION_INTERVAL / 10_000, false);
      generator.generateData(SchemaConfig.DEVICE_4, 100, PARTITION_INTERVAL / 10_000, true);
      generator.generateDeletion(SchemaConfig.DEVICE_2, 2);
      generator.generateDeletion(SchemaConfig.DEVICE_4, 2);
      generator.generateData(SchemaConfig.DEVICE_2, 100, PARTITION_INTERVAL / 10_000, false);
      generator.generateData(SchemaConfig.DEVICE_4, 100, PARTITION_INTERVAL / 10_000, true);
      generator.generateDeletion(SchemaConfig.DEVICE_2, 2);
      generator.generateDeletion(SchemaConfig.DEVICE_4, 2);
      writtenPoint2 = generator.getTotalNumber();
    }

    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {

      statement.execute(String.format("load \"%s\" sglevel=2", tmpDir.getAbsolutePath()));

      try (final ResultSet resultSet =
          statement.executeQuery("select count(*) from root.** group by level=1,2")) {
        if (resultSet.next()) {
          final long sg1Count = resultSet.getLong("count(root.sg.test_0.*.*)");
          Assert.assertEquals(writtenPoint1, sg1Count);
          final long sg2Count = resultSet.getLong("count(root.sg.test_1.*.*)");
          Assert.assertEquals(writtenPoint2, sg2Count);
        } else {
          Assert.fail("This ResultSet is empty.");
        }
      }
    }
  }

  @Test
  public void testLoadWithEmptyTsFile() throws Exception {
    try (final TsFileGenerator ignored = new TsFileGenerator(new File(tmpDir, "1-0-0-0.tsfile"))) {}

    try (final Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      statement.execute(String.format("load \"%s\"", tmpDir.getAbsolutePath()));

      try (final ResultSet resultSet = statement.executeQuery("show timeseries")) {
        Assert.assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testLoadTsFileWithWrongTimestampPrecision() throws Exception {
    try (final TsFileGenerator generator =
        new TsFileGenerator(new File(tmpDir, "1-0-0-0.tsfile"))) {
      generator.registerTimeseries(
          SchemaConfig.DEVICE_0,
          Arrays.asList(
              SchemaConfig.MEASUREMENT_00,
              SchemaConfig.MEASUREMENT_01,
              SchemaConfig.MEASUREMENT_02,
              SchemaConfig.MEASUREMENT_03,
              SchemaConfig.MEASUREMENT_04,
              SchemaConfig.MEASUREMENT_05,
              SchemaConfig.MEASUREMENT_06,
              SchemaConfig.MEASUREMENT_07));
      generator.registerAlignedTimeseries(
          SchemaConfig.DEVICE_1,
          Arrays.asList(
              SchemaConfig.MEASUREMENT_10,
              SchemaConfig.MEASUREMENT_11,
              SchemaConfig.MEASUREMENT_12,
              SchemaConfig.MEASUREMENT_13,
              SchemaConfig.MEASUREMENT_14,
              SchemaConfig.MEASUREMENT_15,
              SchemaConfig.MEASUREMENT_16,
              SchemaConfig.MEASUREMENT_17));
      // generate ns timestamp
      generator.generateData(
          SchemaConfig.DEVICE_0, 100000, PARTITION_INTERVAL / 10_000, false, 1694689856546000000L);
      generator.generateData(
          SchemaConfig.DEVICE_1, 100000, PARTITION_INTERVAL / 10_000, true, 1694689856546000000L);
    }

    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {

      statement.execute(String.format("load \"%s\"", tmpDir.getAbsolutePath()));
    } catch (final IoTDBSQLException e) {
      Assert.assertTrue(e.getMessage().contains("Current system timestamp precision is ms"));
    }
  }

  @Test
  public void testLoadLocally() throws Exception {
    registerSchema();

    final long writtenPoint1;
    // device 0, device 1, sg 0
    try (final TsFileGenerator generator =
        new TsFileGenerator(new File(tmpDir, "1-0-0-0.tsfile"))) {
      generator.registerTimeseries(
          SchemaConfig.DEVICE_0, Collections.singletonList(SchemaConfig.MEASUREMENT_00));
      generator.generateData(SchemaConfig.DEVICE_0, 1, PARTITION_INTERVAL / 10_000, false);
      writtenPoint1 = generator.getTotalNumber();
    }

    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {

      statement.execute(String.format("load \"%s\" sglevel=2", tmpDir.getAbsolutePath()));

      try (final ResultSet resultSet =
          statement.executeQuery("select count(*) from root.** group by level=1,2")) {
        if (resultSet.next()) {
          final long sg1Count = resultSet.getLong("count(root.sg.test_0.*.*)");
          Assert.assertEquals(writtenPoint1, sg1Count);
        } else {
          Assert.fail("This ResultSet is empty.");
        }
      }
    }
  }

  private static class SchemaConfig {
    private static final String STORAGE_GROUP_0 = "root.sg.test_0";
    private static final String STORAGE_GROUP_1 = "root.sg.test_1";

    // device 0, nonaligned, sg 0
    private static final String DEVICE_0 = "root.sg.test_0.d_0";
    private static final MeasurementSchema MEASUREMENT_00 =
        new MeasurementSchema("sensor_00", TSDataType.INT32, TSEncoding.RLE);
    private static final MeasurementSchema MEASUREMENT_01 =
        new MeasurementSchema("sensor_01", TSDataType.INT64, TSEncoding.RLE);
    private static final MeasurementSchema MEASUREMENT_02 =
        new MeasurementSchema("sensor_02", TSDataType.DOUBLE, TSEncoding.GORILLA);
    private static final MeasurementSchema MEASUREMENT_03 =
        new MeasurementSchema("sensor_03", TSDataType.TEXT, TSEncoding.PLAIN);
    private static final MeasurementSchema MEASUREMENT_04 =
        new MeasurementSchema("sensor_04", TSDataType.TIMESTAMP, TSEncoding.RLE);
    private static final MeasurementSchema MEASUREMENT_05 =
        new MeasurementSchema("sensor_05", TSDataType.DATE, TSEncoding.RLE);
    private static final MeasurementSchema MEASUREMENT_06 =
        new MeasurementSchema("sensor_06", TSDataType.BLOB, TSEncoding.PLAIN);
    private static final MeasurementSchema MEASUREMENT_07 =
        new MeasurementSchema("sensor_07", TSDataType.STRING, TSEncoding.PLAIN);

    // device 1, aligned, sg 0
    private static final String DEVICE_1 = "root.sg.test_0.a_1";
    private static final MeasurementSchema MEASUREMENT_10 =
        new MeasurementSchema("sensor_10", TSDataType.INT32, TSEncoding.RLE);
    private static final MeasurementSchema MEASUREMENT_11 =
        new MeasurementSchema("sensor_11", TSDataType.INT64, TSEncoding.RLE);
    private static final MeasurementSchema MEASUREMENT_12 =
        new MeasurementSchema("sensor_12", TSDataType.DOUBLE, TSEncoding.GORILLA);
    private static final MeasurementSchema MEASUREMENT_13 =
        new MeasurementSchema("sensor_13", TSDataType.TEXT, TSEncoding.PLAIN);
    private static final MeasurementSchema MEASUREMENT_14 =
        new MeasurementSchema("sensor_14", TSDataType.TIMESTAMP, TSEncoding.RLE);
    private static final MeasurementSchema MEASUREMENT_15 =
        new MeasurementSchema("sensor_15", TSDataType.DATE, TSEncoding.RLE);
    private static final MeasurementSchema MEASUREMENT_16 =
        new MeasurementSchema("sensor_16", TSDataType.BLOB, TSEncoding.PLAIN);
    private static final MeasurementSchema MEASUREMENT_17 =
        new MeasurementSchema("sensor_17", TSDataType.STRING, TSEncoding.PLAIN);

    // device 2, non aligned, sg 1
    private static final String DEVICE_2 = "root.sg.test_1.d_2";
    private static final MeasurementSchema MEASUREMENT_20 =
        new MeasurementSchema("sensor_20", TSDataType.INT32, TSEncoding.RLE);

    // device 3, non aligned, sg 1
    private static final String DEVICE_3 = "root.sg.test_1.d_3";
    private static final MeasurementSchema MEASUREMENT_30 =
        new MeasurementSchema("sensor_30", TSDataType.INT32, TSEncoding.RLE);

    // device 4, aligned, sg 1
    private static final String DEVICE_4 = "root.sg.test_1.a_4";
    private static final MeasurementSchema MEASUREMENT_40 =
        new MeasurementSchema("sensor_40", TSDataType.INT32, TSEncoding.RLE);
  }
}
