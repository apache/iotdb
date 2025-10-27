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

package org.apache.iotdb.relational.it.db.it;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.it.utils.TsFileTableGenerator;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
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
    EnvFactory.getEnv().getConfig().getCommonConfig().setEnforceStrongPassword(false);
    EnvFactory.getEnv()
        .getConfig()
        .getDataNodeConfig()
        .setConnectionTimeoutInMS(connectionTimeoutInMS)
        .setLoadTsFileAnalyzeSchemaMemorySizeInBytes(loadTsFileAnalyzeSchemaMemorySizeInBytes);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();

    if (!deleteDir()) {
      LOGGER.error("Can not delete tmp dir for loading tsfile.");
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

  private List<Pair<MeasurementSchema, MeasurementSchema>> generateMeasurementSchemas() {
    TSDataType[] dataTypes = {
      TSDataType.STRING,
      TSDataType.TEXT,
      TSDataType.BLOB,
      TSDataType.TIMESTAMP,
      TSDataType.BOOLEAN,
      TSDataType.DATE,
      TSDataType.DOUBLE,
      TSDataType.FLOAT,
      TSDataType.INT32,
      TSDataType.INT64
    };
    List<Pair<MeasurementSchema, MeasurementSchema>> pairs = new ArrayList<>();

    for (TSDataType type : dataTypes) {
      for (TSDataType dataType : dataTypes) {
        String id = String.format("%s2%s", type.name(), dataType.name());
        pairs.add(new Pair<>(new MeasurementSchema(id, type), new MeasurementSchema(id, dataType)));
      }
    }
    return pairs;
  }

  @Test
  public void testLoadWithEmptyDatabaseForTableModel() throws Exception {
    final int lineCount = 10000;

    final List<Pair<MeasurementSchema, MeasurementSchema>> measurementSchemas =
        generateMeasurementSchemas();
    final List<ColumnCategory> columnCategories =
        generateTabletColumnCategory(0, measurementSchemas.size());

    final File file = new File(tmpDir, "1-0-0-0.tsfile");

    final List<IMeasurementSchema> schemaList =
        measurementSchemas.stream().map(pair -> pair.right).collect(Collectors.toList());

    try (final TsFileTableGenerator generator = new TsFileTableGenerator(file)) {
      generator.registerTable(SchemaConfig.TABLE_0, schemaList, columnCategories);

      generator.generateData(SchemaConfig.TABLE_0, lineCount, PARTITION_INTERVAL / 10_000);
    }

    // Prepare normal user
    try (final Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("create user test 'password123456'");
      adminStmt.execute(
          String.format(
              "grant create, insert on %s.%s to user test",
              SchemaConfig.DATABASE_0, SchemaConfig.TABLE_0));

      // auto-create table
      adminStmt.execute(String.format("create database if not exists %s", SchemaConfig.DATABASE_0));
    }

    try (final Connection connection =
            EnvFactory.getEnv().getConnection("test", "password123456", BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(String.format("use %s", SchemaConfig.DATABASE_0));
      statement.execute(String.format("load '%s'", file.getAbsolutePath()));
    }

    try (final Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute(String.format("use %s", SchemaConfig.DATABASE_0));
      try (final ResultSet resultSet =
          adminStmt.executeQuery(String.format("select count(*) from %s", SchemaConfig.TABLE_0))) {
        if (resultSet.next()) {
          Assert.assertEquals(lineCount, resultSet.getLong(1));
        } else {
          Assert.fail("This ResultSet is empty.");
        }
      }
    }
  }

  @Test
  @Ignore("Load with conversion is currently banned")
  public void testLoadWithConvertOnTypeMismatchForTableModel() throws Exception {
    final int lineCount = 10000;

    List<Pair<MeasurementSchema, MeasurementSchema>> measurementSchemas =
        generateMeasurementSchemas();
    List<ColumnCategory> columnCategories =
        generateTabletColumnCategory(0, measurementSchemas.size());

    final File file = new File(tmpDir, "1-0-0-0.tsfile");

    List<MeasurementSchema> schemaList1 =
        measurementSchemas.stream().map(pair -> pair.left).collect(Collectors.toList());
    List<IMeasurementSchema> schemaList2 =
        measurementSchemas.stream().map(pair -> pair.right).collect(Collectors.toList());

    try (final TsFileTableGenerator generator = new TsFileTableGenerator(file)) {
      generator.registerTable(SchemaConfig.TABLE_0, schemaList2, columnCategories);

      generator.generateData(SchemaConfig.TABLE_0, lineCount, PARTITION_INTERVAL / 10_000);
    }

    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(String.format("create database if not exists %s", SchemaConfig.DATABASE_0));
      statement.execute(String.format("use %s", SchemaConfig.DATABASE_0));
      statement.execute(convert2TableSQL(SchemaConfig.TABLE_0, schemaList1, columnCategories));
      statement.execute(
          String.format(
              "load '%s' with ('database'='%s')", file.getAbsolutePath(), SchemaConfig.DATABASE_0));
      try (final ResultSet resultSet =
          statement.executeQuery(String.format("select count(*) from %s", SchemaConfig.TABLE_0))) {
        if (resultSet.next()) {
          Assert.assertEquals(lineCount, resultSet.getLong(1));
        } else {
          Assert.fail("This ResultSet is empty.");
        }
      }
    }
  }

  @Test
  public void testLoadWithTableMod() throws Exception {
    final int lineCount = 10000;

    List<Pair<MeasurementSchema, MeasurementSchema>> measurementSchemas =
        generateMeasurementSchemas();
    List<ColumnCategory> columnCategories =
        generateTabletColumnCategory(0, measurementSchemas.size());

    final File file = new File(tmpDir, "1-0-0-0.tsfile");

    List<MeasurementSchema> schemaList1 =
        measurementSchemas.stream().map(pair -> pair.left).collect(Collectors.toList());

    try (final TsFileTableGenerator generator = new TsFileTableGenerator(file)) {
      generator.registerTable(SchemaConfig.TABLE_0, new ArrayList<>(schemaList1), columnCategories);
      generator.generateData(SchemaConfig.TABLE_0, lineCount, PARTITION_INTERVAL / 10_000);

      generator.registerTable(SchemaConfig.TABLE_1, new ArrayList<>(schemaList1), columnCategories);
      generator.generateData(SchemaConfig.TABLE_1, lineCount, PARTITION_INTERVAL / 10_000);
      generator.generateDeletion(SchemaConfig.TABLE_1);
    }

    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(String.format("create database if not exists %s", SchemaConfig.DATABASE_0));
      statement.execute(String.format("use %s", SchemaConfig.DATABASE_0));
      statement.execute(
          String.format(
              "load '%s' with ('database'='%s')", file.getAbsolutePath(), SchemaConfig.DATABASE_0));
      try (final ResultSet resultSet =
          statement.executeQuery(String.format("select count(*) from %s", SchemaConfig.TABLE_0))) {
        if (resultSet.next()) {
          Assert.assertEquals(lineCount, resultSet.getLong(1));
        } else {
          Assert.fail("This ResultSet is empty.");
        }
      }

      try (final ResultSet resultSet = statement.executeQuery("show tables")) {
        Assert.assertTrue(resultSet.next());
        Assert.assertFalse(resultSet.next());
      }
    }
  }

  private List<ColumnCategory> generateTabletColumnCategory(int tagNum, int filedNum) {
    List<ColumnCategory> columnTypes = new ArrayList<>(tagNum + filedNum);
    for (int i = 0; i < tagNum; i++) {
      columnTypes.add(ColumnCategory.TAG);
    }
    for (int i = 0; i < filedNum; i++) {
      columnTypes.add(ColumnCategory.FIELD);
    }
    return columnTypes;
  }

  private String convert2TableSQL(
      final String tableName,
      final List<MeasurementSchema> schemaList,
      final List<ColumnCategory> columnCategoryList) {
    List<String> columns = new ArrayList<>();
    for (int i = 0; i < schemaList.size(); i++) {
      final MeasurementSchema measurement = schemaList.get(i);
      columns.add(
          String.format(
              "%s %s %s",
              measurement.getMeasurementName(),
              measurement.getType(),
              columnCategoryList.get(i).name()));
    }
    String tableCreation =
        String.format("create table %s(%s)", tableName, String.join(", ", columns));
    LOGGER.info("schema execute: {}", tableCreation);
    return tableCreation;
  }

  private static class SchemaConfig {
    private static final String DATABASE_0 = "root";
    private static final String TABLE_0 = "test";
    private static final String TABLE_1 = "test1";
  }
}
