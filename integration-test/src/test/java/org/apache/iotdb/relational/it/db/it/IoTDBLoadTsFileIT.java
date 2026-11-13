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

import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.it.utils.TsFileTableGenerator;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.apache.tsfile.encoding.encoder.PlainEncoder;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.chunk.ValueChunkWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.schema.Schema;
import org.apache.tsfile.write.writer.TsFileIOWriter;
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
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
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
    EnvFactory.getEnv().getConfig().getCommonConfig().setDatanodeMemoryProportion("3:3:1:1:1:0");
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

  private List<Pair<MeasurementSchema, MeasurementSchema>> generateMeasurementSchemasWithTime(
      final int timeColumnIndex, final String timeColumnName) {
    List<TSDataType> dataTypes =
        new ArrayList<>(
            Arrays.asList(
                TSDataType.STRING,
                TSDataType.TEXT,
                TSDataType.BLOB,
                TSDataType.TIMESTAMP,
                TSDataType.BOOLEAN,
                TSDataType.DATE,
                TSDataType.DOUBLE,
                TSDataType.FLOAT,
                TSDataType.INT32,
                TSDataType.INT64));
    List<Pair<MeasurementSchema, MeasurementSchema>> pairs = new ArrayList<>();
    for (TSDataType type : dataTypes) {
      for (TSDataType dataType : dataTypes) {
        String id = String.format("%s2%s", type.name(), dataType.name());
        pairs.add(new Pair<>(new MeasurementSchema(id, type), new MeasurementSchema(id, dataType)));
      }
    }

    if (timeColumnIndex >= 0) {
      pairs.add(
          timeColumnIndex,
          new Pair<>(
              new MeasurementSchema(timeColumnName, TSDataType.TIMESTAMP),
              new MeasurementSchema(timeColumnName, TSDataType.TIMESTAMP)));
    }

    return pairs;
  }

  @Test
  public void testLoadWithEmptyDatabaseForTableModel() throws Exception {
    final int lineCount = 10000;

    final List<Pair<MeasurementSchema, MeasurementSchema>> measurementSchemas =
        generateMeasurementSchemas();
    final List<ColumnCategory> columnCategories =
        generateTabletColumnCategory(measurementSchemas.size());

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
    List<ColumnCategory> columnCategories = generateTabletColumnCategory(measurementSchemas.size());

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
    List<ColumnCategory> columnCategories = generateTabletColumnCategory(measurementSchemas.size());

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

  @Test
  public void testLoadWithTimeColumn() throws Exception {
    final int lineCount = 10000;

    // from: 1 time
    List<Pair<MeasurementSchema, MeasurementSchema>> measurementSchemas =
        generateMeasurementSchemasWithTime(1, "time");
    List<ColumnCategory> columnCategories =
        generateTabletColumnCategory(0, measurementSchemas.size(), 1);

    File file = new File(tmpDir, "1-0-0-0.tsfile");

    List<MeasurementSchema> schemaList1 =
        measurementSchemas.stream().map(pair -> pair.left).collect(Collectors.toList());

    try (final TsFileTableGenerator generator = new TsFileTableGenerator(file)) {
      generator.registerTable(SchemaConfig.TABLE_0, new ArrayList<>(schemaList1), columnCategories);
      generator.generateData(SchemaConfig.TABLE_0, lineCount, PARTITION_INTERVAL / 10_000);
    }

    // to: 1 time
    testWithTimeColumn(lineCount, schemaList1, columnCategories, file);
    // to: empty
    testWithTimeColumn(lineCount, null, null, file);

    measurementSchemas = generateMeasurementSchemasWithTime(2, "time");
    columnCategories = generateTabletColumnCategory(0, measurementSchemas.size(), 2);
    schemaList1 = measurementSchemas.stream().map(pair -> pair.left).collect(Collectors.toList());

    // to: 2 time
    testWithTimeColumn(lineCount, schemaList1, columnCategories, file);

    // to: 0 time
    measurementSchemas = generateMeasurementSchemasWithTime(-1, "time");
    columnCategories = generateTabletColumnCategory(0, measurementSchemas.size(), -1);
    schemaList1 = measurementSchemas.stream().map(pair -> pair.left).collect(Collectors.toList());
    testWithTimeColumn(lineCount, schemaList1, columnCategories, file);

    // to: 2 time1
    measurementSchemas = generateMeasurementSchemasWithTime(2, "time1");
    columnCategories = generateTabletColumnCategory(0, measurementSchemas.size(), 2);
    schemaList1 = measurementSchemas.stream().map(pair -> pair.left).collect(Collectors.toList());
    testWithTimeColumn(lineCount, schemaList1, columnCategories, file);

    // from: 2 time1
    file = new File(tmpDir, "2-0-0-0.tsfile");
    try (final TsFileTableGenerator generator = new TsFileTableGenerator(file)) {
      generator.registerTable(SchemaConfig.TABLE_0, new ArrayList<>(schemaList1), columnCategories);
      generator.generateData(SchemaConfig.TABLE_0, lineCount, PARTITION_INTERVAL / 10_000);
    }

    // to: 2 time
    measurementSchemas = generateMeasurementSchemasWithTime(2, "time");
    columnCategories = generateTabletColumnCategory(0, measurementSchemas.size(), 2);
    schemaList1 = measurementSchemas.stream().map(pair -> pair.left).collect(Collectors.toList());
    testWithTimeColumn(lineCount, schemaList1, columnCategories, file);

    // to: 1 time
    measurementSchemas = generateMeasurementSchemasWithTime(1, "time1");
    columnCategories = generateTabletColumnCategory(0, measurementSchemas.size(), 1);
    schemaList1 = measurementSchemas.stream().map(pair -> pair.left).collect(Collectors.toList());
    testWithTimeColumn(lineCount, schemaList1, columnCategories, file);

    // to: empty
    testWithTimeColumn(lineCount, null, null, file);

    // to: 0 time
    measurementSchemas = generateMeasurementSchemasWithTime(-1, "time");
    columnCategories = generateTabletColumnCategory(0, measurementSchemas.size(), -1);
    schemaList1 = measurementSchemas.stream().map(pair -> pair.left).collect(Collectors.toList());
    testWithTimeColumn(lineCount, schemaList1, columnCategories, file);

    // from: 0 time
    file = new File(tmpDir, "3-0-0-0.tsfile");
    try (final TsFileTableGenerator generator = new TsFileTableGenerator(file)) {
      generator.registerTable(SchemaConfig.TABLE_0, new ArrayList<>(schemaList1), columnCategories);
      generator.generateData(SchemaConfig.TABLE_0, lineCount, PARTITION_INTERVAL / 10_000);
    }

    // to: 2 time
    measurementSchemas = generateMeasurementSchemasWithTime(2, "time");
    columnCategories = generateTabletColumnCategory(0, measurementSchemas.size(), 2);
    schemaList1 = measurementSchemas.stream().map(pair -> pair.left).collect(Collectors.toList());
    testWithTimeColumn(lineCount, schemaList1, columnCategories, file);

    // to: 1 time1
    measurementSchemas = generateMeasurementSchemasWithTime(1, "time1");
    columnCategories = generateTabletColumnCategory(0, measurementSchemas.size(), 1);
    schemaList1 = measurementSchemas.stream().map(pair -> pair.left).collect(Collectors.toList());
    testWithTimeColumn(lineCount, schemaList1, columnCategories, file);

    // to: 0 time
    measurementSchemas = generateMeasurementSchemasWithTime(-1, "time");
    columnCategories = generateTabletColumnCategory(0, measurementSchemas.size(), -1);
    schemaList1 = measurementSchemas.stream().map(pair -> pair.left).collect(Collectors.toList());
    testWithTimeColumn(lineCount, schemaList1, columnCategories, file);

    // to: empty
    testWithTimeColumn(lineCount, null, null, file);
  }

  private void testWithTimeColumn(
      final long lineCount,
      final List<MeasurementSchema> schemaList1,
      final List<ColumnCategory> columnCategories,
      final File file)
      throws Exception {
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(String.format("create database if not exists %s", SchemaConfig.DATABASE_0));
      statement.execute(String.format("use %s", SchemaConfig.DATABASE_0));
      ResultSet resultSetOld = null;
      if (Objects.nonNull(schemaList1)) {
        statement.execute(convert2TableSQL(SchemaConfig.TABLE_0, schemaList1, columnCategories));
        resultSetOld = statement.executeQuery("desc " + SchemaConfig.TABLE_0);
      }
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

      // Time column's difference shall not affect the old column
      if (Objects.nonNull(resultSetOld)) {
        try (final ResultSet resultSet = statement.executeQuery("desc " + SchemaConfig.TABLE_0)) {
          while (resultSet.next() && resultSetOld.next()) {
            Assert.assertEquals(resultSet.getString(1), resultSetOld.getString(1));
            Assert.assertEquals(resultSet.getString(2), resultSetOld.getString(2));
            Assert.assertEquals(resultSet.getString(3), resultSetOld.getString(3));
          }
          if (resultSet.next() || resultSetOld.next()) {
            Assert.fail("The table schema has changed after load.");
          }
        }
      }

      statement.execute(String.format("drop database %s", SchemaConfig.DATABASE_0));
    }
  }

  private List<ColumnCategory> generateTabletColumnCategory(final int fieldNum) {
    return generateTabletColumnCategory(0, fieldNum, -1);
  }

  private List<ColumnCategory> generateTabletColumnCategory(
      final int tagNum, final int fieldNum, final int timeIndex) {
    List<ColumnCategory> columnTypes =
        new ArrayList<>(tagNum + fieldNum + (timeIndex >= 0 ? 1 : 0));
    for (int i = 0; i < tagNum; i++) {
      columnTypes.add(ColumnCategory.TAG);
    }
    for (int i = 0; i < fieldNum; i++) {
      columnTypes.add(ColumnCategory.FIELD);
    }
    if (timeIndex >= 0) {
      columnTypes.add(timeIndex, ColumnCategory.TIME);
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

  @Test
  public void testLoadWithAllFieldsNullRows() throws Exception {
    final List<IMeasurementSchema> schemas =
        Arrays.asList(
            new MeasurementSchema("f1", TSDataType.INT32),
            new MeasurementSchema("f2", TSDataType.INT64));
    final List<ColumnCategory> columnCategories =
        Arrays.asList(ColumnCategory.FIELD, ColumnCategory.FIELD);

    final File file = new File(tmpDir, "1-0-0-0.tsfile");
    final int totalRows = 20;

    try (final TsFileWriter tsFileWriter = new TsFileWriter(file)) {
      tsFileWriter.registerTableSchema(
          new TableSchema(SchemaConfig.TABLE_0, schemas, columnCategories));

      final List<String> columnNames = Arrays.asList("f1", "f2");
      final List<TSDataType> dataTypes = Arrays.asList(TSDataType.INT32, TSDataType.INT64);
      final Tablet tablet =
          new Tablet(SchemaConfig.TABLE_0, columnNames, dataTypes, columnCategories);

      for (int r = 0; r < totalRows; r++) {
        final int row = tablet.getRowSize();
        tablet.addTimestamp(row, (r + 1) * 1000L);
      }
      tsFileWriter.writeTable(tablet);
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
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(totalRows, resultSet.getLong(1));
      }

      try (final ResultSet resultSet =
          statement.executeQuery(
              String.format("select count(f1), count(f2) from %s", SchemaConfig.TABLE_0))) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(0, resultSet.getLong(1));
        Assert.assertEquals(0, resultSet.getLong(2));
      }

      try (final ResultSet resultSet =
          statement.executeQuery(
              String.format("select time, f1, f2 from %s order by time", SchemaConfig.TABLE_0))) {
        int count = 0;
        while (resultSet.next()) {
          final long time = resultSet.getLong("time");
          final int expectedTime = (count + 1) * 1000;
          Assert.assertEquals(expectedTime, time);
          resultSet.getInt("f1");
          Assert.assertTrue(resultSet.wasNull());
          resultSet.getLong("f2");
          Assert.assertTrue(resultSet.wasNull());
          count++;
        }
        Assert.assertEquals(totalRows, count);
      }
    }
  }

  @Test
  public void testQueryFallbackRestoresTableMetadataAndReadWriteAfterRestart() throws Exception {
    final String target = "load_p1_target";
    final String other = "load_p1_other";
    final String table = "recovered";
    final File source = new File(tmpDir, "610-0-0-0.tsfile");
    final long markerOffset = writeTableRecoverySource(source, table);
    try (final TsFileSequenceReader reader = new TsFileSequenceReader(source.getAbsolutePath())) {
      reader.position(markerOffset);
      Assert.assertEquals(MetaMarker.CHUNK_GROUP_HEADER, reader.readMarker());
    }
    try (final RandomAccessFile file = new RandomAccessFile(source, "rw")) {
      file.seek(markerOffset);
      file.writeByte(0x7f);
    }

    try (final Connection connection =
            EnvFactory.getEnv()
                .getConnection(
                    EnvFactory.getEnv().getDataNodeWrapper(0),
                    SessionConfig.DEFAULT_USER,
                    SessionConfig.DEFAULT_PASSWORD,
                    BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("create database " + target);
      statement.execute("use " + target);
      // The source has a TAG prefix and a different FIELD order, and introduces v.
      statement.execute(
          "create table "
              + table
              + " (t1 STRING TAG,t2 STRING TAG,tail STRING TAG,n INT64 FIELD,extra INT32 FIELD)");
      statement.execute("create database " + other);
      statement.execute("use " + other);
      // Disable proactive tablet conversion: the invalid later marker must fail actual splitting.
      statement.execute(
          String.format(
              "load '%s' with ('database'='%s','on-success'='delete',"
                  + "'tablet-conversion-threshold'='-1')",
              source.getAbsolutePath(), target));
      Assert.assertTrue(
          EnvFactory.getEnv()
              .getDataNodeWrapper(0)
              .logContains(
                  DataNodeQueryMessages
                      .LOAD_SUCCESSFULLY_CONVERTED_TSFILE_ARG_INTO_TABLETS_AND_INSERTED
                      .replace("{}", source.getAbsolutePath())));
    }

    // The coordinator is pinned for the log assertion; verify the result through every DataNode.
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("use " + other);
      try (final ResultSet rows = statement.executeQuery("show tables")) {
        Assert.assertFalse("LOAD must retain its explicit target database", rows.next());
      }
      statement.execute("use " + target);
      assertRecoveredTableSchema(statement, table);
      assertRecoveredTableRows(statement, table, false);
      assertRecoveredTableDevices(statement, table, false);
      Assert.assertFalse(source.exists());
      Assert.assertFalse(new File(source + TsFileResource.RESOURCE_SUFFIX).exists());
      statement.execute("flush");
    }

    TestUtils.restartCluster(EnvFactory.getEnv());
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("use " + target);
      assertRecoveredTableSchema(statement, table);
      assertRecoveredTableRows(statement, table, false);
      assertRecoveredTableDevices(statement, table, false);
      // Merge into a loaded row: retaining v proves the null trailing TAG resolves to the same
      // device identity, which writes at fresh timestamps alone cannot establish.
      statement.execute("insert into " + table + "(time,t1,t2,n) values(20000,'a','b',55)");
      statement.execute(
          "insert into " + table + "(time,t1,t2,n,v) values(20001,'a','b',60,'after')");
      statement.execute(
          "insert into "
              + table
              + "(time,t1,t2,tail,n,extra,v) values(20002,'a','b','tail',70,7,'new-tail')");
      assertRecoveredTableRows(statement, table, true);
      assertRecoveredTableDevices(statement, table, true);
      statement.execute("use " + other);
      try (final ResultSet rows = statement.executeQuery("show tables")) {
        Assert.assertFalse(rows.next());
      }
    }
  }

  private static long writeTableRecoverySource(final File file, final String table)
      throws Exception {
    final List<IMeasurementSchema> schemas =
        Arrays.asList(
            new MeasurementSchema("t1", TSDataType.STRING),
            new MeasurementSchema("t2", TSDataType.STRING),
            new MeasurementSchema("v", TSDataType.STRING),
            new MeasurementSchema("n", TSDataType.INT64));
    final List<String> names = Arrays.asList("t1", "t2", "v", "n");
    final List<TSDataType> types =
        Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.INT64);
    final List<ColumnCategory> categories =
        Arrays.asList(
            ColumnCategory.TAG, ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD);
    try (final TsFileWriter writer = new TsFileWriter(file)) {
      writer.registerTableSchema(new TableSchema(table, schemas, categories));
      final Tablet anchor = new Tablet(table, names, types, categories, 1);
      anchor.addTimestamp(0, 0);
      anchor.addValue(0, "t1", "anchor");
      anchor.addValue(0, "t2", "x");
      anchor.addValue(0, "v", "anchor");
      anchor.addValue(0, "n", -1L);
      writer.writeTable(anchor);
      writer.flush();
      final long markerOffset = writer.getIOWriter().getPos();
      final Tablet tablet = new Tablet(table, names, types, categories, 5);
      final long[] times = {9999, 10000, 10001, 19999, 20000};
      final String[] firstTags = {"a", "a", "a", null, "a"};
      final String[] secondTags = {"b", null, "b", "b", "b"};
      final String[] values = {"one", null, "three", null, "five"};
      final Long[] numbers = {10L, 20L, null, null, 50L};
      for (int row = 0; row < times.length; row++) {
        tablet.addTimestamp(row, times[row]);
        tablet.addValue(row, "t1", firstTags[row]);
        tablet.addValue(row, "t2", secondTags[row]);
        tablet.addValue(row, "v", values[row]);
        if (numbers[row] != null) {
          tablet.addValue(row, "n", numbers[row].longValue());
        }
      }
      writer.writeTable(tablet);
      return markerOffset;
    }
  }

  private static void assertRecoveredTableSchema(final Statement statement, final String table)
      throws SQLException {
    final String[][] expected = {
      {"time", "TIMESTAMP", "TIME"}, {"t1", "STRING", "TAG"}, {"t2", "STRING", "TAG"},
      {"tail", "STRING", "TAG"}, {"n", "INT64", "FIELD"}, {"extra", "INT32", "FIELD"},
      {"v", "STRING", "FIELD"}
    };
    try (final ResultSet rows = statement.executeQuery("describe " + table)) {
      for (final String[] row : expected) {
        Assert.assertTrue(rows.next());
        for (int column = 0; column < row.length; column++) {
          Assert.assertEquals(row[column], rows.getString(column + 1));
        }
      }
      Assert.assertFalse(rows.next());
    }
  }

  private static void assertRecoveredTableRows(
      final Statement statement, final String table, final boolean afterWrites)
      throws SQLException {
    final List<Object[]> expected =
        new ArrayList<>(
            Arrays.asList(
                new Object[] {0L, "anchor", "x", null, -1L, null, "anchor"},
                new Object[] {9999L, "a", "b", null, 10L, null, "one"},
                new Object[] {10000L, "a", null, null, 20L, null, null},
                new Object[] {10001L, "a", "b", null, null, null, "three"},
                new Object[] {19999L, null, "b", null, null, null, null},
                new Object[] {20000L, "a", "b", null, afterWrites ? 55L : 50L, null, "five"}));
    if (afterWrites) {
      expected.add(new Object[] {20001L, "a", "b", null, 60L, null, "after"});
      expected.add(new Object[] {20002L, "a", "b", "tail", 70L, 7, "new-tail"});
    }
    try (final ResultSet rows =
        statement.executeQuery(
            "select time,t1,t2,tail,n,extra,v from " + table + " order by time")) {
      for (final Object[] row : expected) {
        Assert.assertTrue(Arrays.toString(row), rows.next());
        Assert.assertEquals(row[0], rows.getLong(1));
        for (int column = 1; column < row.length; column++) {
          Assert.assertEquals(Arrays.toString(row), row[column], rows.getObject(column + 1));
        }
      }
      Assert.assertFalse(rows.next());
    }
  }

  private static void assertRecoveredTableDevices(
      final Statement statement, final String table, final boolean afterWrites)
      throws SQLException {
    final List<String> expected =
        new ArrayList<>(
            Arrays.asList("anchor,x,null,", "a,b,null,", "a,null,null,", "null,b,null,"));
    if (afterWrites) {
      expected.add("a,b,tail,");
    }
    try (final ResultSet rows = statement.executeQuery("show devices from " + table)) {
      TestUtils.assertResultSetEqualUnordered(rows, "t1,t2,tail,", expected);
    }
  }

  /**
   * The compaction writer emits the series batches behind the first one as value chunks that follow
   * all time chunks of the chunk group. When LOAD splits such a chunk by time partition it has to
   * re-encode it, and the rows of the chunk keep their positions: a null value still occupies a
   * bitmap position and an all-null page still occupies the page slot opposite its time page.
   *
   * <p>The rows span three time partitions of the interval configured in {@code setUp}, so LOAD has
   * to decode the file and split it instead of storing it as it is.
   */
  @Test
  public void testLoadSparseBatchedAlignedTsFile() throws Exception {
    // The first page stays inside one time partition and is copied as it is, the second one crosses
    // a partition boundary and holds a null row, and the third one is entirely null in both
    // partitions it spans.
    final long[][] pages = {
      {0, 1, 2},
      {PARTITION_INTERVAL - 2, PARTITION_INTERVAL - 1, PARTITION_INTERVAL, PARTITION_INTERVAL + 1},
      {
        2 * PARTITION_INTERVAL - 2,
        2 * PARTITION_INTERVAL - 1,
        2 * PARTITION_INTERVAL,
        2 * PARTITION_INTERVAL + 1
      }
    };
    final File file = new File(tmpDir, "1-0-0-0.tsfile");
    writeSparseBatchedAlignedTsFile(file, pages);

    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(String.format("create database if not exists %s", SchemaConfig.DATABASE_0));
      statement.execute(String.format("use %s", SchemaConfig.DATABASE_0));
      statement.execute(
          String.format(
              "load '%s' with ('database'='%s')", file.getAbsolutePath(), SchemaConfig.DATABASE_0));

      try (final ResultSet resultSet =
          statement.executeQuery(
              String.format("select time, f1, f2 from %s order by time", SchemaConfig.TABLE_0))) {
        for (final long[] page : pages) {
          for (final long time : page) {
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(time, resultSet.getLong("time"));
            Assert.assertEquals(time, resultSet.getLong("f1"));
            if (isNullField2(time)) {
              resultSet.getLong("f2");
              Assert.assertTrue(resultSet.wasNull());
            } else {
              Assert.assertEquals(time + 1, resultSet.getLong("f2"));
            }
          }
        }
        Assert.assertFalse(resultSet.next());
      }
    }
  }

  private void writeSparseBatchedAlignedTsFile(final File file, final long[][] pages)
      throws Exception {
    final Schema schema = new Schema();
    schema.registerTableSchema(
        new TableSchema(
            SchemaConfig.TABLE_0,
            Arrays.asList(
                new MeasurementSchema("tag1", TSDataType.STRING),
                new MeasurementSchema("f1", TSDataType.INT64),
                new MeasurementSchema("f2", TSDataType.INT64)),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD)));

    try (final TsFileIOWriter writer = new TsFileIOWriter(file)) {
      writer.setSchema(schema);
      writer.startChunkGroup(new StringArrayDeviceID(SchemaConfig.TABLE_0, "tagA"));

      // The first field shares the chunk group of the time chunk.
      final AlignedChunkWriterImpl alignedWriter =
          new AlignedChunkWriterImpl(
              Collections.singletonList(new MeasurementSchema("f1", TSDataType.INT64)));
      for (final long[] page : pages) {
        for (final long time : page) {
          alignedWriter.getTimeChunkWriter().write(time);
          alignedWriter.getValueChunkWriterByIndex(0).write(time, time, false);
        }
        alignedWriter.sealCurrentPage();
      }
      alignedWriter.writeToFileWriter(writer);

      // The second field follows the time chunk as a value chunk of its own.
      final ValueChunkWriter valueWriter =
          new ValueChunkWriter(
              "f2",
              CompressionType.LZ4,
              TSDataType.INT64,
              TSEncoding.PLAIN,
              new PlainEncoder(TSDataType.INT64, 8));
      for (final long[] page : pages) {
        for (final long time : page) {
          valueWriter.write(time, time + 1, isNullField2(time));
        }
        valueWriter.sealCurrentPage();
      }
      valueWriter.writeToFileWriter(writer);

      writer.endChunkGroup();
      writer.endFile();
    }
  }

  private static boolean isNullField2(final long time) {
    // The row in the middle of the page that crosses the first boundary and the rows of the last
    // page carry no value.
    return time == PARTITION_INTERVAL - 1 || time >= 2 * PARTITION_INTERVAL - 2;
  }

  private static class SchemaConfig {
    private static final String DATABASE_0 = "root";
    private static final String TABLE_0 = "test";
    private static final String TABLE_1 = "test1";
  }
}
