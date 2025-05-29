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

import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.LastCacheLoadStrategy;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.utils.TsFileGenerator;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.jdbc.IoTDBSQLException;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.ColumnSchema;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.v4.ITsFileWriter;
import org.apache.tsfile.write.v4.TsFileWriterBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@SuppressWarnings({"ResultOfMethodCallIgnored", "UnstableApiUsage"})
@RunWith(Parameterized.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBLoadLastCacheIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBLoadLastCacheIT.class);
  private static final long PARTITION_INTERVAL = 10 * 1000L;
  private static final int connectionTimeoutInMS = (int) TimeUnit.SECONDS.toMillis(300);
  private static final long loadTsFileAnalyzeSchemaMemorySizeInBytes = 10 * 1024L;

  private File tmpDir;
  private final LastCacheLoadStrategy lastCacheLoadStrategy;

  @Parameters(name = "loadLastCacheStrategy={0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          {LastCacheLoadStrategy.CLEAN_ALL},
          {LastCacheLoadStrategy.UPDATE},
          {LastCacheLoadStrategy.UPDATE_NO_BLOB},
          {LastCacheLoadStrategy.CLEAN_DEVICE}
        });
  }

  public IoTDBLoadLastCacheIT(LastCacheLoadStrategy lastCacheLoadStrategy) {
    this.lastCacheLoadStrategy = lastCacheLoadStrategy;
  }

  @Before
  public void setUp() throws Exception {
    tmpDir = new File(Files.createTempDirectory("load").toUri());
    EnvFactory.getEnv().getConfig().getCommonConfig().setTimePartitionInterval(PARTITION_INTERVAL);
    EnvFactory.getEnv()
        .getConfig()
        .getDataNodeConfig()
        .setConnectionTimeoutInMS(connectionTimeoutInMS)
        .setLoadTsFileAnalyzeSchemaMemorySizeInBytes(loadTsFileAnalyzeSchemaMemorySizeInBytes);
    EnvFactory.getEnv()
        .getConfig()
        .getDataNodeConfig()
        .setLoadLastCacheStrategy(lastCacheLoadStrategy.name())
        .setCacheLastValuesForLoad(true);
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
            new Path(device, schema.getMeasurementName(), true).getFullPath(),
            schema.getType().name());
    LOGGER.info("schema execute: {}", sql);
    return sql;
  }

  private String convert2AlignedSQL(final String device, final List<IMeasurementSchema> schemas) {
    StringBuilder sql = new StringBuilder(String.format("create aligned timeseries %s(", device));
    for (int i = 0; i < schemas.size(); i++) {
      final IMeasurementSchema schema = schemas.get(i);
      sql.append(String.format("%s %s", schema.getMeasurementName(), schema.getType().name()));
      sql.append(i == schemas.size() - 1 ? ")" : ",");
    }
    LOGGER.info("schema execute: {}.", sql);
    return sql.toString();
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
    for (final File file : Objects.requireNonNull(tmpDir.listFiles())) {
      if (!file.delete()) {
        return false;
      }
    }
    return tmpDir.delete();
  }

  @Test
  public void testTreeModelLoadWithLastCache() throws Exception {
    registerSchema();

    final String device = SchemaConfig.DEVICE_0;
    final String measurement = SchemaConfig.MEASUREMENT_00.getMeasurementName();

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
  public void testTableModelLoadWithLastCache() throws Exception {
    final String database = SchemaConfig.DATABASE_0;
    final String table = SchemaConfig.TABLE_0;
    final String measurement = SchemaConfig.MEASUREMENT_00.getMeasurementName();

    try (final Connection connection = EnvFactory.getEnv().getTableConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE IF NOT EXISTS " + database);
      statement.execute("USE " + database);
      statement.execute(
          "CREATE TABLE IF NOT EXISTS "
              + table
              + " (device_id STRING TAG,"
              + measurement
              + " INT32"
              + ")");

      statement.execute(
          String.format(
              "insert into %s(time, device_id, %s) values(100, 'd0', 100)", table, measurement));

      try (final ResultSet resultSet =
          statement.executeQuery(String.format("select last(%s) from %s", measurement, table))) {
        if (resultSet.next()) {
          final String lastValue = resultSet.getString("_col0");
          Assert.assertEquals("100", lastValue);
        } else {
          Assert.fail("This ResultSet is empty.");
        }
      }
    }

    final File file1 = new File(tmpDir, "1-0-0-0.tsfile");
    TableSchema tableSchema =
        new TableSchema(
            table,
            Arrays.asList(
                new ColumnSchema("device_id", TSDataType.STRING, ColumnCategory.TAG),
                new ColumnSchema(
                    SchemaConfig.MEASUREMENT_00.getMeasurementName(),
                    SchemaConfig.MEASUREMENT_00.getType(),
                    ColumnCategory.FIELD)));
    try (ITsFileWriter tsFileWriter =
        new TsFileWriterBuilder().file(file1).tableSchema(tableSchema).build()) {
      Tablet tablet =
          new Tablet(
              Arrays.asList("device_id", SchemaConfig.MEASUREMENT_00.getMeasurementName()),
              Arrays.asList(TSDataType.STRING, SchemaConfig.MEASUREMENT_00.getType()));
      tablet.addTimestamp(0, PARTITION_INTERVAL);
      tablet.addValue(0, 0, "d0");
      tablet.addValue(0, 1, 10000);
      tsFileWriter.write(tablet);
    }

    try (final Connection connection = EnvFactory.getEnv().getTableConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("USE " + database);
      statement.execute(
          String.format(
              "load '%s' with ('database-name'='%s')", tmpDir.getAbsolutePath(), database));

      try (final ResultSet resultSet =
          statement.executeQuery(String.format("select last(%s) from %s", measurement, table))) {
        if (resultSet.next()) {
          final String lastTime = resultSet.getString("_col0");
          Assert.assertEquals(String.valueOf(PARTITION_INTERVAL), lastTime);
        } else {
          Assert.fail("This ResultSet is empty.");
        }
      }
    }
  }

  private static class PerformanceSchemas {

    private final String database;
    private final TableSchema tableSchema;
    private final List<String> columnNames;
    private final List<TSDataType> dataTypes;

    public PerformanceSchemas(
        String database, String tableName, int measurementNum, int blobMeasurementNum) {
      this.database = database;
      List<ColumnSchema> columnSchemas = new ArrayList<>(measurementNum + blobMeasurementNum);
      columnNames = new ArrayList<>(measurementNum + blobMeasurementNum);
      dataTypes = new ArrayList<>(measurementNum + blobMeasurementNum);

      columnSchemas.add(new ColumnSchema("device_id", TSDataType.STRING, ColumnCategory.TAG));
      columnNames.add("device_id");
      dataTypes.add(TSDataType.STRING);
      for (int i = 0; i < measurementNum; i++) {
        columnSchemas.add(new ColumnSchema("s" + i, TSDataType.INT64, ColumnCategory.FIELD));
        columnNames.add("s" + i);
        dataTypes.add(TSDataType.INT64);
      }
      for (int i = 0; i < blobMeasurementNum; i++) {
        columnSchemas.add(
            new ColumnSchema("s" + (measurementNum + i), TSDataType.BLOB, ColumnCategory.FIELD));
        columnNames.add("s" + (measurementNum + i));
        dataTypes.add(TSDataType.BLOB);
      }
      tableSchema = new TableSchema(tableName, columnSchemas);
    }
  }

  private void generateAndLoadOne(
      int deviceCnt,
      int measurementCnt,
      int blobMeasurementCnt,
      int pointCnt,
      int offset,
      PerformanceSchemas schemas,
      int fileNum,
      Statement statement)
      throws Exception {
    File file = new File("target" + File.separator + fileNum + ".tsfile");
    try (ITsFileWriter tsFileWriter =
        new TsFileWriterBuilder().file(file).tableSchema(schemas.tableSchema).build()) {
      Tablet tablet = new Tablet(schemas.columnNames, schemas.dataTypes, pointCnt * deviceCnt);
      int rowIndex = 0;
      for (int i = 0; i < deviceCnt; i++) {
        for (int j = 0; j < pointCnt; j++) {
          tablet.addTimestamp(rowIndex, j + offset);
          tablet.addValue(rowIndex, 0, "d" + i);
          for (int k = 0; k < measurementCnt; k++) {
            tablet.addValue(rowIndex, k + 1, (long) j + offset);
          }
          for (int k = 0; k < blobMeasurementCnt; k++) {
            tablet.addValue(rowIndex, k + 1 + measurementCnt, String.valueOf(j + offset));
          }
          rowIndex++;
        }
      }
      tsFileWriter.write(tablet);
    }

    statement.execute(
        String.format(
            "load '%s' with ('database-name'='%s')", file.getAbsolutePath(), schemas.database));

    file.delete();
  }

  private void generateAndLoadAll(
      int deviceCnt,
      int measurementCnt,
      int blobMeasurementCnt,
      int pointCnt,
      PerformanceSchemas schemas,
      int fileNum)
      throws Exception {
    try (final Connection connection = EnvFactory.getEnv().getTableConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("USE " + schemas.database);

      for (int i = 0; i < fileNum; i++) {
        generateAndLoadOne(
            deviceCnt,
            measurementCnt,
            blobMeasurementCnt,
            pointCnt,
            pointCnt * i,
            schemas,
            fileNum,
            statement);
      }
    }
  }

  private long queryLastOnce(
      int deviceNum, int measurementNum, PerformanceSchemas schemas, Statement statement)
      throws SQLException {
    try (final ResultSet resultSet =
        statement.executeQuery(
            String.format(
                "select last(time),last_by(%s, time) from %s where device_id='%s'",
                "s" + measurementNum, schemas.tableSchema.getTableName(), "d" + deviceNum))) {
      if (resultSet.next()) {
        return resultSet.getLong("_col0");
      } else {
        return -1;
      }
    } catch (SQLException e) {
      if (!e.getMessage().contains("does not exist")) {
        throw e;
      }
    }
    return -1;
  }

  @SuppressWarnings("BusyWait")
  private void queryAll(
      int deviceCnt,
      int measurementCnt,
      int pointCnt,
      int fileCnt,
      PerformanceSchemas schemas,
      RateLimiter rateLimiter)
      throws SQLException {
    Random random = new Random();
    long totalStart = System.currentTimeMillis();
    List<Long> timeConsumptions = new ArrayList<>();

    try (final Connection connection = EnvFactory.getEnv().getTableConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("USE " + schemas.database);

      while (true) {
        int deviceNum = random.nextInt(deviceCnt);
        int measurementNum = random.nextInt(measurementCnt);
        rateLimiter.acquire();
        long start = System.nanoTime();
        long result = queryLastOnce(deviceNum, measurementNum, schemas, statement);
        long timeConsumption = System.nanoTime() - start;
        if (result == -1) {
          try {
            Thread.sleep(1000);
            continue;
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
        System.out.printf(
            "%s: d%d.s%d %s %s%n", new Date(), deviceNum, measurementNum, result, timeConsumption);
        timeConsumptions.add(timeConsumption);
        if (result == (long) pointCnt * fileCnt - 1) {
          break;
        }
      }
    }

    System.out.printf(
        "Synchronization ends after %dms, query latency avg %fms %n",
        System.currentTimeMillis() - totalStart,
        timeConsumptions.stream().mapToLong(i -> i).average().orElse(0.0) / 1000000);
  }

  @Ignore("Performance")
  @Test
  public void testTableLoadPerformance() throws Exception {
    int deviceCnt = 100;
    int measurementCnt = 100;
    int blobMeasurementCnt = 10;
    int pointCnt = 100;
    int fileCnt = 100000;
    int queryPerSec = 100;
    int queryThreadsNum = 10;

    PerformanceSchemas schemas =
        new PerformanceSchemas("test", "test_table", measurementCnt, blobMeasurementCnt);

    try (final Connection connection = EnvFactory.getEnv().getTableConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE IF NOT EXISTS " + schemas.database);
    }

    Thread loadThread =
        new Thread(
            () -> {
              try {
                generateAndLoadAll(
                    deviceCnt, measurementCnt, blobMeasurementCnt, pointCnt, schemas, fileCnt);
              } catch (Throwable e) {
                e.printStackTrace();
              }
            });

    RateLimiter rateLimiter = RateLimiter.create(queryPerSec);
    List<Thread> queryThreads = new ArrayList<>(queryThreadsNum);
    for (int i = 0; i < queryThreadsNum; i++) {
      Thread queryThread =
          new Thread(
              () -> {
                try {
                  queryAll(
                      deviceCnt,
                      measurementCnt + blobMeasurementCnt,
                      pointCnt,
                      fileCnt,
                      schemas,
                      rateLimiter);
                } catch (Throwable e) {
                  e.printStackTrace();
                }
              });
      queryThreads.add(queryThread);
    }

    loadThread.start();
    queryThreads.forEach(Thread::start);

    loadThread.join();
    for (Thread queryThread : queryThreads) {
      queryThread.join();
    }
  }

  private static class SchemaConfig {

    private static final String DATABASE_0 = "db";
    private static final String TABLE_0 = "test";
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
