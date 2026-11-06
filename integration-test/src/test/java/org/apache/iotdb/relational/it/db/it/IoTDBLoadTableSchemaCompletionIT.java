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
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Tests the LOAD behavior of table-model TsFiles whose TableSchema entry does not describe all the
 * columns of the table.
 *
 * <p>The storage path writes a time-only entry for a table that has no column other than time, and
 * a file written before the table-model file property existed may carry no entry at all. LOAD must
 * complete such a schema from the table in IoTDB instead of failing the whole file, and must still
 * create the table with every column when the file does describe them.
 */
@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBLoadTableSchemaCompletionIT {

  private static final String DATABASE = "load_schema_completion_db";
  private static final String TIME_ONLY_TABLE = "t_time_only";
  private static final String PIPE_TABLE = "t_pipe";
  private static final String AUTO_CREATE_TABLE = "t_auto_create";
  private static final String TAG_VALUE = "tagA";
  private static final int ROW_COUNT = 3;

  /** The file property written by every table-model writer (flush, compaction, recovery, pipe). */
  private static final String TABLE_MODEL_PROPERTY = "isTableModel";

  /** What the TableSchema entry of the generated TsFile contains. */
  private enum SchemaEntry {
    /** Every column of the table, which is what a flush or a compaction writes. */
    FULL,
    /** Only the time column, which is what a table without another column produces. */
    TIME_ONLY,
    /** No entry at all, which a file written before the file property existed can carry. */
    ABSENT
  }

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE IF NOT EXISTS " + DATABASE);
      statement.execute("USE " + DATABASE);
      statement.execute(
          "CREATE TABLE IF NOT EXISTS " + TIME_ONLY_TABLE + " (tag1 STRING TAG, s1 INT32 FIELD)");
      statement.execute(
          "CREATE TABLE IF NOT EXISTS " + PIPE_TABLE + " (tag1 STRING TAG, s1 INT32 FIELD)");
      // This one must stay absent so that LOAD has to create it.
      statement.execute("DROP TABLE IF EXISTS " + AUTO_CREATE_TABLE);
    }
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testLoadTsFileWithTimeOnlyTableSchema() throws Exception {
    // The schema entry keeps only the time column but the chunks still hold s1.
    loadAndVerify(TIME_ONLY_TABLE, SchemaEntry.TIME_ONLY);
  }

  @Test
  public void testLoadTsFileWithoutTableSchemaEntry() throws Exception {
    // A legacy table-model file: the isTableModel property is set but no schema entry exists.
    loadAndVerify(PIPE_TABLE, SchemaEntry.ABSENT);
  }

  @Test
  public void testLoadTsFileCreatesTableWithAllColumns() throws Exception {
    // The table does not exist and the file describes every column, so LOAD has to create the
    // table with all of them, including the tag column that is only carried by the device id.
    loadAndVerify(AUTO_CREATE_TABLE, SchemaEntry.FULL);
  }

  private static void loadAndVerify(final String tableName, final SchemaEntry schemaEntry)
      throws Exception {
    final Path tempDir = Files.createTempDirectory("load-schema-completion");
    try {
      final Path tsFile = writeTableModelTsFile(tempDir, tableName, schemaEntry);
      assertTsFileContent(tsFile, tableName, schemaEntry);

      try (final Connection connection =
              EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
          final Statement statement = connection.createStatement()) {
        statement.execute("USE " + DATABASE);
        statement.execute(String.format("LOAD '%s'", tsFile.toAbsolutePath()));

        try (final ResultSet resultSet =
            statement.executeQuery("SELECT tag1, s1 FROM " + tableName + " ORDER BY time")) {
          int expectedValue = 0;
          while (resultSet.next()) {
            Assert.assertEquals(TAG_VALUE, resultSet.getString(1));
            Assert.assertEquals(expectedValue, resultSet.getInt(2));
            expectedValue++;
          }
          Assert.assertEquals(ROW_COUNT, expectedValue);
        }
      }
    } finally {
      deleteRecursively(tempDir);
    }
  }

  private static Path writeTableModelTsFile(
      final Path dir, final String tableName, final SchemaEntry schemaEntry) throws Exception {
    final Path tsFile = dir.resolve(tableName + ".tsfile");
    final List<IMeasurementSchema> measurementSchemas =
        Arrays.asList(
            new MeasurementSchema("tag1", TSDataType.STRING),
            new MeasurementSchema("s1", TSDataType.INT32));
    try (final TsFileIOWriter writer = new TsFileIOWriter(tsFile.toFile())) {
      switch (schemaEntry) {
        case FULL:
          writer
              .getSchema()
              .registerTableSchema(
                  new TableSchema(
                      tableName,
                      measurementSchemas,
                      Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD)));
          break;
        case TIME_ONLY:
          // Only the time column is kept in the schema entry, which is what the storage path
          // writes when the columns of the table are no longer available.
          writer.getSchema().registerTableSchema(new TableSchema(tableName));
          break;
        case ABSENT:
        default:
          break;
      }
      writer.startChunkGroup(new StringArrayDeviceID(tableName, TAG_VALUE));
      // The tag column is carried by the device id, so only s1 is written into the chunk group.
      final AlignedChunkWriterImpl chunkWriter =
          new AlignedChunkWriterImpl(measurementSchemas.subList(1, measurementSchemas.size()));
      for (int i = 0; i < ROW_COUNT; i++) {
        final long time = 100L + i;
        chunkWriter.getTimeChunkWriter().write(time);
        chunkWriter.getValueChunkWriterByIndex(0).write(time, i, false);
      }
      chunkWriter.writeToFileWriter(writer);
      writer.endChunkGroup();
      writer.addTsFileProperty(TABLE_MODEL_PROPERTY, "true".getBytes(StandardCharsets.UTF_8));
      writer.endFile();
    }
    return tsFile;
  }

  private static void assertTsFileContent(
      final Path tsFile, final String tableName, final SchemaEntry schemaEntry) throws Exception {
    try (final TsFileSequenceReader reader =
        new TsFileSequenceReader(tsFile.toAbsolutePath().toString())) {
      Assert.assertTrue(
          "The TsFile must be marked as a table-model file",
          reader.readFileMetadata().getTsFileProperties().containsKey(TABLE_MODEL_PROPERTY));

      final Map<String, TableSchema> tableSchemaMap = reader.getTableSchemaMap();
      Assert.assertNotNull(tableSchemaMap);
      switch (schemaEntry) {
        case FULL:
          Assert.assertTrue(
              "TsFile has no schema entry for table " + tableName + ": " + tableSchemaMap,
              tableSchemaMap.containsKey(tableName));
          Assert.assertEquals(
              Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD),
              tableSchemaMap.get(tableName).getColumnTypes());
          break;
        case TIME_ONLY:
          Assert.assertTrue(
              "TsFile has no schema entry for table " + tableName + ": " + tableSchemaMap,
              tableSchemaMap.containsKey(tableName));
          Assert.assertTrue(
              "The schema entry must not contain any column other than time: "
                  + tableSchemaMap.get(tableName),
              tableSchemaMap.get(tableName).getColumnSchemas().isEmpty());
          break;
        case ABSENT:
        default:
          Assert.assertTrue(
              "The TsFile must not contain any schema entry: " + tableSchemaMap,
              tableSchemaMap.isEmpty());
          break;
      }
      Assert.assertTrue(
          "The TsFile must contain device " + tableName + "." + TAG_VALUE,
          reader.getAllDevices().stream()
              .anyMatch(
                  device ->
                      Arrays.equals(device.getSegments(), new String[] {tableName, TAG_VALUE})));
    }
  }

  private static void deleteRecursively(final Path path) throws IOException {
    if (!Files.exists(path)) {
      return;
    }
    try (final Stream<Path> stream = Files.walk(path)) {
      stream
          .sorted(Comparator.reverseOrder())
          .forEach(
              file -> {
                try {
                  Files.deleteIfExists(file);
                } catch (final Exception e) {
                  // Ignore cleanup failures, the temporary directory is not part of the assertion.
                }
              });
    }
  }
}
