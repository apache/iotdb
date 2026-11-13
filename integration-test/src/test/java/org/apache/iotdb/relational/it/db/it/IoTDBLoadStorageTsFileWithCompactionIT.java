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

import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTableManualBasic;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTableManualBasic.class})
public class IoTDBLoadStorageTsFileWithCompactionIT {

  private static final String DATABASE = "load_storage_db";
  private static final String TABLE = "t1";
  private static final long COMPACTION_WAIT_SECONDS = 60L;
  private static final String TABLE_MODEL_PROPERTY = "isTableModel";

  private static BaseEnv producerEnv;
  private static BaseEnv loaderEnv;

  @BeforeClass
  public static void setUp() {
    MultiEnvFactory.createEnv(2);
    producerEnv = MultiEnvFactory.getEnv(0);
    loaderEnv = MultiEnvFactory.getEnv(1);

    configure(producerEnv, false);
    configure(loaderEnv, true);

    producerEnv.initClusterEnvironment(1, 1);
    loaderEnv.initClusterEnvironment(1, 1);
  }

  private static void configure(final BaseEnv env, final boolean enableCompaction) {
    env.getConfig()
        .getCommonConfig()
        .setDatanodeMemoryProportion("3:3:1:1:1:0")
        // The test starts one DataNode per cluster. Override the replication factors selected by
        // the CI cluster mode (HighPerformanceMode uses three schema replicas) so that the
        // database can be created with a single DataNode.
        .setSchemaReplicationFactor(1)
        .setDataReplicationFactor(1)
        .setEnforceStrongPassword(false)
        .setEnableSeqSpaceCompaction(enableCompaction)
        .setEnableUnseqSpaceCompaction(enableCompaction)
        .setEnableCrossSpaceCompaction(enableCompaction)
        .setInnerCompactionCandidateFileNum(2);
    // Make the test deterministic: the producer creates two files with compaction disabled,
    // then enables it and waits for the scheduler to merge them.
    env.getConfig().getDataNodeConfig().setCompactionScheduleInterval(1000);
  }

  @AfterClass
  public static void tearDown() {
    if (loaderEnv != null) {
      loaderEnv.cleanClusterEnvironment();
    }
    if (producerEnv != null) {
      producerEnv.cleanClusterEnvironment();
    }
  }

  @Test
  public void testLoadStorageTsFileAfterCompaction() throws Exception {
    final Path tempDir = Files.createTempDirectory("load-storage-tsfile");
    try {
      final long fileCountBeforeCompaction = createTableFilesOnProducer();
      Assert.assertTrue(
          "The producer should create two TsFiles before compaction, but got "
              + fileCountBeforeCompaction,
          fileCountBeforeCompaction >= 2);
      assertAllTsFilesHaveTableSchema(producerEnv);

      enableSeqSpaceCompaction(producerEnv);
      Awaitility.await()
          .pollInterval(200, TimeUnit.MILLISECONDS)
          .atMost(COMPACTION_WAIT_SECONDS, TimeUnit.SECONDS)
          .until(() -> countTsFiles(producerEnv) == 1);

      final Path sourceTsFile = findSingleTsFile(producerEnv);
      assertTableSchemaRegistered(sourceTsFile);

      final Path copiedTsFile = tempDir.resolve(sourceTsFile.getFileName().toString());
      Files.copy(sourceTsFile, copiedTsFile, StandardCopyOption.REPLACE_EXISTING);

      loadIntoReceiver(copiedTsFile);
      assertLoadedRowCount(2);
    } finally {
      deleteRecursively(tempDir);
    }
  }

  private static long createTableFilesOnProducer() throws Exception {
    try (final Connection connection = producerEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE IF NOT EXISTS " + DATABASE);
      statement.execute("USE " + DATABASE);
      statement.execute(
          "CREATE TABLE IF NOT EXISTS " + TABLE + " (tag1 STRING TAG, s1 INT32 FIELD)");

      // Use the same tag and timestamps near the current time so that both files belong to the
      // same data region; otherwise each file is compacted independently.
      final long baseTime = System.currentTimeMillis();
      for (int i = 0; i < 2; i++) {
        statement.execute(
            String.format(
                "INSERT INTO %s(time, tag1, s1) VALUES (%d, 'tag', %d)", TABLE, baseTime + i, i));
        statement.execute("FLUSH");
      }
    }

    Awaitility.await()
        .pollInterval(200, TimeUnit.MILLISECONDS)
        .atMost(COMPACTION_WAIT_SECONDS, TimeUnit.SECONDS)
        .until(() -> countTsFiles(producerEnv) >= 2);
    return countTsFiles(producerEnv);
  }

  private static void enableSeqSpaceCompaction(final BaseEnv env) throws Exception {
    try (final Connection connection = env.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      statement.execute("SET CONFIGURATION enable_seq_space_compaction='true'");
    }
  }

  private static void loadIntoReceiver(final Path tsFile) throws Exception {
    try (final Connection connection = loaderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE IF NOT EXISTS " + DATABASE);
      statement.execute("USE " + DATABASE);
      statement.execute(
          "CREATE TABLE IF NOT EXISTS " + TABLE + " (tag1 STRING TAG, s1 INT32 FIELD)");
      statement.execute(String.format("LOAD '%s'", tsFile.toAbsolutePath()));
    }
  }

  private static void assertLoadedRowCount(final long expected) throws Exception {
    try (final Connection connection = loaderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      try (final ResultSet resultSet = statement.executeQuery("SELECT count(*) FROM " + TABLE)) {
        Assert.assertTrue("LOAD result set is empty", resultSet.next());
        Assert.assertEquals(expected, resultSet.getLong(1));
      }
    }
  }

  private static void assertTableSchemaRegistered(final Path tsFile) throws IOException {
    try (final TsFileSequenceReader reader =
        new TsFileSequenceReader(tsFile.toAbsolutePath().toString())) {
      final Map<String, TableSchema> tableSchemaMap = reader.getTableSchemaMap();
      Assert.assertNotNull("TsFile has no table schema map", tableSchemaMap);
      Assert.assertTrue(
          "TsFile has no schema entry for table " + TABLE + ": " + tableSchemaMap,
          tableSchemaMap.containsKey(TABLE));
      Assert.assertFalse(
          "The storage path must not synthesize a TIME column: " + tableSchemaMap.get(TABLE),
          tableSchemaMap.get(TABLE).getColumnTypes().contains(ColumnCategory.TIME));
      Assert.assertTrue(
          "A table-model TsFile must carry the " + TABLE_MODEL_PROPERTY + " property",
          reader.readFileMetadata().getTsFileProperties().containsKey(TABLE_MODEL_PROPERTY));
    }
  }

  private static void assertAllTsFilesHaveTableSchema(final BaseEnv env) throws IOException {
    final Path dataPath = Paths.get(env.getDataNodeWrapper(0).getDataPath());
    try (final Stream<Path> paths = Files.walk(dataPath)) {
      final List<Path> tsFiles =
          paths
              .filter(Files::isRegularFile)
              .filter(path -> path.getFileName().toString().endsWith(TsFileConstant.TSFILE_SUFFIX))
              .collect(Collectors.toList());
      Assert.assertFalse("No TsFile was produced", tsFiles.isEmpty());
      for (final Path tsFile : tsFiles) {
        assertTableSchemaRegistered(tsFile);
      }
    }
  }

  private static long countTsFiles(final BaseEnv env) throws IOException {
    final Path dataPath = Paths.get(env.getDataNodeWrapper(0).getDataPath());
    if (!Files.exists(dataPath)) {
      return 0;
    }
    try (final Stream<Path> paths = Files.walk(dataPath)) {
      return paths
          .filter(Files::isRegularFile)
          .filter(path -> path.getFileName().toString().endsWith(TsFileConstant.TSFILE_SUFFIX))
          .count();
    }
  }

  private static Path findSingleTsFile(final BaseEnv env) throws IOException {
    final Path dataPath = Paths.get(env.getDataNodeWrapper(0).getDataPath());
    try (final Stream<Path> paths = Files.walk(dataPath)) {
      final List<Path> tsFiles =
          paths
              .filter(Files::isRegularFile)
              .filter(path -> path.getFileName().toString().endsWith(TsFileConstant.TSFILE_SUFFIX))
              .collect(Collectors.toList());
      Assert.assertEquals(
          "Expected exactly one compacted TsFile, but got " + tsFiles, 1, tsFiles.size());
      return tsFiles.get(0);
    }
  }

  private static void deleteRecursively(final Path path) throws IOException {
    if (!Files.exists(path)) {
      return;
    }
    try (final Stream<Path> paths = Files.walk(path)) {
      paths
          .sorted((left, right) -> right.getNameCount() - left.getNameCount())
          .forEach(
              current -> {
                try {
                  Files.deleteIfExists(current);
                } catch (final IOException e) {
                  throw new RuntimeException(e);
                }
              });
    }
  }
}
