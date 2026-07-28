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

package org.apache.iotdb.relational.it.query.recent;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class})
public class IoTDBQueryWithCorruptedTsFileIT {
  private static final String DATABASE_NAME = "test_corrupted_read_tsfile";

  private static File tmpDir;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + DATABASE_NAME);
    }
  }

  @Before
  public void setUpBeforeTest() throws IOException {
    tmpDir = new File(Files.createTempDirectory("corrupt-tsfile").toUri());
  }

  @After
  public void tearDownAfterTest() {
    deleteTmpDir();
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testReadTsFileWithCorruptedMetadataIndexNode() throws Exception {
    File tsFile = new File(tmpDir, "corrupt-meta.tsfile");
    try (TsFileWriter writer = new TsFileWriter(tsFile)) {
      generateTable(
          writer,
          "table1",
          Arrays.asList("tag1"),
          Arrays.asList("s1"),
          TSDataType.INT64,
          1,
          1,
          TSFileDescriptor.getInstance().getConfig().getMaxDegreeOfIndexNode() + 1);
    }

    // TsFile layout: [Header] [Data] [MetadataIndex Tree] [TsFileMetadata] [Magic][Size]
    //                                    ↑                         ↑ fileMetadataPos
    //
    // More devices than the maximum index-node degree force the root device node into
    // TsFileMetadata and its child device index nodes onto disk immediately before it.
    // MetadataIndexNode serialization:
    //   [entryCount (varInt)] [entry1]...[entryN] [endOffset (long, 8B)] [nodeType (1B)]
    //   nodeType valid values: 0=INTERNAL_DEVICE, 1=LEAF_DEVICE, 2=INTERNAL_MEASUREMENT,
    // 3=LEAF_MEASUREMENT
    //   Therefore fileMetadataPos - 1 is the nodeType of the last child device index node.
    long fileMetadataPos;
    try (TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
      fileMetadataPos = reader.getFileMetadataPos();
    }

    byte[] fileBytes = Files.readAllBytes(tsFile.toPath());
    // Corrupt the nodeType byte to 0xFF (all valid types are 0-3).
    fileBytes[(int) fileMetadataPos - 1] = (byte) 0xFF;
    Files.write(tsFile.toPath(), fileBytes);

    tableAssertTestFail(
        "SELECT * FROM read_tsfile(PATHS => '" + toSqlPath(tsFile) + "')",
        "Failed to read metadata index node from TsFile: " + tsFile,
        DATABASE_NAME);
  }

  @Test
  public void testReadTsFileWithCorruptedChunkDataOrPageReader() throws Exception {
    File tsFile = new File(tmpDir, "corrupt-chunk-or-page-reader.tsfile");
    try (TsFileWriter writer = new TsFileWriter(tsFile)) {
      generateTable(
          writer,
          "table1",
          Arrays.asList("tag1"),
          Arrays.asList("s1"),
          TSDataType.INT64,
          1,
          100,
          2);
    }

    corruptDataSection(tsFile);

    tableAssertTestFail(
        "SELECT * FROM read_tsfile(PATHS => '" + toSqlPath(tsFile) + "')",
        "Failed to read chunk data or load page reader from TsFile: " + tsFile,
        DATABASE_NAME);
  }

  @Test
  public void testNormalQueryWithCorruptedChunkDataOrPageReader() throws Exception {
    String tableName = "corrupt_table";
    // 1. Create table and insert data via session — generates TsFiles in the data directory
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      statement.execute(
          "CREATE TABLE " + tableName + "(device_id STRING TAG, s1 INT64 FIELD, s2 INT64 FIELD)");
      for (int i = 1; i <= 200; i++) {
        statement.execute(
            "INSERT INTO "
                + tableName
                + "(time, device_id, s1, s2) VALUES("
                + i
                + ", 'd"
                + (i % 10)
                + "', "
                + i
                + ", "
                + (i * 10)
                + ")");
      }
      statement.execute("FLUSH");
    }

    // 2. Find the generated TsFile in the data directory
    File sequenceDir =
        new File(
            EnvFactory.getEnv().getDataNodeWrapper(0).getDataNodeDir()
                + File.separator
                + "data"
                + File.separator
                + "sequence");
    File tsFile = findTsFileRecursively(sequenceDir);
    if (tsFile == null) {
      fail("Could not find TsFile in data directory: " + sequenceDir.getAbsolutePath());
    }

    // 3. Corrupt the data section of the TsFile
    corruptDataSection(tsFile);

    // 4. Query — should fail with a corruption message that does NOT include the file path
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      try {
        statement.execute("SELECT * FROM " + tableName + " ORDER BY time");
        fail("Expected query on corrupted TsFile to fail");
      } catch (SQLException e) {
        assertTrue(
            e.getMessage(),
            e.getMessage()
                .contains(
                    "Failed to read chunk data or load page reader. The TsFile may be corrupted,"
                        + " please check the logs for the corrupted file path."));
      }
    }
  }

  /**
   * Corrupts a block of bytes in the data section of a TsFile (between the header and the
   * MetadataIndex tree) to exercise the combined chunk-data/page-reader loading stage. Uses {@link
   * TsFileSequenceReader} to read {@code metaOffset} from TsFileMetadata so metadata is not
   * corrupted.
   *
   * <p>TsFile layout: [Header] [Data chunks] [MetadataIndex Tree] [TsFileMetadata] [Magic][Size] ↑
   * metaOffset
   */
  private static void corruptDataSection(File tsFile) throws IOException {
    long metaOffset;
    try (TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
      metaOffset = reader.readFileMetadata().getMetaOffset();
    }

    byte[] fileBytes = Files.readAllBytes(tsFile.toPath());
    int magicLen = TSFileConfig.MAGIC_STRING.getBytes().length;
    int dataStart = magicLen + Byte.BYTES;
    int dataEnd = (int) metaOffset;
    // Corrupt bytes in the middle of the data section to make chunk/page-reader loading fail.
    int middle = dataStart + (dataEnd - dataStart) / 2;
    int corruptLen = Math.min(512, dataEnd - middle);
    for (int i = 0; i < corruptLen; i++) {
      fileBytes[middle + i] ^= 0xFF;
    }
    Files.write(tsFile.toPath(), fileBytes);
  }

  private static File findTsFileRecursively(File dir) {
    if (dir == null || !dir.exists()) {
      return null;
    }
    File[] files = dir.listFiles();
    if (files == null) {
      return null;
    }
    for (File file : files) {
      if (file.isDirectory()) {
        File found = findTsFileRecursively(file);
        if (found != null) {
          return found;
        }
      } else if (file.getName().endsWith(".tsfile") && file.length() > 0) {
        return file;
      }
    }
    return null;
  }

  private static void generateTable(
      TsFileWriter writer,
      String tableName,
      List<String> tagColumns,
      List<String> fieldColumns,
      TSDataType fieldType,
      long startTime,
      long endTime,
      int deviceCount)
      throws IOException, WriteProcessException {
    List<String> columnNames = new ArrayList<>(tagColumns.size() + fieldColumns.size());
    List<TSDataType> columnTypes = new ArrayList<>(tagColumns.size() + fieldColumns.size());
    List<ColumnCategory> columnCategories =
        new ArrayList<>(tagColumns.size() + fieldColumns.size());
    for (String tagColumn : tagColumns) {
      columnNames.add(tagColumn);
      columnTypes.add(TSDataType.STRING);
      columnCategories.add(ColumnCategory.TAG);
    }
    for (String fieldColumn : fieldColumns) {
      columnNames.add(fieldColumn);
      columnTypes.add(fieldType);
      columnCategories.add(ColumnCategory.FIELD);
    }

    writer.registerTableSchema(
        new TableSchema(tableName, columnNames, columnTypes, columnCategories));
    Tablet tablet = new Tablet(tableName, columnNames, columnTypes, columnCategories);
    for (int deviceIndex = 1; deviceIndex <= deviceCount; deviceIndex++) {
      for (long time = startTime; time <= endTime; time++) {
        int row = tablet.getRowSize();
        tablet.addTimestamp(row, time);
        for (int i = 0; i < tagColumns.size(); i++) {
          tablet.addValue(row, i, tagColumns.get(i) + "_" + deviceIndex);
        }
        for (int i = 0; i < fieldColumns.size(); i++) {
          tablet.addValue(row, tagColumns.size() + i, time);
        }
        if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
          writer.writeTable(tablet);
          tablet.reset();
        }
      }
    }
    if (tablet.getRowSize() != 0) {
      writer.writeTable(tablet);
    }
  }

  private static String toSqlPath(File file) {
    return file.getAbsolutePath().replace("\\", "\\\\").replace("'", "''");
  }

  private static void deleteTmpDir() {
    if (tmpDir == null || !tmpDir.exists()) {
      return;
    }
    File[] files = tmpDir.listFiles();
    if (files != null) {
      for (File file : files) {
        deleteRecursively(file);
      }
    }
    try {
      Files.delete(tmpDir.toPath());
    } catch (IOException ignored) {
      // ignore
    }
  }

  private static void deleteRecursively(File file) {
    if (file.isDirectory()) {
      File[] children = file.listFiles();
      if (children != null) {
        for (File child : children) {
          deleteRecursively(child);
        }
      }
    }
    try {
      Files.delete(file.toPath());
    } catch (IOException ignored) {
      // ignore
    }
  }
}
