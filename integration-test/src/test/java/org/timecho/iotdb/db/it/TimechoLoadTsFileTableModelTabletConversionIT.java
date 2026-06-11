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

package org.timecho.iotdb.db.it;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;

/**
 * Integration tests for loading tree-model TsFiles via table-model {@link ITableSession}.
 *
 * <p>Common setup for every case:
 *
 * <ul>
 *   <li>Schema is created with tree-model SQL ({@code CREATE TIMESERIES}, {@code RENAME TO} alias).
 *   <li>TsFiles are written in tree-model format ({@code TsFileWriter.writeTree}).
 *   <li>LOAD is executed through {@link ITableSession#executeNonQueryStatement(String)}.
 *   <li>Query verification uses tree-model JDBC ({@code SELECT ...}).
 * </ul>
 *
 * <p>Scenario matrix:
 *
 * <ul>
 *   <li>Mixed physical + alias + normal, no flag → LOAD fails; with flag → all paths readable
 *   <li>Alias-path TsFile only, no flag → tablet conversion fallback, data on alias
 *   <li>Physical-path TsFile only, no flag → fail; with flag → data on alias, not on physical
 *   <li>Normal-path TsFile only, no flag → direct LOAD success
 *   <li>Alias-path TsFile with flag → still succeeds via tablet conversion
 *   <li>Aligned partial rename + alias-path TsFile → tablet conversion, alias readable
 *   <li>Aligned physical-path TsFile, no flag → fail; with flag → alias readable
 *   <li>Mixed alias + invalid physical without flag → LOAD fails, no partial write
 *   <li>Mixed alias + normal + invalid physical without flag → LOAD fails, no partial write
 *   <li>All-physical-path TsFile without flag → LOAD fails at analyze
 *   <li>Multiple physical paths + alias path without flag → LOAD fails, no partial write
 *   <li>Aligned physical path + alias path in one TsFile without flag → LOAD fails
 * </ul>
 */
@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class TimechoLoadTsFileTableModelTabletConversionIT {

  private static final String MIXED_DB = "root.timecho_table_session_load_mixed";
  private static final String MIXED_FAIL_DB = "root.timecho_table_session_load_mixed_fail";
  private static final String ALIAS_DB = "root.timecho_table_session_load_alias";
  private static final String PHYSICAL_DB = "root.timecho_table_session_load_physical";
  private static final String NORMAL_DB = "root.timecho_table_session_load_normal";
  private static final String ALIGNED_DB = "root.timecho_table_session_load_aligned";
  private static final String MULTI_FAIL_DB = "root.timecho_table_session_load_multi_fail";

  private static File tempDir;

  @BeforeClass
  public static void setUpClass() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setPipeMemoryManagementEnabled(false);
    EnvFactory.getEnv().initClusterEnvironment();
    tempDir = Files.createTempDirectory("timecho-table-session-load-tree").toFile();
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
    deleteRecursively(tempDir);
  }

  /**
   * Tests a single TsFile that mixes three path styles after schema rename: invalid physical path,
   * renamed alias path, and normal path. LOAD is issued twice via table session.
   *
   * <p>Expected:
   *
   * <ul>
   *   <li>Without {@code tsfile-is-physical-path}: LOAD fails (invalid physical series in TsFile).
   *   <li>With {@code tsfile-is-physical-path=true}: LOAD succeeds; physical-path data is mapped to
   *       alias; normal-path data stays on normal path; all three SELECT results match written
   *       values.
   * </ul>
   */
  @Test
  public void testLoadMixedAliasNormalPhysicalTreeTsFileViaTableSession() throws Exception {
    final File tsFile;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      createDatabase(statement, MIXED_DB);

      createTimeseries(statement, MIXED_DB + ".src.d1.s_float", TSDataType.FLOAT);
      createTimeseries(statement, MIXED_DB + ".src.d2.s_text_origin", TSDataType.TEXT);
      createTimeseries(statement, MIXED_DB + ".normal.d1.s_plain", TSDataType.INT32);

      rename(statement, MIXED_DB + ".src.d1.s_float", MIXED_DB + ".alias.d1.s_float");
      rename(
          statement, MIXED_DB + ".src.d2.s_text_origin", MIXED_DB + ".alias_step1.d2.s_text_alias");
      rename(
          statement,
          MIXED_DB + ".alias_step1.d2.s_text_alias",
          MIXED_DB + ".alias_step2.d2.s_text_alias_v2");

      tsFile =
          writeTsFile(
              "mixed-tree.tsfile",
              new DeviceSpec(
                  MIXED_DB + ".src.d1",
                  schemas(schema("s_float", TSDataType.FLOAT)),
                  3L,
                  new Object[] {3.5F}),
              new DeviceSpec(
                  MIXED_DB + ".alias_step2.d2",
                  schemas(schema("s_text_alias_v2", TSDataType.TEXT)),
                  3L,
                  new Object[] {"mixed-alias"}),
              new DeviceSpec(
                  MIXED_DB + ".normal.d1",
                  schemas(schema("s_plain", TSDataType.INT32)),
                  3L,
                  new Object[] {7}));
    }

    assertLoadViaTableSessionFail(tsFile, "Cannot insert data into invalid series");
    loadViaTableSession(tsFile, true);

    resultSetEqualTest(
        "SELECT s_float FROM " + MIXED_DB + ".alias.d1",
        "Time," + MIXED_DB + ".alias.d1.s_float,",
        new String[] {"3,3.5,"});
    resultSetEqualTest(
        "SELECT s_text_alias_v2 FROM " + MIXED_DB + ".alias_step2.d2",
        "Time," + MIXED_DB + ".alias_step2.d2.s_text_alias_v2,",
        new String[] {"3,mixed-alias,"});
    resultSetEqualTest(
        "SELECT s_plain FROM " + MIXED_DB + ".normal.d1",
        "Time," + MIXED_DB + ".normal.d1.s_plain,",
        new String[] {"3,7,"});
  }

  /**
   * Tests alias-only tree TsFile (paths already renamed in schema). TsFile uses alias measurement
   * names; LOAD goes through analyze-time tablet conversion fallback.
   *
   * <p>Expected:
   *
   * <ul>
   *   <li>Default LOAD (no physical-path flag) succeeds.
   *   <li>Data is readable on alias paths ({@code view.d1}).
   *   <li>Disabled physical paths ({@code src.d1}) remain empty.
   * </ul>
   */
  @Test
  public void testLoadAliasPathTreeTsFileViaTableSessionTriggersTabletConversion()
      throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      createDatabase(statement, ALIAS_DB);
      createTimeseries(statement, ALIAS_DB + ".src.d1.s_alias", TSDataType.INT32);
      createTimeseries(statement, ALIAS_DB + ".src.d1.s_text_origin", TSDataType.TEXT);
      rename(statement, ALIAS_DB + ".src.d1.s_alias", ALIAS_DB + ".view.d1.s_alias");
      rename(statement, ALIAS_DB + ".src.d1.s_text_origin", ALIAS_DB + ".view.d1.s_text_renamed");

      final File tsFile =
          writeTsFile(
              "alias-tree.tsfile",
              new DeviceSpec(
                  ALIAS_DB + ".view.d1",
                  schemas(
                      schema("s_alias", TSDataType.INT32),
                      schema("s_text_renamed", TSDataType.TEXT)),
                  5L,
                  new Object[] {55, "alias-text"}));

      loadViaTableSession(tsFile, false);
    }

    resultSetEqualTest(
        "SELECT s_alias FROM " + ALIAS_DB + ".view.d1",
        "Time," + ALIAS_DB + ".view.d1.s_alias,",
        new String[] {"5,55,"});
    resultSetEqualTest(
        "SELECT s_text_renamed FROM " + ALIAS_DB + ".view.d1",
        "Time," + ALIAS_DB + ".view.d1.s_text_renamed,",
        new String[] {"5,alias-text,"});
    assertRowCount("SELECT s_alias FROM " + ALIAS_DB + ".src.d1", 0);
  }

  /**
   * Tests TsFile whose device/measurement paths are disabled physical paths (series already renamed
   * to alias in schema).
   *
   * <p>Expected:
   *
   * <ul>
   *   <li>Without flag: LOAD fails with {@code cannot accept load data}.
   *   <li>With {@code tsfile-is-physical-path=true}: LOAD succeeds; data appears on alias path;
   *       physical path returns zero rows.
   * </ul>
   */
  @Test
  public void testLoadPhysicalPathTreeTsFileViaTableSessionRequiresPhysicalPathFlag()
      throws Exception {
    final File tsFile;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      createDatabase(statement, PHYSICAL_DB);
      createTimeseries(statement, PHYSICAL_DB + ".src.d1.s_long", TSDataType.INT64);
      rename(statement, PHYSICAL_DB + ".src.d1.s_long", PHYSICAL_DB + ".alias.d1.s_long");

      tsFile =
          writeTsFile(
              "physical-tree.tsfile",
              new DeviceSpec(
                  PHYSICAL_DB + ".src.d1",
                  schemas(schema("s_long", TSDataType.INT64)),
                  6L,
                  new Object[] {600L}));
    }

    assertLoadViaTableSessionFail(tsFile, "cannot accept load data");
    loadViaTableSession(tsFile, true);

    resultSetEqualTest(
        "SELECT s_long FROM " + PHYSICAL_DB + ".alias.d1",
        "Time," + PHYSICAL_DB + ".alias.d1.s_long,",
        new String[] {"6,600,"});
    assertRowCount("SELECT s_long FROM " + PHYSICAL_DB + ".src.d1", 0);
  }

  /**
   * Tests TsFile containing only normal (non-renamed) series. No alias or invalid physical paths
   * are involved.
   *
   * <p>Expected:
   *
   * <ul>
   *   <li>Default LOAD succeeds via direct TsFile load (no tablet conversion fallback needed).
   *   <li>SELECT returns the written values on the normal device path.
   * </ul>
   */
  @Test
  public void testLoadNormalPathTreeTsFileViaTableSessionDirectLoad() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      createDatabase(statement, NORMAL_DB);
      createTimeseries(statement, NORMAL_DB + ".plain.d1.s1", TSDataType.INT32);
      createTimeseries(statement, NORMAL_DB + ".plain.d1.s2", TSDataType.FLOAT);

      final File tsFile =
          writeTsFile(
              "normal-tree.tsfile",
              new DeviceSpec(
                  NORMAL_DB + ".plain.d1",
                  schemas(schema("s1", TSDataType.INT32), schema("s2", TSDataType.FLOAT)),
                  10L,
                  new Object[] {42, 3.14F}));

      loadViaTableSession(tsFile, false);
    }

    resultSetEqualTest(
        "SELECT s1, s2 FROM " + NORMAL_DB + ".plain.d1",
        "Time," + NORMAL_DB + ".plain.d1.s1," + NORMAL_DB + ".plain.d1.s2,",
        new String[] {"10,42,3.14,"});
  }

  /**
   * Tests alias-path TsFile when {@code tsfile-is-physical-path=true} is also set. Alias-path loads
   * still require tablet conversion even with the flag enabled.
   *
   * <p>Expected:
   *
   * <ul>
   *   <li>LOAD succeeds.
   *   <li>Data is readable on alias path.
   *   <li>Physical path remains empty.
   * </ul>
   */
  @Test
  public void testLoadAliasPathTreeTsFileViaTableSessionWithPhysicalPathFlagStillSucceeds()
      throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      createDatabase(statement, ALIAS_DB + "_flag");
      final String database = ALIAS_DB + "_flag";
      createTimeseries(statement, database + ".src.d1.s_flag", TSDataType.DOUBLE);
      rename(statement, database + ".src.d1.s_flag", database + ".alias.d1.s_flag");

      final File tsFile =
          writeTsFile(
              "alias-with-flag-tree.tsfile",
              new DeviceSpec(
                  database + ".alias.d1",
                  schemas(schema("s_flag", TSDataType.DOUBLE)),
                  7L,
                  new Object[] {7.7D}));

      loadViaTableSession(tsFile, true);
    }

    final String database = ALIAS_DB + "_flag";
    resultSetEqualTest(
        "SELECT s_flag FROM " + database + ".alias.d1",
        "Time," + database + ".alias.d1.s_flag,",
        new String[] {"7,7.7,"});
    assertRowCount("SELECT s_flag FROM " + database + ".src.d1", 0);
  }

  /**
   * Tests aligned device where one measurement was renamed on the same device. TsFile uses alias
   * measurement names on the aligned device.
   *
   * <p>Expected:
   *
   * <ul>
   *   <li>Default LOAD succeeds through tablet conversion fallback.
   *   <li>Renamed aligned measurement ({@code s1_alias}) and sibling ({@code s2}) are readable.
   *   <li>Original physical measurement name ({@code s1_physical}) returns zero rows.
   * </ul>
   */
  @Test
  public void testLoadAlignedPartialRenameTreeTsFileViaTableSession() throws Exception {
    final String device = ALIGNED_DB + ".src.d1";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      createDatabase(statement, ALIGNED_DB);
      createAlignedTimeseries(
          statement,
          device,
          "s1_physical INT32 encoding=RLE compression=SNAPPY",
          "s2 INT32 encoding=RLE compression=SNAPPY");
      rename(statement, device + ".s1_physical", device + ".s1_alias");

      final File tsFile =
          writeAlignedTsFile(
              "aligned-alias-tree.tsfile",
              new AlignedDeviceSpec(
                  device,
                  schemas(schema("s1_alias", TSDataType.INT32), schema("s2", TSDataType.INT32)),
                  8L,
                  new Object[] {800, 900}));

      loadViaTableSession(tsFile, false);
    }

    resultSetEqualTest(
        "SELECT s1_alias, s2 FROM " + device,
        "Time," + device + ".s1_alias," + device + ".s2,",
        new String[] {"8,800,900,"});
    assertRowCount("SELECT s1_physical FROM " + device, 0);
  }

  /**
   * Tests aligned TsFile written with disabled physical measurement names ({@code s1_physical})
   * after partial rename on the same aligned device.
   *
   * <p>Expected:
   *
   * <ul>
   *   <li>Without flag: LOAD fails with {@code cannot accept load data}.
   *   <li>With {@code tsfile-is-physical-path=true}: LOAD succeeds; data mapped to {@code
   *       s1_alias}; {@code s1_physical} returns zero rows; sibling {@code s2} unchanged.
   * </ul>
   */
  @Test
  public void testLoadAlignedPhysicalPathTreeTsFileViaTableSessionWithPhysicalPathFlag()
      throws Exception {
    final String device = ALIGNED_DB + ".phys.d1";
    final File tsFile;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      createDatabase(statement, ALIGNED_DB);
      createAlignedTimeseries(
          statement,
          device,
          "s1_physical INT32 encoding=RLE compression=SNAPPY",
          "s2 INT32 encoding=RLE compression=SNAPPY");
      rename(statement, device + ".s1_physical", device + ".s1_alias");

      tsFile =
          writeAlignedTsFile(
              "aligned-physical-tree.tsfile",
              new AlignedDeviceSpec(
                  device,
                  schemas(schema("s1_physical", TSDataType.INT32), schema("s2", TSDataType.INT32)),
                  9L,
                  new Object[] {910, 920}));
    }

    assertLoadViaTableSessionFail(tsFile, "cannot accept load data");
    loadViaTableSession(tsFile, true);

    resultSetEqualTest(
        "SELECT s1_alias, s2 FROM " + device,
        "Time," + device + ".s1_alias," + device + ".s2,",
        new String[] {"9,910,920,"});
    assertRowCount("SELECT s1_physical FROM " + device, 0);
  }

  // -------------------------------------------------------------------------
  // Failure paths without tsfile-is-physical-path (alias + invalid physical mixed)
  // -------------------------------------------------------------------------

  /**
   * Tests one TsFile mixing invalid physical-path write and alias-path write, without physical-path
   * flag. Schema has corresponding alias series for both devices.
   *
   * <p>Expected:
   *
   * <ul>
   *   <li>Table-session LOAD fails (analyze or tablet conversion rejects invalid physical data).
   *   <li>No partial success: alias paths and physical paths all have zero rows after failure.
   * </ul>
   */
  @Test
  public void
      testLoadMixedAliasAndPhysicalPathsTreeTsFileViaTableSessionFailsWithoutPhysicalPathFlag()
          throws Exception {
    final String database = MIXED_FAIL_DB + "_alias_physical";
    final File tsFile;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      createDatabase(statement, database);
      createTimeseries(statement, database + ".src.d1.s_float", TSDataType.FLOAT);
      createTimeseries(statement, database + ".src.d2.s_text_origin", TSDataType.TEXT);
      rename(statement, database + ".src.d1.s_float", database + ".alias.d1.s_float");
      rename(statement, database + ".src.d2.s_text_origin", database + ".alias.d2.s_text_alias");

      tsFile =
          writeTsFile(
              "alias-physical-mixed-fail.tsfile",
              new DeviceSpec(
                  database + ".src.d1",
                  schemas(schema("s_float", TSDataType.FLOAT)),
                  11L,
                  new Object[] {1.1F}),
              new DeviceSpec(
                  database + ".alias.d2",
                  schemas(schema("s_text_alias", TSDataType.TEXT)),
                  11L,
                  new Object[] {"alias-and-physical"}));
    }

    assertLoadViaTableSessionFail(
        tsFile, "Cannot insert data into invalid series", "cannot accept load data");
    assertRowCount("SELECT s_float FROM " + database + ".alias.d1", 0);
    assertRowCount("SELECT s_text_alias FROM " + database + ".alias.d2", 0);
    assertRowCount("SELECT s_float FROM " + database + ".src.d1", 0);
  }

  /**
   * Tests one TsFile mixing invalid physical path, alias path, and normal path, without
   * physical-path flag. Ensures failure is not silently ignored when normal/alias data is also
   * present.
   *
   * <p>Expected:
   *
   * <ul>
   *   <li>Table-session LOAD fails.
   *   <li>Alias, normal, and physical paths all remain empty (no partial write).
   * </ul>
   */
  @Test
  public void
      testLoadMixedAliasNormalPhysicalTreeTsFileViaTableSessionFailsWithoutPhysicalPathFlag()
          throws Exception {
    final String database = MIXED_FAIL_DB + "_alias_normal_physical";
    final File tsFile;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      createDatabase(statement, database);
      createTimeseries(statement, database + ".src.d1.s_float", TSDataType.FLOAT);
      createTimeseries(statement, database + ".src.d2.s_text_origin", TSDataType.TEXT);
      createTimeseries(statement, database + ".normal.d1.s_plain", TSDataType.INT32);
      rename(statement, database + ".src.d1.s_float", database + ".alias.d1.s_float");
      rename(
          statement,
          database + ".src.d2.s_text_origin",
          database + ".alias_step2.d2.s_text_alias_v2");

      tsFile =
          writeTsFile(
              "alias-normal-physical-fail.tsfile",
              new DeviceSpec(
                  database + ".src.d1",
                  schemas(schema("s_float", TSDataType.FLOAT)),
                  12L,
                  new Object[] {2.2F}),
              new DeviceSpec(
                  database + ".alias_step2.d2",
                  schemas(schema("s_text_alias_v2", TSDataType.TEXT)),
                  12L,
                  new Object[] {"should-not-load"}),
              new DeviceSpec(
                  database + ".normal.d1",
                  schemas(schema("s_plain", TSDataType.INT32)),
                  12L,
                  new Object[] {12}));
    }

    assertLoadViaTableSessionFail(
        tsFile, "Cannot insert data into invalid series", "cannot accept load data");
    assertRowCount("SELECT s_float FROM " + database + ".alias.d1", 0);
    assertRowCount("SELECT s_text_alias_v2 FROM " + database + ".alias_step2.d2", 0);
    assertRowCount("SELECT s_plain FROM " + database + ".normal.d1", 0);
  }

  /**
   * Tests TsFile where every device uses disabled physical paths only (all target series were
   * renamed to alias). No alias-path entries exist in the TsFile.
   *
   * <p>Expected:
   *
   * <ul>
   *   <li>Without flag: LOAD fails with {@code cannot accept load data}.
   *   <li>Alias paths and physical paths both have zero rows (nothing loaded).
   * </ul>
   */
  @Test
  public void testLoadAllPhysicalPathsTreeTsFileViaTableSessionFailsWithoutPhysicalPathFlag()
      throws Exception {
    final String database = MIXED_FAIL_DB + "_all_physical";
    final File tsFile;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      createDatabase(statement, database);
      createTimeseries(statement, database + ".src.d1.s_a", TSDataType.INT32);
      createTimeseries(statement, database + ".src.d2.s_b", TSDataType.INT64);
      rename(statement, database + ".src.d1.s_a", database + ".alias.d1.s_a");
      rename(statement, database + ".src.d2.s_b", database + ".alias.d2.s_b");

      tsFile =
          writeTsFile(
              "all-physical-fail.tsfile",
              new DeviceSpec(
                  database + ".src.d1",
                  schemas(schema("s_a", TSDataType.INT32)),
                  13L,
                  new Object[] {130}),
              new DeviceSpec(
                  database + ".src.d2",
                  schemas(schema("s_b", TSDataType.INT64)),
                  13L,
                  new Object[] {1300L}));
    }

    assertLoadViaTableSessionFail(tsFile, "cannot accept load data");
    assertRowCount("SELECT s_a FROM " + database + ".alias.d1", 0);
    assertRowCount("SELECT s_b FROM " + database + ".alias.d2", 0);
    assertRowCount("SELECT s_a FROM " + database + ".src.d1", 0);
    assertRowCount("SELECT s_b FROM " + database + ".src.d2", 0);
  }

  /**
   * Tests TsFile with multiple invalid physical-path devices plus one alias-path device in the same
   * file, without physical-path flag.
   *
   * <p>Expected:
   *
   * <ul>
   *   <li>Table-session LOAD fails before or during tablet conversion.
   *   <li>All alias paths involved in the TsFile remain empty (including the alias-only device).
   * </ul>
   */
  @Test
  public void
      testLoadMultiplePhysicalPathsWithAliasTreeTsFileViaTableSessionFailsWithoutPhysicalPathFlag()
          throws Exception {
    final String database = MULTI_FAIL_DB;
    final File tsFile;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      createDatabase(statement, database);
      createTimeseries(statement, database + ".src.d1.s_a", TSDataType.INT32);
      createTimeseries(statement, database + ".src.d2.s_b", TSDataType.INT64);
      createTimeseries(statement, database + ".src.d3.s_c", TSDataType.TEXT);
      rename(statement, database + ".src.d1.s_a", database + ".alias.d1.s_a");
      rename(statement, database + ".src.d2.s_b", database + ".alias.d2.s_b");
      rename(statement, database + ".src.d3.s_c", database + ".alias.d3.s_c");

      tsFile =
          writeTsFile(
              "multi-physical-with-alias-fail.tsfile",
              new DeviceSpec(
                  database + ".src.d1",
                  schemas(schema("s_a", TSDataType.INT32)),
                  14L,
                  new Object[] {1}),
              new DeviceSpec(
                  database + ".src.d2",
                  schemas(schema("s_b", TSDataType.INT64)),
                  14L,
                  new Object[] {2L}),
              new DeviceSpec(
                  database + ".alias.d3",
                  schemas(schema("s_c", TSDataType.TEXT)),
                  14L,
                  new Object[] {"alias-only-path"}));
    }

    assertLoadViaTableSessionFail(
        tsFile, "Cannot insert data into invalid series", "cannot accept load data");
    assertRowCount("SELECT s_a FROM " + database + ".alias.d1", 0);
    assertRowCount("SELECT s_b FROM " + database + ".alias.d2", 0);
    assertRowCount("SELECT s_c FROM " + database + ".alias.d3", 0);
  }

  /**
   * Tests one TsFile containing both aligned invalid physical-path data and non-aligned alias-path
   * data, without physical-path flag.
   *
   * <p>Expected:
   *
   * <ul>
   *   <li>Table-session LOAD fails because aligned physical-path chunk is invalid without the flag.
   *   <li>Neither aligned alias/sibling measurements nor the separate alias device receive data.
   * </ul>
   */
  @Test
  public void
      testLoadAlignedMixedAliasAndPhysicalPathTreeTsFileViaTableSessionFailsWithoutPhysicalPathFlag()
          throws Exception {
    final String physicalDevice = ALIGNED_DB + ".mixed_phys.d1";
    final String aliasDevice = ALIGNED_DB + ".mixed_alias.d2";
    final File tsFile = new File(tempDir, "aligned-alias-physical-mixed-fail.tsfile");
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      createDatabase(statement, ALIGNED_DB);
      createAlignedTimeseries(
          statement,
          physicalDevice,
          "s1_physical INT32 encoding=RLE compression=SNAPPY",
          "s2 INT32 encoding=RLE compression=SNAPPY");
      createTimeseries(statement, aliasDevice + ".s_text_origin", TSDataType.TEXT);
      rename(statement, physicalDevice + ".s1_physical", physicalDevice + ".s1_alias");
      rename(statement, aliasDevice + ".s_text_origin", aliasDevice + ".s_text_alias");

      try (TsFileWriter writer = new TsFileWriter(tsFile)) {
        final List<IMeasurementSchema> alignedSchemas =
            schemas(schema("s1_physical", TSDataType.INT32), schema("s2", TSDataType.INT32));
        writer.registerAlignedTimeseries(new Path(physicalDevice), alignedSchemas);
        final Tablet physicalTablet = new Tablet(physicalDevice, alignedSchemas);
        physicalTablet.addTimestamp(0, 15L);
        physicalTablet.addValue(alignedSchemas.get(0).getMeasurementName(), 0, 150);
        physicalTablet.addValue(alignedSchemas.get(1).getMeasurementName(), 0, 250);
        writer.writeTree(physicalTablet);

        final List<IMeasurementSchema> aliasSchemas =
            schemas(schema("s_text_alias", TSDataType.TEXT));
        writer.registerTimeseries(new Path(aliasDevice), aliasSchemas);
        final Tablet aliasTablet = new Tablet(aliasDevice, aliasSchemas);
        aliasTablet.addTimestamp(0, 16L);
        aliasTablet.addValue(aliasSchemas.get(0).getMeasurementName(), 0, "aligned-mixed-alias");
        writer.writeTree(aliasTablet);
      }
    }

    assertLoadViaTableSessionFail(
        tsFile, "cannot accept load data", "Cannot insert data into invalid series");
    assertRowCount("SELECT s1_alias, s2 FROM " + physicalDevice, 0);
    assertRowCount("SELECT s_text_alias FROM " + aliasDevice, 0);
  }

  // -------------------------------------------------------------------------
  // Table-session LOAD helpers
  // -------------------------------------------------------------------------

  private static void loadViaTableSession(final File tsFile, final boolean physicalPath)
      throws Exception {
    try (final ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      if (physicalPath) {
        session.executeNonQueryStatement(
            String.format(
                "load '%s' with ('tsfile-is-physical-path'='true')", tsFile.getAbsolutePath()));
      } else {
        session.executeNonQueryStatement(String.format("load '%s'", tsFile.getAbsolutePath()));
      }
    }
  }

  private static void assertLoadViaTableSessionFail(
      final File tsFile, final String... expectedMessageParts) throws Exception {
    try (final ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement(String.format("load '%s'", tsFile.getAbsolutePath()));
      Assert.fail("Expected table-session LOAD to fail.");
    } catch (final StatementExecutionException e) {
      boolean matched = false;
      for (final String messagePart : expectedMessageParts) {
        if (e.getMessage().contains(messagePart)) {
          matched = true;
          break;
        }
      }
      Assert.assertTrue("Unexpected LOAD failure message: " + e.getMessage(), matched);
    }
  }

  // -------------------------------------------------------------------------
  // Tree-model schema / TsFile helpers (same style as TimechoLoadTsFileAliasSeriesIT)
  // -------------------------------------------------------------------------

  private static void createDatabase(final Statement statement, final String database)
      throws SQLException {
    try {
      statement.execute("CREATE DATABASE " + database);
    } catch (final SQLException e) {
      if (e.getErrorCode()
          != org.apache.iotdb.rpc.TSStatusCode.DATABASE_ALREADY_EXISTS.getStatusCode()) {
        throw e;
      }
    }
  }

  private static void rename(final Statement statement, final String oldPath, final String newPath)
      throws SQLException {
    statement.execute(String.format("ALTER TIMESERIES %s RENAME TO %s", oldPath, newPath));
  }

  private static void createTimeseries(
      final Statement statement, final String path, final TSDataType dataType) throws SQLException {
    statement.execute(
        String.format(
            "CREATE TIMESERIES %s WITH DATATYPE=%s, ENCODING=%s, COMPRESSION=SNAPPY",
            path, dataType.name(), encoding(dataType).name()));
  }

  private static void createAlignedTimeseries(
      final Statement statement, final String device, final String... measurementSpecs)
      throws SQLException {
    statement.execute(
        String.format(
            "CREATE ALIGNED TIMESERIES %s(%s)", device, String.join(", ", measurementSpecs)));
  }

  private static void assertRowCount(final String sql, final int expectedCount)
      throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      int count = 0;
      while (resultSet.next()) {
        count++;
      }
      Assert.assertEquals(expectedCount, count);
    }
  }

  private static MeasurementSchema schema(final String measurement, final TSDataType dataType) {
    return new MeasurementSchema(measurement, dataType, encoding(dataType));
  }

  private static List<IMeasurementSchema> schemas(final MeasurementSchema... schemas) {
    return Arrays.asList(schemas);
  }

  private static TSEncoding encoding(final TSDataType dataType) {
    switch (dataType) {
      case FLOAT:
      case DOUBLE:
        return TSEncoding.GORILLA;
      case BOOLEAN:
      case TEXT:
      case STRING:
      case BLOB:
        return TSEncoding.PLAIN;
      default:
        return TSEncoding.RLE;
    }
  }

  private static File writeTsFile(final String fileName, final DeviceSpec... deviceSpecs)
      throws Exception {
    final File file = new File(tempDir, fileName);
    try (TsFileWriter writer = new TsFileWriter(file)) {
      for (final DeviceSpec spec : deviceSpecs) {
        writer.registerTimeseries(new Path(spec.device), spec.schemas);
      }
      for (final DeviceSpec spec : deviceSpecs) {
        final Tablet tablet = new Tablet(spec.device, spec.schemas);
        tablet.addTimestamp(0, spec.timestamp);
        for (int i = 0; i < spec.values.length; i++) {
          tablet.addValue(spec.schemas.get(i).getMeasurementName(), 0, spec.values[i]);
        }
        writer.writeTree(tablet);
      }
    }
    return file;
  }

  private static File writeAlignedTsFile(
      final String fileName, final AlignedDeviceSpec... deviceSpecs) throws Exception {
    final File file = new File(tempDir, fileName);
    try (TsFileWriter writer = new TsFileWriter(file)) {
      for (final AlignedDeviceSpec spec : deviceSpecs) {
        writer.registerAlignedTimeseries(new Path(spec.device), spec.schemas);
      }
      for (final AlignedDeviceSpec spec : deviceSpecs) {
        final Tablet tablet = new Tablet(spec.device, spec.schemas);
        tablet.addTimestamp(0, spec.timestamp);
        for (int i = 0; i < spec.values.length; i++) {
          tablet.addValue(spec.schemas.get(i).getMeasurementName(), 0, spec.values[i]);
        }
        writer.writeTree(tablet);
      }
    }
    return file;
  }

  private static void deleteRecursively(final File file) {
    if (file == null || !file.exists()) {
      return;
    }
    if (file.isDirectory()) {
      final File[] children = file.listFiles();
      if (children != null) {
        for (final File child : children) {
          deleteRecursively(child);
        }
      }
    }
    file.delete();
  }

  private static final class DeviceSpec {
    private final String device;
    private final List<IMeasurementSchema> schemas;
    private final long timestamp;
    private final Object[] values;

    private DeviceSpec(
        final String device,
        final List<IMeasurementSchema> schemas,
        final long timestamp,
        final Object[] values) {
      this.device = device;
      this.schemas = schemas;
      this.timestamp = timestamp;
      this.values = values;
    }
  }

  private static final class AlignedDeviceSpec {
    private final String device;
    private final List<IMeasurementSchema> schemas;
    private final long timestamp;
    private final Object[] values;

    private AlignedDeviceSpec(
        final String device,
        final List<IMeasurementSchema> schemas,
        final long timestamp,
        final Object[] values) {
      this.device = device;
      this.schemas = schemas;
      this.timestamp = timestamp;
      this.values = values;
    }
  }
}
