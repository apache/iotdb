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

package com.timecho.iotdb.db.it;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.TSStatusCode;

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

import static org.apache.iotdb.db.it.utils.TestUtils.assertNonQueryTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;

/**
 * Integration tests for LOAD TsFile with alias (renamed) and invalid (disabled physical) series.
 *
 * <p>Scenario matrix (TsFile path style × schema × LOAD options):
 *
 * <ul>
 *   <li>Normal path + normal schema + default LOAD → direct TsFile load, success
 *   <li>Alias path + renamed schema + default LOAD → tablet conversion, success
 *   <li>Physical (invalid) path + renamed schema + default LOAD → fail fast
 *   <li>Physical (invalid) path + renamed schema + tsfile-is-physical-path=true → direct/tablet,
 *       data visible on alias
 *   <li>Mixed physical + alias + normal + tsfile-is-physical-path=true → all succeed
 *   <li>Mixed physical + alias without flag → fail (invalid physical in tablet path)
 *   <li>Alias-only TsFile + tsfile-is-physical-path=true → still success (alias via tablet)
 *   <li>Normal-only TsFile while other series renamed → default LOAD success
 *   <li>Auto-create new device via TsFile + physical-path flag → success
 *   <li>Aligned device + partial rename on same device + alias-path aligned TsFile → tablet,
 *       success
 *   <li>Aligned device + cross-database rename + alias-path aligned TsFile → tablet, success
 *   <li>Aligned device + physical-path aligned TsFile without flag → fail
 *   <li>Aligned device + physical-path aligned TsFile with tsfile-is-physical-path=true → success
 *       on alias; sibling aligned measurements unchanged
 * </ul>
 */
@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class TimechoLoadTsFileAliasSeriesIT {

  private static final String RENAMED_DB = "root.timecho_load_alias_renamed";
  private static final String PHYSICAL_DB = "root.timecho_load_alias_physical";
  private static final String MIXED_DB = "root.timecho_load_alias_mixed";
  private static final String NORMAL_DB = "root.timecho_load_alias_normal";
  private static final String INVALID_ONLY_DB = "root.timecho_load_alias_invalid_only";
  private static final String ALIAS_FLAG_DB = "root.timecho_load_alias_alias_flag";
  private static final String NORMAL_ONLY_DB = "root.timecho_load_alias_normal_only";
  private static final String APPEND_DB = "root.timecho_load_alias_append";
  private static final String AUTO_CREATE_DB = "root.timecho_load_alias_autocreate";
  private static final String ALIAS_NORMAL_MIX_DB = "root.timecho_load_alias_alias_normal_mix";
  private static final String MULTI_INVALID_DB = "root.timecho_load_alias_multi_invalid";
  private static final String VERIFY_OFF_DB = "root.timecho_load_alias_verify_off";
  private static final String ALIGNED_ALIAS_DB = "root.timecho_load_alias_aligned";
  private static final String ALIGNED_PHYSICAL_DB = "root.timecho_load_alias_aligned_phys";
  private static final String ALIGNED_CROSS_SRC_DB = "root.timecho_load_alias_aligned_cross_src";
  private static final String ALIGNED_CROSS_VIEW_DB = "root.timecho_load_alias_aligned_cross_view";

  private static File tempDir;

  @BeforeClass
  public static void setUpClass() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setPipeMemoryManagementEnabled(false);
    EnvFactory.getEnv().initClusterEnvironment();
    tempDir = Files.createTempDirectory("timecho-load-alias").toFile();
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
    deleteRecursively(tempDir);
  }

  // -------------------------------------------------------------------------
  // Existing core scenarios (alias / physical / mixed)
  // -------------------------------------------------------------------------

  @Test
  public void testLoadRenamedAliasSeriesWithSameAndDifferentMeasurementNames() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      createDatabase(statement, RENAMED_DB);

      createTimeseries(statement, RENAMED_DB + ".src.d1.s_int_same", TSDataType.INT32);
      createTimeseries(statement, RENAMED_DB + ".src.d1.s_text_origin", TSDataType.TEXT);
      createTimeseries(statement, RENAMED_DB + ".src.d2.s_bool_origin", TSDataType.BOOLEAN);

      rename(
          statement, RENAMED_DB + ".src.d1.s_int_same", RENAMED_DB + ".alias_same.d1.s_int_same");
      rename(
          statement,
          RENAMED_DB + ".src.d1.s_text_origin",
          RENAMED_DB + ".alias_diff.d1.s_text_alias");
      rename(
          statement,
          RENAMED_DB + ".src.d2.s_bool_origin",
          RENAMED_DB + ".alias_diff.d2.s_bool_alias");

      final File tsFile =
          writeTsFile(
              "renamed.tsfile",
              new DeviceSpec(
                  RENAMED_DB + ".alias_same.d1",
                  schemas(schema("s_int_same", TSDataType.INT32)),
                  1L,
                  new Object[] {11}),
              new DeviceSpec(
                  RENAMED_DB + ".alias_diff.d1",
                  schemas(schema("s_text_alias", TSDataType.TEXT)),
                  1L,
                  new Object[] {"alias-text"}),
              new DeviceSpec(
                  RENAMED_DB + ".alias_diff.d2",
                  schemas(schema("s_bool_alias", TSDataType.BOOLEAN)),
                  1L,
                  new Object[] {true}));

      loadTsFile(statement, tsFile);
    }

    resultSetEqualTest(
        "SELECT s_int_same FROM " + RENAMED_DB + ".alias_same.d1",
        "Time," + RENAMED_DB + ".alias_same.d1.s_int_same,",
        new String[] {"1,11,"});
    resultSetEqualTest(
        "SELECT s_text_alias FROM " + RENAMED_DB + ".alias_diff.d1",
        "Time," + RENAMED_DB + ".alias_diff.d1.s_text_alias,",
        new String[] {"1,alias-text,"});
    resultSetEqualTest(
        "SELECT s_bool_alias FROM " + RENAMED_DB + ".alias_diff.d2",
        "Time," + RENAMED_DB + ".alias_diff.d2.s_bool_alias,",
        new String[] {"1,true,"});
  }

  @Test
  public void testLoadPhysicalPathTsFileRequiresPhysicalPathFlag() throws Exception {
    final File tsFile;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      createDatabase(statement, PHYSICAL_DB);

      createTimeseries(statement, PHYSICAL_DB + ".src.d1.s_long", TSDataType.INT64);
      createTimeseries(statement, PHYSICAL_DB + ".src.d1.s_double_origin", TSDataType.DOUBLE);
      createTimeseries(statement, PHYSICAL_DB + ".src.d2.s_text_origin", TSDataType.TEXT);

      rename(statement, PHYSICAL_DB + ".src.d1.s_long", PHYSICAL_DB + ".alias.d1.s_long");
      rename(
          statement,
          PHYSICAL_DB + ".src.d1.s_double_origin",
          PHYSICAL_DB + ".alias.d1.s_double_alias");
      rename(
          statement, PHYSICAL_DB + ".src.d2.s_text_origin", PHYSICAL_DB + ".alias.d2.s_text_alias");

      tsFile =
          writeTsFile(
              "physical.tsfile",
              new DeviceSpec(
                  PHYSICAL_DB + ".src.d1",
                  schemas(
                      schema("s_long", TSDataType.INT64),
                      schema("s_double_origin", TSDataType.DOUBLE)),
                  2L,
                  new Object[] {100L, 1.25D}),
              new DeviceSpec(
                  PHYSICAL_DB + ".src.d2",
                  schemas(schema("s_text_origin", TSDataType.TEXT)),
                  2L,
                  new Object[] {"physical-path"}));
    }

    assertNonQueryTestFail(
        String.format("LOAD '%s'", tsFile.getAbsolutePath()), "cannot accept load data");

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      loadTsFile(statement, tsFile, true);
    }

    resultSetEqualTest(
        "SELECT s_long, s_double_alias FROM " + PHYSICAL_DB + ".alias.d1",
        "Time," + PHYSICAL_DB + ".alias.d1.s_long," + PHYSICAL_DB + ".alias.d1.s_double_alias,",
        new String[] {"2,100,1.25,"});
    resultSetEqualTest(
        "SELECT s_text_alias FROM " + PHYSICAL_DB + ".alias.d2",
        "Time," + PHYSICAL_DB + ".alias.d2.s_text_alias,",
        new String[] {"2,physical-path,"});
  }

  @Test
  public void testLoadMixedPhysicalAndRenamedPathsWithPhysicalPathFlag() throws Exception {
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
              "mixed.tsfile",
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

    assertNonQueryTestFail(
        String.format("LOAD '%s'", tsFile.getAbsolutePath()),
        "Cannot insert data into invalid series");

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      loadTsFile(statement, tsFile, true);
    }

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

  // -------------------------------------------------------------------------
  // Normal series (no rename) — direct TsFile load
  // -------------------------------------------------------------------------

  @Test
  public void testLoadNormalSeriesTsFileDirectPath() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      createDatabase(statement, NORMAL_DB);
      createTimeseries(statement, NORMAL_DB + ".plain.d1.s1", TSDataType.INT32);
      createTimeseries(statement, NORMAL_DB + ".plain.d1.s2", TSDataType.FLOAT);

      final File tsFile =
          writeTsFile(
              "normal-direct.tsfile",
              new DeviceSpec(
                  NORMAL_DB + ".plain.d1",
                  schemas(schema("s1", TSDataType.INT32), schema("s2", TSDataType.FLOAT)),
                  10L,
                  new Object[] {42, 3.14F}));

      loadTsFile(statement, tsFile);
    }

    resultSetEqualTest(
        "SELECT s1, s2 FROM " + NORMAL_DB + ".plain.d1",
        "Time," + NORMAL_DB + ".plain.d1.s1," + NORMAL_DB + ".plain.d1.s2,",
        new String[] {"10,42,3.14,"});
  }

  // -------------------------------------------------------------------------
  // Invalid physical path only
  // -------------------------------------------------------------------------

  @Test
  public void testLoadOnlyInvalidPhysicalPathFailsWithoutFlag() throws Exception {
    final String database = INVALID_ONLY_DB + "_fail";
    final File tsFile = prepareSingleRenamedInvalidPhysicalTsFile(database, 20L, 200);
    assertNonQueryTestFail(
        String.format("LOAD '%s'", tsFile.getAbsolutePath()), "cannot accept load data");
  }

  @Test
  public void testLoadOnlyInvalidPhysicalPathSucceedsWithFlag() throws Exception {
    final String database = INVALID_ONLY_DB + "_flag";
    final File tsFile = prepareSingleRenamedInvalidPhysicalTsFile(database, 21L, 210);
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      loadTsFile(statement, tsFile, true);
    }
    resultSetEqualTest(
        "SELECT s_val FROM " + database + ".alias.d1",
        "Time," + database + ".alias.d1.s_val,",
        new String[] {"21,210,"});
  }

  @Test
  public void testLoadInvalidPhysicalPathHasNoReadableDataAfterPhysicalPathLoad() throws Exception {
    final String database = PHYSICAL_DB + "_nodata";
    final File tsFile = prepareSingleRenamedInvalidPhysicalTsFile(database, 22L, 220);
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      loadTsFile(statement, tsFile, true);
    }

    assertRowCount("SELECT s_val FROM " + database + ".alias.d1", 1);
    assertRowCount("SELECT s_val FROM " + database + ".src.d1", 0);
  }

  @Test
  public void testLoadMultipleInvalidPhysicalPathsFailWithoutFlag() throws Exception {
    final File tsFile;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      createDatabase(statement, MULTI_INVALID_DB);
      createTimeseries(statement, MULTI_INVALID_DB + ".src.d1.s_a", TSDataType.INT32);
      createTimeseries(statement, MULTI_INVALID_DB + ".src.d2.s_b", TSDataType.INT64);
      rename(statement, MULTI_INVALID_DB + ".src.d1.s_a", MULTI_INVALID_DB + ".alias.d1.s_a");
      rename(statement, MULTI_INVALID_DB + ".src.d2.s_b", MULTI_INVALID_DB + ".alias.d2.s_b");

      tsFile =
          writeTsFile(
              "multi-invalid.tsfile",
              new DeviceSpec(
                  MULTI_INVALID_DB + ".src.d1",
                  schemas(schema("s_a", TSDataType.INT32)),
                  30L,
                  new Object[] {1}),
              new DeviceSpec(
                  MULTI_INVALID_DB + ".src.d2",
                  schemas(schema("s_b", TSDataType.INT64)),
                  30L,
                  new Object[] {2L}));
    }

    assertNonQueryTestFail(
        String.format("LOAD '%s'", tsFile.getAbsolutePath()), "cannot accept load data");
  }

  // -------------------------------------------------------------------------
  // Alias path TsFile + physical-path flag (alias still via tablet)
  // -------------------------------------------------------------------------

  @Test
  public void testLoadAliasPathTsFileWithPhysicalPathFlagStillSucceeds() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      createDatabase(statement, ALIAS_FLAG_DB);
      createTimeseries(statement, ALIAS_FLAG_DB + ".src.d1.s_flag", TSDataType.DOUBLE);
      rename(statement, ALIAS_FLAG_DB + ".src.d1.s_flag", ALIAS_FLAG_DB + ".alias.d1.s_flag");

      final File tsFile =
          writeTsFile(
              "alias-with-flag.tsfile",
              new DeviceSpec(
                  ALIAS_FLAG_DB + ".alias.d1",
                  schemas(schema("s_flag", TSDataType.DOUBLE)),
                  40L,
                  new Object[] {9.9D}));

      loadTsFile(statement, tsFile, true);
    }

    resultSetEqualTest(
        "SELECT s_flag FROM " + ALIAS_FLAG_DB + ".alias.d1",
        "Time," + ALIAS_FLAG_DB + ".alias.d1.s_flag,",
        new String[] {"40,9.9,"});
    assertRowCount("SELECT s_flag FROM " + ALIAS_FLAG_DB + ".src.d1", 0);
  }

  // -------------------------------------------------------------------------
  // Normal-only TsFile while other series in DB are renamed
  // -------------------------------------------------------------------------

  @Test
  public void testLoadOnlyNormalPathWhenOtherSeriesRenamed() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      createDatabase(statement, NORMAL_ONLY_DB);
      createTimeseries(statement, NORMAL_ONLY_DB + ".src.d1.s_renamed", TSDataType.INT32);
      createTimeseries(statement, NORMAL_ONLY_DB + ".keep.d1.s_keep", TSDataType.FLOAT);
      rename(
          statement, NORMAL_ONLY_DB + ".src.d1.s_renamed", NORMAL_ONLY_DB + ".alias.d1.s_renamed");

      final File tsFile =
          writeTsFile(
              "normal-only.tsfile",
              new DeviceSpec(
                  NORMAL_ONLY_DB + ".keep.d1",
                  schemas(schema("s_keep", TSDataType.FLOAT)),
                  50L,
                  new Object[] {5.5F}));

      loadTsFile(statement, tsFile);
    }

    resultSetEqualTest(
        "SELECT s_keep FROM " + NORMAL_ONLY_DB + ".keep.d1",
        "Time," + NORMAL_ONLY_DB + ".keep.d1.s_keep,",
        new String[] {"50,5.5,"});
    assertRowCount("SELECT s_renamed FROM " + NORMAL_ONLY_DB + ".alias.d1", 0);
  }

  // -------------------------------------------------------------------------
  // Alias + normal paths in one TsFile (no invalid physical path)
  // -------------------------------------------------------------------------

  @Test
  public void testLoadAliasAndNormalPathsInSameTsFileWithoutPhysicalFlag() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      createDatabase(statement, ALIAS_NORMAL_MIX_DB);
      createTimeseries(statement, ALIAS_NORMAL_MIX_DB + ".src.d1.s_alias", TSDataType.TEXT);
      createTimeseries(statement, ALIAS_NORMAL_MIX_DB + ".plain.d1.s_plain", TSDataType.INT32);
      rename(
          statement,
          ALIAS_NORMAL_MIX_DB + ".src.d1.s_alias",
          ALIAS_NORMAL_MIX_DB + ".view.d1.s_alias");

      final File tsFile =
          writeTsFile(
              "alias-normal-mix.tsfile",
              new DeviceSpec(
                  ALIAS_NORMAL_MIX_DB + ".view.d1",
                  schemas(schema("s_alias", TSDataType.TEXT)),
                  60L,
                  new Object[] {"via-alias"}),
              new DeviceSpec(
                  ALIAS_NORMAL_MIX_DB + ".plain.d1",
                  schemas(schema("s_plain", TSDataType.INT32)),
                  60L,
                  new Object[] {60}));

      loadTsFile(statement, tsFile);
    }

    resultSetEqualTest(
        "SELECT s_alias FROM " + ALIAS_NORMAL_MIX_DB + ".view.d1",
        "Time," + ALIAS_NORMAL_MIX_DB + ".view.d1.s_alias,",
        new String[] {"60,via-alias,"});
    resultSetEqualTest(
        "SELECT s_plain FROM " + ALIAS_NORMAL_MIX_DB + ".plain.d1",
        "Time," + ALIAS_NORMAL_MIX_DB + ".plain.d1.s_plain,",
        new String[] {"60,60,"});
  }

  // -------------------------------------------------------------------------
  // Append: load alias TsFile twice
  // -------------------------------------------------------------------------

  @Test
  public void testLoadAliasPathTwiceAppendsData() throws Exception {
    final File tsFile1;
    final File tsFile2;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      createDatabase(statement, APPEND_DB);
      createTimeseries(statement, APPEND_DB + ".src.d1.s_append", TSDataType.INT32);
      rename(statement, APPEND_DB + ".src.d1.s_append", APPEND_DB + ".alias.d1.s_append");

      tsFile1 =
          writeTsFile(
              "append-1.tsfile",
              new DeviceSpec(
                  APPEND_DB + ".alias.d1",
                  schemas(schema("s_append", TSDataType.INT32)),
                  70L,
                  new Object[] {700}));
      tsFile2 =
          writeTsFile(
              "append-2.tsfile",
              new DeviceSpec(
                  APPEND_DB + ".alias.d1",
                  schemas(schema("s_append", TSDataType.INT32)),
                  71L,
                  new Object[] {701}));

      loadTsFile(statement, tsFile1);
      loadTsFile(statement, tsFile2);
    }

    resultSetEqualTest(
        "SELECT s_append FROM " + APPEND_DB + ".alias.d1 ORDER BY time",
        "Time," + APPEND_DB + ".alias.d1.s_append,",
        new String[] {"70,700,", "71,701,"});
  }

  // -------------------------------------------------------------------------
  // Auto-create new device / timeseries via LOAD
  // -------------------------------------------------------------------------

  @Test
  public void testLoadAutoCreateNewDeviceDefaultMode() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      createDatabase(statement, AUTO_CREATE_DB);

      final File tsFile =
          writeTsFile(
              "autocreate-default.tsfile",
              new DeviceSpec(
                  AUTO_CREATE_DB + ".new.d1",
                  schemas(schema("s_new", TSDataType.INT32)),
                  80L,
                  new Object[] {800}));

      loadTsFile(statement, tsFile);
    }

    resultSetEqualTest(
        "SELECT s_new FROM " + AUTO_CREATE_DB + ".new.d1",
        "Time," + AUTO_CREATE_DB + ".new.d1.s_new,",
        new String[] {"80,800,"});
  }

  @Test
  public void testLoadAutoCreateNewDeviceWithPhysicalPathFlag() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      createDatabase(statement, AUTO_CREATE_DB + "_phys");

      final File tsFile =
          writeTsFile(
              "autocreate-physical.tsfile",
              new DeviceSpec(
                  AUTO_CREATE_DB + "_phys.new.d1",
                  schemas(schema("s_new_phys", TSDataType.FLOAT)),
                  81L,
                  new Object[] {8.1F}));

      loadTsFile(statement, tsFile, true);
    }

    resultSetEqualTest(
        "SELECT s_new_phys FROM " + AUTO_CREATE_DB + "_phys.new.d1",
        "Time," + AUTO_CREATE_DB + "_phys.new.d1.s_new_phys,",
        new String[] {"81,8.1,"});
  }

  // -------------------------------------------------------------------------
  // Aligned device + alias series
  // -------------------------------------------------------------------------

  @Test
  public void testLoadAlignedAliasPathTsFileAfterPartialRenameOnSameDevice() throws Exception {
    final String device = ALIGNED_ALIAS_DB + ".src.d1";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      createDatabase(statement, ALIGNED_ALIAS_DB);
      createAlignedTimeseries(
          statement,
          device,
          "s1_physical INT32 encoding=RLE compression=SNAPPY",
          "s2 INT32 encoding=RLE compression=SNAPPY",
          "s3 INT32 encoding=RLE compression=SNAPPY");
      rename(statement, device + ".s1_physical", device + ".s1_alias");

      final File tsFile =
          writeAlignedTsFile(
              "aligned-partial-alias.tsfile",
              new AlignedDeviceSpec(
                  device,
                  schemas(
                      schema("s1_alias", TSDataType.INT32),
                      schema("s2", TSDataType.INT32),
                      schema("s3", TSDataType.INT32)),
                  100L,
                  new Object[] {1000, 2000, 3000}));

      loadTsFile(statement, tsFile);
    }

    resultSetEqualTest(
        "SELECT s1_alias, s2, s3 FROM " + device,
        "Time," + device + ".s1_alias," + device + ".s2," + device + ".s3,",
        new String[] {"100,1000,2000,3000,"});
    assertRowCount("SELECT s1_physical FROM " + device, 0);
  }

  @Test
  public void testLoadAlignedCrossDatabaseAliasPathTsFile() throws Exception {
    final String srcDevice = ALIGNED_CROSS_SRC_DB + ".d1";
    final String viewDevice = ALIGNED_CROSS_VIEW_DB + ".d1";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      createDatabase(statement, ALIGNED_CROSS_SRC_DB);
      createDatabase(statement, ALIGNED_CROSS_VIEW_DB);
      createAlignedTimeseries(
          statement,
          srcDevice,
          "s1 INT32 encoding=RLE compression=SNAPPY",
          "s2 FLOAT encoding=RLE compression=SNAPPY",
          "s3 DOUBLE encoding=GORILLA compression=SNAPPY");
      rename(statement, srcDevice + ".s1", viewDevice + ".s1");
      rename(statement, srcDevice + ".s2", viewDevice + ".s2");
      rename(statement, srcDevice + ".s3", viewDevice + ".s3");

      final File tsFile =
          writeAlignedTsFile(
              "aligned-cross-alias.tsfile",
              new AlignedDeviceSpec(
                  viewDevice,
                  schemas(
                      schema("s1", TSDataType.INT32),
                      schema("s2", TSDataType.FLOAT),
                      schema("s3", TSDataType.DOUBLE)),
                  101L,
                  new Object[] {11, 2.2F, 3.3D}));

      loadTsFile(statement, tsFile);
    }

    resultSetEqualTest(
        "SELECT s1, s2, s3 FROM " + viewDevice,
        "Time," + viewDevice + ".s1," + viewDevice + ".s2," + viewDevice + ".s3,",
        new String[] {"101,11,2.2,3.3,"});
    assertRowCount("SELECT s1 FROM " + srcDevice, 0);
  }

  @Test
  public void testLoadAlignedPhysicalPathTsFileFailsWithoutFlag() throws Exception {
    final String device = ALIGNED_PHYSICAL_DB + ".src.d1";
    final File tsFile;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      createDatabase(statement, ALIGNED_PHYSICAL_DB);
      createAlignedTimeseries(
          statement,
          device,
          "s1_physical INT32 encoding=RLE compression=SNAPPY",
          "s2 INT32 encoding=RLE compression=SNAPPY",
          "s3 INT32 encoding=RLE compression=SNAPPY");
      rename(statement, device + ".s1_physical", device + ".s1_alias");

      tsFile =
          writeAlignedTsFile(
              "aligned-physical-fail.tsfile",
              new AlignedDeviceSpec(
                  device,
                  schemas(
                      schema("s1_physical", TSDataType.INT32),
                      schema("s2", TSDataType.INT32),
                      schema("s3", TSDataType.INT32)),
                  102L,
                  new Object[] {1020, 2020, 3020}));
    }

    assertNonQueryTestFail(
        String.format("LOAD '%s'", tsFile.getAbsolutePath()), "cannot accept load data");
  }

  @Test
  public void testLoadAlignedPhysicalPathTsFileSucceedsWithPhysicalPathFlag() throws Exception {
    final String device = ALIGNED_PHYSICAL_DB + ".src.d1";
    final File tsFile;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      createDatabase(statement, ALIGNED_PHYSICAL_DB + "_ok");
      final String deviceOk = ALIGNED_PHYSICAL_DB + "_ok.src.d1";
      createAlignedTimeseries(
          statement,
          deviceOk,
          "s1_physical INT32 encoding=RLE compression=SNAPPY",
          "s2 INT32 encoding=RLE compression=SNAPPY",
          "s3 INT32 encoding=RLE compression=SNAPPY");
      rename(statement, deviceOk + ".s1_physical", deviceOk + ".s1_alias");

      tsFile =
          writeAlignedTsFile(
              "aligned-physical-ok.tsfile",
              new AlignedDeviceSpec(
                  deviceOk,
                  schemas(
                      schema("s1_physical", TSDataType.INT32),
                      schema("s2", TSDataType.INT32),
                      schema("s3", TSDataType.INT32)),
                  103L,
                  new Object[] {1030, 2030, 3030}));

      loadTsFile(statement, tsFile, true);
    }

    final String deviceOk = ALIGNED_PHYSICAL_DB + "_ok.src.d1";
    resultSetEqualTest(
        "SELECT s1_alias, s2, s3 FROM " + deviceOk,
        "Time," + deviceOk + ".s1_alias," + deviceOk + ".s2," + deviceOk + ".s3,",
        new String[] {"103,1030,2030,3030,"});
    assertRowCount("SELECT s1_physical FROM " + deviceOk, 0);
  }

  @Test
  public void testLoadAlignedAliasPathTsFileWithPhysicalPathFlagStillSucceeds() throws Exception {
    final String device = ALIGNED_ALIAS_DB + ".flag.d1";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      createDatabase(statement, ALIGNED_ALIAS_DB);
      createAlignedTimeseries(
          statement,
          device,
          "s1_physical FLOAT encoding=RLE compression=SNAPPY",
          "s2 INT32 encoding=RLE compression=SNAPPY");
      rename(statement, device + ".s1_physical", device + ".s1_alias");

      final File tsFile =
          writeAlignedTsFile(
              "aligned-alias-with-flag.tsfile",
              new AlignedDeviceSpec(
                  device,
                  schemas(schema("s1_alias", TSDataType.FLOAT), schema("s2", TSDataType.INT32)),
                  104L,
                  new Object[] {4.04F, 404}));

      loadTsFile(statement, tsFile, true);
    }

    resultSetEqualTest(
        "SELECT s1_alias, s2 FROM " + device,
        "Time," + device + ".s1_alias," + device + ".s2,",
        new String[] {"104,4.04,404,"});
  }

  // -------------------------------------------------------------------------
  // verify=false bypasses analyze-time invalid/alias guards so LOAD succeeds; SELECT still skips
  // invalid physical paths (data is readable on the alias path, like physical-path flag loads).
  // -------------------------------------------------------------------------

  @Test
  public void testLoadInvalidPhysicalPathWithVerifySchemaFalseBypassesAliasGuard()
      throws Exception {
    final File tsFile;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      createDatabase(statement, VERIFY_OFF_DB);
      createTimeseries(statement, VERIFY_OFF_DB + ".src.d1.s_off", TSDataType.INT32);
      rename(statement, VERIFY_OFF_DB + ".src.d1.s_off", VERIFY_OFF_DB + ".alias.d1.s_off");

      tsFile =
          writeTsFile(
              "verify-off.tsfile",
              new DeviceSpec(
                  VERIFY_OFF_DB + ".src.d1",
                  schemas(schema("s_off", TSDataType.INT32)),
                  90L,
                  new Object[] {900}));
    }

    // Default LOAD would fail at analyze (invalid physical path). verify=false skips that guard.
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          String.format("LOAD '%s' WITH ('verify'='false')", tsFile.getAbsolutePath()));
    }

    // Invalid physical path is not readable via SELECT; data is visible on the alias path.
    assertRowCount("SELECT s_off FROM " + VERIFY_OFF_DB + ".src.d1", 0);
    resultSetEqualTest(
        "SELECT s_off FROM " + VERIFY_OFF_DB + ".alias.d1",
        "Time," + VERIFY_OFF_DB + ".alias.d1.s_off,",
        new String[] {"90,900,"});
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  /**
   * Tree-model SQL ({@code IoTDBSqlParser}) does not support {@code CREATE DATABASE IF NOT EXISTS};
   * only relational grammar does. Ignore 501 when the database already exists (shared cluster in
   * this IT class).
   */
  private static void createDatabase(final Statement statement, final String database)
      throws SQLException {
    try {
      statement.execute("CREATE DATABASE " + database);
    } catch (final SQLException e) {
      if (e.getErrorCode() != TSStatusCode.DATABASE_ALREADY_EXISTS.getStatusCode()) {
        throw e;
      }
    }
  }

  private File prepareSingleRenamedInvalidPhysicalTsFile(
      final String database, final long timestamp, final int value) throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      createDatabase(statement, database);
      createTimeseries(statement, database + ".src.d1.s_val", TSDataType.INT32);
      rename(statement, database + ".src.d1.s_val", database + ".alias.d1.s_val");

      return writeTsFile(
          database.replace("root.", "") + "-invalid.tsfile",
          new DeviceSpec(
              database + ".src.d1",
              schemas(schema("s_val", TSDataType.INT32)),
              timestamp,
              new Object[] {value}));
    }
  }

  private static void loadTsFile(final Statement statement, final File tsFile) throws SQLException {
    loadTsFile(statement, tsFile, false);
  }

  private static void loadTsFile(
      final Statement statement, final File tsFile, final boolean physicalPath)
      throws SQLException {
    if (physicalPath) {
      statement.execute(
          String.format(
              "LOAD '%s' WITH ('tsfile-is-physical-path'='true')", tsFile.getAbsolutePath()));
    } else {
      statement.execute(String.format("LOAD '%s'", tsFile.getAbsolutePath()));
    }
  }

  private static void rename(final Statement statement, final String oldPath, final String newPath)
      throws SQLException {
    statement.execute(String.format("ALTER TIMESERIES %s RENAME TO %s", oldPath, newPath));
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

  private void createTimeseries(
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
