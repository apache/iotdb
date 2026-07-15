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

package org.apache.iotdb.pipe.it.dual.treemodel.manual;

import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTreeManual;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTreeManual.class})
public class IoTDBPipeRenamedSeriesSourcePatternIT {

  private static final int BATCH_POINT_COUNT = 100;
  private static final int HISTORY_TIME_RANGE_POINT_COUNT = 100;
  private static final List<String> TEST_PIPE_NAMES =
      Arrays.asList(
          "pipe_path",
          "pipe_pattern",
          "pipe_iotdb_pattern",
          "pipe_inclusion",
          "pipe_path_exclusion",
          "pipe_pattern_exclusion",
          "pipe_mixed_invalid_physical_alias_normal",
          "pipe_full_with_alias_and_invalid_physical",
          "pipe_alter_path_to_alias",
          "pipe_alter_path_exclusion",
          "pipe_alter_pattern_exclusion",
          "pipe_static_snapshot",
          "pipe_history_without_mods",
          "pipe_history_realtime_time_range",
          "pipe_schema_only_alias_path",
          "pipe_schema_and_data_alias_path",
          "pipe_history_realtime_chained_rename",
          "pipe_chained_rename_first_full_sync",
          "pipe_chained_rename_second_full_sync",
          "pipe_realtime_schema_rename_alias_path",
          "pipe_realtime_schema_rename_physical_path",
          "pipe_cross_database_alias_capture",
          "pipe_empty_alter_keeps_static_snapshot",
          "pipe_full_alter_range_keeps_rename_snapshot",
          "pipe_reject_internal_create",
          "pipe_reject_internal_alter",
          "pipe_invalid_physical_path",
          "pipe_alter_invalid_physical",
          "pipe_invalid_physical_pattern");

  private static BaseEnv senderEnv;
  private static BaseEnv receiverEnv;

  @BeforeClass
  public static void setUp() throws Exception {
    MultiEnvFactory.createEnv(2);
    senderEnv = MultiEnvFactory.getEnv(0);
    receiverEnv = MultiEnvFactory.getEnv(1);
    setupConfig();
    senderEnv.initClusterEnvironment();
    receiverEnv.initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() {
    if (senderEnv != null) {
      senderEnv.cleanClusterEnvironment();
    }
    if (receiverEnv != null) {
      receiverEnv.cleanClusterEnvironment();
    }
    senderEnv = null;
    receiverEnv = null;
  }

  private static void setupConfig() {
    senderEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(false)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setPipeAutoSplitFullEnabled(false);
    senderEnv.getConfig().getDataNodeConfig().setDataNodeMemoryProportion("3:3:1:1:3:1");

    receiverEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(false)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setPipeAutoSplitFullEnabled(false);
    receiverEnv.getConfig().getCommonConfig().setAutoCreateSchemaEnabled(true);

    senderEnv.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);
    receiverEnv.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);
  }

  /**
   * Verifies that a pipe with {@code source.path} set to a renamed alias path captures the
   * corresponding physical series only. Both historical and realtime inserts should be transferred
   * to the receiver under the physical path, deletes issued by alias should remove the physical
   * data, and internal source-pattern attributes must stay hidden from SHOW PIPE.
   */
  @Test
  public void testSourcePathUsesPhysicalSeriesForAliasPath() throws Exception {
    try {
      final String device = "root.db_path.d1";
      setupMatrixSchema(device);

      insertData(
          device, Arrays.asList("s1_alias", "s2_alias", "s3_normal"), 1000, BATCH_POINT_COUNT);
      createFlexiblePipe(
          "pipe_path",
          "source.path",
          device + ".s1_alias",
          "source.inclusion",
          "data.insert,data.delete",
          "source.history.enable",
          "true",
          "source.realtime.enable",
          "true");

      assertDataCount(device + ".s1_physical", 1000, BATCH_POINT_COUNT, BATCH_POINT_COUNT);
      assertDataCount(device + ".s2_physical", 1000, BATCH_POINT_COUNT, 0);
      assertDataCount(device + ".s3_normal", 1000, BATCH_POINT_COUNT, 0);

      insertData(
          device, Arrays.asList("s1_alias", "s2_alias", "s3_normal"), 11000, BATCH_POINT_COUNT);
      assertDataCount(device + ".s1_physical", 11000, BATCH_POINT_COUNT, BATCH_POINT_COUNT);
      assertDataCount(device + ".s2_physical", 11000, BATCH_POINT_COUNT, 0);
      assertDataCount(device + ".s3_normal", 11000, BATCH_POINT_COUNT, 0);

      deleteData(Collections.singletonList(device + ".s1_alias"), 11000, BATCH_POINT_COUNT);
      assertDataCount(device + ".s1_physical", 11000, BATCH_POINT_COUNT, 0);
      assertInternalSourcePatternHidden("pipe_path");
      dropPipe("pipe_path");

    } finally {
      cleanTestResources();
    }
  }

  /**
   * Verifies that the legacy/compatible {@code source.pattern} key also resolves an exact alias
   * path to its physical series. Only the selected alias' physical series should be transferred for
   * historical data, realtime data, and delete events.
   */
  @Test
  public void testSourcePatternUsesPhysicalSeriesForAliasPath() throws Exception {
    try {
      final String device = "root.db_pattern.d1";
      setupMatrixSchema(device);

      insertData(
          device, Arrays.asList("s1_alias", "s2_alias", "s3_normal"), 2000, BATCH_POINT_COUNT);
      createFlexiblePipe(
          "pipe_pattern",
          "source.pattern",
          device + ".s2_alias",
          "source.pattern.format",
          "iotdb",
          "source.inclusion",
          "data.insert,data.delete",
          "source.history.enable",
          "true",
          "source.realtime.enable",
          "true");

      assertDataCount(device + ".s1_physical", 2000, BATCH_POINT_COUNT, 0);
      assertDataCount(device + ".s2_physical", 2000, BATCH_POINT_COUNT, BATCH_POINT_COUNT);
      assertDataCount(device + ".s3_normal", 2000, BATCH_POINT_COUNT, 0);

      insertData(
          device, Arrays.asList("s1_alias", "s2_alias", "s3_normal"), 12000, BATCH_POINT_COUNT);
      assertDataCount(device + ".s2_physical", 12000, BATCH_POINT_COUNT, BATCH_POINT_COUNT);
      deleteData(Collections.singletonList(device + ".s2_alias"), 12000, BATCH_POINT_COUNT);
      assertDataCount(device + ".s2_physical", 12000, BATCH_POINT_COUNT, 0);
      assertInternalSourcePatternHidden("pipe_pattern");
      dropPipe("pipe_pattern");

    } finally {
      cleanTestResources();
    }
  }

  /**
   * Verifies that an IoTDB device pattern snapshots renamed alias metadata and transfers the alias'
   * physical series for historical and realtime data, including delete events.
   */
  @Test
  public void testIotDBSourcePatternUsesPhysicalSeriesForAliasPath() throws Exception {
    try {
      final String device = "root.db_default_prefix_pattern.d1";
      setupMatrixSchema(device);

      insertData(
          device, Arrays.asList("s1_alias", "s2_alias", "s3_normal"), 2100, BATCH_POINT_COUNT);
      createFlexiblePipe(
          "pipe_iotdb_pattern",
          "source.pattern",
          device + ".**",
          "source.pattern.format",
          "iotdb",
          "source.inclusion",
          "data.insert,data.delete",
          "source.history.enable",
          "true",
          "source.realtime.enable",
          "true");

      assertDataCount(device + ".s1_physical", 2100, BATCH_POINT_COUNT, BATCH_POINT_COUNT);
      assertDataCount(device + ".s2_physical", 2100, BATCH_POINT_COUNT, BATCH_POINT_COUNT);
      assertDataCount(device + ".s3_normal", 2100, BATCH_POINT_COUNT, BATCH_POINT_COUNT);

      insertData(
          device, Arrays.asList("s1_alias", "s2_alias", "s3_normal"), 12100, BATCH_POINT_COUNT);
      assertDataCount(device + ".s1_physical", 12100, BATCH_POINT_COUNT, BATCH_POINT_COUNT);
      assertDataCount(device + ".s2_physical", 12100, BATCH_POINT_COUNT, BATCH_POINT_COUNT);
      assertDataCount(device + ".s3_normal", 12100, BATCH_POINT_COUNT, BATCH_POINT_COUNT);
      deleteData(Collections.singletonList(device + ".s1_alias"), 12100, BATCH_POINT_COUNT);
      assertDataCount(device + ".s1_physical", 12100, BATCH_POINT_COUNT, 0);
      assertInternalSourcePatternHidden("pipe_iotdb_pattern");
      dropPipe("pipe_iotdb_pattern");

    } finally {
      cleanTestResources();
    }
  }

  /**
   * Verifies a mixed {@code source.pattern.inclusion} list containing one alias path and one normal
   * path. The alias entry should be captured through its physical path, the normal path should be
   * captured directly, and unrelated renamed series should be filtered out.
   */
  @Test
  public void testSourcePatternInclusionUsesPhysicalSeriesForAliasAndNormalPaths()
      throws Exception {
    try {
      final String device = "root.db_inclusion.d1";
      setupMatrixSchema(device);

      insertData(
          device, Arrays.asList("s1_alias", "s2_alias", "s3_normal"), 3000, BATCH_POINT_COUNT);
      createFlexiblePipe(
          "pipe_inclusion",
          "source.pattern.inclusion",
          device + ".s1_alias," + device + ".s3_normal",
          "source.pattern.format",
          "iotdb",
          "source.inclusion",
          "data.insert,data.delete",
          "source.history.enable",
          "true",
          "source.realtime.enable",
          "true");

      assertDataCount(device + ".s1_physical", 3000, BATCH_POINT_COUNT, BATCH_POINT_COUNT);
      assertDataCount(device + ".s2_physical", 3000, BATCH_POINT_COUNT, 0);
      assertDataCount(device + ".s3_normal", 3000, BATCH_POINT_COUNT, BATCH_POINT_COUNT);

      insertData(
          device, Arrays.asList("s1_alias", "s2_alias", "s3_normal"), 13000, BATCH_POINT_COUNT);
      assertDataCount(device + ".s1_physical", 13000, BATCH_POINT_COUNT, BATCH_POINT_COUNT);
      assertDataCount(device + ".s3_normal", 13000, BATCH_POINT_COUNT, BATCH_POINT_COUNT);
      deleteData(
          Arrays.asList(device + ".s1_alias", device + ".s3_normal"), 13000, BATCH_POINT_COUNT);
      assertDataCount(device + ".s1_physical", 13000, BATCH_POINT_COUNT, 0);
      assertDataCount(device + ".s3_normal", 13000, BATCH_POINT_COUNT, 0);
      assertInternalSourcePatternHidden("pipe_inclusion");
      dropPipe("pipe_inclusion");

    } finally {
      cleanTestResources();
    }
  }

  /**
   * Verifies that {@code source.path.exclusion} written with an alias path excludes the alias'
   * physical series from an otherwise broad {@code source.path} capture. The other alias/normal
   * series should still be transferred and deletions should continue to apply to them.
   */
  @Test
  public void testSourcePathExclusionUsesPhysicalSeriesForAliasPath() throws Exception {
    try {
      final String device = "root.db_path_exclusion.d1";
      setupMatrixSchema(device);

      insertData(
          device, Arrays.asList("s1_alias", "s2_alias", "s3_normal"), 4000, BATCH_POINT_COUNT);
      createFlexiblePipe(
          "pipe_path_exclusion",
          "source.path",
          "root.db_path_exclusion.**",
          "source.path.exclusion",
          device + ".s1_alias",
          "source.inclusion",
          "data.insert,data.delete",
          "source.history.enable",
          "true",
          "source.realtime.enable",
          "true");

      assertDataCount(device + ".s1_physical", 4000, BATCH_POINT_COUNT, 0);
      assertDataCount(device + ".s2_physical", 4000, BATCH_POINT_COUNT, BATCH_POINT_COUNT);
      assertDataCount(device + ".s3_normal", 4000, BATCH_POINT_COUNT, BATCH_POINT_COUNT);

      insertData(
          device, Arrays.asList("s1_alias", "s2_alias", "s3_normal"), 14000, BATCH_POINT_COUNT);
      assertDataCount(device + ".s1_physical", 14000, BATCH_POINT_COUNT, 0);
      assertDataCount(device + ".s2_physical", 14000, BATCH_POINT_COUNT, BATCH_POINT_COUNT);
      assertDataCount(device + ".s3_normal", 14000, BATCH_POINT_COUNT, BATCH_POINT_COUNT);
      deleteData(
          Arrays.asList(device + ".s2_alias", device + ".s3_normal"), 14000, BATCH_POINT_COUNT);
      assertDataCount(device + ".s2_physical", 14000, BATCH_POINT_COUNT, 0);
      assertDataCount(device + ".s3_normal", 14000, BATCH_POINT_COUNT, 0);
      assertInternalSourcePatternHidden("pipe_path_exclusion");
      dropPipe("pipe_path_exclusion");

    } finally {
      cleanTestResources();
    }
  }

  /**
   * Verifies that {@code source.pattern.exclusion} written with an alias path excludes the alias'
   * physical series from a broad {@code source.pattern.inclusion}. This covers both historical and
   * realtime insert/delete filtering for pattern-based exclusions.
   */
  @Test
  public void testSourcePatternExclusionUsesPhysicalSeriesForAliasPath() throws Exception {
    try {
      final String device = "root.db_pattern_exclusion.d1";
      setupMatrixSchema(device);

      insertData(
          device, Arrays.asList("s1_alias", "s2_alias", "s3_normal"), 5000, BATCH_POINT_COUNT);
      createFlexiblePipe(
          "pipe_pattern_exclusion",
          "source.pattern.inclusion",
          "root.db_pattern_exclusion.**",
          "source.pattern.exclusion",
          device + ".s2_alias",
          "source.pattern.format",
          "iotdb",
          "source.inclusion",
          "data.insert,data.delete",
          "source.history.enable",
          "true",
          "source.realtime.enable",
          "true");

      assertDataCount(device + ".s1_physical", 5000, BATCH_POINT_COUNT, BATCH_POINT_COUNT);
      assertDataCount(device + ".s2_physical", 5000, BATCH_POINT_COUNT, 0);
      assertDataCount(device + ".s3_normal", 5000, BATCH_POINT_COUNT, BATCH_POINT_COUNT);

      insertData(
          device, Arrays.asList("s1_alias", "s2_alias", "s3_normal"), 15000, BATCH_POINT_COUNT);
      assertDataCount(device + ".s1_physical", 15000, BATCH_POINT_COUNT, BATCH_POINT_COUNT);
      assertDataCount(device + ".s2_physical", 15000, BATCH_POINT_COUNT, 0);
      assertDataCount(device + ".s3_normal", 15000, BATCH_POINT_COUNT, BATCH_POINT_COUNT);
      deleteData(
          Arrays.asList(device + ".s1_alias", device + ".s3_normal"), 15000, BATCH_POINT_COUNT);
      assertDataCount(device + ".s1_physical", 15000, BATCH_POINT_COUNT, 0);
      assertDataCount(device + ".s3_normal", 15000, BATCH_POINT_COUNT, 0);
      assertInternalSourcePatternHidden("pipe_pattern_exclusion");
      dropPipe("pipe_pattern_exclusion");

    } finally {
      cleanTestResources();
    }
  }

  /**
   * Verifies inclusion-list behavior when users mix an invalid physical path, a valid alias path,
   * and a normal path. Directly selecting the invalid physical path should not transfer data, while
   * the valid alias and normal paths should still be captured correctly.
   */
  @Test
  public void testSourcePatternInclusionMixesInvalidPhysicalAliasAndNormalPaths() throws Exception {
    try {
      final String device = "root.db_mixed.d1";
      setupMatrixSchema(device);

      insertData(
          device, Arrays.asList("s1_alias", "s2_alias", "s3_normal"), 6000, BATCH_POINT_COUNT);
      createFlexiblePipe(
          "pipe_mixed_invalid_physical_alias_normal",
          "source.pattern.inclusion",
          device + ".s1_physical," + device + ".s2_alias," + device + ".s3_normal",
          "source.pattern.format",
          "iotdb",
          "source.inclusion",
          "data.insert,data.delete",
          "source.history.enable",
          "true",
          "source.realtime.enable",
          "true");

      assertDataCount(device + ".s1_physical", 6000, BATCH_POINT_COUNT, 0);
      assertDataCount(device + ".s2_physical", 6000, BATCH_POINT_COUNT, BATCH_POINT_COUNT);
      assertDataCount(device + ".s3_normal", 6000, BATCH_POINT_COUNT, BATCH_POINT_COUNT);

      insertData(
          device, Arrays.asList("s1_alias", "s2_alias", "s3_normal"), 16000, BATCH_POINT_COUNT);
      assertDataCount(device + ".s1_physical", 16000, BATCH_POINT_COUNT, 0);
      assertDataCount(device + ".s2_physical", 16000, BATCH_POINT_COUNT, BATCH_POINT_COUNT);
      assertDataCount(device + ".s3_normal", 16000, BATCH_POINT_COUNT, BATCH_POINT_COUNT);
      deleteData(
          Arrays.asList(device + ".s2_alias", device + ".s3_normal"), 16000, BATCH_POINT_COUNT);
      assertDataCount(device + ".s2_physical", 16000, BATCH_POINT_COUNT, 0);
      assertDataCount(device + ".s3_normal", 16000, BATCH_POINT_COUNT, 0);
      assertInternalSourcePatternHidden("pipe_mixed_invalid_physical_alias_normal");
      dropPipe("pipe_mixed_invalid_physical_alias_normal");

    } finally {
      cleanTestResources();
    }
  }

  /**
   * Verifies full-tree inclusion semantics. Once {@code root.**} is present, the pipe should behave
   * as a full capture even if the list also contains alias and invalid physical paths: all renamed
   * physical series and normal series are transferred and deletions are propagated.
   */
  @Test
  public void testFullSourcePatternCapturesAliasInvalidPhysicalAndNormalPaths() throws Exception {
    try {
      final String device = "root.db_full.d1";
      setupMatrixSchema(device);

      insertData(
          device, Arrays.asList("s1_alias", "s2_alias", "s3_normal"), 7000, BATCH_POINT_COUNT);
      createFlexiblePipe(
          "pipe_full_with_alias_and_invalid_physical",
          "source.pattern.inclusion",
          "root.**," + device + ".s1_alias," + device + ".s1_physical",
          "source.pattern.format",
          "iotdb",
          "source.inclusion",
          "data.insert,data.delete",
          "source.history.enable",
          "true",
          "source.realtime.enable",
          "true");

      assertDataCount(device + ".s1_physical", 7000, BATCH_POINT_COUNT, BATCH_POINT_COUNT);
      assertDataCount(device + ".s2_physical", 7000, BATCH_POINT_COUNT, BATCH_POINT_COUNT);
      assertDataCount(device + ".s3_normal", 7000, BATCH_POINT_COUNT, BATCH_POINT_COUNT);

      insertData(
          device, Arrays.asList("s1_alias", "s2_alias", "s3_normal"), 17000, BATCH_POINT_COUNT);
      assertDataCount(device + ".s1_physical", 17000, BATCH_POINT_COUNT, BATCH_POINT_COUNT);
      assertDataCount(device + ".s2_physical", 17000, BATCH_POINT_COUNT, BATCH_POINT_COUNT);
      assertDataCount(device + ".s3_normal", 17000, BATCH_POINT_COUNT, BATCH_POINT_COUNT);
      deleteData(
          Arrays.asList(device + ".s1_alias", device + ".s2_alias", device + ".s3_normal"),
          17000,
          BATCH_POINT_COUNT);
      assertDataCount(device + ".s1_physical", 17000, BATCH_POINT_COUNT, 0);
      assertDataCount(device + ".s2_physical", 17000, BATCH_POINT_COUNT, 0);
      assertDataCount(device + ".s3_normal", 17000, BATCH_POINT_COUNT, 0);
      assertInternalSourcePatternHidden("pipe_full_with_alias_and_invalid_physical");
      dropPipe("pipe_full_with_alias_and_invalid_physical");

    } finally {
      cleanTestResources();
    }
  }

  /**
   * Verifies that ALTER PIPE refreshes exact path resolution when {@code source.path} is changed
   * from a normal path to an alias path. After ALTER, only the alias' physical series should be
   * transferred and deleted; the previously selected normal path should no longer be captured.
   */
  @Test
  public void testAlterPipeSourcePathToAliasPath() throws Exception {
    try {
      final String device = "root.db_alter_path.d1";
      setupMatrixSchema(device);

      insertData(
          device, Arrays.asList("s1_alias", "s2_alias", "s3_normal"), 8000, BATCH_POINT_COUNT);
      createFlexiblePipe(
          "pipe_alter_path_to_alias",
          "source.path",
          device + ".s3_normal",
          "source.inclusion",
          "data.insert,data.delete",
          "source.history.enable",
          "true",
          "source.realtime.enable",
          "true");
      assertDataCount(device + ".s1_physical", 8000, BATCH_POINT_COUNT, 0);
      assertDataCount(device + ".s2_physical", 8000, BATCH_POINT_COUNT, 0);
      assertDataCount(device + ".s3_normal", 8000, BATCH_POINT_COUNT, BATCH_POINT_COUNT);

      alterPipeSource("pipe_alter_path_to_alias", "source.path", device + ".s1_alias");
      insertData(
          device, Arrays.asList("s1_alias", "s2_alias", "s3_normal"), 18000, BATCH_POINT_COUNT);
      assertDataCount(device + ".s1_physical", 18000, BATCH_POINT_COUNT, BATCH_POINT_COUNT);
      assertDataCount(device + ".s2_physical", 18000, BATCH_POINT_COUNT, 0);
      assertDataCount(device + ".s3_normal", 18000, BATCH_POINT_COUNT, 0);
      deleteData(Collections.singletonList(device + ".s1_alias"), 18000, BATCH_POINT_COUNT);
      assertDataCount(device + ".s1_physical", 18000, BATCH_POINT_COUNT, 0);
      assertInternalSourcePatternHidden("pipe_alter_path_to_alias");
      dropPipe("pipe_alter_path_to_alias");

    } finally {
      cleanTestResources();
    }
  }

  /**
   * Verifies that ALTER PIPE can add {@code source.path.exclusion} with an alias path. Data before
   * ALTER is fully captured; data after ALTER excludes the alias' physical series while keeping the
   * remaining series and delete events working.
   */
  @Test
  public void testAlterPipeAddsAliasPathExclusion() throws Exception {
    try {
      final String device = "root.db_alter_path_exclusion.d1";
      setupMatrixSchema(device);

      insertData(
          device, Arrays.asList("s1_alias", "s2_alias", "s3_normal"), 9000, BATCH_POINT_COUNT);
      createFlexiblePipe(
          "pipe_alter_path_exclusion",
          "source.path",
          "root.db_alter_path_exclusion.**",
          "source.inclusion",
          "data.insert,data.delete",
          "source.history.enable",
          "true",
          "source.realtime.enable",
          "true");
      assertDataCount(device + ".s1_physical", 9000, BATCH_POINT_COUNT, BATCH_POINT_COUNT);
      assertDataCount(device + ".s2_physical", 9000, BATCH_POINT_COUNT, BATCH_POINT_COUNT);
      assertDataCount(device + ".s3_normal", 9000, BATCH_POINT_COUNT, BATCH_POINT_COUNT);

      alterPipeSource("pipe_alter_path_exclusion", "source.path.exclusion", device + ".s1_alias");
      insertData(
          device, Arrays.asList("s1_alias", "s2_alias", "s3_normal"), 19000, BATCH_POINT_COUNT);
      assertDataCount(device + ".s1_physical", 19000, BATCH_POINT_COUNT, 0);
      assertDataCount(device + ".s2_physical", 19000, BATCH_POINT_COUNT, BATCH_POINT_COUNT);
      assertDataCount(device + ".s3_normal", 19000, BATCH_POINT_COUNT, BATCH_POINT_COUNT);
      deleteData(
          Arrays.asList(device + ".s2_alias", device + ".s3_normal"), 19000, BATCH_POINT_COUNT);
      assertDataCount(device + ".s2_physical", 19000, BATCH_POINT_COUNT, 0);
      assertDataCount(device + ".s3_normal", 19000, BATCH_POINT_COUNT, 0);
      assertInternalSourcePatternHidden("pipe_alter_path_exclusion");
      dropPipe("pipe_alter_path_exclusion");

    } finally {
      cleanTestResources();
    }
  }

  /**
   * Verifies that ALTER PIPE can add {@code source.pattern.exclusion} with an alias path. The
   * updated internal exclusion should filter the alias' physical series from later realtime data
   * while preserving the other selected series.
   */
  @Test
  public void testAlterPipeAddsAliasPatternExclusion() throws Exception {
    try {
      final String device = "root.db_alter_pattern_exclusion.d1";
      setupMatrixSchema(device);

      insertData(
          device, Arrays.asList("s1_alias", "s2_alias", "s3_normal"), 10000, BATCH_POINT_COUNT);
      createFlexiblePipe(
          "pipe_alter_pattern_exclusion",
          "source.pattern.inclusion",
          "root.db_alter_pattern_exclusion.**",
          "source.pattern.format",
          "iotdb",
          "source.inclusion",
          "data.insert,data.delete",
          "source.history.enable",
          "true",
          "source.realtime.enable",
          "true");
      assertDataCount(device + ".s1_physical", 10000, BATCH_POINT_COUNT, BATCH_POINT_COUNT);
      assertDataCount(device + ".s2_physical", 10000, BATCH_POINT_COUNT, BATCH_POINT_COUNT);
      assertDataCount(device + ".s3_normal", 10000, BATCH_POINT_COUNT, BATCH_POINT_COUNT);

      alterPipeSource(
          "pipe_alter_pattern_exclusion",
          "source.pattern.exclusion",
          device + ".s2_alias",
          "source.pattern.format",
          "iotdb");
      insertData(
          device, Arrays.asList("s1_alias", "s2_alias", "s3_normal"), 20000, BATCH_POINT_COUNT);
      assertDataCount(device + ".s1_physical", 20000, BATCH_POINT_COUNT, BATCH_POINT_COUNT);
      assertDataCount(device + ".s2_physical", 20000, BATCH_POINT_COUNT, 0);
      assertDataCount(device + ".s3_normal", 20000, BATCH_POINT_COUNT, BATCH_POINT_COUNT);
      deleteData(
          Arrays.asList(device + ".s1_alias", device + ".s3_normal"), 20000, BATCH_POINT_COUNT);
      assertDataCount(device + ".s1_physical", 20000, BATCH_POINT_COUNT, 0);
      assertDataCount(device + ".s3_normal", 20000, BATCH_POINT_COUNT, 0);
      assertInternalSourcePatternHidden("pipe_alter_pattern_exclusion");
      dropPipe("pipe_alter_pattern_exclusion");

    } finally {
      cleanTestResources();
    }
  }

  /**
   * Verifies that wildcard alias-resolution snapshots are refreshed by ALTER PIPE. A renamed series
   * created after the pipe is created is not captured by the old snapshot; after ALTER refreshes
   * the source pattern, the same alias path is captured and deleted through its physical path.
   */
  @Test
  public void testAlterPipeRefreshesStaticRenamedSeriesSnapshot() throws Exception {
    try {
      final String device = "root.db_static.d1";
      createFlexiblePipe(
          "pipe_static_snapshot",
          "source.pattern",
          device + ".s4_alias",
          "source.pattern.format",
          "iotdb",
          "source.inclusion",
          "data.insert,data.delete",
          "source.history.enable",
          "false",
          "source.realtime.enable",
          "true");
      createTs(senderEnv, device + ".s4_physical");
      setAlias(device + ".s4_physical", device + ".s4_alias");
      createTs(receiverEnv, device + ".s4_physical");
      insertData(device, Collections.singletonList("s4_alias"), 21000, BATCH_POINT_COUNT);
      assertDataCount(device + ".s4_physical", 21000, BATCH_POINT_COUNT, 0);

      alterPipeSource(
          "pipe_static_snapshot",
          "source.pattern",
          device + ".s4_alias",
          "source.pattern.format",
          "iotdb");
      insertData(device, Collections.singletonList("s4_alias"), 22000, BATCH_POINT_COUNT);
      assertDataCount(device + ".s4_physical", 22000, BATCH_POINT_COUNT, BATCH_POINT_COUNT);
      deleteData(Collections.singletonList(device + ".s4_alias"), 22000, BATCH_POINT_COUNT);
      assertDataCount(device + ".s4_physical", 22000, BATCH_POINT_COUNT, 0);
      assertInternalSourcePatternHidden("pipe_static_snapshot");
      dropPipe("pipe_static_snapshot");

    } finally {
      cleanTestResources();
    }
  }

  /**
   * Verifies history-only capture for an alias path when modification-file filtering is disabled.
   * Even if the sender deletes the flushed alias data before pipe creation, historical scanning
   * with {@code source.mods.enable=false} should still transfer the physical data from TsFiles.
   */
  @Test
  public void testHistoryPipeWithoutModsCapturesFlushedDeletedAliasData() throws Exception {
    try {
      final String device = "root.db_history_without_mods.d1";
      setupMatrixSchema(device);

      insertData(device, Collections.singletonList("s1_alias"), 23000, BATCH_POINT_COUNT);
      deleteData(Collections.singletonList(device + ".s1_alias"), 23000, BATCH_POINT_COUNT);
      createFlexiblePipe(
          "pipe_history_without_mods",
          "source.path",
          device + ".s1_alias",
          "source.inclusion",
          "data.insert",
          "source.history.enable",
          "true",
          "source.realtime.enable",
          "false",
          "source.mods.enable",
          "false");

      assertDataCount(device + ".s1_physical", 23000, BATCH_POINT_COUNT, BATCH_POINT_COUNT);
      assertInternalSourcePatternHidden("pipe_history_without_mods");
      dropPipe("pipe_history_without_mods");

    } finally {
      cleanTestResources();
    }
  }

  /**
   * Verifies that alias-to-physical resolution respects {@code source.start-time} and {@code
   * source.end-time}. Historical and realtime data before or after the configured time window must
   * be filtered out, while points inside the window are transferred.
   */
  @Test
  public void testHistoryAndRealtimePipeWithTimeRangeCapturesOnlyMatchedAliasData()
      throws Exception {
    try {
      final String device = "root.db_time_range.d1";
      setupMatrixSchema(device);

      insertData(
          device, Collections.singletonList("s1_alias"), 25000, HISTORY_TIME_RANGE_POINT_COUNT);
      createFlexiblePipe(
          "pipe_history_realtime_time_range",
          "source.path",
          device + ".s1_alias",
          "source.start-time",
          "25023",
          "source.end-time",
          "25103",
          "source.inclusion",
          "data.insert",
          "source.history.enable",
          "true",
          "source.realtime.enable",
          "true");

      assertDataCount(device + ".s1_physical", 25000, 23, 0);
      assertDataCount(device + ".s1_physical", 25023, 77, 77);

      insertData(device, Collections.singletonList("s1_alias"), 25100, BATCH_POINT_COUNT);
      assertDataCount(device + ".s1_physical", 25100, 4, 4);
      assertDataCount(device + ".s1_physical", 25104, 96, 0);
      assertInternalSourcePatternHidden("pipe_history_realtime_time_range");
      dropPipe("pipe_history_realtime_time_range");

    } finally {
      cleanTestResources();
    }
  }

  /**
   * Verifies schema-only synchronization when {@code source.path} is an alias path. The receiver
   * should get the user-visible alias timeseries only, not the renamed physical path or unrelated
   * series, and internal source-pattern attributes should not be shown.
   */
  @Test
  public void testSchemaOnlyPipeSyncsRenamedSeriesByAliasPath() throws Exception {
    try {
      final String device = "root.db_schema_only.d1";
      setupSenderRenamedSeriesSchema(device);
      createTs(receiverEnv, device + ".s1_physical");

      createFlexiblePipe(
          "pipe_schema_only_alias_path",
          "source.path",
          device + ".s1_alias",
          "source.inclusion",
          "schema.timeseries",
          "source.history.enable",
          "true",
          "source.realtime.enable",
          "true");

      assertTimeseriesCount(device + ".s1_alias", 1);
      assertTimeseriesCount(device + ".s1_physical", 0);
      assertTimeseriesCount(device + ".s2_alias", 0);
      assertTimeseriesCount(device + ".s3_normal", 0);
      assertInternalSourcePatternHidden("pipe_schema_only_alias_path");
      dropPipe("pipe_schema_only_alias_path");

    } finally {
      cleanTestResources();
    }
  }

  /**
   * Verifies combined realtime schema and data synchronization for a renamed series. After a schema
   * rename is synced, inserts and deletes through the alias path should be queryable on the
   * receiver by alias, proving schema rename and data events stay consistent.
   */
  @Test
  public void testSchemaAndDataPipeSyncsAndQueriesRenamedSeriesByAliasPath() throws Exception {
    try {
      final String device = "root.db_schema_data.d1";
      final String physical = device + ".s1_physical";
      final String alias = device + ".s1_alias";
      createTs(senderEnv, physical);
      createTs(receiverEnv, physical);

      createFlexiblePipe(
          "pipe_schema_and_data_alias_path",
          "source.path",
          device + ".**",
          "source.inclusion",
          "data.insert,data.delete,schema.timeseries",
          "source.history.enable",
          "false",
          "source.realtime.enable",
          "true");

      setAlias(physical, alias);
      assertTimeseriesCount(alias, 1);
      assertTimeseriesCount(physical, 0);

      insertDataWithoutFlush(
          device, Collections.singletonList("s1_alias"), 27000, BATCH_POINT_COUNT);
      assertQueryCount(
          "SELECT COUNT(s1_alias) FROM " + device + " WHERE time >= 27000 AND time <= 27099",
          BATCH_POINT_COUNT);

      insertDataWithoutFlush(
          device, Collections.singletonList("s1_alias"), 28000, BATCH_POINT_COUNT);
      assertQueryCount(
          "SELECT COUNT(s1_alias) FROM " + device + " WHERE time >= 28000 AND time <= 28099",
          BATCH_POINT_COUNT);
      deleteData(Collections.singletonList(device + ".s1_alias"), 28000, BATCH_POINT_COUNT);
      assertQueryCount(
          "SELECT COUNT(s1_alias) FROM " + device + " WHERE time >= 28000 AND time <= 28099", 0);
      assertInternalSourcePatternHidden("pipe_schema_and_data_alias_path");
      dropPipe("pipe_schema_and_data_alias_path");

    } finally {
      cleanTestResources();
    }
  }

  /**
   * Verifies end-to-end schema and data synchronization when the receiver starts with no metadata
   * and the sender has already renamed a series several times before pipe creation. The historical
   * snapshot should restore only the latest user-visible alias, historical data written through
   * that alias should be transferred, realtime data should continue to be queryable, and a later
   * realtime rename should also be synchronized.
   */
  @Test
  public void testHistoryAndRealtimeSyncAfterChainedRenameFromEmptyReceiver() throws Exception {
    try {
      final String device = "root.db_chained_rename.d1";
      final String physical = device + ".s_physical";
      final String firstAlias = device + ".s_alias_1";
      final String secondAlias = device + ".s_alias_2";
      final String thirdAlias = device + ".s_alias_3";
      final String fourthAlias = device + ".s_alias_4";

      assertTimeseriesCount(device + ".**", 0);

      createTs(senderEnv, physical);
      setAlias(physical, firstAlias);
      setAlias(firstAlias, secondAlias);
      setAlias(secondAlias, thirdAlias);

      insertData(device, Collections.singletonList("s_alias_3"), 30000, 10);

      assertTimeseriesCount(device + ".**", 0);

      createFlexiblePipe(
          "pipe_history_realtime_chained_rename",
          "source.path",
          device + ".**",
          "source.inclusion",
          "data.insert,schema.timeseries",
          "source.history.enable",
          "true",
          "source.realtime.enable",
          "true");

      assertTimeseriesCount(physical, 0);
      assertTimeseriesCount(firstAlias, 0);
      assertTimeseriesCount(secondAlias, 0);
      assertTimeseriesCount(thirdAlias, 1);
      assertQueryCount(
          "SELECT COUNT(s_alias_3) FROM " + device + " WHERE time >= 30000 AND time <= 30009", 10);

      insertData(device, Collections.singletonList("s_alias_3"), 30100, 10);
      assertQueryCount(
          "SELECT COUNT(s_alias_3) FROM " + device + " WHERE time >= 30100 AND time <= 30109", 10);

      setAlias(thirdAlias, fourthAlias);
      assertTimeseriesCount(thirdAlias, 0);
      assertTimeseriesCount(fourthAlias, 1);

      insertData(device, Collections.singletonList("s_alias_4"), 30200, 10);
      assertQueryCount(
          "SELECT COUNT(s_alias_4) FROM " + device + " WHERE time >= 30200 AND time <= 30209", 10);
      assertInternalSourcePatternHidden("pipe_history_realtime_chained_rename");
      dropPipe("pipe_history_realtime_chained_rename");

    } finally {
      cleanTestResources();
    }
  }

  /**
   * Verifies repeated full synchronization across pipe recreation. After the first full pipe syncs
   * a renamed series and is dropped, the sender continues renaming the same series and writing
   * through the latest alias. A second full pipe should recover the latest alias and all historical
   * data accumulated before the second pipe creation.
   */
  @Test
  public void testRecreatedFullPipeSyncsLatestAliasAfterMoreChainedRenames() throws Exception {
    try {
      final String device = "root.db_recreated_full_chained_rename.d1";
      final String physical = device + ".s_physical";
      final String firstAlias = device + ".s_alias_1";
      final String secondAlias = device + ".s_alias_2";
      final String thirdAlias = device + ".s_alias_3";
      final String fourthAlias = device + ".s_alias_4";
      final String fifthAlias = device + ".s_alias_5";

      assertTimeseriesCount(device + ".**", 0);

      createTs(senderEnv, physical);
      setAlias(physical, firstAlias);
      setAlias(firstAlias, secondAlias);
      insertData(device, Collections.singletonList("s_alias_2"), 31000, 10);

      createFlexiblePipe(
          "pipe_chained_rename_first_full_sync",
          "source.path",
          device + ".**",
          "source.inclusion",
          "data.insert,schema.timeseries",
          "source.history.enable",
          "true",
          "source.realtime.enable",
          "true");

      assertTimeseriesCount(secondAlias, 1);
      assertQueryCount(
          "SELECT COUNT(s_alias_2) FROM " + device + " WHERE time >= 31000 AND time <= 31009", 10);
      dropPipe("pipe_chained_rename_first_full_sync");

      setAlias(secondAlias, thirdAlias);
      setAlias(thirdAlias, fourthAlias);
      setAlias(fourthAlias, fifthAlias);
      insertData(device, Collections.singletonList("s_alias_5"), 31100, 10);

      createFlexiblePipe(
          "pipe_chained_rename_second_full_sync",
          "source.path",
          device + ".**",
          "source.inclusion",
          "data.insert,schema.timeseries",
          "source.history.enable",
          "true",
          "source.realtime.enable",
          "true");

      assertTimeseriesCount(fifthAlias, 1);
      assertQueryCount(
          "SELECT COUNT(s_alias_5) FROM " + device + " WHERE time >= 31000 AND time <= 31009", 10);
      assertQueryCount(
          "SELECT COUNT(s_alias_5) FROM " + device + " WHERE time >= 31100 AND time <= 31109", 10);

      insertData(device, Collections.singletonList("s_alias_5"), 31200, 10);
      assertQueryCount(
          "SELECT COUNT(s_alias_5) FROM " + device + " WHERE time >= 31200 AND time <= 31209", 10);
      assertInternalSourcePatternHidden("pipe_chained_rename_second_full_sync");
      dropPipe("pipe_chained_rename_second_full_sync");

    } finally {
      cleanTestResources();
    }
  }

  /**
   * Verifies schema-only realtime rename synchronization when the source path is the original
   * physical path. Creating the physical timeseries and then renaming it should make the receiver
   * expose only the alias path, not the obsolete physical path.
   */
  @Test
  public void testSchemaOnlyPipeSyncsRealtimeRenameByPhysicalPathAndQueriesByAliasPath()
      throws Exception {
    try {
      final String device = "root.db_realtime_schema_rename.d1";

      createFlexiblePipe(
          "pipe_realtime_schema_rename_physical_path",
          "source.path",
          device + ".s1_physical",
          "source.inclusion",
          "schema.timeseries",
          "source.history.enable",
          "false",
          "source.realtime.enable",
          "true");

      createTs(senderEnv, device + ".s1_physical");
      setAlias(device + ".s1_physical", device + ".s1_alias");

      assertTimeseriesCount(device + ".s1_physical", 0);
      assertTimeseriesCount(device + ".s1_alias", 1);
      assertInternalSourcePatternHidden("pipe_realtime_schema_rename_physical_path");
      dropPipe("pipe_realtime_schema_rename_physical_path");

    } finally {
      cleanTestResources();
    }
  }

  /**
   * Verifies cross-database renamed-series capture with schema synchronization enabled. Two
   * physical series are renamed to aliases under each other's database. The pipe first captures the
   * first alias database and transfers data written through that alias, then ALTER PIPE switches
   * the capture range to the other alias database and transfers only data written through the
   * second alias.
   */
  @Test
  public void testAlterPipeCapturesCrossDatabaseRenamedSeriesWithSchemaSync() throws Exception {
    try {
      final String deviceInDatabaseA = "root.db_cross_a.d1";
      final String deviceInDatabaseB = "root.db_cross_b.d1";
      final String physicalInDatabaseA = deviceInDatabaseA + ".s_physical";
      final String physicalInDatabaseB = deviceInDatabaseB + ".s_physical";
      final String aliasInDatabaseA = deviceInDatabaseA + ".s_alias_from_b";
      final String aliasInDatabaseB = deviceInDatabaseB + ".s_alias_from_a";

      createTs(senderEnv, physicalInDatabaseA, physicalInDatabaseB);
      createTs(receiverEnv, physicalInDatabaseA, physicalInDatabaseB);

      setAlias(physicalInDatabaseB, aliasInDatabaseA);

      createFlexiblePipe(
          "pipe_cross_database_alias_capture",
          "source.pattern.inclusion",
          "root.db_cross_a.**",
          "source.pattern.format",
          "iotdb",
          "source.inclusion",
          "data.insert,schema.timeseries",
          "source.history.enable",
          "true",
          "source.realtime.enable",
          "true");

      assertTimeseriesCount(aliasInDatabaseB, 0);
      assertTimeseriesCount(aliasInDatabaseA, 1);

      insertData(deviceInDatabaseA, Collections.singletonList("s_alias_from_b"), 29000, 10);
      insertData(deviceInDatabaseA, Collections.singletonList("s_physical"), 29100, 10);

      assertQueryCount(
          "SELECT COUNT(s_physical) FROM "
              + deviceInDatabaseA
              + " WHERE time >= 29100 AND time <= 29109",
          10);
      assertQueryCount(
          "SELECT COUNT(s_alias_from_b) FROM "
              + deviceInDatabaseA
              + " WHERE time >= 29000 AND time <= 29009",
          10);

      setAlias(physicalInDatabaseA, aliasInDatabaseB);
      assertTimeseriesCount(aliasInDatabaseB, 1);

      alterPipeSource(
          "pipe_cross_database_alias_capture",
          "source.pattern.inclusion",
          "root.db_cross_b.**",
          "source.pattern.format",
          "iotdb");

      insertData(deviceInDatabaseA, Collections.singletonList("s_alias_from_b"), 29200, 10);
      insertData(deviceInDatabaseB, Collections.singletonList("s_alias_from_a"), 29300, 10);
      assertQueryCount(
          "SELECT COUNT(s_alias_from_b) FROM "
              + deviceInDatabaseA
              + " WHERE time >= 29200 AND time <= 29209",
          0);

      assertQueryCount(
          "SELECT COUNT(s_alias_from_a) FROM "
              + deviceInDatabaseB
              + " WHERE time >= 29300 AND time <= 29309",
          10);
      assertInternalSourcePatternHidden("pipe_cross_database_alias_capture");
      dropPipe("pipe_cross_database_alias_capture");

    } finally {
      cleanTestResources();
    }
  }

  /**
   * Verifies that an empty ALTER PIPE does not refresh the resolved internal source pattern
   * snapshot. After the pipe is created, a newly renamed cross-database alias should not be
   * captured unless source attributes are explicitly altered.
   */
  @Test
  public void testEmptyAlterPipeDoesNotRefreshRenamedSeriesSnapshot() throws Exception {
    try {
      final String deviceInDatabaseA = "root.db_empty_alter_a.d1";
      final String deviceInDatabaseB = "root.db_empty_alter_b.d1";
      final String firstPhysicalInDatabaseB = deviceInDatabaseB + ".s_physical_1";
      final String secondPhysicalInDatabaseB = deviceInDatabaseB + ".s_physical_2";
      final String firstAliasInDatabaseA = deviceInDatabaseA + ".s_alias_from_b_1";
      final String secondAliasInDatabaseA = deviceInDatabaseA + ".s_alias_from_b_2";

      createTs(senderEnv, firstPhysicalInDatabaseB, secondPhysicalInDatabaseB);
      createTs(receiverEnv, firstPhysicalInDatabaseB, secondPhysicalInDatabaseB);
      setAlias(firstPhysicalInDatabaseB, firstAliasInDatabaseA);

      createFlexiblePipe(
          "pipe_empty_alter_keeps_static_snapshot",
          "source.pattern.inclusion",
          "root.db_empty_alter_a.**",
          "source.pattern.format",
          "iotdb",
          "source.inclusion",
          "data.insert",
          "source.history.enable",
          "false",
          "source.realtime.enable",
          "true");

      insertData(deviceInDatabaseA, Collections.singletonList("s_alias_from_b_1"), 30000, 10);
      assertDataCount(firstPhysicalInDatabaseB, 30000, 10, 10);

      setAlias(secondPhysicalInDatabaseB, secondAliasInDatabaseA);
      alterPipeWithoutAttributes("pipe_empty_alter_keeps_static_snapshot");

      insertData(deviceInDatabaseA, Collections.singletonList("s_alias_from_b_2"), 30100, 10);
      assertDataCount(secondPhysicalInDatabaseB, 30100, 10, 0);

      insertData(deviceInDatabaseA, Collections.singletonList("s_alias_from_b_1"), 30200, 10);
      assertDataCount(firstPhysicalInDatabaseB, 30200, 10, 10);
      assertInternalSourcePatternHidden("pipe_empty_alter_keeps_static_snapshot");
      dropPipe("pipe_empty_alter_keeps_static_snapshot");

    } finally {
      cleanTestResources();
    }
  }

  /**
   * Verifies validation failures for unsafe or invalid source attributes: users cannot set internal
   * {@code __system.source.pattern.*} keys during CREATE/ALTER, and inclusion that only matches an
   * invalid renamed physical series is rejected for both path and pattern forms.
   */
  @Test
  public void testRenamedSeriesSourcePatternRejectsInvalidSourceAttributes() throws Exception {
    try {
      final String device = "root.db_reject.d1";
      setupMatrixSchema(device);

      try {
        createPipe(
            "pipe_reject_internal_create",
            "'__system.source.pattern.inclusion'='"
                + "root.db_reject.**"
                + "','source.pattern.inclusion'='"
                + "root.db_reject.**"
                + "'");
        Assert.fail("Expected create pipe to fail");
      } catch (final SQLException e) {
        Assert.assertTrue(
            "Unexpected error message: " + e.getMessage(),
            e.getMessage() != null && e.getMessage().contains("not allowed"));
      }

      createPipe("pipe_reject_internal_alter", "'source.path'='" + device + ".s3_normal" + "'");
      try {
        alterPipeSource(
            "pipe_reject_internal_alter",
            "__system.source.pattern.exclusion",
            device + ".s1_physical");
        Assert.fail("Expected alter pipe to fail");
      } catch (final SQLException e) {
        Assert.assertTrue(
            "Unexpected error message: " + e.getMessage(),
            e.getMessage() != null && e.getMessage().contains("not allowed"));
      }

      try {
        createPipe("pipe_invalid_physical_path", "'source.path'='" + device + ".s1_physical" + "'");
        Assert.fail("Expected create pipe to fail");
      } catch (final SQLException e) {
        Assert.assertTrue(
            "Unexpected error message: " + e.getMessage(),
            e.getMessage() != null && e.getMessage().contains("only matches invalid"));
      }

      createPipe("pipe_alter_invalid_physical", "'source.path'='" + device + ".s3_normal" + "'");
      try {
        alterPipeSource(
            "pipe_alter_invalid_physical",
            "source.pattern.inclusion",
            device + ".s1_physical",
            "source.pattern.format",
            "iotdb");
        Assert.fail("Expected alter pipe to fail");
      } catch (final SQLException e) {
        Assert.assertTrue(
            "Unexpected error message: " + e.getMessage(),
            e.getMessage() != null && e.getMessage().contains("only matches invalid"));
      }

      try {
        createPipe(
            "pipe_invalid_physical_pattern",
            "'source.pattern.inclusion'='"
                + device
                + ".s1_physical','source.pattern.format'='iotdb'");
        Assert.fail("Expected create pipe to fail");
      } catch (final SQLException e) {
        Assert.assertTrue(
            "Unexpected error message: " + e.getMessage(),
            e.getMessage() != null && e.getMessage().contains("only matches invalid"));
      }

    } finally {
      cleanTestResources();
    }
  }

  private static void setupMatrixSchema(final String device) throws SQLException {
    final String physical1 = device + ".s1_physical";
    final String physical2 = device + ".s2_physical";
    final String normal = device + ".s3_normal";
    createTs(senderEnv, physical1, physical2, normal);
    setAlias(physical1, device + ".s1_alias");
    setAlias(physical2, device + ".s2_alias");
    createTs(receiverEnv, physical1, physical2, normal);
  }

  private static void setupSenderRenamedSeriesSchema(final String device) throws SQLException {
    final String physical1 = device + ".s1_physical";
    final String physical2 = device + ".s2_physical";
    final String normal = device + ".s3_normal";
    createTs(senderEnv, physical1, physical2, normal);
    setAlias(physical1, device + ".s1_alias");
    setAlias(physical2, device + ".s2_alias");
  }

  private static void createTs(final BaseEnv env, final String... fullPaths) throws SQLException {
    try (final Connection connection = env.getConnection();
        final Statement statement = connection.createStatement()) {
      for (final String fullPath : fullPaths) {
        createDatabaseIfNeeded(statement, getDatabase(fullPath));
        statement.execute(
            "CREATE TIMESERIES "
                + fullPath
                + " WITH DATATYPE=INT64, ENCODING=PLAIN, COMPRESSION=SNAPPY");
      }
    }
  }

  private static void createDatabaseIfNeeded(final Statement statement, final String database)
      throws SQLException {
    try {
      statement.execute("CREATE DATABASE " + database);
    } catch (final SQLException e) {
      if (e.getMessage() == null
          || !(e.getMessage().contains("already exist")
              || e.getMessage().contains("has already been created as database"))) {
        throw e;
      }
    }
  }

  private static String getDatabase(final String fullPath) {
    final String[] nodes = fullPath.split("\\.");
    return nodes[0] + "." + nodes[1];
  }

  private static void setAlias(final String physicalPath, final String aliasPath)
      throws SQLException {
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("ALTER TIMESERIES " + physicalPath + " RENAME TO " + aliasPath);
    }
  }

  private static void createFlexiblePipe(final String pipeName, final String... sourceAttributes)
      throws SQLException {
    final StringBuilder attributes =
        new StringBuilder("'source'='iotdb-source','source.realtime.enable'='true'");
    for (int i = 0; i < sourceAttributes.length; i += 2) {
      if (sourceAttributes[i + 1] != null) {
        attributes
            .append(",'")
            .append(sourceAttributes[i])
            .append("'='")
            .append(sourceAttributes[i + 1])
            .append("'");
      }
    }

    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);
    final String sql =
        String.format(
            "CREATE PIPE %s WITH SOURCE (%s) "
                + "WITH SINK ('sink'='iotdb-thrift-sink', 'sink.ip'='%s', 'sink.port'='%s', "
                + "'sink.batch.enable'='false')",
            pipeName, attributes, receiverDataNode.getIp(), receiverDataNode.getPort());
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }

  private static void insertData(
      final String device, final List<String> measurements, final long startTime, final int count)
      throws SQLException {
    final List<String> sqls = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      final long time = startTime + i;
      final StringBuilder sql = new StringBuilder("INSERT INTO ").append(device).append("(time");
      for (final String measurement : measurements) {
        sql.append(",").append(measurement);
      }
      sql.append(") VALUES (").append(time);
      for (int j = 0; j < measurements.size(); j++) {
        sql.append(",").append(time + j);
      }
      sql.append(")");
      sqls.add(sql.toString());
    }
    sqls.add("flush");
    TestUtils.executeNonQueries(senderEnv, sqls, null);
  }

  private static void insertDataWithoutFlush(
      final String device, final List<String> measurements, final long startTime, final int count)
      throws SQLException {
    final List<String> sqls = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      final long time = startTime + i;
      final StringBuilder sql = new StringBuilder("INSERT INTO ").append(device).append("(time");
      for (final String measurement : measurements) {
        sql.append(",").append(measurement);
      }
      sql.append(") VALUES (").append(time);
      for (int j = 0; j < measurements.size(); j++) {
        sql.append(",").append(time + j);
      }
      sql.append(")");
      sqls.add(sql.toString());
    }
    TestUtils.executeNonQueries(senderEnv, sqls, null);
  }

  private static void deleteData(final List<String> paths, final long startTime, final int count)
      throws SQLException {
    final List<String> sqls = new ArrayList<>();
    for (final String path : paths) {
      sqls.add(
          "DELETE FROM "
              + path
              + " WHERE time >= "
              + startTime
              + " AND time < "
              + (startTime + count));
    }
    sqls.add("flush");
    TestUtils.executeNonQueries(senderEnv, sqls, null);
  }

  private static void assertDataCount(
      final String fullPath, final long startTime, final int count, final int expectedCount) {
    final String measurement = fullPath.substring(fullPath.lastIndexOf('.') + 1);
    final String device = fullPath.substring(0, fullPath.lastIndexOf('.'));
    final long endTime = startTime + count - 1;
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "SELECT count("
            + measurement
            + ") FROM "
            + device
            + " WHERE time >= "
            + startTime
            + " AND time <= "
            + endTime,
        "count(" + fullPath + "),",
        Collections.singleton(expectedCount + ","));
  }

  private static void assertTimeseriesCount(final String pathPattern, final int expectedCount) {
    assertQueryCount("COUNT TIMESERIES " + pathPattern, expectedCount);
  }

  private static void assertQueryCount(final String sql, final int expectedCount) {
    await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              try (final Connection connection = receiverEnv.getConnection();
                  final Statement statement = connection.createStatement();
                  final ResultSet resultSet = statement.executeQuery(sql)) {
                Assert.assertEquals(expectedCount, resultSet.next() ? resultSet.getInt(1) : 0);
              }
            });
  }

  private static void createPipe(final String pipeName, final String sourcePathAttribute)
      throws SQLException {
    createPipe(pipeName, sourcePathAttribute, "data.insert");
  }

  private static void createPipe(
      final String pipeName, final String sourcePathAttribute, final String sourceInclusion)
      throws SQLException {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);
    final String sql =
        String.format(
            "CREATE PIPE %s WITH SOURCE ('source'='iotdb-source', %s,"
                + "'source.inclusion'='%s',"
                + "'source.history.enable'='false','source.realtime.enable'='true') "
                + "WITH SINK ('sink'='iotdb-thrift-sink', 'sink.ip'='%s', 'sink.port'='%s', "
                + "'sink.batch.enable'='false')",
            pipeName,
            sourcePathAttribute,
            sourceInclusion,
            receiverDataNode.getIp(),
            receiverDataNode.getPort());
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }

  private static void alterPipeSource(final String pipeName, final String... sourceAttributes)
      throws SQLException, InterruptedException {
    final StringBuilder attributes = new StringBuilder();
    for (int i = 0; i < sourceAttributes.length; i += 2) {
      if (sourceAttributes[i + 1] == null) {
        continue;
      }
      if (attributes.length() > 0) {
        attributes.append(",");
      }
      attributes
          .append("'")
          .append(sourceAttributes[i])
          .append("'='")
          .append(sourceAttributes[i + 1])
          .append("'");
    }
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("ALTER PIPE " + pipeName + " MODIFY SOURCE (" + attributes + ")");
    }
    Thread.sleep(1000);
  }

  private static void alterPipeWithoutAttributes(final String pipeName)
      throws SQLException, InterruptedException {
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("ALTER PIPE " + pipeName);
    }
    Thread.sleep(1000);
  }

  private static void dropPipe(final String pipeName) throws SQLException {
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("DROP PIPE IF EXISTS " + pipeName);
    }
  }

  private static void cleanTestResources() throws SQLException {
    SQLException exception = null;
    exception = dropPipes(exception);
    exception = dropDatabases(senderEnv, exception);
    exception = dropDatabases(receiverEnv, exception);
    if (exception != null) {
      throw exception;
    }
  }

  private static SQLException dropPipes(SQLException exception) {
    if (senderEnv == null) {
      return exception;
    }
    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      for (final String pipeName : TEST_PIPE_NAMES) {
        statement.execute("DROP PIPE IF EXISTS " + pipeName);
      }
    } catch (final SQLException e) {
      exception = addSuppressedException(exception, e);
    }
    return exception;
  }

  private static SQLException dropDatabases(final BaseEnv env, SQLException exception) {
    if (env == null) {
      return exception;
    }
    try (final Connection connection = env.getConnection();
        final Statement statement = connection.createStatement()) {
      // Cross-database renamed series leave invalid physical series in the original database.
      // Delete timeseries first so the alias side is removed before dropping the database side.
      try {
        statement.execute("DELETE TIMESERIES root.**");
      } catch (final SQLException e) {
        if (!isPathNotExistException(e)) {
          exception = addSuppressedException(exception, e);
        }
      }
      statement.execute("DELETE DATABASE root.**");
    } catch (final SQLException e) {
      if (isPathNotExistException(e)) {
        return exception;
      }
      exception = addSuppressedException(exception, e);
    }
    return exception;
  }

  private static boolean isPathNotExistException(final SQLException exception) {
    final String message = exception.getMessage();
    return message != null
        && (message.contains("Path [root.**] does not exist")
            || message.contains("Timeseries [root.**] does not exist"));
  }

  private static SQLException addSuppressedException(
      final SQLException current, final SQLException next) {
    if (current == null) {
      return next;
    }
    current.addSuppressed(next);
    return current;
  }

  private static void assertInternalSourcePatternHidden(final String pipeName) throws Exception {
    final String extractor = getPipeExtractor(pipeName);
    Assert.assertFalse(extractor, extractor.contains("__system.source.pattern.inclusion"));
    Assert.assertFalse(extractor, extractor.contains("__system.source.pattern.exclusion"));
  }

  private static String getPipeExtractor(final String pipeName) throws Exception {
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final List<TShowPipeInfo> showPipeResult =
          client.showPipe(new TShowPipeReq().setUserName("root")).pipeInfoList;
      for (final TShowPipeInfo pipeInfo : showPipeResult) {
        if (pipeName.equals(pipeInfo.getId())) {
          return pipeInfo.getPipeExtractor();
        }
      }
      Assert.fail("Cannot find pipe " + pipeName);
      return "";
    }
  }
}
