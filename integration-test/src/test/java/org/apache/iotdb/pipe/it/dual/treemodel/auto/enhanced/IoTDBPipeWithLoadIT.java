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

package org.apache.iotdb.pipe.it.dual.treemodel.auto.enhanced;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTreeAutoEnhanced;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.pipe.it.dual.treemodel.auto.AbstractPipeDualTreeModelAutoIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTreeAutoEnhanced.class})
public class IoTDBPipeWithLoadIT extends AbstractPipeDualTreeModelAutoIT {

  @Before
  public void setUp() {
    MultiEnvFactory.createEnv(2);
    senderEnv = MultiEnvFactory.getEnv(0);
    receiverEnv = MultiEnvFactory.getEnv(1);

    senderEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setDatanodeMemoryProportion("3:3:1:1:1:0")
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        // Disable sender compaction to test mods
        .setEnableSeqSpaceCompaction(false)
        .setEnableUnseqSpaceCompaction(false)
        .setEnableCrossSpaceCompaction(false)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false);
    receiverEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setDatanodeMemoryProportion("3:3:1:1:1:0")
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false);

    // 10 min, assert that the operations will not time out
    senderEnv.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);
    receiverEnv.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);

    senderEnv.getConfig().getConfigNodeConfig().setLeaderDistributionPolicy("HASH");

    senderEnv.initClusterEnvironment();
    receiverEnv.initClusterEnvironment();
  }

  /**
   * Test that when the receiver loads data from TsFile, it will not load timeseries that are
   * completed deleted by mods.
   *
   * @throws Exception
   */
  @Test
  public void testReceiverNotLoadDeletedTimeseries() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);
    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    final Map<String, String> sourceAttributes = new HashMap<>();
    final Map<String, String> processorAttributes = new HashMap<>();
    final Map<String, String> sinkAttributes = new HashMap<>();

    // Enable mods transfer
    sourceAttributes.put("source.mods.enable", "true");
    sourceAttributes.put("user", "root");

    sinkAttributes.put("sink.batch.enable", "false");
    sinkAttributes.put("sink.ip", receiverIp);
    sinkAttributes.put("sink.port", Integer.toString(receiverPort));

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      // Generate TsFile and mods on sender. There are 6 time-series in total.
      // Time-series not affected by mods: d1.s1, d2.s1
      // Time-series partially deleted by mods: d1.s2, d3.s1
      // Time-series completely deleted by mods: d1.s3, d4.s1 (should not be loaded by receiver)
      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              "insert into root.db.d1 (time, s1, s2, s3) values (1, 1, 1, 1), (3, 3, 3, 3)",
              "insert into root.db.d2 (time, s1) values (1, 1), (3, 3)",
              "insert into root.db.d3 (time, s1) values (1, 1), (3, 3)",
              "insert into root.db.d4 (time, s1) values (1, 1), (3, 3)",
              "flush"),
          null);
      // adding new devices may create a new region, and thus trigger leader re-balancing
      // the old leader may not have synced data to the new leader, and the result is that
      // the following deletions performed on the new leader can not delete anything
      // wait for a while to increase the chance that data has been synced to the new leader
      Thread.sleep(5000);
      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              "delete from root.db.d1.s2 where time <= 2",
              "delete from root.db.d1.s3 where time >= 1 and time <= 3",
              "delete from root.db.d3.** where time <= 2",
              "delete from root.db.d4.** where time >= 1 and time <= 3",
              "flush"),
          null);

      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "count timeseries root.db.**",
          "count(timeseries),",
          Collections.singleton("4,"));
    }
  }

  @Test
  public void testSqlLoadResetsOldPipeMarkerAndPipeLoadIsNotForwardedAgain() throws Exception {
    final File tmpDir = Files.createTempDirectory("pipe-load-marker").toFile();
    final String loadedDatabase = "root.load_p119";
    final String loadedDevice = loadedDatabase + ".stale_resource_device";
    final String localDatabase = "root.load_p119_local";
    final String localDevice = localDatabase + ".receiver_device";
    final File inputFile = new File(tmpDir, "1-0-0-0.tsfile");
    try {
      try (Connection connection = senderEnv.getConnection();
          Statement statement = connection.createStatement()) {
        statement.execute("create database " + loadedDatabase);
        statement.execute("create timeseries " + loadedDevice + ".value INT64");
        statement.execute(
            "insert into " + loadedDevice + " (time, value) values (1, 11), (2, 22), (3, 33)");
        statement.execute("flush");
        statement.execute("delete from " + loadedDevice + ".value where time = 2");
        statement.execute("flush");
      }

      // Create the pipe only after sealing the file and its mods. A realtime tablet event
      // could otherwise make the query succeed before any Pipe-marked TsFile exists.
      createLoadPipe(senderEnv, "bootstrapPipe", receiverEnv, true);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select time, value from " + loadedDevice,
          "Time," + loadedDevice + ".value,",
          Set.of("1,11,", "3,33,"));

      stopPipe(senderEnv, "bootstrapPipe");
      final File pipeGeneratedFile = findPipeGeneratedTsFile(receiverEnv);
      Assert.assertNotNull("Pipe receive did not persist a Pipe-marked TsFile", pipeGeneratedFile);
      final TsFileResource pipeGeneratedResource = new TsFileResource(pipeGeneratedFile);
      pipeGeneratedResource.deserialize();
      Assert.assertTrue(pipeGeneratedResource.isGeneratedByPipe());
      final File pipeGeneratedMods =
          new File(pipeGeneratedFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX);
      Assert.assertTrue(
          "The Pipe fixture must retain its deletion mods", pipeGeneratedMods.exists());
      Files.copy(pipeGeneratedFile.toPath(), inputFile.toPath());
      Files.copy(
          new File(pipeGeneratedFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).toPath(),
          new File(inputFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).toPath());
      copySidecarIfExists(pipeGeneratedFile, inputFile, ModificationFile.FILE_SUFFIX);
      Assert.assertTrue(
          "The copied SQL LOAD input must retain its deletion mods",
          new File(inputFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX).exists());

      deleteDatabase(senderEnv, loadedDatabase);
      assertNoTimeseries(senderEnv, loadedDatabase);

      createLoadPipe(receiverEnv, "doNotForwardPipeLoad", senderEnv, false);
      try (Connection connection = receiverEnv.getConnection();
          Statement statement = connection.createStatement()) {
        statement.execute("create database " + localDatabase);
        statement.execute("create timeseries " + localDevice + ".value INT64");
        statement.execute("insert into " + localDevice + " (time, value) values (100, 999)");
      }

      TestUtils.assertDataEventuallyOnEnv(
          senderEnv,
          "select time, value from " + localDevice,
          "Time," + localDevice + ".value,",
          Collections.singleton("100,999,"));

      try (Connection connection = senderEnv.getConnection();
          Statement statement = connection.createStatement();
          ResultSet resultSet =
              statement.executeQuery("show timeseries " + loadedDatabase + ".**")) {
        Assert.assertFalse(
            "A Pipe-generated historical LOAD was forwarded to its source", resultSet.next());
      }
      stopPipe(receiverEnv, "doNotForwardPipeLoad");

      deleteDatabase(senderEnv, loadedDatabase);
      deleteDatabase(receiverEnv, loadedDatabase);
      assertNoTimeseries(senderEnv, loadedDatabase);
      assertNoTimeseries(receiverEnv, loadedDatabase);

      createLoadPipe(senderEnv, "loadToReceiver", receiverEnv, false);
      try (Connection connection = senderEnv.getConnection();
          Statement statement = connection.createStatement()) {
        statement.execute(
            String.format(
                "load \"%s\" with ('database-level'='2','on-success'='none')",
                inputFile.getAbsolutePath()));
      }

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select time, value from " + loadedDevice,
          "Time," + loadedDevice + ".value,",
          Set.of("1,11,", "3,33,"));
      try (Connection connection = receiverEnv.getConnection();
          Statement statement = connection.createStatement();
          ResultSet resultSet =
              statement.executeQuery(
                  "select time, value from " + loadedDevice + " where time = 2")) {
        Assert.assertFalse("The SQL LOAD copy's deletion mods must be preserved", resultSet.next());
      }

      stopPipe(senderEnv, "loadToReceiver");

      deleteDatabase(receiverEnv, loadedDatabase);
      deleteDatabase(senderEnv, localDatabase);
      deleteDatabase(receiverEnv, localDatabase);
      assertNoTimeseries(receiverEnv, loadedDatabase);
      assertNoTimeseries(senderEnv, localDatabase);
      assertNoTimeseries(receiverEnv, localDatabase);

      createLoadPipe(senderEnv, "historicalLoadToReceiver", receiverEnv, false);
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select time, value from " + loadedDevice,
          "Time," + loadedDevice + ".value,",
          Set.of("1,11,", "3,33,"));
      try (Connection connection = receiverEnv.getConnection();
          Statement statement = connection.createStatement();
          ResultSet resultSet =
              statement.executeQuery(
                  "select time, value from " + loadedDevice + " where time = 2")) {
        Assert.assertFalse(
            "Historical extraction must preserve the SQL LOAD copy's deletion mods",
            resultSet.next());
      }

      stopPipe(senderEnv, "historicalLoadToReceiver");
    } finally {
      stopPipeIfPresent(receiverEnv, "doNotForwardPipeLoad");
      stopPipeIfPresent(senderEnv, "loadToReceiver");
      stopPipeIfPresent(senderEnv, "historicalLoadToReceiver");
      stopPipeIfPresent(senderEnv, "bootstrapPipe");
      deleteDatabase(senderEnv, loadedDatabase);
      deleteDatabase(receiverEnv, loadedDatabase);
      deleteDatabase(senderEnv, localDatabase);
      deleteDatabase(receiverEnv, localDatabase);
      deleteRecursively(tmpDir);
    }
  }

  private void createLoadPipe(
      final BaseEnv sourceEnv,
      final String pipeName,
      final BaseEnv sinkEnv,
      final boolean forwardingPipeRequests)
      throws Exception {
    final DataNodeWrapper sinkDataNode = sinkEnv.getDataNodeWrapper(0);
    final Map<String, String> sourceAttributes = new HashMap<>();
    sourceAttributes.put("source.mods.enable", "true");
    sourceAttributes.put(
        "source.forwarding-pipe-requests", Boolean.toString(forwardingPipeRequests));
    sourceAttributes.put("user", "root");

    final Map<String, String> processorAttributes = new HashMap<>();
    final Map<String, String> sinkAttributes = new HashMap<>();
    sinkAttributes.put("sink.batch.enable", "false");
    sinkAttributes.put("sink.ip", sinkDataNode.getIp());
    sinkAttributes.put("sink.port", Integer.toString(sinkDataNode.getPort()));

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) sourceEnv.getLeaderConfigNodeConnection()) {
      final TSStatus createStatus =
          client.createPipe(
              new TCreatePipeReq(pipeName, sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), createStatus.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe(pipeName).getCode());
    }
  }

  private void stopPipe(final BaseEnv sourceEnv, final String pipeName) throws Exception {
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) sourceEnv.getLeaderConfigNodeConnection()) {
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.stopPipe(pipeName).getCode());
    }
  }

  private void stopPipeIfPresent(final BaseEnv sourceEnv, final String pipeName) {
    try {
      stopPipe(sourceEnv, pipeName);
    } catch (final Exception ignored) {
      // A pipe may not have been created if its setup failed.
    }
  }

  private File findPipeGeneratedTsFile(final BaseEnv env) throws Exception {
    for (final DataNodeWrapper dataNodeWrapper : env.getDataNodeWrapperList()) {
      final File tsFile = findPipeGeneratedTsFile(dataNodeWrapper);
      if (tsFile != null) {
        return tsFile;
      }
    }
    return null;
  }

  private File findPipeGeneratedTsFile(final DataNodeWrapper dataNodeWrapper) throws Exception {
    final Path dataPath = new File(dataNodeWrapper.getDataPath()).toPath();
    try (Stream<Path> paths = Files.walk(dataPath)) {
      final Iterator<Path> iterator =
          paths.filter(path -> path.toString().endsWith(".tsfile")).iterator();
      while (iterator.hasNext()) {
        final File tsFile = iterator.next().toFile();
        final File resourceFile =
            new File(tsFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX);
        if (!resourceFile.exists()) {
          continue;
        }
        try {
          final TsFileResource resource = new TsFileResource(tsFile);
          resource.deserialize();
          if (resource.isGeneratedByPipe()) {
            return tsFile;
          }
        } catch (final Exception ignored) {
          // Ignore unrelated or concurrently changing TsFile resources.
        }
      }
    }
    return null;
  }

  private void deleteDatabase(final BaseEnv env, final String database) {
    try (Connection connection = env.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("delete database " + database);
    } catch (final Exception ignored) {
      // The database may not exist in this environment.
    }
  }

  private void assertNoTimeseries(final BaseEnv env, final String database) throws Exception {
    try (Connection connection = env.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("show timeseries " + database + ".**")) {
      Assert.assertFalse("Database should be empty before LOAD: " + database, resultSet.next());
    }
  }

  private void copySidecarIfExists(
      final File sourceTsFile, final File targetTsFile, final String suffix) throws Exception {
    final File sourceSidecar = new File(sourceTsFile.getAbsolutePath() + suffix);
    if (sourceSidecar.exists()) {
      Files.copy(
          sourceSidecar.toPath(), new File(targetTsFile.getAbsolutePath() + suffix).toPath());
    }
  }

  private void deleteRecursively(final File file) throws Exception {
    if (file == null || !file.exists()) {
      return;
    }
    final File[] children = file.listFiles();
    if (children != null) {
      for (final File child : children) {
        deleteRecursively(child);
      }
    }
    Files.deleteIfExists(file.toPath());
  }
}
