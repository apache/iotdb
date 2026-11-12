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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.it.utils.NodeDebugger;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.TreeDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.load.active.ActiveLoadPathHelper;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.config.MppDataNodeConfig;
import org.apache.iotdb.it.env.cluster.config.MppJVMConfig;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.rpc.TSStatusCode;

import com.sun.jdi.StackFrame;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.TsFileSequenceReaderTimeseriesMetadataIterator;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.iotdb.db.it.utils.TestUtils.createUser;
import static org.apache.iotdb.db.it.utils.TestUtils.grantUserSeriesPrivilege;

/** Small, single-replica process fixture for whole-file and asynchronous handoff boundaries. */
@RunWith(IoTDBTestRunner.class)
@Category(ClusterIT.class)
public class IoTDBLoadTsFileLifecycleIT {
  private static final String DATABASE = "root.load.lifecycle";
  private static final String LOAD_UTIL = "org.apache.iotdb.db.storageengine.load.util.LoadUtil";
  private static final List<IMeasurementSchema> SCHEMAS =
      Arrays.asList(
          new MeasurementSchema("s", TSDataType.INT64),
          new MeasurementSchema("txt", TSDataType.TEXT));
  private static final long[] LOCAL_TIMES = {1, 2, 3};
  private static final long[] PARTITION_TIMES = {-10001, -10000, -1, 0, 9999, 10000};
  private static final long[] WIDE_TIMES = {0, 9999, 10000};
  private static int debugPort;
  private final Set<String> sourceNames = new HashSet<>();
  private File directory;

  @BeforeClass
  public static void setUpClass() throws Exception {
    try (ServerSocket socket = new ServerSocket(0)) {
      debugPort = socket.getLocalPort();
    }
    ((MppJVMConfig) EnvFactory.getEnv().getConfig().getDataNodeJVMConfig()).setDebugPort(debugPort);
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setTimePartitionInterval(10000)
        .setDataReplicationFactor(1)
        .setSchemaReplicationFactor(1)
        .setDatanodeMemoryProportion("1:10:1:1:1:0")
        .setDataRegionConsensusProtocolClass(ConsensusFactory.SIMPLE_CONSENSUS)
        .setDefaultDataRegionGroupNumPerDatabase(1)
        .setDataRegionGroupExtensionPolicy("CUSTOM");
    final MppDataNodeConfig config =
        (MppDataNodeConfig) EnvFactory.getEnv().getConfig().getDataNodeConfig();
    // Restarts fence the first real scan until Region routing is ready, independent of this period.
    config.setLoadActiveListeningCheckIntervalSeconds(1);
    final Properties split = new Properties();
    split.setProperty("load_tsfile_statement_split_threshold", "2");
    split.setProperty("load_tsfile_sub_statement_batch_size", "2");
    config.updateProperties(split);
    EnvFactory.getEnv().initClusterEnvironment(1, 1);
  }

  @AfterClass
  public static void tearDownClass() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Before
  public void setUp() throws Exception {
    directory = Files.createTempDirectory("load-lifecycle-").toFile();
    execute("create database " + DATABASE);
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (!remainingAsyncFiles().isEmpty()) {
        // A failed assertion can leave a published task executing. Stop its owner before removing
        // only this method's file groups, so it cannot repopulate the database in the next test.
        node().stop();
        // Re-read after stopping: a loader may have moved a file from pending to failed meanwhile.
        for (Path file : remainingAsyncFiles()) {
          Files.deleteIfExists(file);
        }
      }
      if (!node().isAlive()) {
        startNode(false);
      }
      setStatus(NodeStatus.Running);
      execute("delete database " + DATABASE);
    } finally {
      deleteDirectory(directory);
    }
  }

  private List<Path> remainingAsyncFiles() throws IOException {
    final List<Path> remaining = new ArrayList<>();
    for (File root : new File[] {pending(), new File(node().getNodePath(), "ext/load/failed")}) {
      if (root.exists()) {
        try (Stream<Path> files = Files.walk(root.toPath())) {
          files
              .filter(Files::isRegularFile)
              .filter(
                  path -> {
                    final String name = path.getFileName().toString();
                    return sourceNames.stream()
                        .anyMatch(
                            source ->
                                name.equals(source)
                                    || name.equals(source + TsFileResource.RESOURCE_SUFFIX)
                                    || name.equals(source + ModificationFile.FILE_SUFFIX));
                  })
              .forEach(remaining::add);
        }
      }
    }
    return remaining;
  }

  /** P1-10: prove the local strategy ran, unlink the source group, then recover installed files. */
  @Test
  public void testLocalLoadSurvivesSourceUnlinkAndRestart() throws Exception {
    for (boolean delete : new boolean[] {false, true}) {
      final String prefix = DATABASE + (delete ? ".local_delete" : ".local_none");
      final File source = source(delete ? "101" : "102");
      writeSource(source, prefix, LOCAL_TIMES, 1);
      execute(load(source, delete, "'database-level'='2',"));
      Assert.assertTrue(
          "Whole-file loading must not be replaced by tablet fallback",
          node()
              .logContains(
                  DataNodeQueryMessages.START_LOAD_TSFILE_LOCALLY.replace(
                      "{}", source.getAbsolutePath())));
      assertSource(source, !delete);
      assertRows(prefix, LOCAL_TIMES);
      if (!delete) {
        deleteSource(source);
      }
      restartNode();
      assertRows(prefix, LOCAL_TIMES);
      assertSource(source, false);
    }
  }

  /** P0-07: both partial and complete unpublished groups stay invisible across a real crash. */
  @Test
  public void testAsyncUnpublishedGroupCrashPreservesSources() throws Exception {
    for (boolean delete : new boolean[] {false, true}) {
      for (boolean complete : new boolean[] {false, true}) {
        final String suffix = (delete ? "delete" : "none") + (complete ? "_full" : "_partial");
        final String prefix = DATABASE + ".unpublished_" + suffix;
        final File source = source("201" + (delete ? "1" : "0") + (complete ? "1" : "0"));
        writeSource(source, prefix, LOCAL_TIMES, 1);
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        File staging = null;
        try {
          try (NodeDebugger debugger =
              new NodeDebugger(
                  debugPort, LOAD_UTIL, complete ? "publishTransferDirectory" : "transferFile")) {
            final Future<?> request =
                submit(
                    executor,
                    load(source, delete, "'async'='true','database-level'='2','verify'='true',"));
            final StackFrame frame =
                debugger.await(
                    candidate ->
                        complete
                            || NodeDebugger.filePath(candidate.getArgumentValues().get(0))
                                .equals(source.getAbsolutePath()));
            // Keep this handoff paused without trapping the canary's local transferFile call.
            debugger.disableFurtherEntryPauses();
            staging =
                complete
                    ? new File(NodeDebugger.filePath(frame.getArgumentValues().get(0)))
                    : new File(NodeDebugger.filePath(frame.getArgumentValues().get(1)))
                        .getParentFile();
            Assert.assertTrue(ActiveLoadPathHelper.isTransferStagingFile(staging, pending()));
            Assert.assertTrue(
                new File(staging, source.getName() + TsFileResource.RESOURCE_SUFFIX).isFile());
            Assert.assertTrue(
                new File(staging, source.getName() + ModificationFile.FILE_SUFFIX).isFile());
            Assert.assertEquals(complete, new File(staging, source.getName()).isFile());
            assertSource(source, true);
            assertScannerRunningWithoutImport(prefix, suffix + "_before");
            Assert.assertFalse(
                "The original handoff must remain paused while the canary is imported",
                request.isDone());
            node().stopForcibly();
            assertUnacknowledged(request);
          }
          startNode();
          assertScannerRunningWithoutImport(prefix, suffix + "_after");
          assertSource(source, true);
          // SQL has no stable handoff id: this is explicitly a new request, not original-task
          // replay.
          execute(load(source, delete, "'database-level'='2',"));
          assertRows(prefix, LOCAL_TIMES);
          assertSource(source, !delete);
        } finally {
          executor.shutdownNow();
          Assert.assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
          // Hidden crash leftovers have no automatic-reclamation contract. Remove only this test's
          // observed directory, after proving that the scanner did not ingest it.
          deleteDirectory(staging);
        }
      }
    }
  }

  /**
   * P0-07: published groups survive response loss; an acknowledged pending publication resumes too.
   */
  @Test
  public void testPublishedAsyncGroupSurvivesCrashAndReadOnlyPause() throws Exception {
    final String user = "load_handoff_user";
    final String password = "Load!Handoff2026";
    createUser(user, password);
    try {
      grantUserSeriesPrivilege(user, PrivilegeType.WRITE_DATA, DATABASE + ".**");
      grantUserSeriesPrivilege(user, PrivilegeType.WRITE_SCHEMA, DATABASE + ".**");
      for (boolean delete : new boolean[] {false, true}) {
        final String prefix = DATABASE + (delete ? ".published_delete" : ".published_none");
        final File source = source(delete ? "211" : "212");
        writeSource(source, prefix, PARTITION_TIMES, 1);
        createSchema(prefix);
        execute("insert into " + prefix + ".allocation(time,s) values(1,1)");
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        File transfer = null;
        try {
          try (NodeDebugger debugger =
              new NodeDebugger(debugPort, LOAD_UTIL, "publishTransferDirectory")) {
            final Future<?> request =
                executor.submit(
                    () -> {
                      try (Connection connection =
                              EnvFactory.getEnv().getConnection(user, password);
                          Statement statement = connection.createStatement()) {
                        statement.execute(
                            load(
                                source,
                                delete,
                                "'async'='true','database-level'='2','verify'='true','auto-create-schema'='false',"));
                      }
                      return null;
                    });
            debugger.await(candidate -> true);
            // Analysis has admitted this request, but no published group exists yet. Pause scanning
            // through the supported product API before allowing publication, without suspending it.
            setStatus(NodeStatus.ReadOnly);
            awaitDataRegionStatus(DATABASE, NodeStatus.ReadOnly);
            debugger.pauseOnExit();
            debugger.resume();
            final StackFrame published = debugger.await(candidate -> true);
            transfer = new File(NodeDebugger.filePath(published.getArgumentValues().get(1)));
            final File owned = new File(transfer, source.getName());
            assertSource(owned, true);
            Assert.assertFalse(ActiveLoadPathHelper.isTransferStagingFile(owned, pending()));
            final java.util.Map<String, String> attributes =
                ActiveLoadPathHelper.parseAttributes(owned, pending());
            Assert.assertEquals(user, attributes.get(ActiveLoadPathHelper.USER_KEY));
            Assert.assertEquals("2", attributes.get("database-level"));
            Assert.assertEquals("true", attributes.get("verify"));
            Assert.assertEquals("false", attributes.get("auto-create-schema"));
            assertSource(source, true);
            if (delete) {
              // The read-only scanner has not enqueued this publication in the in-memory loader.
              // Keep scanning paused while the acknowledged handoff deletes the source. The
              // pending group is consumed only after restart, once its Region is writable again.
              debugger.resume();
              request.get(60, TimeUnit.SECONDS);
              assertSource(source, false);
              final List<Path> publishedFiles = tsFiles(pending());
              Assert.assertEquals(1, publishedFiles.size());
              Assert.assertEquals(source.getName(), publishedFiles.get(0).getFileName().toString());
            }
            node().stopForcibly();
            if (!delete) {
              // Publication is durable, but the client did not receive an acknowledgement.
              assertUnacknowledged(request);
            }
          }
          startNode();
          setStatus(NodeStatus.Running);
          awaitRows(prefix, PARTITION_TIMES);
          assertSource(source, !delete);
          Awaitility.await()
              .atMost(60, TimeUnit.SECONDS)
              .untilAsserted(
                  () -> Assert.assertEquals(Collections.emptyList(), tsFiles(pending())));
          for (String suffix :
              new String[] {"", TsFileResource.RESOURCE_SUFFIX, ModificationFile.FILE_SUFFIX}) {
            Assert.assertFalse(new File(transfer, source.getName() + suffix).exists());
          }
          final File normal = source(delete ? "213" : "214");
          writeSource(normal, prefix + "_next", LOCAL_TIMES, 1);
          execute(load(normal, true, "'database-level'='2',"));
          assertRows(prefix + "_next", LOCAL_TIMES);
        } finally {
          executor.shutdownNow();
          Assert.assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
          if (!node().isAlive()) {
            startNode();
          }
          setStatus(NodeStatus.Running);
        }
      }
    } finally {
      execute("drop user " + user);
    }
  }

  /** P0-07: a recovered task with auto-create-schema=false must not create missing schema. */
  @Test
  public void testAsyncRecoveryHonorsDisabledSchemaCreation() throws Exception {
    final String prefix = DATABASE + ".disabled_schema";
    final File source = source("215");
    writeSource(source, prefix, PARTITION_TIMES, 1);
    execute("insert into " + DATABASE + ".allocation(time,s) values(1,1)");
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      try (NodeDebugger debugger =
          new NodeDebugger(debugPort, LOAD_UTIL, "publishTransferDirectory")) {
        final Future<?> request =
            submit(
                executor,
                load(
                    source,
                    false,
                    "'async'='true','database-level'='2','verify'='true','auto-create-schema'='false',"));
        debugger.await(candidate -> true);
        setStatus(NodeStatus.ReadOnly);
        awaitDataRegionStatus(DATABASE, NodeStatus.ReadOnly);
        debugger.pauseOnExit();
        debugger.resume();
        final StackFrame published = debugger.await(candidate -> true);
        final File transfer = new File(NodeDebugger.filePath(published.getArgumentValues().get(1)));
        final File owned = new File(transfer, source.getName());
        Assert.assertTrue(owned.isFile());
        Assert.assertFalse(ActiveLoadPathHelper.isTransferStagingFile(owned, pending()));
        Assert.assertEquals(
            "false",
            ActiveLoadPathHelper.parseAttributes(owned, pending()).get("auto-create-schema"));
        assertNoTreeSchema(prefix);
        assertSource(source, true);

        debugger.resume();
        request.get(60, TimeUnit.SECONDS);
        Assert.assertTrue(
            "The acknowledged async handoff leaves the original source", source.isFile());
        node().stopForcibly();
      }

      startNode();
      setStatus(NodeStatus.Running);
      awaitFailedSource(source);
      assertNoTreeSchema(prefix);
      assertSource(source, true);

      // The failed queued copy is terminal. This explicit new SQL request uses the retained source
      // after the caller supplies the missing schema; it is not the original task resuming.
      createSchema(prefix);
      execute(
          load(
              source, false, "'database-level'='2','verify'='true','auto-create-schema'='false',"));
      assertRows(prefix, PARTITION_TIMES);
    } finally {
      executor.shutdownNow();
      Assert.assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
      if (!node().isAlive()) {
        startNode();
      }
      setStatus(NodeStatus.Running);
      deleteFailedSource(source);
    }
  }

  /** P1-11: missing, correct and parseable stale sidecars route all real devices and partitions. */
  @Test
  public void testUntrustedResourceCannotTruncateDeviceOrTimeRange() throws Exception {
    for (int mode : new int[] {0, 1, 2}) {
      final String prefix = DATABASE + ".resource_" + mode;
      final File source = source("30" + mode);
      writeSource(source, prefix, PARTITION_TIMES, mode);
      execute(load(source, false, "'database-level'='2',"));
      assertRows(prefix, PARTITION_TIMES);
    }
    restartNode();
    for (int mode : new int[] {0, 1, 2}) {
      assertRows(DATABASE + ".resource_" + mode, PARTITION_TIMES);
    }
    final File malformed = source("309");
    final String prefix = DATABASE + ".resource_malformed";
    writeSource(malformed, prefix, PARTITION_TIMES, 1);
    Files.write(new File(malformed + TsFileResource.RESOURCE_SUFFIX).toPath(), new byte[] {0});
    final SQLException failure =
        Assert.assertThrows(
            SQLException.class, () -> execute(load(malformed, true, "'database-level'='2',")));
    Assert.assertEquals(TSStatusCode.LOAD_FILE_ERROR.getStatusCode(), failure.getErrorCode());
    assertSource(malformed, true);
    Files.delete(new File(malformed + TsFileResource.RESOURCE_SUFFIX).toPath());
    execute(load(malformed, true, "'database-level'='2',"));
    assertRows(prefix, PARTITION_TIMES);
    assertSource(malformed, false);
  }

  /** P1-13: 2 files do not split; 3 and 5 exercise threshold+1 and a short final batch. */
  @Test
  public void testDirectorySubStatementsPreserveAttributesAndTail() throws Exception {
    for (int count : new int[] {2, 3, 5}) {
      for (boolean delete : new boolean[] {false, true}) {
        assertDirectorySubStatements(count, delete, true);
      }
    }
    for (boolean delete : new boolean[] {false, true}) {
      assertDirectorySubStatements(5, delete, false);
    }
    assertDirectorySubStatementsCreateDatabasesAtExplicitLevel();
  }

  private void assertDirectorySubStatementsCreateDatabasesAtExplicitLevel() throws Exception {
    final String namespace = "root.load_split_levels";
    final File batch = new File(directory, "database-level-batch");
    Assert.assertTrue(batch.mkdir());
    final Set<String> expectedDatabases = new HashSet<>();
    for (int file = 0; file < 5; file++) {
      final String database = namespace + ".b" + file;
      expectedDatabases.add(database);
      writeSource(new File(batch, (410 + file) + "-0-0-0.tsfile"), database, PARTITION_TIMES, 1);
    }
    final Set<String> actualDatabases = new HashSet<>();
    try {
      execute(load(batch, true, "'database-level'='2','verify'='true',"));
      try (Connection connection = EnvFactory.getEnv().getConnection();
          Statement statement = connection.createStatement();
          ResultSet databases = statement.executeQuery("show databases")) {
        while (databases.next()) {
          final String database = databases.getString("Database");
          if (database.equals(namespace) || database.startsWith(namespace + ".")) {
            actualDatabases.add(database);
          }
        }
      }
      Assert.assertEquals(
          "Every child must create at explicit level 2, not the default level 1",
          expectedDatabases,
          actualDatabases);
      for (String database : expectedDatabases) {
        assertRows(database, PARTITION_TIMES);
      }
      for (int file = 0; file < 5; file++) {
        assertSource(new File(batch, (410 + file) + "-0-0-0.tsfile"), false);
      }
    } finally {
      // Discover again even when LOAD failed before the result assertion.
      try (Connection connection = EnvFactory.getEnv().getConnection();
          Statement statement = connection.createStatement();
          ResultSet databases = statement.executeQuery("show databases")) {
        while (databases.next()) {
          final String database = databases.getString("Database");
          if (database.equals(namespace) || database.startsWith(namespace + ".")) {
            actualDatabases.add(database);
          }
        }
      }
      for (String database : actualDatabases) {
        execute("delete database " + database);
      }
    }
  }

  private void assertDirectorySubStatements(int count, boolean delete, boolean verify)
      throws Exception {
    final File batch = new File(directory, "batch_" + count + "_" + delete + "_" + verify);
    Assert.assertTrue(batch.mkdir());
    final String prefix = DATABASE + ".split_" + count + "_" + delete + "_" + verify;
    for (int file = 0; file < count; file++) {
      writeSource(
          new File(batch, (400 + file) + "-0-0-0.tsfile"),
          prefix + ".f" + file,
          PARTITION_TIMES,
          1);
      if (verify) {
        createSchema(prefix + ".f" + file);
      }
    }
    final int oldBatches = batchStartCount();
    final String successRecord =
        "Successfully executed sub-statement "
            + ((count + 1) / 2)
            + "/"
            + ((count + 1) / 2)
            + " in tree model";
    final int oldSuccessRecords = logCount(successRecord);
    execute(
        load(
            batch,
            delete,
            "'database-level'='2','verify'='" + verify + "','auto-create-schema'='false',"));
    Assert.assertEquals(count > 2 ? 1 : 0, batchStartCount() - oldBatches);
    if (count > 2) {
      Assert.assertEquals(1, logCount(successRecord) - oldSuccessRecords);
    }
    for (int file = 0; file < count; file++) {
      if (!verify) {
        // If a child loses verify=false, analysis rejects the absent schema. If it loses
        // auto-create-schema=false, this assertion catches unintended schema creation.
        assertNoTreeSchema(prefix + ".f" + file);
        createSchema(prefix + ".f" + file);
      }
      assertRows(prefix + ".f" + file, PARTITION_TIMES);
      assertSource(new File(batch, (400 + file) + "-0-0-0.tsfile"), !delete);
    }
  }

  /** P1-13: failure in batch two stops batch three and cannot be replaced by a later success. */
  @Test
  public void testSubStatementSchemaFailureStopsLaterBatchesAndKeepsSources() throws Exception {
    for (boolean delete : new boolean[] {false, true}) {
      final File batch = new File(directory, "failed-batch-" + delete);
      Assert.assertTrue(batch.mkdir());
      final String prefix = DATABASE + ".split_failure_" + delete;
      for (int file = 0; file < 5; file++) {
        writeSource(
            new File(batch, (500 + file) + "-0-0-0.tsfile"),
            prefix + ".f" + file,
            PARTITION_TIMES,
            1);
        if (file != 2) {
          createSchema(prefix + ".f" + file);
        }
      }
      final int oldBatches = batchStartCount();
      final String failureRecord = "Failed to execute sub-statement 2/3 in tree model";
      final int oldFailureRecords = logCount(failureRecord);
      final SQLException failure =
          Assert.assertThrows(
              SQLException.class,
              () ->
                  execute(
                      load(
                          batch,
                          delete,
                          "'database-level'='2','verify'='true','auto-create-schema'='false',")));
      Assert.assertEquals(TSStatusCode.LOAD_FILE_ERROR.getStatusCode(), failure.getErrorCode());
      Assert.assertTrue(failure.getMessage(), failure.getMessage().contains(prefix + ".f2.d"));
      Assert.assertEquals(1, batchStartCount() - oldBatches);
      Assert.assertEquals(1, logCount(failureRecord) - oldFailureRecords);
      for (int file = 0; file < 5; file++) {
        final File source = new File(batch, (500 + file) + "-0-0-0.tsfile");
        assertSource(source, file >= 2 || !delete);
        if (file < 2) {
          assertRows(prefix + ".f" + file, PARTITION_TIMES);
        } else {
          assertEmpty(prefix + ".f" + file);
        }
      }
      try (Connection connection = EnvFactory.getEnv().getConnection();
          Statement statement = connection.createStatement();
          ResultSet schema = statement.executeQuery("show timeseries " + prefix + ".f2.**")) {
        Assert.assertFalse("auto-create-schema=false must survive splitting", schema.next());
      }
      createSchema(prefix + ".f2");
      // Retained successful sources were checked above. This new retry loads only the three files
      // that still need work, even under on-success=none.
      for (int file = 2; file < 5; file++) {
        execute(
            load(
                new File(batch, (500 + file) + "-0-0-0.tsfile"),
                delete,
                "'database-level'='2','verify'='true','auto-create-schema'='false',"));
      }
      for (int file = 0; file < 5; file++) {
        assertRows(prefix + ".f" + file, PARTITION_TIMES);
        assertSource(new File(batch, (500 + file) + "-0-0-0.tsfile"), !delete);
      }
    }
  }

  /** P1-14: no writable target must reject synchronous LOAD without deleting its source group. */
  @Test
  public void testReadOnlyTargetRejectsLoadAndResumesAfterRecovery() throws Exception {
    final String prefix = DATABASE + ".readonly";
    final File source = source("601");
    writeSource(source, prefix, LOCAL_TIMES, 1);
    createSchema(prefix);
    // Allocate the only DataRegion while it is writable. A missing partition would instead test
    // allocation failure, without reaching the existing-region read-only LOAD contract.
    execute("insert into " + DATABASE + ".allocation(time,s) values(1,1)");
    try {
      setStatus(NodeStatus.ReadOnly);
      awaitDataRegionStatus(DATABASE, NodeStatus.ReadOnly);
      final SQLException failure =
          Assert.assertThrows(
              SQLException.class, () -> execute(load(source, true, "'database-level'='2',")));
      Assert.assertEquals(TSStatusCode.LOAD_FILE_ERROR.getStatusCode(), failure.getErrorCode());
      Assert.assertTrue(failure.getMessage(), failure.getMessage().contains(source.getName()));
      Assert.assertTrue(
          failure.getMessage(), failure.getMessage().contains("no available DataRegionGroups"));
      Assert.assertTrue(failure.getMessage(), failure.getMessage().contains(DATABASE));
      assertSource(source, true);
      assertEmpty(prefix);
    } finally {
      setStatus(NodeStatus.Running);
      awaitDataRegionStatus(DATABASE, NodeStatus.Running);
    }
    execute(load(source, true, "'database-level'='2',"));
    assertRows(prefix, LOCAL_TIMES);
    assertSource(source, false);
  }

  /** P1-12: actual default metadata batches and repeated groups survive Scan-to-Query recovery. */
  @Test
  public void testWideRepeatedChunkGroupsSurviveQueryFallbackAtMetadataBoundary() throws Exception {
    // The reader's real default is 4000 metadata entries (including aligned time metadata). Verify
    // the observed batches below instead of changing the unrelated LOAD analysis memory setting.
    for (int columns : new int[] {3999, 4000}) {
      final boolean delete = columns == 4000;
      final String prefix = DATABASE + ".wide_" + columns;
      final File source = source("70" + columns);
      final List<IMeasurementSchema> schemas = new ArrayList<>();
      for (int column = 0; column < columns; column++) {
        schemas.add(new MeasurementSchema("s" + column, TSDataType.INT64));
      }
      final long damagedMarker;
      try (TsFileWriter writer = new TsFileWriter(source)) {
        writer.registerAlignedTimeseries(prefix + ".d0", schemas);
        writeWideRows(writer, prefix + ".d0", schemas, 0, 0, 1);
        writer.flush();
        // The scanner needs an intact next group marker to return the first aligned group.
        // Damage a later group so fallback must resume after a genuinely emitted prefix.
        writeWideRows(writer, prefix + ".d0", schemas, 0, 1, 1);
        writer.flush();
        damagedMarker = writer.getIOWriter().getPos();
        writeWideRows(writer, prefix + ".d0", schemas, 0, 2, 1);
        writer.flush();
        writer.registerAlignedTimeseries(prefix + ".d1", schemas.subList(0, 2));
        writeWideRows(writer, prefix + ".d1", schemas.subList(0, 2), 1, 0, 3);
      }
      final List<Integer> firstDeviceBatchSizes = new ArrayList<>();
      try (TsFileSequenceReader reader = new TsFileSequenceReader(source.getAbsolutePath())) {
        final TsFileSequenceReaderTimeseriesMetadataIterator batches =
            new TsFileSequenceReaderTimeseriesMetadataIterator(reader, false);
        while (batches.hasNext()) {
          final java.util.Map<IDeviceID, List<TimeseriesMetadata>> batch = batches.next();
          final List<TimeseriesMetadata> metadata =
              batch.get(IDeviceID.Factory.DEFAULT_FACTORY.create(prefix + ".d0"));
          if (metadata != null) {
            firstDeviceBatchSizes.add(metadata.size());
          }
        }
      }
      Assert.assertEquals(
          "Actual metadata batch boundary, including the aligned time column",
          columns == 3999 ? Collections.singletonList(4000) : Arrays.asList(4000, 1),
          firstDeviceBatchSizes);
      try (RandomAccessFile bytes = new RandomAccessFile(source, "rw")) {
        bytes.seek(damagedMarker);
        Assert.assertEquals(MetaMarker.CHUNK_GROUP_HEADER, bytes.readByte());
        bytes.seek(damagedMarker);
        bytes.writeByte(0x7f);
      }
      final TsFileResource resource = new TsFileResource(source);
      for (int device = 0; device < 2; device++) {
        final IDeviceID id = IDeviceID.Factory.DEFAULT_FACTORY.create(prefix + ".d" + device);
        resource.updateStartTime(id, WIDE_TIMES[0]);
        resource.updateEndTime(id, WIDE_TIMES[2]);
      }
      resource.serialize();
      try (ModificationFile mods = resource.getExclusiveModFile()) {
        mods.write(
            new TreeDeletionEntry(
                new MeasurementPath(prefix + ".d0.s1"), WIDE_TIMES[1], WIDE_TIMES[1]));
      }
      final ExecutorService executor = Executors.newSingleThreadExecutor();
      try {
        final Future<?> request;
        try (NodeDebugger debugger =
            new NodeDebugger(
                debugPort,
                "org.apache.iotdb.db.storageengine.load.converter.LoadTreeTsFileTabletIterator",
                "switchFromScanToQuery")) {
          request = submit(executor, load(source, delete, "'database-level'='2',"));
          final StackFrame frame = debugger.await(candidate -> true);
          final com.sun.jdi.ObjectReference emitted =
              (com.sun.jdi.ObjectReference)
                  frame
                      .thisObject()
                      .getValue(
                          frame
                              .thisObject()
                              .referenceType()
                              .fieldByName("lastEmittedTimestampsByDevice"));
          Assert.assertTrue(
              "The scan parser must already have emitted a prefix",
              ((com.sun.jdi.IntegerValue)
                          emitted.getValue(emitted.referenceType().fieldByName("size")))
                      .value()
                  > 0);
        }
        request.get(90, TimeUnit.SECONDS);
        assertSource(source, !delete);
        Assert.assertTrue(
            node()
                .logContains(
                    DataNodeQueryMessages
                        .LOAD_SUCCESSFULLY_CONVERTED_TSFILE_ARG_INTO_TABLETS_AND_INSERTED
                        .replace("{}", source.getAbsolutePath())));
        for (int device = 0; device < 2; device++) {
          final String path = prefix + ".d" + device;
          final int expectedColumns = device == 0 ? columns : 2;
          try (Connection connection = EnvFactory.getEnv().getConnection();
              Statement statement = connection.createStatement();
              ResultSet rows = statement.executeQuery("select * from " + path)) {
            Assert.assertEquals(expectedColumns + 1, rows.getMetaData().getColumnCount());
            for (int row = 0; row < 3; row++) {
              Assert.assertTrue(rows.next());
              Assert.assertEquals(WIDE_TIMES[row], rows.getLong(1));
              for (int column = 0; column < expectedColumns; column++) {
                final Long expected =
                    (device == 0 && row == 1 && column == 1)
                            || (row == 2 && column == expectedColumns - 1)
                        ? null
                        : device * 100000L + row * 10000L + column;
                Assert.assertEquals(
                    path + ".s" + column + " time=" + row,
                    expected,
                    rows.getObject(path + ".s" + column));
              }
            }
            Assert.assertFalse(rows.next());
          }
        }
      } finally {
        executor.shutdownNow();
        Assert.assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
      }
    }
  }

  /** P1-12: query fallback cannot turn an unreadable tail into successful LOAD completion. */
  @Test
  public void testWideRepeatedChunkGroupsWithUnrecoverableTailFailAndRetainSource()
      throws Exception {
    final int columns = 4000;
    final String prefix = DATABASE + ".wide_unrecoverable";
    final File source = source("711");
    final List<IMeasurementSchema> schemas = new ArrayList<>();
    for (int column = 0; column < columns; column++) {
      schemas.add(new MeasurementSchema("s" + column, TSDataType.INT64));
    }
    final long secondGroup;
    try (TsFileWriter writer = new TsFileWriter(source)) {
      final String device = prefix + ".d0";
      writer.registerAlignedTimeseries(device, schemas);
      writeWideRows(writer, device, schemas.subList(0, columns - 1), 0, 0, 1);
      writer.flush();
      secondGroup = writer.getIOWriter().getPos();
      final List<IMeasurementSchema> tailSchema =
          Collections.singletonList(schemas.get(columns - 1));
      final Tablet tail = new Tablet(device, tailSchema, 1);
      tail.addTimestamp(0, WIDE_TIMES[2]);
      tail.addValue(tailSchema.get(0).getMeasurementName(), 0, 100000L + columns - 1);
      writer.writeAligned(tail);
    }
    int firstDeviceBatches = 0;
    try (TsFileSequenceReader reader = new TsFileSequenceReader(source.getAbsolutePath())) {
      final TsFileSequenceReaderTimeseriesMetadataIterator batches =
          new TsFileSequenceReaderTimeseriesMetadataIterator(reader, false);
      while (batches.hasNext()) {
        if (batches.next().containsKey(IDeviceID.Factory.DEFAULT_FACTORY.create(prefix + ".d0"))) {
          firstDeviceBatches++;
        }
      }
      reader.position(secondGroup);
      Assert.assertEquals(MetaMarker.CHUNK_GROUP_HEADER, reader.readMarker());
      reader.readChunkGroupHeader();
      reader.readChunkHeader(reader.readMarker());
      final long compressionOffset = reader.position() - 2;
      try (RandomAccessFile bytes = new RandomAccessFile(source, "rw")) {
        bytes.seek(compressionOffset);
        bytes.writeByte(0x7f);
      }
    }
    Assert.assertEquals("The same device spans both real metadata batches", 2, firstDeviceBatches);
    final TsFileResource resource = new TsFileResource(source);
    final IDeviceID id = IDeviceID.Factory.DEFAULT_FACTORY.create(prefix + ".d0");
    resource.updateStartTime(id, 0);
    resource.updateEndTime(id, WIDE_TIMES[2]);
    resource.serialize();
    try (ModificationFile mods = resource.getExclusiveModFile()) {
      mods.write(
          new TreeDeletionEntry(
              new MeasurementPath(prefix + ".d0.s1"), WIDE_TIMES[0], WIDE_TIMES[0]));
    }

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      final Future<?> request;
      try (NodeDebugger debugger =
          new NodeDebugger(
              debugPort,
              "org.apache.iotdb.db.storageengine.load.converter.LoadTreeTsFileTabletIterator",
              "switchFromScanToQuery")) {
        request = submit(executor, load(source, true, "'database-level'='2',"));
        final StackFrame frame = debugger.await(candidate -> true);
        final com.sun.jdi.ObjectReference emitted =
            (com.sun.jdi.ObjectReference)
                frame
                    .thisObject()
                    .getValue(
                        frame
                            .thisObject()
                            .referenceType()
                            .fieldByName("lastEmittedTimestampsByDevice"));
        Assert.assertTrue(
            "The scan parser must emit a prefix before fallback reaches the damaged tail",
            ((com.sun.jdi.IntegerValue)
                        emitted.getValue(emitted.referenceType().fieldByName("size")))
                    .value()
                > 0);
      }
      final ExecutionException failure =
          Assert.assertThrows(ExecutionException.class, () -> request.get(90, TimeUnit.SECONDS));
      Assert.assertTrue(failure.toString(), failure.getCause() instanceof SQLException);
      final SQLException sqlFailure = (SQLException) failure.getCause();
      Assert.assertEquals(TSStatusCode.LOAD_FILE_ERROR.getStatusCode(), sqlFailure.getErrorCode());
      Assert.assertTrue(
          sqlFailure.getMessage(), sqlFailure.getMessage().contains(source.getName()));
      assertSource(source, true);
    } finally {
      executor.shutdownNow();
      Assert.assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
    }
  }

  /** The Scan-to-Query path must retain the caller's disabled schema-creation policy. */
  private static void writeWideRows(
      TsFileWriter writer,
      String device,
      List<IMeasurementSchema> schemas,
      int deviceIndex,
      int first,
      int count)
      throws Exception {
    final Tablet tablet = new Tablet(device, schemas, count);
    for (int row = 0; row < count; row++) {
      // Crossing a partition forces the piece reader; a one-partition file would bypass the
      // damaged group through LocalLoadStrategy without ever invoking Scan-to-Query.
      tablet.addTimestamp(row, WIDE_TIMES[first + row]);
      for (int column = 0; column < schemas.size(); column++) {
        final Long value =
            first + row == 2 && column == schemas.size() - 1
                ? null
                : deviceIndex * 100000L + (first + row) * 10000L + column;
        tablet.addValue(schemas.get(column).getMeasurementName(), row, value);
      }
    }
    writer.writeAligned(tablet);
  }

  private static void writeSource(File file, String prefix, long[] times, int resourceMode)
      throws Exception {
    try (TsFileWriter writer = new TsFileWriter(file)) {
      for (int device = 0; device < 2; device++) {
        final String path = prefix + ".d" + device;
        writer.registerAlignedTimeseries(path, SCHEMAS);
        final Tablet tablet = new Tablet(path, SCHEMAS, times.length);
        for (int row = 0; row < times.length; row++) {
          tablet.addTimestamp(row, times[row]);
          tablet.addValue(row, "s", device * 100L + row);
          tablet.addValue(row, "txt", row == 0 ? null : "d" + device + "_r" + row);
        }
        writer.writeAligned(tablet);
      }
    }
    final TsFileResource resource = new TsFileResource(file);
    for (int device = 0; device < (resourceMode == 2 ? 1 : 2); device++) {
      final IDeviceID id = IDeviceID.Factory.DEFAULT_FACTORY.create(prefix + ".d" + device);
      resource.updateStartTime(id, times[0]);
      resource.updateEndTime(id, resourceMode == 2 ? times[0] : times[times.length - 1]);
    }
    if (resourceMode != 0) {
      resource.serialize();
    }
    try (ModificationFile mods = resource.getExclusiveModFile()) {
      for (int device = 0; device < 2; device++) {
        mods.write(
            new TreeDeletionEntry(
                new MeasurementPath(prefix + ".d" + device + ".s"), times[1], times[1]));
      }
    }
  }

  private static void assertRows(String prefix, long[] times) throws Exception {
    for (int device = 0; device < 2; device++) {
      final String path = prefix + ".d" + device;
      try (Connection connection = EnvFactory.getEnv().getConnection();
          Statement statement = connection.createStatement();
          ResultSet rows = statement.executeQuery("select s,txt from " + path)) {
        for (int row = 0; row < times.length; row++) {
          Assert.assertTrue(path + " time=" + times[row], rows.next());
          Assert.assertEquals(times[row], rows.getLong(1));
          Assert.assertEquals(row == 1 ? null : device * 100L + row, rows.getObject(2));
          Assert.assertEquals(row == 0 ? null : "d" + device + "_r" + row, rows.getString(3));
        }
        Assert.assertFalse(path, rows.next());
        // Independent device/time pruning checks expose a stale start/end index even if a broad
        // query happened to read the file through another device.
        for (int row = 0; row < times.length; row++) {
          try (ResultSet point =
              statement.executeQuery("select txt,s from " + path + " where time=" + times[row])) {
            Assert.assertTrue(path + " time=" + times[row], point.next());
            Assert.assertEquals(times[row], point.getLong(1));
            Assert.assertEquals(row == 0 ? null : "d" + device + "_r" + row, point.getString(2));
            Assert.assertEquals(row == 1 ? null : device * 100L + row, point.getObject(3));
            Assert.assertFalse(point.next());
          }
        }
      }
    }
  }

  private static void assertEmpty(String prefix) throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet rows = statement.executeQuery("select ** from " + prefix + ".**")) {
      Assert.assertFalse(prefix, rows.next());
    }
  }

  private static void awaitRows(String prefix, long[] times) {
    Awaitility.await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> assertRows(prefix, times));
  }

  private static void createSchema(String prefix) throws Exception {
    for (int device = 0; device < 2; device++) {
      execute("create aligned timeseries " + prefix + ".d" + device + "(s INT64,txt TEXT)");
    }
  }

  private void assertScannerRunningWithoutImport(String prefix, String canary) throws Exception {
    final File file = new File(pending(), "799" + System.nanoTime() + "-0-0-0.tsfile");
    sourceNames.add(file.getName());
    final String canaryPrefix = DATABASE + ".canary_" + canary;
    // Publish sidecars before the complete TsFile; only an atomic final rename makes it
    // discoverable.
    final File staged = new File(file.getParentFile().getParentFile(), file.getName());
    try {
      writeSource(staged, canaryPrefix, LOCAL_TIMES, 1);
      for (String suffix :
          new String[] {TsFileResource.RESOURCE_SUFFIX, ModificationFile.FILE_SUFFIX, ""}) {
        Files.move(new File(staged + suffix).toPath(), new File(file + suffix).toPath());
      }
      awaitRows(canaryPrefix, LOCAL_TIMES);
      Awaitility.await()
          .atMost(60, TimeUnit.SECONDS)
          .untilAsserted(() -> assertSource(file, false));
      assertEmpty(prefix);
    } finally {
      for (String suffix :
          new String[] {"", TsFileResource.RESOURCE_SUFFIX, ModificationFile.FILE_SUFFIX}) {
        Files.deleteIfExists(new File(staged + suffix).toPath());
      }
    }
  }

  private static void assertSource(File source, boolean exists) {
    for (String suffix :
        new String[] {"", TsFileResource.RESOURCE_SUFFIX, ModificationFile.FILE_SUFFIX}) {
      Assert.assertEquals(source + suffix, exists, new File(source + suffix).exists());
    }
  }

  private static void assertNoTreeSchema(String prefix) throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet schema = statement.executeQuery("show timeseries " + prefix + ".**")) {
      Assert.assertFalse("Schema must remain absent for " + prefix, schema.next());
    }
  }

  private static void awaitFailedSource(File source) {
    Awaitility.await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> assertFailedSource(source));
  }

  private static void assertFailedSource(File source) {
    final File failed = new File(node().getNodePath(), "ext/load/failed");
    for (String suffix :
        new String[] {"", TsFileResource.RESOURCE_SUFFIX, ModificationFile.FILE_SUFFIX}) {
      Assert.assertTrue(new File(failed, source.getName() + suffix).isFile());
    }
  }

  private static void deleteFailedSource(File source) throws IOException {
    final File failed = new File(node().getNodePath(), "ext/load/failed");
    for (String suffix :
        new String[] {"", TsFileResource.RESOURCE_SUFFIX, ModificationFile.FILE_SUFFIX}) {
      Files.deleteIfExists(new File(failed, source.getName() + suffix).toPath());
    }
  }

  private static void deleteSource(File source) throws Exception {
    for (String suffix :
        new String[] {"", TsFileResource.RESOURCE_SUFFIX, ModificationFile.FILE_SUFFIX}) {
      Files.delete(new File(source + suffix).toPath());
    }
  }

  private static String load(File source, boolean delete, String attributes) {
    return "load '"
        + source.getAbsolutePath()
        + "' with ("
        + attributes
        + "'tablet-conversion-threshold'='-1','on-success'='"
        + (delete ? "delete" : "none")
        + "')";
  }

  private static Future<?> submit(ExecutorService executor, String sql) {
    return executor.submit(
        () -> {
          execute(sql);
          return null;
        });
  }

  private static void assertUnacknowledged(Future<?> request) {
    final ExecutionException failure =
        Assert.assertThrows(ExecutionException.class, () -> request.get(30, TimeUnit.SECONDS));
    Assert.assertTrue(failure.toString(), failure.getCause() instanceof SQLException);
  }

  private static void execute(String sql) throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnectionWithSpecifiedDataNode(node());
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }

  private static void setStatus(NodeStatus status) throws Exception {
    final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> manager =
        new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
            .createClientManager(
                new ClientPoolFactory.SyncDataNodeInternalServiceClientPoolFactory());
    try (SyncDataNodeInternalServiceClient client =
        manager.borrowClient(new TEndPoint(node().getIp(), node().getInternalPort()))) {
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client.setSystemStatus(status.getStatus()).getCode());
    } finally {
      manager.close();
    }
    EnvFactory.getEnv()
        .ensureNodeStatus(Collections.singletonList(node()), Collections.singletonList(status));
  }

  private static DataNodeWrapper node() {
    return EnvFactory.getEnv().getDataNodeWrapper(0);
  }

  private static File pending() {
    return new File(node().getNodePath(), "ext/load/pending");
  }

  private File source(String number) {
    final String name = number + "-0-0-0.tsfile";
    sourceNames.add(name);
    return new File(directory, name);
  }

  private static void restartNode() throws Exception {
    node().stop();
    startNode();
  }

  private static void startNode() throws Exception {
    startNode(true);
  }

  private static void startNode(boolean requireDataRegion) throws Exception {
    node().startWithDebugger();
    try (NodeDebugger scanner =
        new NodeDebugger(
            debugPort,
            "org.apache.iotdb.db.storageengine.load.active.ActiveLoadDirScanner",
            "scan")) {
      scanner.resumeStartup();
      scanner.await(frame -> true);
      // StorageEngine schedules scanning before startup has finished reporting Region heartbeats.
      // Only the scanner thread is paused; RPC, recovery and heartbeat threads remain live.
      EnvFactory.getEnv()
          .ensureNodeStatus(
              Collections.singletonList(node()), Collections.singletonList(NodeStatus.Running));
      if (requireDataRegion) {
        awaitDataRegionStatus(DATABASE, NodeStatus.Running);
      }
    }
  }

  private static void awaitDataRegionStatus(String database, NodeStatus expectedStatus) {
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(200, TimeUnit.MILLISECONDS)
        .ignoreExceptions()
        .untilAsserted(
            () -> {
              try (Connection connection = EnvFactory.getEnv().getConnection();
                  Statement statement = connection.createStatement();
                  ResultSet regions = statement.executeQuery("show regions")) {
                int dataRegionCount = 0;
                while (regions.next()) {
                  if (database.equals(regions.getString("Database"))
                      && "DataRegion".equals(regions.getString("Type"))) {
                    dataRegionCount++;
                    Assert.assertEquals(expectedStatus.getStatus(), regions.getString("Status"));
                  }
                }
                Assert.assertTrue("No DataRegion found for " + database, dataRegionCount > 0);
              }
            });
  }

  private static List<Path> tsFiles(File root) throws Exception {
    if (!root.exists()) {
      return Collections.emptyList();
    }
    try (Stream<Path> files = Files.walk(root.toPath())) {
      return files
          .filter(Files::isRegularFile)
          .filter(path -> path.toString().endsWith(".tsfile"))
          .collect(Collectors.toList());
    }
  }

  private static int batchStartCount() throws Exception {
    try (Stream<String> lines =
        Files.lines(new File(node().getLogDirPath(), node().getId() + ".log").toPath())) {
      return (int)
          lines
              .filter(
                  line -> line.contains("Start batch executing") && line.contains("in tree model"))
              .count();
    }
  }

  private static int logCount(String message) throws Exception {
    try (Stream<String> lines =
        Files.lines(new File(node().getLogDirPath(), node().getId() + ".log").toPath())) {
      return (int) lines.filter(line -> line.contains(message)).count();
    }
  }

  private static void deleteDirectory(File root) throws Exception {
    if (root == null || !root.exists()) {
      return;
    }
    try (Stream<Path> paths = Files.walk(root.toPath())) {
      for (Path path : paths.sorted(Comparator.reverseOrder()).collect(Collectors.toList())) {
        Files.delete(path);
      }
    }
  }
}
