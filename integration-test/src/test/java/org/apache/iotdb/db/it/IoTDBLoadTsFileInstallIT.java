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

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.utils.NodeDebugger;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.TreeDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.v1.Deletion;
import org.apache.iotdb.db.storageengine.dataregion.modification.v1.ModificationFileV1;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.config.MppJVMConfig;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;

import com.sun.jdi.ObjectReference;
import com.sun.jdi.StackFrame;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
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
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** A single-replica process fixture for the local whole-file installation commit boundary. */
@RunWith(IoTDBTestRunner.class)
@Category(ClusterIT.class)
public class IoTDBLoadTsFileInstallIT {
  private static final String DATABASE = "root.load.install";
  private static final String DATA_REGION =
      "org.apache.iotdb.db.storageengine.dataregion.DataRegion";
  private static final long[] TIMES = {-3, -2, -1};
  private static final List<IMeasurementSchema> SCHEMAS =
      Arrays.asList(
          new MeasurementSchema("s", TSDataType.INT64),
          new MeasurementSchema("txt", TSDataType.TEXT));
  private static int debugPort;
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
        .setDataReplicationFactor(1)
        .setSchemaReplicationFactor(1)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.SIMPLE_CONSENSUS)
        .setDefaultDataRegionGroupNumPerDatabase(1)
        .setDataRegionGroupExtensionPolicy("CUSTOM");
    EnvFactory.getEnv().initClusterEnvironment(1, 1);
  }

  @AfterClass
  public static void tearDownClass() {
    try {
      EnvFactory.getEnv().cleanClusterEnvironment();
    } finally {
      ((MppJVMConfig) EnvFactory.getEnv().getConfig().getDataNodeJVMConfig()).setDebugPort(0);
    }
  }

  @Before
  public void setUp() throws Exception {
    directory = Files.createTempDirectory("load-install-").toFile();
    execute("create database " + DATABASE);
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (!node().isAlive()) {
        startNode();
      }
      execute("delete database " + DATABASE);
    } finally {
      try (Stream<Path> paths = Files.walk(directory.toPath())) {
        for (Path path : paths.sorted(Comparator.reverseOrder()).collect(Collectors.toList())) {
          Files.delete(path);
        }
      }
    }
  }

  @Test
  public void testCrashBeforeDeletionInstallationDoesNotPublishPartialFile() throws Exception {
    assertCrashBoundary(false, false);
  }

  @Test
  public void testCrashAfterDeletionInstallationDoesNotPublishPartialFile() throws Exception {
    assertCrashBoundary(false, true);
  }

  @Test
  public void testPublishedFileSurvivesCrashBeforeClientAcknowledgement() throws Exception {
    assertCrashBoundary(true, true);
  }

  private void assertCrashBoundary(boolean published, boolean deletionsInstalled) throws Exception {
    for (boolean delete : new boolean[] {false, true}) {
      for (boolean legacy : new boolean[] {false, true}) {
        final String prefix = DATABASE + ".d" + delete + "_v" + legacy;
        final File source =
            new File(directory, (100 + (delete ? 2 : 0) + (legacy ? 1 : 0)) + "-0-0-0.tsfile");
        writeSource(source, prefix, legacy);
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        final File target;
        final boolean visibleBeforeCrash;
        final Future<?> request;
        try {
          try (NodeDebugger debugger =
              new NodeDebugger(
                  debugPort, DATA_REGION, published ? "loadTsFileToUnSequence" : "loadModFile")) {
            request =
                executor.submit(
                    () -> {
                      execute(load(source, delete));
                      return null;
                    });
            StackFrame frame =
                debugger.await(
                    candidate ->
                        NodeDebugger.filePath(candidate.getArgumentValues().get(0))
                            .equals(source.getAbsolutePath()));
            if (published) {
              debugger.pauseOnExit();
              debugger.resume();
              frame = debugger.await(candidate -> true);
              final ObjectReference resource = (ObjectReference) frame.getArgumentValues().get(1);
              target =
                  new File(
                      NodeDebugger.filePath(
                          resource.getValue(resource.referenceType().fieldByName("file"))));
              Assert.assertTrue(
                  "Published file must already include all deletions",
                  ModificationFile.getExclusiveMods(target).isFile());
            } else {
              target = new File(NodeDebugger.filePath(frame.getArgumentValues().get(1)));
              if (deletionsInstalled) {
                debugger.pauseOnExit();
                debugger.resume();
                debugger.await(candidate -> true);
              }
              Assert.assertEquals(
                  deletionsInstalled, ModificationFile.getExclusiveMods(target).isFile());
            }
            Assert.assertTrue(new File(target + TsFileResource.RESOURCE_SUFFIX).isFile());
            visibleBeforeCrash = target.isFile();
            Assert.assertFalse(
                "The original SQL must still be awaiting its response", request.isDone());
            node().stopForcibly();
            final ExecutionException failure =
                Assert.assertThrows(
                    ExecutionException.class, () -> request.get(30, TimeUnit.SECONDS));
            Assert.assertTrue(failure.toString(), failure.getCause() instanceof SQLException);
          }
          // The commit can complete its source cleanup before the client receives a response.
          assertSource(source, legacy, !published || !delete);
          startNode();
          if (published) {
            Assert.assertTrue(visibleBeforeCrash);
            assertRows(prefix);
          } else {
            // Check recovery before any new LOAD; retrying first could conceal resurrection.
            try (Connection connection = EnvFactory.getEnv().getConnection();
                Statement statement = connection.createStatement();
                ResultSet rows =
                    statement.executeQuery("select s from " + prefix + ".d0 where time=-2")) {
              Assert.assertFalse("Recovery must not resurrect a deleted value", rows.next());
            }
            Assert.assertFalse(
                "A file without its deletions must never be published", visibleBeforeCrash);
            try (Connection connection = EnvFactory.getEnv().getConnection();
                Statement statement = connection.createStatement();
                ResultSet rows = statement.executeQuery("select ** from " + prefix + ".**")) {
              Assert.assertFalse(
                  "Unpublished data must stay invisible after recovery", rows.next());
            }
            Assert.assertFalse(target.exists());
            Assert.assertFalse(new File(target + ".load-pending").exists());
            Assert.assertFalse(new File(target + TsFileResource.RESOURCE_SUFFIX).exists());
            Assert.assertFalse(ModificationFile.getExclusiveMods(target).exists());
            Assert.assertFalse(ModificationFileV1.getNormalMods(target).exists());
            // This is an explicit new submission, not recovery of the interrupted request.
            execute(load(source, delete));
            assertRows(prefix);
            assertSource(source, legacy, !delete);
          }
        } finally {
          executor.shutdownNow();
          Assert.assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
        }
      }
    }
  }

  private static void writeSource(File source, String prefix, boolean legacy) throws Exception {
    try (TsFileWriter writer = new TsFileWriter(source)) {
      for (int device = 0; device < 2; device++) {
        final String path = prefix + ".d" + device;
        writer.registerAlignedTimeseries(path, SCHEMAS);
        final Tablet tablet = new Tablet(path, SCHEMAS, TIMES.length);
        for (int row = 0; row < TIMES.length; row++) {
          tablet.addTimestamp(row, TIMES[row]);
          tablet.addValue(row, "s", device * 100L + row);
          tablet.addValue(row, "txt", row == 0 ? null : "d" + device + "_r" + row);
        }
        writer.writeAligned(tablet);
      }
    }
    final TsFileResource resource = new TsFileResource(source);
    for (int device = 0; device < 2; device++) {
      final IDeviceID id = IDeviceID.Factory.DEFAULT_FACTORY.create(prefix + ".d" + device);
      resource.updateStartTime(id, TIMES[0]);
      resource.updateEndTime(id, TIMES[TIMES.length - 1]);
    }
    resource.serialize();
    if (legacy) {
      try (ModificationFileV1 mods = ModificationFileV1.getNormalMods(resource)) {
        for (int device = 0; device < 2; device++) {
          mods.write(
              new Deletion(
                  new MeasurementPath(prefix + ".d" + device + ".s"),
                  Long.MAX_VALUE,
                  TIMES[1],
                  TIMES[1]));
        }
      }
    } else {
      try (ModificationFile mods = resource.getExclusiveModFile()) {
        for (int device = 0; device < 2; device++) {
          mods.write(
              new TreeDeletionEntry(
                  new MeasurementPath(prefix + ".d" + device + ".s"), TIMES[1], TIMES[1]));
        }
      }
    }
  }

  private static void assertRows(String prefix) throws Exception {
    for (int device = 0; device < 2; device++) {
      try (Connection connection = EnvFactory.getEnv().getConnection();
          Statement statement = connection.createStatement();
          ResultSet rows = statement.executeQuery("select s,txt from " + prefix + ".d" + device)) {
        for (int row = 0; row < TIMES.length; row++) {
          Assert.assertTrue(rows.next());
          Assert.assertEquals(TIMES[row], rows.getLong(1));
          Assert.assertEquals(row == 1 ? null : device * 100L + row, rows.getObject(2));
          Assert.assertEquals(row == 0 ? null : "d" + device + "_r" + row, rows.getString(3));
        }
        Assert.assertFalse(rows.next());
      }
    }
  }

  private static void assertSource(File source, boolean legacy, boolean exists) {
    Assert.assertEquals(exists, source.isFile());
    Assert.assertEquals(exists, new File(source + TsFileResource.RESOURCE_SUFFIX).isFile());
    Assert.assertEquals(
        exists,
        (legacy
                ? ModificationFileV1.getNormalMods(source)
                : ModificationFile.getExclusiveMods(source))
            .isFile());
  }

  private static String load(File source, boolean delete) {
    return "load '"
        + source.getAbsolutePath()
        + "' with ('database-level'='2','tablet-conversion-threshold'='-1','on-success'='"
        + (delete ? "delete" : "none")
        + "')";
  }

  private static void execute(String sql) throws Exception {
    try (Connection connection =
            EnvFactory.getEnv().getWriteOnlyConnectionWithSpecifiedDataNode(node());
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }

  private static DataNodeWrapper node() {
    return EnvFactory.getEnv().getDataNodeWrapperList().get(0);
  }

  private static void startNode() throws Exception {
    node().start();
    EnvFactory.getEnv()
        .ensureNodeStatus(
            Collections.singletonList(node()), Collections.singletonList(NodeStatus.Running));
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              try (Connection connection = EnvFactory.getEnv().getConnection();
                  Statement statement = connection.createStatement();
                  ResultSet regions = statement.executeQuery("show regions")) {
                int count = 0;
                while (regions.next()) {
                  if (DATABASE.equals(regions.getString("Database"))
                      && "DataRegion".equals(regions.getString("Type"))) {
                    count++;
                    Assert.assertEquals(
                        NodeStatus.Running.getStatus(), regions.getString("Status"));
                  }
                }
                Assert.assertTrue("The actual DataRegion must be ready after restart", count > 0);
              }
            });
  }
}
