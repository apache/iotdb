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

package org.apache.iotdb.db.storageengine.dataregion.snapshot;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.ObjectTypeUtils;

import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.TsFileGeneratorUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;
import static org.apache.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

/**
 * SnapshotObjectFilesTest verifies the integrated snapshot capabilities for both standard TsFiles
 * and custom Object files within IoTDB Storage Engine.
 */
public class SnapshotObjectFilesTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotObjectFilesTest.class);

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  private String[][] originDataDirs;
  private Path dataRootPath;
  private Path snapshotPath;

  private final String testSgName = "root.testsg";
  private final String testRegionId = "0";
  private final String timePartition = "0";

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    originDataDirs = IoTDBDescriptor.getInstance().getConfig().getTierDataDirs();

    // Initialize sandbox directories
    dataRootPath = tempFolder.newFolder("data").toPath();
    String[][] testDataDirs = new String[][] {{dataRootPath.toAbsolutePath().toString()}};
    IoTDBDescriptor.getInstance().getConfig().setTierDataDirs(testDataDirs);
    TierManager.getInstance().resetFolders();

    // Target directory where SnapshotTaker will export data
    snapshotPath = tempFolder.newFolder("snapshot_export").toPath();
  }

  @After
  public void tearDown() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setTierDataDirs(originDataDirs);
    TierManager.getInstance().resetFolders();
    EnvironmentUtils.cleanEnv();
  }

  /**
   * Test basic snapshot creation. Logic: Populates DataRegion with TsFiles and Object files, then
   * triggers SnapshotTaker. Verification: Validates the physical existence of linked files in the
   * snapshot directory and the integrity of the Snapshot Log.
   */
  @Test
  public void testCreateSnapshotWithMixedFiles() throws Exception {
    DataRegion region = createAndPopulateDataRegion();
    Set<Path> expectedObjectFiles = prepareObjectFiles(testRegionId);

    SnapshotTaker snapshotTaker = new SnapshotTaker(region);
    // Execute snapshot
    boolean success =
        snapshotTaker.takeFullSnapshot(snapshotPath.toAbsolutePath().toString(), true);
    Assert.assertTrue("Snapshot execution failed", success);

    // 1. Verify Snapshot Log
    File logFile = snapshotPath.resolve(SnapshotLogger.SNAPSHOT_LOG_NAME).toFile();
    Assert.assertTrue("Snapshot log must exist", logFile.exists());

    SnapshotLogAnalyzer analyzer = new SnapshotLogAnalyzer(logFile);
    Assert.assertTrue("Log must mark snapshot as complete", analyzer.isSnapshotComplete());

    Path actualRoot = resolveActualSnapshotRoot();
    // 2. Verify TsFile physical structure in snapshot
    validateTsFileSnapshotStructure(actualRoot);

    // 3. Verify Object file physical structure
    validateObjectFileSnapshotStructure(actualRoot, expectedObjectFiles);

    LOGGER.info("testCreateSnapshotWithMixedFiles completed successfully.");
  }

  private Path resolveActualSnapshotRoot() {
    // Get the first data directory configured in the test
    String dataDir = IoTDBDescriptor.getInstance().getConfig().getTierDataDirs()[0][0];

    return Paths.get(dataDir)
        .resolve(IoTDBConstant.SNAPSHOT_FOLDER_NAME) // "snapshot"
        .resolve(testSgName + IoTDBConstant.FILE_NAME_SEPARATOR + testRegionId) // "root.testsg-0"
        .resolve(snapshotPath.getFileName().toString()); // "snapshot_export"
  }

  /**
   * Test snapshot recovery (loading). Logic: Creates a snapshot, closes the current region, and
   * uses SnapshotLoader to restore. Verification: Checks if DataRegion is re-instantiated and files
   * are restored to storage tiers.
   */
  @Test
  public void testLoadSnapshotWithMixedFiles() throws Exception {
    DataRegion originalRegion = createAndPopulateDataRegion();
    Set<Path> expectedObjectFiles = prepareObjectFiles(testRegionId);

    // Create snapshot
    new SnapshotTaker(originalRegion)
        .takeFullSnapshot(snapshotPath.toAbsolutePath().toString(), true);

    // Load snapshot
    SnapshotLoader loader =
        new SnapshotLoader(snapshotPath.toAbsolutePath().toString(), testSgName, testRegionId);
    DataRegion restoredRegion = loader.loadSnapshotForStateMachine();

    Assert.assertNotNull("Restored DataRegion should not be null", restoredRegion);
    Assert.assertEquals(
        "Restored TsFile count mismatch", 5, restoredRegion.getTsFileManager().size(true));

    // Verify Object files are back in storage tiers
    for (Path path : expectedObjectFiles) {
      Assert.assertTrue("Object file not restored to tiers: " + path, existsInStorageTiers(path));
    }

    LOGGER.info("testLoadSnapshotWithMixedFiles completed successfully.");
  }

  // --- Industrial Helper Methods ---

  /**
   * Generates TsFiles following the exact IoTDB directory hierarchy:
   * data/sequence/{database}/{regionId}/{timePartition}/{fileName}
   */
  private List<TsFileResource> writeTsFiles() throws IOException, WriteProcessException {
    List<TsFileResource> resources = new ArrayList<>();
    // Align with IoTDBSnapshotTest structure
    Path dataRegionDir =
        dataRootPath
            .resolve(IoTDBConstant.SEQUENCE_FOLDER_NAME)
            .resolve(testSgName)
            .resolve(testRegionId)
            .resolve(timePartition);

    Files.createDirectories(dataRegionDir);

    for (int i = 1; i <= 5; i++) {
      String fileName = String.format("%d-%d-0-0.tsfile", i, i);
      Path tsFilePath = dataRegionDir.resolve(fileName);

      // Use standard generator for valid TsFile content
      TsFileGeneratorUtils.generateMixTsFile(tsFilePath.toString(), 2, 2, 10, 0, 100, 10, 10);

      TsFileResource resource = new TsFileResource(tsFilePath.toFile());
      // Resource must be serialized to satisfy SnapshotTaker
      IDeviceID deviceID =
          IDeviceID.Factory.DEFAULT_FACTORY.create(testSgName + PATH_SEPARATOR + "d1");
      resource.updateStartTime(deviceID, 0);
      resource.updateEndTime(deviceID, 100);
      resource.setStatusForTest(TsFileResourceStatus.NORMAL);
      resource.serialize();

      resources.add(resource);
    }
    return resources;
  }

  private Set<Path> prepareObjectFiles(String regionId) throws IOException {
    Path objectBaseFolder = Paths.get(TierManager.getInstance().getAllObjectFileFolders().get(0));
    Set<Path> absolutePaths = new HashSet<>();

    List<Path> relativeLogicPaths =
        Arrays.asList(
            Paths.get(regionId, timePartition, "obj_a" + ObjectTypeUtils.OBJECT_FILE_SUFFIX),
            Paths.get(regionId, timePartition, "obj_b" + ObjectTypeUtils.OBJECT_TEMP_FILE_SUFFIX));

    for (Path rel : relativeLogicPaths) {
      Path abs = objectBaseFolder.resolve(rel);
      Files.createDirectories(abs.getParent());
      Files.write(abs, "mock-object-data".getBytes(StandardCharsets.UTF_8));
      absolutePaths.add(abs);
    }
    return absolutePaths;
  }

  private void validateTsFileSnapshotStructure(Path actualRoot) {
    Path tsSnapshotDir =
        actualRoot
            .resolve(IoTDBConstant.SEQUENCE_FOLDER_NAME)
            .resolve(testSgName)
            .resolve(testRegionId)
            .resolve(timePartition);

    Assert.assertTrue("TsFile snapshot directory missing", Files.exists(tsSnapshotDir));
    File[] files = tsSnapshotDir.toFile().listFiles((dir, name) -> name.endsWith(TSFILE_SUFFIX));
    Assert.assertNotNull(files);
    Assert.assertEquals("Snapshot TsFile count mismatch", 5, files.length);

    for (File f : files) {
      Assert.assertTrue(
          "Resource file missing for " + f.getName(),
          new File(f.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).exists());
    }
  }

  private void validateObjectFileSnapshotStructure(Path actualRoot, Set<Path> expectedFiles) {
    Path objectSnapshotDir = actualRoot.resolve(IoTDBConstant.OBJECT_FOLDER_NAME);
    Assert.assertTrue(
        "Object snapshot directory missing at: " + objectSnapshotDir,
        Files.exists(objectSnapshotDir));

    for (Path sourcePath : expectedFiles) {
      // In snapshot, object files retain their relative structure under 'object/' folder
      Path fileName = sourcePath.getFileName();
      Path relativeParent = Paths.get(testRegionId).resolve(timePartition);
      Path targetPath = objectSnapshotDir.resolve(relativeParent).resolve(fileName);
      Assert.assertTrue("Object file missing in snapshot: " + targetPath, Files.exists(targetPath));
    }
  }


  private boolean existsInStorageTiers(Path originalAbsPath) {
    // Check all configured tiers for the existence of the file relative to the tier root
    Path relativePath =
        Paths.get(TierManager.getInstance().getAllObjectFileFolders().get(0))
            .relativize(originalAbsPath);
    return TierManager.getInstance().getAllObjectFileFolders().stream()
        .map(folder -> Paths.get(folder).resolve(relativePath))
        .anyMatch(Files::exists);
  }

  private DataRegion createAndPopulateDataRegion() throws IOException, WriteProcessException {
    List<TsFileResource> resources = writeTsFiles();
    DataRegion region = new DataRegion(testSgName, testRegionId);
    region.getTsFileManager().addAll(resources, true);
    return region;
  }
}
