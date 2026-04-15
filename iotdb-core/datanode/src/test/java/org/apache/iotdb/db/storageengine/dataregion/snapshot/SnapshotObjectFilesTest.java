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
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.ObjectTypeUtils;

import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.utils.TsFileGeneratorUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

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

public class SnapshotObjectFilesTest {

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  private String[][] originDataDirs;
  private Path snapshotPath;
  private final String testSgName = "root.testsg";
  private final String testRegionId = "0";
  private Path objectBaseFolder;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    originDataDirs = IoTDBDescriptor.getInstance().getConfig().getTierDataDirs();

    Path dataDir = tempFolder.newFolder("data").toPath();
    String[][] testDataDirs = new String[][] {{dataDir.toAbsolutePath().toString()}};
    IoTDBDescriptor.getInstance().getConfig().setTierDataDirs(testDataDirs);
    TierManager.getInstance().resetFolders();
    snapshotPath = tempFolder.newFolder("snapshot").toPath();
  }

  @After
  public void tearDown() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setTierDataDirs(originDataDirs);
    TierManager.getInstance().resetFolders();
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testCreateSnapshotWithObjectFiles() throws Exception {
    DataRegion region = createAndPopulateDataRegion();
    Set<Path> expectedObjectFiles = prepareObjectFiles(testRegionId);
    SnapshotTaker snapshotTaker = new SnapshotTaker(region);
    boolean success =
        snapshotTaker.takeFullSnapshot(snapshotPath.toAbsolutePath().toString(), true);
    Assert.assertTrue("Snapshot generation failed", success);
    String snapshotId = snapshotPath.getFileName().toString();
    Path objectSnapshotRoot = resolveExpectedObjectSnapshotRoot(snapshotId);
    validateSnapshotPhysicalIntegrity(objectSnapshotRoot, expectedObjectFiles);
  }

  @Test
  public void testLoadSnapshotWithObjectFiles() throws Exception {
    DataRegion originalRegion = createAndPopulateDataRegion();
    Set<Path> expectedObjectFiles = prepareObjectFiles(testRegionId);
    Assert.assertTrue(
        new SnapshotTaker(originalRegion)
            .takeFullSnapshot(snapshotPath.toAbsolutePath().toString(), true));

    SnapshotLoader loader =
        new SnapshotLoader(snapshotPath.toAbsolutePath().toString(), testSgName, testRegionId);
    DataRegion loadedRegion = loader.loadSnapshotForStateMachine();

    Assert.assertNotNull("Loaded DataRegion is null", loadedRegion);
    for (Path path : expectedObjectFiles) {
      Assert.assertTrue("Object file not restored: " + path, existsInStorageTiers(path));
    }
  }

  private Path resolveExpectedObjectSnapshotRoot(String snapshotId) {
    String dataRoot = IoTDBDescriptor.getInstance().getConfig().getTierDataDirs()[0][0];
    return Paths.get(dataRoot)
        .resolve(IoTDBConstant.SNAPSHOT_FOLDER_NAME)
        .resolve(testSgName + IoTDBConstant.FILE_NAME_SEPARATOR + testRegionId)
        .resolve(snapshotId)
        .resolve(IoTDBConstant.OBJECT_FOLDER_NAME);
  }

  private void validateSnapshotPhysicalIntegrity(Path objectSnapshotRoot, Set<Path> sourceFiles)
      throws IOException {
    Assert.assertTrue(
        "Object snapshot root missing: " + objectSnapshotRoot, Files.exists(objectSnapshotRoot));
    for (Path sourceFile : sourceFiles) {
      Path relativePath = objectBaseFolder.relativize(sourceFile);
      Path fileName = relativePath.getFileName();
      Path relativeParent = relativePath.getParent();
      Path targetPath =
          relativeParent == null
              ? objectSnapshotRoot.resolve(fileName)
              : objectSnapshotRoot.resolve(relativeParent).resolve(fileName);
      Assert.assertTrue(
          "Physical file missing in snapshot: " + targetPath, Files.exists(targetPath));
      Assert.assertEquals("File size mismatch", Files.size(sourceFile), Files.size(targetPath));
    }
  }

  private DataRegion createAndPopulateDataRegion()
      throws IOException, WriteProcessException, DataRegionException {
    List<TsFileResource> resources = writeTsFiles();
    DataRegion region = new DataRegion(testSgName, testRegionId);
    region.getTsFileManager().addAll(resources, true);
    return region;
  }

  private Set<Path> prepareObjectFiles(String regionId) throws IOException {
    List<String> objectFolders = TierManager.getInstance().getAllObjectFileFolders();
    Assert.assertFalse(objectFolders.isEmpty());
    objectBaseFolder = Paths.get(objectFolders.get(0));
    List<Path> relativeLogicPaths =
        Arrays.asList(
            Paths.get(regionId, "100", "object-a" + ObjectTypeUtils.OBJECT_FILE_SUFFIX),
            Paths.get(regionId, "200", "object-b" + ObjectTypeUtils.OBJECT_TEMP_FILE_SUFFIX),
            Paths.get(regionId, "300", "object-c" + ObjectTypeUtils.OBJECT_BACK_FILE_SUFFIX));
    Set<Path> absolutePaths = new HashSet<>();
    for (Path rel : relativeLogicPaths) {
      Path abs = objectBaseFolder.resolve(rel);
      Files.createDirectories(abs.getParent());
      Files.write(abs, ("data-" + rel).getBytes(StandardCharsets.UTF_8));
      absolutePaths.add(abs);
    }
    return absolutePaths;
  }

  private boolean existsInStorageTiers(Path originalAbsPath) {
    Path relativePath = objectBaseFolder.relativize(originalAbsPath);
    for (String folder : TierManager.getInstance().getAllObjectFileFolders()) {
      Path testPath = Paths.get(folder).resolve(relativePath);
      if (Files.exists(testPath)) {
        return true;
      }
    }
    return false;
  }

  private List<TsFileResource> writeTsFiles() throws IOException, WriteProcessException {
    List<TsFileResource> resources = new ArrayList<>();
    Path dataRegionDir =
        Paths.get(IoTDBDescriptor.getInstance().getConfig().getTierDataDirs()[0][0])
            .resolve("sequence")
            .resolve(testSgName)
            .resolve(testRegionId)
            .resolve("0");
    Files.createDirectories(dataRegionDir);
    for (int i = 1; i <= 5; i++) {
      Path tsFilePath = dataRegionDir.resolve(i + "-" + i + "-0-0.tsfile");
      TsFileGeneratorUtils.generateMixTsFile(tsFilePath.toString(), 2, 2, 10, 0, 100, 10, 10);
      TsFileResource resource = new TsFileResource(tsFilePath.toFile());
      resources.add(resource);
      resource.setStatusForTest(TsFileResourceStatus.NORMAL);
      resource.serialize();
    }
    return resources;
  }
}
