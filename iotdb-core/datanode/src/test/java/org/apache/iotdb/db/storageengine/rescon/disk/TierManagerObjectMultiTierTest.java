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

package org.apache.iotdb.db.storageengine.rescon.disk;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TierManagerObjectMultiTierTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private String[][] originalTierDataDirs;
  private File hotDir;
  private File coldDir;

  @Before
  public void setUp() throws Exception {
    originalTierDataDirs = config.getTierDataDirs();
    hotDir = temporaryFolder.newFolder("hot");
    coldDir = temporaryFolder.newFolder("cold");
    config.setTierDataDirs(
        new String[][] {{hotDir.getAbsolutePath()}, {coldDir.getAbsolutePath()}});
    TierManager.getInstance().resetFolders();
  }

  @After
  public void tearDown() {
    config.setTierDataDirs(originalTierDataDirs);
    TierManager.getInstance().resetFolders();
  }

  @Test
  public void testAllObjectFoldersCoverEveryTier() {
    List<String> objectFolders = TierManager.getInstance().getAllObjectFileFolders();
    assertEquals(2, objectFolders.size());
    assertTrue(objectFolders.get(0).startsWith(hotDir.getAbsolutePath()));
    assertTrue(objectFolders.get(1).startsWith(coldDir.getAbsolutePath()));
    assertTrue(objectFolders.get(0).endsWith(IoTDBConstant.OBJECT_FOLDER_NAME));
    assertTrue(objectFolders.get(1).endsWith(IoTDBConstant.OBJECT_FOLDER_NAME));
  }

  /** Same relative path on both tiers: lookup returns the hot copy. */
  @Test
  public void testCrossTierLookupPrefersHotCopy() throws Exception {
    String relativePath = "1" + File.separator + "t" + File.separator + "1.bin";
    File hotObject =
        new File(hotDir, IoTDBConstant.OBJECT_FOLDER_NAME + File.separator + relativePath);
    File coldObject =
        new File(coldDir, IoTDBConstant.OBJECT_FOLDER_NAME + File.separator + relativePath);
    Files.createDirectories(hotObject.getParentFile().toPath());
    Files.createDirectories(coldObject.getParentFile().toPath());
    Files.write(hotObject.toPath(), "hot".getBytes(StandardCharsets.UTF_8));
    Files.write(coldObject.toPath(), "cold".getBytes(StandardCharsets.UTF_8));

    Optional<File> resolved = TierManager.getInstance().getAbsoluteObjectFilePath(relativePath);
    assertTrue(resolved.isPresent());
    assertEquals(hotObject.getCanonicalFile(), resolved.get().getCanonicalFile());
  }

  /** After the hot copy is gone, lookup still finds the cold-tier file. */
  @Test
  public void testLookupFindsColdOnlyCopy() throws Exception {
    String relativePath = "1" + File.separator + "t" + File.separator + "2.bin";
    File coldObject =
        new File(coldDir, IoTDBConstant.OBJECT_FOLDER_NAME + File.separator + relativePath);
    Files.createDirectories(coldObject.getParentFile().toPath());
    Files.write(coldObject.toPath(), "cold-only".getBytes(StandardCharsets.UTF_8));

    Optional<File> resolved = TierManager.getInstance().getAbsoluteObjectFilePath(relativePath);
    assertTrue(resolved.isPresent());
    assertEquals(coldObject.getCanonicalFile(), resolved.get().getCanonicalFile());
  }

  /** New OBJECT writes still allocate under tier0 object. */
  @Test
  public void testNewObjectFolderAllocatedOnHotTier() throws Exception {
    String folder = TierManager.getInstance().getNextFolderForObjectFile();
    assertTrue(folder.startsWith(hotDir.getAbsolutePath()));
    assertTrue(folder.contains(IoTDBConstant.OBJECT_FOLDER_NAME));
  }

  /** Per-tier folder list and getObjectFileTierLevel match the file's data dir. */
  @Test
  public void testObjectFoldersForTierAndTierLevel() throws Exception {
    List<String> hotFolders = TierManager.getInstance().getObjectFoldersForTier(0);
    List<String> coldFolders = TierManager.getInstance().getObjectFoldersForTier(1);
    assertEquals(1, hotFolders.size());
    assertEquals(1, coldFolders.size());
    assertTrue(hotFolders.get(0).startsWith(hotDir.getAbsolutePath()));
    assertTrue(coldFolders.get(0).startsWith(coldDir.getAbsolutePath()));

    String relativePath = "1" + File.separator + "t" + File.separator + "3.bin";
    File coldObject =
        new File(coldDir, IoTDBConstant.OBJECT_FOLDER_NAME + File.separator + relativePath);
    Files.createDirectories(coldObject.getParentFile().toPath());
    Files.write(coldObject.toPath(), "x".getBytes(StandardCharsets.UTF_8));
    assertEquals(1, TierManager.getInstance().getObjectFileTierLevel(coldObject));
  }
}
