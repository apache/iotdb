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

package org.apache.iotdb.db.storageengine.load.util;

import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.v1.ModificationFileV1;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class LoadUtilTest {

  private File tempDir;
  private File sourceDir;
  private File targetDir;

  @Before
  public void setUp() throws Exception {
    tempDir = Files.createTempDirectory("load-util").toFile();
    sourceDir = new File(tempDir, "source");
    targetDir = new File(tempDir, "target");
    Assert.assertTrue(sourceDir.mkdirs());
    Assert.assertTrue(targetDir.mkdirs());
  }

  @After
  public void tearDown() {
    deleteRecursively(tempDir);
  }

  @Test
  public void testTransferFilesKeepsSameNamedGroupsIsolatedAndDeletesSourcesAfterHandoff()
      throws Exception {
    final List<File> sourceFiles = createTsFileAndCompanions();
    LoadUtil.transferFilesToActiveDir(targetDir, sourceFiles, true);

    for (final File sourceFile : sourceFiles) {
      Files.write(
          sourceFile.toPath(), ("second-" + sourceFile.getName()).getBytes(StandardCharsets.UTF_8));
    }
    LoadUtil.transferFilesToActiveDir(targetDir, sourceFiles, true);

    final File[] transferDirs = targetDir.listFiles(File::isDirectory);
    Assert.assertNotNull(transferDirs);
    Assert.assertEquals(2, transferDirs.length);
    for (final File sourceFile : sourceFiles) {
      Assert.assertFalse(sourceFile.exists());
    }

    final Set<String> transferredPrefixes = new HashSet<>();
    for (final File transferDir : transferDirs) {
      final File tsFile = new File(transferDir, "1-0-0-0.tsfile");
      final String tsFileContent =
          new String(Files.readAllBytes(tsFile.toPath()), StandardCharsets.UTF_8);
      final String prefix = tsFileContent.startsWith("second-") ? "second-" : "";
      transferredPrefixes.add(prefix);
      for (final File sourceFile : sourceFiles) {
        final File transferredFile = new File(transferDir, sourceFile.getName());
        Assert.assertTrue(transferredFile.exists());
        Assert.assertArrayEquals(
            (prefix + sourceFile.getName()).getBytes(StandardCharsets.UTF_8),
            Files.readAllBytes(transferredFile.toPath()));
      }
    }
    Assert.assertEquals(new HashSet<>(Arrays.asList("", "second-")), transferredPrefixes);
  }

  @Test
  public void testTransferFailureDoesNotDeleteSources() throws Exception {
    final List<File> sourceFiles = createTsFileAndCompanions();
    // A regular file cannot contain the temporary transfer directory, forcing handoff to fail
    // before ownership of any source file can be released.
    final File invalidTargetDir = new File(tempDir, "target-file");
    Assert.assertTrue(invalidTargetDir.createNewFile());

    try {
      LoadUtil.transferFilesToActiveDir(invalidTargetDir, sourceFiles, true);
      Assert.fail("Expected IOException");
    } catch (final IOException ignored) {
      // expected
    }

    for (final File sourceFile : sourceFiles) {
      Assert.assertTrue(sourceFile.exists());
    }
  }

  private List<File> createTsFileAndCompanions() throws Exception {
    final File tsFile = new File(sourceDir, "1-0-0-0.tsfile");
    final File resourceFile = new File(tsFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX);
    final File modsV1File = new File(tsFile.getAbsolutePath() + ModificationFileV1.FILE_SUFFIX);
    final File modsV2File = new File(tsFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX);
    final List<File> sourceFiles = Arrays.asList(resourceFile, modsV1File, modsV2File, tsFile);
    for (final File sourceFile : sourceFiles) {
      Files.write(sourceFile.toPath(), sourceFile.getName().getBytes(StandardCharsets.UTF_8));
    }
    return sourceFiles;
  }

  private static void deleteRecursively(final File file) {
    if (file == null || !file.exists()) {
      return;
    }
    final File[] children = file.listFiles();
    if (children != null) {
      for (final File child : children) {
        deleteRecursively(child);
      }
    }
    Assert.assertTrue(file.delete());
  }
}
