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

package org.apache.iotdb.db.pipe.resource.tsfile;

import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class PipeTsFileDirPathMappingTest {

  private Path rootDir;

  private String dataDir1;
  private String dataDir2;

  @Before
  public void setUp() throws Exception {
    rootDir =
        Files.createTempDirectory("pipe-hardlink-dir-mapping-test")
            .toFile()
            .getCanonicalFile()
            .toPath();
    dataDir1 = new File(rootDir.toFile(), "data1").getCanonicalPath();
    dataDir2 = new File(rootDir.toFile(), "data2").getCanonicalPath();
  }

  @After
  public void tearDown() {
    FileUtils.deleteFileOrDirectory(rootDir.toFile(), true);
  }

  @Test
  public void testEveryDataDirUsesItsOwnPipeDir() throws Exception {
    // Every data dir keeps its pipe dir on its own disk.
    final PipeTsFileDirPathMapping mapping =
        new PipeTsFileDirPathMapping(new String[] {dataDir1, dataDir2}, "pipe", "tsfile");

    Assert.assertEquals(
        new File(
            new File(new File(new File(dataDir1, "pipe"), "tsfile"), "pipe1"),
            "sequence-root.db-0-0-1-1-0-0.tsfile"),
        mapping.getHardlinkOrCopiedFile(
            new File(dataDir1, "sequence/root.db/0/0/1-1-0-0.tsfile"), "pipe1"));

    Assert.assertEquals(
        new File(
            new File(new File(new File(dataDir2, "pipe"), "tsfile"), "pipe1"),
            "unsequence-root.db-0-0-f2.tsfile"),
        mapping.getHardlinkOrCopiedFile(
            new File(dataDir2, "unsequence/root.db/0/0/f2.tsfile"), "pipe1"));
  }

  @Test
  public void testPipeDirOutsideDataDirsIsCanonical() throws Exception {
    // A symlinked ancestor must not yield two paths for one physical pipe file.
    final File nonCanonicalRoot = Files.createTempDirectory("pipe-hardlink-canonical").toFile();
    try {
      final PipeTsFileDirPathMapping mapping =
          new PipeTsFileDirPathMapping(new String[0], "pipe", "tsfile");

      final File resolved =
          mapping.getHardlinkOrCopiedFile(
              new File(nonCanonicalRoot, "sequence" + File.separator + "f.tsfile"), null);

      Assert.assertEquals(resolved.getCanonicalPath(), resolved.getPath());
      Assert.assertTrue(resolved.getPath().startsWith(nonCanonicalRoot.getCanonicalPath()));
    } finally {
      FileUtils.deleteFileOrDirectory(nonCanonicalRoot, true);
    }
  }

  @Test
  public void testFileOutsideDataDirsFallsBackToLegacyLayout() throws Exception {
    final PipeTsFileDirPathMapping mapping =
        new PipeTsFileDirPathMapping(new String[] {dataDir1}, "pipe", "tsfile");

    final String otherDataDir = new File(rootDir.toFile(), "data3").getCanonicalPath();
    Assert.assertEquals(
        new File(
            new File(new File(new File(otherDataDir, "pipe"), "tsfile"), "pipe1"),
            "sequence-root.db-0-0-f3.tsfile"),
        mapping.getHardlinkOrCopiedFile(
            new File(otherDataDir, "sequence/root.db/0/0/f3.tsfile"), "pipe1"));
  }

  @Test
  public void testPublicPipeFileIsLinkedIntoTheRequestingPipe() throws Exception {
    final PipeTsFileDirPathMapping mapping =
        new PipeTsFileDirPathMapping(new String[] {dataDir1}, "pipe", "tsfile");

    final File tsFile = new File(dataDir1, "sequence/root.db/0/0/f7.tsfile");

    // The assigner pins the TsFile into the public pipe directory.
    final File publicPipeFile = mapping.getHardlinkOrCopiedFile(tsFile, null);
    Assert.assertEquals(
        new File(new File(dataDir1, "pipe/tsfile"), "sequence-root.db-0-0-f7.tsfile"),
        publicPipeFile);

    // Every pipe gets a file of its own, so that it can not delete the assigner's file.
    final File pipeFile = mapping.getHardlinkOrCopiedFile(publicPipeFile, "pipe1");
    Assert.assertEquals(
        new File(new File(dataDir1, "pipe/tsfile/pipe1"), "sequence-root.db-0-0-f7.tsfile"),
        pipeFile);
    Assert.assertNotEquals(
        publicPipeFile, mapping.getHardlinkOrCopiedFile(publicPipeFile, "pipe2"));

    // A file of the asking scope is reused, never nested.
    Assert.assertEquals(publicPipeFile, mapping.getHardlinkOrCopiedFile(publicPipeFile, null));
    Assert.assertEquals(pipeFile, mapping.getHardlinkOrCopiedFile(pipeFile, "pipe1"));
  }

  @Test
  public void testFileAlreadyInPipeDirIsReturnedAsIs() throws Exception {
    final PipeTsFileDirPathMapping mapping =
        new PipeTsFileDirPathMapping(new String[] {dataDir1}, "pipe", "tsfile");

    final File pipeTsFile =
        new File(new File(new File(dataDir1, "pipe"), "tsfile/pipe1"), "f5.tsfile");
    Assert.assertEquals(pipeTsFile, mapping.getHardlinkOrCopiedFile(pipeTsFile, "pipe1"));
  }

  @Test
  public void testMappingIsRefreshedWhenDataDirsAreHotReloaded() throws Exception {
    final PipeTsFileDirPathResolver resolver = new PipeTsFileDirPathResolver();

    final PipeTsFileDirPathMapping mappingBeforeReload = resolver.refresh(new String[] {dataDir1});
    // Unchanged data dirs must reuse the cached mapping.
    Assert.assertSame(mappingBeforeReload, resolver.refresh(new String[] {dataDir1}));

    // A disk appended by a hot reload of dn_data_dirs must be taken into account.
    final PipeTsFileDirPathMapping mappingAfterReload =
        resolver.refresh(new String[] {dataDir1, dataDir2});
    Assert.assertNotSame(mappingBeforeReload, mappingAfterReload);
    Assert.assertEquals(
        new File(
            new File(new File(new File(dataDir2, "pipe"), "tsfile"), "pipe1"),
            "sequence-root.db-0-0-f6.tsfile"),
        mappingAfterReload.getHardlinkOrCopiedFile(
            new File(dataDir2, "sequence/root.db/0/0/f6.tsfile"), "pipe1"));
  }

  @Test
  public void testFileInAnExistingPipeDirIsResolvedFromTheLayout() throws Exception {
    final PipeTsFileDirPathMapping mapping =
        new PipeTsFileDirPathMapping(new String[] {dataDir1}, "pipe", "tsfile");

    // A leftover pipe file outside the data dirs, in a pipe dir named like the pipe base dir.
    final String otherDataDir = new File(rootDir.toFile(), "data3").getCanonicalPath();
    final File pipeFile =
        new File(new File(new File(new File(otherDataDir, "pipe"), "tsfile"), "pipe"), "f8.tsfile");
    Assert.assertEquals(pipeFile, mapping.getHardlinkOrCopiedFile(pipeFile, "pipe"));

    // A file of another pipe is linked into the asking pipe, not adopted by it.
    Assert.assertEquals(
        new File(new File(new File(otherDataDir, "pipe"), "tsfile/pipe2"), "f8.tsfile"),
        mapping.getHardlinkOrCopiedFile(pipeFile, "pipe2"));
  }

  @Test
  public void testUnresolvableFileFailsInsteadOfUsingItsAbsolutePath() throws Exception {
    final PipeTsFileDirPathMapping mapping =
        new PipeTsFileDirPathMapping(new String[] {dataDir1}, "pipe", "tsfile");
    final File unresolvableFile =
        new UnresolvableFile(new File(dataDir1, "sequence/root.db/0/0/f9.tsfile").getPath());

    // The failure must not be swallowed: a pin on a non-canonical path would make one physical file
    // look like two, so the resolution fails instead.
    try {
      mapping.getHardlinkOrCopiedFile(unresolvableFile, "pipe1");
      Assert.fail("The path which can not be resolved should fail the resolution");
    } catch (final IOException e) {
      Assert.assertEquals(
          String.format(
              DataNodePipeMessages
                  .PIPE_EXCEPTION_FAILED_TO_GET_HARDLINK_OR_COPIED_FILE_IN_PIPE_DIR_FOR_FILE_F009D86E,
              unresolvableFile.getPath()),
          e.getMessage());
      Assert.assertTrue(e.getCause() instanceof IOException);
    }
  }

  private static class UnresolvableFile extends File {

    private UnresolvableFile(final String path) {
      super(path);
    }

    @Override
    public String getCanonicalPath() throws IOException {
      throw new IOException("mock path resolution failure");
    }
  }
}
