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
package org.apache.iotdb.commons.disk;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.disk.strategy.DirectoryStrategyType;
import org.apache.iotdb.commons.disk.strategy.MinFolderOccupiedSpaceFirstStrategy;
import org.apache.iotdb.commons.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.commons.utils.JVMCommonUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

/**
 * Integration test that drives {@link MinFolderOccupiedSpaceFirstStrategy} and {@link
 * FolderManager} against the real filesystem (no mocking). It exercises the actual {@code
 * Files.walk}-based occupied-space computation and verifies both the selection semantics and the
 * caching behaviour that fixes the snapshot-receive hotspot (a full directory scan on every single
 * selection).
 */
public class MinFolderOccupiedSpaceFirstStrategyRealFsTest {

  private final List<Path> tempDirs = new ArrayList<>();
  private List<String> folders;
  private double originalThreshold;

  @Before
  public void setUp() throws IOException {
    // The temp dirs live on the test machine's disk; make hasSpace() deterministic regardless of
    // how full that disk happens to be.
    originalThreshold = CommonDescriptor.getInstance().getConfig().getDiskSpaceWarningThreshold();
    JVMCommonUtils.setDiskSpaceWarningThreshold(0.0);

    folders = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      Path dir = Files.createTempDirectory("min-occupied-strategy-" + i + "-");
      tempDirs.add(dir);
      folders.add(dir.toFile().getAbsolutePath());
    }
  }

  @After
  public void tearDown() throws IOException {
    JVMCommonUtils.setDiskSpaceWarningThreshold(originalThreshold);
    for (Path dir : tempDirs) {
      if (!Files.exists(dir)) {
        continue;
      }
      try (Stream<Path> walk = Files.walk(dir)) {
        walk.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
      }
    }
  }

  private void writeFile(int folderIndex, String name, int sizeBytes) throws IOException {
    File file = new File(folders.get(folderIndex), name);
    byte[] payload = new byte[sizeBytes];
    Arrays.fill(payload, (byte) 1);
    Files.write(file.toPath(), payload);
  }

  @Test
  public void selectsLeastOccupiedRealDirectory()
      throws DiskSpaceInsufficientException, IOException {
    // Folder 2 holds a real 1 MiB file, folders 0 and 1 are empty.
    writeFile(2, "occupied.bin", 1024 * 1024);

    FolderManager folderManager =
        new FolderManager(folders, DirectoryStrategyType.MIN_FOLDER_OCCUPIED_SPACE_FIRST_STRATEGY);

    // The least occupied folder (folder 0, ties broken by index) must be chosen.
    assertEquals(folders.get(0), folderManager.getNextFolder());
  }

  @Test
  public void cachesOccupiedSpaceAndRefreshesAgainstRealFiles()
      throws DiskSpaceInsufficientException, IOException {
    MinFolderOccupiedSpaceFirstStrategy strategy = new MinFolderOccupiedSpaceFirstStrategy();
    // Disable the time-based refresh so the count-based refresh is the only trigger under test.
    strategy.setRefreshIntervalMs(Long.MAX_VALUE);
    strategy.setRefreshSelectionThreshold(2);
    strategy.setFolders(folders);

    // All folders are empty on disk, so folder 0 is selected and the cache is built.
    assertEquals(0, strategy.nextFolderIndex());

    // Make folder 0 the most occupied folder on disk by writing a real 2 MiB file into it.
    writeFile(0, "big.bin", 2 * 1024 * 1024);

    // The cache is still valid (threshold not reached), so the selection must not change though
    // folder 0 is now the largest: this proves the strategy did not re-walk the directory tree.
    assertEquals(0, strategy.nextFolderIndex());

    // The threshold is now reached: the next selection refreshes the cache via a real walk
    // and correctly avoids the now-largest folder 0, picking the least occupied folder 1.
    assertEquals(1, strategy.nextFolderIndex());
  }
}
