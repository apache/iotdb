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

package org.apache.iotdb.db.conf;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

public class ActiveLoadListeningDirConfigTest {

  // Reject listening dirs under data/ and keep the original config; dirs under ext/load/ are
  // allowed.
  @Test
  public void testActiveLoadListeningDirUnderDataDirIsSkipped() throws Exception {
    final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    final String[][] originalTierDataDirs = config.getTierDataDirs();
    final String[] originalListeningDirs = config.getLoadActiveListeningDirs().clone();
    final String originalPipeListeningDir = config.getLoadActiveListeningPipeDir();
    final Path dataNodeDir = Files.createTempDirectory("active-load-listening-config");
    final Path dataDir = dataNodeDir.resolve("data");
    final Path allowedDir = dataNodeDir.resolve("ext").resolve("load").resolve("pending");
    Files.createDirectories(allowedDir);

    try {
      config.setTierDataDirs(new String[][] {{dataDir.toString()}});

      config.setLoadActiveListeningDirs(new String[] {dataDir.resolve("pending").toString()});
      Assert.assertArrayEquals(originalListeningDirs, config.getLoadActiveListeningDirs());

      config.setLoadActiveListeningPipeDir(dataDir.resolve("pipe").toString());
      Assert.assertEquals(originalPipeListeningDir, config.getLoadActiveListeningPipeDir());

      config.setLoadActiveListeningDirs(new String[] {allowedDir.toString()});
      config.setLoadActiveListeningPipeDir(
          dataNodeDir.resolve("ext").resolve("load").resolve("pipe").toString());
      Assert.assertFalse(config.isUnderInternalDataDir(allowedDir.toString()));
    } finally {
      config.setTierDataDirs(originalTierDataDirs);
      config.setLoadActiveListeningDirs(originalListeningDirs);
      config.setLoadActiveListeningPipeDir(originalPipeListeningDir);
      deleteRecursively(dataNodeDir);
    }
  }

  // Reject the data directory itself as load_active_listening_dirs and keep the original config.
  @Test
  public void testDataDirectoryItselfIsSkippedAsActiveLoadListeningDir() throws Exception {
    final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    final String[][] originalTierDataDirs = config.getTierDataDirs();
    final String[] originalListeningDirs = config.getLoadActiveListeningDirs().clone();
    final Path dataNodeDir = Files.createTempDirectory("active-load-listening-data-root");
    final Path dataDir = Files.createDirectories(dataNodeDir.resolve("data"));

    try {
      config.setTierDataDirs(new String[][] {{dataDir.toString()}});

      config.setLoadActiveListeningDirs(new String[] {dataDir.toString()});
      Assert.assertArrayEquals(originalListeningDirs, config.getLoadActiveListeningDirs());
    } finally {
      config.setTierDataDirs(originalTierDataDirs);
      config.setLoadActiveListeningDirs(originalListeningDirs);
      deleteRecursively(dataNodeDir);
    }
  }

  // Invalid data-dir updates must not overwrite previously accepted pending/pipe listening dirs.
  @Test
  public void testInvalidActiveLoadListeningConfigDoesNotOverwriteExistingValue() throws Exception {
    final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    final String[][] originalTierDataDirs = config.getTierDataDirs();
    final String[] originalListeningDirs = config.getLoadActiveListeningDirs().clone();
    final String originalPipeListeningDir = config.getLoadActiveListeningPipeDir();
    final Path dataNodeDir = Files.createTempDirectory("active-load-listening-rollback");
    final Path dataDir = dataNodeDir.resolve("data");
    final Path allowedDir = dataNodeDir.resolve("ext").resolve("load").resolve("pending");
    final Path allowedPipeDir = dataNodeDir.resolve("ext").resolve("load").resolve("pipe");
    Files.createDirectories(allowedDir);
    Files.createDirectories(allowedPipeDir);

    try {
      config.setTierDataDirs(new String[][] {{dataDir.toString()}});
      config.setLoadActiveListeningDirs(new String[] {allowedDir.toString()});
      config.setLoadActiveListeningPipeDir(allowedPipeDir.toString());

      config.setLoadActiveListeningPipeDir(dataDir.resolve("pipe").toString());
      Assert.assertEquals(allowedPipeDir.toString(), config.getLoadActiveListeningPipeDir());

      config.setLoadActiveListeningDirs(new String[] {dataDir.resolve("pending").toString()});
      Assert.assertArrayEquals(
          new String[] {allowedDir.toString()}, config.getLoadActiveListeningDirs());
    } finally {
      config.setTierDataDirs(originalTierDataDirs);
      config.setLoadActiveListeningDirs(originalListeningDirs);
      config.setLoadActiveListeningPipeDir(originalPipeListeningDir);
      deleteRecursively(dataNodeDir);
    }
  }

  private static void deleteRecursively(final Path path) throws IOException {
    if (path == null || !Files.exists(path)) {
      return;
    }

    try (final Stream<Path> pathStream = Files.walk(path)) {
      pathStream
          .sorted(Comparator.reverseOrder())
          .forEach(
              currentPath -> {
                try {
                  Files.deleteIfExists(currentPath);
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              });
    }
  }
}
