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

package org.apache.iotdb.db.queryengine.plan.statement.crud;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.junit.Assert;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

public class LoadTsFileStatementTest {

  @Test
  public void testSubStatementsKeepDatabase() throws Exception {
    final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    final int originalBatchSize = config.getLoadTsFileSubStatementBatchSize();
    final String[] originalAllowedDirs = config.getLoadTsFileAllowedDirs().clone();
    final Path tempDir = Files.createTempDirectory("load-tsfile-sub-statements");

    try {
      config.setLoadTsFileSubStatementBatchSize(1);
      config.setLoadTsFileAllowedDirs(new String[] {tempDir.toString()});
      Files.createFile(tempDir.resolve("a.tsfile"));
      Files.createFile(tempDir.resolve("b.tsfile"));

      final LoadTsFileStatement statement = new LoadTsFileStatement(tempDir.toString());
      statement.setDatabase("test_db");

      final List<LoadTsFileStatement> subStatements = statement.getSubStatements();
      Assert.assertEquals(2, subStatements.size());
      subStatements.forEach(
          subStatement -> Assert.assertEquals("test_db", subStatement.getDatabase()));
    } finally {
      config.setLoadTsFileSubStatementBatchSize(originalBatchSize);
      config.setLoadTsFileAllowedDirs(originalAllowedDirs);
      deleteRecursively(tempDir);
    }
  }

  @Test
  public void testLoadSourcePathMustBeInAllowedDirs() throws Exception {
    final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    final String[] originalAllowedDirs = config.getLoadTsFileAllowedDirs().clone();
    final boolean originalCheckEnabled = config.isLoadTsFileSourcePathCheckEnabled();
    final Path allowedDir = Files.createTempDirectory("load-tsfile-allowed");
    final Path deniedDir = Files.createTempDirectory("load-tsfile-denied");

    try {
      config.setLoadTsFileSourcePathCheckEnabled(true);
      config.setLoadTsFileAllowedDirs(new String[] {allowedDir.toString()});
      final Path deniedTsFile = Files.createFile(deniedDir.resolve("denied.tsfile"));
      final Path traversalTsFile =
          allowedDir.resolve("..").resolve(deniedDir.getFileName()).resolve("denied.tsfile");

      assertLoadSourcePathRejected(deniedTsFile);
      assertLoadSourcePathRejected(traversalTsFile);
    } finally {
      config.setLoadTsFileAllowedDirs(originalAllowedDirs);
      config.setLoadTsFileSourcePathCheckEnabled(originalCheckEnabled);
      deleteRecursively(allowedDir);
      deleteRecursively(deniedDir);
    }
  }

  @Test
  public void testLoadSourcePathCheckCanBeDisabled() throws Exception {
    final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    final String[] originalAllowedDirs = config.getLoadTsFileAllowedDirs().clone();
    final boolean originalCheckEnabled = config.isLoadTsFileSourcePathCheckEnabled();
    final Path allowedDir = Files.createTempDirectory("load-tsfile-allowed");
    final Path deniedDir = Files.createTempDirectory("load-tsfile-denied");

    try {
      config.setLoadTsFileSourcePathCheckEnabled(false);
      config.setLoadTsFileAllowedDirs(new String[] {allowedDir.toString()});
      final Path deniedTsFile = Files.createFile(deniedDir.resolve("denied.tsfile"));

      new LoadTsFileStatement(deniedTsFile.toString());
    } finally {
      config.setLoadTsFileAllowedDirs(originalAllowedDirs);
      config.setLoadTsFileSourcePathCheckEnabled(originalCheckEnabled);
      deleteRecursively(allowedDir);
      deleteRecursively(deniedDir);
    }
  }

  @Test
  public void testLoadInternalTsFileIsRejectedWithoutLeakingPath() throws Exception {
    final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    final String[][] originalTierDataDirs = config.getTierDataDirs();
    final boolean originalCheckEnabled = config.isLoadTsFileSourcePathCheckEnabled();
    final Path dataNodeDir = Files.createTempDirectory("load-tsfile-datanode");
    final Path dataDir = dataNodeDir.resolve("data");
    final Path internalTsFile =
        Files.createDirectories(dataDir.resolve("pipe-hardlink")).resolve("a.tsfile");
    Files.createFile(internalTsFile);

    try {
      config.setTierDataDirs(new String[][] {{dataDir.toString()}});
      config.setLoadTsFileSourcePathCheckEnabled(false);

      try {
        new LoadTsFileStatement(internalTsFile.toString());
        Assert.fail("Expected internal IoTDB data directory to be rejected.");
      } catch (final FileNotFoundException e) {
        Assert.assertEquals(
            "Cannot load files because the specified directory contains IoTDB data.",
            e.getMessage());
        Assert.assertFalse(e.getMessage().contains(dataDir.toString()));
        Assert.assertFalse(e.getMessage().contains(internalTsFile.toString()));
      }

      Assert.assertEquals(
          1, LoadTsFileStatement.createForPipe(internalTsFile.toString()).getTsFiles().size());
    } finally {
      config.setTierDataDirs(originalTierDataDirs);
      config.setLoadTsFileSourcePathCheckEnabled(originalCheckEnabled);
      deleteRecursively(dataNodeDir);
    }
  }

  @Test
  public void testLoadEmptyInternalDataDirIsRejected() throws Exception {
    final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    final String[][] originalTierDataDirs = config.getTierDataDirs();
    final boolean originalCheckEnabled = config.isLoadTsFileSourcePathCheckEnabled();
    final Path dataNodeDir = Files.createTempDirectory("load-tsfile-datanode");
    final Path dataDir = Files.createDirectories(dataNodeDir.resolve("data"));

    try {
      config.setTierDataDirs(new String[][] {{dataDir.toString()}});
      config.setLoadTsFileSourcePathCheckEnabled(false);

      try {
        new LoadTsFileStatement(dataDir.toString());
        Assert.fail("Expected empty internal IoTDB data directory to be rejected.");
      } catch (final FileNotFoundException e) {
        Assert.assertTrue(e.getMessage().contains("Can not find"));
      }
    } finally {
      config.setTierDataDirs(originalTierDataDirs);
      config.setLoadTsFileSourcePathCheckEnabled(originalCheckEnabled);
      deleteRecursively(dataNodeDir);
    }
  }

  @Test
  public void testLoadNonEmptyInternalDataDirIsRejected() throws Exception {
    final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    final String[][] originalTierDataDirs = config.getTierDataDirs();
    final boolean originalCheckEnabled = config.isLoadTsFileSourcePathCheckEnabled();
    final Path dataNodeDir = Files.createTempDirectory("load-tsfile-datanode");
    final Path dataDir = Files.createDirectories(dataNodeDir.resolve("data"));
    Files.createFile(dataDir.resolve("a.tsfile"));

    try {
      config.setTierDataDirs(new String[][] {{dataDir.toString()}});
      config.setLoadTsFileSourcePathCheckEnabled(false);

      try {
        new LoadTsFileStatement(dataDir.toString());
        Assert.fail("Expected non-empty internal IoTDB data directory to be rejected.");
      } catch (final FileNotFoundException e) {
        Assert.assertEquals(
            "Cannot load files because the specified directory contains IoTDB data.",
            e.getMessage());
      }
    } finally {
      config.setTierDataDirs(originalTierDataDirs);
      config.setLoadTsFileSourcePathCheckEnabled(originalCheckEnabled);
      deleteRecursively(dataNodeDir);
    }
  }

  @Test
  public void testLoadPipeReceiverTsFileOutsideDataDirIsAllowed() throws Exception {
    final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    final String[][] originalTierDataDirs = config.getTierDataDirs();
    final boolean originalCheckEnabled = config.isLoadTsFileSourcePathCheckEnabled();
    final Path dataNodeDir = Files.createTempDirectory("load-tsfile-datanode");
    final Path dataDir = dataNodeDir.resolve("data");
    final Path pipeReceiverDir =
        Files.createDirectories(dataNodeDir.resolve("system").resolve("pipe").resolve("receiver"));
    final Path pipeReceiverTsFile = Files.createFile(pipeReceiverDir.resolve("a.tsfile"));

    try {
      config.setTierDataDirs(new String[][] {{dataDir.toString()}});
      config.setLoadTsFileSourcePathCheckEnabled(false);

      final LoadTsFileStatement statement = new LoadTsFileStatement(pipeReceiverTsFile.toString());
      Assert.assertEquals(1, statement.getTsFiles().size());
      Assert.assertEquals(pipeReceiverTsFile.toFile(), statement.getTsFiles().get(0));
    } finally {
      config.setTierDataDirs(originalTierDataDirs);
      config.setLoadTsFileSourcePathCheckEnabled(originalCheckEnabled);
      deleteRecursively(dataNodeDir);
    }
  }

  private static void assertLoadSourcePathRejected(final Path sourcePath) {
    try {
      new LoadTsFileStatement(sourcePath.toString());
      Assert.fail("Expected disallowed LOAD TSFILE source path to be rejected.");
    } catch (final FileNotFoundException e) {
      Assert.assertTrue(e.getMessage().contains("outside allowed directories"));
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
