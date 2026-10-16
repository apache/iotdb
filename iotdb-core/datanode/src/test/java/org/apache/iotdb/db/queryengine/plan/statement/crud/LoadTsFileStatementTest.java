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
import java.util.stream.Stream;

public class LoadTsFileStatementTest {

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
