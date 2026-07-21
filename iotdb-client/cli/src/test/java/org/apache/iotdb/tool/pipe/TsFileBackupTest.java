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

package org.apache.iotdb.tool.pipe;

import org.apache.iotdb.commons.conf.IoTDBConstant;

import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TsFileBackupTest {

  private final List<Path> tempDirs = new ArrayList<>();

  @After
  public void tearDown() throws IOException {
    for (Path tempDir : tempDirs) {
      deleteRecursively(tempDir);
    }
  }

  @Test
  public void testResolvePluginJarFromCliOverride() throws IOException {
    Path tempDir = createTempDir();
    Path pluginJar = Files.createFile(tempDir.resolve("custom-plugin.jar"));

    File resolvedPluginJar = TsFileBackup.resolvePluginJar(pluginJar.toString(), null);

    assertEquals(pluginJar.toFile().getCanonicalFile(), resolvedPluginJar.getCanonicalFile());
  }

  @Test
  public void testResolvePluginJarFromIoTDBHomeExtPipe() throws IOException {
    Path tempDir = createTempDir();
    Path pluginDir = Files.createDirectories(tempDir.resolve("ext").resolve("pipe"));
    Path pluginJar =
        Files.createFile(
            pluginDir.resolve(
                "tsfile-remote-sink-" + IoTDBConstant.VERSION + "-jar-with-dependencies.jar"));

    File resolvedPluginJar = TsFileBackup.resolvePluginJar(null, tempDir.toString());

    assertEquals(pluginJar.toFile().getCanonicalFile(), resolvedPluginJar.getCanonicalFile());
  }

  @Test
  public void testResolvePluginJarAllowsMissingCliOverride() throws IOException {
    Path tempDir = createTempDir();
    Path missingPluginJar = tempDir.resolve("missing-plugin.jar");

    File resolvedPluginJar = TsFileBackup.resolvePluginJar(missingPluginJar.toString(), null);

    assertEquals(
        missingPluginJar.toFile().getCanonicalFile(), resolvedPluginJar.getCanonicalFile());
  }

  @Test
  public void testResolvePluginJarUsesDefaultPathWhenDefaultJarIsMissing() throws IOException {
    Path tempDir = createTempDir();
    Path pluginDir = Files.createDirectories(tempDir.resolve("ext").resolve("pipe"));

    File resolvedPluginJar = TsFileBackup.resolvePluginJar(null, tempDir.toString());

    assertEquals(pluginDir.toFile(), resolvedPluginJar.getParentFile());
    assertTrue(resolvedPluginJar.getName().startsWith("tsfile-remote-sink-"));
    assertTrue(resolvedPluginJar.getName().endsWith("-jar-with-dependencies.jar"));
  }

  private Path createTempDir() throws IOException {
    Path tempDir = Files.createTempDirectory("tsfile-backup-test");
    tempDirs.add(tempDir);
    return tempDir;
  }

  private void deleteRecursively(Path root) throws IOException {
    if (root == null || !Files.exists(root)) {
      return;
    }
    try (java.util.stream.Stream<Path> stream = Files.walk(root)) {
      stream.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    }
  }
}
