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

package org.apache.iotdb.commons.pipe.agent.plugin.service;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.stream.Stream;

public class PipePluginExecutableManagerTest {

  @Test
  public void testPluginPathsAreConfinedToInstallDirectory() throws Exception {
    final Path root = Files.createTempDirectory("pipe-plugin-executable-manager-test");
    final Path temporaryLibRoot = root.resolve("temporary");
    final Path libRoot = root.resolve("lib");
    final PipePluginExecutableManager manager =
        new PipePluginExecutableManager(temporaryLibRoot.toString(), libRoot.toString());
    final String traversalFileName = ".." + File.separator + ".." + File.separator + "outside.jar";
    final Path outsideFile = libRoot.resolve("outside.jar");
    final byte[] pluginContent = "plugin".getBytes(StandardCharsets.UTF_8);

    try {
      Assert.assertEquals(
          libRoot
              .toAbsolutePath()
              .normalize()
              .resolve("install")
              .resolve("TEST-PLUGIN")
              .resolve("test.jar"),
          Paths.get(manager.getPluginInstallPathV2("test-plugin", "test.jar")));
      manager.savePluginToInstallDir(ByteBuffer.wrap(pluginContent), "test-plugin", "test.jar");
      Assert.assertArrayEquals(
          pluginContent,
          Files.readAllBytes(Paths.get(manager.getPluginInstallPathV2("test-plugin", "test.jar"))));

      Assert.assertThrows(
          IllegalArgumentException.class,
          () -> manager.getPluginsDirPath(".." + File.separator + "outside"));
      Assert.assertThrows(
          IllegalArgumentException.class,
          () -> manager.getPluginsDirPath(".." + otherFileSeparator() + "outside"));
      Assert.assertThrows(
          IllegalArgumentException.class, () -> manager.getPluginInstallPathV1(traversalFileName));
      Assert.assertThrows(
          IllegalArgumentException.class,
          () ->
              manager.savePluginToInstallDir(
                  ByteBuffer.wrap(new byte[] {1}), "plugin", traversalFileName));
      Assert.assertThrows(
          IllegalArgumentException.class,
          () ->
              manager.savePluginToInstallDir(
                  ByteBuffer.wrap(new byte[] {1}),
                  ".." + otherFileSeparator() + "plugin",
                  "test.jar"));
      Assert.assertFalse(Files.exists(outsideFile));

      Files.createDirectories(outsideFile.getParent());
      Files.write(outsideFile, "preserve".getBytes(StandardCharsets.UTF_8));
      Assert.assertThrows(
          IllegalArgumentException.class,
          () -> manager.removePluginFileUnderLibRoot("plugin", traversalFileName));
      Assert.assertArrayEquals(
          "preserve".getBytes(StandardCharsets.UTF_8), Files.readAllBytes(outsideFile));

      Assert.assertThrows(
          IllegalArgumentException.class,
          () -> manager.linkExistedPlugin("source", "target", traversalFileName));
    } finally {
      deleteRecursively(root);
    }
  }

  private static String otherFileSeparator() {
    return File.separatorChar == '/' ? "\\" : "/";
  }

  private static void deleteRecursively(final Path path) throws IOException {
    try (final Stream<Path> stream = Files.walk(path)) {
      for (final Path subPath :
          (Iterable<Path>) stream.sorted(Comparator.reverseOrder())::iterator) {
        Files.deleteIfExists(subPath);
      }
    }
  }
}
