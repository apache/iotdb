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

package org.apache.iotdb.commons.executable;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.stream.Stream;

public class ExecutableManagerTest {

  private static final String TEST_ROOT =
      "target".concat(File.separator).concat("ExecutableManagerTest");
  private static final String TEMPORARY_ROOT = TEST_ROOT.concat(File.separator).concat("tmp");
  private static final String LIB_ROOT = TEST_ROOT.concat(File.separator).concat("lib");

  private ExecutableManager executableManager;

  @Before
  public void setUp() throws Exception {
    executableManager = new ExecutableManager(TEMPORARY_ROOT, LIB_ROOT);
    Files.createDirectories(Paths.get(TEMPORARY_ROOT));
    Files.createDirectories(Paths.get(LIB_ROOT));
  }

  @After
  public void tearDown() throws Exception {
    final Path root = Paths.get(TEST_ROOT);
    if (!Files.exists(root)) {
      return;
    }
    try (final Stream<Path> paths = Files.walk(root)) {
      paths.sorted(Comparator.reverseOrder()).forEach(path -> path.toFile().delete());
    }
  }

  @Test
  public void testResolveUnderRootAcceptsNamesInsideTheRoot() throws IOException {
    final Path root = Paths.get(LIB_ROOT).toAbsolutePath().normalize();

    Assert.assertEquals(
        root.resolve("udf.jar"), ExecutableManager.resolveUnderRoot(LIB_ROOT, "udf.jar"));
    Assert.assertEquals(
        root.resolve("install").resolve("udf.jar"),
        ExecutableManager.resolveUnderRoot(LIB_ROOT, "install".concat(File.separator).concat("udf.jar")));
    // a name that walks out and back in still resolves inside the root
    Assert.assertEquals(
        root.resolve("udf.jar"),
        ExecutableManager.resolveUnderRoot(LIB_ROOT, "sub".concat(File.separator).concat("..").concat(File.separator).concat("udf.jar")));
  }

  @Test
  public void testResolveUnderRootRejectsNamesOutsideTheRoot() {
    final String[] escapingNames =
        new String[] {
          "..".concat(File.separator).concat("escaped.jar"),
          "..".concat(File.separator).concat("..").concat(File.separator).concat("escaped.jar"),
          "sub"
              .concat(File.separator)
              .concat("..")
              .concat(File.separator)
              .concat("..")
              .concat(File.separator)
              .concat("escaped.jar"),
          File.separator.concat("tmp").concat(File.separator).concat("escaped.jar"),
        };

    for (final String name : escapingNames) {
      try {
        ExecutableManager.resolveUnderRoot(LIB_ROOT, name);
        Assert.fail("expected the name to be rejected: ".concat(name));
      } catch (final IOException expected) {
        // the resolved path is outside the root
      }
    }
  }

  @Test
  public void testSaveTextUnderTemporaryRootStaysInsideTheRoot() throws IOException {
    executableManager.saveTextAsFileUnderTemporaryRoot("content", "plugin.txt");
    Assert.assertTrue(executableManager.hasFileUnderTemporaryRoot("plugin.txt"));
    Assert.assertEquals(
        "content", executableManager.readTextFromFileUnderTemporaryRoot("plugin.txt"));
  }

  @Test
  public void testSaveTextUnderTemporaryRootRejectsAnEscapingName() {
    final String escaping =
        "..".concat(File.separator).concat("..").concat(File.separator).concat("escaped.txt");
    try {
      executableManager.saveTextAsFileUnderTemporaryRoot("content", escaping);
      Assert.fail("expected the name to be rejected");
    } catch (final IOException expected) {
      // the resolved path is outside the temporary root
    }
    Assert.assertFalse(
        Files.exists(Paths.get(TEST_ROOT).toAbsolutePath().normalize().resolve("escaped.txt")));
  }

  @Test
  public void testRemoveAndReadUnderTemporaryRootRejectAnEscapingName() {
    final String escaping = "..".concat(File.separator).concat("escaped.txt");
    try {
      executableManager.removeFileUnderTemporaryRoot(escaping);
      Assert.fail("expected the name to be rejected");
    } catch (final IOException expected) {
      // expected
    }
    try {
      executableManager.readTextFromFileUnderTemporaryRoot(escaping);
      Assert.fail("expected the name to be rejected");
    } catch (final IOException expected) {
      // expected
    }
  }

  @Test
  public void testRemoveUnderLibRootRejectsAnEscapingName() throws IOException {
    final Path outside =
        Paths.get(TEST_ROOT).toAbsolutePath().normalize().resolve("outside-lib.jar");
    Files.write(outside, "keep".getBytes());

    try {
      executableManager.removeFileUnderLibRoot(
          "..".concat(File.separator).concat("outside-lib.jar"));
      Assert.fail("expected the name to be rejected");
    } catch (final IOException expected) {
      // expected
    }
    Assert.assertTrue("the file outside the lib root must not be deleted", Files.exists(outside));
  }

  @Test
  public void testHasFileAccessorsReturnFalseForAnEscapingName() throws IOException {
    final Path outside =
        Paths.get(TEST_ROOT).toAbsolutePath().normalize().resolve("outside-probe.jar");
    Files.write(outside, "probe".getBytes());

    final String escaping = "..".concat(File.separator).concat("outside-probe.jar");
    Assert.assertFalse(executableManager.hasFileUnderLibRoot(escaping));
    Assert.assertFalse(executableManager.hasFileUnderTemporaryRoot(escaping));
    Assert.assertFalse(
        executableManager.hasFileUnderInstallDir(
            "..".concat(File.separator).concat("..").concat(File.separator).concat("outside-probe.jar")));
  }
}
