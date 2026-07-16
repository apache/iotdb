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

package org.apache.iotdb.db.storageengine.load.active;

import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.storageengine.load.config.LoadTsFileConfigurator;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Map;

public class ActiveLoadPathHelperTest {

  @Test
  public void testUserAttributeShouldBeMaskedInPathAndDecodedWhenParsing() throws Exception {
    final String userName = "active_load_user";
    final File pendingDir = Files.createTempDirectory("active-load-path").toFile();
    try {
      final File targetDir =
          ActiveLoadPathHelper.resolveTargetDir(
              pendingDir,
              ActiveLoadPathHelper.buildAttributes(null, null, null, null, null, null, userName));
      final File tsFile = new File(targetDir, "1-0-0-0.tsfile");

      Assert.assertTrue(targetDir.getAbsolutePath().contains("user-v1-"));
      Assert.assertFalse(targetDir.getAbsolutePath().contains(userName));
      Assert.assertFalse(targetDir.getAbsolutePath().contains("b64%3A"));

      final Map<String, String> attributes =
          ActiveLoadPathHelper.parseAttributes(tsFile, pendingDir);
      Assert.assertEquals(userName, attributes.get(ActiveLoadPathHelper.USER_KEY));
    } finally {
      deleteRecursively(pendingDir);
    }
  }

  @Test
  public void testRawUserAttributeShouldBeIgnored() throws Exception {
    final File pendingDir = Files.createTempDirectory("active-load-path").toFile();
    try {
      final File tsFile = new File(new File(pendingDir, "user-active_load_user"), "1-0-0-0.tsfile");

      final Map<String, String> attributes =
          ActiveLoadPathHelper.parseAttributes(tsFile, pendingDir);
      Assert.assertFalse(attributes.containsKey(ActiveLoadPathHelper.USER_KEY));
    } finally {
      deleteRecursively(pendingDir);
    }
  }

  @Test
  public void testNonV1UserAttributeShouldBeIgnored() throws Exception {
    final File pendingDir = Files.createTempDirectory("active-load-path").toFile();
    try {
      final File tsFile =
          new File(new File(pendingDir, "user-v2-active_load_user"), "1-0-0-0.tsfile");

      final Map<String, String> attributes =
          ActiveLoadPathHelper.parseAttributes(tsFile, pendingDir);
      Assert.assertFalse(attributes.containsKey(ActiveLoadPathHelper.USER_KEY));
    } finally {
      deleteRecursively(pendingDir);
    }
  }

  @Test
  public void testUnknownRawAttributeDirectoryShouldBeIgnoredForDowngradeCompatibility()
      throws Exception {
    final File pendingDir = Files.createTempDirectory("active-load-path").toFile();
    try {
      final File tsFile =
          new File(new File(pendingDir, "future-load-param-future-value"), "1-0-0-0.tsfile");
      createFile(tsFile);

      final Map<String, String> attributes =
          ActiveLoadPathHelper.parseAttributes(tsFile, pendingDir);
      Assert.assertFalse(attributes.containsKey("future-load-param"));

      final LoadTsFileStatement statement =
          LoadTsFileStatement.createUnchecked(tsFile.getAbsolutePath());
      ActiveLoadPathHelper.applyAttributesToStatement(attributes, statement, true);
      Assert.assertTrue(statement.isVerifySchema());
    } finally {
      deleteRecursively(pendingDir);
    }
  }

  @Test
  public void testKnownPrefixWithInvalidFutureLikeValueShouldBeIgnoredForDowngradeCompatibility()
      throws Exception {
    final File pendingDir = Files.createTempDirectory("active-load-path").toFile();
    try {
      final File tsFile = new File(new File(pendingDir, "verify-future-value"), "1-0-0-0.tsfile");
      createFile(tsFile);

      final Map<String, String> attributes =
          ActiveLoadPathHelper.parseAttributes(tsFile, pendingDir);
      Assert.assertFalse(attributes.containsKey(LoadTsFileConfigurator.VERIFY_KEY));

      final LoadTsFileStatement statement =
          LoadTsFileStatement.createUnchecked(tsFile.getAbsolutePath());
      ActiveLoadPathHelper.applyAttributesToStatement(attributes, statement, true);
      Assert.assertTrue(statement.isVerifySchema());
    } finally {
      deleteRecursively(pendingDir);
    }
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

  private static void createFile(final File file) throws Exception {
    Assert.assertTrue(file.getParentFile().mkdirs());
    Assert.assertTrue(file.createNewFile());
  }
}
