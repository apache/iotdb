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
  public void testAutoCreateSchemaAttributeShouldSurviveActiveLoadPath() throws Exception {
    final File pendingDir = Files.createTempDirectory("active-load-schema").toFile();
    try {
      final Map<String, String> attributes =
          ActiveLoadPathHelper.buildAttributes(null, null, null, true, false, null, true);
      final File targetDir = ActiveLoadPathHelper.resolveTargetDir(pendingDir, attributes);
      Assert.assertTrue(targetDir.mkdirs());
      final File tsFile = new File(targetDir, "1-0-0-0.tsfile");
      Assert.assertTrue(tsFile.createNewFile());

      final Map<String, String> parsedAttributes =
          ActiveLoadPathHelper.parseAttributes(tsFile, pendingDir);
      Assert.assertEquals(
          Boolean.FALSE.toString(),
          parsedAttributes.get(LoadTsFileConfigurator.AUTO_CREATE_SCHEMA_KEY));

      final LoadTsFileStatement statement =
          LoadTsFileStatement.createUnchecked(tsFile.getAbsolutePath());
      ActiveLoadPathHelper.applyAttributesToStatement(parsedAttributes, statement, false);
      Assert.assertTrue(statement.isVerifySchema());
      Assert.assertFalse(statement.isAutoCreateSchema());
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
}
