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

package org.timecho.iotdb.db.queryengine.plan.statement.crud;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LoadTsFile;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.storageengine.load.active.ActiveLoadPathHelper;
import org.apache.iotdb.db.storageengine.load.config.LoadTsFileConfigurator;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class TimechoLoadTsFileStatementTest {

  @Test
  public void testPhysicalPathAttributeKeepsAcrossStatements() throws Exception {
    final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    final int originalBatchSize = config.getLoadTsFileSubStatementBatchSize();
    final String[] originalAllowedDirs = config.getLoadTsFileAllowedDirs().clone();
    final Path tempDir = Files.createTempDirectory("load-tsfile-physical-path");

    try {
      config.setLoadTsFileSubStatementBatchSize(1);
      config.setLoadTsFileAllowedDirs(new String[] {tempDir.toString()});
      Files.createFile(tempDir.resolve("a.tsfile"));
      Files.createFile(tempDir.resolve("b.tsfile"));

      final LoadTsFileStatement statement = new LoadTsFileStatement(tempDir.toString());
      final Map<String, String> loadAttributes = new HashMap<>();
      loadAttributes.put(
          LoadTsFileConfigurator.TSFILE_IS_PHYSICAL_PATH_KEY, Boolean.TRUE.toString());
      statement.setLoadAttributes(loadAttributes);

      Assert.assertTrue(statement.isTsFilePhysicalPath());

      final List<LoadTsFileStatement> subStatements = statement.getSubStatements();
      Assert.assertEquals(2, subStatements.size());
      subStatements.forEach(subStatement -> Assert.assertTrue(subStatement.isTsFilePhysicalPath()));

      final LoadTsFile relationalStatement = (LoadTsFile) statement.toRelationalStatement(null);
      Assert.assertTrue(relationalStatement.isTsFilePhysicalPath());

      final List<LoadTsFile> relationalSubStatements = relationalStatement.getSubStatements();
      Assert.assertEquals(2, relationalSubStatements.size());
      relationalSubStatements.forEach(
          subStatement -> Assert.assertTrue(subStatement.isTsFilePhysicalPath()));
    } finally {
      config.setLoadTsFileSubStatementBatchSize(originalBatchSize);
      config.setLoadTsFileAllowedDirs(originalAllowedDirs);
      deleteRecursively(tempDir);
    }
  }

  @Test
  public void testActiveLoadAttributesKeepPhysicalPathFlag() throws Exception {
    final Path tsFile = Files.createTempFile("load-tsfile-active-physical-path", ".tsfile");

    try {
      final Map<String, String> attributes =
          ActiveLoadPathHelper.buildAttributes(null, 1, true, true, 1024L, false, true);

      final LoadTsFileStatement statement = LoadTsFileStatement.createUnchecked(tsFile.toString());
      ActiveLoadPathHelper.applyAttributesToStatement(attributes, statement, true);

      Assert.assertTrue(statement.isTsFilePhysicalPath());
    } finally {
      deleteRecursively(tsFile);
    }
  }

  private static void deleteRecursively(final Path path) throws IOException {
    if (path == null || !Files.exists(path)) {
      return;
    }

    try (Stream<Path> paths = Files.walk(path)) {
      paths
          .sorted(Comparator.reverseOrder())
          .forEach(
              currentPath -> {
                try {
                  Files.deleteIfExists(currentPath);
                } catch (final IOException e) {
                  throw new RuntimeException(e);
                }
              });
    } catch (final RuntimeException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      }
      throw e;
    }
  }
}
