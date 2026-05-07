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
    final Path tempDir = Files.createTempDirectory("load-tsfile-sub-statements");

    try {
      config.setLoadTsFileSubStatementBatchSize(1);
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
      deleteRecursively(tempDir);
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
