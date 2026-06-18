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

package org.apache.iotdb.db.pipe.receiver.protocol.thrift;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.storageengine.load.active.ActiveLoadPathHelper;
import org.apache.iotdb.db.storageengine.load.config.LoadTsFileConfigurator;

import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public class IoTDBDataNodeReceiverTest {

  @Test
  public void testLoadTsFileSyncStatementUsesTreeDatabaseLevelFromDatabaseName() throws Exception {
    final Path tsFile = Files.createTempFile("pipe-load-tree-database-level", ".tsfile");
    try {
      final LoadTsFileStatement statement =
          IoTDBDataNodeReceiver.buildLoadTsFileStatementForSync(
              "root.test.sg_0", tsFile.toString(), true, true);

      Assert.assertEquals("root.test.sg_0", statement.getDatabase());
      Assert.assertEquals(2, statement.getDatabaseLevel());
    } finally {
      Files.deleteIfExists(tsFile);
    }
  }

  @Test
  public void testLoadTsFileAsyncAttributesUseTreeDatabaseLevelFromDatabaseName() throws Exception {
    final Path tsFile = Files.createTempFile("pipe-async-load-tree-database-level", ".tsfile");
    try {
      final Map<String, String> attributes =
          IoTDBDataNodeReceiver.buildLoadTsFileAttributesForAsync(
              "root.test.sg_0", true, false, true);

      Assert.assertEquals(
          "root.test.sg_0", attributes.get(LoadTsFileConfigurator.DATABASE_NAME_KEY));
      Assert.assertEquals("2", attributes.get(LoadTsFileConfigurator.DATABASE_LEVEL_KEY));

      final LoadTsFileStatement statement = LoadTsFileStatement.createUnchecked(tsFile.toString());
      ActiveLoadPathHelper.applyAttributesToStatement(attributes, statement, false);
      Assert.assertEquals("root.test.sg_0", statement.getDatabase());
      Assert.assertEquals(2, statement.getDatabaseLevel());
      Assert.assertTrue(statement.isVerifySchema());
    } finally {
      Files.deleteIfExists(tsFile);
    }
  }

  @Test
  public void testLoadTsFileSyncStatementKeepsDefaultDatabaseLevelWhenDatabaseNameIsNull()
      throws Exception {
    final Path tsFile = Files.createTempFile("pipe-load-default-database-level", ".tsfile");
    try {
      final LoadTsFileStatement statement =
          IoTDBDataNodeReceiver.buildLoadTsFileStatementForSync(
              null, tsFile.toString(), true, true);

      Assert.assertNull(statement.getDatabase());
      Assert.assertEquals(
          IoTDBDescriptor.getInstance().getConfig().getDefaultDatabaseLevel(),
          statement.getDatabaseLevel());
    } finally {
      Files.deleteIfExists(tsFile);
    }
  }

  @Test
  public void testRepeatedStatementExceptionLogIsReduced() throws Exception {
    final Path tsFile = Files.createTempFile("pipe-load-log-reducer", ".tsfile");
    try {
      final LoadTsFileStatement statement =
          IoTDBDataNodeReceiver.buildLoadTsFileStatementForSync(
              "root.test.sg_0", tsFile.toString(), true, true);
      final long receiverId = System.nanoTime();
      final Exception exception = new RuntimeException("repeated receiver exception " + receiverId);

      Assert.assertTrue(
          IoTDBDataNodeReceiver.shouldLogStatementException(receiverId, statement, exception));
      Assert.assertFalse(
          IoTDBDataNodeReceiver.shouldLogStatementException(receiverId, statement, exception));
      Assert.assertTrue(
          IoTDBDataNodeReceiver.shouldLogStatementException(
              receiverId, statement, new RuntimeException("another receiver exception")));
    } finally {
      Files.deleteIfExists(tsFile);
    }
  }

  @Test
  public void testTreeSchemaSnapshotDatabaseIsFilteredByPattern() {
    Assert.assertTrue(
        IoTDBDataNodeReceiver.shouldLoadTreeSchemaSnapshotDatabase("root.ln.**", true, "root.ln"));
    Assert.assertTrue(
        IoTDBDataNodeReceiver.shouldLoadTreeSchemaSnapshotDatabase("root.**", true, "root.db"));
    Assert.assertFalse(
        IoTDBDataNodeReceiver.shouldLoadTreeSchemaSnapshotDatabase("root.ln.**", true, "root.db"));
    Assert.assertFalse(
        IoTDBDataNodeReceiver.shouldLoadTreeSchemaSnapshotDatabase("root.ln.**", false, "root.ln"));
  }

  @Test
  public void testLoadTsFileSyncStatementVerifiesSchemaWhenConvertingType() throws Exception {
    final Path tsFile = Files.createTempFile("pipe-load-convert-verify-schema", ".tsfile");
    try {
      final LoadTsFileStatement statement =
          IoTDBDataNodeReceiver.buildLoadTsFileStatementForSync(
              "root.test.sg_0", tsFile.toString(), false, true);

      Assert.assertTrue(statement.isConvertOnTypeMismatch());
      Assert.assertTrue(statement.isVerifySchema());
    } finally {
      Files.deleteIfExists(tsFile);
    }
  }

  @Test
  public void testLoadTsFileSyncStatementCanSkipVerifySchemaWhenNotConvertingType()
      throws Exception {
    final Path tsFile = Files.createTempFile("pipe-load-no-convert-no-verify-schema", ".tsfile");
    try {
      final LoadTsFileStatement statement =
          IoTDBDataNodeReceiver.buildLoadTsFileStatementForSync(
              "root.test.sg_0", tsFile.toString(), false, false);

      Assert.assertFalse(statement.isConvertOnTypeMismatch());
      Assert.assertFalse(statement.isVerifySchema());
    } finally {
      Files.deleteIfExists(tsFile);
    }
  }
}
