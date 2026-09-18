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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.receiver.runtime.PipeReceiverRuntimeRegistry;
import org.apache.iotdb.commons.pipe.receiver.runtime.PipeReceiverRuntimeSnapshot;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTsFileSealWithModReq;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.storageengine.load.active.ActiveLoadPathHelper;
import org.apache.iotdb.db.storageengine.load.config.LoadTsFileConfigurator;
import org.apache.iotdb.db.storageengine.load.converter.PipeTsFileConversionTaskManager;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class IoTDBDataNodeReceiverTest {

  private final PipeReceiverRuntimeRegistry registry = PipeReceiverRuntimeRegistry.getInstance();

  @Before
  public void setUp() {
    registry.clear();
  }

  @After
  public void tearDown() {
    registry.clear();
  }

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
      Assert.assertTrue(statement.isAutoCreateSchema());
    } finally {
      Files.deleteIfExists(tsFile);
    }
  }

  @Test
  public void testLoadTsFileWaitsForSchemaInSyncAndAsyncModes() throws Exception {
    final Path tsFile = Files.createTempFile("pipe-load-wait-for-schema", ".tsfile");
    try {
      final LoadTsFileStatement syncStatement =
          IoTDBDataNodeReceiver.buildLoadTsFileStatementForSync(
              "root.test.sg_0", tsFile.toString(), false, false, true);
      Assert.assertTrue(syncStatement.isVerifySchema());
      Assert.assertFalse(syncStatement.isAutoCreateSchema());

      final Map<String, String> asyncAttributes =
          IoTDBDataNodeReceiver.buildLoadTsFileAttributesForAsync(
              "root.test.sg_0", false, false, true, true);
      Assert.assertEquals(
          Boolean.TRUE.toString(), asyncAttributes.get(LoadTsFileConfigurator.VERIFY_KEY));
      Assert.assertEquals(
          Boolean.FALSE.toString(),
          asyncAttributes.get(LoadTsFileConfigurator.AUTO_CREATE_SCHEMA_KEY));

      final LoadTsFileStatement asyncStatement =
          LoadTsFileStatement.createUnchecked(tsFile.toString());
      ActiveLoadPathHelper.applyAttributesToStatement(asyncAttributes, asyncStatement, false);
      Assert.assertTrue(asyncStatement.isVerifySchema());
      Assert.assertFalse(asyncStatement.isAutoCreateSchema());
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
  public void testStatementExceptionLogIsReducedByFailureLocation() {
    final InsertRowStatement firstStatement = new InsertRowStatement();
    firstStatement.setTime(1);
    firstStatement.setValues(new Object[] {"first statement value"});
    final InsertRowStatement secondStatement = new InsertRowStatement();
    secondStatement.setTime(2);
    secondStatement.setValues(new Object[] {"second statement value"});

    final String testId = Long.toString(System.nanoTime());
    final String sameFailureLocation = "sameFailureLocation" + testId;
    Assert.assertTrue(
        IoTDBDataNodeReceiver.shouldLogStatementException(
            firstStatement, newStatementException("first message", sameFailureLocation)));
    Assert.assertFalse(
        IoTDBDataNodeReceiver.shouldLogStatementException(
            secondStatement, newStatementException("second message", sameFailureLocation)));
    Assert.assertTrue(
        IoTDBDataNodeReceiver.shouldLogStatementException(
            secondStatement,
            newStatementException("second message", "differentFailureLocation" + testId)));
    Assert.assertTrue(
        IoTDBDataNodeReceiver.shouldLogStatementException(
            new InsertRowsStatement(),
            newStatementException("second message", sameFailureLocation)));
  }

  @Test
  public void testInsertRowsPipeLoggingStringIsCompact() {
    final InsertRowStatement firstStatement = new InsertRowStatement();
    firstStatement.setTime(1);
    firstStatement.setValues(new Object[] {"first secret value"});
    final InsertRowStatement middleStatement = new InsertRowStatement();
    middleStatement.setTime(2);
    middleStatement.setValues(new Object[] {"middle secret value"});
    final InsertRowStatement lastStatement = new InsertRowStatement();
    lastStatement.setTime(3);
    lastStatement.setValues(new Object[] {"last secret value"});

    final InsertRowsStatement statement = new InsertRowsStatement();
    statement.setInsertRowStatementList(
        Arrays.asList(firstStatement, middleStatement, lastStatement));

    final String pipeLoggingString = statement.getPipeLoggingString();
    Assert.assertTrue(pipeLoggingString.contains("rowCount=3"));
    Assert.assertTrue(pipeLoggingString.contains("firstRow="));
    Assert.assertTrue(pipeLoggingString.contains("time=1"));
    Assert.assertTrue(pipeLoggingString.contains("lastRow="));
    Assert.assertTrue(pipeLoggingString.contains("time=3"));
    Assert.assertFalse(pipeLoggingString.contains("time=2"));
    Assert.assertFalse(pipeLoggingString.contains("first secret value"));
    Assert.assertFalse(pipeLoggingString.contains("middle secret value"));
    Assert.assertFalse(pipeLoggingString.contains("last secret value"));
  }

  private static Exception newStatementException(
      final String message, final String failureLocation) {
    final NullPointerException rootCause = new NullPointerException(message);
    rootCause.setStackTrace(
        new StackTraceElement[] {
          new StackTraceElement(
              IoTDBDataNodeReceiverTest.class.getName(),
              failureLocation,
              "IoTDBDataNodeReceiverTest.java",
              1)
        });
    return new RuntimeException("wrapper " + message, rootCause);
  }

  @Test
  public void testTreeSchemaSnapshotDatabaseIsFilteredByPattern() {
    Assert.assertTrue(
        IoTDBDataNodeReceiver.shouldLoadTreeSchemaSnapshotDatabase("root.ln.**", true, "root.ln"));
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

  @Test
  public void testDataNodeReceiverRuntimeIsClearedOnHandleExitAndCanReconnect() {
    final TestingDataNodeReceiver receiver = new TestingDataNodeReceiver();

    receiver.recordDataNodeReceiverRuntime(3, "10.0.0.1", "9001", "root", "cluster-a", "pipe-a", 1);

    List<PipeReceiverRuntimeSnapshot> snapshots = registry.snapshot();
    Assert.assertEquals(1, snapshots.size());
    Assert.assertTrue(snapshots.get(0).getPipeIds().contains("pipe-a@"));

    receiver.handleExit();
    Assert.assertTrue(registry.snapshot().isEmpty());

    receiver.recordDataNodeReceiverRuntime(3, "10.0.0.1", "9002", "root", "cluster-a", "pipe-b", 2);

    snapshots = registry.snapshot();
    Assert.assertEquals(1, snapshots.size());
    Assert.assertEquals("9002", snapshots.get(0).getSenderPorts());
    Assert.assertTrue(snapshots.get(0).getPipeIds().contains("pipe-b@"));
  }

  @Test
  public void testRepeatedHandshakeKeepsPipesOnTheSameReceiverRuntimeSession() {
    final TestingDataNodeReceiver receiver = new TestingDataNodeReceiver();

    receiver.setReceiverId(1);
    receiver.recordDataNodeReceiverRuntime(3, "10.0.0.1", "9001", "root", "cluster-a", "pipe-a", 1);
    receiver.setReceiverId(2);
    receiver.recordDataNodeReceiverRuntime(3, "10.0.0.1", "9001", "root", "cluster-a", "pipe-b", 2);

    final List<PipeReceiverRuntimeSnapshot> snapshots = registry.snapshot();
    Assert.assertEquals(1, snapshots.size());
    Assert.assertEquals(1, snapshots.get(0).getConnectionCount());
    Assert.assertEquals(2, snapshots.get(0).getPipeCount());
    Assert.assertTrue(snapshots.get(0).getPipeIds().contains("pipe-a@"));
    Assert.assertTrue(snapshots.get(0).getPipeIds().contains("pipe-b@"));
  }

  @Test
  public void testHandshakeOnChangedConnectionReplacesReceiverRuntimeSession() {
    final TestingDataNodeReceiver receiver = new TestingDataNodeReceiver();

    receiver.setReceiverId(1);
    receiver.recordDataNodeReceiverRuntime(3, "10.0.0.1", "9001", "root", "cluster-a", "pipe-a", 1);
    receiver.setReceiverId(2);
    receiver.recordDataNodeReceiverRuntime(3, "10.0.0.1", "9002", "root", "cluster-a", "pipe-b", 2);

    final List<PipeReceiverRuntimeSnapshot> snapshots = registry.snapshot();
    Assert.assertEquals(1, snapshots.size());
    Assert.assertEquals("9002", snapshots.get(0).getSenderPorts());
    Assert.assertEquals(1, snapshots.get(0).getConnectionCount());
    Assert.assertEquals(1, snapshots.get(0).getPipeCount());
    Assert.assertFalse(snapshots.get(0).getPipeIds().contains("pipe-a@"));
    Assert.assertTrue(snapshots.get(0).getPipeIds().contains("pipe-b@"));
  }

  @Test
  public void testConfigNodeReceiverRuntimeIsClearedOnHandleExit() throws Exception {
    final TestingDataNodeReceiver receiver = new TestingDataNodeReceiver();
    final String configNodeSessionKey = "ConfigNode-7-thrift-test-config-receiver";

    registry.registerOrUpdateSession(
        configNodeSessionKey,
        PipeReceiverRuntimeRegistry.NODE_TYPE_CONFIG_NODE,
        7,
        PipeReceiverRuntimeRegistry.PROTOCOL_THRIFT,
        "10.0.0.2",
        9003,
        "root",
        "cluster-a",
        "pipe-config",
        11,
        100);
    setConfigPipeReceiverRuntimeSessionKey(receiver, configNodeSessionKey);

    Assert.assertEquals(1, registry.snapshot().size());

    receiver.handleExit();

    Assert.assertTrue(registry.snapshot().isEmpty());
  }

  @Test
  public void testClearTreeDatabaseNameForLoadTsFileStatement() throws Exception {
    final Path tsFile = Files.createTempFile("pipe-load-clear-tree-database", ".tsfile");
    try {
      final LoadTsFileStatement statement =
          IoTDBDataNodeReceiver.buildLoadTsFileStatementForSync(
              "root.test.sg_0", tsFile.toString(), true, true);

      IoTDBDataNodeReceiver.clearTreeDatabaseName(statement);

      Assert.assertNull(statement.getDatabase());
      Assert.assertEquals(
          IoTDBDescriptor.getInstance().getConfig().getDefaultDatabaseLevel(),
          statement.getDatabaseLevel());
    } finally {
      Files.deleteIfExists(tsFile);
    }
  }

  @Test
  public void testClearTreeDatabaseNameForBatchInsertStatements() {
    final InsertRowStatement rowStatement1 = new InsertRowStatement();
    rowStatement1.setDatabaseName("root.test.sg_0");
    final InsertRowStatement rowStatement2 = new InsertRowStatement();
    rowStatement2.setDatabaseName("root.test.sg_0");
    final InsertRowsStatement insertRowsStatement = new InsertRowsStatement();
    insertRowsStatement.setDatabaseName("root.test.sg_0");
    insertRowsStatement.setInsertRowStatementList(Arrays.asList(rowStatement1, rowStatement2));

    IoTDBDataNodeReceiver.clearTreeDatabaseName(insertRowsStatement);

    Assert.assertFalse(insertRowsStatement.getDatabaseName().isPresent());
    Assert.assertFalse(rowStatement1.getDatabaseName().isPresent());
    Assert.assertFalse(rowStatement2.getDatabaseName().isPresent());

    final InsertTabletStatement tabletStatement = new InsertTabletStatement();
    tabletStatement.setDatabaseName("root.test.sg_0");
    final InsertMultiTabletsStatement insertMultiTabletsStatement =
        new InsertMultiTabletsStatement();
    insertMultiTabletsStatement.setDatabaseName("root.test.sg_0");
    insertMultiTabletsStatement.setInsertTabletStatementList(
        Collections.singletonList(tabletStatement));

    IoTDBDataNodeReceiver.clearTreeDatabaseName(insertMultiTabletsStatement);

    Assert.assertFalse(insertMultiTabletsStatement.getDatabaseName().isPresent());
    Assert.assertFalse(tabletStatement.getDatabaseName().isPresent());
  }

  @Test
  public void testAsyncTakeoverDecisionPausesMemoryPressureLocally() {
    final TSStatus conversionFailure = new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode());
    Assert.assertTrue(
        IoTDBDataNodeReceiver.shouldTakeOverToAsyncLoad(
            conversionFailure, false, true, true, true));
    Assert.assertFalse(
        IoTDBDataNodeReceiver.shouldTakeOverToAsyncLoad(
            conversionFailure, false, true, false, true));
    Assert.assertFalse(
        IoTDBDataNodeReceiver.shouldTakeOverToAsyncLoad(
            conversionFailure, false, true, true, false));
    Assert.assertFalse(
        IoTDBDataNodeReceiver.shouldTakeOverToAsyncLoad(conversionFailure, true, true, true, true));

    Assert.assertFalse(
        IoTDBDataNodeReceiver.shouldTakeOverToAsyncLoad(
            new TSStatus(TSStatusCode.LOAD_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode()),
            false,
            true,
            true,
            true));
    Assert.assertFalse(
        IoTDBDataNodeReceiver.shouldTakeOverToAsyncLoad(
            new TSStatus(
                TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode()),
            false,
            true,
            true,
            true));
  }

  @Test
  public void testRetryableTaskKeepsReceiverStagingFilesForDuplicateSeal() throws Exception {
    final String taskId = "receiver-cleanup-" + System.nanoTime();
    final PipeTransferTsFileSealWithModReq request =
        PipeTransferTsFileSealWithModReq.toTPipeTransferReq("1.tsfile", 1, "root.db")
            .setConversionTaskInfo(taskId, false);
    final ExposedReceiver receiver = new ExposedReceiver();
    try {
      Assert.assertNull(
          PipeTsFileConversionTaskManager.registerAndGetDuplicateStatus(taskId, false));
      PipeTsFileConversionTaskManager.markRunning(taskId);
      Assert.assertFalse(receiver.shouldDelete(request, new TSStatus(1)));
      PipeTsFileConversionTaskManager.markPaused(taskId, new TSStatus(2));
      Assert.assertFalse(receiver.shouldDelete(request, new TSStatus(2)));
      PipeTsFileConversionTaskManager.markSuccess(taskId);
      Assert.assertTrue(receiver.shouldDelete(request, new TSStatus(0)));
    } finally {
      PipeTsFileConversionTaskManager.markSuccess(taskId);
    }
  }

  private static final class ExposedReceiver extends IoTDBDataNodeReceiver {
    private boolean shouldDelete(
        final PipeTransferTsFileSealWithModReq req, final TSStatus status) {
      return shouldDeleteSealedFilesOnFailure(req, status);
    }
  }

  @SuppressWarnings("unchecked")
  private static void setConfigPipeReceiverRuntimeSessionKey(
      final IoTDBDataNodeReceiver receiver, final String sessionKey) throws Exception {
    final Field field =
        IoTDBDataNodeReceiver.class.getDeclaredField("configPipeReceiverRuntimeSessionKey");
    field.setAccessible(true);
    ((AtomicReference<String>) field.get(receiver)).set(sessionKey);
  }

  private static class TestingDataNodeReceiver extends IoTDBDataNodeReceiver {

    private String senderHost;
    private String senderPort;

    private void setReceiverId(final long receiverId) {
      this.receiverId.set(receiverId);
    }

    private void recordDataNodeReceiverRuntime(
        final int receiverNodeId,
        final String senderHost,
        final String senderPort,
        final String userName,
        final String senderClusterId,
        final String pipeName,
        final long pipeCreationTime) {
      this.senderHost = senderHost;
      this.senderPort = senderPort;
      this.username = userName;
      this.senderClusterId = senderClusterId;
      this.receiverPipeName = pipeName;
      this.receiverPipeCreationTime = pipeCreationTime;
      recordPipeReceiverHandshake(
          PipeReceiverRuntimeRegistry.NODE_TYPE_DATA_NODE,
          receiverNodeId,
          PipeReceiverRuntimeRegistry.PROTOCOL_THRIFT);
    }

    @Override
    protected String getSenderHost() {
      return senderHost;
    }

    @Override
    protected String getSenderPort() {
      return senderPort;
    }

    @Override
    protected void closeSession() {
      // Avoid touching SessionManager in this unit test.
    }
  }
}
