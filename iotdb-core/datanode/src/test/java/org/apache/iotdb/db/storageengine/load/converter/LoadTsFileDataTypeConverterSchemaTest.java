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

package org.apache.iotdb.db.storageengine.load.converter;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.InternalClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.lock.DataNodeSchemaLockManager;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.storageengine.load.active.ActiveLoadPathHelper;
import org.apache.iotdb.db.storageengine.load.active.ActiveLoadPendingQueue;
import org.apache.iotdb.db.storageengine.load.active.ActiveLoadTsFileLoader;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.lang.reflect.Method;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*", "javax.management.*"})
@PrepareForTest(Coordinator.class)
public class LoadTsFileDataTypeConverterSchemaTest {
  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();
  private final List<SessionInfo> conversionSessions = new ArrayList<>();
  private final List<Boolean> conversionSessionLoginStates = new ArrayList<>();
  private final List<String> conversionSessionUserNames = new ArrayList<>();
  private InternalClientSession session;
  private int originalDataNodeId;
  private File treeTsFile;

  @Before
  public void setUp() throws Exception {
    originalDataNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(0);
    session = new InternalClientSession("load_schema_conversion");
    session.setUsername("root");
    session.setLogin(true);
    session.setClientVersion(IoTDBConstant.ClientVersion.V_1_0);
    session.setZoneId(ZoneOffset.UTC);
    SessionManager.getInstance().registerSession(session);
    PowerMockito.mockStatic(Coordinator.class);
    final Coordinator coordinator = mock(Coordinator.class);
    when(Coordinator.getInstance()).thenReturn(coordinator);
    when(coordinator.executeForTreeModel(
            any(Statement.class),
            anyLong(),
            any(SessionInfo.class),
            anyString(),
            any(IPartitionFetcher.class),
            any(ISchemaFetcher.class),
            anyLong(),
            anyBoolean(),
            anyBoolean()))
        .thenAnswer(
            invocation -> {
              conversionSessions.add(invocation.getArgument(2));
              final IClientSession currentSession = SessionManager.getInstance().getCurrSession();
              conversionSessionLoginStates.add(currentSession != null && currentSession.isLogin());
              conversionSessionUserNames.add(
                  currentSession == null ? null : currentSession.getUsername());
              return new ExecutionResult(
                  new QueryId("tree_load_schema_conversion"),
                  new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
            });
    treeTsFile = temporaryFolder.newFile("tree.tsfile");
    final List<MeasurementSchema> measurements =
        Collections.singletonList(new MeasurementSchema("s1", TSDataType.INT64));
    try (final TsFileWriter writer = new TsFileWriter(treeTsFile)) {
      writer.registerTimeseries(new Path("root.load_schema.d1"), measurements.get(0));
      final Tablet tablet = new Tablet("root.load_schema.d1", measurements, 1);
      tablet.addTimestamp(0, -1);
      tablet.addValue("s1", 0, 1L);
      tablet.rowSize = 1;
      writer.write(tablet);
    }
  }

  @After
  public void tearDown() {
    SessionManager.getInstance().removeCurrSession();
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(originalDataNodeId);
  }

  @Test
  public void testTreeConversionHonorsRequestWithoutChangingClientSession() throws Exception {
    for (final boolean generatedByPipe : new boolean[] {false, true}) {
      final MPPQueryContext context =
          generatedByPipe
              ? new MPPQueryContext(
                  "",
                  new QueryId("tree_load_schema"),
                  new SessionInfo(
                      session.getId(),
                      "root",
                      ZoneOffset.UTC,
                      IoTDBConstant.ClientVersion.V_1_0,
                      "",
                      true),
                  null,
                  null)
              : new MPPQueryContext(new QueryId("tree_load_schema"));
      final LoadTsFileDataTypeConverter converter =
          new LoadTsFileDataTypeConverter(context, generatedByPipe);
      try {
        for (final boolean requested : new boolean[] {false, true, false}) {
          assertTreeConversionSchemaFlag(converter, requested, requested);
          Assert.assertSame(session, SessionManager.getInstance().getCurrSession());
        }
      } finally {
        DataNodeSchemaLockManager.getInstance().releaseReadLock(context);
      }
    }
  }

  @Test
  public void testPipeTreeConversionRejectsMissingOrMismatchedIdentity() throws Exception {
    assertPipeTreeConversionStatus(
        new MPPQueryContext(new QueryId("pipe_load_missing_user")),
        TSStatusCode.NOT_LOGIN.getStatusCode());
    final SessionInfo mismatchedSession =
        new SessionInfo(
            session.getId(), "alice", ZoneOffset.UTC, IoTDBConstant.ClientVersion.V_1_0, "", true);
    assertPipeTreeConversionStatus(
        new MPPQueryContext(
            "", new QueryId("pipe_load_mismatched_user"), mismatchedSession, null, null),
        TSStatusCode.NO_PERMISSION.getStatusCode());
  }

  @Test
  public void testPipeTreeConversionRejectsMatchingButUnauthenticatedWorkerSession()
      throws Exception {
    session.setLogin(false);
    try {
      assertPipeTreeConversionStatus(
          new MPPQueryContext(
              "",
              new QueryId("pipe_load_unauthenticated_worker"),
              new SessionInfo(
                  session.getId(),
                  "root",
                  ZoneOffset.UTC,
                  IoTDBConstant.ClientVersion.V_1_0,
                  "",
                  true),
              null,
              null),
          TSStatusCode.NOT_LOGIN.getStatusCode());
    } finally {
      session.setLogin(true);
    }
  }

  @Test
  public void testActivePipeLoadBindsAndClearsWorkerIdentity() throws Exception {
    final String pipeUser = "pipe_loader_user";
    final Map<String, String> attributes =
        ActiveLoadPathHelper.buildAttributes(null, null, null, null, null, true, pipeUser);
    attributes.put(ActiveLoadPathHelper.AUTHENTICATED_USER_KEY, Boolean.TRUE.toString());
    attributes.put(ActiveLoadPathHelper.PIPE_AUTHENTICATED_USER_KEY, Boolean.TRUE.toString());
    final File pendingDir = temporaryFolder.getRoot();
    final File targetDir =
        ActiveLoadPathHelper.resolvePipeTransferTargetDir(pendingDir, attributes);
    Assert.assertTrue(targetDir.mkdirs());
    final File activeTsFile = new File(targetDir, "authenticated-pipe-load.tsfile");
    Assert.assertTrue(activeTsFile.createNewFile());
    final ActiveLoadPendingQueue.ActiveLoadEntry entry =
        new ActiveLoadPendingQueue.ActiveLoadEntry(
            activeTsFile.getAbsolutePath(), pendingDir.getAbsolutePath(), true);

    session.setLogin(false);
    session.setUsername(null);
    conversionSessionLoginStates.clear();
    conversionSessionUserNames.clear();
    final Method loadTsFile =
        ActiveLoadTsFileLoader.class.getDeclaredMethod(
            "loadTsFile", ActiveLoadPendingQueue.ActiveLoadEntry.class, IClientSession.class);
    loadTsFile.setAccessible(true);

    final TSStatus status =
        (TSStatus) loadTsFile.invoke(new ActiveLoadTsFileLoader(), entry, session);

    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertEquals(Collections.singletonList(true), conversionSessionLoginStates);
    Assert.assertEquals(Collections.singletonList(pipeUser), conversionSessionUserNames);
    Assert.assertFalse(session.isLogin());
    Assert.assertNull(session.getUsername());
  }

  @Test
  public void testPipeIdentityMarkerWithoutAuthenticatedMarkerIsRejected() throws Exception {
    final String pipeUser = "pipe_loader_user";
    final Map<String, String> attributes =
        ActiveLoadPathHelper.buildAttributes(null, null, null, null, null, false, pipeUser);
    attributes.put(ActiveLoadPathHelper.PIPE_AUTHENTICATED_USER_KEY, Boolean.TRUE.toString());
    final File pendingDir = temporaryFolder.getRoot();
    final File targetDir =
        ActiveLoadPathHelper.resolvePipeTransferTargetDir(pendingDir, attributes);
    Assert.assertTrue(targetDir.mkdirs());
    final File activeTsFile = new File(targetDir, "unmarked-pipe-load.tsfile");
    Assert.assertTrue(activeTsFile.createNewFile());
    final ActiveLoadPendingQueue.ActiveLoadEntry entry =
        new ActiveLoadPendingQueue.ActiveLoadEntry(
            activeTsFile.getAbsolutePath(), pendingDir.getAbsolutePath(), false);
    session.setLogin(false);
    session.setUsername(null);
    final Method loadTsFile =
        ActiveLoadTsFileLoader.class.getDeclaredMethod(
            "loadTsFile", ActiveLoadPendingQueue.ActiveLoadEntry.class, IClientSession.class);
    loadTsFile.setAccessible(true);
    conversionSessionLoginStates.clear();

    final TSStatus status =
        (TSStatus) loadTsFile.invoke(new ActiveLoadTsFileLoader(), entry, session);

    Assert.assertEquals(TSStatusCode.NOT_LOGIN.getStatusCode(), status.getCode());
    Assert.assertTrue(conversionSessionLoginStates.isEmpty());
    Assert.assertNull(session.getUsername());
    Assert.assertFalse(session.isLogin());
  }

  private void assertPipeTreeConversionStatus(
      final MPPQueryContext context, final int expectedStatusCode) throws Exception {
    conversionSessions.clear();
    try {
      final Optional<TSStatus> status =
          new LoadTsFileDataTypeConverter(context, true)
              .convertForTreeModel(
                  LoadTsFileStatement.createUnchecked(treeTsFile.getAbsolutePath()));
      Assert.assertTrue(status.isPresent());
      Assert.assertEquals(expectedStatusCode, status.get().getCode());
      Assert.assertTrue(conversionSessions.isEmpty());
      Assert.assertSame(session, SessionManager.getInstance().getCurrSession());
    } finally {
      DataNodeSchemaLockManager.getInstance().releaseReadLock(context);
    }
  }

  @Test
  public void testTreeConversionPreservesDisabledQuerySession() throws Exception {
    assertDisabledQuerySession(false);
  }

  @Test
  public void testTreeConversionPreservesDisabledQuerySessionWithoutClientSession()
      throws Exception {
    assertDisabledQuerySession(true);
  }

  private void assertDisabledQuerySession(final boolean withoutClientSession) throws Exception {
    if (withoutClientSession) {
      SessionManager.getInstance().removeCurrSession();
    }
    final SessionInfo querySession =
        new SessionInfo(
            session.getId(), "root", ZoneOffset.UTC, IoTDBConstant.ClientVersion.V_1_0, "", false);
    final MPPQueryContext context =
        new MPPQueryContext("", new QueryId("tree_load_schema"), querySession, null, null);
    try {
      assertTreeConversionSchemaFlag(new LoadTsFileDataTypeConverter(context, false), true, false);
      Assert.assertSame(querySession, context.getSession());
      Assert.assertFalse(querySession.isEnableAutoCreateSchema());
      if (withoutClientSession) {
        Assert.assertNull(SessionManager.getInstance().getCurrSession());
      } else {
        Assert.assertSame(session, SessionManager.getInstance().getCurrSession());
      }
    } finally {
      DataNodeSchemaLockManager.getInstance().releaseReadLock(context);
    }
  }

  private void assertTreeConversionSchemaFlag(
      final LoadTsFileDataTypeConverter converter, final boolean requested, final boolean expected)
      throws Exception {
    conversionSessions.clear();
    final LoadTsFileStatement statement =
        LoadTsFileStatement.createUnchecked(treeTsFile.getAbsolutePath());
    statement.setAutoCreateSchema(requested);
    final Optional<TSStatus> status = converter.convertForTreeModel(statement);
    Assert.assertTrue(status.isPresent());
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.get().getCode());
    Assert.assertFalse(conversionSessions.isEmpty());
    for (final SessionInfo conversionSession : conversionSessions) {
      Assert.assertEquals(expected, conversionSession.isEnableAutoCreateSchema());
      Assert.assertEquals("root", conversionSession.getUserName());
    }
    Assert.assertEquals(requested, statement.isAutoCreateSchema());
    Assert.assertTrue(treeTsFile.exists());
  }
}
