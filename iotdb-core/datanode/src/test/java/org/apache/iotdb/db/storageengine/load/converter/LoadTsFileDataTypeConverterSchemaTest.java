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
import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.common.SqlDialect;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.InternalClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.lock.DataNodeSchemaLockManager;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LoadTsFile;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.schema.Schema;
import org.apache.tsfile.write.writer.TsFileIOWriter;
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
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*", "javax.management.*"})
@PrepareForTest({Coordinator.class, AuthorityChecker.class})
public class LoadTsFileDataTypeConverterSchemaTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final List<SessionInfo> conversionSessions = new ArrayList<>();
  private final List<SessionInfo> treeConversionSessions = new ArrayList<>();
  private InternalClientSession session;
  private int originalDataNodeId;
  private File tsFile;
  private File treeTsFile;

  @Before
  public void setUp() throws Exception {
    originalDataNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(0);
    session = new InternalClientSession("load_schema_conversion");
    session.setUserId(0);
    session.setUsername("root");
    session.setClientVersion(IoTDBConstant.ClientVersion.V_1_0);
    session.setZoneId(ZoneOffset.UTC);
    session.setSqlDialect(SqlDialect.TREE);
    SessionManager.getInstance().registerSession(session);

    PowerMockito.mockStatic(Coordinator.class);
    PowerMockito.mockStatic(AuthorityChecker.class);
    PowerMockito.when(AuthorityChecker.getUserId(anyString())).thenReturn(Optional.of(0L));
    final Coordinator coordinator = mock(Coordinator.class);
    when(Coordinator.getInstance()).thenReturn(coordinator);
    when(coordinator.executeForTableModel(
            any(Statement.class),
            any(SqlParser.class),
            any(IClientSession.class),
            anyLong(),
            any(SessionInfo.class),
            anyString(),
            any(Metadata.class),
            anyLong()))
        .thenAnswer(
            invocation -> {
              conversionSessions.add(invocation.getArgument(4));
              return new ExecutionResult(
                  new QueryId("load_schema_conversion"),
                  new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
            });
    tsFile = new File(temporaryFolder.getRoot(), "table.tsfile");
    writeTableTsFile();
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
              treeConversionSessions.add(invocation.getArgument(2));
              return new ExecutionResult(
                  new QueryId("tree_load_schema_conversion"),
                  new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
            });
    treeTsFile = new File(temporaryFolder.getRoot(), "tree.tsfile");
    final List<IMeasurementSchema> measurements =
        Collections.singletonList(new MeasurementSchema("s1", TSDataType.INT64));
    try (final TsFileWriter writer = new TsFileWriter(treeTsFile)) {
      writer.registerTimeseries(new Path("root.load_schema.d1"), measurements.get(0));
      final Tablet tablet = new Tablet("root.load_schema.d1", measurements, 1);
      tablet.addTimestamp(0, -1);
      tablet.addValue("s1", 0, 1L);
      writer.writeTree(tablet);
    }
  }

  @After
  public void tearDown() {
    SessionManager.getInstance().removeCurrSession();
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(originalDataNodeId);
  }

  @Test
  public void testTableConversionHonorsRequestWithoutChangingClientSession() {
    final LoadTsFileDataTypeConverter converter =
        new LoadTsFileDataTypeConverter(new MPPQueryContext(new QueryId("load_schema")), false);
    for (final boolean requested : new boolean[] {false, true, false}) {
      assertConversionSchemaFlag(converter, requested, requested);
      Assert.assertTrue(session.isEnableAutoCreateSchema());
      Assert.assertEquals(SqlDialect.TREE, session.getSqlDialect());
      Assert.assertSame(session, SessionManager.getInstance().getCurrSession());
    }
  }

  @Test
  public void testTableConversionPreservesDisabledClientSessionSchemaCreation() {
    session.setEnableAutoCreateSchema(false);
    final LoadTsFileDataTypeConverter converter =
        new LoadTsFileDataTypeConverter(new MPPQueryContext(new QueryId("load_schema")), false);
    assertConversionSchemaFlag(converter, true, false);
    Assert.assertFalse(session.isEnableAutoCreateSchema());
  }

  @Test
  public void testTableConversionPreservesDisabledQuerySessionWithoutClientSession() {
    SessionManager.getInstance().removeCurrSession();
    final MPPQueryContext context = new MPPQueryContext(new QueryId("load_schema"));
    context.setSession(
        new SessionInfo(
            session.getId(),
            new UserEntity(session.getUserId(), session.getUsername(), session.getClientAddress()),
            ZoneOffset.UTC,
            session.getClientVersion(),
            "db",
            SqlDialect.TABLE,
            false));
    final LoadTsFileDataTypeConverter converter = new LoadTsFileDataTypeConverter(context, false);

    assertConversionFlagWithoutClientSession(converter);

    Assert.assertNull(SessionManager.getInstance().getCurrSession());
  }

  @Test
  public void testTreeConversionHonorsRequestWithoutChangingClientSession() throws Exception {
    for (final boolean generatedByPipe : new boolean[] {false, true}) {
      final MPPQueryContext context = new MPPQueryContext(new QueryId("tree_load_schema"));
      final LoadTsFileDataTypeConverter converter =
          new LoadTsFileDataTypeConverter(context, generatedByPipe);
      try {
        for (final boolean requested : new boolean[] {false, true, false}) {
          assertTreeConversionSchemaFlag(converter, requested, requested);
          Assert.assertTrue(session.isEnableAutoCreateSchema());
          Assert.assertEquals(SqlDialect.TREE, session.getSqlDialect());
          Assert.assertSame(session, SessionManager.getInstance().getCurrSession());
        }
      } finally {
        DataNodeSchemaLockManager.getInstance().releaseReadLock(context);
      }
    }
  }

  @Test
  public void testTreeConversionPreservesDisabledClientSession() throws Exception {
    session.setEnableAutoCreateSchema(false);
    final MPPQueryContext context = new MPPQueryContext(new QueryId("tree_load_schema"));
    try {
      assertTreeConversionSchemaFlag(new LoadTsFileDataTypeConverter(context, false), true, false);
      Assert.assertFalse(session.isEnableAutoCreateSchema());
    } finally {
      DataNodeSchemaLockManager.getInstance().releaseReadLock(context);
    }
  }

  @Test
  public void testTreeConversionPreservesDisabledQuerySession() throws Exception {
    final MPPQueryContext context = new MPPQueryContext(new QueryId("tree_load_schema"));
    context.setSession(
        new SessionInfo(
            session.getId(),
            new UserEntity(session.getUserId(), session.getUsername(), session.getClientAddress()),
            ZoneOffset.UTC,
            session.getClientVersion(),
            null,
            SqlDialect.TREE,
            false));
    try {
      final LoadTsFileDataTypeConverter converter = new LoadTsFileDataTypeConverter(context, false);
      assertTreeConversionSchemaFlag(converter, true, false);
      Assert.assertTrue(session.isEnableAutoCreateSchema());
      SessionManager.getInstance().removeCurrSession();
      assertTreeConversionSchemaFlag(converter, true, false);
      Assert.assertNull(SessionManager.getInstance().getCurrSession());
    } finally {
      DataNodeSchemaLockManager.getInstance().releaseReadLock(context);
    }
  }

  private void assertTreeConversionSchemaFlag(
      final LoadTsFileDataTypeConverter converter, final boolean requested, final boolean expected)
      throws Exception {
    treeConversionSessions.clear();
    final LoadTsFileStatement statement =
        LoadTsFileStatement.createUnchecked(treeTsFile.getAbsolutePath());
    statement.setAutoCreateSchema(requested);
    final Optional<TSStatus> status = converter.convertForTreeModel(statement);
    Assert.assertTrue(status.isPresent());
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.get().getCode());
    Assert.assertFalse(treeConversionSessions.isEmpty());
    for (final SessionInfo conversionSession : treeConversionSessions) {
      Assert.assertEquals(expected, conversionSession.isEnableAutoCreateSchema());
      Assert.assertEquals(SqlDialect.TREE, conversionSession.getSqlDialect());
      Assert.assertEquals("root", conversionSession.getUserName());
    }
    Assert.assertEquals(requested, statement.isAutoCreateSchema());
    Assert.assertTrue(treeTsFile.exists());
  }

  private void assertConversionSchemaFlag(
      final LoadTsFileDataTypeConverter converter,
      final boolean requested,
      final boolean expected) {
    conversionSessions.clear();
    final LoadTsFile statement =
        LoadTsFile.createUnchecked(
                null,
                tsFile.getAbsolutePath(),
                requested
                    ? Collections.emptyMap()
                    : Collections.singletonMap("auto-create-schema", "false"))
            .setDatabase("db");
    final Optional<TSStatus> status = converter.convertForTableModel(statement);
    Assert.assertTrue(status.isPresent());
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.get().getCode());
    Assert.assertFalse(conversionSessions.isEmpty());
    for (final SessionInfo conversionSession : conversionSessions) {
      Assert.assertEquals(expected, conversionSession.isEnableAutoCreateSchema());
      Assert.assertEquals("db", conversionSession.getDatabaseName().orElse(null));
      Assert.assertEquals(SqlDialect.TABLE, conversionSession.getSqlDialect());
      Assert.assertEquals("root", conversionSession.getUserName());
    }
  }

  private void assertConversionFlagWithoutClientSession(
      final LoadTsFileDataTypeConverter converter) {
    conversionSessions.clear();
    final LoadTsFile statement =
        LoadTsFile.createUnchecked(
                null,
                tsFile.getAbsolutePath(),
                Collections.singletonMap("auto-create-schema", "true"))
            .setDatabase("db");
    final Optional<TSStatus> status = converter.convertForTableModel(statement);
    Assert.assertTrue(status.isPresent());
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.get().getCode());
    Assert.assertFalse(conversionSessions.isEmpty());
    for (final SessionInfo conversionSession : conversionSessions) {
      Assert.assertFalse(conversionSession.isEnableAutoCreateSchema());
    }
  }

  private void writeTableTsFile() throws Exception {
    final List<IMeasurementSchema> columns =
        Arrays.asList(
            new MeasurementSchema("tag1", TSDataType.STRING),
            new MeasurementSchema("s1", TSDataType.INT64));
    final Schema schema = new Schema();
    schema.registerTableSchema(
        new TableSchema(
            "table1", columns, Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD)));
    try (final TsFileIOWriter writer = new TsFileIOWriter(tsFile)) {
      writer.setSchema(schema);
      writer.startChunkGroup(new StringArrayDeviceID(new String[] {"table1", "tagA"}));
      final AlignedChunkWriterImpl chunkWriter =
          new AlignedChunkWriterImpl(columns.subList(1, columns.size()));
      chunkWriter.getTimeChunkWriter().write(1);
      chunkWriter.getValueChunkWriterByIndex(0).write(1, 1L, false);
      chunkWriter.writeToFileWriter(writer);
      writer.endChunkGroup();
      writer.endFile();
    }
  }
}
