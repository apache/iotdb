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
import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.sink.compressor.PipeCompressor;
import org.apache.iotdb.commons.pipe.sink.compressor.PipeCompressorFactory;
import org.apache.iotdb.commons.pipe.sink.payload.airgap.AirGapPseudoTPipeTransferRequest;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferCompressedReq;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferSliceReq;
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.arithmetic.AdditionViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.leaf.TimeSeriesViewOperand;
import org.apache.iotdb.commons.schema.view.viewExpression.multi.FunctionViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.ternary.BetweenViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.unary.NegationViewExpression;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferDataNodeHandshakeV2Req;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferPlanNodeReq;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.InternalClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.view.AlterLogicalViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.view.CreateLogicalViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.storageengine.load.active.ActiveLoadPathHelper;
import org.apache.iotdb.db.storageengine.load.config.LoadTsFileConfigurator;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class IoTDBDataNodeReceiverTest {

  private static final String ADMISSION_USER = "pipe-test-user";
  private final SessionManager sessionManager = SessionManager.getInstance();
  private final IClientSession testSession = new InternalClientSession("pipe-test-host");

  @Before
  public void registerSession() {
    Assert.assertTrue(sessionManager.registerSession(testSession));
    AuthorityChecker.getAuthorityFetcher()
        .getAuthorCache()
        .putUserCache(ADMISSION_USER, new User(ADMISSION_USER, "password"));
  }

  @After
  public void removeSession() {
    sessionManager.removeCurrSession();
    AuthorityChecker.invalidateCache(ADMISSION_USER, null);
  }

  private void authenticateSession(final String userName) {
    sessionManager.supplySession(
        testSession, userName, ZoneId.systemDefault(), IoTDBConstant.ClientVersion.V_1_0);
  }

  @Test
  public void testRatisRejectsInsertionInPlanNodeRequests() throws Exception {
    final String originalProtocol =
        IoTDBDescriptor.getInstance().getConfig().getDataRegionConsensusProtocolClass();
    try {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setDataRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS);
      for (final PlanNode node : insertionPlans()) {
        assertPlanRequestEnvelopes(node, true);
      }
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setDataRegionConsensusProtocolClass(originalProtocol);
    }
  }

  @Test
  public void testRatisAllowsMetadataAndDeletionPlanNodeRequests() throws Exception {
    final String originalProtocol =
        IoTDBDescriptor.getInstance().getConfig().getDataRegionConsensusProtocolClass();
    try {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setDataRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS);
      assertPlanRequestEnvelopes(
          new DeleteDataNode(
              new PlanNodeId("delete"),
              Collections.singletonList(new MeasurementPath("root.sg.d.s")),
              0,
              1),
          false);
      assertPlanRequestEnvelopes(
          new CreateTimeSeriesNode(
              new PlanNodeId("schema"),
              new MeasurementPath("root.sg.d.s"),
              TSDataType.INT32,
              TSEncoding.PLAIN,
              CompressionType.UNCOMPRESSED,
              null,
              null,
              null,
              null),
          false);
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setDataRegionConsensusProtocolClass(originalProtocol);
    }
  }

  @Test
  public void testNonRatisAllowsInsertionPlanNodeRequests() throws Exception {
    final String originalProtocol =
        IoTDBDescriptor.getInstance().getConfig().getDataRegionConsensusProtocolClass();
    try {
      for (final String protocol :
          new String[] {
            ConsensusFactory.IOT_CONSENSUS,
            ConsensusFactory.IOT_CONSENSUS_V2,
            ConsensusFactory.SIMPLE_CONSENSUS
          }) {
        IoTDBDescriptor.getInstance().getConfig().setDataRegionConsensusProtocolClass(protocol);
        for (final PlanNode node : insertionPlans()) {
          assertPlanRequestEnvelopes(node, false);
        }
      }
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setDataRegionConsensusProtocolClass(originalProtocol);
    }
  }

  private List<PlanNode> insertionPlans() throws Exception {
    final InsertRowNode row =
        new InsertRowNode(
            new PlanNodeId("row"),
            new PartialPath("root.sg.d"),
            false,
            new String[] {"s"},
            new TSDataType[] {TSDataType.INT32},
            1L,
            new Object[] {1},
            false);
    return Arrays.asList(
        row,
        new InsertRowsNode(
            new PlanNodeId("rows"), Collections.singletonList(0), Collections.singletonList(row)),
        new InsertTabletNode(
            new PlanNodeId("tablet"),
            new PartialPath("root.sg.d"),
            false,
            new String[] {"s"},
            new TSDataType[] {TSDataType.INT32},
            new long[] {1},
            null,
            new Object[] {new int[] {1}},
            1));
  }

  private void assertPlanRequestEnvelopes(final PlanNode node, final boolean rejected)
      throws Exception {
    authenticateSession(ADMISSION_USER);
    final AdmissionTestReceiver receiver = new AdmissionTestReceiver();
    final int expectedCode =
        rejected
            ? TSStatusCode.UNSUPPORTED_OPERATION.getStatusCode()
            : TSStatusCode.NO_PERMISSION.getStatusCode();
    Assert.assertEquals(
        expectedCode,
        receiver.receive(PipeTransferPlanNodeReq.toTPipeTransferReq(node)).getStatus().getCode());

    final TPipeTransferReq plain = PipeTransferPlanNodeReq.toTPipeTransferReq(node);
    final TPipeTransferReq airGap =
        new AirGapPseudoTPipeTransferRequest()
            .setVersion(plain.getVersion())
            .setType(plain.getType())
            .setBody(plain.body.duplicate());
    Assert.assertEquals(expectedCode, receiver.receive(airGap).getStatus().getCode());
    Assert.assertEquals(
        expectedCode,
        receiver
            .receive(
                PipeTransferCompressedReq.toTPipeTransferReq(
                    PipeTransferPlanNodeReq.toTPipeTransferReq(node),
                    Collections.singletonList(
                        PipeCompressorFactory.getCompressor(
                            PipeCompressor.PipeCompressionType.GZIP.getIndex()))))
            .getStatus()
            .getCode());

    final TPipeTransferReq sliced = PipeTransferPlanNodeReq.toTPipeTransferReq(node);
    final ByteBuffer body = sliced.body.duplicate();
    final int midpoint = body.limit() / 2;
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        receiver
            .receive(
                PipeTransferSliceReq.toTPipeTransferReq(
                    1, sliced.getType(), 0, 2, body, 0, midpoint))
            .getStatus()
            .getCode());
    Assert.assertEquals(
        expectedCode,
        receiver
            .receive(
                PipeTransferSliceReq.toTPipeTransferReq(
                    1, sliced.getType(), 1, 2, body, midpoint, body.limit()))
            .getStatus()
            .getCode());
    Assert.assertEquals(rejected ? 7 : 11, receiver.loginAttempts);
  }

  @Test
  public void testCompressedAndSliceRequestsRequireAuthentication() throws Exception {
    final AdmissionTestReceiver receiver = new AdmissionTestReceiver(false);

    final PlanNode node = insertionPlans().get(0);
    final TPipeTransferReq planRequest = PipeTransferPlanNodeReq.toTPipeTransferReq(node);
    final TPipeTransferReq compressedRequest =
        PipeTransferCompressedReq.toTPipeTransferReq(
            planRequest,
            Collections.singletonList(
                PipeCompressorFactory.getCompressor(
                    PipeCompressor.PipeCompressionType.GZIP.getIndex())));
    Assert.assertEquals(
        TSStatusCode.NOT_LOGIN.getStatusCode(),
        receiver.receive(compressedRequest).getStatus().getCode());

    final ByteBuffer body = planRequest.body.duplicate();
    Assert.assertEquals(
        TSStatusCode.NOT_LOGIN.getStatusCode(),
        receiver
            .receive(
                PipeTransferSliceReq.toTPipeTransferReq(
                    1, planRequest.getType(), 0, 2, body, 0, body.limit() / 2))
            .getStatus()
            .getCode());
    Assert.assertEquals(0, receiver.loginAttempts);
  }

  @Test
  public void testClosedSessionCannotContinueReceivingSlices() throws Exception {
    authenticateSession(ADMISSION_USER);
    final IoTDBDataNodeReceiver receiver = new IoTDBDataNodeReceiver();
    final TPipeTransferReq request =
        PipeTransferPlanNodeReq.toTPipeTransferReq(insertionPlans().get(0));
    final ByteBuffer body = request.body.duplicate();
    final int midpoint = body.limit() / 2;
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        receiver
            .receive(
                PipeTransferSliceReq.toTPipeTransferReq(
                    1, request.getType(), 0, 2, body, 0, midpoint))
            .getStatus()
            .getCode());

    Assert.assertTrue(sessionManager.closeSession(testSession, queryId -> {}));
    Assert.assertFalse(testSession.isLogin());
    Assert.assertNull(testSession.getUsername());
    authenticateSession(ADMISSION_USER);
    Assert.assertEquals(
        TSStatusCode.PIPE_TRANSFER_SLICE_OUT_OF_ORDER.getStatusCode(),
        receiver
            .receive(
                PipeTransferSliceReq.toTPipeTransferReq(
                    1, request.getType(), 1, 2, body, midpoint, body.limit()))
            .getStatus()
            .getCode());
  }

  @Test
  public void testFailedPipeHandshakeCannotReuseExistingSession() throws Exception {
    authenticateSession(ADMISSION_USER);
    final IoTDBDataNodeReceiver receiver = new AdmissionTestReceiver();
    Assert.assertNotEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        receiver
            .receive(PipeTransferDataNodeHandshakeV2Req.toTPipeTransferReq(Collections.emptyMap()))
            .getStatus()
            .getCode());
    Assert.assertEquals(
        TSStatusCode.NOT_LOGIN.getStatusCode(),
        receiver
            .receive(PipeTransferPlanNodeReq.toTPipeTransferReq(insertionPlans().get(0)))
            .getStatus()
            .getCode());
  }

  @Test
  public void testCreateLogicalViewPlanRequiresReadSchemaForEveryNestedSource() throws Exception {
    final List<PartialPath> sourcePaths =
        Arrays.asList(
            new PartialPath("root.sg.secret.unary"),
            new PartialPath("root.sg.secret.binary"),
            new PartialPath("root.sg.secret.ternaryFirst"),
            new PartialPath("root.sg.secret.ternarySecond"),
            new PartialPath("root.sg.secret.ternaryThird"));
    final ViewExpression viewExpression =
        new FunctionViewExpression(
            "nested",
            Collections.emptyList(),
            Collections.emptyList(),
            Arrays.asList(
                new AdditionViewExpression(
                    new NegationViewExpression(
                        new TimeSeriesViewOperand(sourcePaths.get(0).getFullPath())),
                    new TimeSeriesViewOperand(sourcePaths.get(1).getFullPath())),
                new BetweenViewExpression(
                    new TimeSeriesViewOperand(sourcePaths.get(2).getFullPath()),
                    new TimeSeriesViewOperand(sourcePaths.get(3).getFullPath()),
                    new TimeSeriesViewOperand(sourcePaths.get(4).getFullPath()))));
    final Map<PartialPath, ViewExpression> viewPathToSourceMap =
        Collections.singletonMap(new PartialPath("root.sg.target.view"), viewExpression);
    final List<PlanNode> viewNodes =
        Arrays.asList(
            new CreateLogicalViewNode(new PlanNodeId("create-view"), viewPathToSourceMap),
            new AlterLogicalViewNode(new PlanNodeId("alter-view"), viewPathToSourceMap));
    final IoTDBDataNodeReceiver receiver = new IoTDBDataNodeReceiver();

    for (int deniedSourceIndex = 0; deniedSourceIndex < sourcePaths.size(); deniedSourceIndex++) {
      final String username = "pipeLogicalViewReceiverUser" + deniedSourceIndex;
      final User user = new User(username, "password");
      user.addPathPrivilege(
          new PartialPath("root.sg.target.**"), PrivilegeType.WRITE_SCHEMA.ordinal(), false);
      for (int sourceIndex = 0; sourceIndex < sourcePaths.size(); sourceIndex++) {
        if (sourceIndex != deniedSourceIndex) {
          user.addPathPrivilege(
              sourcePaths.get(sourceIndex), PrivilegeType.READ_SCHEMA.ordinal(), false);
        }
      }
      AuthorityChecker.getAuthorityFetcher().getAuthorCache().putUserCache(username, user);

      try {
        authenticateSession(username);
        Assert.assertTrue(
            AuthorityChecker.checkFullPathListPermission(
                    username,
                    Collections.singletonList(new PartialPath("root.sg.target.view")),
                    PrivilegeType.WRITE_SCHEMA.ordinal())
                .isEmpty());
        for (final PlanNode node : viewNodes) {
          Assert.assertEquals(
              TSStatusCode.NO_PERMISSION.getStatusCode(),
              receiver
                  .receive(PipeTransferPlanNodeReq.toTPipeTransferReq(node))
                  .getStatus()
                  .getCode());
        }
      } finally {
        AuthorityChecker.invalidateCache(username, null);
      }
    }
  }

  private static class AdmissionTestReceiver extends IoTDBDataNodeReceiver {
    private int loginAttempts;

    private AdmissionTestReceiver() {
      this(true);
    }

    private AdmissionTestReceiver(final boolean authenticated) {
      hasPipeHandshakeCredential = authenticated;
      if (authenticated) {
        username = ADMISSION_USER;
        userEntity = new UserEntity(-1, username, "pipe-test-host");
      }
    }

    @Override
    protected TSStatus loginIfNecessary() {
      loginAttempts++;
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }
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
              "root.test.sg_0", true, false, true, false, null, "load-test-user");

      Assert.assertEquals(
          "root.test.sg_0", attributes.get(LoadTsFileConfigurator.DATABASE_NAME_KEY));
      Assert.assertEquals("2", attributes.get(LoadTsFileConfigurator.DATABASE_LEVEL_KEY));
      Assert.assertEquals("load-test-user", attributes.get(ActiveLoadPathHelper.USER_KEY));
      Assert.assertEquals(
          Boolean.TRUE.toString(), attributes.get(ActiveLoadPathHelper.AUTHENTICATED_USER_KEY));
      Assert.assertEquals(
          Boolean.TRUE.toString(),
          attributes.get(ActiveLoadPathHelper.PIPE_AUTHENTICATED_USER_KEY));

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
              "root.test.sg_0", false, false, true, true, null, "load-test-user");
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
}
