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

package org.apache.iotdb.db.queryengine.plan.analyze;

import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.confignode.rpc.thrift.IConfigNodeRPCService;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TTimeSlotList;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.query.QueryTimeoutRuntimeException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.queryengine.exception.MemoryNotEnoughException;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.parser.StatementGenerator;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.utils.ErrorHandlingUtils;
import org.apache.iotdb.rpc.TElasticFramedTransport;
import org.apache.iotdb.rpc.TSStatusCode;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class QueryPartitionMemoryTest {

  private int originalDataNodeId;
  private Logger logger;
  private Level originalLogLevel;
  private ListAppender<ILoggingEvent> appender;

  @Before
  public void setUp() {
    originalDataNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(0);
    logger = (Logger) LoggerFactory.getLogger(Coordinator.class);
    originalLogLevel = logger.getLevel();
    logger.setLevel(Level.INFO);
    appender = new ListAppender<>();
    appender.start();
    logger.addAppender(appender);
  }

  @After
  public void tearDown() {
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(originalDataNodeId);
    logger.detachAppender(appender);
    appender.stop();
    logger.setLevel(originalLogLevel);
  }

  @Test
  public void testPartitionMemoryFailureReportsItsSourceAndSql() {
    long freeMemory = LocalExecutionPlanner.getInstance().getFreeMemoryForOperators();
    String sql = "select s1 from root.sg.d1 where time >= 0 and time <= " + (Long.MAX_VALUE - 1);
    for (long queryId = 200; queryId < 203; queryId++) {
      final long id = queryId;
      FakeSchemaFetcherImpl schemaFetcher = reservingSchemaFetcher(false);
      MemoryNotEnoughException failure =
          assertThrows(MemoryNotEnoughException.class, () -> execute(sql, id, schemaFetcher));
      assertTrue(failure.getMessage(), failure.getMessage().contains("time partitions"));
      assertFalse(failure.getMessage(), failure.getMessage().contains("too many columns"));
      assertFalse(failure.getMessage(), failure.getMessage().contains("SLIMIT"));
      assertEquals(
          TSStatusCode.QUOTA_MEM_QUERY_NOT_ENOUGH.getStatusCode(),
          ErrorHandlingUtils.onQueryException(failure, "query").getCode());
      assertEquals(freeMemory, LocalExecutionPlanner.getInstance().getFreeMemoryForOperators());
      assertNull(Coordinator.getInstance().getQueryExecution(queryId));
    }
    assertFailureLogs(sql, 3);
  }

  @Test
  public void testPartitionTimeoutLogsSqlAndReleasesEarlierReservations() {
    long freeMemory = LocalExecutionPlanner.getInstance().getFreeMemoryForOperators();
    String sql = "select s1 from root.sg.d1 where time >= 0 and time <= 100";
    FakeSchemaFetcherImpl schemaFetcher = reservingSchemaFetcher(true);
    assertThrows(QueryTimeoutRuntimeException.class, () -> execute(sql, 300, schemaFetcher));
    assertEquals(freeMemory, LocalExecutionPlanner.getInstance().getFreeMemoryForOperators());
    assertNull(Coordinator.getInstance().getQueryExecution(300L));
    assertFailureLogs(sql, 1);
  }

  @Test
  public void testSemanticFailureOnlyLogsSqlContext() {
    long freeMemory = LocalExecutionPlanner.getInstance().getFreeMemoryForOperators();
    String sql = "select count(s1), s1 from root.sg.d1";
    FakeSchemaFetcherImpl schemaFetcher = new FakeSchemaFetcherImpl();
    SemanticException failure =
        assertThrows(SemanticException.class, () -> execute(sql, 600, schemaFetcher));
    assertEquals(
        TSStatusCode.SEMANTIC_ERROR.getStatusCode(),
        ErrorHandlingUtils.onQueryException(failure, "query").getCode());
    assertEquals(freeMemory, LocalExecutionPlanner.getInstance().getFreeMemoryForOperators());
    assertNull(Coordinator.getInstance().getQueryExecution(600L));
    assertFailureLogs(sql, 1);
  }

  @Test
  public void testAnalysisErrorIsLoggedAndRethrownWithoutRetainingMemory() {
    long freeMemory = LocalExecutionPlanner.getInstance().getFreeMemoryForOperators();
    String sql = "select s1 from root.sg.d1 where time >= 0 and time <= 100";
    OutOfMemoryError failure = new OutOfMemoryError("simulated analysis allocation failure");
    FakeSchemaFetcherImpl schemaFetcher =
        new FakeSchemaFetcherImpl() {
          @Override
          public ClusterSchemaTree fetchSchema(
              PathPatternTree patternTree, boolean withTemplate, MPPQueryContext context) {
            context.reserveMemoryForFrontEnd(2 * 1024 * 1024);
            throw failure;
          }
        };
    assertSame(
        failure, assertThrows(OutOfMemoryError.class, () -> execute(sql, 500, schemaFetcher)));
    assertEquals(freeMemory, LocalExecutionPlanner.getInstance().getFreeMemoryForOperators());
    assertNull(Coordinator.getInstance().getQueryExecution(500L));
    assertFailureLogs(sql, 1);
  }

  @Test
  public void testNormalQueryDoesNotLogFailure() {
    String sql = "select s1 from root.sg.d1 where time > 10 and time < 5";
    long freeMemory = LocalExecutionPlanner.getInstance().getFreeMemoryForOperators();
    try {
      assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          execute(sql, 400, new FakeSchemaFetcherImpl()));
      assertTrue(appender.list.isEmpty());
      assertEquals(freeMemory, LocalExecutionPlanner.getInstance().getFreeMemoryForOperators());
    } finally {
      Coordinator.getInstance().cleanupQueryExecution(400L);
    }
  }

  private int execute(String sql, long queryId, FakeSchemaFetcherImpl schemaFetcher) {
    return Coordinator.getInstance()
        .executeForTreeModel(
            StatementGenerator.createStatement(sql, ZoneOffset.UTC),
            queryId,
            new SessionInfo(0, "root", ZoneOffset.UTC, ""),
            sql,
            new FakePartitionFetcherImpl(),
            schemaFetcher,
            60_000,
            true,
            false)
        .status
        .getCode();
  }

  private FakeSchemaFetcherImpl reservingSchemaFetcher(boolean expire) {
    return new FakeSchemaFetcherImpl() {
      @Override
      public ClusterSchemaTree fetchSchema(
          PathPatternTree patternTree, boolean withTemplate, MPPQueryContext context) {
        context.reserveMemoryForFrontEnd(2 * 1024 * 1024);
        if (expire) {
          context.setStartTime(1);
          context.setTimeOut(1);
        }
        return super.fetchSchema(patternTree, withTemplate, context);
      }
    };
  }

  private void assertFailureLogs(String sql, int count) {
    assertEquals(count, appender.list.size());
    for (ILoggingEvent event : appender.list) {
      assertEquals(Level.INFO, event.getLevel());
      assertTrue(event.getFormattedMessage(), event.getFormattedMessage().contains(sql));
      assertTrue(event.getFormattedMessage(), event.getFormattedMessage().contains("queryId:"));
      assertNull(event.getThrowableProxy());
    }
  }

  @Test
  public void testOversizedPartitionRequestReleasesFrontEndMemory() {
    LocalExecutionPlanner planner = LocalExecutionPlanner.getInstance();
    long freeMemory = planner.getFreeMemoryForOperators();
    String sql =
        "select s1 from root.sg.d1 where time >= 0 and time <= "
            + TimePartitionUtils.getTimePartitionInterval() * 50_000;
    for (long queryId = 100; queryId < 103; queryId++) {
      final int[] serializedBytes = {0};
      IPartitionFetcher partitionFetcher =
          new FakePartitionFetcherImpl() {
            @Override
            public DataPartition getDataPartition(
                Map<String, List<DataPartitionQueryParam>> params) {
              assertTrue(planner.getFreeMemoryForOperators() < freeMemory);
              DataPartitionQueryParam param = params.get("root.sg").get(0);
              TTimeSlotList slots =
                  new TTimeSlotList(param.getTimePartitionSlotList(), false, false);
              TDataPartitionReq request =
                  new TDataPartitionReq(
                      Collections.singletonMap(
                          "root.sg", Collections.singletonMap(new TSeriesPartitionSlot(0), slots)));
              try (TElasticFramedTransport transport =
                  new TElasticFramedTransport(new TMemoryBuffer(0), 16, 1024, false) {
                    @Override
                    public void close() {
                      serializedBytes[0] = writeBuffer.getPos();
                      super.close();
                    }
                  }) {
                new IConfigNodeRPCService.Client(new TBinaryProtocol(transport))
                    .send_getDataPartitionTable(request);
                fail("The partition request must exceed the frame size limit");
              } catch (TException e) {
                throw new StatementAnalyzeException(e.getMessage());
              }
              return null;
            }
          };
      final long id = queryId;
      Coordinator coordinator = Coordinator.getInstance();
      Statement statement = StatementGenerator.createStatement(sql, ZoneOffset.UTC);
      SessionInfo session = new SessionInfo(0, "root", ZoneOffset.UTC, "");
      FakeSchemaFetcherImpl schemaFetcher = new FakeSchemaFetcherImpl();
      StatementAnalyzeException failure =
          assertThrows(
              StatementAnalyzeException.class,
              () ->
                  coordinator.executeForTreeModel(
                      statement,
                      id,
                      session,
                      sql,
                      partitionFetcher,
                      schemaFetcher,
                      60_000,
                      true,
                      false));
      assertTrue(failure.getMessage(), failure.getMessage().contains("at least"));
      assertTrue(failure.getMessage(), failure.getMessage().contains("1024 bytes"));
      assertTrue(failure.getMessage(), failure.getMessage().contains("dn_thrift_max_frame_size"));
      assertTrue(serializedBytes[0] > 0);
      assertTrue("Serialization must stop at the frame limit", serializedBytes[0] <= 1024);
      assertEquals(freeMemory, planner.getFreeMemoryForOperators());
      assertNull(Coordinator.getInstance().getQueryExecution(queryId));
    }
    assertFailureLogs(sql, 3);
  }
}
