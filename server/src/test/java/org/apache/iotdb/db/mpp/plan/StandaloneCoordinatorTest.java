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

package org.apache.iotdb.db.mpp.plan;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngineV2;
import org.apache.iotdb.db.engine.flush.FlushManager;
import org.apache.iotdb.db.localconfignode.LocalConfigNode;
import org.apache.iotdb.db.mpp.common.SessionInfo;
import org.apache.iotdb.db.mpp.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.ISchemaFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.StandalonePartitionFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.StandaloneSchemaFetcher;
import org.apache.iotdb.db.mpp.plan.execution.ExecutionResult;
import org.apache.iotdb.db.mpp.plan.parser.StatementGenerator;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.wal.WALManager;
import org.apache.iotdb.db.wal.recover.WALRecoverManager;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.ZoneId;
import java.util.HashMap;

public class StandaloneCoordinatorTest {

  private static final IoTDBConfig conf = IoTDBDescriptor.getInstance().getConfig();

  private static LocalConfigNode configNode;
  private static Coordinator coordinator;
  private static ISchemaFetcher schemaFetcher;
  private static IPartitionFetcher partitionFetcher;

  @Before
  public void setUp() throws Exception {
    conf.setDataNodeId(0);
    coordinator = Coordinator.getInstance();
    schemaFetcher = StandaloneSchemaFetcher.getInstance();
    partitionFetcher = StandalonePartitionFetcher.getInstance();

    configNode = LocalConfigNode.getInstance();
    configNode.init();
    WALManager.getInstance().start();
    FlushManager.getInstance().start();
    StorageEngineV2.getInstance().start();
  }

  @After
  public void tearDown() throws Exception {
    configNode.clear();
    WALManager.getInstance().clear();
    WALRecoverManager.getInstance().clear();
    WALManager.getInstance().stop();
    StorageEngineV2.getInstance().stop();
    FlushManager.getInstance().stop();
    EnvironmentUtils.cleanAllDir();
    conf.setDataNodeId(-1);
  }

  @Test
  public void testCreateTimeseriesAndQuery() throws IllegalPathException {
    CreateTimeSeriesStatement createTimeSeriesStatement = new CreateTimeSeriesStatement();
    createTimeSeriesStatement.setPath(new PartialPath("root.ln.wf01.wt01.status"));
    createTimeSeriesStatement.setDataType(TSDataType.BOOLEAN);
    createTimeSeriesStatement.setEncoding(TSEncoding.PLAIN);
    createTimeSeriesStatement.setCompressor(CompressionType.SNAPPY);
    createTimeSeriesStatement.setAlias("meter1");
    createTimeSeriesStatement.setAttributes(
        new HashMap<String, String>() {
          {
            put("attr1", "a1");
            put("attr2", "a2");
          }
        });
    createTimeSeriesStatement.setProps(
        new HashMap<String, String>() {
          {
            put("MAX_POINT_NUMBER", "3");
          }
        });
    createTimeSeriesStatement.setTags(
        new HashMap<String, String>() {
          {
            put("tag1", "v1");
            put("tag2", "v2");
          }
        });
    executeStatement(createTimeSeriesStatement);
  }

  @Test
  public void testInsertData() {

    String insertSql = "insert into root.sg.d1(time,s1,s2) values (100,222,333)";
    Statement insertStmt = StatementGenerator.createStatement(insertSql, ZoneId.systemDefault());
    executeStatement(insertStmt);
  }

  @Test
  public void createUser() {
    String createUserSql = "create user username 'password'";
    Statement createStmt =
        StatementGenerator.createStatement(createUserSql, ZoneId.systemDefault());
    executeStatement(createStmt);
  }

  private void executeStatement(Statement statement) {
    long queryId = SessionManager.getInstance().requestQueryId();
    ExecutionResult executionResult =
        coordinator.execute(
            statement,
            queryId,
            new SessionInfo(0, "root", "+5:00"),
            "",
            partitionFetcher,
            schemaFetcher,
            conf.getQueryTimeoutThreshold());
    try {
      int statusCode = executionResult.status.getCode();
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), statusCode);
    } finally {
      coordinator.cleanupQueryExecution(queryId);
    }
  }
}
