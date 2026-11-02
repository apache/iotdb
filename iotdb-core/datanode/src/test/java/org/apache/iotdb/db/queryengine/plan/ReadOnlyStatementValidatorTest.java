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

package org.apache.iotdb.db.queryengine.plan;

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.protocol.session.ClientSession;
import org.apache.iotdb.db.protocol.session.PreparedStatementInfo;
import org.apache.iotdb.db.queryengine.plan.parser.StatementGenerator;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.internal.DeviceSchemaFetchStatement;
import org.apache.iotdb.db.queryengine.plan.statement.internal.InternalBatchActivateTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.internal.InternalCreateMultiTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.internal.SeriesSchemaFetchStatement;
import org.apache.iotdb.db.queryengine.plan.statement.pipe.PipeEnrichedStatement;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.ZoneOffset;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ReadOnlyStatementValidatorTest {
  private final CommonConfig config = CommonDescriptor.getInstance().getConfig();
  private final SqlParser parser = new SqlParser();
  private final ClientSession session = new ClientSession(null);
  private NodeStatus originalStatus;
  private String originalReason;

  @Before
  public void setUp() {
    originalStatus = config.getNodeStatus();
    originalReason = config.getStatusReason();
    config.setNodeStatusWithReason(NodeStatus.ReadOnly, NodeStatus.MANUAL);
    session.setZoneId(ZoneOffset.UTC);
  }

  @After
  public void tearDown() {
    config.setNodeStatusWithReason(originalStatus, originalReason);
  }

  @Test
  public void testTreeReads() {
    for (String sql :
        new String[] {
          "show databases",
          "show timeseries root.**",
          "show devices root.**",
          "count timeseries root.**",
          "count devices root.**",
          "show ttl on root.**",
          "show pipes",
          "show pipeplugins",
          "show functions",
          "show triggers",
          "show schema templates",
          "show cluster",
          "show datanodes",
          "show confignodes",
          "show variables",
          "show version",
          "show current_user",
          "show queries",
          "show disk_usage from root.**",
          "list user",
          "list role",
          "select s from root.db.d",
          "explain select s from root.db.d",
          "explain analyze select s from root.db.d"
        }) {
      ReadOnlyStatementValidator.check(tree(sql));
    }
  }

  @Test
  public void testInternalSchemaReadsAndWrites() throws Exception {
    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(new PartialPath("root.db.**"));
    patternTree.constructTree();
    ReadOnlyStatementValidator.check(
        new SeriesSchemaFetchStatement(
            patternTree, Collections.emptyMap(), false, false, true, false, false));
    ReadOnlyStatementValidator.check(
        new DeviceSchemaFetchStatement(patternTree, patternTree, false));
    assertRejected(
        () ->
            ReadOnlyStatementValidator.check(
                new InternalCreateMultiTimeSeriesStatement(Collections.emptyMap())));
    assertRejected(
        () ->
            ReadOnlyStatementValidator.check(
                new InternalBatchActivateTemplateStatement(Collections.emptyMap())));
  }

  @Test
  public void testTableReads() {
    for (String sql :
        new String[] {
          "show databases",
          "count databases",
          "show tables from db",
          "show tables details from db",
          "describe db.t",
          "describe db.t details",
          "show create database db",
          "show create table db.t",
          "show pipes",
          "show pipeplugins",
          "show functions",
          "show cluster",
          "show datanodes",
          "show confignodes",
          "show variables",
          "show version",
          "show current_user",
          "show current_database",
          "show current_sql_dialect",
          "show devices from db.t",
          "count devices from db.t",
          "list user",
          "list role",
          "select * from db.t",
          "select * from information_schema.tables",
          "explain select * from db.t",
          "explain analyze select * from db.t"
        }) {
      checkTable(sql);
    }
  }

  @Test
  public void testDiskRecoveryAndSessionOperations() {
    ReadOnlyStatementValidator.check(tree("delete database root.db"));
    ReadOnlyStatementValidator.check(tree("set ttl to root.db.** 1"));
    ReadOnlyStatementValidator.check(tree("set system to running on local"));
    ReadOnlyStatementValidator.check(tree("set sql_dialect=table"));
    checkTable("drop database db");
    checkTable("alter database db set properties ttl=1");
    checkTable("alter table db.t set properties ttl=1");
    checkTable("set system to running on local");
    checkTable("use db");
    checkTable("kill query '20250206_093300_00001_1'");
    checkTable("set sql_dialect=tree");
    checkTable("prepare q from select * from db.t");
    checkTable("deallocate prepare q");
  }

  @Test
  public void testTreeWritesAreRejected() {
    for (String sql :
        new String[] {
          "insert into root.db.d(time,s) values(1,1)",
          "delete from root.db.d.s where time<=1",
          "select s into root.dest.d(s) from root.db.d",
          "explain analyze select s into root.dest.d(s) from root.db.d",
          "create database root.newdb",
          "create timeseries root.db.d.s with datatype=INT32,encoding=PLAIN",
          "delete timeseries root.db.d.s",
          "create user readonlyuser 'IoTDB!Test2024'",
          "flush",
          "start repair data",
          "unset ttl to root.db.**",
          "set configuration 'query_timeout_threshold'='10000'"
        }) {
      assertRejected(sql, () -> ReadOnlyStatementValidator.check(tree(sql)));
    }
    assertRejected(
        () ->
            ReadOnlyStatementValidator.check(
                new PipeEnrichedStatement(tree("create database root.newdb"))));
  }

  @Test
  public void testTableWritesAreRejected() {
    for (String sql :
        new String[] {
          "insert into db.t select * from db.source",
          "insert into db.t(time,s) select * from db.source",
          "delete from db.t where time<=1",
          "update db.t set a='new' where tag='d'",
          "create database newdb",
          "create table db.newtable(tag STRING TAG,s INT32 FIELD)",
          "drop table db.t",
          "alter table db.t add column s2 INT32 FIELD",
          "alter database db set properties ttl=1, schema_region_group_num=2",
          "alter table db.t set properties ttl=1, comment='changed'",
          "alter table db.t set properties ttl=default",
          "create role readonlyrole",
          "flush",
          "set configuration query_timeout_threshold='10000'",
          "explain analyze execute immediate 'insert into db.t select * from db.source'"
        }) {
      assertRejected(sql, () -> checkTable(sql));
    }
  }

  @Test
  public void testPreparedExecutionChecksTheInnerStatement() {
    session.addPreparedStatement(
        "q", new PreparedStatementInfo("q", table("select * from db.t"), 0));
    session.addPreparedStatement(
        "w", new PreparedStatementInfo("w", table("insert into db.t select * from db.source"), 0));
    checkTable("execute q");
    checkTable("explain analyze execute q");
    checkTable("execute immediate 'select * from db.t'");
    assertRejected(() -> checkTable("execute w"));
    assertRejected(() -> checkTable("explain analyze execute w"));
    assertRejected(
        () -> checkTable("execute immediate 'insert into db.t select * from db.source'"));
    session.addPreparedStatement(
        "cycle", new PreparedStatementInfo("cycle", table("execute cycle"), 0));
    assertRejected(() -> checkTable("execute cycle"));
  }

  @Test
  public void testRunningModeKeepsExistingBehavior() {
    config.setNodeStatus(NodeStatus.Running);
    ReadOnlyStatementValidator.check(tree("create database root.db"));
    checkTable("create database db");
  }

  private Statement tree(String sql) {
    return StatementGenerator.createStatement(sql, ZoneOffset.UTC);
  }

  private org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement table(String sql) {
    return parser.createStatement(sql, ZoneOffset.UTC, session);
  }

  private void checkTable(String sql) {
    ReadOnlyStatementValidator.check(table(sql), parser, session);
  }

  private void assertRejected(Runnable action) {
    assertRejected("", action);
  }

  private void assertRejected(String sql, Runnable action) {
    try {
      action.run();
      fail("Expected SYSTEM_READ_ONLY for " + sql);
    } catch (IoTDBRuntimeException e) {
      assertEquals(
          sql + ": " + e.getMessage(),
          TSStatusCode.SYSTEM_READ_ONLY.getStatusCode(),
          e.getErrorCode());
    }
  }
}
