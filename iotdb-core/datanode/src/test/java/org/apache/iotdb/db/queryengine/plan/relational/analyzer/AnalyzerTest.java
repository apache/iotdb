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

package org.apache.iotdb.db.queryengine.plan.relational.analyzer;

import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.relational.function.BoundSignature;
import org.apache.iotdb.db.queryengine.plan.relational.function.FunctionId;
import org.apache.iotdb.db.queryengine.plan.relational.function.FunctionKind;
import org.apache.iotdb.db.queryengine.plan.relational.function.OperatorType;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnHandle;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.OperatorNotFoundException;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ResolvedFunction;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableHandle;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableSchema;
import org.apache.iotdb.db.queryengine.plan.relational.security.AccessControl;
import org.apache.iotdb.db.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.relational.sql.tree.Statement;

import org.junit.Test;
import org.mockito.Mockito;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.iotdb.db.queryengine.execution.warnings.WarningCollector.NOOP;
import static org.apache.iotdb.tsfile.read.common.type.BooleanType.BOOLEAN;
import static org.apache.iotdb.tsfile.read.common.type.IntType.INT32;
import static org.apache.iotdb.tsfile.read.common.type.LongType.INT64;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.eq;

public class AnalyzerTest {

  private final SqlParser sqlParser = new SqlParser();

  private final NopAccessControl nopAccessControl = new NopAccessControl();

  @Test
  public void testMockQuery() throws OperatorNotFoundException {
    String sql =
        "SELECT s1, (s1 + 1) as t from table1 where time > 100 and s2 > 10 offset 2 limit 3";
    Metadata metadata = Mockito.mock(Metadata.class);
    Mockito.when(metadata.tableExists(Mockito.any())).thenReturn(true);

    TableHandle tableHandle = Mockito.mock(TableHandle.class);
    Mockito.when(
            metadata.getTableHandle(Mockito.any(), eq(new QualifiedObjectName("testdb", "table1"))))
        .thenReturn(Optional.of(tableHandle));

    Map<String, ColumnHandle> map = new HashMap<>();
    TableSchema tableSchema = Mockito.mock(TableSchema.class);
    Mockito.when(tableSchema.getTableName()).thenReturn("table1");
    ColumnSchema column1 =
        ColumnSchema.builder().setName("time").setType(INT64).setHidden(false).build();
    ColumnHandle column1Handle = Mockito.mock(ColumnHandle.class);
    map.put("time", column1Handle);
    ColumnSchema column2 =
        ColumnSchema.builder().setName("s1").setType(INT32).setHidden(false).build();
    ColumnHandle column2Handle = Mockito.mock(ColumnHandle.class);
    map.put("s1", column2Handle);
    ColumnSchema column3 =
        ColumnSchema.builder().setName("s2").setType(INT64).setHidden(false).build();
    ColumnHandle column3Handle = Mockito.mock(ColumnHandle.class);
    map.put("s2", column3Handle);
    List<ColumnSchema> columnSchemaList = Arrays.asList(column1, column2, column3);
    Mockito.when(tableSchema.getColumns()).thenReturn(columnSchemaList);

    Mockito.when(metadata.getTableSchema(Mockito.any(), eq(tableHandle))).thenReturn(tableSchema);
    Mockito.when(metadata.getColumnHandles(Mockito.any(), eq(tableHandle))).thenReturn(map);

    ResolvedFunction lLessThanI =
        new ResolvedFunction(
            new BoundSignature("l<i", BOOLEAN, Arrays.asList(INT64, INT32)),
            new FunctionId("l<i"),
            FunctionKind.SCALAR,
            true);

    ResolvedFunction iAddi =
        new ResolvedFunction(
            new BoundSignature("l+i", INT64, Arrays.asList(INT32, INT32)),
            new FunctionId("l+i"),
            FunctionKind.SCALAR,
            true);

    Mockito.when(
            metadata.resolveOperator(eq(OperatorType.LESS_THAN), eq(Arrays.asList(INT64, INT32))))
        .thenReturn(lLessThanI);
    Mockito.when(metadata.resolveOperator(eq(OperatorType.ADD), eq(Arrays.asList(INT32, INT32))))
        .thenReturn(iAddi);

    Analysis actualAnalysis = analyzeSQL(sql, metadata);
    assertNotNull(actualAnalysis);
    System.out.println(actualAnalysis.getTypes());
  }

  @Test
  public void testSingleTableQuery() throws OperatorNotFoundException {
    String sql =
        "SELECT tag1 as tmp_tag, tag2, attribute1, s1+1 as add_s1, s2 FROM table1 "
            + "WHERE time>1 AND tag1=\"A\" and tag3=\"B\" AND s1=1 AND s3=3 ORDER BY time DESC OFFSET 10 LIMIT 5";
    Metadata metadata = new TestMatadata();

    TableHandle tableHandle = Mockito.mock(TableHandle.class);
    Mockito.when(
            metadata.getTableHandle(Mockito.any(), eq(new QualifiedObjectName("testdb", "table1"))))
        .thenReturn(Optional.of(tableHandle));

    Map<String, ColumnHandle> map = new HashMap<>();
    TableSchema tableSchema = Mockito.mock(TableSchema.class);
    Mockito.when(tableSchema.getTableName()).thenReturn("table1");
    ColumnSchema column1 =
        ColumnSchema.builder().setName("time").setType(INT64).setHidden(false).build();
    ColumnHandle column1Handle = Mockito.mock(ColumnHandle.class);
    map.put("time", column1Handle);
    ColumnSchema column2 =
        ColumnSchema.builder().setName("s1").setType(INT32).setHidden(false).build();
    ColumnHandle column2Handle = Mockito.mock(ColumnHandle.class);
    map.put("s1", column2Handle);
    ColumnSchema column3 =
        ColumnSchema.builder().setName("s2").setType(INT64).setHidden(false).build();
    ColumnHandle column3Handle = Mockito.mock(ColumnHandle.class);
    map.put("s2", column3Handle);
    List<ColumnSchema> columnSchemaList = Arrays.asList(column1, column2, column3);
    Mockito.when(tableSchema.getColumns()).thenReturn(columnSchemaList);

    Mockito.when(metadata.getTableSchema(Mockito.any(), eq(tableHandle))).thenReturn(tableSchema);
    Mockito.when(metadata.getColumnHandles(Mockito.any(), eq(tableHandle))).thenReturn(map);

    ResolvedFunction lLessThanI =
        new ResolvedFunction(
            new BoundSignature("l<i", BOOLEAN, Arrays.asList(INT64, INT32)),
            new FunctionId("l<i"),
            FunctionKind.SCALAR,
            true);

    ResolvedFunction iAddi =
        new ResolvedFunction(
            new BoundSignature("l+i", INT64, Arrays.asList(INT32, INT32)),
            new FunctionId("l+i"),
            FunctionKind.SCALAR,
            true);

    Mockito.when(
            metadata.resolveOperator(eq(OperatorType.LESS_THAN), eq(Arrays.asList(INT64, INT32))))
        .thenReturn(lLessThanI);
    Mockito.when(metadata.resolveOperator(eq(OperatorType.ADD), eq(Arrays.asList(INT32, INT32))))
        .thenReturn(iAddi);

    Analysis actualAnalysis = analyzeSQL(sql, metadata);
    assertNotNull(actualAnalysis);
    System.out.println(actualAnalysis.getTypes());
  }

  private Analysis analyzeSQL(String sql, Metadata metadata) {
    try {
      Statement statement = sqlParser.createStatement(sql);
      SessionInfo session = new SessionInfo(0, "test", ZoneId.systemDefault(), "testdb");
      StatementAnalyzerFactory statementAnalyzerFactory =
          new StatementAnalyzerFactory(metadata, sqlParser, nopAccessControl);

      Analyzer analyzer =
          new Analyzer(
              session,
              statementAnalyzerFactory,
              Collections.emptyList(),
              Collections.emptyMap(),
              NOOP);
      return analyzer.analyze(statement);
    } catch (Exception e) {
      e.printStackTrace();
      fail(sql + ", " + e.getMessage());
    }
    fail();
    return null;
  }

  private static class NopAccessControl implements AccessControl {}
}
