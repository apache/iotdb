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

package org.apache.iotdb.db.queryengine.plan.analyze.schema;

import org.apache.iotdb.commons.exception.auth.AccessDeniedException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.common.SqlDialect;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.security.AccessControl;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InsertRows;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.time.ZoneId;
import java.util.Arrays;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;

public class SchemaValidatorTest {

  private static final String USER = "pipe-user";

  @Test
  public void testAllInsertRowsTablesCheckedBeforeSchemaValidation() {
    final MPPQueryContext context =
        new MPPQueryContext(
            "",
            new QueryId("check_all_insert_rows_tables"),
            new SessionInfo(1L, USER, ZoneId.systemDefault(), "db1", SqlDialect.TABLE),
            null,
            null);
    final InsertRowsStatement statement = new InsertRowsStatement();
    statement.setWriteToTable(true);
    statement.setInsertRowStatementList(
        Arrays.asList(
            createInsertRowStatement("db1", "table1"),
            createInsertRowStatement("db1", "table2"),
            createInsertRowStatement("db1", "table1")));

    final Metadata metadata = Mockito.mock(Metadata.class);
    final AccessControl accessControl = Mockito.mock(AccessControl.class);
    final QualifiedObjectName table1 = new QualifiedObjectName("db1", "table1");
    final QualifiedObjectName table2 = new QualifiedObjectName("db1", "table2");
    Mockito.doThrow(new AccessDeniedException("denied"))
        .when(accessControl)
        .checkCanInsertIntoTable(eq(USER), eq(table2), same(context));

    Assert.assertThrows(
        AccessDeniedException.class,
        () ->
            SchemaValidator.validate(
                metadata, new InsertRows(statement, context), context, accessControl));

    final InOrder inOrder = Mockito.inOrder(accessControl);
    inOrder.verify(accessControl).checkCanInsertIntoTable(eq(USER), eq(table1), same(context));
    inOrder.verify(accessControl).checkCanInsertIntoTable(eq(USER), eq(table2), same(context));
    verify(accessControl, times(1)).checkCanInsertIntoTable(eq(USER), eq(table1), same(context));
    verifyNoMoreInteractions(accessControl);
    verifyZeroInteractions(metadata);
  }

  private static InsertRowStatement createInsertRowStatement(
      final String database, final String table) {
    final InsertRowStatement statement = new InsertRowStatement();
    statement.setWriteToTable(true);
    statement.setDatabaseName(database);
    statement.setDevicePath(new PartialPath(table, false));
    return statement;
  }
}
