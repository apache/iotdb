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

package org.apache.iotdb.db.queryengine.plan.relational.sql.parser;

import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.parser.ParsingException;
import org.apache.iotdb.commons.schema.table.TableType;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.InternalClientSession;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AddColumn;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ColumnDefinition;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateWritableView;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowTables;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ViewFieldDefinition;

import org.junit.Test;

import java.time.ZonedDateTime;
import java.util.EnumSet;

import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.ATTRIBUTE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class WritableViewStatementTest {

  private final SqlParser parser = new SqlParser();
  private final IClientSession clientSession = new InternalClientSession("internal");

  @Test
  public void testCreateWritableViewTracksExplicitCommentsSeparately() {
    clientSession.setDatabaseName("test");

    final CreateWritableView statement =
        (CreateWritableView)
            parser.createStatement(
                "create writable view target_view as select a comment 'view_a', b from source_table "
                    + "with (schema_cascade=true)",
                ZonedDateTime.now().getOffset(),
                clientSession);

    assertEquals("view_a", statement.getViewColumnCommentMap().get("a"));
    assertFalse(statement.getViewColumnCommentMap().containsKey("b"));
  }

  @Test
  public void testCreateWritableViewUsesTsTableCommentField() {
    clientSession.setDatabaseName("test");

    final CreateWritableView statement =
        (CreateWritableView)
            parser.createStatement(
                "create writable view target_view as select * from source_table comment 'view comment' "
                    + "with (schema_cascade=true)",
                ZonedDateTime.now().getOffset(),
                clientSession);

    assertEquals("view comment", statement.getComment());
    assertNull(statement.getViewColumnCommentMap());
  }

  @Test
  public void testCreateWritableViewRejectsDuplicatedTargetColumns() {
    clientSession.setDatabaseName("test");

    final SemanticException exception =
        assertThrows(
            SemanticException.class,
            () ->
                parser.createStatement(
                    "create writable view target_view as select a as col_1, b as col_1 "
                        + "from source_table",
                    ZonedDateTime.now().getOffset(),
                    clientSession));

    assertEquals(
        "The view column col_1 cannot be mapped from multiple source columns a and b",
        exception.getMessage());
  }

  @Test
  public void testAlterWritableViewAddColumnUsesWritableColumnDefinition() {
    clientSession.setDatabaseName("test");

    final AddColumn alterTableStatement =
        (AddColumn)
            parser.createStatement(
                "alter table target_view add column source_col as view_col comment 'view col'",
                ZonedDateTime.now().getOffset(),
                clientSession);
    assertFalse(alterTableStatement.isView());
    assertTrue(alterTableStatement.getWritableViewColumn().isPresent());
    assertEquals(
        "source_col", alterTableStatement.getWritableViewColumn().get().getSourceColumnName());
    assertEquals("view_col", alterTableStatement.getWritableViewColumn().get().getViewColumnName());
    assertEquals("view col", alterTableStatement.getWritableViewColumn().get().getComment());

    final AddColumn alterViewStatement =
        (AddColumn)
            parser.createStatement(
                "alter view target_view add column source_col as view_col",
                ZonedDateTime.now().getOffset(),
                clientSession);
    assertTrue(alterViewStatement.isView());
    assertTrue(alterViewStatement.getWritableViewColumn().isPresent());
  }

  @Test
  public void testAlterViewCanParseAttributeColumnWithViewSyntax() {
    clientSession.setDatabaseName("test");

    final AddColumn statement =
        (AddColumn)
            parser.createStatement(
                "alter view target_view add column label attribute",
                ZonedDateTime.now().getOffset(),
                clientSession);

    final ColumnDefinition columnDefinition = statement.getColumn();
    assertTrue(statement.isView());
    assertEquals("label", columnDefinition.getName().getValue());
    assertEquals(ATTRIBUTE, columnDefinition.getColumnCategory());
  }

  @Test
  public void testAlterViewAddColumnTracksExplicitFromSyntax() {
    clientSession.setDatabaseName("test");

    final AddColumn statement =
        (AddColumn)
            parser.createStatement(
                "alter view target_view add column view_col int32 field from source_col",
                ZonedDateTime.now().getOffset(),
                clientSession);

    assertTrue(statement.isView());
    assertTrue(statement.getColumn() instanceof ViewFieldDefinition);
    assertTrue(((ViewFieldDefinition) statement.getColumn()).isExplicitFrom());
  }

  @Test
  public void testShowViewsUsesExpectedTableTypeFilters() {
    clientSession.setDatabaseName("test");

    final ShowTables showView =
        (ShowTables)
            parser.createStatement("show views", ZonedDateTime.now().getOffset(), clientSession);
    final ShowTables showTreeView =
        (ShowTables)
            parser.createStatement(
                "show tree views", ZonedDateTime.now().getOffset(), clientSession);
    final ShowTables showWritableView =
        (ShowTables)
            parser.createStatement(
                "show writable views", ZonedDateTime.now().getOffset(), clientSession);

    assertEquals(
        EnumSet.of(TableType.VIEW_FROM_TREE, TableType.WRITABLE_VIEW),
        showView.getTableTypeFilter().get());
    assertEquals(EnumSet.of(TableType.VIEW_FROM_TREE), showTreeView.getTableTypeFilter().get());
    assertEquals(EnumSet.of(TableType.WRITABLE_VIEW), showWritableView.getTableTypeFilter().get());
  }

  @Test
  public void testShowViewRejectsSingularSyntax() {
    clientSession.setDatabaseName("test");

    assertThrows(
        ParsingException.class,
        () -> parser.createStatement("show view", ZonedDateTime.now().getOffset(), clientSession));
    assertThrows(
        ParsingException.class,
        () ->
            parser.createStatement(
                "show tree view", ZonedDateTime.now().getOffset(), clientSession));
    assertThrows(
        ParsingException.class,
        () ->
            parser.createStatement(
                "show writable view", ZonedDateTime.now().getOffset(), clientSession));
  }
}
