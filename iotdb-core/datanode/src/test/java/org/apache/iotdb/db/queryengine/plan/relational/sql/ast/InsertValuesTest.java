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

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.common.SqlDialect;
import org.apache.iotdb.commons.queryengine.plan.relational.type.InternalTypeManager;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.FieldColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TagColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TimeColumnSchema;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.protocol.session.InternalClientSession;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl;
import org.apache.iotdb.db.queryengine.plan.relational.planner.PlannerContext;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.enums.TSDataType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.ZoneId;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class InsertValuesTest {
  private static final String DATABASE = "insert_values_test";
  private static final String TABLE = "values_table";
  private static final ZoneId ZONE = ZoneId.of("UTC");
  private final PlannerContext plannerContext =
      new PlannerContext(new TableMetadataImpl(), new InternalTypeManager());
  private final SessionInfo session = new SessionInfo(0, "root", ZONE, DATABASE, SqlDialect.TABLE);

  @Before
  public void setUp() {
    TsTable table = new TsTable(TABLE);
    table.addColumnSchema(new TimeColumnSchema("time", TSDataType.TIMESTAMP));
    table.addColumnSchema(new TagColumnSchema("device_id", TSDataType.STRING));
    table.addColumnSchema(new FieldColumnSchema("value", TSDataType.INT32));
    cacheTable(table);
  }

  @After
  public void tearDown() {
    DataNodeTableCache.getInstance().invalid(DATABASE);
  }

  @Test
  public void testFoldedRowsRetainUniformTargets() {
    for (String columns : new String[] {" (time, device_id, value)", ""}) {
      InsertValues insert = parse(columns + " VALUES (1 + 1, 'd1', 2 + 3), (2 + 1, 'd2', 3 + 4)");
      insert.materialize(plannerContext, session);
      InsertRowsStatement statement = insert.getInnerTreeStatement();
      assertTrue(statement.isUniformTargets());
      assertEquals(2, statement.getInsertRowStatementList().size());
      for (int i = 0; i < 2; i++) {
        InsertRowStatement row = statement.getInsertRowStatementList().get(i);
        assertEquals(i + 2, row.getTime());
        assertArrayEquals(new String[] {"device_id", "value"}, row.getMeasurements());
        assertEquals(i * 2 + 5, ((Number) row.getValues()[1]).intValue());
      }
    }
  }

  @Test
  public void testFoldedCustomTimeColumn() {
    cacheTable(
        new TsTable(
            TABLE,
            ImmutableList.of(
                new TagColumnSchema("device_id", TSDataType.STRING),
                new TimeColumnSchema("event_time", TSDataType.TIMESTAMP),
                new FieldColumnSchema("value", TSDataType.INT32))));
    for (String columns : new String[] {" (device_id, event_time, value)", ""}) {
      InsertValues insert = parse(columns + " VALUES ('d1', 10 + 5, 20 + 2)");
      insert.materialize(plannerContext, session);
      InsertRowStatement row = insert.getInnerTreeStatement().getInsertRowStatementList().get(0);
      assertEquals(15, row.getTime());
      assertEquals(22, ((Number) row.getValues()[1]).intValue());
    }
  }

  @Test
  public void testFoldedNumericInPredicates() {
    for (String columns : new String[] {" (time, device_id, value)", ""}) {
      for (String predicate :
          new String[] {"1 + 1 IN (2, 3)", "2 IN (1 + 1, 3)", "1 + 1 IN (2.0, 3.0)"}) {
        InsertValues insert = parse(columns + " VALUES (1, 'd1', if(" + predicate + ", 11, 22))");
        insert.materialize(plannerContext, session);
        InsertRowStatement row = insert.getInnerTreeStatement().getInsertRowStatementList().get(0);
        assertEquals(predicate, 11, ((Number) row.getValues()[1]).intValue());
      }
    }
  }

  @Test
  public void testFoldedNaNComparisons() {
    for (String columns : new String[] {" (time, device_id, value)", ""}) {
      for (String predicate :
          new String[] {
            "1.0 < (0.0 / 0.0)",
            "(0.0 / 0.0) >= 1.0",
            "(0.0 / 0.0) = (0.0 / 0.0)",
            "(0.0 / 0.0) <> 1.0"
          }) {
        InsertValues insert = parse(columns + " VALUES (1, 'd1', if(" + predicate + ", 11, 22))");
        insert.materialize(plannerContext, session);
        InsertRowStatement row = insert.getInnerTreeStatement().getInsertRowStatementList().get(0);
        assertEquals(predicate, 22, ((Number) row.getValues()[1]).intValue());
      }
    }
  }

  @Test
  public void testUnboundParametersRemainSemanticErrors() {
    for (String columns : new String[] {" (time, device_id, value)", ""}) {
      for (String values :
          new String[] {" (?, 'd1', 1)", " (1, 'd1', 1 + ?)", " (1, 'd1', 1), (?, 'd2', 2)"}) {
        InsertValues insert = parse(columns + " VALUES" + values);
        SemanticException exception =
            assertThrows(
                SemanticException.class, () -> insert.materialize(plannerContext, session));
        assertTrue(exception.getMessage().startsWith(DataNodeQueryMessages.UNSUPPORTED_EXPRESSION));
        assertTrue(insert.getInnerTreeStatement().getInsertRowStatementList().isEmpty());
      }
    }
  }

  private InsertValues parse(String suffix) {
    return (InsertValues)
        new SqlParser()
            .createStatement(
                "INSERT INTO " + DATABASE + "." + TABLE + suffix,
                ZONE,
                new InternalClientSession("insert_values_test"));
  }

  private void cacheTable(TsTable table) {
    DataNodeTableCache.getInstance().preUpdateTable(DATABASE, table, null);
    DataNodeTableCache.getInstance().commitUpdateTable(DATABASE, TABLE, null);
  }
}
