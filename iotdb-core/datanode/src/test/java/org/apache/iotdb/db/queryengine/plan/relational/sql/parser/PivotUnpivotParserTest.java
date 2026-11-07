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

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.PivotRelation;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.QuerySpecification;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.UnpivotRelation;
import org.apache.iotdb.db.protocol.session.InternalClientSession;

import org.junit.Test;

import java.time.ZoneId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PivotUnpivotParserTest {
  private final SqlParser parser = new SqlParser();
  private final InternalClientSession session = new InternalClientSession("parser-test");

  private Query parse(String sql) {
    return (Query) parser.createStatement(sql, ZoneId.systemDefault(), session);
  }

  @Test
  public void testSimplifiedPivotInCteAndLimit() {
    Query query =
        parse(
            "WITH p AS (PIVOT metrics ON region IN ('north' AS n, NULL), status "
                + "USING AVG(temperature) AS avg_temp GROUP BY device_id) "
                + "SELECT * FROM p ORDER BY device_id LIMIT 1");
    PivotRelation pivot =
        (PivotRelation) query.getWith().get().getQueries().get(0).getQuery().getQueryBody();
    assertTrue(pivot.isDynamic());
    assertEquals(2, pivot.getOnItems().size());
    assertEquals(2, pivot.getOnItems().get(0).values().get().size());
    assertEquals(1, pivot.getGroupBy().get().size());
    assertTrue(((QuerySpecification) query.getQueryBody()).getLimit().isPresent());
  }

  @Test
  public void testSqlStylePivotAndUnpivot() {
    Query pivotQuery =
        parse(
            "SELECT * FROM metrics PIVOT (AVG(temperature), MAX(temperature) FOR "
                + "region IN ('north', 'south') status IN ('on', 'off') GROUP BY device_id)");
    PivotRelation pivot =
        (PivotRelation) ((QuerySpecification) pivotQuery.getQueryBody()).getFrom().get();
    assertFalse(pivot.isDynamic());
    assertEquals(2, pivot.getUsingItems().size());
    assertEquals(2, pivot.getOnItems().size());

    Query unpivotQuery =
        parse(
            "SELECT * FROM snapshots UNPIVOT INCLUDE NULLS "
                + "((temperature, humidity) FOR sample IN "
                + "((temperature_current, humidity_current) AS current, "
                + "(temperature_previous, humidity_previous) AS previous))");
    UnpivotRelation unpivot =
        (UnpivotRelation) ((QuerySpecification) unpivotQuery.getQueryBody()).getFrom().get();
    assertTrue(unpivot.isIncludeNulls());
    assertEquals(2, unpivot.getValueColumns().size());
    assertEquals(2, unpivot.getItems().size());
  }

  @Test
  public void testSimplifiedUnpivotAndColumns() {
    Query query =
        parse(
            "UNPIVOT (SELECT time, device_id, temperature FROM metrics) "
                + "ON COLUMNS(* EXCLUDE (time, device_id)) "
                + "INTO NAME measurement VALUE value ORDER BY measurement LIMIT 2");
    UnpivotRelation unpivot = (UnpivotRelation) query.getQueryBody();
    assertEquals(2, unpivot.getItems().get(0).excluded().get().size());
    assertEquals("measurement", unpivot.getNameColumn().getValue());
    assertTrue(query.getOrderBy().isPresent());
    assertTrue(query.getLimit().isPresent());
  }

  @Test
  public void testNonReservedIdentifiers() {
    parse("SELECT pivot, unpivot, include, exclude, name FROM pivot");
    parse("SELECT PiVoT, UnPiVoT, InClUdE, ExClUdE, NaMe FROM PiVoT");
    parse("SELECT * FROM metrics pivot");
    parse("SELECT * FROM metrics unpivot");
    parse("SELECT * FROM metrics AS UnPiVoT");
    parse("SELECT PiVoT AS InClUdE FROM metrics AS NaMe");
    parse("SELECT * FROM exclude(1)");
    parse("SELECT * FROM metrics AS PIVOT");
    parse("PIVOT(SELECT region,temp FROM metrics) ON region IN ('n') USING SUM(temp)");
  }
}
