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

package org.apache.iotdb.db.queryengine.plan.statement.metadata;

import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.parser.StatementGenerator;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.ZoneId;

public class CreateContinuousQueryStatementTest {

  private long originalMinimumEvery;

  @Before
  public void setUp() {
    originalMinimumEvery =
        IoTDBDescriptor.getInstance().getConfig().getContinuousQueryMinimumEveryInterval();
  }

  @After
  public void tearDown() {
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setContinuousQueryMinimumEveryInterval(originalMinimumEvery);
  }

  @Test
  public void calendarEveryAndRangeAreAccepted() {
    CreateContinuousQueryStatement statement = parse(calendarSql("1mo", "1mo"));
    statement.semanticCheck();
    Assert.assertEquals(1, statement.getEveryDuration().monthDuration);
    Assert.assertEquals(0, statement.getEveryDuration().nonMonthDuration);
    Assert.assertEquals(1, statement.getStartTimeOffsetDuration().monthDuration);
  }

  @Test
  public void inheritedGroupByMonthKeepsCalendarEvery() {
    CreateContinuousQueryStatement statement =
        parse(
            "CREATE CQ cq_inherited BEGIN "
                + "SELECT max_value(s1) INTO root.sg.d1(s1_max) FROM root.sg.d1 GROUP BY(1mo) END");
    statement.semanticCheck();
    Assert.assertEquals(1, statement.getEveryDuration().monthDuration);
    Assert.assertEquals(1, statement.getStartTimeOffsetDuration().monthDuration);
  }

  @Test
  public void compoundCalendarDurationDominatesAndIsAccepted() {
    parse(calendarSql("1mo", "1mo3d")).semanticCheck();
  }

  @Test
  public void incomparableCalendarAndFixedDurationsAreRejected() {
    try {
      parse(calendarSql("1mo", "30d")).semanticCheck();
      Assert.fail("expected incomparable RANGE vs EVERY to be rejected");
    } catch (SemanticException e) {
      Assert.assertTrue(
          e.getMessage()
              .contains("The start time offset should be greater than or equal to every interval"));
    }
  }

  @Test
  public void monthAndYearAliasesAreRejectedByTheParser() {
    assertParseFails(calendarSql("1month", "1month"));
    assertParseFails(calendarSql("1year", "1year"));
    assertParseFails(calendarSql("2months", "2months"));
    assertParseFails(calendarSql("2years", "2years"));
  }

  @Test
  public void inheritedGroupByYearKeepsCalendarEvery() {
    CreateContinuousQueryStatement statement =
        parse(
            "CREATE CQ cq_inherited_year BEGIN "
                + "SELECT max_value(s1) INTO root.sg.d1(s1_max) FROM root.sg.d1 GROUP BY(1y) END");
    statement.semanticCheck();
    Assert.assertEquals(12, statement.getEveryDuration().monthDuration);
    Assert.assertEquals(12, statement.getStartTimeOffsetDuration().monthDuration);
  }

  @Test
  public void calendarMinimumEveryUsesElapsedLowerBound() {
    // 1mo lower bound is M * 28d - 36h. Reject when the configured minimum exceeds that bound.
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setContinuousQueryMinimumEveryInterval(2_289_600_001L);
    try {
      parse(calendarSql("1mo", "1mo")).semanticCheck();
      Assert.fail("expected 1mo to fail the conservative minimum-EVERY bound");
    } catch (SemanticException e) {
      Assert.assertTrue(e.getMessage().contains("1mo"));
      Assert.assertFalse(e.getMessage().contains("[0]"));
    }
  }

  private static CreateContinuousQueryStatement parse(String sql) {
    return (CreateContinuousQueryStatement)
        StatementGenerator.createStatement(sql, ZoneId.of("UTC"));
  }

  private static void assertParseFails(String sql) {
    try {
      StatementGenerator.createStatement(sql, ZoneId.of("UTC"));
      Assert.fail("expected parser to reject " + sql);
    } catch (Exception ignored) {
      // Tree SQL duration literals only accept y/mo abbreviations.
    }
  }

  private static String calendarSql(String every, String range) {
    return "CREATE CQ cq_calendar RESAMPLE EVERY "
        + every
        + " RANGE "
        + range
        + " BEGIN SELECT max_value(s1) INTO root.sg.d1(s1_max) FROM root.sg.d1 GROUP BY(1mo) END";
  }
}
