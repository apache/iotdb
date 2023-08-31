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

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.parser.StatementGenerator;

import org.junit.Assert;
import org.junit.Test;

import java.time.ZonedDateTime;

import static org.apache.iotdb.db.queryengine.plan.expression.visitor.CountTimeAggregationAmountVisitor.COUNT_TIME_ONLY_SUPPORT_ONE_WILDCARD;
import static org.apache.iotdb.db.queryengine.plan.statement.component.IntoComponent.DEVICE_ALIGNMENT_INCONSISTENT_ERROR_MSG;
import static org.apache.iotdb.db.queryengine.plan.statement.component.IntoComponent.DEVICE_NUM_MISMATCH_ERROR_MSG;
import static org.apache.iotdb.db.queryengine.plan.statement.component.IntoComponent.DUPLICATE_TARGET_PATH_ERROR_MSG;
import static org.apache.iotdb.db.queryengine.plan.statement.component.IntoComponent.FORBID_PLACEHOLDER_ERROR_MSG;
import static org.apache.iotdb.db.queryengine.plan.statement.component.IntoComponent.PATH_NUM_MISMATCH_ERROR_MSG;
import static org.apache.iotdb.db.queryengine.plan.statement.component.IntoComponent.PLACEHOLDER_MISMATCH_ERROR_MSG;
import static org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement.COUNT_TIME_CAN_ONLY_EXIST_ONE_IN_SELECT;
import static org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement.COUNT_TIME_NOT_SUPPORT_GROUP_BY_LEVEL;
import static org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement.COUNT_TIME_NOT_SUPPORT_GROUP_BY_TAG;
import static org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement.COUNT_TIME_NOT_SUPPORT_USED_WITH_OTHER_OPERATION;
import static org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement.COUNT_TIME_NOT_SUPPORT_USE_WITH_HAVING;
import static org.junit.Assert.fail;

public class AnalyzeFailTest {

  @Test
  public void illegalAggregationTest1() {
    String message = "Raw data and aggregation result hybrid calculation is not supported.";
    assertAnalyzeSemanticException("SELECT sum(s1) + s1 FROM root.sg.d1", message);
  }

  @Test
  public void illegalAggregationTest2() {
    String message = "Aggregation results cannot be as input of the aggregation function.";
    assertAnalyzeSemanticException("SELECT sum(sum(s1)) FROM root.sg.d1", message);
  }

  @Test
  public void illegalAggregationTest3() {
    String message = "Raw data and aggregation hybrid query is not supported.";
    assertAnalyzeSemanticException("SELECT sum(s1), s1 FROM root.sg.d1", message);
  }

  @Test
  public void samePropertyKeyTest() {
    assertAnalyzeSemanticException(
        "CREATE TIMESERIES root.sg1.d1.s1 INT32 TAGS('a'='1') ATTRIBUTES('a'='1')",
        "Tag and attribute shouldn't have the same property key");
  }

  @Test
  public void sameMeasurementsInAlignedTest() {
    assertAnalyzeSemanticException(
        "CREATE ALIGNED TIMESERIES root.ln.wf01.GPS(latitude FLOAT, latitude FLOAT)",
        "Measurement under an aligned device is not allowed to have the same measurement name");
  }

  @Test
  public void selectIntoTest() {
    assertAnalyzeSemanticException(
        "select s1, s2 into root.sg_bk.new_d(t1, t2) from root.sg.* align by device;",
        DEVICE_NUM_MISMATCH_ERROR_MSG);
    assertAnalyzeSemanticException(
        "select s1, s2 into root.sg_bk.new_d(t1, t2) from root.sg.*;", PATH_NUM_MISMATCH_ERROR_MSG);

    assertAnalyzeSemanticException(
        "select s1, s2 into root.sg_bk.new_d(t1, t2), root.sg_bk.new_d(t3, t1) from root.sg.* align by device;",
        DUPLICATE_TARGET_PATH_ERROR_MSG);
    assertAnalyzeSemanticException(
        "select s1, s2 into root.sg_bk.new_d(t1, t2, t1, t4) from root.sg.*;",
        DUPLICATE_TARGET_PATH_ERROR_MSG);
    assertAnalyzeSemanticException(
        "select s1, s2 into root.sg_bk.new_${1}(t1, t2) from root.sg.* align by device;",
        DUPLICATE_TARGET_PATH_ERROR_MSG);
    assertAnalyzeSemanticException(
        "select s1, s2 into root.sg_bk.new_${1}(t1, t2), root.sg_bk.new_${1}(t1, t2) from root.sg.*;",
        DUPLICATE_TARGET_PATH_ERROR_MSG);
    assertAnalyzeSemanticException(
        "select s1, s2 into root.sg_bk.new_d(::) from root.sg.*;", DUPLICATE_TARGET_PATH_ERROR_MSG);

    assertAnalyzeSemanticException(
        "select s1, s2 into root.sg_bk.new_d(t1, t2), aligned root.sg_bk.new_d(t3, t4) from root.sg.* align by device;",
        DEVICE_ALIGNMENT_INCONSISTENT_ERROR_MSG);
    assertAnalyzeSemanticException(
        "select s1, s2 into root.sg_bk.new_d(t1, t2), aligned root.sg_bk.new_d(t3, t4) from root.sg.*;",
        DEVICE_ALIGNMENT_INCONSISTENT_ERROR_MSG);

    assertAnalyzeSemanticException(
        "select count(s1), last_value(s2) into root.sg_bk.new_${2}(::) from root.sg.* align by device;",
        FORBID_PLACEHOLDER_ERROR_MSG);
    assertAnalyzeSemanticException(
        "select count(s1), last_value(s2) into root.sg_bk.new_d1(::), root.sg_bk.new_d2(::) from root.sg.* align by device;",
        FORBID_PLACEHOLDER_ERROR_MSG);
    assertAnalyzeSemanticException(
        "select count(s1), last_value(s2) into root.sg_bk.new_d1(::), root.sg_bk.new_d2(::) from root.sg.*;",
        FORBID_PLACEHOLDER_ERROR_MSG);
    assertAnalyzeSemanticException(
        "select count(s1), last_value(s2) into root.sg_bk.new_${2}(t1, t2, t3, t4) from root.sg.*;",
        FORBID_PLACEHOLDER_ERROR_MSG);
    assertAnalyzeSemanticException(
        "select count(s1), last_value(s2) into root.sg_bk.new_${2}(::) from root.sg.*;",
        FORBID_PLACEHOLDER_ERROR_MSG);

    assertAnalyzeSemanticException(
        "select s1 + 1, s1 + s2 into root.sg_bk.new_${2}(::) from root.sg.* align by device;",
        FORBID_PLACEHOLDER_ERROR_MSG);
    assertAnalyzeSemanticException(
        "select s1 + 1, s1 + s2 into root.sg_bk.new_d1(::), root.sg_bk.new_d2(::) from root.sg.* align by device;",
        FORBID_PLACEHOLDER_ERROR_MSG);
    assertAnalyzeSemanticException(
        "select s1 + 1, s1 + s2 into root.sg_bk.new_d1(::), root.sg_bk.new_d2(::) from root.sg.*;",
        FORBID_PLACEHOLDER_ERROR_MSG);
    assertAnalyzeSemanticException(
        "select s1 + 1, s1 + s2 into root.sg_bk.new_${2}(t1, t2, t3, t4) from root.sg.*;",
        FORBID_PLACEHOLDER_ERROR_MSG);
    assertAnalyzeSemanticException(
        "select s1 + 1, s1 + s2 into root.sg_bk.new_${2}(::) from root.sg.*;",
        FORBID_PLACEHOLDER_ERROR_MSG);

    assertAnalyzeSemanticException(
        "select * into root.sg_bk.new_d1(::), root.sg_bk.new_d2(::) from root.sg.**;",
        PLACEHOLDER_MISMATCH_ERROR_MSG);
    assertAnalyzeSemanticException(
        "select s1, s2 into root.sg_bk.new_d1(${3}, s2) from root.sg.d1;",
        PLACEHOLDER_MISMATCH_ERROR_MSG);
    assertAnalyzeSemanticException(
        "select * into root.sg_bk.new_${2}(::, s1) from root.sg.**;",
        PLACEHOLDER_MISMATCH_ERROR_MSG);
    assertAnalyzeSemanticException(
        "select d1.s1, d1.s2, d2.s3, d3.s4 into ::(s1_1, s2_2, backup_${4}) from root.sg",
        PLACEHOLDER_MISMATCH_ERROR_MSG);
    assertAnalyzeSemanticException(
        "select s1, s2 into ::(s1_1, s2_2), root.backup_sg.::(s1, s2) from root.sg.* align by device;",
        PLACEHOLDER_MISMATCH_ERROR_MSG);
  }

  @Test
  public void countTimeTest() {
    assertAnalyzeSemanticException(
        "select count_time(*) from root.sg.* group by level=1;",
        COUNT_TIME_NOT_SUPPORT_GROUP_BY_LEVEL);

    assertAnalyzeSemanticException(
        "select count_time(*) from root.sg.* group by tags(key);",
        COUNT_TIME_NOT_SUPPORT_GROUP_BY_TAG);

    assertAnalyzeSemanticException(
        "select count_time(s1) from root.sg.*;", COUNT_TIME_ONLY_SUPPORT_ONE_WILDCARD);

    assertAnalyzeSemanticException(
        "select count_time(s1) from root.sg.d1;", COUNT_TIME_ONLY_SUPPORT_ONE_WILDCARD);

    assertAnalyzeSemanticException(
        "select count_time(d1.s1) from root.sg.*;", COUNT_TIME_ONLY_SUPPORT_ONE_WILDCARD);

    assertAnalyzeSemanticException(
        "select count_time(d1.*) from root.sg.*;", COUNT_TIME_ONLY_SUPPORT_ONE_WILDCARD);

    assertAnalyzeSemanticException(
        "select count_time(* + *) from root.sg.*;", COUNT_TIME_ONLY_SUPPORT_ONE_WILDCARD);

    assertAnalyzeSemanticException(
        "select count_time(*) / 2 from root.sg.*;",
        COUNT_TIME_NOT_SUPPORT_USED_WITH_OTHER_OPERATION);

    assertAnalyzeSemanticException(
        "select sum(s1) / count_time(*) from root.sg.*;",
        COUNT_TIME_NOT_SUPPORT_USED_WITH_OTHER_OPERATION);

    assertAnalyzeSemanticException(
        "select count_time(*),count_time(*) from root.sg.**;",
        COUNT_TIME_CAN_ONLY_EXIST_ONE_IN_SELECT);

    assertAnalyzeSemanticException(
        "select count_time(*),count_time(*) from root.sg.d1,root.sg.d2;",
        COUNT_TIME_CAN_ONLY_EXIST_ONE_IN_SELECT);

    assertAnalyzeSemanticException(
        "select COUNT_TIME(*),COUNT_TIME(*) from root.sg.d1,root.sg.d2;",
        COUNT_TIME_CAN_ONLY_EXIST_ONE_IN_SELECT);

    assertAnalyzeSemanticException(
        "select COUNT_TIME(*),count_time(*) from root.sg.d1,root.sg.d2;",
        COUNT_TIME_CAN_ONLY_EXIST_ONE_IN_SELECT);

    assertAnalyzeSemanticException(
        "select COUNT_TIME(*),COUNT_time(*) from root.sg.d1,root.sg.d2;",
        COUNT_TIME_CAN_ONLY_EXIST_ONE_IN_SELECT);

    assertAnalyzeSemanticException(
        "select count_time(*) from root.sg.* having count(*) > 1;",
        COUNT_TIME_NOT_SUPPORT_USE_WITH_HAVING);

    assertAnalyzeSemanticException(
        "select count_time(*) from root.sg.* having count_time(s1) > 1;",
        COUNT_TIME_NOT_SUPPORT_USE_WITH_HAVING);

    assertAnalyzeSemanticException(
        "select count_time(*) from root.sg.* having count_time(*) > 1;",
        COUNT_TIME_NOT_SUPPORT_USE_WITH_HAVING);
  }

  private void assertAnalyzeSemanticException(String sql, String message) {
    try {
      Analyzer analyzer =
          new Analyzer(
              new MPPQueryContext(new QueryId("test_query")),
              new FakePartitionFetcherImpl(),
              new FakeSchemaFetcherImpl());
      analyzer.analyze(StatementGenerator.createStatement(sql, ZonedDateTime.now().getOffset()));
      fail();
    } catch (SemanticException e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains(message));
    }
  }
}
