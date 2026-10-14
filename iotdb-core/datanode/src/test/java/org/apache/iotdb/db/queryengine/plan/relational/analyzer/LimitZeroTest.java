/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.analyzer;

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.ValuesNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.PlanTester;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LimitZeroTest {
  @Test
  public void testZeroLimitProducesEmptyValues() {
    String[] queries = {
      "SELECT s1 FROM table1 LIMIT 0",
      "SELECT s1 + 1 AS value FROM table1 LIMIT 0",
      "SELECT s1 FROM table1 WHERE s1 > 1 LIMIT 0",
      "SELECT s1 FROM table1 ORDER BY time LIMIT 0",
      "SELECT s1 FROM table1 ORDER BY s1 DESC LIMIT 0",
      "SELECT s1 FROM table1 LIMIT 0 OFFSET 10",
      "SELECT count(*) FROM table1 LIMIT 0",
      "SELECT DISTINCT tag1 FROM table1 LIMIT 0",
      "SELECT tag1, count(*) FROM table1 GROUP BY tag1 HAVING count(*) > 0 LIMIT 0",
      "SELECT row_number() OVER (ORDER BY time) FROM table1 LIMIT 0",
      "SELECT a.s1 FROM table1 a JOIN table1 b ON a.time = b.time LIMIT 0",
      "SELECT s1 FROM table1 UNION ALL SELECT s1 FROM table1 LIMIT 0"
    };
    for (String sql : queries) {
      PlanTester tester = new PlanTester();
      PlanNode root = tester.createPlan(sql).getRootNode();
      assertTrue(sql, root instanceof OutputNode);
      assertEmptySource(sql, root.getChildren().get(0));
      assertEquals(sql, 1, tester.getDistributedPlan().getFragments().size());
      assertEmptySource(sql, tester.getFragmentPlan(0).getChildren().get(0));
    }
  }

  private static void assertEmptySource(String sql, PlanNode source) {
    // A projection may remain to select or rename the columns of the empty relation.
    while (source instanceof ProjectNode) {
      source = source.getChildren().get(0);
    }
    assertTrue(sql, source instanceof ValuesNode);
    assertEquals(sql, 0, ((ValuesNode) source).getRowCount());
  }
}
