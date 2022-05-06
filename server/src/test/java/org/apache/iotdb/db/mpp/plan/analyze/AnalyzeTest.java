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

package org.apache.iotdb.db.mpp.plan.analyze;

import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.plan.parser.StatementGenerator;
import org.apache.iotdb.db.mpp.plan.statement.Statement;

import org.junit.Test;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.fail;

public class AnalyzeTest {

  @Test
  public void testRawDataQuery() {
    List<String> sqls =
        Arrays.asList(
            // test query filter
            "select * from root.** where time > 100",
            "select * from root.** where time > 100 and time < 200",
            "select * from root.** where time > 100 or time < 200",
            "select s1 from root.sg.* where time > 100 and s1 > 10",
            "select s1 from root.sg.* where time > 100 and s2 > 10",
            "select s1 from root.sg.* where time > 100 or s1 > 10",
            "select s1 from root.sg.* where time > 100 or s2 > 10",
            "select s1 from root.sg.* where time > 100 and s2 + 1 > 10",
            "select s1 from root.sg.* where time > 100 or s2 + 1 > 10",
            "select s1 from root.sg.* where time > 100 or s2 + 1 > 10",
            // test SLIMIT/SOFFSET
            "select * from root.** SLIMIT 2 SOFFSET 2",
            "select * from root.sg.d1, root.sg.d2 SLIMIT 2 SOFFSET 2",
            "select s1, s1, s2, * from root.sg.* SLIMIT 2 SOFFSET 2",
            "select s1 as t, status, * from root.sg.d1",
            // test WITHOUT NULL
            "select s1, s2, * from root.sg.* WITHOUT NULL ANY",
            "select s1, s2, * from root.sg.* WITHOUT NULL ALL",
            "select s1, s2, * from root.sg.* WITHOUT NULL ANY (s1)",
            "select s1, s2, * from root.sg.* WITHOUT NULL ANY (s1, s2)",
            "select s1 as t, s2 from root.sg.d1 WITHOUT NULL ANY (t)",
            "select s1 as t, status from root.sg.d1 WITHOUT NULL ALL (t, status)",
            "select s1, s1, s2, * from root.sg.* SLIMIT 1 SOFFSET 2 WITHOUT NULL ALL (s1, s2)",
            // test FILL
            "select s1, s2, * from root.sg.* fill(previous)",
            "select s1, s2, * from root.sg.* fill(1)",
            "select s1 as t, status from root.sg.d1 fill(linear)");
    for (String sql : sqls) {
      Analysis analysis = analyzeSQL(sql);
      System.out.println(sql);
    }
  }

  // TODO: @lmh add more UTs

  private Analysis analyzeSQL(String sql) {
    try {
      Statement statement =
          StatementGenerator.createStatement(sql, ZonedDateTime.now().getOffset());
      MPPQueryContext context = new MPPQueryContext(new QueryId("test_query"));
      Analyzer analyzer =
          new Analyzer(context, new FakePartitionFetcherImpl(), new FakeSchemaFetcherImpl());
      return analyzer.analyze(statement);
    } catch (Exception e) {
      fail(e.getMessage());
    }
    fail();
    return null;
  }
}
