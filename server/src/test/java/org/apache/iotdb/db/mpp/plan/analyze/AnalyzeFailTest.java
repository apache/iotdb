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

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.plan.parser.StatementGenerator;

import org.junit.Assert;
import org.junit.Test;

import java.time.ZonedDateTime;

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
      Assert.assertTrue(e.getMessage().contains(message));
    }
  }
}
