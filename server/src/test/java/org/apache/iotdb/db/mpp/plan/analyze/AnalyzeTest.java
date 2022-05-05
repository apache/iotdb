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
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.junit.Assert;
import org.junit.Test;

import java.time.ZonedDateTime;

import static org.junit.Assert.fail;

public class AnalyzeTest {

  @Test
  public void testRawDataQuery() {
    Analysis analysis = analyzeSQL("SELECT s1, s2 FROM root.sg.*");

    TypeProvider expectedTypeProvider = new TypeProvider();
    expectedTypeProvider.setType("root.sg.d1.s1", TSDataType.INT32);
    expectedTypeProvider.setType("root.sg.d1.s2", TSDataType.INT32);
    expectedTypeProvider.setType("root.sg.d2.s1", TSDataType.INT32);
    expectedTypeProvider.setType("root.sg.d2.s2", TSDataType.INT32);
    assertQueryAnalysisEquals(analysis, expectedTypeProvider);
  }

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

  private void assertQueryAnalysisEquals(
      Analysis actualAnalysis, TypeProvider expectedTypeProvider) {
    Assert.assertEquals(actualAnalysis.getTypeProvider(), expectedTypeProvider);
  }
}
