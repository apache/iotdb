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

package org.apache.iotdb.db.queryengine.plan.relational.planner;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestMatadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;

import org.apache.tsfile.utils.Pair;
import org.junit.Test;

import java.time.ZoneId;

import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.AnalyzerTest.analyzeSQL;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.PredicateUtils.extractGlobalTimePredicate;

public class PredicateUtilsTest {
  @Test
  public void extractGlobalTimePredicateTest() {
    String sql = "SELECT tag1 FROM table1 where time>1 and s1>1";
    final Metadata metadata = new TestMatadata();
    final MPPQueryContext context =
        new MPPQueryContext(
            sql,
            new QueryId("test_query"),
            new SessionInfo(
                1L,
                "iotdb-user",
                ZoneId.systemDefault(),
                IoTDBConstant.ClientVersion.V_1_0,
                "db",
                IClientSession.SqlDialect.TABLE),
            null,
            null);

    Analysis actualAnalysis = analyzeSQL(sql, metadata, context);
    Pair<Expression, Boolean> ret =
        extractGlobalTimePredicate(
            actualAnalysis.getWhereMap().values().iterator().next(), true, true);
    System.out.println(ret.getLeft());

    sql = "SELECT tag1 FROM table1 where time>1 and s1>1 or tag1='A'";
    actualAnalysis = analyzeSQL(sql, metadata, context);
    ret =
        extractGlobalTimePredicate(
            actualAnalysis.getWhereMap().values().iterator().next(), true, true);
    System.out.println(ret.getLeft());
  }
}
