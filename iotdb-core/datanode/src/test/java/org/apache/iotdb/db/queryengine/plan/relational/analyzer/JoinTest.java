/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.analyzer;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.execution.warnings.WarningCollector;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.distribute.TableDistributedPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.StreamSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;

import org.junit.Assert;
import org.junit.Test;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.AnalyzerTest.analyzeSQL;
import static org.junit.Assert.fail;

public class JoinTest {

  static QueryId queryId = new QueryId("test_query");
  static SessionInfo sessionInfo =
      new SessionInfo(
          1L,
          "iotdb-user",
          ZoneId.systemDefault(),
          IoTDBConstant.ClientVersion.V_1_0,
          "db",
          IClientSession.SqlDialect.TABLE);
  static Metadata metadata = new TestMatadata();
  String sql;
  Analysis analysis;
  MPPQueryContext context;
  WarningCollector warningCollector = WarningCollector.NOOP;
  LogicalQueryPlan logicalQueryPlan;
  PlanNode logicalPlanNode;
  OutputNode outputNode;
  ProjectNode projectNode;
  StreamSortNode streamSortNode;
  TableDistributedPlanner distributionPlanner;
  DistributedQueryPlan distributedQueryPlan;
  TableScanNode tableScanNode;
  List<String> originalDeviceEntries1 =
      Arrays.asList(
          "table1.shanghai.B3.YY",
          "table1.shenzhen.B1.XX",
          "table1.shenzhen.B2.ZZ",
          "table1.shanghai.A3.YY");
  static List<String> originalDeviceEntries2 =
      Arrays.asList("table1.shenzhen.B1.XX", "table1.shenzhen.B2.ZZ");

  @Test
  public void subQueryWithJoinTest() {}

  @Test
  public void sortWithJoinTest() {}

  // ========== unsupported test ===============
  @Test
  public void unsupportedJoinTest() {
    assertAnalyzeSemanticException(
        "SELECT * FROM table1 t1 FULL JOIN table1 t2 ON t1.time=t2.time",
        "Only support INNER JOIN in current version, FULL JOIN is not supported");
  }

  private void assertAnalyzeSemanticException(String sql, String message) {
    try {
      context = new MPPQueryContext(sql, queryId, sessionInfo, null, null);
      analysis = analyzeSQL(sql, metadata, context);
      fail();
    } catch (SemanticException e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains(message));
    }
  }
}
