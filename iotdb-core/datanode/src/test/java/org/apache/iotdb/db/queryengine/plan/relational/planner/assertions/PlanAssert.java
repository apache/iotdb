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
package org.apache.iotdb.db.queryengine.plan.relational.planner.assertions;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestMatadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.PlanNodeSearcher;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.GroupReference;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Lookup;

import java.time.ZoneId;
import java.util.stream.Stream;

import static java.lang.String.format;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Lookup.noLookup;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Plans.resolveGroupReferences;

public final class PlanAssert {
  private PlanAssert() {}

  public static void assertPlan(LogicalQueryPlan actual, PlanMatchPattern pattern) {
    assertPlan(
        actual.getContext().getSession(),
        new TestMatadata(),
        actual.getRootNode(),
        noLookup(),
        pattern);
  }

  public static void assertPlan(PlanNode actual, PlanMatchPattern pattern) {
    assertPlan(
        new SessionInfo(
            1L,
            "iotdb-user",
            ZoneId.systemDefault(),
            IoTDBConstant.ClientVersion.V_1_0,
            "db",
            IClientSession.SqlDialect.TABLE),
        new TestMatadata(),
        actual,
        noLookup(),
        pattern);
  }

  public static void assertPlan(
      SessionInfo sessionInfo,
      Metadata metadata,
      PlanNode actual,
      Lookup lookup,
      PlanMatchPattern pattern) {
    MatchResult matches =
        actual.accept(new PlanMatchingVisitor(sessionInfo, metadata, lookup), pattern);
    if (!matches.isMatch()) {
      if (!containsGroupReferences(actual)) {
        throw new AssertionError(
            format(
                "Plan does not match, expected [\n\n%s\n] but found [\n\n%s\n], matches:[%s]",
                pattern, actual, matches));
      }
      // TODO support print plan tree
      PlanNode resolvedPlan = resolveGroupReferences(actual, lookup);
      throw new AssertionError(
          format(
              "Plan does not match, expected [\n\n%s\n] but found [\n\n%s\n] which resolves to [\n\n%s\n]",
              pattern, actual, resolvedPlan));
    }
  }

  private static boolean containsGroupReferences(PlanNode node) {
    return PlanNodeSearcher.searchFrom(node, Lookup.from(Stream::of))
        .where(GroupReference.class::isInstance)
        .findFirst()
        .isPresent();
  }
}
