package org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule;

import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKRankingNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.WindowNode;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import com.google.common.collect.ImmutableList;

import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.Math.toIntExact;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.Util.toTopNRankingType;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.ChildReplacer.replaceChildren;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.limit;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.source;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.window;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKRankingNode.RankingType.ROW_NUMBER;
import static org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture.newCapture;

public class PushDownLimitIntoWindow implements Rule<LimitNode> {
  private static final Capture<WindowNode> childCapture = newCapture();
  private final Pattern<LimitNode> pattern;

  public PushDownLimitIntoWindow() {
    this.pattern =
        limit()
            .matching(
                limit ->
                    !limit.isWithTies()
                        && limit.getCount() != 0
                        && limit.getCount() <= Integer.MAX_VALUE
                        && !limit.requiresPreSortedInputs())
            .with(
                source()
                    .matching(
                        window()
                            .matching(
                                window -> window.getSpecification().getOrderingScheme().isPresent())
                            .matching(window -> toTopNRankingType(window).isPresent())
                            .capturedAs(childCapture)));
  }

  // TODO: isOptimizeTopNRanking(session);
  @Override
  public boolean isEnabled(SessionInfo session) {
    return true;
  }

  @Override
  public Pattern<LimitNode> getPattern() {
    return pattern;
  }

  @Override
  public Result apply(LimitNode node, Captures captures, Context context) {
    WindowNode source = captures.get(childCapture);

    Optional<TopKRankingNode.RankingType> rankingType = toTopNRankingType(source);

    int limit = toIntExact(node.getCount());
    TopKRankingNode topNRowNumberNode =
        new TopKRankingNode(
            source.getPlanNodeId(),
            source.getChildren(),
            source.getSpecification(),
            rankingType.get(),
            getOnlyElement(source.getWindowFunctions().keySet()),
            limit,
            false);
    if (rankingType.get() == ROW_NUMBER && source.getSpecification().getPartitionBy().isEmpty()) {
      return Result.ofPlanNode(topNRowNumberNode);
    }
    return Result.ofPlanNode(replaceChildren(node, ImmutableList.of(topNRowNumberNode)));
  }
}
