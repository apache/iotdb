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

package org.apache.iotdb.db.queryengine.plan.relational.planner.iterative;

import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.execution.warnings.WarningCollector;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.relational.execution.querystats.PlanOptimizersStatsCollector;
import org.apache.iotdb.db.queryengine.plan.relational.execution.querystats.QueryPlanOptimizerStatistics;
import org.apache.iotdb.db.queryengine.plan.relational.planner.PlannerContext;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolAllocator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.AdaptivePlanOptimizer;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.PlanOptimizer;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Match;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;
import org.apache.iotdb.session.Session;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.lang.System.nanoTime;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.joining;
import static org.apache.iotdb.db.queryengine.plan.relational.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture.newCapture;
import static org.apache.iotdb.rpc.TSStatusCode.OPTIMIZER_TIMEOUT;

public class IterativeOptimizer implements AdaptivePlanOptimizer {
  private static final Logger LOG = LoggerFactory.getLogger(IterativeOptimizer.class);

  private final RuleStatsRecorder stats;
  private final List<PlanOptimizer> legacyRules;
  private final Set<Rule<?>> rules;
  private final RuleIndex ruleIndex;
  private final Predicate<Session> useLegacyRules;
  private final PlannerContext plannerContext;

  public IterativeOptimizer(
      PlannerContext plannerContext, RuleStatsRecorder stats, Set<Rule<?>> rules) {
    this(plannerContext, stats, session -> false, ImmutableList.of(), rules);
  }

  public IterativeOptimizer(
      PlannerContext plannerContext,
      RuleStatsRecorder stats,
      Predicate<Session> useLegacyRules,
      List<PlanOptimizer> legacyRules,
      Set<Rule<?>> newRules) {
    this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    this.stats = requireNonNull(stats, "stats is null");
    this.useLegacyRules = requireNonNull(useLegacyRules, "useLegacyRules is null");
    this.rules = requireNonNull(newRules, "rules is null");
    this.legacyRules = ImmutableList.copyOf(legacyRules);
    this.ruleIndex = RuleIndex.builder().register(newRules).build();

    stats.registerAll(newRules);
  }

  @Override
  public Result optimizeAndMarkPlanChanges(PlanNode plan, PlanOptimizer.Context context) {
    // TODO Use properties in session to specify whether to use the old optimizer
    /*// only disable new rules if we have legacy rules to fall back to
    if (useLegacyRules.test(session-> false) && !legacyRules.isEmpty()) {
        for (PlanOptimizer optimizer : legacyRules) {
            plan = optimizer.optimize(plan, context);
        }
        return new Result(plan, ImmutableSet.of());
    }*/

    Set<PlanNodeId> changedPlanNodeIds = new HashSet<>();
    Memo memo = new Memo(context.idAllocator(), plan);
    Lookup lookup = Lookup.from(planNode -> Stream.of(memo.resolve(planNode)));

    // TODO Use properties in session to specify, use default as Trino now
    Duration timeout = new Duration(3, MINUTES);
    Context optimizerContext =
        new Context(
            memo,
            lookup,
            context.idAllocator(),
            context.getSymbolAllocator(),
            nanoTime(),
            timeout.toMillis(),
            context.sessionInfo(),
            context.warningCollector());
    exploreGroup(memo.getRootGroup(), optimizerContext, changedPlanNodeIds);
    context
        .planOptimizersStatsCollector()
        .add(optimizerContext.getIterativeOptimizerStatsCollector());
    return new Result(memo.extract(), ImmutableSet.copyOf(changedPlanNodeIds));
  }

  // Used for diagnostics.
  public Set<Rule<?>> getRules() {
    return rules;
  }

  private boolean exploreGroup(int group, Context context, Set<PlanNodeId> changedPlanNodeIds) {
    // tracks whether this group or any children groups change as
    // this method executes
    boolean progress = exploreNode(group, context, changedPlanNodeIds);

    while (exploreChildren(group, context, changedPlanNodeIds)) {
      progress = true;

      // if children changed, try current group again
      // in case we can match additional rules
      if (!exploreNode(group, context, changedPlanNodeIds)) {
        // no additional matches, so bail out
        break;
      }
    }

    return progress;
  }

  private boolean exploreNode(int group, Context context, Set<PlanNodeId> changedPlanNodeIds) {
    PlanNode node = context.memo.getNode(group);

    boolean done = false;
    boolean progress = false;

    while (!done) {
      context.checkTimeoutNotExhausted();

      done = true;
      Iterator<Rule<?>> possiblyMatchingRules = ruleIndex.getCandidates(node).iterator();
      while (possiblyMatchingRules.hasNext()) {
        Rule<?> rule = possiblyMatchingRules.next();
        long timeStart = nanoTime();
        long timeEnd;
        boolean invoked = false;
        boolean applied = false;

        if (rule.isEnabled(context.sessionInfo)) {
          invoked = true;
          Rule.Result result = transform(node, rule, context);
          timeEnd = nanoTime();
          if (result.getTransformedPlan().isPresent()) {
            changedPlanNodeIds.add(result.getTransformedPlan().get().getPlanNodeId());
          }
          if (result.getTransformedPlan().isPresent()) {
            node =
                context.memo.replace(
                    group, result.getTransformedPlan().get(), rule.getClass().getName());

            applied = true;
            done = false;
            progress = true;
          }
        } else {
          timeEnd = nanoTime();
        }

        context.recordRuleInvocation(rule, invoked, applied, timeEnd - timeStart);
      }
    }

    return progress;
  }

  private <T> Rule.Result transform(PlanNode node, Rule<T> rule, Context context) {
    Capture<T> nodeCapture = newCapture();
    Pattern<T> pattern = rule.getPattern().capturedAs(nodeCapture);
    Iterator<Match> matches = pattern.match(node, context.lookup).iterator();
    while (matches.hasNext()) {
      Match match = matches.next();
      long duration;
      Rule.Result result;
      try {
        long start = nanoTime();
        result = rule.apply(match.capture(nodeCapture), match.captures(), ruleContext(context));

        if (LOG.isDebugEnabled() && !result.isEmpty()) {
          LOG.debug("Rule: %s\nBefore:\n%s\nAfter:\n%s", rule.getClass().getName(), "", "");
        }
        duration = nanoTime() - start;
      } catch (RuntimeException e) {
        stats.recordFailure(rule);
        context.iterativeOptimizerStatsCollector.recordFailure(rule);
        throw e;
      }
      stats.record(rule, duration, !result.isEmpty());

      if (result.getTransformedPlan().isPresent()) {
        return result;
      }
    }

    return Rule.Result.empty();
  }

  private boolean exploreChildren(int group, Context context, Set<PlanNodeId> changedPlanNodeIds) {
    boolean progress = false;

    PlanNode expression = context.memo.getNode(group);
    for (PlanNode child : expression.getChildren()) {
      checkArgument(
          child instanceof GroupReference,
          "Expected child to be a group reference. Found: " + child.getClass().getName());

      if (exploreGroup(((GroupReference) child).getGroupId(), context, changedPlanNodeIds)) {
        progress = true;
      }
    }

    return progress;
  }

  private Rule.Context ruleContext(Context context) {
    // StatsProvider statsProvider = new CachingStatsProvider(statsCalculator,
    // Optional.of(context.memo), context.lookup, context.session,
    // context.symbolAllocator.getTypes(), context.tableStatsProvider,
    // context.runtimeStatsProvider);
    // CostProvider costProvider = new CachingCostProvider(costCalculator, statsProvider,
    // Optional.of(context.memo), context.session, context.symbolAllocator.getTypes());

    return new Rule.Context() {
      @Override
      public Lookup getLookup() {
        return context.lookup;
      }

      @Override
      public QueryId getIdAllocator() {
        return context.idAllocator;
      }

      @Override
      public SymbolAllocator getSymbolAllocator() {
        return context.symbolAllocator;
      }

      @Override
      public SessionInfo getSessionInfo() {
        return context.sessionInfo;
      }

      /*@Override
      public StatsProvider getStatsProvider()
      {
          return statsProvider;
      }

      @Override
      public CostProvider getCostProvider()
      {
          return costProvider;
      }*/

      @Override
      public void checkTimeoutNotExhausted() {
        context.checkTimeoutNotExhausted();
      }

      @Override
      public WarningCollector getWarningCollector() {
        return context.warningCollector;
      }
    };
  }

  private static class Context {
    private final Memo memo;
    private final Lookup lookup;
    private final QueryId idAllocator;
    private final SymbolAllocator symbolAllocator;
    private final long startTimeInNanos;
    private final long timeoutInMilliseconds;
    private final SessionInfo sessionInfo;
    private final WarningCollector warningCollector;
    // private final TableStatsProvider tableStatsProvider;
    // private final RuntimeInfoProvider runtimeStatsProvider;

    private final PlanOptimizersStatsCollector iterativeOptimizerStatsCollector;

    public Context(
        Memo memo,
        Lookup lookup,
        QueryId idAllocator,
        SymbolAllocator symbolAllocator,
        long startTimeInNanos,
        long timeoutInMilliseconds,
        SessionInfo sessionInfo,
        WarningCollector warningCollector) {
      checkArgument(
          timeoutInMilliseconds >= 0, "Timeout has to be a non-negative number [milliseconds]");

      this.memo = memo;
      this.lookup = lookup;
      this.idAllocator = idAllocator;
      this.symbolAllocator = symbolAllocator;
      this.startTimeInNanos = startTimeInNanos;
      this.timeoutInMilliseconds = timeoutInMilliseconds;
      this.sessionInfo = sessionInfo;
      this.warningCollector = warningCollector;
      this.iterativeOptimizerStatsCollector = createPlanOptimizersStatsCollector();
    }

    public void checkTimeoutNotExhausted() {
      if (NANOSECONDS.toMillis(nanoTime() - startTimeInNanos) >= timeoutInMilliseconds) {
        String message =
            format(
                OPTIMIZER_TIMEOUT.getStatusCode()
                    + ": The optimizer exhausted the time limit of %d ms",
                timeoutInMilliseconds);
        List<QueryPlanOptimizerStatistics> topRulesByTime =
            iterativeOptimizerStatsCollector.getTopRuleStats(5);
        if (topRulesByTime.isEmpty()) {
          message += ": no rules invoked";
        } else {
          message +=
              ": Top rules: "
                  + topRulesByTime.stream()
                      .map(
                          ruleStats ->
                              format(
                                  "%s: %s ms, %s invocations, %s applications",
                                  ruleStats.rule(),
                                  NANOSECONDS.toMillis(ruleStats.totalTime()),
                                  ruleStats.invocations(),
                                  ruleStats.applied()))
                      .collect(joining(",\n\t\t", "{\n\t\t", " }"));
        }
        throw new RuntimeException(message);
      }
    }

    public PlanOptimizersStatsCollector getIterativeOptimizerStatsCollector() {
      return iterativeOptimizerStatsCollector;
    }

    void recordRuleInvocation(Rule<?> rule, boolean invoked, boolean applied, long elapsedNanos) {
      iterativeOptimizerStatsCollector.recordRule(rule, invoked, applied, elapsedNanos);
    }
  }
}
