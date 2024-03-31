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
package org.apache.iotdb.db.queryengine.plan.relational.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.NodeRef;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.relational.sql.tree.Delete;
import org.apache.iotdb.db.relational.sql.tree.Expression;
import org.apache.iotdb.db.relational.sql.tree.Node;
import org.apache.iotdb.db.relational.sql.tree.Offset;
import org.apache.iotdb.db.relational.sql.tree.Query;
import org.apache.iotdb.db.relational.sql.tree.QuerySpecification;
import org.apache.iotdb.db.relational.sql.tree.Relation;
import org.apache.iotdb.db.relational.sql.tree.Union;
import org.apache.iotdb.session.Session;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.PlanBuilder.newPlanBuilder;

class QueryPlanner
{
    private static final int MAX_BIGINT_PRECISION = 19;
    private final Analysis analysis;
    private final SymbolAllocator symbolAllocator;
    private final QueryId idAllocator;
    private final Map<NodeRef<LambdaArgumentDeclaration>, Symbol> lambdaDeclarationToSymbolMap;
    private final PlannerContext plannerContext;
    private final Session session;
    private final SubqueryPlanner subqueryPlanner;
    private final Map<NodeRef<Node>, RelationPlan> recursiveSubqueries;

    QueryPlanner(
            Analysis analysis,
            SymbolAllocator symbolAllocator,
            QueryId idAllocator,
            Session session,
            Map<NodeRef<Node>, RelationPlan> recursiveSubqueries) {
        requireNonNull(analysis, "analysis is null");
        requireNonNull(symbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");
        requireNonNull(session, "session is null");
        requireNonNull(recursiveSubqueries, "recursiveSubqueries is null");

        this.analysis = analysis;
        this.symbolAllocator = symbolAllocator;
        this.idAllocator = idAllocator;
        this.session = session;
        this.subqueryPlanner = null;
        this.recursiveSubqueries = recursiveSubqueries;
    }

    public RelationPlan plan(Query query) {
        PlanBuilder builder = planQueryBody(query);

        List<Expression> orderBy = analysis.getOrderByExpressions(query);
        // builder = subqueryPlanner.handleSubqueries(builder, orderBy, analysis.getSubqueries(query));

        List<Analysis.SelectExpression> selectExpressions = analysis.getSelectExpressions(query);
        List<Expression> outputs = selectExpressions.stream()
                .map(SelectExpression::getExpression)
                .collect(toImmutableList());
        builder = builder.appendProjections(Iterables.concat(orderBy, outputs), symbolAllocator, idAllocator);

        Optional<OrderingScheme> orderingScheme = orderingScheme(builder, query.getOrderBy(), analysis.getOrderByExpressions(query));
        builder = sort(builder, orderingScheme);
        builder = offset(builder, query.getOffset());
        builder = limit(builder, query.getLimit(), orderingScheme);
        builder = builder.appendProjections(outputs, symbolAllocator, idAllocator);

        return new RelationPlan(
                builder.getRoot(),
                analysis.getScope(query),
                computeOutputs(builder, outputs));
    }

    public RelationPlan planExpand(Query query) {
        checkArgument(analysis.isExpandableQuery(query), "query is not registered as expandable");

        Union union = (Union) query.getQueryBody();
        ImmutableList.Builder<NodeAndMappings> recursionSteps = ImmutableList.builder();

        // plan anchor relation
        Relation anchorNode = union.getRelations().get(0);
        RelationPlan anchorPlan = new RelationPlanner(analysis, symbolAllocator, idAllocator, session, recursiveSubqueries)
                .process(anchorNode, null);

        // prune anchor plan outputs to contain only the symbols exposed in the scope
        NodeAndMappings prunedAnchorPlan = pruneInvisibleFields(anchorPlan, idAllocator);

        // if the anchor plan has duplicate output symbols, add projection on top to make the symbols unique
        // This is necessary to successfully unroll recursion: the recursion step relation must follow
        // the same layout while it might not have duplicate outputs where the anchor plan did
        NodeAndMappings disambiguatedAnchorPlan = disambiguateOutputs(prunedAnchorPlan, symbolAllocator, idAllocator);
        anchorPlan = new RelationPlan(disambiguatedAnchorPlan.getNode(), analysis.getScope(query), disambiguatedAnchorPlan.getFields());

        recursionSteps.add(copy(anchorPlan.getRoot(), anchorPlan.getFieldMappings()));

        // plan recursion step
        Relation recursionStepRelation = union.getRelations().get(1);
        RelationPlan recursionStepPlan = new RelationPlanner(
                analysis,
                symbolAllocator,
                idAllocator,
                session,
                ImmutableMap.of(NodeRef.of(analysis.getRecursiveReference(query)), anchorPlan))
                .process(recursionStepRelation, null);

        // coerce recursion step outputs and prune them to contain only the symbols exposed in the scope
        NodeAndMappings coercedRecursionStep;
        List<Type> types = analysis.getRelationCoercion(recursionStepRelation);
        if (types == null) {
            coercedRecursionStep = pruneInvisibleFields(recursionStepPlan, idAllocator);
        }
        else {
            coercedRecursionStep = coerce(recursionStepPlan, types, symbolAllocator, idAllocator);
        }

        NodeAndMappings replacementSpot = new NodeAndMappings(anchorPlan.getRoot(), anchorPlan.getFieldMappings());
        PlanNode recursionStep = coercedRecursionStep.getNode();
        List<Symbol> mappings = coercedRecursionStep.getFields();

        // unroll recursion
        int maxRecursionDepth = getMaxRecursionDepth(session);
        for (int i = 0; i < maxRecursionDepth; i++) {
            recursionSteps.add(copy(recursionStep, mappings));
            NodeAndMappings replacement = copy(recursionStep, mappings);

            // if the recursion step plan has duplicate output symbols, add projection on top to make the symbols unique
            // This is necessary to successfully unroll recursion: the relation on the next recursion step must follow
            // the same layout while it might not have duplicate outputs where the plan for this step did
            replacement = disambiguateOutputs(replacement, symbolAllocator, idAllocator);
            recursionStep = replace(recursionStep, replacementSpot, replacement);
            replacementSpot = replacement;
        }

        // after the last recursion step, check if the recursion converged. the last step is expected to return empty result
        // 1. append window to count rows
        NodeAndMappings checkConvergenceStep = copy(recursionStep, mappings);
        Symbol countSymbol = symbolAllocator.newSymbol("count", BIGINT);
        ResolvedFunction function = .resolveBuiltinFunction("count", ImmutableList.of());
        WindowNode.Function countFunction = new WindowNode.Function(function, ImmutableList.of(), DEFAULT_FRAME, false);

        WindowNode windowNode = new WindowNode(
                idAllocator.getNextId(),
                checkConvergenceStep.getNode(),
                new DataOrganizationSpecification(ImmutableList.of(), Optional.empty()),
                ImmutableMap.of(countSymbol, countFunction),
                Optional.empty(),
                ImmutableSet.of(),
                0);

        // 2. append filter to fail on non-empty result
        String recursionLimitExceededMessage = format("Recursion depth limit exceeded (%s). Use 'max_recursion_depth' session property to modify the limit.", maxRecursionDepth);
        Expression predicate = ifExpression(
                new ComparisonExpression(
                        GREATER_THAN_OR_EQUAL,
                        countSymbol.toSymbolReference(),
                        new Constant(BIGINT, 0L)),
                new Cast(
                        failFunction(plannerContext.getMetadata(), NOT_SUPPORTED, recursionLimitExceededMessage),
                        BOOLEAN),
                TRUE_LITERAL);
        FilterNode filterNode = new FilterNode(idAllocator.getNextId(), windowNode, predicate);

        recursionSteps.add(new NodeAndMappings(filterNode, checkConvergenceStep.getFields()));

        // union all the recursion steps
        List<NodeAndMappings> recursionStepsToUnion = recursionSteps.build();

        List<Symbol> unionOutputSymbols = anchorPlan.getFieldMappings().stream()
                .map(symbol -> symbolAllocator.newSymbol(symbol, "_expanded"))
                .collect(toImmutableList());

        ImmutableListMultimap.Builder<Symbol, Symbol> unionSymbolMapping = ImmutableListMultimap.builder();
        for (NodeAndMappings plan : recursionStepsToUnion) {
            for (int i = 0; i < unionOutputSymbols.size(); i++) {
                unionSymbolMapping.put(unionOutputSymbols.get(i), plan.getFields().get(i));
            }
        }

        List<PlanNode> nodesToUnion = recursionStepsToUnion.stream()
                .map(NodeAndMappings::getNode)
                .collect(toImmutableList());

        PlanNode result = new UnionNode(idAllocator.getNextId(), nodesToUnion, unionSymbolMapping.build(), unionOutputSymbols);

        if (union.isDistinct()) {
            result = singleAggregation(
                    idAllocator.getNextId(),
                    result,
                    ImmutableMap.of(),
                    singleGroupingSet(result.getOutputSymbols()));
        }

        return new RelationPlan(result, anchorPlan.getScope(), unionOutputSymbols, outerContext);
    }

    // Return a copy of the plan and remapped field mappings. In the copied plan:
    // - all PlanNodeIds are replaced with new values,
    // - all symbols are replaced with new symbols.
    // Copying the plan might reorder symbols. The returned field mappings keep the original
    // order and might be used to identify the original output symbols with their copies.
    private NodeAndMappings copy(PlanNode plan, List<Symbol> fields)
    {
        return PlanCopier.copyPlan(plan, fields, symbolAllocator, idAllocator);
    }

    private PlanNode replace(PlanNode plan, NodeAndMappings replacementSpot, NodeAndMappings replacement)
    {
        checkArgument(
                replacementSpot.getFields().size() == replacement.getFields().size(),
                "mismatching outputs in replacement, expected: %s, got: %s",
                replacementSpot.getFields().size(),
                replacement.getFields().size());

        return SimplePlanRewriter.rewriteWith(new SimplePlanRewriter<Void>()
        {
            @Override
            protected PlanNode visitPlan(PlanNode node, RewriteContext<Void> context)
            {
                return node.replaceChildren(node.getSources().stream()
                        .map(child -> {
                            if (child == replacementSpot.getNode()) {
                                // add projection to adjust symbols
                                Assignments.Builder assignments = Assignments.builder();
                                for (int i = 0; i < replacementSpot.getFields().size(); i++) {
                                    assignments.put(replacementSpot.getFields().get(i), replacement.getFields().get(i).toSymbolReference());
                                }
                                return new ProjectNode(idAllocator.getNextId(), replacement.getNode(), assignments.build());
                            }
                            return context.rewrite(child);
                        })
                        .collect(toImmutableList()));
            }
        }, plan, null);
    }

    public RelationPlan plan(QuerySpecification node) {
        PlanBuilder builder = planFrom(node);

        builder = filter(builder, analysis.getWhere(node), node);
        // builder = aggregate(builder, node);
        builder = filter(builder, analysis.getHaving(node), node);
        // builder = planWindowFunctions(node, builder, ImmutableList.copyOf(analysis.getWindowFunctions(node)));
        // builder = planWindowMeasures(node, builder, ImmutableList.copyOf(analysis.getWindowMeasures(node)));

        List<Analysis.SelectExpression> selectExpressions = analysis.getSelectExpressions(node);
        List<Expression> expressions = selectExpressions.stream()
                .map(SelectExpression::getExpression)
                .collect(toImmutableList());
        builder = subqueryPlanner.handleSubqueries(builder, expressions, analysis.getSubqueries(node));

        if (hasExpressionsToUnfold(selectExpressions)) {
            // pre-project the folded expressions to preserve any non-deterministic semantics of functions that might be referenced
            builder = builder.appendProjections(expressions, symbolAllocator, idAllocator);
        }

        List<Expression> outputs = outputExpressions(selectExpressions);
        if (node.getOrderBy().isPresent()) {
            // ORDER BY requires outputs of SELECT to be visible.
            // For queries with aggregation, it also requires grouping keys and translated aggregations.
            if (analysis.isAggregation(node)) {
                // Add projections for aggregations required by ORDER BY. After this step, grouping keys and translated
                // aggregations are visible.
                List<Expression> orderByAggregates = analysis.getOrderByAggregates(node.getOrderBy().get());
                builder = builder.appendProjections(orderByAggregates, symbolAllocator, idAllocator);
            }

            // Add projections for the outputs of SELECT, but stack them on top of the ones from the FROM clause so both are visible
            // when resolving the ORDER BY clause.
            builder = builder.appendProjections(outputs, symbolAllocator, idAllocator);

            // The new scope is the composite of the fields from the FROM and SELECT clause (local nested scopes). Fields from the bottom of
            // the scope stack need to be placed first to match the expected layout for nested scopes.
            List<Symbol> newFields = new ArrayList<>();
            newFields.addAll(builder.getTranslations().getFieldSymbols());

            outputs.stream()
                    .map(builder::translate)
                    .forEach(newFields::add);

            builder = builder.withScope(analysis.getScope(node.getOrderBy().get()), newFields);

            builder = planWindowFunctions(node, builder, ImmutableList.copyOf(analysis.getOrderByWindowFunctions(node.getOrderBy().get())));
            builder = planWindowMeasures(node, builder, ImmutableList.copyOf(analysis.getOrderByWindowMeasures(node.getOrderBy().get())));
        }

        List<io.trino.sql.tree.Expression> orderBy = analysis.getOrderByExpressions(node);
        builder = subqueryPlanner.handleSubqueries(builder, orderBy, analysis.getSubqueries(node));
        builder = builder.appendProjections(Iterables.concat(orderBy, outputs), symbolAllocator, idAllocator);

        builder = distinct(builder, node, outputs);
        Optional<OrderingScheme> orderingScheme = orderingScheme(builder, node.getOrderBy(), analysis.getOrderByExpressions(node));
        builder = sort(builder, orderingScheme);
        builder = offset(builder, node.getOffset());
        builder = limit(builder, node.getLimit(), orderingScheme);
        builder = builder.appendProjections(outputs, symbolAllocator, idAllocator);

        return new RelationPlan(
                builder.getRoot(),
                analysis.getScope(node),
                computeOutputs(builder, outputs),
                outerContext);
    }

    private static boolean hasExpressionsToUnfold(List<Analysis.SelectExpression> selectExpressions)
    {
        return selectExpressions.stream()
                .map(SelectExpression::getUnfoldedExpressions)
                .anyMatch(Optional::isPresent);
    }

    private static List<Expression> outputExpressions(List<Analysis.SelectExpression> selectExpressions)
    {
        ImmutableList.Builder<Expression> result = ImmutableList.builder();
        for (Analysis.SelectExpression selectExpression : selectExpressions) {
            if (selectExpression.getUnfoldedExpressions().isPresent()) {
                result.addAll(selectExpression.getUnfoldedExpressions().get());
            }
            else {
                result.add(selectExpression.getExpression());
            }
        }
        return result.build();
    }

    public PlanNode plan(Delete node) {
        // implement delete logic
        return null;
    }

    private static List<Symbol> computeOutputs(PlanBuilder builder, List<Expression> outputExpressions) {
        ImmutableList.Builder<Symbol> outputSymbols = ImmutableList.builder();
        for (Expression expression : outputExpressions) {
            outputSymbols.add(builder.translate(expression));
        }
        return outputSymbols.build();
    }

    private PlanBuilder planQueryBody(Query query) {
        RelationPlan relationPlan = new RelationPlanner(analysis, symbolAllocator, idAllocator, session, recursiveSubqueries)
                .process(query.getQueryBody(), null);

        return newPlanBuilder(relationPlan, analysis, session);
    }

    private PlanBuilder planFrom(QuerySpecification node) {
        if (node.getFrom().isPresent()) {
            RelationPlan relationPlan = new RelationPlanner(analysis, symbolAllocator, idAllocator, session, recursiveSubqueries)
                    .process(node.getFrom().get(), null);
            return newPlanBuilder(relationPlan, analysis, session);
        }

        return new PlanBuilder(
                new TranslationMap(outerContext, analysis.getImplicitFromScope(node), analysis, lambdaDeclarationToSymbolMap, ImmutableList.of(), session, plannerContext),
                new ValuesNode(idAllocator.getNextId(), 1));
    }

    private PlanBuilder filter(PlanBuilder subPlan, Expression predicate, Node node)
    {
        if (predicate == null) {
            return subPlan;
        }

        // subPlan = subqueryPlanner.handleSubqueries(subPlan, predicate, analysis.getSubqueries(node));

        return subPlan.withNewRoot(new FilterNode(idAllocator.getNextId(), subPlan.getRoot(), coerceIfNecessary(analysis, predicate, subPlan.rewrite(predicate))));
    }

    private <T extends io.trino.sql.tree.Expression> List<T> scopeAwareDistinct(PlanBuilder subPlan, List<T> expressions)
    {
        return expressions.stream()
                .map(function -> scopeAwareKey(function, analysis, subPlan.getScope()))
                .distinct()
                .map(ScopeAware::getNode)
                .collect(toImmutableList());
    }

    private FrameBoundPlanAndSymbols planFrameBound(PlanBuilder subPlan, PlanAndMappings coercions, Optional<io.trino.sql.tree.Expression> frameOffset, ResolvedWindow window, Map<Type, Symbol> sortKeyCoercions)
    {
        Optional<ResolvedFunction> frameBoundCalculationFunction = frameOffset.map(analysis::getFrameBoundCalculation);

        // Empty frameBoundCalculationFunction indicates that frame bound type is CURRENT ROW or UNBOUNDED.
        // Handling it doesn't require any additional symbols.
        if (frameBoundCalculationFunction.isEmpty()) {
            return new FrameBoundPlanAndSymbols(subPlan, Optional.empty(), Optional.empty());
        }

        // Present frameBoundCalculationFunction indicates that frame bound type is <expression> PRECEDING or <expression> FOLLOWING.
        // It requires adding certain projections to the plan so that the operator can determine frame bounds.

        // First, append filter to validate offset values. They mustn't be negative or null.
        Symbol offsetSymbol = coercions.get(frameOffset.get());
        Expression zeroOffset = zeroOfType(symbolAllocator.getTypes().get(offsetSymbol));
        Expression predicate = ifExpression(
                new ComparisonExpression(
                        GREATER_THAN_OR_EQUAL,
                        offsetSymbol.toSymbolReference(),
                        zeroOffset),
                TRUE_LITERAL,
                new Cast(
                        failFunction(plannerContext.getMetadata(), INVALID_WINDOW_FRAME, "Window frame offset value must not be negative or null"),
                        BOOLEAN));
        subPlan = subPlan.withNewRoot(new FilterNode(
                idAllocator.getNextId(),
                subPlan.getRoot(),
                predicate));

        // Then, coerce the sortKey so that we can add / subtract the offset.
        // Note: for that we cannot rely on the usual mechanism of using the coerce() method. The coerce() method can only handle one coercion for a node,
        // while the sortKey node might require several different coercions, e.g. one for frame start and one for frame end.
        io.trino.sql.tree.Expression sortKey = Iterables.getOnlyElement(window.getOrderBy().orElseThrow().getSortItems()).getSortKey();
        Symbol sortKeyCoercedForFrameBoundCalculation = coercions.get(sortKey);
        Optional<Type> coercion = frameOffset.map(analysis::getSortKeyCoercionForFrameBoundCalculation);
        if (coercion.isPresent()) {
            Type expectedType = coercion.get();
            Symbol alreadyCoerced = sortKeyCoercions.get(expectedType);
            if (alreadyCoerced != null) {
                sortKeyCoercedForFrameBoundCalculation = alreadyCoerced;
            }
            else {
                Expression cast = new Cast(
                        coercions.get(sortKey).toSymbolReference(),
                        expectedType,
                        false);
                sortKeyCoercedForFrameBoundCalculation = symbolAllocator.newSymbol(cast, expectedType);
                sortKeyCoercions.put(expectedType, sortKeyCoercedForFrameBoundCalculation);
                subPlan = subPlan.withNewRoot(new ProjectNode(
                        idAllocator.getNextId(),
                        subPlan.getRoot(),
                        Assignments.builder()
                                .putIdentities(subPlan.getRoot().getOutputSymbols())
                                .put(sortKeyCoercedForFrameBoundCalculation, cast)
                                .build()));
            }
        }

        // Next, pre-project the function which combines sortKey with the offset.
        // Note: if frameOffset needs a coercion, it was added before by a call to coerce() method.
        ResolvedFunction function = frameBoundCalculationFunction.get();
        Expression functionCall = new FunctionCall(
                function,
                ImmutableList.of(
                        sortKeyCoercedForFrameBoundCalculation.toSymbolReference(),
                        offsetSymbol.toSymbolReference()));
        Symbol frameBoundSymbol = symbolAllocator.newSymbol(functionCall, function.getSignature().getReturnType());
        subPlan = subPlan.withNewRoot(new ProjectNode(
                idAllocator.getNextId(),
                subPlan.getRoot(),
                Assignments.builder()
                        .putIdentities(subPlan.getRoot().getOutputSymbols())
                        .put(frameBoundSymbol, functionCall)
                        .build()));

        // Finally, coerce the sortKey to the type of frameBound so that the operator can perform comparisons on them
        Optional<Symbol> sortKeyCoercedForFrameBoundComparison = Optional.of(coercions.get(sortKey));
        coercion = frameOffset.map(analysis::getSortKeyCoercionForFrameBoundComparison);
        if (coercion.isPresent()) {
            Type expectedType = coercion.get();
            Symbol alreadyCoerced = sortKeyCoercions.get(expectedType);
            if (alreadyCoerced != null) {
                sortKeyCoercedForFrameBoundComparison = Optional.of(alreadyCoerced);
            }
            else {
                Expression cast = new Cast(
                        coercions.get(sortKey).toSymbolReference(),
                        expectedType,
                        false);
                Symbol castSymbol = symbolAllocator.newSymbol(cast, expectedType);
                sortKeyCoercions.put(expectedType, castSymbol);
                subPlan = subPlan.withNewRoot(new ProjectNode(
                        idAllocator.getNextId(),
                        subPlan.getRoot(),
                        Assignments.builder()
                                .putIdentities(subPlan.getRoot().getOutputSymbols())
                                .put(castSymbol, cast)
                                .build()));
                sortKeyCoercedForFrameBoundComparison = Optional.of(castSymbol);
            }
        }

        return new FrameBoundPlanAndSymbols(subPlan, Optional.of(frameBoundSymbol), sortKeyCoercedForFrameBoundComparison);
    }

    private FrameOffsetPlanAndSymbol planFrameOffset(PlanBuilder subPlan, Optional<Symbol> frameOffset)
    {
        if (frameOffset.isEmpty()) {
            return new FrameOffsetPlanAndSymbol(subPlan, Optional.empty());
        }

        Symbol offsetSymbol = frameOffset.get();
        Type offsetType = symbolAllocator.getTypes().get(offsetSymbol);

        // Append filter to validate offset values. They mustn't be negative or null.
        Expression zeroOffset = zeroOfType(offsetType);
        Expression predicate = ifExpression(
                new ComparisonExpression(GREATER_THAN_OR_EQUAL, offsetSymbol.toSymbolReference(), zeroOffset),
                TRUE_LITERAL,
                new Cast(
                        failFunction(plannerContext.getMetadata(), INVALID_WINDOW_FRAME, "Window frame offset value must not be negative or null"),
                        BOOLEAN));
        subPlan = subPlan.withNewRoot(new FilterNode(
                idAllocator.getNextId(),
                subPlan.getRoot(),
                predicate));

        if (offsetType.equals(BIGINT)) {
            return new FrameOffsetPlanAndSymbol(subPlan, Optional.of(offsetSymbol));
        }

        Expression offsetToBigint;

        if (offsetType instanceof DecimalType decimalType && !decimalType.isShort()) {
            int actualPrecision = decimalType.getPrecision();

            if (actualPrecision < MAX_BIGINT_PRECISION) {
                offsetToBigint = new Cast(offsetSymbol.toSymbolReference(), BIGINT);
            }
            else if (actualPrecision > MAX_BIGINT_PRECISION) {
                // If the offset value exceeds max bigint, it implies that the frame bound falls beyond the partition bound.
                // In such case, the frame bound is set to the partition bound. Passing max bigint as the offset value has
                // the same effect. The offset value can be truncated to max bigint for the purpose of cast.
                offsetToBigint = new Constant(BIGINT, Long.MAX_VALUE);
            }
            else {
                offsetToBigint = ifExpression(
                        new ComparisonExpression(LESS_THAN_OR_EQUAL, offsetSymbol.toSymbolReference(), new Constant(decimalType, Int128.valueOf(Long.MAX_VALUE))),
                        new Cast(offsetSymbol.toSymbolReference(), BIGINT),
                        new Constant(BIGINT, Long.MAX_VALUE));
            }
        }
        else {
            offsetToBigint = new Cast(
                    offsetSymbol.toSymbolReference(),
                    BIGINT,
                    false);
        }

        Symbol coercedOffsetSymbol = symbolAllocator.newSymbol(offsetToBigint, BIGINT);
        subPlan = subPlan.withNewRoot(new ProjectNode(
                idAllocator.getNextId(),
                subPlan.getRoot(),
                Assignments.builder()
                        .putIdentities(subPlan.getRoot().getOutputSymbols())
                        .put(coercedOffsetSymbol, offsetToBigint)
                        .build()));

        return new FrameOffsetPlanAndSymbol(subPlan, Optional.of(coercedOffsetSymbol));
    }

    private static Expression zeroOfType(Type type)
    {
        if (type.equals(BIGINT) ||
                type.equals(INTEGER) ||
                type.equals(SMALLINT) ||
                type.equals(TINYINT) ||
                type.equals(INTERVAL_DAY_TIME) ||
                type.equals(INTERVAL_YEAR_MONTH)) {
            return new Constant(type, 0L);
        }

        if (type.equals(DOUBLE)) {
            return new Constant(DOUBLE, 0.0);
        }

        if (type instanceof DecimalType decimal) {
            if (decimal.isShort()) {
                return new Constant(type, 0L);
            }

            return new Constant(type, Int128.valueOf(0));
        }

        if (type.equals(REAL)) {
            return new Constant(type, Reals.toReal(0));
        }

        throw new IllegalArgumentException("unexpected type: " + type);
    }

    private FrameBoundType mapFrameBoundType(FrameBound.Type type)
    {
        return switch (type) {
            case UNBOUNDED_PRECEDING -> FrameBoundType.UNBOUNDED_PRECEDING;
            case PRECEDING -> FrameBoundType.PRECEDING;
            case CURRENT_ROW -> CURRENT_ROW;
            case FOLLOWING -> FrameBoundType.FOLLOWING;
            case UNBOUNDED_FOLLOWING -> FrameBoundType.UNBOUNDED_FOLLOWING;
        };
    }

    public static DataOrganizationSpecification planWindowSpecification(List<io.trino.sql.tree.Expression> partitionBy, Optional<OrderBy> orderBy, Function<io.trino.sql.tree.Expression, Symbol> expressionRewrite)
    {
        // Rewrite PARTITION BY
        ImmutableList.Builder<Symbol> partitionBySymbols = ImmutableList.builder();
        for (io.trino.sql.tree.Expression expression : partitionBy) {
            partitionBySymbols.add(expressionRewrite.apply(expression));
        }

        // Rewrite ORDER BY
        LinkedHashMap<Symbol, SortOrder> orderings = new LinkedHashMap<>();
        for (SortItem item : getSortItemsFromOrderBy(orderBy)) {
            Symbol symbol = expressionRewrite.apply(item.getSortKey());
            // don't override existing keys, i.e. when "ORDER BY a ASC, a DESC" is specified
            orderings.putIfAbsent(symbol, sortItemToSortOrder(item));
        }

        Optional<OrderingScheme> orderingScheme = Optional.empty();
        if (!orderings.isEmpty()) {
            orderingScheme = Optional.of(new OrderingScheme(ImmutableList.copyOf(orderings.keySet()), orderings));
        }

        return new DataOrganizationSpecification(partitionBySymbols.build(), orderingScheme);
    }

    private PlanBuilder planWindowMeasures(Node node, PlanBuilder subPlan, List<WindowOperation> windowMeasures)
    {
        if (windowMeasures.isEmpty()) {
            return subPlan;
        }

        for (WindowOperation windowMeasure : scopeAwareDistinct(subPlan, windowMeasures)) {
            ResolvedWindow window = analysis.getWindow(windowMeasure);
            checkState(window != null, "no resolved window for: " + windowMeasure);

            // pre-project inputs
            ImmutableList.Builder<io.trino.sql.tree.Expression> inputsBuilder = ImmutableList.<io.trino.sql.tree.Expression>builder()
                    .addAll(window.getPartitionBy())
                    .addAll(getSortItemsFromOrderBy(window.getOrderBy()).stream()
                            .map(SortItem::getSortKey)
                            .iterator());
            WindowFrame frame = window.getFrame().orElseThrow();
            Optional<io.trino.sql.tree.Expression> endValue = frame.getEnd().orElseThrow().getValue();

            List<io.trino.sql.tree.Expression> inputs = inputsBuilder.build();

            subPlan = subqueryPlanner.handleSubqueries(subPlan, inputs, analysis.getSubqueries(node));
            subPlan = subPlan.appendProjections(inputs, symbolAllocator, idAllocator);

            // Add projection for frame end, since WindowNode expects a symbol and does not support literals
            // We don't use appendProjects because we don't want a mapping to be added for the literal
            Optional<Symbol> endValueSymbol = Optional.empty();
            if (endValue.isPresent()) {
                io.trino.sql.tree.Expression expression = endValue.get();
                Assignments.Builder assignments = Assignments.builder();
                assignments.putIdentities(subPlan.getRoot().getOutputSymbols());
                Symbol symbol = symbolAllocator.newSymbol("end", analysis.getType(expression));
                assignments.put(symbol, subPlan.rewrite(expression));

                endValueSymbol = Optional.of(symbol);
                subPlan = subPlan.withNewRoot(new ProjectNode(idAllocator.getNextId(), subPlan.getRoot(), assignments.build()));
            }

            // process frame end
            FrameOffsetPlanAndSymbol plan = planFrameOffset(subPlan, endValueSymbol);
            subPlan = plan.getSubPlan();
            Optional<Symbol> frameEnd = plan.getFrameOffsetSymbol();

            subPlan = subqueryPlanner.handleSubqueries(subPlan, extractPatternRecognitionExpressions(frame.getVariableDefinitions(), frame.getMeasures()), analysis.getSubqueries(node));
            subPlan = planPatternRecognition(subPlan, windowMeasure, window, frameEnd);
        }

        return subPlan;
    }

    public static List<io.trino.sql.tree.Expression> extractPatternRecognitionExpressions(List<VariableDefinition> variableDefinitions, List<MeasureDefinition> measureDefinitions)
    {
        ImmutableList.Builder<io.trino.sql.tree.Expression> expressions = ImmutableList.builder();

        variableDefinitions.stream()
                .map(VariableDefinition::getExpression)
                .forEach(expressions::add);

        measureDefinitions.stream()
                .map(MeasureDefinition::getExpression)
                .forEach(expressions::add);

        return expressions.build();
    }

    /**
     * Creates a projection with any additional coercions by identity of the provided expressions.
     *
     * @return the new subplan and a mapping of each expression to the symbol representing the coercion or an existing symbol if a coercion wasn't needed
     */
    public static PlanAndMappings coerce(PlanBuilder subPlan, List<io.trino.sql.tree.Expression> expressions, Analysis analysis, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator)
    {
        Assignments.Builder assignments = Assignments.builder();
        assignments.putIdentities(subPlan.getRoot().getOutputSymbols());

        Map<NodeRef<io.trino.sql.tree.Expression>, Symbol> mappings = new HashMap<>();
        for (io.trino.sql.tree.Expression expression : expressions) {
            Type coercion = analysis.getCoercion(expression);

            // expressions may be repeated, for example, when resolving ordinal references in a GROUP BY clause
            if (!mappings.containsKey(NodeRef.of(expression))) {
                if (coercion != null) {
                    Symbol symbol = symbolAllocator.newSymbol("expr", coercion);

                    assignments.put(symbol, new Cast(
                            subPlan.rewrite(expression),
                            coercion,
                            false));

                    mappings.put(NodeRef.of(expression), symbol);
                }
                else {
                    mappings.put(NodeRef.of(expression), subPlan.translate(expression));
                }
            }
        }

        subPlan = subPlan.withNewRoot(
                new ProjectNode(
                        idAllocator.getNextId(),
                        subPlan.getRoot(),
                        assignments.build()));

        return new PlanAndMappings(subPlan, mappings);
    }

    public static Expression coerceIfNecessary(Analysis analysis, io.trino.sql.tree.Expression original, Expression rewritten)
    {
        Type coercion = analysis.getCoercion(original);
        if (coercion == null) {
            return rewritten;
        }

        return new Cast(rewritten, coercion, false);
    }

    public static NodeAndMappings coerce(RelationPlan plan, List<Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        List<Symbol> visibleFields = visibleFields(plan);
        checkArgument(visibleFields.size() == types.size());

        Assignments.Builder assignments = Assignments.builder();
        ImmutableList.Builder<Symbol> mappings = ImmutableList.builder();
        for (int i = 0; i < types.size(); i++) {
            Symbol input = visibleFields.get(i);
            Type type = types.get(i);

            if (!symbolAllocator.getTypes().get(input).equals(type)) {
                Symbol coerced = symbolAllocator.newSymbol(input.getName(), type);
                assignments.put(coerced, new Cast(input.toSymbolReference(), type));
                mappings.add(coerced);
            }
            else {
                assignments.putIdentity(input);
                mappings.add(input);
            }
        }

        ProjectNode coerced = new ProjectNode(idAllocator.getNextId(), plan.getRoot(), assignments.build());
        return new NodeAndMappings(coerced, mappings.build());
    }

    public static List<Symbol> visibleFields(RelationPlan subPlan)
    {
        RelationType descriptor = subPlan.getDescriptor();
        return descriptor.getAllFields().stream()
                .filter(field -> !field.isHidden())
                .map(descriptor::indexOf)
                .map(subPlan.getFieldMappings()::get)
                .collect(toImmutableList());
    }

    public static NodeAndMappings pruneInvisibleFields(RelationPlan plan, PlanNodeIdAllocator idAllocator)
    {
        List<Symbol> visibleFields = visibleFields(plan);
        ProjectNode pruned = new ProjectNode(idAllocator.getNextId(), plan.getRoot(), Assignments.identity(visibleFields));
        return new NodeAndMappings(pruned, visibleFields);
    }

    public static NodeAndMappings disambiguateOutputs(NodeAndMappings plan, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        Set<Symbol> distinctOutputs = ImmutableSet.copyOf(plan.getFields());

        if (distinctOutputs.size() < plan.getFields().size()) {
            Assignments.Builder assignments = Assignments.builder();
            ImmutableList.Builder<Symbol> newOutputs = ImmutableList.builder();
            Set<Symbol> uniqueOutputs = new HashSet<>();

            for (Symbol output : plan.getFields()) {
                if (uniqueOutputs.add(output)) {
                    assignments.putIdentity(output);
                    newOutputs.add(output);
                }
                else {
                    Symbol newOutput = symbolAllocator.newSymbol(output);
                    assignments.put(newOutput, output.toSymbolReference());
                    newOutputs.add(newOutput);
                }
            }

            return new NodeAndMappings(new ProjectNode(idAllocator.getNextId(), plan.getNode(), assignments.build()), newOutputs.build());
        }

        return plan;
    }

    private PlanBuilder distinct(PlanBuilder subPlan, QuerySpecification node, List<io.trino.sql.tree.Expression> expressions)
    {
        if (node.getSelect().isDistinct()) {
            List<Symbol> symbols = expressions.stream()
                    .map(subPlan::translate)
                    .collect(Collectors.toList());

            return subPlan.withNewRoot(
                    singleAggregation(
                            idAllocator.getNextId(),
                            subPlan.getRoot(),
                            ImmutableMap.of(),
                            singleGroupingSet(symbols)));
        }

        return subPlan;
    }

    private Optional<OrderingScheme> orderingScheme(PlanBuilder subPlan, Optional<OrderBy> orderBy, List<io.trino.sql.tree.Expression> orderByExpressions)
    {
        if (orderBy.isEmpty() || (isSkipRedundantSort(session) && analysis.isOrderByRedundant(orderBy.get()))) {
            return Optional.empty();
        }

        Iterator<SortItem> sortItems = orderBy.get().getSortItems().iterator();

        ImmutableList.Builder<Symbol> orderBySymbols = ImmutableList.builder();
        Map<Symbol, SortOrder> orderings = new HashMap<>();
        for (io.trino.sql.tree.Expression fieldOrExpression : orderByExpressions) {
            Symbol symbol = subPlan.translate(fieldOrExpression);

            SortItem sortItem = sortItems.next();
            if (!orderings.containsKey(symbol)) {
                orderBySymbols.add(symbol);
                orderings.put(symbol, sortItemToSortOrder(sortItem));
            }
        }
        return Optional.of(new OrderingScheme(orderBySymbols.build(), orderings));
    }

    private PlanBuilder sort(PlanBuilder subPlan, Optional<OrderingScheme> orderingScheme)
    {
        if (orderingScheme.isEmpty()) {
            return subPlan;
        }

        return subPlan.withNewRoot(
                new SortNode(
                        idAllocator.getNextId(),
                        subPlan.getRoot(),
                        orderingScheme.get(),
                        false));
    }

    private PlanBuilder offset(PlanBuilder subPlan, Optional<Offset> offset)
    {
        if (offset.isEmpty()) {
            return subPlan;
        }

        return subPlan.withNewRoot(
                new OffsetNode(
                        idAllocator.getNextId(),
                        subPlan.getRoot(),
                        analysis.getOffset(offset.get())));
    }

    private PlanBuilder limit(PlanBuilder subPlan, Optional<Node> limit, Optional<OrderingScheme> orderingScheme)
    {
        if (limit.isPresent() && analysis.getLimit(limit.get()).isPresent()) {
            Optional<OrderingScheme> tiesResolvingScheme = Optional.empty();
            if (limit.get() instanceof FetchFirst && ((FetchFirst) limit.get()).isWithTies()) {
                tiesResolvingScheme = orderingScheme;
            }
            return subPlan.withNewRoot(
                    new LimitNode(
                            idAllocator.getNextId(),
                            subPlan.getRoot(),
                            analysis.getLimit(limit.get()).getAsLong(),
                            tiesResolvingScheme,
                            false,
                            ImmutableList.of()));
        }
        return subPlan;
    }

    public static class PlanAndMappings
    {
        private final PlanBuilder subPlan;
        private final Map<NodeRef<io.trino.sql.tree.Expression>, Symbol> mappings;

        public PlanAndMappings(PlanBuilder subPlan, Map<NodeRef<io.trino.sql.tree.Expression>, Symbol> mappings)
        {
            this.subPlan = subPlan;
            this.mappings = ImmutableMap.copyOf(mappings);
        }

        public PlanBuilder getSubPlan()
        {
            return subPlan;
        }

        public Symbol get(Expression expression)
        {
            return tryGet(expression)
                    .orElseThrow(() -> new IllegalArgumentException(format("No mapping for expression: %s (%s)", expression, System.identityHashCode(expression))));
        }

        public Optional<Symbol> tryGet(Expression expression)
        {
            Symbol result = mappings.get(NodeRef.of(expression));

            if (result != null) {
                return Optional.of(result);
            }

            return Optional.empty();
        }
    }

    private static class AggregationAssignment {
        private final Symbol symbol;
        private final Expression astExpression;
        private final Aggregation aggregation;

        public AggregationAssignment(Symbol symbol, Expression astExpression, Aggregation aggregation) {
            this.astExpression = astExpression;
            this.symbol = symbol;
            this.aggregation = aggregation;
        }

        public Symbol getSymbol()
        {
            return symbol;
        }

        public Expression getAstExpression()
        {
            return astExpression;
        }

        public Aggregation getRewritten()
        {
            return aggregation;
        }
    }

    private static class FrameBoundPlanAndSymbols
    {
        private final PlanBuilder subPlan;
        private final Optional<Symbol> frameBoundSymbol;
        private final Optional<Symbol> sortKeyCoercedForFrameBoundComparison;

        public FrameBoundPlanAndSymbols(PlanBuilder subPlan, Optional<Symbol> frameBoundSymbol, Optional<Symbol> sortKeyCoercedForFrameBoundComparison)
        {
            this.subPlan = subPlan;
            this.frameBoundSymbol = frameBoundSymbol;
            this.sortKeyCoercedForFrameBoundComparison = sortKeyCoercedForFrameBoundComparison;
        }

        public PlanBuilder getSubPlan()
        {
            return subPlan;
        }

        public Optional<Symbol> getFrameBoundSymbol()
        {
            return frameBoundSymbol;
        }

        public Optional<Symbol> getSortKeyCoercedForFrameBoundComparison()
        {
            return sortKeyCoercedForFrameBoundComparison;
        }
    }

    private static class FrameOffsetPlanAndSymbol
    {
        private final PlanBuilder subPlan;
        private final Optional<Symbol> frameOffsetSymbol;

        public FrameOffsetPlanAndSymbol(PlanBuilder subPlan, Optional<Symbol> frameOffsetSymbol)
        {
            this.subPlan = subPlan;
            this.frameOffsetSymbol = frameOffsetSymbol;
        }

        public PlanBuilder getSubPlan()
        {
            return subPlan;
        }

        public Optional<Symbol> getFrameOffsetSymbol()
        {
            return frameOffsetSymbol;
        }
    }
}
