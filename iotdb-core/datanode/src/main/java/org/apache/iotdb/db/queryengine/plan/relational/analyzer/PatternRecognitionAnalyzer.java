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

package org.apache.iotdb.db.queryengine.plan.relational.analyzer;

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis.Range;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ExcludedPattern;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.MeasureDefinition;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.PatternRecognitionRelation;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.PatternRecognitionRelation.RowsPerMatch;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RangeQuantifier;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RowPattern;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SkipTo;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SubsetDefinition;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.VariableDefinition;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.ExpressionTreeUtils.extractExpressions;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ProcessingMode.Mode.FINAL;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.util.AstUtil.preOrder;

public class PatternRecognitionAnalyzer {
  private PatternRecognitionAnalyzer() {}

  public static PatternRecognitionAnalysis analyze(
      List<SubsetDefinition> subsets,
      List<VariableDefinition> variableDefinitions,
      List<MeasureDefinition> measures,
      RowPattern pattern,
      Optional<SkipTo> skipTo) {
    // extract label names (Identifiers) from PATTERN and SUBSET clauses. create labels respecting
    // SQL identifier semantics
    Set<String> primaryLabels =
        extractExpressions(ImmutableList.of(pattern), Identifier.class).stream()
            .map(PatternRecognitionAnalyzer::label)
            .collect(toImmutableSet());
    List<String> unionLabels =
        subsets.stream()
            .map(SubsetDefinition::getName)
            .map(PatternRecognitionAnalyzer::label)
            .collect(toImmutableList());

    // analyze SUBSET
    Set<String> unique = new HashSet<>();
    for (SubsetDefinition subset : subsets) {
      String label = label(subset.getName());
      if (primaryLabels.contains(label)) {
        throw new SemanticException(
            String.format(
                "union pattern variable name: %s is a duplicate of primary pattern variable name",
                subset.getName()));
      }
      if (!unique.add(label)) {
        throw new SemanticException(
            String.format("union pattern variable name: %s is declared twice", subset.getName()));
      }
      for (Identifier element : subset.getIdentifiers()) {
        if (!primaryLabels.contains(label(element))) {
          throw new SemanticException(
              String.format("subset element: %s is not a primary pattern variable", element));
        }
      }
    }

    // analyze DEFINE
    unique = new HashSet<>();
    for (VariableDefinition definition : variableDefinitions) {
      String label = label(definition.getName());
      if (!primaryLabels.contains(label)) {
        throw new SemanticException(
            String.format(
                "defined variable: %s is not a primary pattern variable", definition.getName()));
      }
      if (!unique.add(label)) {
        throw new SemanticException(
            String.format("pattern variable with name: %s is defined twice", definition.getName()));
      }
      // DEFINE clause only supports RUNNING semantics which is default
      Expression expression = definition.getExpression();
      extractExpressions(ImmutableList.of(expression), FunctionCall.class).stream()
          .filter(
              functionCall ->
                  functionCall
                      .getProcessingMode()
                      .map(mode -> mode.getMode() == FINAL)
                      .orElse(false))
          .findFirst()
          .ifPresent(
              functionCall -> {
                throw new SemanticException(
                    String.format("FINAL semantics is not supported in DEFINE clause"));
              });
    }
    // record primary labels without definitions. they are implicitly associated with `true`
    // condition
    Set<String> undefinedLabels = Sets.difference(primaryLabels, unique);

    // validate pattern quantifiers
    ImmutableMap.Builder<NodeRef<RangeQuantifier>, Range> ranges = ImmutableMap.builder();
    preOrder(pattern)
        .filter(RangeQuantifier.class::isInstance)
        .map(RangeQuantifier.class::cast)
        .forEach(
            quantifier -> {
              Optional<Long> atLeast = quantifier.getAtLeast().map(LongLiteral::getParsedValue);
              atLeast.ifPresent(
                  value -> {
                    if (value < 0) {
                      throw new SemanticException(
                          "Pattern quantifier lower bound must be greater than or equal to 0");
                    }
                    if (value > Integer.MAX_VALUE) {
                      throw new SemanticException(
                          "Pattern quantifier lower bound must not exceed " + Integer.MAX_VALUE);
                    }
                  });
              Optional<Long> atMost = quantifier.getAtMost().map(LongLiteral::getParsedValue);
              atMost.ifPresent(
                  value -> {
                    if (value < 1) {
                      throw new SemanticException(
                          "Pattern quantifier upper bound must be greater than or equal to 1");
                    }
                    if (value > Integer.MAX_VALUE) {
                      throw new SemanticException(
                          "Pattern quantifier upper bound must not exceed " + Integer.MAX_VALUE);
                    }
                  });
              if (atLeast.isPresent() && atMost.isPresent()) {
                if (atLeast.get() > atMost.get()) {
                  throw new SemanticException(
                      "Pattern quantifier lower bound must not exceed upper bound");
                }
              }
              ranges.put(
                  NodeRef.of(quantifier),
                  new Range(atLeast.map(Math::toIntExact), atMost.map(Math::toIntExact)));
            });

    // validate AFTER MATCH SKIP
    Set<String> allLabels =
        ImmutableSet.<String>builder().addAll(primaryLabels).addAll(unionLabels).build();
    skipTo
        .flatMap(SkipTo::getIdentifier)
        .ifPresent(
            identifier -> {
              String label = label(identifier);
              if (!allLabels.contains(label)) {
                throw new SemanticException(
                    String.format("%s is not a primary or union pattern variable", identifier));
              }
            });

    // check no prohibited nesting: cannot nest one row pattern recognition within another
    List<Expression> expressions =
        Streams.concat(
                measures.stream().map(MeasureDefinition::getExpression),
                variableDefinitions.stream().map(VariableDefinition::getExpression))
            .collect(toImmutableList());
    expressions.forEach(
        expression ->
            preOrder(expression)
                .filter(
                    child ->
                        child instanceof PatternRecognitionRelation || child instanceof RowPattern)
                .findFirst()
                .ifPresent(
                    nested -> {
                      throw new SemanticException(
                          "nested row pattern recognition in row pattern recognition");
                    }));

    return new PatternRecognitionAnalysis(allLabels, undefinedLabels, ranges.buildOrThrow());
  }

  public static void validatePatternExclusions(
      Optional<RowsPerMatch> rowsPerMatch, RowPattern pattern) {
    // exclusion syntax is not allowed in row pattern if ALL ROWS PER MATCH WITH UNMATCHED ROWS is
    // specified
    if (rowsPerMatch.isPresent() && (rowsPerMatch.get() == RowsPerMatch.ALL_WITH_UNMATCHED)) {
      preOrder(pattern)
          .filter(ExcludedPattern.class::isInstance)
          .findFirst()
          .ifPresent(
              exclusion -> {
                throw new SemanticException(
                    "Pattern exclusion syntax is not allowed when ALL ROWS PER MATCH WITH UNMATCHED ROWS is specified");
              });
    }
  }

  private static String label(Identifier identifier) {
    return identifier.getCanonicalValue();
  }
}
