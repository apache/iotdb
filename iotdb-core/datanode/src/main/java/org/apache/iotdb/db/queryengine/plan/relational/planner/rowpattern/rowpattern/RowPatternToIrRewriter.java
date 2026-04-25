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

package org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.rowpattern;

import org.apache.iotdb.commons.queryengine.plan.relational.planner.rowpattern.IrAlternation;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.rowpattern.IrAnchor;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.rowpattern.IrAnchor.Type;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.rowpattern.IrConcatenation;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.rowpattern.IrEmpty;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.rowpattern.IrExclusion;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.rowpattern.IrLabel;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.rowpattern.IrPermutation;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.rowpattern.IrQuantified;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.rowpattern.IrQuantifier;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.rowpattern.IrRowPattern;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.AnchorPattern;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.EmptyPattern;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ExcludedPattern;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.OneOrMoreQuantifier;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.PatternAlternation;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.PatternConcatenation;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.PatternPermutation;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.PatternQuantifier;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.PatternVariable;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.QuantifiedPattern;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.RangeQuantifier;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.RowPattern;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ZeroOrMoreQuantifier;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ZeroOrOneQuantifier;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis.Range;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AstVisitor;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.commons.queryengine.plan.relational.planner.rowpattern.IrQuantifier.oneOrMore;
import static org.apache.iotdb.commons.queryengine.plan.relational.planner.rowpattern.IrQuantifier.range;
import static org.apache.iotdb.commons.queryengine.plan.relational.planner.rowpattern.IrQuantifier.zeroOrMore;
import static org.apache.iotdb.commons.queryengine.plan.relational.planner.rowpattern.IrQuantifier.zeroOrOne;

public class RowPatternToIrRewriter implements AstVisitor<IrRowPattern, Void> {
  private final Analysis analysis;

  public RowPatternToIrRewriter(Analysis analysis) {
    this.analysis = requireNonNull(analysis, "analysis is null");
  }

  public static IrRowPattern rewrite(RowPattern node, Analysis analysis) {
    return new RowPatternToIrRewriter(analysis).process(node);
  }

  @Override
  public IrRowPattern visitPatternAlternation(PatternAlternation node, Void context) {
    List<IrRowPattern> patterns =
        node.getPatterns().stream().map(this::process).collect(toImmutableList());

    return new IrAlternation(patterns);
  }

  @Override
  public IrRowPattern visitPatternConcatenation(PatternConcatenation node, Void context) {
    List<IrRowPattern> patterns =
        node.getPatterns().stream().map(this::process).collect(toImmutableList());

    return new IrConcatenation(patterns);
  }

  @Override
  public IrRowPattern visitQuantifiedPattern(QuantifiedPattern node, Void context) {
    IrRowPattern pattern = process(node.getPattern());
    IrQuantifier quantifier = rewritePatternQuantifier(node.getPatternQuantifier());

    return new IrQuantified(pattern, quantifier);
  }

  private IrQuantifier rewritePatternQuantifier(PatternQuantifier quantifier) {
    if (quantifier instanceof ZeroOrMoreQuantifier) {
      return zeroOrMore(quantifier.isGreedy());
    }

    if (quantifier instanceof OneOrMoreQuantifier) {
      return oneOrMore(quantifier.isGreedy());
    }

    if (quantifier instanceof ZeroOrOneQuantifier) {
      return zeroOrOne(quantifier.isGreedy());
    }

    if (quantifier instanceof RangeQuantifier) {
      Range range = analysis.getRange((RangeQuantifier) quantifier);
      return range(range.getAtLeast(), range.getAtMost(), quantifier.isGreedy());
    }

    throw new IllegalStateException(
        "unsupported pattern quantifier type: " + quantifier.getClass().getSimpleName());
  }

  @Override
  public IrRowPattern visitAnchorPattern(AnchorPattern node, Void context) {
    Type type;
    switch (node.getType()) {
      case PARTITION_START:
        type = IrAnchor.Type.PARTITION_START;
        break;
      case PARTITION_END:
        type = IrAnchor.Type.PARTITION_END;
        break;
      default:
        throw new IllegalArgumentException("Unexpected value: " + node.getType());
    }

    return new IrAnchor(type);
  }

  @Override
  public IrRowPattern visitEmptyPattern(EmptyPattern node, Void context) {
    return new IrEmpty();
  }

  @Override
  public IrRowPattern visitExcludedPattern(ExcludedPattern node, Void context) {
    IrRowPattern pattern = process(node.getPattern());

    return new IrExclusion(pattern);
  }

  @Override
  public IrRowPattern visitPatternPermutation(PatternPermutation node, Void context) {
    List<IrRowPattern> patterns =
        node.getPatterns().stream().map(this::process).collect(toImmutableList());

    return new IrPermutation(patterns);
  }

  @Override
  public IrRowPattern visitPatternVariable(PatternVariable node, Void context) {
    return new IrLabel(node.getName().getCanonicalValue());
  }
}
