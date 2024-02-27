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

import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.execution.warnings.WarningCollector;
import org.apache.iotdb.db.queryengine.plan.relational.security.AccessControl;
import org.apache.iotdb.db.relational.sql.tree.Expression;
import org.apache.iotdb.db.relational.sql.tree.Identifier;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ExpressionAnalyzer {

  public static ExpressionAnalysis analyzeExpression(
      SessionInfo session,
      AccessControl accessControl,
      Scope scope,
      Analysis analysis,
      Expression expression,
      WarningCollector warningCollector,
      CorrelationSupport correlationSupport) {
    //    ExpressionAnalyzer analyzer =
    //        new ExpressionAnalyzer(accessControl, analysis, session,
    //            TypeProvider.empty(), warningCollector);
    //    analyzer.analyze(expression, scope, correlationSupport);
    //
    //    updateAnalysis(analysis, analyzer, session, accessControl);
    //    analysis.addExpressionFields(expression, analyzer.getSourceFields());
    //
    //    return new ExpressionAnalysis(
    //        analyzer.getExpressionTypes(),
    //        analyzer.getExpressionCoercions(),
    //        analyzer.getSubqueryInPredicates(),
    //        analyzer.getSubqueries(),
    //        analyzer.getExistsSubqueries(),
    //        analyzer.getColumnReferences(),
    //        analyzer.getTypeOnlyCoercions(),
    //        analyzer.getQuantifiedComparisons(),
    //        analyzer.getWindowFunctions());
    return null;
  }

  public static class LabelPrefixedReference {
    private final String label;
    private final Optional<Identifier> column;

    public LabelPrefixedReference(String label, Identifier column) {
      this(label, Optional.of(requireNonNull(column, "column is null")));
    }

    public LabelPrefixedReference(String label) {
      this(label, Optional.empty());
    }

    private LabelPrefixedReference(String label, Optional<Identifier> column) {
      this.label = requireNonNull(label, "label is null");
      this.column = requireNonNull(column, "column is null");
    }

    public String getLabel() {
      return label;
    }

    public Optional<Identifier> getColumn() {
      return column;
    }
  }
}
