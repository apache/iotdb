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
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.execution.warnings.WarningCollector;
import org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Parameter;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.PipeEnriched;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WrappedInsertStatement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.rewrite.StatementRewrite;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet.ANALYZER;

public class Analyzer {

  private final StatementAnalyzerFactory statementAnalyzerFactory;

  private final MPPQueryContext context;
  private final SessionInfo session;
  private final List<Expression> parameters;

  private final Map<NodeRef<Parameter>, Expression> parameterLookup;

  private final StatementRewrite statementRewrite;

  private final WarningCollector warningCollector;

  public Analyzer(
      final MPPQueryContext context,
      final SessionInfo session,
      final StatementAnalyzerFactory statementAnalyzerFactory,
      final List<Expression> parameters,
      final Map<NodeRef<Parameter>, Expression> parameterLookup,
      final StatementRewrite statementRewrite,
      final WarningCollector warningCollector) {
    this.context = context;
    this.session = requireNonNull(session, "session is null");
    this.statementAnalyzerFactory =
        requireNonNull(statementAnalyzerFactory, "statementAnalyzerFactory is null");
    this.parameters = parameters;
    this.parameterLookup = parameterLookup;
    this.statementRewrite = statementRewrite;
    this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
  }

  public Analysis analyze(Statement statement) {
    Statement rewrittenStatement =
        statementRewrite.rewrite(
            statementAnalyzerFactory,
            session,
            statement,
            parameters,
            parameterLookup,
            warningCollector);

    Analysis analysis = new Analysis(rewrittenStatement, parameterLookup);
    Statement innerStatement =
        rewrittenStatement instanceof PipeEnriched
            ? ((PipeEnriched) rewrittenStatement).getInnerStatement()
            : rewrittenStatement;
    if (innerStatement instanceof WrappedInsertStatement) {
      WrappedInsertStatement insertStatement = (WrappedInsertStatement) innerStatement;
      if (insertStatement.getDatabase() != null) {
        analysis.setDatabaseName(insertStatement.getDatabase());
      } else if (session.getDatabaseName().isPresent()) {
        analysis.setDatabaseName(session.getDatabaseName().get());
      } else {
        throw new SemanticException("database is not specified for insert:" + statement);
      }
    } else if (session.getDatabaseName().isPresent()) {
      analysis.setDatabaseName(session.getDatabaseName().get());
    }

    long startTime = System.nanoTime();
    StatementAnalyzer analyzer =
        statementAnalyzerFactory.createStatementAnalyzer(
            analysis, context, session, warningCollector, CorrelationSupport.ALLOWED);

    analyzer.analyze(rewrittenStatement);
    if (analysis.isQuery()) {
      long analyzeCost = System.nanoTime() - startTime;
      QueryPlanCostMetricSet.getInstance().recordTreePlanCost(ANALYZER, analyzeCost);
      context.setAnalyzeCost(analyzeCost);
    }

    return analysis;
  }
}
