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

package org.apache.iotdb.db.queryengine.plan.relational.planner.ir;

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext.ExplainType;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BinaryLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BooleanLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DereferenceExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DoubleLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Literal;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NotExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.StringLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SubqueryExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.With;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WithQuery;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

public class PredicateWithUncorrelatedScalarSubqueryReconstructor {

  private static final Coordinator coordinator = Coordinator.getInstance();

  public PredicateWithUncorrelatedScalarSubqueryReconstructor() {}

  public void reconstructPredicateWithUncorrelatedScalarSubquery(
      MPPQueryContext context, Analysis analysis, Expression expression) {
    if (expression instanceof LogicalExpression) {
      LogicalExpression logicalExpression = (LogicalExpression) expression;
      for (Expression term : logicalExpression.getTerms()) {
        reconstructPredicateWithUncorrelatedScalarSubquery(context, analysis, term);
      }
    } else if (expression instanceof NotExpression) {
      NotExpression notExpression = (NotExpression) expression;
      reconstructPredicateWithUncorrelatedScalarSubquery(
          context, analysis, notExpression.getValue());
    } else if (expression instanceof ComparisonExpression) {
      ComparisonExpression comparisonExpression = (ComparisonExpression) expression;
      Expression left = comparisonExpression.getLeft();
      Expression right = comparisonExpression.getRight();
      if ((left instanceof Identifier
              || left instanceof FunctionCall
              || left instanceof DereferenceExpression)
          && right instanceof SubqueryExpression) {
        Optional<Literal> result =
            fetchUncorrelatedSubqueryResultForPredicate(
                context, analysis.getSqlParser(), (SubqueryExpression) right, analysis.getWith());
        // If the subquery result is not present, we cannot reconstruct the predicate.
        result.ifPresent(comparisonExpression::setShadowRight);
      } else if ((right instanceof Identifier
              || right instanceof FunctionCall
              || right instanceof DereferenceExpression)
          && left instanceof SubqueryExpression) {
        Optional<Literal> result =
            fetchUncorrelatedSubqueryResultForPredicate(
                context, analysis.getSqlParser(), (SubqueryExpression) left, analysis.getWith());
        result.ifPresent(comparisonExpression::setShadowLeft);
      }
    }
  }

  /**
   * @return an Optional containing the result of the uncorrelated scalar subquery. Returns
   *     Optional.empty() if the subquery cannot be executed in advance or if it does not return a
   *     valid result.
   */
  public Optional<Literal> fetchUncorrelatedSubqueryResultForPredicate(
      MPPQueryContext context,
      SqlParser relationSqlParser,
      SubqueryExpression subqueryExpression,
      With with) {
    final long queryId = SessionManager.getInstance().requestQueryId();
    Throwable t = null;

    try {
      Query query = subqueryExpression.getQuery();
      Query q = query;
      if (with != null) {
        List<Identifier> tables = context.getTables(query);
        List<WithQuery> withQueries =
            with.getQueries().stream()
                .filter(
                    x ->
                        tables.contains(x.getName())
                            && !x.getQuery().isMaterialized()
                            && !x.getQuery().isDone())
                .collect(Collectors.toList());

        if (!withQueries.isEmpty()) {
          With w = new With(with.getLocation().orElse(null), with.isRecursive(), withQueries);
          q =
              new Query(
                  Optional.of(w),
                  query.getQueryBody(),
                  query.getFill(),
                  query.getOrderBy(),
                  query.getOffset(),
                  query.getLimit());
        }
      }
      final ExecutionResult executionResult =
          coordinator.executeForTableModel(
              q,
              relationSqlParser,
              SessionManager.getInstance().getCurrSession(),
              queryId,
              SessionManager.getInstance()
                  .getSessionInfoOfTableModel(SessionManager.getInstance().getCurrSession()),
              "Try to Fetch Uncorrelated Scalar Subquery Result for Predicate",
              LocalExecutionPlanner.getInstance().metadata,
              context.getCteQueries(),
              ExplainType.NONE,
              context.getTimeOut(),
              false);

      // This may occur when the subquery cannot be executed in advance (for example, with
      // correlated scalar subqueries).
      // Since we cannot determine the subquery's validity beforehand, we must submit the subquery.
      // This approach may slow down filter involving correlated scalar subqueries.
      if (executionResult.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return Optional.empty();
      }

      Column column = null;
      TSDataType dataType = null;
      int rowCount = 0;
      while (coordinator.getQueryExecution(queryId).hasNextResult()) {
        final Optional<TsBlock> tsBlock;
        try {
          tsBlock = coordinator.getQueryExecution(queryId).getBatchResult();
        } catch (final IoTDBException e) {
          t = e;
          throw new RuntimeException("Failed to Fetch Subquery Result.", e);
        }
        if (!tsBlock.isPresent() || tsBlock.get().isEmpty()) {
          continue;
        }
        final Column[] columns = tsBlock.get().getValueColumns();
        // check column count
        checkArgument(columns.length == 1, "Scalar Subquery result should only have one column.");
        // check row count
        rowCount += tsBlock.get().getPositionCount();
        checkArgument(
            rowCount == 1 && !columns[0].isNull(0),
            "Scalar Subquery result should only have one row.");
        column = columns[0];

        // column type
        DatasetHeader datasetHeader = coordinator.getQueryExecution(queryId).getDatasetHeader();
        List<TSDataType> dataTypes = datasetHeader.getRespDataTypes();
        checkArgument(dataTypes.size() == 1, "Scalar Subquery result should only have one column.");
        dataType = dataTypes.get(0);
      }
      checkArgument(
          dataType != null && column != null,
          "Scalar Subquery result should not get null dataType or null column.");
      switch (dataType) {
        case INT32:
        case DATE:
          return Optional.of(new LongLiteral(Long.toString(column.getInt(0))));
        case INT64:
        case TIMESTAMP:
          return Optional.of(new LongLiteral(Long.toString(column.getLong(0))));
        case FLOAT:
          return Optional.of(new DoubleLiteral(Double.toString(column.getFloat(0))));
        case DOUBLE:
          return Optional.of(new DoubleLiteral(Double.toString(column.getDouble(0))));
        case BOOLEAN:
          return Optional.of(new BooleanLiteral(Boolean.toString(column.getBoolean(0))));
        case BLOB:
          return Optional.of(new BinaryLiteral(column.getBinary(0).toString()));
        case TEXT:
        case STRING:
          return Optional.of(new StringLiteral(column.getBinary(0).toString()));
        default:
          throw new IllegalArgumentException(
              String.format(
                  "Unsupported data type for scalar subquery result: %s", column.getDataType()));
      }
    } catch (final Throwable throwable) {
      t = throwable;
    } finally {
      coordinator.cleanupQueryExecution(queryId, null, t);
    }
    return Optional.empty();
  }

  public void clearShadowExpression(Expression expression) {
    if (expression instanceof LogicalExpression) {
      LogicalExpression logicalExpression = (LogicalExpression) expression;
      for (Expression term : logicalExpression.getTerms()) {
        clearShadowExpression(term);
      }
    } else if (expression instanceof NotExpression) {
      NotExpression notExpression = (NotExpression) expression;
      clearShadowExpression(notExpression.getValue());
    } else if (expression instanceof ComparisonExpression) {
      ComparisonExpression comparisonExpression = (ComparisonExpression) expression;
      comparisonExpression.clearShadow();
      clearShadowExpression(comparisonExpression.getLeft());
      clearShadowExpression(comparisonExpression.getRight());
    }
  }
}
