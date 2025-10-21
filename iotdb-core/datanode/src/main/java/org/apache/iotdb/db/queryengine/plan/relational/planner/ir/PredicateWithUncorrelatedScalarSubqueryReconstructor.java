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
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BinaryLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BooleanLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DoubleLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Literal;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NotExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SubqueryExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.block.TsBlock;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public class PredicateWithUncorrelatedScalarSubqueryReconstructor {

  private static final SqlParser relationSqlParser = new SqlParser();

  private static final Coordinator coordinator = Coordinator.getInstance();

  private PredicateWithUncorrelatedScalarSubqueryReconstructor() {
    // utility class
  }

  public static void reconstructPredicateWithUncorrelatedScalarSubquery(
      Expression expression, MPPQueryContext context) {
    if (expression instanceof LogicalExpression) {
      LogicalExpression logicalExpression = (LogicalExpression) expression;
      for (Expression term : logicalExpression.getTerms()) {
        reconstructPredicateWithUncorrelatedScalarSubquery(term, context);
      }
    } else if (expression instanceof NotExpression) {
      NotExpression notExpression = (NotExpression) expression;
      reconstructPredicateWithUncorrelatedScalarSubquery(notExpression.getValue(), context);
    } else if (expression instanceof ComparisonExpression) {
      ComparisonExpression comparisonExpression = (ComparisonExpression) expression;
      Expression left = comparisonExpression.getLeft();
      Expression right = comparisonExpression.getRight();
      if (left instanceof Identifier && right instanceof SubqueryExpression) {
        Optional<Literal> result =
            fetchUncorrelatedSubqueryResultForPredicate((SubqueryExpression) right, context);
        // If the subquery result is not present, we cannot reconstruct the predicate.
        if (result.isPresent()) {
          right = result.get();
        }
      } else if (right instanceof Identifier && left instanceof SubqueryExpression) {
        Optional<Literal> result =
            fetchUncorrelatedSubqueryResultForPredicate((SubqueryExpression) left, context);
        if (result.isPresent()) {
          left = result.get();
        }
      }
      comparisonExpression.setLeft(left);
      comparisonExpression.setRight(right);
    }
  }

  /**
   * @return an Optional containing the result of the uncorrelated scalar subquery. Returns
   *     Optional.empty() if the subquery cannot be executed in advance or if it does not return a
   *     valid result.
   */
  private static Optional<Literal> fetchUncorrelatedSubqueryResultForPredicate(
      SubqueryExpression subqueryExpression, MPPQueryContext context) {
    final long queryId = SessionManager.getInstance().requestQueryId();
    Throwable t = null;

    try {
      final ExecutionResult executionResult =
          coordinator.executeForTableModel(
              subqueryExpression.getQuery(),
              relationSqlParser,
              SessionManager.getInstance().getCurrSession(),
              queryId,
              SessionManager.getInstance()
                  .getSessionInfoOfTableModel(SessionManager.getInstance().getCurrSession()),
              "Try to Fetch Uncorrelated Scalar Subquery Result for Predicate",
              LocalExecutionPlanner.getInstance().metadata,
              context.getCteDataStores(),
              context.getTimeOut(),
              false);

      // This may occur when the subquery cannot be executed in advance (for example, with
      // correlated scalar subqueries).
      // Since we cannot determine the subquery's validity beforehand, we must submit the subquery.
      // This approach may slow down filter involving correlated scalar subqueries.
      if (executionResult.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return Optional.empty();
      }

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
        checkArgument(columns.length == 1, "Scalar Subquery result should only have one column.");
        checkArgument(
            tsBlock.get().getPositionCount() == 1 && !tsBlock.get().getColumn(0).isNull(0),
            "Scalar Subquery result should only have one row.");
        switch (columns[0].getDataType()) {
          case INT32:
          case DATE:
            return Optional.of(new LongLiteral(Long.toString(columns[0].getInt(0))));
          case INT64:
          case TIMESTAMP:
            return Optional.of(new LongLiteral(Long.toString(columns[0].getLong(0))));
          case FLOAT:
            return Optional.of(new DoubleLiteral(Double.toString(columns[0].getFloat(0))));
          case DOUBLE:
            return Optional.of(new DoubleLiteral(Double.toString(columns[0].getDouble(0))));
          case BOOLEAN:
            return Optional.of(new BooleanLiteral(Boolean.toString(columns[0].getBoolean(0))));
          case BLOB:
          case TEXT:
          case STRING:
            return Optional.of(new BinaryLiteral(columns[0].getBinary(0).toString()));
          default:
            throw new IllegalArgumentException(
                String.format(
                    "Unsupported data type for scalar subquery result: %s",
                    columns[0].getDataType()));
        }
      }
    } catch (final Throwable throwable) {
      t = throwable;
    } finally {
      coordinator.cleanupQueryExecution(queryId, null, t);
    }
    return Optional.empty();
  }
}
