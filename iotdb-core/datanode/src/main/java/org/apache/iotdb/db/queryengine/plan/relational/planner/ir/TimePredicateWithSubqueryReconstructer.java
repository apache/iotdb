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
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Identifier;
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

public class TimePredicateWithSubqueryReconstructer {

  private static final SqlParser relationSqlParser = new SqlParser();

  private static final Coordinator coordinator = Coordinator.getInstance();

  private TimePredicateWithSubqueryReconstructer() {
    // utility class
  }

  public static void reconstructTimePredicateWithSubquery(
      Expression expression, MPPQueryContext context) {
    if (expression instanceof LogicalExpression) {
      LogicalExpression logicalExpression = (LogicalExpression) expression;
      for (Expression term : logicalExpression.getTerms()) {
        reconstructTimePredicateWithSubquery(term, context);
      }
    } else if (expression instanceof NotExpression) {
      NotExpression notExpression = (NotExpression) expression;
      reconstructTimePredicateWithSubquery(notExpression.getValue(), context);
    } else if (expression instanceof ComparisonExpression) {
      ComparisonExpression comparisonExpression = (ComparisonExpression) expression;
      Expression left = comparisonExpression.getLeft();
      Expression right = comparisonExpression.getRight();
      if (left instanceof Identifier
          && ((Identifier) left).getValue().equals("time")
          && right instanceof SubqueryExpression) {
        Optional<LongLiteral> result =
            fetchSubqueryResultForTimePredicate((SubqueryExpression) right, context);
        if (result.isPresent()) {
          right = result.get();
        }
      } else if (right instanceof Identifier
          && ((Identifier) right).getValue().equals("time")
          && left instanceof SubqueryExpression) {
        Optional<LongLiteral> result =
            fetchSubqueryResultForTimePredicate((SubqueryExpression) left, context);
        if (result.isPresent()) {
          left = result.get();
        }
      }
      comparisonExpression.setLeft(left);
      comparisonExpression.setRight(right);
    }
  }

  private static Optional<LongLiteral> fetchSubqueryResultForTimePredicate(
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
              "Fetch Subquery Result for Time Predicate",
              LocalExecutionPlanner.getInstance().metadata,
              context.getTimeOut(),
              false);

      if (executionResult.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return Optional.empty();
      }

      while (coordinator.getQueryExecution(queryId).hasNextResult()) {
        final Optional<TsBlock> tsBlock;
        try {
          tsBlock = coordinator.getQueryExecution(queryId).getBatchResult();
        } catch (final IoTDBException e) {
          t = e;
          throw new RuntimeException("Fetch Table Device Schema failed. ", e);
        }
        if (!tsBlock.isPresent() || tsBlock.get().isEmpty()) {
          continue;
        }
        final Column[] columns = tsBlock.get().getValueColumns();
        checkArgument(columns.length == 1, "Subquery result should have only one column.");
        checkArgument(
            tsBlock.get().getPositionCount() == 1 && !tsBlock.get().getColumn(0).isNull(0),
            "Subquery result should have only one row.");
        // result of subquery expression in time predicate must be numeric, otherwise Exception will
        // be thrown
        final long value = columns[0].getLong(0);
        return Optional.of(new LongLiteral(Long.toString(value)));
      }
    } catch (final Throwable throwable) {
      t = throwable;
    } finally {
      coordinator.cleanupQueryExecution(queryId, null, t);
    }
    return Optional.empty();
  }
}
