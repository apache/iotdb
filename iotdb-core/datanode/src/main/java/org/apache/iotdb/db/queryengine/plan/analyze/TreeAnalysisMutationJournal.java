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
package org.apache.iotdb.db.queryengine.plan.analyze;

import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.statement.component.GroupByTimeComponent;
import org.apache.iotdb.db.queryengine.plan.statement.component.OrderByComponent;
import org.apache.iotdb.db.queryengine.plan.statement.component.ResultColumn;
import org.apache.iotdb.db.queryengine.plan.statement.component.WhereCondition;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;

import java.util.List;
import java.util.Set;

/**
 * Records the parser-owned state changed by one tree-query analysis attempt.
 *
 * <p>A retryable dispatch failure causes {@code QueryExecution} to analyze the same statement
 * object again. The journal records only the references and primitive values that an analysis
 * attempt replaces. Objects reachable from a recorded reference must therefore remain untouched;
 * callers that need to change a nested mutable object must first obtain a copy-on-write working
 * object from this class.
 *
 * <p>The journal belongs to the planner for one query execution. It is not a general AST snapshot
 * and must not be used by cached relational prepared-statement ASTs.
 */
public final class TreeAnalysisMutationJournal {

  private static final int SELECT_RESULT_COLUMNS = 1;
  private static final int WHERE_CONDITION = 1 << 1;
  private static final int GROUP_BY_CONTROL_EXPRESSION = 1 << 2;
  private static final int ORDER_BY_COMPONENT = 1 << 3;
  private static final int LIMIT_OFFSET = 1 << 4;
  private static final int COUNT_TIME_AGGREGATION = 1 << 5;

  private boolean active;
  private int recordedFields;
  private QueryStatement statement;

  private List<ResultColumn> originalResultColumns;
  private WhereCondition originalWhereCondition;
  private Expression originalGroupByControlExpression;
  private OrderByComponent originalOrderByComponent;
  private boolean orderByDetached;

  private long originalRowLimit;
  private long originalRowOffset;
  private boolean originalResultSetEmpty;
  private GroupByTimeComponent originalGroupByTimeComponent;
  private long originalGroupByStartTime;
  private long originalGroupByEndTime;
  private boolean originalCountTimeAggregation;

  /** Starts a new attempt, rolling back an unfinished preceding attempt if necessary. */
  public void begin() {
    if (active) {
      rollback();
    }
    active = true;
  }

  /**
   * Makes repeated direct Analyzer invocations behave like QueryExecution retry attempts.
   *
   * <p>Production code starts and finishes attempts through the planner lifecycle. Unit tests and a
   * few internal callers invoke Analyzer directly, so Analyzer calls this method before walking the
   * statement.
   */
  public void prepareForAnalyze() {
    if (!active) {
      active = true;
    } else if (recordedFields != 0) {
      rollback();
      active = true;
    }
  }

  /** Restores the parser-owned state and releases all references held by this attempt. */
  public void rollback() {
    if (!active) {
      return;
    }
    if (statement != null) {
      if (isRecorded(SELECT_RESULT_COLUMNS)) {
        statement.getSelectComponent().setResultColumns(originalResultColumns);
      }
      if (isRecorded(WHERE_CONDITION)) {
        statement.setWhereCondition(originalWhereCondition);
      }
      if (isRecorded(GROUP_BY_CONTROL_EXPRESSION)) {
        statement
            .getGroupByComponent()
            .setControlColumnExpressionForAnalyze(originalGroupByControlExpression);
      }
      if (isRecorded(ORDER_BY_COMPONENT)) {
        statement.setOrderByComponent(originalOrderByComponent);
      }
      if (isRecorded(LIMIT_OFFSET)) {
        statement.setRowLimit(originalRowLimit);
        statement.setRowOffset(originalRowOffset);
        statement.setResultSetEmpty(originalResultSetEmpty);
        originalGroupByTimeComponent.setStartTime(originalGroupByStartTime);
        originalGroupByTimeComponent.setEndTime(originalGroupByEndTime);
      }
      if (isRecorded(COUNT_TIME_AGGREGATION)) {
        statement.setCountTimeAggregation(originalCountTimeAggregation);
      }
    }
    clear();
  }

  /**
   * Commits an attempt by discarding its undo information.
   *
   * <p>The working statement is still referenced by Analysis and the generated plans, so a
   * successful attempt is not restored after the dispatch retry window has closed.
   */
  public void commit() {
    clear();
  }

  public void replaceResultColumns(
      QueryStatement queryStatement, List<ResultColumn> resultColumns) {
    ensureActive(queryStatement);
    if (!isRecorded(SELECT_RESULT_COLUMNS)) {
      originalResultColumns = queryStatement.getSelectComponent().getResultColumns();
      recordedFields |= SELECT_RESULT_COLUMNS;
    }
    queryStatement.getSelectComponent().setResultColumns(resultColumns);
  }

  public void replaceWhereCondition(QueryStatement queryStatement, WhereCondition whereCondition) {
    ensureActive(queryStatement);
    if (!isRecorded(WHERE_CONDITION)) {
      originalWhereCondition = queryStatement.getWhereCondition();
      recordedFields |= WHERE_CONDITION;
    }
    queryStatement.setWhereCondition(whereCondition);
  }

  public void replaceGroupByControlExpression(
      QueryStatement queryStatement, Expression controlExpression) {
    ensureActive(queryStatement);
    if (!isRecorded(GROUP_BY_CONTROL_EXPRESSION)) {
      originalGroupByControlExpression =
          queryStatement.getGroupByComponent().getControlColumnExpression();
      recordedFields |= GROUP_BY_CONTROL_EXPRESSION;
    }
    queryStatement.getGroupByComponent().setControlColumnExpressionForAnalyze(controlExpression);
  }

  /**
   * Returns an attempt-local ORDER BY component whose nested lists and SortItems may be changed.
   */
  public OrderByComponent getMutableOrderByComponent(QueryStatement queryStatement) {
    ensureActive(queryStatement);
    if (queryStatement.getOrderByComponent() == null) {
      return null;
    }
    recordOrderByComponent(queryStatement);
    if (!orderByDetached) {
      queryStatement.setOrderByComponent(
          OrderByComponent.copyOf(queryStatement.getOrderByComponent()));
      orderByDetached = true;
    }
    return queryStatement.getOrderByComponent();
  }

  public void clearOrderByComponent(QueryStatement queryStatement) {
    ensureActive(queryStatement);
    recordOrderByComponent(queryStatement);
    queryStatement.setOrderByComponent(null);
  }

  public void updateSortItems(QueryStatement queryStatement, Set<Expression> orderByExpressions) {
    if (getMutableOrderByComponent(queryStatement) != null) {
      queryStatement.updateSortItems(orderByExpressions);
    }
  }

  /** Records all values changed together by LIMIT/OFFSET pushdown. */
  public void recordLimitOffsetPushDown(QueryStatement queryStatement) {
    ensureActive(queryStatement);
    if (isRecorded(LIMIT_OFFSET)) {
      return;
    }
    originalRowLimit = queryStatement.getRowLimit();
    originalRowOffset = queryStatement.getRowOffset();
    originalResultSetEmpty = queryStatement.isResultSetEmpty();
    originalGroupByTimeComponent = queryStatement.getGroupByTimeComponent();
    originalGroupByStartTime = originalGroupByTimeComponent.getStartTime();
    originalGroupByEndTime = originalGroupByTimeComponent.getEndTime();
    recordedFields |= LIMIT_OFFSET;
  }

  /** Records the derived flag that semantic validation sets for {@code count_time(*)}. */
  public void recordCountTimeAggregationChange(
      QueryStatement queryStatement, boolean originalValue) {
    ensureActive(queryStatement);
    if (!isRecorded(COUNT_TIME_AGGREGATION)) {
      originalCountTimeAggregation = originalValue;
      recordedFields |= COUNT_TIME_AGGREGATION;
    }
  }

  private void recordOrderByComponent(QueryStatement queryStatement) {
    if (!isRecorded(ORDER_BY_COMPONENT)) {
      originalOrderByComponent = queryStatement.getOrderByComponent();
      recordedFields |= ORDER_BY_COMPONENT;
    }
  }

  private boolean isRecorded(int field) {
    return (recordedFields & field) != 0;
  }

  private void ensureActive(QueryStatement queryStatement) {
    if (!active) {
      active = true;
    }
    if (statement == null) {
      statement = queryStatement;
    } else if (statement != queryStatement) {
      throw new IllegalStateException();
    }
  }

  private void clear() {
    active = false;
    recordedFields = 0;
    statement = null;
    originalResultColumns = null;
    originalWhereCondition = null;
    originalGroupByControlExpression = null;
    originalOrderByComponent = null;
    orderByDetached = false;
    originalGroupByTimeComponent = null;
  }
}
