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

package org.apache.iotdb.db.queryengine.plan.statement.crud;

import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.execution.operator.window.WindowType;
import org.apache.iotdb.db.queryengine.execution.operator.window.ainode.InferenceWindow;
import org.apache.iotdb.db.queryengine.plan.analyze.ExpressionAnalyzer;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.queryengine.plan.expression.visitor.CountTimeAggregationAmountVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.AuthorityInformationStatement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.component.FillComponent;
import org.apache.iotdb.db.queryengine.plan.statement.component.FromComponent;
import org.apache.iotdb.db.queryengine.plan.statement.component.GroupByComponent;
import org.apache.iotdb.db.queryengine.plan.statement.component.GroupByLevelComponent;
import org.apache.iotdb.db.queryengine.plan.statement.component.GroupByTagComponent;
import org.apache.iotdb.db.queryengine.plan.statement.component.GroupByTimeComponent;
import org.apache.iotdb.db.queryengine.plan.statement.component.HavingCondition;
import org.apache.iotdb.db.queryengine.plan.statement.component.IntoComponent;
import org.apache.iotdb.db.queryengine.plan.statement.component.OrderByComponent;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.queryengine.plan.statement.component.ResultColumn;
import org.apache.iotdb.db.queryengine.plan.statement.component.ResultSetFormat;
import org.apache.iotdb.db.queryengine.plan.statement.component.SelectComponent;
import org.apache.iotdb.db.queryengine.plan.statement.component.SortItem;
import org.apache.iotdb.db.queryengine.plan.statement.component.WhereCondition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.calc.utils.constant.SqlConstant.COUNT_TIME;

/**
 * Base class of SELECT statement.
 *
 * <p>Here is the syntax definition of SELECT statement:
 *
 * <ul>
 *   SELECT
 *   <li>[LAST] resultColumn [, resultColumn] ...
 *   <li>FROM prefixPath [, prefixPath] ...
 *   <li>WHERE whereCondition
 *   <li>[GROUP BY ([startTime, endTime), interval, slidingStep)]
 *   <li>[GROUP BY LEVEL = levelNum [, levelNum] ...]
 *   <li>[FILL ({PREVIOUS | LINEAR | constant})]
 *   <li>[LIMIT rowLimit] [OFFSET rowOffset]
 *   <li>[SLIMIT seriesLimit] [SOFFSET seriesOffset]
 *   <li>[ORDER BY TIME {ASC | DESC}]
 *   <li>[{ALIGN BY DEVICE | DISABLE ALIGN}]
 * </ul>
 */
public class QueryStatement extends AuthorityInformationStatement {

  private SelectComponent selectComponent;
  private FromComponent fromComponent;
  private WhereCondition whereCondition;
  private HavingCondition havingCondition;

  // row limit for result set. The default value is 0, which means no limit
  private long rowLimit = 0;
  // row offset for result set. The default value is 0
  private long rowOffset = 0;

  // series limit and offset for result set. The default value is 0, which means no limit
  private long seriesLimit = 0;
  // series offset for result set. The default value is 0
  private long seriesOffset = 0;

  private FillComponent fillComponent;

  private OrderByComponent orderByComponent;

  private ResultSetFormat resultSetFormat = ResultSetFormat.ALIGN_BY_TIME;

  // `GROUP BY TIME` clause
  private GroupByTimeComponent groupByTimeComponent;

  // `GROUP BY LEVEL` clause
  private GroupByLevelComponent groupByLevelComponent;

  // `GROUP BY TAG` clause
  private GroupByTagComponent groupByTagComponent;

  // `GROUP BY VARIATION` clause
  private GroupByComponent groupByComponent;

  // `INTO` clause
  private IntoComponent intoComponent;

  private boolean isCqQueryBody;

  private boolean isOutputEndTime = false;

  private boolean useWildcard = true;

  private boolean isCountTimeAggregation = false;

  // used for limit and offset push down optimizer, if we select all columns from aligned device, we
  // can use statistics to skip
  private boolean lastLevelUseWildcard = false;

  // used in limit/offset push down optimizer, if the result set is empty after pushing down in
  // ASTVisitor,
  // we can skip the query
  private boolean isResultSetEmpty = false;

  // [IoTDB-AI] used for model inference, which will be removed in the future
  private String modelId;
  private boolean hasModelInference = false;
  private boolean generateTime = false;
  private InferenceWindow inferenceWindow = null;
  private Map<String, String> inferenceAttribute = null;

  public void setGenerateTime(boolean generateTime) {
    this.generateTime = generateTime;
  }

  public boolean isGenerateTime() {
    return generateTime;
  }

  public void setModelId(String modelId) {
    this.modelId = modelId;
  }

  public String getModelId() {
    return modelId;
  }

  public void setHasModelInference(boolean hasModelInference) {
    this.hasModelInference = hasModelInference;
  }

  public boolean hasModelInference() {
    return hasModelInference;
  }

  public void setInferenceWindow(InferenceWindow inferenceWindow) {
    this.inferenceWindow = inferenceWindow;
  }

  public boolean isSetInferenceWindow() {
    return this.inferenceWindow != null;
  }

  public InferenceWindow getInferenceWindow() {
    return inferenceWindow;
  }

  public void addInferenceAttribute(String key, String value) {
    if (inferenceAttribute == null) {
      inferenceAttribute = new HashMap<>();
    }
    inferenceAttribute.put(key, value);
  }

  public Map<String, String> getInferenceAttributes() {
    return inferenceAttribute;
  }

  public boolean hasInferenceAttributes() {
    return inferenceAttribute != null;
  }

  // [IoTDB-AI] END

  public QueryStatement() {
    this.statementType = StatementType.QUERY;
  }

  @Override
  public boolean isQuery() {
    return true;
  }

  @Override
  public List<PartialPath> getPaths() {
    Set<PartialPath> authPaths = new HashSet<>();
    List<PartialPath> prefixPaths = fromComponent.getPrefixPaths();
    List<ResultColumn> resultColumns = selectComponent.getResultColumns();
    for (ResultColumn resultColumn : resultColumns) {
      Expression expression = resultColumn.getExpression();
      authPaths.addAll(ExpressionAnalyzer.concatExpressionWithSuffixPaths(expression, prefixPaths));
    }
    return new ArrayList<>(authPaths);
  }

  public SelectComponent getSelectComponent() {
    return selectComponent;
  }

  public void setSelectComponent(SelectComponent selectComponent) {
    this.selectComponent = selectComponent;
  }

  public FromComponent getFromComponent() {
    return fromComponent;
  }

  public void setFromComponent(FromComponent fromComponent) {
    this.fromComponent = fromComponent;
  }

  public boolean hasWhere() {
    return whereCondition != null;
  }

  public WhereCondition getWhereCondition() {
    return whereCondition;
  }

  public void setWhereCondition(WhereCondition whereCondition) {
    this.whereCondition = whereCondition;
  }

  public boolean hasHaving() {
    return havingCondition != null;
  }

  public HavingCondition getHavingCondition() {
    return havingCondition;
  }

  public void setHavingCondition(HavingCondition havingCondition) {
    this.havingCondition = havingCondition;
  }

  public long getRowLimit() {
    return rowLimit;
  }

  public void setRowLimit(long rowLimit) {
    this.rowLimit = rowLimit;
  }

  public long getRowOffset() {
    return rowOffset;
  }

  public void setRowOffset(long rowOffset) {
    this.rowOffset = rowOffset;
  }

  public boolean isResultSetEmpty() {
    return isResultSetEmpty;
  }

  public void setResultSetEmpty(boolean resultSetEmpty) {
    isResultSetEmpty = resultSetEmpty;
  }

  public long getSeriesLimit() {
    return seriesLimit;
  }

  public void setSeriesLimit(long seriesLimit) {
    this.seriesLimit = seriesLimit;
  }

  public long getSeriesOffset() {
    return seriesOffset;
  }

  public void setSeriesOffset(long seriesOffset) {
    this.seriesOffset = seriesOffset;
  }

  public FillComponent getFillComponent() {
    return fillComponent;
  }

  public void setFillComponent(FillComponent fillComponent) {
    this.fillComponent = fillComponent;
  }

  public OrderByComponent getOrderByComponent() {
    return orderByComponent;
  }

  public void setOrderByComponent(OrderByComponent orderByComponent) {
    this.orderByComponent = orderByComponent;
  }

  public void setResultSetFormat(ResultSetFormat resultSetFormat) {
    this.resultSetFormat = resultSetFormat;
  }

  public GroupByTimeComponent getGroupByTimeComponent() {
    return groupByTimeComponent;
  }

  public void setGroupByTimeComponent(GroupByTimeComponent groupByTimeComponent) {
    this.groupByTimeComponent = groupByTimeComponent;
  }

  public GroupByLevelComponent getGroupByLevelComponent() {
    return groupByLevelComponent;
  }

  public void setGroupByLevelComponent(GroupByLevelComponent groupByLevelComponent) {
    this.groupByLevelComponent = groupByLevelComponent;
  }

  public GroupByTagComponent getGroupByTagComponent() {
    return groupByTagComponent;
  }

  public void setGroupByTagComponent(GroupByTagComponent groupByTagComponent) {
    this.groupByTagComponent = groupByTagComponent;
  }

  public GroupByComponent getGroupByComponent() {
    return groupByComponent;
  }

  public void setGroupByComponent(GroupByComponent groupByComponent) {
    this.groupByComponent = groupByComponent;
  }

  public void setOutputEndTime(boolean outputEndTime) {
    isOutputEndTime = outputEndTime;
  }

  public boolean isOutputEndTime() {
    return isOutputEndTime;
  }

  public boolean isLastQuery() {
    return selectComponent.hasLast();
  }

  public boolean isAggregationQuery() {
    return selectComponent.hasBuiltInAggregationFunction();
  }

  public boolean isGroupByLevel() {
    return groupByLevelComponent != null;
  }

  public boolean isGroupByTag() {
    return groupByTagComponent != null;
  }

  public boolean isGroupByTime() {
    return groupByTimeComponent != null;
  }

  public boolean isGroupBy() {
    return isGroupByTime() || groupByComponent != null;
  }

  private boolean isGroupByVariation() {
    return groupByComponent != null
        && groupByComponent.getWindowType() == WindowType.VARIATION_WINDOW;
  }

  private boolean isGroupByCondition() {
    return groupByComponent != null
        && groupByComponent.getWindowType() == WindowType.CONDITION_WINDOW;
  }

  private boolean isGroupByCount() {
    return groupByComponent != null && groupByComponent.getWindowType() == WindowType.COUNT_WINDOW;
  }

  private boolean hasAggregationFunction(Expression expression) {
    if (expression instanceof FunctionExpression) {
      return expression.isAggregationFunctionExpression();
    } else {
      if (expression instanceof TimeSeriesOperand) {
        return false;
      }
      for (Expression subExpression : expression.getExpressions()) {
        if (!subExpression.isAggregationFunctionExpression()) {
          return false;
        }
      }
    }
    return true;
  }

  public boolean hasGroupByExpression() {
    return isGroupByVariation() || isGroupByCondition() || isGroupByCount();
  }

  public boolean hasOrderByExpression() {
    return !getExpressionSortItemList().isEmpty();
  }

  public boolean isAlignByDevice() {
    return resultSetFormat == ResultSetFormat.ALIGN_BY_DEVICE;
  }

  public boolean isOrderByTime() {
    return orderByComponent != null && orderByComponent.isOrderByTime();
  }

  public boolean isOrderByTimeInDevices() {
    return orderByComponent == null
        || (orderByComponent.isBasedOnDevice()
            && (orderByComponent.getSortItemList().size() == 1
                || orderByComponent.getTimeOrderPriority() == 1));
  }

  public boolean isOrderByTimeseries() {
    return orderByComponent != null && orderByComponent.isOrderByTimeseries();
  }

  public boolean onlyOrderByTimeseries() {
    return isOrderByTimeseries() && orderByComponent.getSortItemList().size() == 1;
  }

  public boolean isOrderByDevice() {
    return orderByComponent != null && orderByComponent.isOrderByDevice();
  }

  public IntoComponent getIntoComponent() {
    return intoComponent;
  }

  public void setIntoComponent(IntoComponent intoComponent) {
    this.intoComponent = intoComponent;
  }

  public Ordering getResultTimeOrder() {
    if (orderByComponent == null || !orderByComponent.isOrderByTime()) {
      return Ordering.ASC;
    }
    return orderByComponent.getTimeOrder();
  }

  public Ordering getResultDeviceOrder() {
    if (orderByComponent == null || !orderByComponent.isOrderByDevice()) {
      return Ordering.ASC;
    }
    return orderByComponent.getDeviceOrder();
  }

  // align by device + order by device, expression
  public boolean needPushDownSort() {
    return !isAggregationQuery() && hasOrderByExpression() && isOrderByBasedOnDevice();
  }

  public boolean isOrderByBasedOnDevice() {
    return orderByComponent != null && orderByComponent.isBasedOnDevice();
  }

  public boolean isOrderByBasedOnTime() {
    return orderByComponent != null && orderByComponent.isBasedOnTime();
  }

  public List<SortItem> getSortItemList() {
    if (orderByComponent == null) {
      return Collections.emptyList();
    }
    return orderByComponent.getSortItemList();
  }

  public List<Expression> getExpressionSortItemList() {
    if (orderByComponent == null) {
      return Collections.emptyList();
    }
    return orderByComponent.getExpressionSortItemList();
  }

  //  update the sortItems with expressionSortItems
  public void updateSortItems(Set<Expression> orderByExpressions) {
    Expression[] sortItemExpressions = orderByExpressions.toArray(new Expression[0]);
    List<SortItem> sortItems = getSortItemList();
    int expressionIndex = 0;
    for (int i = 0; i < sortItems.size() && expressionIndex < sortItemExpressions.length; i++) {
      SortItem sortItem = sortItems.get(i);
      if (sortItem.isExpression()) {
        sortItem.setExpression(sortItemExpressions[expressionIndex]);
        expressionIndex++;
      }
    }
  }

  public List<SortItem> getUpdatedSortItems(Set<Expression> orderByExpressions) {
    Expression[] sortItemExpressions = orderByExpressions.toArray(new Expression[0]);
    List<SortItem> sortItems = getSortItemList();
    List<SortItem> newSortItems = new ArrayList<>();
    int expressionIndex = 0;
    for (SortItem sortItem : sortItems) {
      SortItem newSortItem =
          new SortItem(sortItem.getSortKey(), sortItem.getOrdering(), sortItem.getNullOrdering());
      if (sortItem.isExpression()) {
        newSortItem.setExpression(sortItemExpressions[expressionIndex]);
        expressionIndex++;
      }
      newSortItems.add(newSortItem);
    }
    return newSortItems;
  }

  public boolean hasFill() {
    return fillComponent != null;
  }

  public boolean hasOrderBy() {
    return orderByComponent != null;
  }

  public boolean isSelectInto() {
    return intoComponent != null;
  }

  public boolean isCqQueryBody() {
    return isCqQueryBody;
  }

  public void setCqQueryBody(boolean cqQueryBody) {
    isCqQueryBody = cqQueryBody;
  }

  public boolean hasLimit() {
    return rowLimit > 0;
  }

  public boolean hasOffset() {
    return rowOffset > 0;
  }

  public void setUseWildcard(boolean useWildcard) {
    this.useWildcard = useWildcard;
  }

  public boolean useWildcard() {
    return useWildcard;
  }

  public void setCountTimeAggregation(boolean countTimeAggregation) {
    this.isCountTimeAggregation = countTimeAggregation;
  }

  public boolean isCountTimeAggregation() {
    return this.isCountTimeAggregation;
  }

  public boolean isLastLevelUseWildcard() {
    return lastLevelUseWildcard;
  }

  public void setLastLevelUseWildcard(boolean lastLevelUseWildcard) {
    this.lastLevelUseWildcard = lastLevelUseWildcard;
  }

  public static final String RAW_AGGREGATION_HYBRID_QUERY_ERROR_MSG =
      "Raw data and aggregation hybrid query is not supported.";

  public static final String COUNT_TIME_NOT_SUPPORT_GROUP_BY_LEVEL =
      "Count_time aggregation function using with group by level is not supported.";

  public static final String COUNT_TIME_NOT_SUPPORT_GROUP_BY_TAG =
      "Count_time aggregation function using with group by tag is not supported.";

  public static final String COUNT_TIME_CAN_ONLY_EXIST_ALONE =
      "Count_time aggregation can only exist alone, "
          + "and cannot used with other queries or aggregations.";

  public static final String COUNT_TIME_NOT_SUPPORT_USE_WITH_HAVING =
      "Count_time aggregation function can not be used with having clause.";

  @SuppressWarnings({"squid:S3776", "squid:S6541"}) // Suppress high Cognitive Complexity warning
  public void semanticCheck() {

    if (hasModelInference) {
      if (isAlignByDevice()) {
        throw new SemanticException(
            DataNodeQueryMessages.MODEL_INFERENCE_DOES_NOT_SUPPORT_ALIGN_BY_DEVICE);
      }
      if (isSelectInto()) {
        throw new SemanticException(
            DataNodeQueryMessages.MODEL_INFERENCE_DOES_NOT_SUPPORT_SELECT_INTO_NOW);
      }
    }

    if (isAggregationQuery()) {
      if (groupByComponent != null && isGroupByLevel()) {
        throw new SemanticException(
            DataNodeQueryMessages.GROUP_BY_CLAUSES_DOESN_T_SUPPORT_GROUP_BY);
      }
      if (isGroupByLevel() && isAlignByDevice()) {
        throw new SemanticException(DataNodeQueryMessages.GROUP_BY_LEVEL_DOES_NOT_SUPPORT_ALIGN_BY);
      }
      if (isGroupByTag() && isAlignByDevice()) {
        throw new SemanticException(DataNodeQueryMessages.GROUP_BY_TAGS_DOES_NOT_SUPPORT_ALIGN_BY);
      }
      Set<String> outputColumn = new HashSet<>();
      for (ResultColumn resultColumn : selectComponent.getResultColumns()) {
        if (resultColumn.getColumnType() != ResultColumn.ColumnType.AGGREGATION) {
          throw new SemanticException(RAW_AGGREGATION_HYBRID_QUERY_ERROR_MSG);
        }

        String expressionString = resultColumn.getExpression().getExpressionString();
        if (expressionString.toLowerCase().contains(COUNT_TIME)) {
          checkCountTimeValidationInSelect(
              resultColumn.getExpression(), outputColumn, selectComponent.getResultColumns());
        }
        outputColumn.add(
            resultColumn.getAlias() != null ? resultColumn.getAlias() : expressionString);
      }

      for (Expression expression : getExpressionSortItemList()) {
        if (!hasAggregationFunction(expression)) {
          throw new SemanticException(RAW_AGGREGATION_HYBRID_QUERY_ERROR_MSG);
        }
      }

      if (isGroupByTag()) {
        if (hasHaving()) {
          throw new SemanticException(
              DataNodeQueryMessages.HAVING_CLAUSE_IS_NOT_SUPPORTED_YET_IN_GROUP);
        }
        for (String s : getGroupByTagComponent().getTagKeys()) {
          if (outputColumn.contains(s)) {
            throw new SemanticException(
                DataNodeQueryMessages.OUTPUT_COLUMN_IS_DUPLICATED_WITH_THE_TAG_KEY + s);
          }
        }
        if (rowLimit > 0 || rowOffset > 0 || seriesLimit > 0 || seriesOffset > 0) {
          throw new SemanticException(
              DataNodeQueryMessages.LIMIT_OR_SLIMIT_ARE_NOT_SUPPORTED_YET_IN);
        }
        for (ResultColumn resultColumn : selectComponent.getResultColumns()) {
          Expression expression = resultColumn.getExpression();
          if (!(expression instanceof FunctionExpression
              && expression.getExpressions().get(0) instanceof TimeSeriesOperand
              && expression.isAggregationFunctionExpression())) {
            throw new SemanticException(
                expression
                    + DataNodeQueryMessages
                        .CAN_T_BE_USED_IN_GROUP_BY_TAG_IT_WILL_BE_SUPPORTED_IN_THE_FUTURE);
          }
        }
      }
      if (hasGroupByExpression()) {
        // Aggregation expression shouldn't exist in group by clause.
        List<Expression> aggregationExpression =
            ExpressionAnalyzer.searchAggregationExpressions(
                groupByComponent.getControlColumnExpression());
        if (aggregationExpression != null && !aggregationExpression.isEmpty()) {
          throw new SemanticException(
              DataNodeQueryMessages.AGGREGATION_EXPRESSION_SHOULDN_T_EXIST_IN_GROUP_BY);
        }
      }
    } else {
      if (isGroupBy() || isGroupByLevel() || isGroupByTag()) {
        throw new SemanticException(
            DataNodeQueryMessages
                .COMMON_QUERIES_AND_AGGREGATED_QUERIES_ARE_NOT_ALLOWED_TO_APPEAR_AT_THE_SAME_TIME);
      }
      for (Expression expression : getExpressionSortItemList()) {
        if (hasAggregationFunction(expression)) {
          throw new SemanticException(RAW_AGGREGATION_HYBRID_QUERY_ERROR_MSG);
        }
      }
    }

    if (hasWhere()) {
      Expression whereExpression = getWhereCondition().getPredicate();
      if (ExpressionAnalyzer.identifyOutputColumnType(whereExpression, true)
          == ResultColumn.ColumnType.AGGREGATION) {
        throw new SemanticException(
            DataNodeQueryMessages.AGGREGATE_FUNCTIONS_ARE_NOT_SUPPORTED_IN_WHERE_CLAUSE);
      }
    }

    if (hasHaving()) {
      Expression havingExpression = getHavingCondition().getPredicate();
      if (ExpressionAnalyzer.identifyOutputColumnType(havingExpression, true)
          != ResultColumn.ColumnType.AGGREGATION) {
        throw new SemanticException(
            DataNodeQueryMessages.EXPRESSION_OF_HAVING_CLAUSE_MUST_TO_BE_AN);
      }
      if (!isAggregationQuery()) {
        throw new SemanticException(
            DataNodeQueryMessages
                .EXPRESSION_OF_HAVING_CLAUSE_CAN_NOT_BE_USED_IN_NONAGGREGATIONQUERY);
      }
      if (havingExpression.toString().toLowerCase().contains(COUNT_TIME)
          && (!new CountTimeAggregationAmountVisitor().process(havingExpression, null).isEmpty())) {
        throw new SemanticException(COUNT_TIME_NOT_SUPPORT_USE_WITH_HAVING);
      }
      try {
        if (isGroupByLevel()) {
          // check path in SELECT and HAVING only have one node
          for (ResultColumn resultColumn : getSelectComponent().getResultColumns()) {
            ExpressionAnalyzer.checkIsAllMeasurement(resultColumn.getExpression());
          }
          ExpressionAnalyzer.checkIsAllMeasurement(havingExpression);
        }
      } catch (SemanticException e) {
        throw new SemanticException(
            DataNodeQueryMessages.WHEN_HAVING_USED_WITH_GROUPBYLEVEL + e.getMessage());
      }
    }

    if (isAlignByDevice()) {
      // the paths can only be measurement or one-level wildcard in ALIGN BY DEVICE
      try {
        for (ResultColumn resultColumn : selectComponent.getResultColumns()) {
          ExpressionAnalyzer.checkIsAllMeasurement(resultColumn.getExpression());
        }
        if (hasGroupByExpression()) {
          ExpressionAnalyzer.checkIsAllMeasurement(
              getGroupByComponent().getControlColumnExpression());
        }
        if (hasOrderByExpression()) {
          for (Expression expression : getExpressionSortItemList()) {
            ExpressionAnalyzer.checkIsAllMeasurement(expression);
          }
        }
        if (getWhereCondition() != null) {
          ExpressionAnalyzer.checkIsAllMeasurement(getWhereCondition().getPredicate());
        }
        if (hasHaving()) {
          ExpressionAnalyzer.checkIsAllMeasurement(getHavingCondition().getPredicate());
        }
      } catch (SemanticException e) {
        throw new SemanticException(DataNodeQueryMessages.ALIGN_BY_DEVICE + e.getMessage());
      }

      if (isOrderByTimeseries()) {
        throw new SemanticException(
            DataNodeQueryMessages.SORTING_BY_TIMESERIES_IS_ONLY_SUPPORTED_IN_LAST);
      }
    }

    if (isLastQuery()) {
      if (isAlignByDevice()) {
        throw new SemanticException(
            DataNodeQueryMessages.LAST_QUERY_DOESN_T_SUPPORT_ALIGN_BY_DEVICE);
      }
      for (ResultColumn resultColumn : selectComponent.getResultColumns()) {
        Expression expression = resultColumn.getExpression();
        if (!(expression instanceof TimeSeriesOperand)) {
          throw new SemanticException(
              DataNodeQueryMessages.LAST_QUERIES_CAN_ONLY_BE_APPLIED_ON_RAW);
        }
      }
      if (isOrderByDevice()) {
        throw new SemanticException(
            DataNodeQueryMessages.SORTING_BY_DEVICE_IS_ONLY_SUPPORTED_IN_ALIGN_BY_DEVICE_QUERIES);
      }
      if (seriesLimit != 0 || seriesOffset != 0) {
        throw new SemanticException(DataNodeQueryMessages.SLIMIT_AND_SOFFSET_CAN_NOT_BE_USED_IN);
      }
    }

    if (!isAlignByDevice() && !isLastQuery()) {
      if (isOrderByTimeseries()) {
        throw new SemanticException(
            DataNodeQueryMessages.SORTING_BY_TIMESERIES_IS_ONLY_SUPPORTED_IN_LAST);
      }
      if (isOrderByDevice()) {
        throw new SemanticException(
            DataNodeQueryMessages.SORTING_BY_DEVICE_IS_ONLY_SUPPORTED_IN_ALIGN_BY_DEVICE_QUERIES);
      }
    }

    if (isSelectInto()) {
      if (getSeriesLimit() > 0) {
        throw new SemanticException(
            DataNodeQueryMessages.SELECT_INTO_SLIMIT_CLAUSES_ARE_NOT_SUPPORTED);
      }
      if (getSeriesOffset() > 0) {
        throw new SemanticException(
            DataNodeQueryMessages.SELECT_INTO_SOFFSET_CLAUSES_ARE_NOT_SUPPORTED);
      }
      if (isLastQuery()) {
        throw new SemanticException(
            DataNodeQueryMessages.SELECT_INTO_LAST_CLAUSES_ARE_NOT_SUPPORTED);
      }
      if (isGroupByTag()) {
        throw new SemanticException(DataNodeQueryMessages.SELECT_INTO_GROUP_BY_TAGS_CLAUSE_ARE_NOT);
      }
    }
  }

  public String constructFormattedSQL() {
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append(selectComponent.toSQLString()).append("\n");
    if (isSelectInto()) {
      sqlBuilder.append("\t").append(intoComponent.toSQLString()).append("\n");
    }
    sqlBuilder.append("\t").append(fromComponent.toSQLString()).append("\n");
    if (hasWhere()) {
      sqlBuilder.append("\t").append(whereCondition.toSQLString()).append("\n");
    }
    if (isGroupByTime()) {
      sqlBuilder.append("\t").append(groupByTimeComponent.toSQLString()).append("\n");
    }
    if (isGroupByLevel()) {
      sqlBuilder
          .append("\t")
          .append(groupByLevelComponent.toSQLString(isGroupByTime()))
          .append("\n");
    }
    if (hasHaving()) {
      sqlBuilder.append("\t").append(havingCondition.toSQLString()).append("\n");
    }
    if (hasFill()) {
      sqlBuilder.append("\t").append(fillComponent.toSQLString()).append("\n");
    }
    if (hasOrderBy()) {
      sqlBuilder.append("\t").append(orderByComponent.toSQLString()).append("\n");
    }
    if (rowLimit != 0) {
      sqlBuilder.append("\t").append("LIMIT").append(' ').append(rowLimit).append("\n");
    }
    if (rowOffset != 0) {
      sqlBuilder.append("\t").append("OFFSET").append(' ').append(rowOffset).append("\n");
    }
    if (seriesLimit != 0) {
      sqlBuilder.append("\t").append("SLIMIT").append(' ').append(seriesLimit).append("\n");
    }
    if (seriesOffset != 0) {
      sqlBuilder.append("\t").append("SOFFSET").append(' ').append(seriesOffset).append("\n");
    }
    if (isAlignByDevice()) {
      sqlBuilder.append("\t").append("ALIGN BY DEVICE").append("\n");
    }
    sqlBuilder.append(';');
    return sqlBuilder.toString();
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitQuery(this, context);
  }

  private void checkCountTimeValidationInSelect(
      Expression expression, Set<String> outputColumn, List<ResultColumn> resultColumns) {
    int countTimeAggSize = new CountTimeAggregationAmountVisitor().process(expression, null).size();

    if (countTimeAggSize > 1) {
      // e.g. select count_time(*) + count_time(*) from root.**
      throw new SemanticException(COUNT_TIME_CAN_ONLY_EXIST_ALONE);
    } else if (countTimeAggSize == 1) {
      // e.g. select count_time(*) / 2; select sum(*) / count_time(*)
      if (!(expression instanceof FunctionExpression)) {
        throw new SemanticException(COUNT_TIME_CAN_ONLY_EXIST_ALONE);
      }

      // e.g. select count(*), count(*) from root.db.**
      if (!outputColumn.isEmpty()) {
        throw new SemanticException(COUNT_TIME_CAN_ONLY_EXIST_ALONE);
      }

      // e.g. select count_time(*), count(*)
      if (resultColumns.size() > 1) {
        throw new SemanticException(COUNT_TIME_CAN_ONLY_EXIST_ALONE);
      }

      if (isGroupByTag()) {
        throw new SemanticException(COUNT_TIME_NOT_SUPPORT_GROUP_BY_TAG);
      }

      if (isGroupByLevel()) {
        throw new SemanticException(COUNT_TIME_NOT_SUPPORT_GROUP_BY_LEVEL);
      }

      setCountTimeAggregation(true);

      if (hasHaving()) {
        throw new SemanticException(COUNT_TIME_NOT_SUPPORT_USE_WITH_HAVING);
      }
    }
  }
}
