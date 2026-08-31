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

package org.apache.iotdb.db.queryengine.plan.statement.metadata;

import org.apache.iotdb.commons.cq.TimeoutPolicy;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.utils.TimestampPrecisionUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.plan.analyze.PredicateUtils;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.statement.IConfigStatement;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.component.GroupByTimeComponent;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;

import org.apache.tsfile.utils.TimeDuration;

import java.util.Collections;
import java.util.List;

public class CreateContinuousQueryStatement extends Statement implements IConfigStatement {

  private String cqId;

  // The query execution time interval, default value is group_by_interval in group by clause.
  private long everyInterval;

  // A date that represents the execution time of a certain cq task, default value is 0.
  private long boundaryTime = 0;

  // The start time of each query execution, default value is every_interval
  private long startTimeOffset;

  // The end time of each query execution, default value is 0.
  private long endTimeOffset = 0;

  // Structured representations retained for calendar-aware CQ scheduling. The legacy long
  // fields above are kept as wire/backward-compatible projections for fixed-only durations.
  private TimeDuration everyDuration = new TimeDuration(0, 0);
  private TimeDuration startTimeOffsetDuration = new TimeDuration(0, 0);
  private TimeDuration endTimeOffsetDuration = new TimeDuration(0, 0);
  private boolean boundaryExplicit;

  // Specify how we deal with the cq task whose previous time interval execution is not finished
  // while the next execution time has reached, default value is BLOCKED.
  private TimeoutPolicy timeoutPolicy = TimeoutPolicy.BLOCKED;

  private QueryStatement queryBodyStatement;
  private String queryBody;

  public CreateContinuousQueryStatement() {
    super();
    statementType = StatementType.CREATE_CONTINUOUS_QUERY;
  }

  public String getCqId() {
    return cqId;
  }

  public void setCqId(String cqId) {
    this.cqId = cqId;
  }

  public long getEveryInterval() {
    return everyInterval;
  }

  public void setEveryInterval(long everyInterval) {
    this.everyInterval = everyInterval;
    this.everyDuration = new TimeDuration(0, everyInterval);
  }

  public TimeDuration getEveryDuration() {
    return everyDuration;
  }

  public void setEveryDuration(TimeDuration everyDuration) {
    this.everyDuration = everyDuration;
    this.everyInterval =
        everyDuration.monthDuration == 0
            ? everyDuration.getTotalDuration(TimestampPrecisionUtils.currPrecision)
            : 0;
  }

  public long getBoundaryTime() {
    return boundaryTime;
  }

  public void setBoundaryTime(long boundaryTime) {
    this.boundaryTime = boundaryTime;
  }

  public boolean isBoundaryExplicit() {
    return boundaryExplicit;
  }

  public void setBoundaryExplicit(boolean boundaryExplicit) {
    this.boundaryExplicit = boundaryExplicit;
  }

  public long getStartTimeOffset() {
    return startTimeOffset;
  }

  public void setStartTimeOffset(long startTimeOffset) {
    this.startTimeOffset = startTimeOffset;
    this.startTimeOffsetDuration = new TimeDuration(0, startTimeOffset);
  }

  public TimeDuration getStartTimeOffsetDuration() {
    return startTimeOffsetDuration;
  }

  public void setStartTimeOffsetDuration(TimeDuration duration) {
    this.startTimeOffsetDuration = duration;
    this.startTimeOffset =
        duration.monthDuration == 0
            ? duration.getTotalDuration(TimestampPrecisionUtils.currPrecision)
            : 0;
  }

  public long getEndTimeOffset() {
    return endTimeOffset;
  }

  public void setEndTimeOffset(long endTimeOffset) {
    this.endTimeOffset = endTimeOffset;
    this.endTimeOffsetDuration = new TimeDuration(0, endTimeOffset);
  }

  public TimeDuration getEndTimeOffsetDuration() {
    return endTimeOffsetDuration;
  }

  public void setEndTimeOffsetDuration(TimeDuration duration) {
    this.endTimeOffsetDuration = duration;
    this.endTimeOffset =
        duration.monthDuration == 0
            ? duration.getTotalDuration(TimestampPrecisionUtils.currPrecision)
            : 0;
  }

  public TimeoutPolicy getTimeoutPolicy() {
    return timeoutPolicy;
  }

  public void setTimeoutPolicy(TimeoutPolicy timeoutPolicy) {
    this.timeoutPolicy = timeoutPolicy;
  }

  public QueryStatement getQueryBodyStatement() {
    return queryBodyStatement;
  }

  public void setQueryBodyStatement(QueryStatement queryBodyStatement) {
    this.queryBodyStatement = queryBodyStatement;
  }

  public String getSql() {
    return constructFormattedSQL();
  }

  public String getQueryBody() {
    if (queryBody == null) {
      queryBody = queryBodyStatement.constructFormattedSQL();
    }
    return queryBody;
  }

  public String constructFormattedSQL() {
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append("CREATE CQ ").append(cqId).append('\n');
    sqlBuilder.append("RESAMPLE\n");
    sqlBuilder.append('\t').append("EVERY ").append(everyInterval).append("ms\n");
    sqlBuilder.append('\t').append("BOUNDARY ").append(boundaryTime).append("\n");
    sqlBuilder.append('\t').append("RANGE ").append(startTimeOffset).append("ms");
    if (endTimeOffset != 0) {
      sqlBuilder.append(", ").append(endTimeOffset).append("ms\n");
    } else {
      sqlBuilder.append("\n");
    }
    sqlBuilder.append("TIMEOUT POLICY ").append(timeoutPolicy.toString()).append('\n');
    sqlBuilder.append("BEGIN\n");
    String[] queryBodySlices = getQueryBody().split("\n");
    for (int i = 0; i < queryBodySlices.length - 1; i++) { // skip ';' in queryBody
      sqlBuilder.append('\t').append(queryBodySlices[i]).append('\n');
    }
    sqlBuilder.append("END\n");
    sqlBuilder.append(";");
    return sqlBuilder.toString();
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.OTHER;
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.emptyList();
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitCreateContinuousQuery(this, context);
  }

  public void semanticCheck() {
    long minimumEvery =
        IoTDBDescriptor.getInstance().getConfig().getContinuousQueryMinimumEveryInterval();
    long minimumElapsed =
        everyDuration.monthDuration == 0
            ? everyDuration.nonMonthDuration
            : Math.subtractExact(
                Math.addExact(
                    Math.multiplyExact(
                        (long) everyDuration.monthDuration,
                        TimestampPrecisionUtils.currPrecision.convert(
                            28L * 86_400_000L, java.util.concurrent.TimeUnit.MILLISECONDS)),
                    everyDuration.nonMonthDuration),
                TimestampPrecisionUtils.currPrecision.convert(
                    36L * 3_600_000L, java.util.concurrent.TimeUnit.MILLISECONDS));
    if (minimumElapsed < minimumEvery) {
      throw new SemanticException(
          String.format(
              DataNodeQueryMessages
                  .CQ_EVERY_INTERVAL_D_SHOULD_NOT_BE_LOWER_THAN_THE_CONTINUOUS_QUERY_MINIMUM_EVERY_INTERVAL,
              everyInterval,
              minimumEvery));
    }
    if (!isPositive(everyDuration)) {
      throw new SemanticException("CQ: The every interval should be greater than 0.");
    }
    if (!isPositive(startTimeOffsetDuration)) {
      throw new SemanticException("CQ: The start time offset should be greater than 0.");
    }
    if (endTimeOffsetDuration.monthDuration < 0 || endTimeOffsetDuration.nonMonthDuration < 0) {
      throw new SemanticException("CQ: The end time offset should be greater than or equal to 0.");
    }
    if (!dominates(startTimeOffsetDuration, endTimeOffsetDuration, true)) {
      throw new SemanticException(
          DataNodeQueryMessages.CQ_THE_START_TIME_OFFSET_SHOULD_BE_GREATER_THAN_END_TIME_OFFSET);
    }
    if (!dominates(startTimeOffsetDuration, everyDuration, false)) {
      throw new SemanticException(
          DataNodeQueryMessages
              .CQ_THE_START_TIME_OFFSET_SHOULD_BE_GREATER_THAN_OR_EQUAL_TO_EVERY_INTERVAL);
    }

    if (!queryBodyStatement.isSelectInto()) {
      throw new SemanticException(DataNodeQueryMessages.CQ_THE_QUERY_BODY_MISSES_AN_INTO_CLAUSE);
    }
    GroupByTimeComponent groupByTimeComponent = queryBodyStatement.getGroupByTimeComponent();
    if (groupByTimeComponent != null
        && (groupByTimeComponent.getStartTime() != 0 || groupByTimeComponent.getEndTime() != 0)) {
      throw new SemanticException(
          DataNodeQueryMessages.CQ_SPECIFYING_TIME_RANGE_IN_GROUP_BY_TIME_CLAUSE_IS_PROHIBITED);
    }
    if (queryBodyStatement.getWhereCondition() != null
        && PredicateUtils.checkIfTimeFilterExist(
            queryBodyStatement.getWhereCondition().getPredicate())) {
      throw new SemanticException(
          DataNodeQueryMessages.CQ_SPECIFYING_TIME_FILTERS_IN_THE_QUERY_BODY);
    }
  }

  private static boolean dominates(TimeDuration left, TimeDuration right, boolean strict) {
    boolean result =
        left.monthDuration >= right.monthDuration
            && left.nonMonthDuration >= right.nonMonthDuration;
    if (!result) {
      return false;
    }
    return !strict
        || left.monthDuration != right.monthDuration
        || left.nonMonthDuration != right.nonMonthDuration;
  }

  private static boolean isPositive(TimeDuration duration) {
    return duration.monthDuration > 0 || duration.nonMonthDuration > 0;
  }
}
