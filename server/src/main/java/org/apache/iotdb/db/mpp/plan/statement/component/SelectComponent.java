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

package org.apache.iotdb.db.mpp.plan.statement.component;

import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.statement.StatementNode;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** This class maintains information of {@code SELECT} clause. */
public class SelectComponent extends StatementNode {

  private final ZoneId zoneId;

  private boolean hasLast = false;

  private boolean hasBuiltInAggregationFunction = false;

  protected List<ResultColumn> resultColumns = new ArrayList<>();

  private Map<String, Expression> aliasToColumnMap;

  public SelectComponent(ZoneId zoneId) {
    this.zoneId = zoneId;
  }

  public ZoneId getZoneId() {
    return zoneId;
  }

  public boolean isHasBuiltInAggregationFunction() {
    return hasBuiltInAggregationFunction;
  }

  public void addResultColumn(ResultColumn resultColumn) {
    resultColumns.add(resultColumn);
    ResultColumn.ColumnType columnType = resultColumn.getColumnType();
    if (columnType == ResultColumn.ColumnType.AGGREGATION) {
      hasBuiltInAggregationFunction = true;
    }
  }

  public void setResultColumns(List<ResultColumn> resultColumns) {
    this.resultColumns = resultColumns;
  }

  public List<ResultColumn> getResultColumns() {
    return resultColumns;
  }

  public Map<String, Expression> getAliasToColumnMap() {
    return aliasToColumnMap;
  }

  public void setAliasToColumnMap(Map<String, Expression> aliasToColumnMap) {
    this.aliasToColumnMap = aliasToColumnMap;
  }

  public boolean hasLast() {
    return hasLast;
  }

  public void setHasLast(boolean hasLast) {
    this.hasLast = hasLast;
  }

  public void setHasBuiltInAggregationFunction(boolean hasBuiltInAggregationFunction) {
    this.hasBuiltInAggregationFunction = hasBuiltInAggregationFunction;
  }

  public String toSQLString() {
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append("SELECT").append(' ');
    if (hasLast()) {
      sqlBuilder.append("LAST").append(' ');
    }
    for (int i = 0; i < resultColumns.size(); i++) {
      ResultColumn resultColumn = resultColumns.get(i);
      sqlBuilder.append(resultColumn.getExpression().toString());
      if (resultColumn.hasAlias()) {
        sqlBuilder.append('(').append(resultColumn.getAlias()).append(')');
      }
      if (i < resultColumns.size() - 1) {
        sqlBuilder.append(", ");
      }
    }
    return sqlBuilder.toString();
  }
}
