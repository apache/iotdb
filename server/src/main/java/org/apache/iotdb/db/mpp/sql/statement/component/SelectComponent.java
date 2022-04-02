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

package org.apache.iotdb.db.mpp.sql.statement.component;

import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.sql.statement.StatementNode;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.unary.FunctionExpression;
import org.apache.iotdb.db.query.expression.unary.TimeSeriesOperand;

import java.time.ZoneId;
import java.util.*;

/** This class maintains information of {@code SELECT} clause. */
public class SelectComponent extends StatementNode {

  private final ZoneId zoneId;

  private boolean hasBuiltInAggregationFunction = false;
  private boolean hasTimeSeriesGeneratingFunction = false;
  private boolean hasUserDefinedAggregationFunction = false;

  protected List<ResultColumn> resultColumns = new ArrayList<>();

  Set<String> aliasSet;

  private List<PartialPath> pathsCache;
  private List<String> aggregationFunctionsCache;
  private Map<String, Set<PartialPath>> deviceIdToPathsCache;

  public SelectComponent(ZoneId zoneId) {
    this.zoneId = zoneId;
  }

  public SelectComponent(SelectComponent another) {
    zoneId = another.getZoneId();

    hasBuiltInAggregationFunction = another.isHasBuiltInAggregationFunction();
    hasTimeSeriesGeneratingFunction = another.isHasTimeSeriesGeneratingFunction();
    hasUserDefinedAggregationFunction = another.isHasUserDefinedAggregationFunction();

    resultColumns.addAll(another.getResultColumns());
    aliasSet.addAll(another.getAliasSet());
  }

  public ZoneId getZoneId() {
    return zoneId;
  }

  public boolean isHasBuiltInAggregationFunction() {
    return hasBuiltInAggregationFunction;
  }

  public boolean isHasTimeSeriesGeneratingFunction() {
    return hasTimeSeriesGeneratingFunction;
  }

  public boolean isHasUserDefinedAggregationFunction() {
    return hasUserDefinedAggregationFunction;
  }

  public void addResultColumn(ResultColumn resultColumn) {
    resultColumns.add(resultColumn);
    if (resultColumn.getExpression().isUserDefinedAggregationFunctionExpression()) {
      hasUserDefinedAggregationFunction = true;
    }
    if (resultColumn.getExpression().isBuiltInAggregationFunctionExpression()) {
      hasBuiltInAggregationFunction = true;
    }
    if (resultColumn.getExpression().isTimeSeriesGeneratingFunctionExpression()) {
      hasTimeSeriesGeneratingFunction = true;
    }
  }

  public void setResultColumns(List<ResultColumn> resultColumns) {
    this.resultColumns = resultColumns;
  }

  public List<ResultColumn> getResultColumns() {
    return resultColumns;
  }

  public void setAliasSet(Set<String> aliasSet) {
    this.aliasSet = aliasSet;
  }

  public Set<String> getAliasSet() {
    return aliasSet;
  }

  public List<PartialPath> getPaths() {
    if (pathsCache == null) {
      pathsCache = new ArrayList<>();
      for (ResultColumn resultColumn : resultColumns) {
        Expression expression = resultColumn.getExpression();
        if (expression instanceof TimeSeriesOperand) {
          pathsCache.add(((TimeSeriesOperand) expression).getPath());
        } else if (expression instanceof FunctionExpression
            && expression.isBuiltInAggregationFunctionExpression()) {
          pathsCache.add(((TimeSeriesOperand) expression.getExpressions().get(0)).getPath());
        } else {
          pathsCache.add(null);
        }
      }
    }
    return pathsCache;
  }

  public List<String> getAggregationFunctions() {
    if (aggregationFunctionsCache == null) {
      aggregationFunctionsCache = new ArrayList<>();
      for (ResultColumn resultColumn : resultColumns) {
        Expression expression = resultColumn.getExpression();
        aggregationFunctionsCache.add(
            expression instanceof FunctionExpression
                ? ((FunctionExpression) resultColumn.getExpression()).getFunctionName()
                : null);
      }
    }
    return aggregationFunctionsCache;
  }

  public Map<String, Set<PartialPath>> getDeviceIdToPathsMap() {
    if (deviceIdToPathsCache == null) {
      deviceIdToPathsCache = new HashMap<>();
      for (ResultColumn resultColumn : resultColumns) {
        for (PartialPath path : resultColumn.collectPaths()) {
          deviceIdToPathsCache.computeIfAbsent(path.getDevice(), k -> new HashSet<>()).add(path);
        }
      }
    }
    return deviceIdToPathsCache;
  }
}
