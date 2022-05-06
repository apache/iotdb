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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.statement.StatementNode;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.query.expression.multi.FunctionExpression;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** This class maintains information of {@code SELECT} clause. */
public class SelectComponent extends StatementNode {

  private final ZoneId zoneId;

  private boolean hasLast = false;

  private boolean hasBuiltInAggregationFunction = false;
  private boolean hasTimeSeriesGeneratingFunction = false;
  private boolean hasUserDefinedAggregationFunction = false;

  private List<ResultColumn> resultColumns = new ArrayList<>();

  private Map<String, Expression> aliasToColumnMap;

  private List<PartialPath> pathsCache;
  private Map<String, Set<PartialPath>> deviceNameToDeduplicatedPathsCache;

  public SelectComponent(ZoneId zoneId) {
    this.zoneId = zoneId;
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

  public Map<String, Expression> getAliasToColumnMap() {
    return aliasToColumnMap;
  }

  public void setAliasToColumnMap(Map<String, Expression> aliasToColumnMap) {
    this.aliasToColumnMap = aliasToColumnMap;
  }

  public boolean isHasLast() {
    return hasLast;
  }

  public void setHasLast(boolean hasLast) {
    this.hasLast = hasLast;
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

  public Map<String, Set<PartialPath>> getDeviceNameToDeduplicatedPathsMap() {
    if (deviceNameToDeduplicatedPathsCache == null) {
      deviceNameToDeduplicatedPathsCache = new HashMap<>();
      for (ResultColumn resultColumn : resultColumns) {
        for (PartialPath path : resultColumn.collectPaths()) {
          deviceNameToDeduplicatedPathsCache
              .computeIfAbsent(path.getDeviceIdString(), k -> new HashSet<>())
              .add(path);
        }
      }
    }
    return deviceNameToDeduplicatedPathsCache;
  }
}
