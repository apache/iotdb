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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.common.schematree.ISchemaTree;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.statement.component.ResultColumn;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;
import org.apache.iotdb.db.schemaengine.template.ClusterTemplateManager;
import org.apache.iotdb.db.schemaengine.template.Template;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.DEVICE_EXPRESSION;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.END_TIME_EXPRESSION;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.analyzeAlias;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.checkAliasUniqueness;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.updateDeviceToSelectExpressions;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.updateMeasurementToDeviceSelectExpressions;
import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionAnalyzer.concatDeviceAndBindSchemaForExpression;
import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionAnalyzer.toLowerCaseExpression;
import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionTypeAnalyzer.analyzeExpression;

/**
 * This class provides accelerated implementation for multiple devices align by device query. This
 * optimization is only used for devices with same template, using template can avoid many
 * unnecessary judgements.
 *
 * <p>e.g. for query `SELECT * FROM root.xx.** order by device/time/expression align by device`, the
 * device list of `root.xx.**` must use same template.
 */
public class TemplatedDeviceAnalyze {

  protected static List<Pair<Expression, String>> analyzeSelect(
      Analysis analysis,
      QueryStatement queryStatement,
      ISchemaTree schemaTree,
      List<PartialPath> deviceList) {

    Template template = ClusterTemplateManager.getInstance().getAllTemplates().get(0);

    List<Pair<Expression, String>> outputExpressions = new ArrayList<>();
    Map<String, Set<Expression>> deviceToSelectExpressions = new HashMap<>();

    ColumnPaginationController paginationController =
        new ColumnPaginationController(
            queryStatement.getSeriesLimit(), queryStatement.getSeriesOffset(), false);

    for (ResultColumn resultColumn : queryStatement.getSelectComponent().getResultColumns()) {
      Expression selectExpression = resultColumn.getExpression();

      // select expression after removing wildcard, LinkedHashMap for order-preserving
      Map<Expression, Map<String, Expression>> measurementToDeviceSelectExpressions =
          new LinkedHashMap<>();
      for (PartialPath device : deviceList) {
        List<Expression> selectExpressionsOfOneDevice =
            concatDeviceAndBindSchemaForExpression(selectExpression, device, schemaTree);
        if (selectExpressionsOfOneDevice.isEmpty()) {
          continue;
        }

        updateMeasurementToDeviceSelectExpressions(
            analysis, measurementToDeviceSelectExpressions, device, selectExpressionsOfOneDevice);
      }

      checkAliasUniqueness(resultColumn.getAlias(), measurementToDeviceSelectExpressions);

      for (Map.Entry<Expression, Map<String, Expression>> entry :
          measurementToDeviceSelectExpressions.entrySet()) {
        Expression measurementExpression = entry.getKey();
        Map<String, Expression> deviceToSelectExpressionsOfOneMeasurement = entry.getValue();

        if (paginationController.hasCurOffset()) {
          paginationController.consumeOffset();
        } else if (paginationController.hasCurLimit()) {
          deviceToSelectExpressionsOfOneMeasurement
              .values()
              .forEach(expression -> analyzeExpression(analysis, expression));

          // fix: devices used same template must have consistent type, no need to
          // checkDataTypeConsistency

          Expression lowerCaseMeasurementExpression = toLowerCaseExpression(measurementExpression);
          analyzeExpression(analysis, lowerCaseMeasurementExpression);

          outputExpressions.add(
              new Pair<>(
                  lowerCaseMeasurementExpression,
                  analyzeAlias(
                      resultColumn.getAlias(),
                      measurementExpression,
                      lowerCaseMeasurementExpression,
                      queryStatement)));

          updateDeviceToSelectExpressions(
              analysis, deviceToSelectExpressions, deviceToSelectExpressionsOfOneMeasurement);

          paginationController.consumeLimit();
        } else {
          break;
        }
      }
    }

    removeDevicesWithoutMeasurements(deviceList, deviceToSelectExpressions, analysis);

    Set<Expression> selectExpressions = new LinkedHashSet<>();
    selectExpressions.add(DEVICE_EXPRESSION);
    if (queryStatement.isOutputEndTime()) {
      selectExpressions.add(END_TIME_EXPRESSION);
    }
    outputExpressions.forEach(pair -> selectExpressions.add(pair.getLeft()));
    analysis.setSelectExpressions(selectExpressions);
    analysis.setDeviceToSelectExpressions(deviceToSelectExpressions);


    return outputExpressions;
  }

  private static void removeDevicesWithoutMeasurements(
      List<PartialPath> deviceList,
      Map<String, Set<Expression>> deviceToSelectExpressions,
      Analysis analysis) {
    // remove devices without measurements to compute
    Set<PartialPath> noMeasurementDevices = new HashSet<>();
    for (PartialPath device : deviceList) {
      if (!deviceToSelectExpressions.containsKey(device.getFullPath())) {
        noMeasurementDevices.add(device);
      }
    }
    deviceList.removeAll(noMeasurementDevices);

    // when the select expression of any device is empty,
    // the where expression map also need remove this device
    if (analysis.getDeviceToWhereExpression() != null) {
      noMeasurementDevices.forEach(
          devicePath -> analysis.getDeviceToWhereExpression().remove(devicePath.getFullPath()));
    }
  }
}
