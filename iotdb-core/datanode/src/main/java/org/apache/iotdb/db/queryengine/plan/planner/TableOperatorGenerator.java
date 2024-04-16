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

package org.apache.iotdb.db.queryengine.plan.planner;

import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.db.queryengine.execution.driver.DataDriverContext;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.source.AlignedSeriesScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator;
import org.apache.iotdb.db.queryengine.plan.analyze.PredicateUtils;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.MergeSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.db.relational.sql.tree.Expression;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.analyze.PredicateUtils.convertPredicateToFilter;
import static org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager.getTSDataType;

/** This Visitor is responsible for transferring Table PlanNode Tree to Table Operator Tree. */
public class TableOperatorGenerator extends PlanVisitor<Operator, LocalExecutionPlanContext> {

  @Override
  public Operator visitPlan(PlanNode node, LocalExecutionPlanContext context) {
    throw new UnsupportedOperationException("should call the concrete visitXX() method");
  }

  @Override
  public Operator visitTableScan(TableScanNode node, LocalExecutionPlanContext context) {

    List<Symbol> outputColumnNames = node.getOutputSymbols();
    int outputColumnCount = outputColumnNames.size();
    List<ColumnSchema> columnSchemas = new ArrayList<>(outputColumnCount);
    int[] columnsIndexArray = new int[outputColumnCount];
    Map<Symbol, ColumnSchema> columnSchemaMap = node.getAssignments();
    Map<Symbol, Integer> idAndAttributeColumnsIndexMap = node.getAttributesMap();
    List<String> measurementColumnNames = new ArrayList<>();
    List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
    int measurementColumnCount = 0;
    for (int i = 0; i < outputColumnCount; i++) {
      Symbol columnName = outputColumnNames.get(i);
      ColumnSchema schema =
          requireNonNull(columnSchemaMap.get(columnName), columnName + " is null");
      columnSchemas.add(schema);
      switch (schema.getColumnCategory()) {
        case ID:
        case ATTRIBUTE:
          columnsIndexArray[i] =
              requireNonNull(
                  idAndAttributeColumnsIndexMap.get(columnName), columnName + " is null");
          break;
        case MEASUREMENT:
          columnsIndexArray[i] = measurementColumnCount;
          measurementColumnCount++;
          measurementColumnNames.add(columnName.getName());
          measurementSchemas.add(
              new MeasurementSchema(schema.getName(), getTSDataType(schema.getType())));
          break;
        default:
          throw new IllegalArgumentException(
              "Unexpected column category: " + schema.getColumnCategory());
      }
    }

    SeriesScanOptions.Builder scanOptionsBuilder = getSeriesScanOptionsBuilder(node, context);
    scanOptionsBuilder.withPushDownLimit(node.getPushDownLimit());
    scanOptionsBuilder.withPushDownOffset(node.getPushDownOffset());
    scanOptionsBuilder.withAllSensors(new HashSet<>(measurementColumnNames));

    Expression pushDownPredicate = node.getPushDownPredicate();
    boolean predicateCanPushIntoScan = canPushIntoScan(pushDownPredicate);
    if (pushDownPredicate != null && predicateCanPushIntoScan) {
      scanOptionsBuilder.withPushDownFilter(
          convertPredicateToFilter(
              pushDownPredicate,
              node.getAlignedPath().getMeasurementList(),
              context.getTypeProvider().getTemplatedInfo() != null,
              context.getTypeProvider()));
    }

    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                AlignedSeriesScanOperator.class.getSimpleName());

    int maxTsBlockLineNum = TSFileDescriptor.getInstance().getConfig().getMaxTsBlockLineNumber();
    if (context.getTypeProvider().getTemplatedInfo() != null) {
      maxTsBlockLineNum =
          (int)
              Math.min(
                  context.getTypeProvider().getTemplatedInfo().getLimitValue(), maxTsBlockLineNum);
    }

    TableScanOperator tableScanOperator =
        new TableScanOperator(
            operatorContext,
            node.getPlanNodeId(),
            seriesPath,
            node.getScanOrder(),
            scanOptionsBuilder.build(),
            node.isQueryAllSensors(),
            context.getTypeProvider().getTemplatedInfo() != null
                ? context.getTypeProvider().getTemplatedInfo().getDataTypes()
                : null,
            maxTsBlockLineNum);

    ((DataDriverContext) context.getDriverContext()).addSourceOperator(seriesScanOperator);
    ((DataDriverContext) context.getDriverContext()).addPath(seriesPath);
    context.getDriverContext().setInputDriver(true);

    if (!predicateCanPushIntoScan) {
      if (context.isBuildPlanUseTemplate()) {
        TemplatedInfo templatedInfo = context.getTemplatedInfo();
        return constructFilterOperator(
            pushDownPredicate,
            seriesScanOperator,
            templatedInfo.getProjectExpressions(),
            templatedInfo.getDataTypes(),
            templatedInfo.getLayoutMap(),
            templatedInfo.isKeepNull(),
            node.getPlanNodeId(),
            templatedInfo.getScanOrder(),
            context);
      }

      AlignedPath alignedPath = node.getAlignedPath();
      List<Expression> expressions = new ArrayList<>();
      List<TSDataType> dataTypes = new ArrayList<>();
      for (int i = 0; i < alignedPath.getMeasurementList().size(); i++) {
        expressions.add(ExpressionFactory.timeSeries(alignedPath.getSubMeasurementPath(i)));
        dataTypes.add(alignedPath.getSubMeasurementDataType(i));
      }

      return constructFilterOperator(
          pushDownPredicate,
          seriesScanOperator,
          expressions.toArray(new Expression[0]),
          dataTypes,
          makeLayout(Collections.singletonList(node)),
          false,
          node.getPlanNodeId(),
          node.getScanOrder(),
          context);
    }
    return seriesScanOperator;
  }

  private SeriesScanOptions.Builder getSeriesScanOptionsBuilder(
      TableScanNode node, LocalExecutionPlanContext context) {
    SeriesScanOptions.Builder scanOptionsBuilder = new SeriesScanOptions.Builder();

    Filter globalTimeFilter = context.getGlobalTimeFilter();
    if (globalTimeFilter != null) {
      // time filter may be stateful, so we need to copy it
      scanOptionsBuilder.withGlobalTimeFilter(globalTimeFilter.copy());
    }

    return scanOptionsBuilder;
  }

  private boolean canPushIntoScan(Expression pushDownPredicate) {
    return pushDownPredicate == null || PredicateUtils.predicateCanPushIntoScan(pushDownPredicate);
  }

  @Override
  public Operator visitFilter(FilterNode node, LocalExecutionPlanContext context) {
    return super.visitFilter(node, context);
  }

  @Override
  public Operator visitProject(ProjectNode node, LocalExecutionPlanContext context) {
    return super.visitProject(node, context);
  }

  @Override
  public Operator visitLimit(LimitNode node, LocalExecutionPlanContext context) {
    return super.visitLimit(node, context);
  }

  @Override
  public Operator visitOffset(OffsetNode node, LocalExecutionPlanContext context) {
    return super.visitOffset(node, context);
  }

  @Override
  public Operator visitMergeSort(MergeSortNode node, LocalExecutionPlanContext context) {
    return super.visitMergeSort(node, context);
  }

  @Override
  public Operator visitOutput(OutputNode node, LocalExecutionPlanContext context) {
    return super.visitOutput(node, context);
  }

  @Override
  public Operator visitSort(SortNode node, LocalExecutionPlanContext context) {
    return super.visitSort(node, context);
  }

  @Override
  public Operator visitTopK(TopKNode node, LocalExecutionPlanContext context) {
    return super.visitTopK(node, context);
  }
}
