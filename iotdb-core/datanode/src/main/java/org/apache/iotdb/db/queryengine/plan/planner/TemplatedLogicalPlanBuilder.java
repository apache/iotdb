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
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.FilterNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedSeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.tsfile.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import com.google.common.base.Function;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TemplatedLogicalPlanBuilder extends LogicalPlanBuilder {
  private PlanNode root;

  private final MPPQueryContext context;

  private final Analysis analysis;

  private final List<String> measurementList;
  private final List<IMeasurementSchema> schemaList;

  private final Function<Expression, TSDataType> getPreAnalyzedType;

  public TemplatedLogicalPlanBuilder(
      Analysis analysis,
      MPPQueryContext context,
      List<String> measurementList,
      List<IMeasurementSchema> schemaList) {
    super(analysis, context);
    this.analysis = analysis;
    this.context = context;
    this.measurementList = measurementList;
    this.schemaList = schemaList;
    this.getPreAnalyzedType = analysis::getType;
  }

  public TemplatedLogicalPlanBuilder planRawDataSource(
      PartialPath devicePath,
      Ordering scanOrder,
      Filter timeFilter,
      long offset,
      long limit,
      boolean lastLevelUseWildcard) {
    List<PlanNode> sourceNodeList = new ArrayList<>();

    if (analysis.getDeviceTemplate().isDirectAligned()) {
      AlignedPath path = new AlignedPath(devicePath);
      path.setMeasurementList(measurementList);
      path.addSchemas(schemaList);

      AlignedSeriesScanNode alignedSeriesScanNode =
          new AlignedSeriesScanNode(
              context.getQueryId().genPlanNodeId(),
              path,
              scanOrder,
              timeFilter,
              timeFilter,
              limit,
              offset,
              null,
              lastLevelUseWildcard);
      sourceNodeList.add(alignedSeriesScanNode);
    } else {
      for (int i = 0; i < measurementList.size(); i++) {
        MeasurementPath measurementPath =
            new MeasurementPath(devicePath.concatNode(measurementList.get(i)), schemaList.get(i));
        SeriesScanNode seriesScanNode =
            new SeriesScanNode(
                context.getQueryId().genPlanNodeId(),
                measurementPath,
                scanOrder,
                timeFilter,
                timeFilter,
                limit,
                offset,
                null);
        sourceNodeList.add(seriesScanNode);

        // why alignedPath not need type provider
        // context.getTypeProvider().setType(measurementPath.toString(),
        // schemaList.get(i).getType());
      }
    }

    // updateTypeProvider(sourceExpressions);

    this.root = convergeWithTimeJoin(sourceNodeList, scanOrder);
    return this;
  }

  private PlanNode convergeWithTimeJoin(List<PlanNode> sourceNodes, Ordering mergeOrder) {
    PlanNode tmpNode;
    if (sourceNodes.size() == 1) {
      tmpNode = sourceNodes.get(0);
    } else {
      tmpNode = new TimeJoinNode(context.getQueryId().genPlanNodeId(), mergeOrder, sourceNodes);
    }
    return tmpNode;
  }

  public TemplatedLogicalPlanBuilder planFilter(
      Expression filterExpression,
      Expression[] outputExpressions,
      boolean isGroupByTime,
      ZoneId zoneId,
      Ordering scanOrder) {

    if (filterExpression == null) {
      return this;
    }

    this.root =
        new FilterNode(
            context.getQueryId().genPlanNodeId(),
            this.getRoot(),
            outputExpressions,
            filterExpression,
            isGroupByTime,
            zoneId,
            scanOrder);

    updateTypeProvider(Collections.singletonList(filterExpression));

    return this;
  }

  @Override
  public PlanNode getRoot() {
    return root;
  }

  @Override
  public TemplatedLogicalPlanBuilder withNewRoot(PlanNode newRoot) {
    this.root = newRoot;
    return this;
  }
}
