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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedSeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This class provides accelerated implementation for multiple devices align by device query. This
 * optimization is only used for devices set in only one template, using template can avoid many
 * unnecessary judgements.
 */
public class TemplatedLogicalPlanBuilder extends LogicalPlanBuilder {
  private final MPPQueryContext context;

  private final Analysis analysis;

  private final List<String> measurementList;
  private final List<IMeasurementSchema> schemaList;

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
  }

  public TemplatedLogicalPlanBuilder planRawDataSource(
      PartialPath devicePath,
      Ordering scanOrder,
      long offset,
      long limit,
      boolean lastLevelUseWildcard) {
    List<PlanNode> sourceNodeList = new ArrayList<>();

    if (analysis.getDeviceTemplate().isDirectAligned()) {
      AlignedPath path = new AlignedPath(devicePath);
      path.setMeasurementList(measurementList);
      path.addSchemas(schemaList);

      // if value filter push down is implemented,
      // the serializeUseTemplate and deserializeUseTemplate method of AlignedSeriesScanNode also
      // need adapt
      AlignedSeriesScanNode alignedSeriesScanNode =
          new AlignedSeriesScanNode(
              context.getQueryId().genPlanNodeId(),
              path,
              scanOrder,
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
                limit,
                offset,
                null);
        sourceNodeList.add(seriesScanNode);
      }
    }

    this.root = convergeWithTimeJoin(sourceNodeList, scanOrder);
    return this;
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
  public TemplatedLogicalPlanBuilder withNewRoot(PlanNode newRoot) {
    this.root = newRoot;
    return this;
  }
}
