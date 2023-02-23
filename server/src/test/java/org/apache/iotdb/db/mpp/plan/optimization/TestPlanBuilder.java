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

package org.apache.iotdb.db.mpp.plan.optimization;

import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.DeviceViewNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.FillNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.FilterNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.IntoNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.OffsetNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.TransformNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.AlignedSeriesScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.FillDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.IntoPathDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.OrderByParameter;
import org.apache.iotdb.db.mpp.plan.statement.component.FillPolicy;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.db.mpp.plan.statement.component.SortItem;
import org.apache.iotdb.db.mpp.plan.statement.component.SortKey;
import org.apache.iotdb.db.mpp.plan.statement.literal.LongLiteral;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant.DEVICE;

public class TestPlanBuilder {

  private PlanNode root;

  public TestPlanBuilder() {}

  public PlanNode getRoot() {
    return root;
  }

  public TestPlanBuilder scan(String id, PartialPath path) {
    this.root = new SeriesScanNode(new PlanNodeId(id), (MeasurementPath) path);
    return this;
  }

  public TestPlanBuilder scanAligned(String id, PartialPath path) {
    this.root = new AlignedSeriesScanNode(new PlanNodeId(id), (AlignedPath) path);
    return this;
  }

  public TestPlanBuilder scan(String id, PartialPath path, int limit, int offset) {
    SeriesScanNode node = new SeriesScanNode(new PlanNodeId(id), (MeasurementPath) path);
    node.setLimit(limit);
    node.setOffset(offset);
    this.root = node;
    return this;
  }

  public TestPlanBuilder scanAligned(String id, PartialPath path, int limit, int offset) {
    AlignedSeriesScanNode node = new AlignedSeriesScanNode(new PlanNodeId(id), (AlignedPath) path);
    node.setLimit(limit);
    node.setOffset(offset);
    this.root = node;
    return this;
  }

  public TestPlanBuilder timeJoin(List<PartialPath> paths) {
    int planId = 0;

    List<PlanNode> seriesSourceNodes = new ArrayList<>();
    for (PartialPath path : paths) {
      seriesSourceNodes.add(
          new SeriesScanNode(new PlanNodeId(String.valueOf(planId)), (MeasurementPath) path));
      planId++;
    }
    this.root =
        new TimeJoinNode(new PlanNodeId(String.valueOf(planId)), Ordering.ASC, seriesSourceNodes);
    return this;
  }

  public TestPlanBuilder transform(String id, List<Expression> expressions) {
    this.root =
        new TransformNode(
            new PlanNodeId(id),
            getRoot(),
            expressions.toArray(new Expression[0]),
            false,
            ZonedDateTime.now().getOffset(),
            Ordering.ASC);
    return this;
  }

  public TestPlanBuilder limit(String id, long limit) {
    this.root = new LimitNode(new PlanNodeId(id), getRoot(), limit);
    return this;
  }

  public TestPlanBuilder offset(String id, long offset) {
    this.root = new OffsetNode(new PlanNodeId(id), getRoot(), offset);
    return this;
  }

  public TestPlanBuilder fill(String id, FillPolicy fillPolicy) {
    this.root =
        new FillNode(new PlanNodeId(id), getRoot(), new FillDescriptor(fillPolicy), Ordering.ASC);
    return this;
  }

  public TestPlanBuilder fill(String id, String intValue) {
    this.root =
        new FillNode(
            new PlanNodeId(id),
            getRoot(),
            new FillDescriptor(FillPolicy.VALUE, new LongLiteral(intValue)),
            Ordering.ASC);
    return this;
  }

  public TestPlanBuilder singleDeviceView(String id, String device, String measurement) {
    Map<String, List<Integer>> deviceToMeasurementIndexesMap = new HashMap<>();
    deviceToMeasurementIndexesMap.put(device, Collections.singletonList(1));
    DeviceViewNode deviceViewNode =
        new DeviceViewNode(
            new PlanNodeId(id),
            new OrderByParameter(
                Arrays.asList(
                    new SortItem(SortKey.DEVICE, Ordering.ASC),
                    new SortItem(SortKey.TIME, Ordering.ASC))),
            Arrays.asList(DEVICE, measurement),
            deviceToMeasurementIndexesMap);
    deviceViewNode.addChildDeviceNode(device, getRoot());
    this.root = deviceViewNode;
    return this;
  }

  public TestPlanBuilder filter(String id, List<Expression> expressions, Expression predicate) {
    this.root =
        new FilterNode(
            new PlanNodeId(id),
            getRoot(),
            expressions.toArray(new Expression[0]),
            predicate,
            false,
            ZonedDateTime.now().getOffset(),
            Ordering.ASC);
    return this;
  }

  public TestPlanBuilder into(String id, PartialPath sourcePath, PartialPath intoPath) {
    IntoPathDescriptor intoPathDescriptor = new IntoPathDescriptor();
    intoPathDescriptor.specifyTargetPath(sourcePath.toString(), intoPath);
    intoPathDescriptor.specifyDeviceAlignment(intoPath.getDevice(), false);
    intoPathDescriptor.recordSourceColumnDataType(
        sourcePath.toString(), sourcePath.getSeriesType());
    this.root = new IntoNode(new PlanNodeId(id), getRoot(), intoPathDescriptor);
    return this;
  }
}
