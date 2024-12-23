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
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.SimplePlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.FilterNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.GroupByLevelNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.GroupByTagNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleDeviceViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SlidingWindowAggregationNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.last.LastQueryCollectNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.last.LastQueryMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.last.LastQueryNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.last.LastQueryTransformNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedLastQueryScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedSeriesAggregationScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.LastQueryScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesAggregationScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.AggregationDescriptor;

import java.util.Objects;
import java.util.stream.Stream;

public class SubPlanTypeExtractor {

  private SubPlanTypeExtractor() {}

  public static TypeProvider extractor(PlanNode root, TypeProvider allTypes) {

    TypeProvider typeProvider =
        new TypeProvider(allTypes.getTreeModelTypeMap(), allTypes.getTemplatedInfo());
    root.accept(new Visitor(typeProvider, allTypes), null);
    return typeProvider;
  }

  private static class Visitor extends SimplePlanVisitor<Void> {

    private final TypeProvider typeProvider;
    private final TypeProvider allTypes;

    public Visitor(TypeProvider typeProvider, TypeProvider allTypes) {
      this.typeProvider = typeProvider;
      this.allTypes = allTypes;
    }

    @Override
    public Void visitPlan(PlanNode node, Void context) {
      node.getOutputColumnNames()
          .forEach(name -> typeProvider.setTreeModelType(name, allTypes.getTreeModelType(name)));
      for (PlanNode source : node.getChildren()) {
        source.accept(this, context);
      }
      return null;
    }

    @Override
    public Void visitSeriesAggregationScan(SeriesAggregationScanNode node, Void context) {
      String sourcePath = node.getSeriesPath().getFullPath();
      typeProvider.setTreeModelType(sourcePath, allTypes.getTreeModelType(sourcePath));
      return visitPlan(node, context);
    }

    @Override
    public Void visitAlignedSeriesAggregationScan(
        AlignedSeriesAggregationScanNode node, Void context) {
      // if TemplateInfo is not empty, all type infos used by AlignedSeriesAggregationScanNode have
      // been stored
      // in TemplateInfo
      if (typeProvider.getTemplatedInfo() != null) {
        return null;
      }

      AlignedPath alignedPath = node.getAlignedPath();
      for (int i = 0; i < alignedPath.getColumnNum(); i++) {
        String sourcePath = alignedPath.getPathWithMeasurement(i).getFullPath();
        typeProvider.setTreeModelType(sourcePath, allTypes.getTreeModelType(sourcePath));
      }
      return visitPlan(node, context);
    }

    @Override
    public Void visitAggregation(AggregationNode node, Void context) {
      updateTypeProviderByAggregationDescriptor(node.getAggregationDescriptorList().stream());
      return visitPlan(node, context);
    }

    @Override
    public Void visitSlidingWindowAggregation(SlidingWindowAggregationNode node, Void context) {
      updateTypeProviderByAggregationDescriptor(node.getAggregationDescriptorList().stream());
      return visitPlan(node, context);
    }

    @Override
    public Void visitGroupByLevel(GroupByLevelNode node, Void context) {
      updateTypeProviderByAggregationDescriptor(node.getGroupByLevelDescriptors().stream());
      return visitPlan(node, context);
    }

    @Override
    public Void visitGroupByTag(GroupByTagNode node, Void context) {
      node.getTagValuesToAggregationDescriptors()
          .values()
          .forEach(
              v -> updateTypeProviderByAggregationDescriptor(v.stream().filter(Objects::nonNull)));
      return visitPlan(node, context);
    }

    // region PlanNode of last read
    // No need to deal with type of last read
    @Override
    public Void visitLastQueryScan(LastQueryScanNode node, Void context) {
      return null;
    }

    @Override
    public Void visitAlignedLastQueryScan(AlignedLastQueryScanNode node, Void context) {
      return null;
    }

    @Override
    public Void visitLastQuery(LastQueryNode node, Void context) {
      if (node.isContainsLastTransformNode()) {
        return visitPlan(node, context);
      }
      return null;
    }

    @Override
    public Void visitLastQueryMerge(LastQueryMergeNode node, Void context) {
      if (node.isContainsLastTransformNode()) {
        return visitPlan(node, context);
      }
      return null;
    }

    @Override
    public Void visitLastQueryCollect(LastQueryCollectNode node, Void context) {
      if (node.isContainsLastTransformNode()) {
        return visitPlan(node, context);
      }
      return null;
    }

    @Override
    public Void visitLastQueryTransform(LastQueryTransformNode node, Void context) {
      return visitPlan(node, context);
    }

    @Override
    public Void visitSingleDeviceView(SingleDeviceViewNode node, Void context) {
      // if TemplateInfo is not empty, all type infos used by SingleDeviceViewNode have been stored
      // in TemplateInfo
      if (typeProvider.getTemplatedInfo() != null) {
        return null;
      }

      return visitPlan(node, context);
    }

    @Override
    public Void visitFilter(FilterNode node, Void context) {
      // if TemplateInfo is not empty, all type infos used by FilterNode have been stored in
      // TemplateInfo
      if (typeProvider.getTemplatedInfo() != null) {
        return null;
      }

      return visitPlan(node, context);
    }

    @Override
    public Void visitProject(ProjectNode node, Void context) {
      // if TemplateInfo is not empty, all type infos used by ProjectNode have been stored in
      // TemplateInfo
      if (typeProvider.getTemplatedInfo() != null) {
        return null;
      }
      return visitPlan(node, context);
    }

    // end region PlanNode of last read

    private void updateTypeProviderByAggregationDescriptor(
        Stream<? extends AggregationDescriptor> aggregationDescriptorList) {
      aggregationDescriptorList
          .flatMap(aggregationDescriptor -> aggregationDescriptor.getInputExpressions().stream())
          .forEach(
              expression -> {
                String expressionStr = expression.getExpressionString();
                typeProvider.setTreeModelType(
                    expressionStr, allTypes.getTreeModelType(expressionStr));
              });
    }
  }
}
