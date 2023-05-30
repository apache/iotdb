/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.mpp.execution.operator.schema.source;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.schema.view.LogicalViewSchema;
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.db.metadata.plan.schemaregion.impl.read.SchemaRegionReadPlanFactory;
import org.apache.iotdb.db.metadata.plan.schemaregion.result.ShowTimeSeriesResult;
import org.apache.iotdb.db.metadata.query.info.ITimeSeriesSchemaInfo;
import org.apache.iotdb.db.metadata.query.reader.ISchemaReader;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.db.metadata.view.viewExpression.visitor.GetSourcePathsVisitor;
import org.apache.iotdb.db.metadata.view.viewExpression.visitor.TransformToExpressionVisitor;
import org.apache.iotdb.db.mpp.common.NodeRef;
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.mpp.common.schematree.ISchemaTree;
import org.apache.iotdb.db.mpp.plan.analyze.ExpressionTypeAnalyzer;
import org.apache.iotdb.db.mpp.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.visitor.CompleteMeasurementSchemaVisitor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.metadata.MetadataConstant.ALL_MATCH_PATTERN;

public class TimeSeriesSchemaSource implements ISchemaSource<ITimeSeriesSchemaInfo> {

  private final PartialPath pathPattern;
  private final boolean isPrefixMatch;

  private final long limit;
  private final long offset;

  private final SchemaFilter schemaFilter;

  private final Map<Integer, Template> templateMap;

  /**
   * The task of processing logical views will be delayed. Those infos will be stored here in
   * function transformToTsBlockColumns(). <b>If there is no delayed infos of logical views, this
   * variable may be null.</b>
   */
  private List<ITimeSeriesSchemaInfo> delayedLogicalViewList;

  TimeSeriesSchemaSource(
      PartialPath pathPattern,
      boolean isPrefixMatch,
      long limit,
      long offset,
      SchemaFilter schemaFilter,
      Map<Integer, Template> templateMap) {
    this.pathPattern = pathPattern;
    this.isPrefixMatch = isPrefixMatch;

    this.limit = limit;
    this.offset = offset;

    this.schemaFilter = schemaFilter;

    this.templateMap = templateMap;
  }

  @Override
  public ISchemaReader<ITimeSeriesSchemaInfo> getSchemaReader(ISchemaRegion schemaRegion) {
    try {
      return schemaRegion.getTimeSeriesReader(
          SchemaRegionReadPlanFactory.getShowTimeSeriesPlan(
              pathPattern, templateMap, limit, offset, isPrefixMatch, schemaFilter));
    } catch (MetadataException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  @Override
  public List<ColumnHeader> getInfoQueryColumnHeaders() {
    return ColumnHeaderConstant.showTimeSeriesColumnHeaders;
  }

  @Override
  public void transformToTsBlockColumns(
      ITimeSeriesSchemaInfo series, TsBlockBuilder builder, String database) {
    if (series.isLogicalView()) {
      if (this.delayedLogicalViewList == null) {
        this.delayedLogicalViewList = new ArrayList<>();
      }
      this.delayedLogicalViewList.add(
          new ShowTimeSeriesResult(
              series.getFullPath(),
              series.getAlias(),
              series.getSchema(),
              series.getTags(),
              series.getAttributes(),
              series.isUnderAlignedDevice()));
      return;
    }
    Pair<String, String> deadbandInfo = MetaUtils.parseDeadbandInfo(series.getSchema().getProps());
    builder.getTimeColumnBuilder().writeLong(0);
    builder.writeNullableText(0, series.getFullPath());
    builder.writeNullableText(1, series.getAlias());
    builder.writeNullableText(2, database);
    builder.writeNullableText(3, series.getSchema().getType().toString());
    builder.writeNullableText(4, series.getSchema().getEncodingType().toString());
    builder.writeNullableText(5, series.getSchema().getCompressor().toString());
    builder.writeNullableText(6, mapToString(series.getTags()));
    builder.writeNullableText(7, mapToString(series.getAttributes()));
    builder.writeNullableText(8, deadbandInfo.left);
    builder.writeNullableText(9, deadbandInfo.right);
    builder.writeNullableText(10, "");
    builder.declarePosition();
  }

  @Override
  public boolean hasSchemaStatistic(ISchemaRegion schemaRegion) {
    return pathPattern.equals(ALL_MATCH_PATTERN) && (schemaFilter == null);
  }

  @Override
  public long getSchemaStatistic(ISchemaRegion schemaRegion) {
    return schemaRegion.getSchemaRegionStatistics().getSeriesNumber();
  }

  private List<TSDataType> analyzeDataTypeOfDelayedViews() {
    if (this.delayedLogicalViewList == null || this.delayedLogicalViewList.size() <= 0) {
      return new ArrayList<>();
    }
    // fetch schema of source paths of views
    List<ViewExpression> viewExpressionList = new ArrayList<>();
    for (ITimeSeriesSchemaInfo series : this.delayedLogicalViewList) {
      viewExpressionList.add(((LogicalViewSchema) series.getSchema()).getExpression());
    }
    GetSourcePathsVisitor getSourcePathsVisitor = new GetSourcePathsVisitor();
    List<PartialPath> sourcePathsNeedFetch;
    PathPatternTree patternTree = new PathPatternTree();
    for (ViewExpression viewExpression : viewExpressionList) {
      sourcePathsNeedFetch = getSourcePathsVisitor.process(viewExpression, null);
      for (PartialPath path : sourcePathsNeedFetch) {
        patternTree.appendFullPath(path);
      }
    }
    ISchemaTree schemaTree = ClusterSchemaFetcher.getInstance().fetchSchema(patternTree, null);
    // process each view expression and get data type
    TransformToExpressionVisitor transformToExpressionVisitor = new TransformToExpressionVisitor();
    CompleteMeasurementSchemaVisitor completeMeasurementSchemaVisitor =
        new CompleteMeasurementSchemaVisitor();
    Map<NodeRef<Expression>, TSDataType> expressionTypes = new HashMap<>();
    List<TSDataType> dataTypeList = new ArrayList<>();
    for (ViewExpression viewExpression : viewExpressionList) {
      Expression expression = transformToExpressionVisitor.process(viewExpression, null);
      expression = completeMeasurementSchemaVisitor.process(expression, schemaTree);
      ExpressionTypeAnalyzer.analyzeExpression(expressionTypes, expression);
      dataTypeList.add(expressionTypes.get(NodeRef.of(expression)));
    }
    return dataTypeList;
  }

  @Override
  public void processDelayedTask(TsBlockBuilder builder, String database) {
    if (this.delayedLogicalViewList == null || this.delayedLogicalViewList.size() <= 0) {
      return;
    }
    List<TSDataType> dataTypeList = this.analyzeDataTypeOfDelayedViews();
    // process delayed tasks
    for (int index = 0; index < this.delayedLogicalViewList.size(); index++) {
      ITimeSeriesSchemaInfo series = this.delayedLogicalViewList.get(index);
      TSDataType expressionTypeOfThisView = dataTypeList.get(index);

      Pair<String, String> deadbandInfo =
          MetaUtils.parseDeadbandInfo(series.getSchema().getProps());
      builder.getTimeColumnBuilder().writeLong(0);
      builder.writeNullableText(0, series.getFullPath());
      builder.writeNullableText(1, series.getAlias());
      builder.writeNullableText(2, database);
      builder.writeNullableText(3, expressionTypeOfThisView.toString());
      builder.writeNullableText(4, null);
      builder.writeNullableText(5, null);
      builder.writeNullableText(6, mapToString(series.getTags()));
      builder.writeNullableText(7, mapToString(series.getAttributes()));
      builder.writeNullableText(8, deadbandInfo.left);
      builder.writeNullableText(9, deadbandInfo.right);
      builder.writeNullableText(10, "logical");
      builder.declarePosition();
    }
  }

  private String mapToString(Map<String, String> map) {
    if (map == null || map.isEmpty()) {
      return null;
    }
    String content =
        map.entrySet().stream()
            .map(e -> "\"" + e.getKey() + "\"" + ":" + "\"" + e.getValue() + "\"")
            .collect(Collectors.joining(","));
    return "{" + content + "}";
  }
}
