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
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.db.metadata.plan.schemaregion.impl.read.SchemaRegionReadPlanFactory;
import org.apache.iotdb.db.metadata.query.info.ITimeSeriesSchemaInfo;
import org.apache.iotdb.db.metadata.query.reader.ISchemaReader;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Pair;

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
  private static final String viewTypeOfLogicalView = "logical";
  private static final String viewTypeOfNonView = "";

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
    Pair<String, String> deadbandInfo = MetaUtils.parseDeadbandInfo(series.getSchema().getProps());
    builder.getTimeColumnBuilder().writeLong(0);
    builder.writeNullableText(0, series.getFullPath());
    builder.writeNullableText(1, series.getAlias());
    builder.writeNullableText(2, database);
    builder.writeNullableText(3, series.getSchema().getType().toString());
    if (series.isLogicalView()) {
      builder.writeNullableText(4, null);
      builder.writeNullableText(5, null);
      builder.writeNullableText(10, viewTypeOfLogicalView);
    } else {
      builder.writeNullableText(4, series.getSchema().getEncodingType().toString());
      builder.writeNullableText(5, series.getSchema().getCompressor().toString());
      builder.writeNullableText(10, viewTypeOfNonView);
    }
    builder.writeNullableText(6, mapToString(series.getTags()));
    builder.writeNullableText(7, mapToString(series.getAttributes()));
    builder.writeNullableText(8, deadbandInfo.left);
    builder.writeNullableText(9, deadbandInfo.right);
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

  private List<String> analyzeDataTypeOfDelayedViews() {
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
    List<String> dataTypeStringList = new ArrayList<>();
    for (ViewExpression viewExpression : viewExpressionList) {
      Expression expression = null;
      boolean viewIsBroken = false;
      try {
        expression = transformToExpressionVisitor.process(viewExpression, null);
        expression = completeMeasurementSchemaVisitor.process(expression, schemaTree);
        ExpressionTypeAnalyzer.analyzeExpression(expressionTypes, expression);
      } catch (Exception e) {
        viewIsBroken = true;
      }
      if (viewIsBroken) {
        dataTypeStringList.add(unknownDataTypeString);
      } else {
        dataTypeStringList.add(expressionTypes.get(NodeRef.of(expression)).toString());
      }
    }
    return dataTypeStringList;
  }

  @Override
  public void processDelayedTask(TsBlockBuilder builder, String database) {
    if (this.delayedLogicalViewList == null || this.delayedLogicalViewList.size() <= 0) {
      return;
    }
    List<String> dataTypeStringList = this.analyzeDataTypeOfDelayedViews();
    // process delayed tasks
    for (int index = 0; index < this.delayedLogicalViewList.size(); index++) {
      ITimeSeriesSchemaInfo series = this.delayedLogicalViewList.get(index);
      String expressionTypeOfThisView = dataTypeStringList.get(index);

      Pair<String, String> deadbandInfo =
          MetaUtils.parseDeadbandInfo(series.getSchema().getProps());
      builder.getTimeColumnBuilder().writeLong(0);
      builder.writeNullableText(0, series.getFullPath());
      builder.writeNullableText(1, series.getAlias());
      builder.writeNullableText(2, database);
      builder.writeNullableText(3, expressionTypeOfThisView);
      builder.writeNullableText(4, null);
      builder.writeNullableText(5, null);
      builder.writeNullableText(6, mapToString(series.getTags()));
      builder.writeNullableText(7, mapToString(series.getAttributes()));
      builder.writeNullableText(8, deadbandInfo.left);
      builder.writeNullableText(9, deadbandInfo.right);
      builder.writeNullableText(10, viewTypeOfLogicalView);
      builder.declarePosition();
    }
    this.delayedLogicalViewList.clear();
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
