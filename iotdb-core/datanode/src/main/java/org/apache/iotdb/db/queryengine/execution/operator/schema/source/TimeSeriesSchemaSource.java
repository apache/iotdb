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

package org.apache.iotdb.db.queryengine.execution.operator.schema.source;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.exception.runtime.SchemaExecutionException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.schema.view.ViewType;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.schemaengine.schemaregion.read.req.SchemaRegionReadPlanFactory;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.ITimeSeriesSchemaInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.reader.ISchemaReader;
import org.apache.iotdb.db.schemaengine.schemaregion.utils.MetaUtils;
import org.apache.iotdb.db.schemaengine.template.Template;

import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Pair;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.schema.SchemaConstant.ALL_MATCH_PATTERN;

public class TimeSeriesSchemaSource implements ISchemaSource<ITimeSeriesSchemaInfo> {

  private final PartialPath pathPattern;
  private final PathPatternTree scope;
  private final boolean isPrefixMatch;
  private final long limit;
  private final long offset;
  private final SchemaFilter schemaFilter;
  private final Map<Integer, Template> templateMap;
  private final boolean needViewDetail;

  TimeSeriesSchemaSource(
      PartialPath pathPattern,
      boolean isPrefixMatch,
      long limit,
      long offset,
      SchemaFilter schemaFilter,
      Map<Integer, Template> templateMap,
      boolean needViewDetail,
      PathPatternTree scope) {
    this.pathPattern = pathPattern;
    this.isPrefixMatch = isPrefixMatch;
    this.limit = limit;
    this.offset = offset;
    this.schemaFilter = schemaFilter;
    this.templateMap = templateMap;
    this.needViewDetail = needViewDetail;
    this.scope = scope;
  }

  @Override
  public ISchemaReader<ITimeSeriesSchemaInfo> getSchemaReader(ISchemaRegion schemaRegion) {
    try {
      return schemaRegion.getTimeSeriesReader(
          SchemaRegionReadPlanFactory.getShowTimeSeriesPlan(
              pathPattern,
              templateMap,
              limit,
              offset,
              isPrefixMatch,
              schemaFilter,
              needViewDetail,
              scope));
    } catch (MetadataException e) {
      throw new SchemaExecutionException(e.getMessage(), e);
    }
  }

  @Override
  public List<ColumnHeader> getInfoQueryColumnHeaders() {
    return ColumnHeaderConstant.showTimeSeriesColumnHeaders;
  }

  @Override
  public void transformToTsBlockColumns(
      ITimeSeriesSchemaInfo series, TsBlockBuilder builder, String database) {
    builder.getTimeColumnBuilder().writeLong(0);
    builder.writeNullableText(0, series.getFullPath());
    builder.writeNullableText(1, series.getAlias());
    builder.writeNullableText(2, database);
    builder.writeNullableText(3, series.getSchema().getType().toString());
    if (series.isLogicalView()) {
      builder.writeNullableText(4, null);
      builder.writeNullableText(5, null);
      builder.writeNullableText(10, ViewType.VIEW.name());
    } else {
      builder.writeNullableText(4, series.getSchema().getEncodingType().toString());
      builder.writeNullableText(5, series.getSchema().getCompressor().toString());
      builder.writeNullableText(10, ViewType.BASE.name());
    }
    builder.writeNullableText(6, mapToString(series.getTags()));
    builder.writeNullableText(7, mapToString(series.getAttributes()));
    Pair<String, String> deadbandInfo = MetaUtils.parseDeadbandInfo(series.getSchema().getProps());
    builder.writeNullableText(8, deadbandInfo.left);
    builder.writeNullableText(9, deadbandInfo.right);
    builder.declarePosition();
  }

  @Override
  public boolean hasSchemaStatistic(ISchemaRegion schemaRegion) {
    return (pathPattern.equals(ALL_MATCH_PATTERN)
            || pathPattern.include(
                new PartialPath((schemaRegion.getDatabaseFullPath() + ".**").split("\\."))))
        && (schemaFilter == null)
        && scope.equals(SchemaConstant.ALL_MATCH_SCOPE);
  }

  @Override
  public long getSchemaStatistic(ISchemaRegion schemaRegion) {
    return schemaRegion.getSchemaRegionStatistics().getSeriesNumber(true);
  }

  public static String mapToString(Map<String, String> map) {
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
