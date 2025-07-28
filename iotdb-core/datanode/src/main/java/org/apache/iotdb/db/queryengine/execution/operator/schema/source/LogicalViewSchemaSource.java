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
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.schema.filter.SchemaFilterFactory;
import org.apache.iotdb.commons.schema.view.LogicalViewSchema;
import org.apache.iotdb.commons.schema.view.ViewType;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.schemaengine.schemaregion.read.req.SchemaRegionReadPlanFactory;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.ITimeSeriesSchemaInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.reader.ISchemaReader;

import org.apache.tsfile.read.common.block.TsBlockBuilder;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LogicalViewSchemaSource implements ISchemaSource<ITimeSeriesSchemaInfo> {

  private final PartialPath pathPattern;
  private final PathPatternTree scope;

  private final long limit;
  private final long offset;

  private final SchemaFilter schemaFilter;

  LogicalViewSchemaSource(
      PartialPath pathPattern,
      long limit,
      long offset,
      SchemaFilter schemaFilter,
      PathPatternTree scope) {
    this.pathPattern = pathPattern;
    this.scope = scope;

    this.limit = limit;
    this.offset = offset;

    this.schemaFilter = schemaFilter;
  }

  @Override
  public ISchemaReader<ITimeSeriesSchemaInfo> getSchemaReader(ISchemaRegion schemaRegion) {
    try {
      return schemaRegion.getTimeSeriesReader(
          SchemaRegionReadPlanFactory.getShowTimeSeriesPlan(
              pathPattern,
              Collections.emptyMap(),
              limit,
              offset,
              false,
              SchemaFilterFactory.and(
                  schemaFilter, SchemaFilterFactory.createViewTypeFilter(ViewType.VIEW)),
              true,
              scope));
    } catch (MetadataException e) {
      throw new SchemaExecutionException(e.getMessage(), e);
    }
  }

  @Override
  public List<ColumnHeader> getInfoQueryColumnHeaders() {
    return ColumnHeaderConstant.showLogicalViewColumnHeaders;
  }

  @Override
  public void transformToTsBlockColumns(
      ITimeSeriesSchemaInfo series, TsBlockBuilder builder, String database) {
    builder.getTimeColumnBuilder().writeLong(0);
    builder.writeNullableText(0, series.getFullPath());
    builder.writeNullableText(1, database);

    builder.writeNullableText(2, series.getSchema().getType().toString());

    builder.writeNullableText(3, mapToString(series.getTags()));
    builder.writeNullableText(4, mapToString(series.getAttributes()));

    builder.writeNullableText(5, ViewType.VIEW.name());
    builder.writeNullableText(
        6, ((LogicalViewSchema) series.getSchema()).getExpression().toString());
    builder.declarePosition();
  }

  @Override
  public boolean hasSchemaStatistic(ISchemaRegion schemaRegion) {
    return false;
  }

  @Override
  public long getSchemaStatistic(ISchemaRegion schemaRegion) {
    return 0;
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
