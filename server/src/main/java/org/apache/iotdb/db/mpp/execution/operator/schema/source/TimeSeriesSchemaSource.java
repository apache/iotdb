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

public class TimeSeriesSchemaSource implements ISchemaSource<ITimeSeriesSchemaInfo> {

  private final PartialPath pathPattern;
  private final boolean isPrefixMatch;

  private final long limit;
  private final long offset;

  private final String key;
  private final String value;
  private final boolean isContains;

  private final Map<Integer, Template> templateMap;

  TimeSeriesSchemaSource(
      PartialPath pathPattern,
      boolean isPrefixMatch,
      long limit,
      long offset,
      String key,
      String value,
      boolean isContains,
      Map<Integer, Template> templateMap) {
    this.pathPattern = pathPattern;
    this.isPrefixMatch = isPrefixMatch;

    this.limit = limit;
    this.offset = offset;

    this.key = key;
    this.value = value;
    this.isContains = isContains;

    this.templateMap = templateMap;
  }

  @Override
  public ISchemaReader<ITimeSeriesSchemaInfo> getSchemaReader(ISchemaRegion schemaRegion) {
    try {
      return schemaRegion.getTimeSeriesReader(
          SchemaRegionReadPlanFactory.getShowTimeSeriesPlan(
              pathPattern, templateMap, isContains, key, value, limit, offset, isPrefixMatch));
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
    builder.writeNullableText(4, series.getSchema().getEncodingType().toString());
    builder.writeNullableText(5, series.getSchema().getCompressor().toString());
    builder.writeNullableText(6, mapToString(series.getTags()));
    builder.writeNullableText(7, mapToString(series.getAttributes()));
    builder.writeNullableText(8, deadbandInfo.left);
    builder.writeNullableText(9, deadbandInfo.right);
    builder.declarePosition();
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
