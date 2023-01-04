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
package org.apache.iotdb.db.mpp.execution.operator.schema;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.plan.schemaregion.impl.read.SchemaRegionReadPlanFactory;
import org.apache.iotdb.db.metadata.query.info.ITimeSeriesSchemaInfo;
import org.apache.iotdb.db.metadata.query.reader.ISchemaReader;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.mpp.execution.driver.SchemaDriverContext;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.Map;
import java.util.stream.Collectors;

public class TimeSeriesSchemaScanOperator extends SchemaQueryScanOperator<ITimeSeriesSchemaInfo> {
  private final String key;
  private final String value;
  private final boolean isContains;

  private final Map<Integer, Template> templateMap;

  public TimeSeriesSchemaScanOperator(
      PlanNodeId planNodeId,
      OperatorContext operatorContext,
      int limit,
      int offset,
      PartialPath partialPath,
      String key,
      String value,
      boolean isContains,
      boolean isPrefixPath,
      Map<Integer, Template> templateMap) {
    super(
        planNodeId,
        operatorContext,
        limit,
        offset,
        partialPath,
        isPrefixPath,
        ColumnHeaderConstant.showTimeSeriesColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList()));
    this.isContains = isContains;
    this.key = key;
    this.value = value;
    this.templateMap = templateMap;
  }

  public String getKey() {
    return key;
  }

  public String getValue() {
    return value;
  }

  public boolean isContains() {
    return isContains;
  }

  @Override
  protected ISchemaReader<ITimeSeriesSchemaInfo> createSchemaReader() {
    try {
      return ((SchemaDriverContext) operatorContext.getInstanceContext().getDriverContext())
          .getSchemaRegion()
          .getTimeSeriesReader(
              SchemaRegionReadPlanFactory.getShowTimeSeriesPlan(
                  partialPath, templateMap, isContains, key, value, limit, offset, false));
    } catch (MetadataException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void setColumns(ITimeSeriesSchemaInfo series, TsBlockBuilder builder) {
    Pair<Map<String, String>, Map<String, String>> tagAndAttribute = series.getTagAndAttribute();
    Pair<String, String> deadbandInfo = MetaUtils.parseDeadbandInfo(series.getSchema().getProps());
    builder.getTimeColumnBuilder().writeLong(0);
    builder.writeNullableText(0, series.getFullPath());
    builder.writeNullableText(1, series.getAlias());
    builder.writeNullableText(2, getDatabase());
    builder.writeNullableText(3, series.getSchema().getType().toString());
    builder.writeNullableText(4, series.getSchema().getEncodingType().toString());
    builder.writeNullableText(5, series.getSchema().getCompressor().toString());
    builder.writeNullableText(6, mapToString(tagAndAttribute.left));
    builder.writeNullableText(7, mapToString(tagAndAttribute.right));
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
