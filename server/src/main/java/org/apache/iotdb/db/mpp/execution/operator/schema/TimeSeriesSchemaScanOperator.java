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
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.mpp.execution.driver.SchemaDriverContext;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.query.dataset.ShowTimeSeriesResult;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TimeSeriesSchemaScanOperator extends SchemaQueryScanOperator {
  private final String key;
  private final String value;
  private final boolean isContains;

  // if is true, the result will be sorted according to the inserting frequency of the timeseries
  private final boolean orderByHeat;

  private final Map<Integer, Template> templateMap;

  private final List<TSDataType> outputDataTypes;

  public TimeSeriesSchemaScanOperator(
      PlanNodeId planNodeId,
      OperatorContext operatorContext,
      int limit,
      int offset,
      PartialPath partialPath,
      String key,
      String value,
      boolean isContains,
      boolean orderByHeat,
      boolean isPrefixPath,
      Map<Integer, Template> templateMap) {
    super(planNodeId, operatorContext, limit, offset, partialPath, isPrefixPath);
    this.isContains = isContains;
    this.key = key;
    this.value = value;
    this.orderByHeat = orderByHeat;
    this.templateMap = templateMap;
    this.outputDataTypes =
        ColumnHeaderConstant.showTimeSeriesColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
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

  public boolean isOrderByHeat() {
    return orderByHeat;
  }

  @Override
  protected TsBlock createTsBlock() {
    TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);
    try {
      ((SchemaDriverContext) operatorContext.getInstanceContext().getDriverContext())
          .getSchemaRegion()
          .showTimeseries(convertToPhysicalPlan(), operatorContext.getInstanceContext())
          .left
          .forEach(series -> setColumns(series, builder));
    } catch (MetadataException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
    return builder.build();
  }

  // ToDo @xinzhongtianxia remove this temporary converter after mpp online
  private ShowTimeSeriesPlan convertToPhysicalPlan() {
    ShowTimeSeriesPlan plan =
        new ShowTimeSeriesPlan(partialPath, isContains, key, value, limit, offset, false);
    plan.setRelatedTemplate(templateMap);
    return plan;
  }

  private void setColumns(ShowTimeSeriesResult series, TsBlockBuilder builder) {
    builder.getTimeColumnBuilder().writeLong(series.getLastTime());
    builder.writeNullableText(0, series.getName());
    builder.writeNullableText(1, series.getAlias());
    builder.writeNullableText(2, series.getSgName());
    builder.writeNullableText(3, series.getDataType().toString());
    builder.writeNullableText(4, series.getEncoding().toString());
    builder.writeNullableText(5, series.getCompressor().toString());
    builder.writeNullableText(6, mapToString(series.getTag()));
    builder.writeNullableText(7, mapToString(series.getAttribute()));
    builder.declarePosition();
  }

  private String mapToString(Map<String, String> map) {
    String content =
        map.entrySet().stream()
            .map(e -> "\"" + e.getKey() + "\"" + ":" + "\"" + e.getValue() + "\"")
            .collect(Collectors.joining(","));
    if (content.isEmpty()) {
      return "null";
    } else {
      return "{" + content + "}";
    }
  }
}
