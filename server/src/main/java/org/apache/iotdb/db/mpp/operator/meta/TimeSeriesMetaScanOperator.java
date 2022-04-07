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
package org.apache.iotdb.db.mpp.operator.meta;

import java.io.IOException;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.operator.OperatorContext;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.query.dataset.ShowTimeSeriesResult;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TimeSeriesMetaScanOperator extends MetaScanOperator {
  private String key;
  private String value;
  private boolean isContains;

  // if is true, the result will be sorted according to the inserting frequency of the timeseries
  private boolean orderByHeat;

  private static final TSDataType[] resourceTypes = {
    TSDataType.TEXT,
    TSDataType.TEXT,
    TSDataType.TEXT,
    TSDataType.TEXT,
    TSDataType.TEXT,
    TSDataType.TEXT,
    TSDataType.TEXT,
    TSDataType.TEXT
  };

  public TimeSeriesMetaScanOperator(
      OperatorContext operatorContext,
      ConsensusGroupId schemaRegionId,
      int limit,
      int offset,
      PartialPath partialPath,
      String key,
      String value,
      boolean isContains,
      boolean orderByHeat,
      boolean isPrefixPath,
      List<String> columns) {
    super(operatorContext, schemaRegionId, limit, offset, partialPath, isPrefixPath, columns);
    this.isContains = isContains;
    this.key = key;
    this.value = value;
    this.orderByHeat = orderByHeat;
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
  protected TsBlock createTsBlock() throws MetadataException {
    TsBlockBuilder builder = new TsBlockBuilder(Arrays.asList(resourceTypes));
    schemaRegion
        .showTimeseries(convertToPhysicalPlan(), operatorContext.getInstanceContext())
        .left
        .forEach(series -> setColumns(series, builder));
    return builder.build();
  }

  private ShowTimeSeriesPlan convertToPhysicalPlan() {
    return new ShowTimeSeriesPlan(partialPath, isContains, key, value, limit, offset, orderByHeat);
  }

  private void setColumns(ShowTimeSeriesResult series, TsBlockBuilder builder) {
    builder.getTimeColumnBuilder().writeLong(series.getLastTime());
    writeValueColumn(builder, 0, series.getName());
    writeValueColumn(builder, 1, series.getAlias());
    writeValueColumn(builder, 2, series.getSgName());
    writeValueColumn(builder, 3, series.getDataType().toString());
    writeValueColumn(builder, 4, series.getEncoding().toString());
    writeValueColumn(builder, 5, series.getCompressor().toString());
    writeValueColumn(builder, 6, mapToString(series.getTag()));
    writeValueColumn(builder, 7, mapToString(series.getAttribute()));
    builder.declarePosition();
  }

  private void writeValueColumn(TsBlockBuilder builder, int columnIndex, String value) {
    if (value == null) {
      builder.getColumnBuilder(columnIndex).appendNull();
    } else {
      builder.getColumnBuilder(columnIndex).writeBinary(new Binary(value));
    }
  }

  private String mapToString(Map<String, String> map) {
    return map.entrySet().stream()
        .map(e -> "\"" + e.getKey() + "\"" + ":" + "\"" + e.getValue() + "\"")
        .collect(Collectors.joining(","));
  }

  @Override
  public PlanNodeId getSourceId() {
    return null;
  }

  @Override
  public void initQueryDataSource(QueryDataSource dataSource) {

  }

  @Override
  public boolean isFinished() throws IOException {
    return false;
  }
}
