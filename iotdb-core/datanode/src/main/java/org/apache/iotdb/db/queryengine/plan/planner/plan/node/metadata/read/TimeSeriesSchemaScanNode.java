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
package org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.schemaengine.template.Template;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import javax.validation.constraints.NotNull;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class TimeSeriesSchemaScanNode extends SchemaQueryScanNode {

  // if is true, the result will be sorted according to the inserting frequency of the timeseries
  private final boolean orderByHeat;

  private final SchemaFilter schemaFilter;

  private final Map<Integer, Template> templateMap;

  public TimeSeriesSchemaScanNode(
      PlanNodeId id,
      PartialPath partialPath,
      SchemaFilter schemaFilter,
      long limit,
      long offset,
      boolean orderByHeat,
      boolean isPrefixPath,
      @NotNull Map<Integer, Template> templateMap,
      @NotNull PathPatternTree scope) {
    super(id, partialPath, limit, offset, isPrefixPath, scope);
    this.schemaFilter = schemaFilter;
    this.orderByHeat = orderByHeat;
    this.templateMap = templateMap;
  }

  public SchemaFilter getSchemaFilter() {
    return schemaFilter;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.TIME_SERIES_SCHEMA_SCAN.serialize(byteBuffer);
    ReadWriteIOUtils.write(path.getFullPath(), byteBuffer);
    scope.serialize(byteBuffer);
    SchemaFilter.serialize(schemaFilter, byteBuffer);
    ReadWriteIOUtils.write(limit, byteBuffer);
    ReadWriteIOUtils.write(offset, byteBuffer);
    ReadWriteIOUtils.write(orderByHeat, byteBuffer);
    ReadWriteIOUtils.write(isPrefixPath, byteBuffer);

    ReadWriteIOUtils.write(templateMap.size(), byteBuffer);
    for (Template template : templateMap.values()) {
      template.serialize(byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.TIME_SERIES_SCHEMA_SCAN.serialize(stream);
    ReadWriteIOUtils.write(path.getFullPath(), stream);
    scope.serialize(stream);
    SchemaFilter.serialize(schemaFilter, stream);
    ReadWriteIOUtils.write(limit, stream);
    ReadWriteIOUtils.write(offset, stream);
    ReadWriteIOUtils.write(orderByHeat, stream);
    ReadWriteIOUtils.write(isPrefixPath, stream);

    ReadWriteIOUtils.write(templateMap.size(), stream);
    for (Template template : templateMap.values()) {
      template.serialize(stream);
    }
  }

  public static TimeSeriesSchemaScanNode deserialize(ByteBuffer byteBuffer) {
    String fullPath = ReadWriteIOUtils.readString(byteBuffer);
    PartialPath path;
    try {
      path = new MeasurementPath(fullPath);
    } catch (IllegalPathException e) {
      throw new IllegalArgumentException("Cannot deserialize TimeSeriesSchemaScanNode", e);
    }
    PathPatternTree scope = PathPatternTree.deserialize(byteBuffer);
    SchemaFilter schemaFilter = SchemaFilter.deserialize(byteBuffer);
    long limit = ReadWriteIOUtils.readLong(byteBuffer);
    long offset = ReadWriteIOUtils.readLong(byteBuffer);
    boolean oderByHeat = ReadWriteIOUtils.readBool(byteBuffer);
    boolean isPrefixPath = ReadWriteIOUtils.readBool(byteBuffer);

    int templateNum = ReadWriteIOUtils.readInt(byteBuffer);
    Map<Integer, Template> templateMap = new HashMap<>();
    Template template;
    for (int i = 0; i < templateNum; i++) {
      template = new Template();
      template.deserialize(byteBuffer);
      templateMap.put(template.getId(), template);
    }

    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);

    return new TimeSeriesSchemaScanNode(
        planNodeId,
        path,
        schemaFilter,
        limit,
        offset,
        oderByHeat,
        isPrefixPath,
        templateMap,
        scope);
  }

  public boolean isOrderByHeat() {
    return orderByHeat;
  }

  public Map<Integer, Template> getTemplateMap() {
    return templateMap;
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.TIME_SERIES_SCHEMA_SCAN;
  }

  @Override
  public PlanNode clone() {
    return new TimeSeriesSchemaScanNode(
        getPlanNodeId(),
        path,
        schemaFilter,
        limit,
        offset,
        orderByHeat,
        isPrefixPath,
        templateMap,
        scope);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return ColumnHeaderConstant.showTimeSeriesColumnHeaders.stream()
        .map(ColumnHeader::getColumnName)
        .collect(Collectors.toList());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    TimeSeriesSchemaScanNode that = (TimeSeriesSchemaScanNode) o;
    return orderByHeat == that.orderByHeat && Objects.equals(schemaFilter, that.schemaFilter);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), schemaFilter, orderByHeat);
  }

  @Override
  public String toString() {
    return String.format(
        "TimeSeriesSchemaScanNode-%s:[DataRegion: %s]",
        this.getPlanNodeId(), this.getRegionReplicaSet());
  }
}
