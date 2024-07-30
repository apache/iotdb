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

public class LevelTimeSeriesCountNode extends SchemaQueryScanNode {
  private final int level;
  private final SchemaFilter schemaFilter;
  private final Map<Integer, Template> templateMap;

  public LevelTimeSeriesCountNode(
      PlanNodeId id,
      PartialPath partialPath,
      boolean isPrefixPath,
      int level,
      SchemaFilter schemaFilter,
      @NotNull Map<Integer, Template> templateMap,
      @NotNull PathPatternTree scope) {
    super(id, partialPath, isPrefixPath, scope);
    this.level = level;
    this.schemaFilter = schemaFilter;
    this.templateMap = templateMap;
  }

  public SchemaFilter getSchemaFilter() {
    return schemaFilter;
  }

  public int getLevel() {
    return level;
  }

  public Map<Integer, Template> getTemplateMap() {
    return templateMap;
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.LEVEL_TIME_SERIES_COUNT;
  }

  @Override
  public PlanNode clone() {
    return new LevelTimeSeriesCountNode(
        getPlanNodeId(), path, isPrefixPath, level, schemaFilter, templateMap, scope);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return ColumnHeaderConstant.countLevelTimeSeriesColumnHeaders.stream()
        .map(ColumnHeader::getColumnName)
        .collect(Collectors.toList());
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.LEVEL_TIME_SERIES_COUNT.serialize(byteBuffer);
    ReadWriteIOUtils.write(path.getFullPath(), byteBuffer);
    scope.serialize(byteBuffer);
    ReadWriteIOUtils.write(isPrefixPath, byteBuffer);
    ReadWriteIOUtils.write(level, byteBuffer);
    SchemaFilter.serialize(schemaFilter, byteBuffer);
    ReadWriteIOUtils.write(templateMap.size(), byteBuffer);
    for (Template template : templateMap.values()) {
      template.serialize(byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.LEVEL_TIME_SERIES_COUNT.serialize(stream);
    ReadWriteIOUtils.write(path.getFullPath(), stream);
    scope.serialize(stream);
    ReadWriteIOUtils.write(isPrefixPath, stream);
    ReadWriteIOUtils.write(level, stream);
    SchemaFilter.serialize(schemaFilter, stream);
    ReadWriteIOUtils.write(templateMap.size(), stream);
    for (Template template : templateMap.values()) {
      template.serialize(stream);
    }
  }

  public static PlanNode deserialize(ByteBuffer buffer) {
    String fullPath = ReadWriteIOUtils.readString(buffer);
    PartialPath path;
    try {
      path = new PartialPath(fullPath);
    } catch (IllegalPathException e) {
      throw new IllegalArgumentException("Cannot deserialize DevicesSchemaScanNode", e);
    }
    PathPatternTree scope = PathPatternTree.deserialize(buffer);
    boolean isPrefixPath = ReadWriteIOUtils.readBool(buffer);
    int level = ReadWriteIOUtils.readInt(buffer);
    SchemaFilter schemaFilter = SchemaFilter.deserialize(buffer);
    int templateNum = ReadWriteIOUtils.readInt(buffer);
    Map<Integer, Template> templateMap = new HashMap<>();
    Template template;
    for (int i = 0; i < templateNum; i++) {
      template = new Template();
      template.deserialize(buffer);
      templateMap.put(template.getId(), template);
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(buffer);
    return new LevelTimeSeriesCountNode(
        planNodeId, path, isPrefixPath, level, schemaFilter, templateMap, scope);
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
    LevelTimeSeriesCountNode that = (LevelTimeSeriesCountNode) o;
    return level == that.level;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), level);
  }

  @Override
  public String toString() {
    return String.format(
        "LevelTimeSeriesCountNode-%s:[DataRegion: %s]",
        this.getPlanNodeId(), this.getRegionReplicaSet());
  }
}
