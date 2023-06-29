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

package org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

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
  private final String key;
  private final String value;
  private final boolean isContains;
  private final Map<Integer, Template> templateMap;

  public LevelTimeSeriesCountNode(
      PlanNodeId id,
      PartialPath partialPath,
      boolean isPrefixPath,
      int level,
      String key,
      String value,
      boolean isContains,
      @NotNull Map<Integer, Template> templateMap) {
    super(id, partialPath, isPrefixPath);
    this.level = level;
    this.key = key;
    this.value = value;
    this.isContains = isContains;
    this.templateMap = templateMap;
  }

  public int getLevel() {
    return level;
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

  public Map<Integer, Template> getTemplateMap() {
    return templateMap;
  }

  @Override
  public PlanNode clone() {
    return new LevelTimeSeriesCountNode(
        getPlanNodeId(), path, isPrefixPath, level, key, value, isContains, templateMap);
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
    ReadWriteIOUtils.write(isPrefixPath, byteBuffer);
    ReadWriteIOUtils.write(level, byteBuffer);
    ReadWriteIOUtils.write(key, byteBuffer);
    ReadWriteIOUtils.write(value, byteBuffer);
    ReadWriteIOUtils.write(isContains, byteBuffer);
    ReadWriteIOUtils.write(templateMap.size(), byteBuffer);
    for (Template template : templateMap.values()) {
      template.serialize(byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.LEVEL_TIME_SERIES_COUNT.serialize(stream);
    ReadWriteIOUtils.write(path.getFullPath(), stream);
    ReadWriteIOUtils.write(isPrefixPath, stream);
    ReadWriteIOUtils.write(level, stream);
    ReadWriteIOUtils.write(key, stream);
    ReadWriteIOUtils.write(value, stream);
    ReadWriteIOUtils.write(isContains, stream);
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
    boolean isPrefixPath = ReadWriteIOUtils.readBool(buffer);
    int level = ReadWriteIOUtils.readInt(buffer);
    String key = ReadWriteIOUtils.readString(buffer);
    String value = ReadWriteIOUtils.readString(buffer);
    boolean isContains = ReadWriteIOUtils.readBool(buffer);
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
        planNodeId, path, isPrefixPath, level, key, value, isContains, templateMap);
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
