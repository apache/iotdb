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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeUtil;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.schemaengine.template.Template;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/** This class defines the scan task of schema fetcher. */
public class SeriesSchemaFetchScanNode extends SchemaFetchScanNode {

  private final Map<Integer, Template> templateMap;
  private final boolean withTags;
  private final boolean withAttributes;
  private final boolean withTemplate;
  private final boolean withAliasForce;

  public SeriesSchemaFetchScanNode(
      PlanNodeId id,
      PartialPath storageGroup,
      PathPatternTree patternTree,
      Map<Integer, Template> templateMap,
      boolean withTags,
      boolean withAttributes,
      boolean withTemplate,
      boolean withAliasForce) {
    super(id, storageGroup, patternTree);
    this.templateMap = templateMap;
    this.withTags = withTags;
    this.withAttributes = withAttributes;
    this.withTemplate = withTemplate;
    this.withAliasForce = withAliasForce;
  }

  public Map<Integer, Template> getTemplateMap() {
    return templateMap;
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.SERIES_SCHEMA_FETCH_SCAN;
  }

  @Override
  public PlanNode clone() {
    return new SeriesSchemaFetchScanNode(
        getPlanNodeId(),
        storageGroup,
        patternTree,
        templateMap,
        withTags,
        withAttributes,
        withTemplate,
        withAliasForce);
  }

  @Override
  public String toString() {
    return String.format(
        "SeriesSchemaFetchScanNode-%s:[StorageGroup: %s, DataRegion: %s]",
        this.getPlanNodeId(),
        storageGroup,
        PlanNodeUtil.printRegionReplicaSet(getRegionReplicaSet()));
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.SERIES_SCHEMA_FETCH_SCAN.serialize(byteBuffer);
    storageGroup.serialize(byteBuffer);
    patternTree.serialize(byteBuffer);

    ReadWriteIOUtils.write(templateMap.size(), byteBuffer);
    for (Template template : templateMap.values()) {
      template.serialize(byteBuffer);
    }
    ReadWriteIOUtils.write(withTags, byteBuffer);
    ReadWriteIOUtils.write(withAttributes, byteBuffer);
    ReadWriteIOUtils.write(withTemplate, byteBuffer);
    ReadWriteIOUtils.write(withAliasForce, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.SERIES_SCHEMA_FETCH_SCAN.serialize(stream);
    storageGroup.serialize(stream);
    patternTree.serialize(stream);
    ReadWriteIOUtils.write(templateMap.size(), stream);
    for (Template template : templateMap.values()) {
      template.serialize(stream);
    }
    ReadWriteIOUtils.write(withTags, stream);
    ReadWriteIOUtils.write(withAttributes, stream);
    ReadWriteIOUtils.write(withTemplate, stream);
    ReadWriteIOUtils.write(withAliasForce, stream);
  }

  public static SeriesSchemaFetchScanNode deserialize(ByteBuffer byteBuffer) {
    PartialPath storageGroup = (PartialPath) PathDeserializeUtil.deserialize(byteBuffer);
    PathPatternTree patternTree = PathPatternTree.deserialize(byteBuffer);

    int templateNum = ReadWriteIOUtils.readInt(byteBuffer);
    Map<Integer, Template> templateMap = new HashMap<>();
    Template template;
    for (int i = 0; i < templateNum; i++) {
      template = new Template();
      template.deserialize(byteBuffer);
      templateMap.put(template.getId(), template);
    }
    boolean withTags = ReadWriteIOUtils.readBool(byteBuffer);
    boolean withAttributes = ReadWriteIOUtils.readBool(byteBuffer);
    boolean withTemplate = ReadWriteIOUtils.readBool(byteBuffer);
    boolean withAliasForce = ReadWriteIOUtils.readBool(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new SeriesSchemaFetchScanNode(
        planNodeId,
        storageGroup,
        patternTree,
        templateMap,
        withTags,
        withAttributes,
        withTemplate,
        withAliasForce);
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitSeriesSchemaFetchScan(this, context);
  }

  public boolean isWithTags() {
    return withTags;
  }

  public boolean isWithTemplate() {
    return withTemplate;
  }

  public boolean isWithAttributes() {
    return withAttributes;
  }

  public boolean isWithAliasForce() {
    return withAliasForce;
  }
}
