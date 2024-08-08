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

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeUtil;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SourceNode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import com.google.common.collect.ImmutableList;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** This class defines the scan task of schema fetcher. */
public class SchemaFetchScanNode extends SourceNode {

  private final PartialPath storageGroup;
  private final PathPatternTree patternTree;
  private final Map<Integer, Template> templateMap;
  private final boolean withTags;
  private TRegionReplicaSet schemaRegionReplicaSet;

  public SchemaFetchScanNode(
      PlanNodeId id,
      PartialPath storageGroup,
      PathPatternTree patternTree,
      Map<Integer, Template> templateMap,
      boolean withTags) {
    super(id);
    this.storageGroup = storageGroup;
    this.patternTree = patternTree;
    this.patternTree.constructTree();
    this.templateMap = templateMap;
    this.withTags = withTags;
  }

  public PartialPath getStorageGroup() {
    return storageGroup;
  }

  public PathPatternTree getPatternTree() {
    return patternTree;
  }

  public Map<Integer, Template> getTemplateMap() {
    return templateMap;
  }

  @Override
  public List<PlanNode> getChildren() {
    return Collections.emptyList();
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  public PlanNode clone() {
    return new SchemaFetchScanNode(
        getPlanNodeId(), storageGroup, patternTree, templateMap, withTags);
  }

  @Override
  public int allowedChildCount() {
    return 0;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return ImmutableList.of();
  }

  @Override
  public String toString() {
    return String.format(
        "SchemaFetchScanNode-%s:[StorageGroup: %s, DataRegion: %s]",
        this.getPlanNodeId(),
        storageGroup,
        PlanNodeUtil.printRegionReplicaSet(getRegionReplicaSet()));
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.SCHEMA_FETCH_SCAN.serialize(byteBuffer);
    storageGroup.serialize(byteBuffer);
    patternTree.serialize(byteBuffer);

    ReadWriteIOUtils.write(templateMap.size(), byteBuffer);
    for (Template template : templateMap.values()) {
      template.serialize(byteBuffer);
    }
    ReadWriteIOUtils.write(withTags, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.SCHEMA_FETCH_SCAN.serialize(stream);
    storageGroup.serialize(stream);
    patternTree.serialize(stream);
    ReadWriteIOUtils.write(templateMap.size(), stream);
    for (Template template : templateMap.values()) {
      template.serialize(stream);
    }
    ReadWriteIOUtils.write(withTags, stream);
  }

  public static SchemaFetchScanNode deserialize(ByteBuffer byteBuffer) {
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
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new SchemaFetchScanNode(planNodeId, storageGroup, patternTree, templateMap, withTags);
  }

  @Override
  public void open() throws Exception {}

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return schemaRegionReplicaSet;
  }

  @Override
  public void setRegionReplicaSet(TRegionReplicaSet schemaRegionReplicaSet) {
    this.schemaRegionReplicaSet = schemaRegionReplicaSet;
  }

  @Override
  public void close() throws Exception {}

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitSchemaFetchScan(this, context);
  }

  public boolean isWithTags() {
    return withTags;
  }
}
