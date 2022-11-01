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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class PathsUsingTemplateScanNode extends SchemaQueryScanNode {

  private List<PartialPath> pathPatternList;

  private int templateId;

  public PathsUsingTemplateScanNode(
      PlanNodeId id, List<PartialPath> pathPatternList, int templateId) {
    super(id);
    this.pathPatternList = pathPatternList;
    this.templateId = templateId;
  }

  public List<PartialPath> getPathPatternList() {
    return pathPatternList;
  }

  public void setPathPatternList(List<PartialPath> pathPatternList) {
    this.pathPatternList = pathPatternList;
  }

  public int getTemplateId() {
    return templateId;
  }

  @Override
  public PlanNode clone() {
    return new PathsUsingTemplateScanNode(getPlanNodeId(), pathPatternList, templateId);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return ColumnHeaderConstant.showPathsUsingTemplateHeaders.stream()
        .map(ColumnHeader::getColumnName)
        .collect(Collectors.toList());
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.PATHS_USING_TEMPLATE_SCAN.serialize(byteBuffer);
    ReadWriteIOUtils.write(pathPatternList.size(), byteBuffer);
    for (PartialPath pathPattern : pathPatternList) {
      pathPattern.serialize(byteBuffer);
    }
    ReadWriteIOUtils.write(templateId, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.PATHS_USING_TEMPLATE_SCAN.serialize(stream);
    ReadWriteIOUtils.write(pathPatternList.size(), stream);
    for (PartialPath pathPattern : pathPatternList) {
      pathPattern.serialize(stream);
    }
    ReadWriteIOUtils.write(templateId, stream);
  }

  public static PathsUsingTemplateScanNode deserialize(ByteBuffer buffer) {
    int size = ReadWriteIOUtils.readInt(buffer);
    List<PartialPath> pathPatternList = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      pathPatternList.add((PartialPath) PathDeserializeUtil.deserialize(buffer));
    }
    int templateId = ReadWriteIOUtils.readInt(buffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(buffer);
    return new PathsUsingTemplateScanNode(planNodeId, pathPatternList, templateId);
  }

  @Override
  public String toString() {
    return String.format(
        "PathsUsingTemplateScanNode-%s:[DataRegion: %s]",
        this.getPlanNodeId(), this.getRegionReplicaSet());
  }
}
