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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.AlterTimeSeriesStatement.AlterType;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.exception.NotImplementedException;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class AlterTimeSeriesNode extends WritePlanNode {
  private PartialPath path;
  private AlterType alterType;

  /**
   * used when the alterType is RENAME, SET, DROP, ADD_TAGS, ADD_ATTRIBUTES. when the alterType is
   * RENAME, alterMap has only one entry, key is the previousName, value is the currentName. when
   * the alterType is DROP, only the keySet of alterMap is useful, it contains all the key names
   * needed to be removed
   */
  private Map<String, String> alterMap;

  /** Used when the alterType is UPSERT. */
  private String alias;

  private Map<String, String> tagsMap;
  private Map<String, String> attributesMap;

  private boolean isAlterView;

  private TRegionReplicaSet regionReplicaSet;

  public AlterTimeSeriesNode(
      PlanNodeId id,
      PartialPath path,
      AlterType alterType,
      Map<String, String> alterMap,
      String alias,
      Map<String, String> tagsMap,
      Map<String, String> attributesMap,
      boolean isAlterView) {
    super(id);
    this.path = path;
    this.alterType = alterType;
    this.alterMap = alterMap;
    this.alias = alias;
    this.tagsMap = tagsMap;
    this.attributesMap = attributesMap;
    this.isAlterView = isAlterView;
  }

  public PartialPath getPath() {
    return path;
  }

  public void setPath(PartialPath path) {
    this.path = path;
  }

  public AlterType getAlterType() {
    return alterType;
  }

  public void setAlterType(AlterType alterType) {
    this.alterType = alterType;
  }

  public Map<String, String> getAlterMap() {
    return alterMap;
  }

  public void setAlterMap(Map<String, String> alterMap) {
    this.alterMap = alterMap;
  }

  public String getAlias() {
    return alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  public Map<String, String> getTagsMap() {
    return tagsMap;
  }

  public void setTagsMap(Map<String, String> tagsMap) {
    this.tagsMap = tagsMap;
  }

  public Map<String, String> getAttributesMap() {
    return attributesMap;
  }

  public void setAttributesMap(Map<String, String> attributesMap) {
    this.attributesMap = attributesMap;
  }

  public boolean isAlterView() {
    return isAlterView;
  }

  public void setAlterView(boolean alterView) {
    isAlterView = alterView;
  }

  @Override
  public List<PlanNode> getChildren() {
    return null;
  }

  @Override
  public void addChild(PlanNode child) {
    // Do nothing
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.ALTER_TIME_SERIES;
  }

  @Override
  public PlanNode clone() {
    throw new NotImplementedException("Clone of AlterTimeSeriesNode is not implemented");
  }

  @Override
  public int allowedChildCount() {
    return NO_CHILD_ALLOWED;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return null;
  }

  public static AlterTimeSeriesNode deserialize(ByteBuffer byteBuffer) {
    String id;
    MeasurementPath path;
    AlterType alterType;
    String alias = null;
    Map<String, String> alterMap = null;
    Map<String, String> tagsMap = null;
    Map<String, String> attributesMap = null;

    int length = byteBuffer.getInt();
    byte[] bytes = new byte[length];
    byteBuffer.get(bytes);
    try {
      path = new MeasurementPath(new String(bytes, TSFileConfig.STRING_CHARSET));
    } catch (IllegalPathException e) {
      throw new IllegalArgumentException("Can not deserialize AlterTimeSeriesNode", e);
    }

    boolean isAlterView = false;
    int alterTypeOrdinal = byteBuffer.get();
    if (alterTypeOrdinal == -1) {
      isAlterView = true;
      alterTypeOrdinal = byteBuffer.get();
    }

    alterType = AlterType.values()[alterTypeOrdinal];

    // alias
    if (byteBuffer.get() == 1) {
      alias = ReadWriteIOUtils.readString(byteBuffer);
    }

    // alterMap
    byte label = byteBuffer.get();
    if (label == 0) {
      alterMap = new HashMap<>();
    } else if (label == 1) {
      alterMap = ReadWriteIOUtils.readMap(byteBuffer);
    }

    // tagsMap
    label = byteBuffer.get();
    if (label == 0) {
      tagsMap = new HashMap<>();
    } else if (label == 1) {
      tagsMap = ReadWriteIOUtils.readMap(byteBuffer);
    }

    // attributesMap
    label = byteBuffer.get();
    if (label == 0) {
      attributesMap = new HashMap<>();
    } else if (label == 1) {
      attributesMap = ReadWriteIOUtils.readMap(byteBuffer);
    }

    id = ReadWriteIOUtils.readString(byteBuffer);
    return new AlterTimeSeriesNode(
        new PlanNodeId(id), path, alterType, alterMap, alias, tagsMap, attributesMap, isAlterView);
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C schemaRegion) {
    return visitor.visitAlterTimeSeries(this, schemaRegion);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.ALTER_TIME_SERIES.serialize(byteBuffer);
    byte[] bytes = path.getFullPath().getBytes();
    ReadWriteIOUtils.write(bytes.length, byteBuffer);
    byteBuffer.put(bytes);

    if (isAlterView) {
      byteBuffer.put((byte) -1);
    }
    byteBuffer.put((byte) alterType.ordinal());

    // alias
    if (alias != null) {
      byteBuffer.put((byte) 1);
      ReadWriteIOUtils.write(alias, byteBuffer);
    } else {
      byteBuffer.put((byte) 0);
    }

    // alterMap
    if (alterMap == null) {
      byteBuffer.put((byte) -1);
    } else if (alterMap.isEmpty()) {
      byteBuffer.put((byte) 0);
    } else {
      byteBuffer.put((byte) 1);
      ReadWriteIOUtils.write(alterMap, byteBuffer);
    }

    // tagsMap
    if (tagsMap == null) {
      byteBuffer.put((byte) -1);
    } else if (tagsMap.isEmpty()) {
      byteBuffer.put((byte) 0);
    } else {
      byteBuffer.put((byte) 1);
      ReadWriteIOUtils.write(tagsMap, byteBuffer);
    }

    // attributesMap
    if (attributesMap == null) {
      byteBuffer.put((byte) -1);
    } else if (attributesMap.isEmpty()) {
      byteBuffer.put((byte) 0);
    } else {
      byteBuffer.put((byte) 1);
      ReadWriteIOUtils.write(attributesMap, byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.ALTER_TIME_SERIES.serialize(stream);
    byte[] bytes = path.getFullPath().getBytes();
    ReadWriteIOUtils.write(bytes.length, stream);
    stream.write(bytes);

    if (isAlterView) {
      stream.write((byte) -1);
    }
    stream.write((byte) alterType.ordinal());

    // alias
    if (alias != null) {
      stream.write((byte) 1);
      ReadWriteIOUtils.write(alias, stream);
    } else {
      stream.write((byte) 0);
    }

    // alterMap
    if (alterMap == null) {
      stream.write((byte) -1);
    } else if (alterMap.isEmpty()) {
      stream.write((byte) 0);
    } else {
      stream.write((byte) 1);
      ReadWriteIOUtils.write(alterMap, stream);
    }

    // tagsMap
    if (tagsMap == null) {
      stream.write((byte) -1);
    } else if (tagsMap.isEmpty()) {
      stream.write((byte) 0);
    } else {
      stream.write((byte) 1);
      ReadWriteIOUtils.write(tagsMap, stream);
    }

    // attributesMap
    if (attributesMap == null) {
      stream.write((byte) -1);
    } else if (attributesMap.isEmpty()) {
      stream.write((byte) 0);
    } else {
      stream.write((byte) 1);
      ReadWriteIOUtils.write(attributesMap, stream);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AlterTimeSeriesNode that = (AlterTimeSeriesNode) o;

    return this.getPlanNodeId().equals(that.getPlanNodeId())
        && Objects.equals(path, that.path)
        && alterType == that.alterType
        && Objects.equals(alterMap, that.alterMap)
        && Objects.equals(alias, that.alias)
        && Objects.equals(tagsMap, that.tagsMap)
        && Objects.equals(attributesMap, that.attributesMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        this.getPlanNodeId(), path, alias, alterType, alterMap, attributesMap, tagsMap);
  }

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return regionReplicaSet;
  }

  public void setRegionReplicaSet(TRegionReplicaSet regionReplicaSet) {
    this.regionReplicaSet = regionReplicaSet;
  }

  @Override
  public List<WritePlanNode> splitByPartition(IAnalysis analysis) {
    TRegionReplicaSet regionReplicaSet =
        analysis.getSchemaPartitionInfo().getSchemaRegionReplicaSet(path.getIDeviceID());
    setRegionReplicaSet(regionReplicaSet);
    return ImmutableList.of(this);
  }
}
