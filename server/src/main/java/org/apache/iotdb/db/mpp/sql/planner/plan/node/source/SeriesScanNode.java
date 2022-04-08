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
package org.apache.iotdb.db.mpp.sql.planner.plan.node.source;

import java.io.IOException;
import org.apache.iotdb.commons.partition.RegionReplicaSet;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.path.PathDeserializeUtil;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.sql.statement.component.OrderBy;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import com.google.common.collect.ImmutableList;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * SeriesScanOperator is responsible for read data a specific series. When reading data, the
 * SeriesScanOperator can read the raw data batch by batch. And also, it can leverage the filter and
 * other info to decrease the result set.
 *
 * <p>Children type: no child is allowed for SeriesScanNode
 */
public class SeriesScanNode extends SourceNode {

  // The path of the target series which will be scanned.
  private PartialPath seriesPath;

  // all the sensors in seriesPath's device of current query
  private Set<String> allSensors;

  // The order to traverse the data.
  // Currently, we only support TIMESTAMP_ASC and TIMESTAMP_DESC here.
  // The default order is TIMESTAMP_ASC, which means "order by timestamp asc"
  private OrderBy scanOrder = OrderBy.TIMESTAMP_ASC;

  // time filter for current series, could be null if doesn't exist
  private Filter timeFilter;

  // value filter for current series, could be null if doesn't exist
  private Filter valueFilter;

  // Limit for result set. The default value is -1, which means no limit
  private int limit;

  // offset for result set. The default value is 0
  private int offset;

  private String columnName;

  // The id of DataRegion where the node will run
  private RegionReplicaSet regionReplicaSet;

  public SeriesScanNode(PlanNodeId id, PartialPath seriesPath) {
    super(id);
    this.seriesPath = seriesPath;
  }

  public SeriesScanNode(PlanNodeId id, PartialPath seriesPath, RegionReplicaSet regionReplicaSet) {
    this(id, seriesPath);
    this.regionReplicaSet = regionReplicaSet;
  }

  public void setTimeFilter(Filter timeFilter) {
    this.timeFilter = timeFilter;
  }

  public void setValueFilter(Filter valueFilter) {
    this.valueFilter = valueFilter;
  }

  @Override
  public void close() throws Exception {}

  @Override
  public void open() throws Exception {}

  @Override
  public RegionReplicaSet getDataRegionReplicaSet() {
    return regionReplicaSet;
  }

  public void setDataRegionReplicaSet(RegionReplicaSet dataRegion) {
    this.regionReplicaSet = dataRegion;
  }

  @Override
  public String getDeviceName() {
    return seriesPath.getDevice();
  }

  @Override
  protected String getExpressionString() {
    return seriesPath.getFullPath();
  }

  public int getLimit() {
    return limit;
  }

  public int getOffset() {
    return offset;
  }

  public void setScanOrder(OrderBy scanOrder) {
    this.scanOrder = scanOrder;
  }

  public void setLimit(int limit) {
    this.limit = limit;
  }

  public void setOffset(int offset) {
    this.offset = offset;
  }

  @Override
  public List<PlanNode> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  public PlanNode clone() {
    return new SeriesScanNode(getId(), getSeriesPath(), this.regionReplicaSet);
  }

  @Override
  public int allowedChildCount() {
    return NO_CHILD_ALLOWED;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return ImmutableList.of(columnName);
  }

  public Set<String> getAllSensors() {
    return allSensors;
  }

  public OrderBy getScanOrder() {
    return scanOrder;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitSeriesScan(this, context);
  }

  public static SeriesScanNode deserialize(ByteBuffer byteBuffer) {
    PartialPath partialPath = (PartialPath) PathDeserializeUtil.deserialize(byteBuffer);
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    Set<String> allSensors = null;
    if (size != -1) {
      allSensors = new HashSet<>();
      for (int i = 0; i < size; i++) {
        allSensors.add(ReadWriteIOUtils.readString(byteBuffer));
      }
    }
    OrderBy scanOrder = OrderBy.values()[ReadWriteIOUtils.readInt(byteBuffer)];
    byte isNull = ReadWriteIOUtils.readByte(byteBuffer);
    Filter timeFilter = null;
    if (isNull == 1) timeFilter = FilterFactory.deserialize(byteBuffer);
    isNull = ReadWriteIOUtils.readByte(byteBuffer);
    Filter valueFilter = null;
    if (isNull == 1) valueFilter = FilterFactory.deserialize(byteBuffer);
    int limit = ReadWriteIOUtils.readInt(byteBuffer);
    int offset = ReadWriteIOUtils.readInt(byteBuffer);
    String columnName = ReadWriteIOUtils.readString(byteBuffer);
    RegionReplicaSet dataRegionReplicaSet = new RegionReplicaSet();
    try {
      dataRegionReplicaSet.deserializeImpl(byteBuffer);
    } catch (IOException e) {
      e.printStackTrace();
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    SeriesScanNode seriesScanNode = new SeriesScanNode(planNodeId, partialPath);
    seriesScanNode.allSensors = allSensors;
    seriesScanNode.columnName = columnName;
    seriesScanNode.limit = limit;
    seriesScanNode.offset = offset;
    seriesScanNode.scanOrder = scanOrder;
    seriesScanNode.regionReplicaSet = dataRegionReplicaSet;
    seriesScanNode.timeFilter = timeFilter;
    seriesScanNode.valueFilter = valueFilter;
    return seriesScanNode;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.SERIES_SCAN.serialize(byteBuffer);
    seriesPath.serialize(byteBuffer);
    if (allSensors == null) {
      ReadWriteIOUtils.write(-1, byteBuffer);
    } else {
      ReadWriteIOUtils.write(allSensors.size(), byteBuffer);
      for (String sensor : allSensors) {
        ReadWriteIOUtils.write(sensor, byteBuffer);
      }
    }
    ReadWriteIOUtils.write(scanOrder.ordinal(), byteBuffer);
    if (timeFilter == null) {
      ReadWriteIOUtils.write((byte) 0, byteBuffer);
    } else {
      ReadWriteIOUtils.write((byte) 1, byteBuffer);
      timeFilter.serialize(byteBuffer);
    }

    if (valueFilter == null) {
      ReadWriteIOUtils.write((byte) 0, byteBuffer);
    } else {
      ReadWriteIOUtils.write((byte) 1, byteBuffer);
      valueFilter.serialize(byteBuffer);
    }
    ReadWriteIOUtils.write(limit, byteBuffer);
    ReadWriteIOUtils.write(offset, byteBuffer);
    ReadWriteIOUtils.write(columnName, byteBuffer);
    regionReplicaSet.serializeImpl(byteBuffer);
  }

  public PartialPath getSeriesPath() {
    return seriesPath;
  }

  public Filter getTimeFilter() {
    return timeFilter;
  }

  public Filter getValueFilter() {
    return valueFilter;
  }

  public String toString() {
    return String.format(
        "SeriesScanNode-%s:[SeriesPath: %s, DataRegion: %s]",
        this.getId(), this.getSeriesPath(), this.getDataRegionReplicaSet());
  }

  @TestOnly
  public Pair<String, List<String>> print() {
    String title = String.format("[SeriesScanNode (%s)]", this.getId());
    List<String> attributes = new ArrayList<>();
    attributes.add("SeriesPath: " + this.getSeriesPath());
    attributes.add("scanOrder: " + this.getScanOrder());
    return new Pair<>(title, attributes);
  }
}
