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
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

public class TimeSeriesCountNode extends SchemaQueryScanNode {

  private final String key;
  private final String value;
  private final boolean isContains;

  public TimeSeriesCountNode(
      PlanNodeId id,
      PartialPath partialPath,
      boolean isPrefixPath,
      String key,
      String value,
      boolean isContains) {
    super(id, partialPath, isPrefixPath);
    this.key = key;
    this.value = value;
    this.isContains = isContains;
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

  @Override
  public PlanNode clone() {
    return new TimeSeriesCountNode(getPlanNodeId(), path, isPrefixPath, key, value, isContains);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return ColumnHeaderConstant.countTimeSeriesColumnHeaders.stream()
        .map(ColumnHeader::getColumnName)
        .collect(Collectors.toList());
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.TIME_SERIES_COUNT.serialize(byteBuffer);
    ReadWriteIOUtils.write(path.getFullPath(), byteBuffer);
    ReadWriteIOUtils.write(isPrefixPath, byteBuffer);
    ReadWriteIOUtils.write(key, byteBuffer);
    ReadWriteIOUtils.write(value, byteBuffer);
    ReadWriteIOUtils.write(isContains, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.TIME_SERIES_COUNT.serialize(stream);
    ReadWriteIOUtils.write(path.getFullPath(), stream);
    ReadWriteIOUtils.write(isPrefixPath, stream);
    ReadWriteIOUtils.write(key, stream);
    ReadWriteIOUtils.write(value, stream);
    ReadWriteIOUtils.write(isContains, stream);
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
    String key = ReadWriteIOUtils.readString(buffer);
    String value = ReadWriteIOUtils.readString(buffer);
    boolean isContains = ReadWriteIOUtils.readBool(buffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(buffer);
    return new TimeSeriesCountNode(planNodeId, path, isPrefixPath, key, value, isContains);
  }
}
