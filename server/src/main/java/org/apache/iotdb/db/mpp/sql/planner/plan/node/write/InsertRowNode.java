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
package org.apache.iotdb.db.mpp.sql.planner.plan.node.write;

import org.apache.iotdb.commons.partition.TimePartitionId;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.sql.analyze.Analysis;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class InsertRowNode extends InsertNode {

  private long time;
  private Object[] values;
  private boolean isNeedInferType = false;

  public InsertRowNode(PlanNodeId id) {
    super(id);
  }

  public InsertRowNode(
      PlanNodeId id,
      PartialPath devicePath,
      boolean isAligned,
      MeasurementSchema[] measurements,
      TSDataType[] dataTypes,
      long time,
      Object[] values,
      boolean isNeedInferType) {
    super(id, devicePath, isAligned, measurements, dataTypes);
    this.time = time;
    this.values = values;
    this.isNeedInferType = isNeedInferType;
  }

  @Override
  public List<InsertNode> splitByPartition(Analysis analysis) {
    TimePartitionId timePartitionId = StorageEngine.getTimePartitionId(time);
    this.dataRegionReplicaSet =
        analysis
            .getDataPartitionInfo()
            .getDataRegionReplicaSetForWriting(devicePath.getFullPath(), timePartitionId);
    return Collections.singletonList(this);
  }

  @Override
  public List<PlanNode> getChildren() {
    return null;
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  public PlanNode clone() {
    throw new NotImplementedException("clone of Insert is not implemented");
  }

  @Override
  public int allowedChildCount() {
    return NO_CHILD_ALLOWED;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return null;
  }

  public static InsertRowNode deserialize(ByteBuffer byteBuffer) {
    return null;
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {}

  public boolean isNeedInferType() {
    return isNeedInferType;
  }

  public void setNeedInferType(boolean needInferType) {
    isNeedInferType = needInferType;
  }

  public Object[] getValues() {
    return values;
  }

  public void setValues(Object[] values) {
    this.values = values;
  }

  public long getTime() {
    return time;
  }

  public void setTime(long time) {
    this.time = time;
  }
}
