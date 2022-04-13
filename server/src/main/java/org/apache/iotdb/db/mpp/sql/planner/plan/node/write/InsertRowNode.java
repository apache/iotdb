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

import org.apache.iotdb.commons.partition.TimePartitionSlot;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.sql.analyze.Analysis;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.wal.buffer.WALEntryValue;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class InsertRowNode extends InsertNode implements WALEntryValue {

  private static final Logger logger = LoggerFactory.getLogger(InsertRowNode.class);

  private static final byte TYPE_NULL = -2;

  private long time;
  private Object[] values;

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
      Object[] values) {
    super(id, devicePath, isAligned, measurements, dataTypes);
    this.time = time;
    this.values = values;
  }

  @Override
  public List<InsertNode> splitByPartition(Analysis analysis) {
    TimePartitionSlot timePartitionSlot = StorageEngine.getTimePartitionSlot(time);
    this.dataRegionReplicaSet =
        analysis
            .getDataPartitionInfo()
            .getDataRegionReplicaSetForWriting(devicePath.getFullPath(), timePartitionSlot);
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
  public int serializedSize() {
    return 0;
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    byteBuffer.putShort((short) PlanNodeType.INSERT_ROW.ordinal());
    ReadWriteIOUtils.write(this.getPlanNodeId().getId(), byteBuffer);
    subSerialize(byteBuffer);
  }

  void subSerialize(ByteBuffer buffer) {
    buffer.putLong(time);
    ReadWriteIOUtils.write(devicePath.getFullPath(),buffer);
    serializeMeasurementsAndValues(buffer);
  }

  void serializeMeasurementsAndValues(ByteBuffer buffer) {
//    buffer.putInt(
//            measurements.length - ( == null ? 0 : failedMeasurements.size()));


    for (String measurement : measurements) {
      if (measurement != null) {
        ReadWriteIOUtils.write(measurement,buffer);
      }
    }
    try {
      buffer.putInt(dataTypes.length);
      putValues(buffer);
    } catch (QueryProcessException e) {
      logger.error("Failed to serialize values for {}", this, e);
    }

    buffer.put((byte) (isAligned ? 1 : 0));
  }

  private void putValues(ByteBuffer buffer) throws QueryProcessException {
    for (int i = 0; i < values.length; i++) {
      if(dataTypes[i] != null) {
        if (values[i] == null){
          ReadWriteIOUtils.write(TYPE_NULL, buffer);
          continue;
        }
        ReadWriteIOUtils.write(dataTypes[i], buffer);
        switch (dataTypes[i]) {
          case BOOLEAN:
            ReadWriteIOUtils.write((Boolean) values[i], buffer);
            break;
          case INT32:
            ReadWriteIOUtils.write((Integer) values[i], buffer);
            break;
          case INT64:
            ReadWriteIOUtils.write((Long) values[i], buffer);
            break;
          case FLOAT:
            ReadWriteIOUtils.write((Float) values[i], buffer);
            break;
          case DOUBLE:
            ReadWriteIOUtils.write((Double) values[i], buffer);
            break;
          case TEXT:
            ReadWriteIOUtils.write((Binary) values[i], buffer);
            break;
          default:
            throw new QueryProcessException("Unsupported data type:" + dataTypes[i]);
        }
      }
    }
  }

  @Override
  public void serializeToWAL(IWALByteBufferView buffer) {}

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

  public static InsertRowNode deserialize(ByteBuffer byteBuffer) {
    return null;
  }
}
