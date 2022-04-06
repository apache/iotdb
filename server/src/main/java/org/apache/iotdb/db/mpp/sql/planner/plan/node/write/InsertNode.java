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

import org.apache.iotdb.db.metadata.idtable.entry.IDeviceID;
import org.apache.iotdb.db.metadata.idtable.entry.PlainDeviceID;
import org.apache.iotdb.db.metadata.idtable.entry.SHA256DeviceID;
import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.sql.analyze.Analysis;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.nio.ByteBuffer;
import java.util.List;
import sun.security.krb5.internal.PAData;

public abstract class InsertNode extends PlanNode {

  /**
   * if use id table, this filed is id form of device path <br>
   * if not, this filed is device path<br>
   */
  protected PartialPath devicePath;

  protected boolean isAligned;
  protected MeasurementSchema[] measurements;
  protected TSDataType[] dataTypes;
  // TODO(INSERT) need to change it to a function handle to update last time value
  //  protected IMeasurementMNode[] measurementMNodes;

  /**
   * device id reference, for reuse device id in both id table and memtable <br>
   * used in memtable
   */
  protected IDeviceID deviceID;

  protected InsertNode(PlanNodeId id) {
    super(id);
  }

  // TODO(INSERT) split this insert node into multiple InsertNode according to the data partition
  // info
  public abstract List<InsertNode> splitByPartition(Analysis analysis);

  public boolean needSplit() {
    return true;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    if (devicePath instanceof MeasurementPath) {
      ReadWriteIOUtils.write((byte)0, byteBuffer);
    } else if (devicePath instanceof AlignedPath) {
      ReadWriteIOUtils.write((byte)1, byteBuffer);
    } else {
      ReadWriteIOUtils.write((byte)2, byteBuffer);
    }
    devicePath.serialize(byteBuffer);
    ReadWriteIOUtils.write(isAligned, byteBuffer);
    ReadWriteIOUtils.write(measurements.length, byteBuffer);
    for (int i = 0; i < measurements.length; i ++) {
      measurements[i].serializeTo(byteBuffer);
    }
    ReadWriteIOUtils.write(dataTypes.length, byteBuffer);
    for (int i = 0; i < dataTypes.length; i ++) {
      dataTypes[i].serializeTo(byteBuffer);
    }
    if (deviceID instanceof PlainDeviceID) {
      ReadWriteIOUtils.write((byte)0, byteBuffer);
    } else if (deviceID instanceof SHA256DeviceID) {
      ReadWriteIOUtils.write((byte)1, byteBuffer);
    }
    deviceID.serialize(byteBuffer);
  }

  protected static void deserializeAttributes(InsertNode insertNode, ByteBuffer byteBuffer) {
    PartialPath partialPath = null;
    byte pathType = ReadWriteIOUtils.readByte(byteBuffer);
    if (pathType == 0) {
      partialPath = MeasurementPath.deserialize(byteBuffer);
    } else if (pathType == 1) {
      partialPath = AlignedPath.deserialize(byteBuffer);
    } else {
      partialPath = PartialPath.deserialize(byteBuffer);
    }
    boolean isAligned = ReadWriteIOUtils.readBool(byteBuffer);
    int measurementSize = ReadWriteIOUtils.readInt(byteBuffer);
    MeasurementSchema[] measurements = new MeasurementSchema[measurementSize];
    for (int i = 0; i < measurementSize; i ++) {
      measurements[i] = MeasurementSchema.deserializeFrom(byteBuffer);
    }
    int dataTypeSize = ReadWriteIOUtils.readInt(byteBuffer);
    TSDataType[] dataTypes = new TSDataType[dataTypeSize];
    for (int i = 0; i < dataTypeSize; i ++) {
      dataTypes[i] = TSDataType.deserializeFrom(byteBuffer);
    }

    byte deviceIdType = ReadWriteIOUtils.readByte(byteBuffer);
    IDeviceID deviceID = null;
    if (deviceIdType == 0) {
      deviceID = PlainDeviceID.deserialize(byteBuffer);
    } else {
      deviceID = SHA256DeviceID.deserialize(byteBuffer);
    }
    insertNode.deviceID = deviceID;
    insertNode.devicePath = partialPath;
    insertNode.isAligned = isAligned;
    insertNode.dataTypes = dataTypes;
    insertNode.measurements = measurements;
  }

  protected static void copyAttributes(InsertNode destNode, InsertNode srcNode) {
    destNode.deviceID = srcNode.deviceID;
    destNode.devicePath = srcNode.devicePath;
    destNode.isAligned = srcNode.isAligned;
    destNode.dataTypes = srcNode.dataTypes;
    destNode.measurements = srcNode.measurements;
  }
}
