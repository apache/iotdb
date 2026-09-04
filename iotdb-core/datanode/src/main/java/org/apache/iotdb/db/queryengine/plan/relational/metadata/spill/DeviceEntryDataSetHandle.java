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

package org.apache.iotdb.db.queryengine.plan.relational.metadata.spill;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public final class DeviceEntryDataSetHandle {

  private final String queryId;
  private final PlanNodeId planNodeId;
  private final TEndPoint coordinatorMppDataExchangeEndPoint;
  private final int segmentCount;
  private final int entryCount;
  private final boolean ordered;

  public DeviceEntryDataSetHandle(
      String queryId,
      PlanNodeId planNodeId,
      TEndPoint coordinatorMppDataExchangeEndPoint,
      int segmentCount,
      int entryCount,
      boolean ordered) {
    if (segmentCount < 0 || entryCount < 0) {
      throw new IllegalArgumentException(
          DataNodeQueryMessages
              .EXCEPTION_DEVICEENTRY_SEGMENT_AND_ENTRY_COUNTS_MUST_BE_NON_NEGATIVE_7C6E569C);
    }
    this.queryId = queryId;
    this.planNodeId = planNodeId;
    this.coordinatorMppDataExchangeEndPoint = coordinatorMppDataExchangeEndPoint;
    this.segmentCount = segmentCount;
    this.entryCount = entryCount;
    this.ordered = ordered;
  }

  public String getQueryId() {
    return queryId;
  }

  public PlanNodeId getPlanNodeId() {
    return planNodeId;
  }

  public TEndPoint getCoordinatorMppDataExchangeEndPoint() {
    return coordinatorMppDataExchangeEndPoint;
  }

  public int getSegmentCount() {
    return segmentCount;
  }

  public int getEntryCount() {
    return entryCount;
  }

  public boolean isOrdered() {
    return ordered;
  }

  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(queryId, byteBuffer);
    ReadWriteIOUtils.write(planNodeId.getId(), byteBuffer);
    ThriftCommonsSerDeUtils.serializeTEndPoint(coordinatorMppDataExchangeEndPoint, byteBuffer);
    ReadWriteIOUtils.write(segmentCount, byteBuffer);
    ReadWriteIOUtils.write(entryCount, byteBuffer);
    ReadWriteIOUtils.write(ordered, byteBuffer);
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(queryId, stream);
    ReadWriteIOUtils.write(planNodeId.getId(), stream);
    ThriftCommonsSerDeUtils.serializeTEndPoint(coordinatorMppDataExchangeEndPoint, stream);
    ReadWriteIOUtils.write(segmentCount, stream);
    ReadWriteIOUtils.write(entryCount, stream);
    ReadWriteIOUtils.write(ordered, stream);
  }

  public static DeviceEntryDataSetHandle deserialize(ByteBuffer byteBuffer) {
    return new DeviceEntryDataSetHandle(
        ReadWriteIOUtils.readString(byteBuffer),
        new PlanNodeId(ReadWriteIOUtils.readString(byteBuffer)),
        ThriftCommonsSerDeUtils.deserializeTEndPoint(byteBuffer),
        ReadWriteIOUtils.readInt(byteBuffer),
        ReadWriteIOUtils.readInt(byteBuffer),
        ReadWriteIOUtils.readBool(byteBuffer));
  }
}
