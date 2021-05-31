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
package org.apache.iotdb.db.qp.physical.crud;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.BatchPlan;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class InsertRowsOfOneDevicePlan extends InsertPlan implements BatchPlan {

  boolean[] isExecuted;
  private InsertRowPlan[] rowPlans;

  public InsertRowsOfOneDevicePlan(
      PartialPath deviceId,
      Long[] insertTimes,
      List<List<String>> measurements,
      ByteBuffer[] insertValues)
      throws QueryProcessException {
    super(OperatorType.BATCH_INSERT_ONE_DEVICE);
    this.deviceId = deviceId;
    rowPlans = new InsertRowPlan[insertTimes.length];
    for (int i = 0; i < insertTimes.length; i++) {
      for (ByteBuffer b : insertValues) {
        b.toString();
      }
      rowPlans[i] =
          new InsertRowPlan(
              deviceId,
              insertTimes[i],
              measurements.get(i).toArray(new String[0]),
              insertValues[i]);
      if (rowPlans[i].getMeasurements().length == 0) {
        throw new QueryProcessException(
            "The measurements are null, deviceId:" + deviceId + ", time:" + insertTimes[i]);
      }
      if (rowPlans[i].getValues().length == 0) {
        throw new QueryProcessException(
            "The size of values in InsertRowsOfOneDevicePlan is 0, deviceId:"
                + deviceId
                + ", time:"
                + insertTimes[i]);
      }
    }
  }

  @Override
  public void checkIntegrity() {}

  // TODO I see InsertRowPlan rewrites the hashCode, but do we need to rewrite hashCode?

  @Override
  public List<PartialPath> getPaths() {
    Set<PartialPath> paths = new HashSet<>();
    for (InsertRowPlan plan : rowPlans) {
      paths.addAll(plan.getPaths());
    }
    return new ArrayList<>(paths);
  }

  @Override
  public long getMinTime() {
    long minTime = Long.MAX_VALUE;
    for (InsertRowPlan plan : rowPlans) {
      if (minTime > plan.getTime()) {
        minTime = plan.getTime();
      }
    }
    return minTime;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    int type = PhysicalPlanType.BATCH_INSERT_ONE_DEVICE.ordinal();
    stream.writeByte((byte) type);
    putString(stream, deviceId.getFullPath());

    stream.writeInt(rowPlans.length);
    for (InsertRowPlan plan : rowPlans) {
      stream.writeLong(plan.getTime());
      plan.serializeMeasurementsAndValues(stream);
    }
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    int type = PhysicalPlanType.BATCH_INSERT_ONE_DEVICE.ordinal();
    buffer.put((byte) type);

    putString(buffer, deviceId.getFullPath());
    buffer.putInt(rowPlans.length);
    for (InsertRowPlan plan : rowPlans) {
      buffer.putLong(plan.getTime());
      plan.serializeMeasurementsAndValues(buffer);
    }
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {
    this.deviceId = new PartialPath(readString(buffer));
    this.rowPlans = new InsertRowPlan[buffer.getInt()];
    for (int i = 0; i < rowPlans.length; i++) {
      rowPlans[i] = new InsertRowPlan();
      rowPlans[i].setDeviceId(deviceId);
      rowPlans[i].setTime(buffer.getLong());
      rowPlans[i].deserializeMeasurementsAndValues(buffer);
    }
  }

  @Override
  public void setIndex(long index) {
    super.setIndex(index);
    for (InsertRowPlan plan : rowPlans) {
      // use the InsertRowsOfOneDevicePlan's index as the sub InsertRowPlan's index
      plan.setIndex(index);
    }
  }

  @Override
  public String toString() {
    return "deviceId: " + deviceId + ", times: " + rowPlans.length;
  }

  @Override
  public InsertPlan getPlanFromFailed() {
    if (super.getPlanFromFailed() == null) {
      return null;
    }
    List<InsertRowPlan> plans = new ArrayList<>();
    for (InsertRowPlan plan : rowPlans) {
      if (plan.hasFailedValues()) {
        plans.add((InsertRowPlan) plan.getPlanFromFailed());
      }
    }
    this.rowPlans = plans.toArray(new InsertRowPlan[0]);
    return this;
  }

  public InsertRowPlan[] getRowPlans() {
    return rowPlans;
  }

  @Override
  public void setIsExecuted(int i) {
    if (isExecuted == null) {
      isExecuted = new boolean[getBatchSize()];
    }
    isExecuted[i] = true;
  }

  @Override
  public boolean isExecuted(int i) {
    if (isExecuted == null) {
      isExecuted = new boolean[getBatchSize()];
    }
    return isExecuted[i];
  }

  @Override
  public int getBatchSize() {
    return rowPlans.length;
  }

  @Override
  public void unsetIsExecuted(int i) {
    if (isExecuted == null) {
      isExecuted = new boolean[getBatchSize()];
    }
    isExecuted[i] = false;
  }
}
