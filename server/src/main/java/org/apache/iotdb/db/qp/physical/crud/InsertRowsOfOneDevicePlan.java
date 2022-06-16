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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.BatchPlan;
import org.apache.iotdb.tsfile.exception.NotImplementedException;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class InsertRowsOfOneDevicePlan extends InsertPlan implements BatchPlan {

  /**
   * This class has some duplicated codes with InsertRowsPlan, they should be refined in the future
   */
  boolean[] isExecuted;

  private InsertRowPlan[] rowPlans;

  /**
   * Suppose there is an InsertRowsOfOneDevicePlan, which contains 5 InsertRowPlans,
   * rowPlans={InsertRowPlan_0, InsertRowPlan_1, InsertRowPlan_2, InsertRowPlan_3, InsertRowPlan_4},
   * then the rowPlanIndexList={0, 1, 2, 3, 4} respectively. But when the InsertRowsOfOneDevicePlan
   * is split into two InsertRowsOfOneDevicePlans according to the time partition in cluster
   * version, suppose that the InsertRowsOfOneDevicePlan_1's rowPlanIndexList = {InsertRowPlan_0,
   * InsertRowPlan_3, InsertRowPlan_4}, then InsertRowsOfOneDevicePlan_1's rowPlanIndexList = {0, 3,
   * 4}; InsertRowsOfOneDevicePlan_2's rowPlanIndexList = {InsertRowPlan_1, InsertRowPlan_2} then
   * InsertRowsOfOneDevicePlan_2's rowPlanIndexList = {1, 2} respectively;
   */
  private int[] rowPlanIndexList;

  /** record the result of insert rows */
  private Map<Integer, TSStatus> results = new HashMap<>();

  private List<PartialPath> paths;

  public InsertRowsOfOneDevicePlan() {
    super(OperatorType.BATCH_INSERT_ONE_DEVICE);
  }

  public InsertRowsOfOneDevicePlan(
      PartialPath prefixPath,
      List<Long> insertTimes,
      List<List<String>> measurements,
      List<ByteBuffer> insertValues,
      boolean isAligned)
      throws QueryProcessException {
    this();
    this.devicePath = prefixPath;
    rowPlans = new InsertRowPlan[insertTimes.size()];
    rowPlanIndexList = new int[insertTimes.size()];
    for (int i = 0; i < insertTimes.size(); i++) {
      rowPlans[i] =
          new InsertRowPlan(
              prefixPath,
              insertTimes.get(i),
              measurements.get(i).toArray(new String[0]),
              insertValues.get(i),
              isAligned);
      if (rowPlans[i].getMeasurements().length == 0) {
        throw new QueryProcessException(
            "The measurements are null, deviceId:" + prefixPath + ", time:" + insertTimes.get(i));
      }
      if (rowPlans[i].getValues().length == 0) {
        throw new QueryProcessException(
            "The size of values in InsertRowsOfOneDevicePlan is 0, deviceId:"
                + prefixPath
                + ", time:"
                + insertTimes.get(i));
      }
      rowPlanIndexList[i] = i;
    }
  }

  /**
   * This constructor is used for splitting parent InsertRowsOfOneDevicePlan into sub ones. So
   * there's no need to validate rowPlans.
   */
  public InsertRowsOfOneDevicePlan(
      PartialPath prefixPath, InsertRowPlan[] rowPlans, int[] rowPlanIndexList) {
    this();
    this.devicePath = prefixPath;
    this.rowPlans = rowPlans;
    this.rowPlanIndexList = rowPlanIndexList;
  }

  @Override
  public void checkIntegrity() {}

  // TODO I see InsertRowPlan rewrites the hashCode, but do we need to rewrite hashCode?

  @Override
  public List<PartialPath> getPaths() {
    if (paths != null) {
      return paths;
    }
    Set<PartialPath> pathSet = new HashSet<>();
    for (InsertRowPlan plan : rowPlans) {
      pathSet.addAll(plan.getPaths());
    }
    paths = new ArrayList<>(pathSet);
    return paths;
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
  public Object getFirstValueOfIndex(int index) {
    throw new NotImplementedException();
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    int type = PhysicalPlanType.BATCH_INSERT_ONE_DEVICE.ordinal();
    stream.writeByte((byte) type);
    putString(stream, devicePath.getFullPath());

    stream.writeInt(rowPlans.length);
    for (InsertRowPlan plan : rowPlans) {
      stream.writeLong(plan.getTime());
      plan.serializeMeasurementsAndValues(stream);
    }
    for (Integer index : rowPlanIndexList) {
      stream.writeInt(index);
    }
  }

  @Override
  public void serializeImpl(ByteBuffer buffer) {
    int type = PhysicalPlanType.BATCH_INSERT_ONE_DEVICE.ordinal();
    buffer.put((byte) type);

    putString(buffer, devicePath.getFullPath());
    buffer.putInt(rowPlans.length);
    for (InsertRowPlan plan : rowPlans) {
      buffer.putLong(plan.getTime());
      plan.serializeMeasurementsAndValues(buffer);
    }
    for (Integer index : rowPlanIndexList) {
      buffer.putInt(index);
    }
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {
    this.devicePath = new PartialPath(readString(buffer));
    this.rowPlans = new InsertRowPlan[buffer.getInt()];
    for (int i = 0; i < rowPlans.length; i++) {
      rowPlans[i] = new InsertRowPlan();
      rowPlans[i].setDevicePath(devicePath);
      rowPlans[i].setTime(buffer.getLong());
      rowPlans[i].deserializeMeasurementsAndValues(buffer);
    }
    this.rowPlanIndexList = new int[rowPlans.length];
    for (int i = 0; i < rowPlans.length; i++) {
      rowPlanIndexList[i] = buffer.getInt();
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
    return "deviceId: " + devicePath + ", times: " + rowPlans.length;
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
  public Map<Integer, TSStatus> getResults() {
    return results;
  }

  @Override
  public List<PartialPath> getPrefixPaths() {
    return Collections.singletonList(this.devicePath);
  }

  @Override
  public int getBatchSize() {
    return rowPlans.length;
  }

  public int[] getRowPlanIndexList() {
    return rowPlanIndexList;
  }

  @Override
  public void unsetIsExecuted(int i) {
    if (isExecuted == null) {
      isExecuted = new boolean[getBatchSize()];
    }
    isExecuted[i] = false;
    if (rowPlanIndexList != null && rowPlanIndexList.length > 0) {
      results.remove(rowPlanIndexList[i]);
    } else {
      results.remove(i);
    }
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof InsertRowsOfOneDevicePlan
        && Arrays.equals(((InsertRowsOfOneDevicePlan) o).rowPlanIndexList, this.rowPlanIndexList)
        && Arrays.equals(((InsertRowsOfOneDevicePlan) o).rowPlans, this.rowPlans)
        && ((InsertRowsOfOneDevicePlan) o).results.equals(this.results)
        && ((InsertRowsOfOneDevicePlan) o).getDevicePath().equals(this.getDevicePath());
  }

  @Override
  public int hashCode() {
    int result = rowPlans != null ? Arrays.hashCode(rowPlans) : 0;
    result = 31 * result + (rowPlanIndexList != null ? Arrays.hashCode(rowPlanIndexList) : 0);
    result = 31 * result + (results != null ? results.hashCode() : 0);
    return result;
  }
}
