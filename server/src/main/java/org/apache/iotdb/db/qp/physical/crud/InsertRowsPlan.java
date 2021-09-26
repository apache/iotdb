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
import org.apache.iotdb.db.utils.StatusUtils;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class InsertRowsPlan extends InsertPlan implements BatchPlan {

  /**
   * Suppose there is an InsertRowsPlan, which contains 5 InsertRowPlans,
   * insertRowPlanList={InsertRowPlan_0, InsertRowPlan_1, InsertRowPlan_2, InsertRowPlan_3,
   * InsertRowPlan_4}, then the insertRowPlanIndexList={0, 1, 2, 3, 4} respectively. But when the
   * InsertRowsPlan is split into two InsertRowsPlans according to different storage group in
   * cluster version, suppose that the InsertRowsPlan_1's insertRowPlanList = {InsertRowPlan_0,
   * InsertRowPlan_3, InsertRowPlan_4}, then InsertRowsPlan_1's insertRowPlanIndexList = {0, 3, 4};
   * InsertRowsPlan_2's insertRowPlanList = {InsertRowPlan_1, * InsertRowPlan_2} then
   * InsertRowsPlan_2's insertRowPlanIndexList= {1, 2} respectively;
   */
  private List<Integer> insertRowPlanIndexList;

  /** the InsertRowsPlan list */
  private List<InsertRowPlan> insertRowPlanList;

  boolean[] isExecuted;

  /** record the result of insert rows */
  private Map<Integer, TSStatus> results = new HashMap<>();

  private List<PartialPath> paths;
  private List<PartialPath> prefixPaths;

  public InsertRowsPlan() {
    super(OperatorType.BATCH_INSERT_ROWS);
    insertRowPlanList = new ArrayList<>();
    insertRowPlanIndexList = new ArrayList<>();
  }

  @Override
  public long getMinTime() {
    long minTime = Long.MAX_VALUE;
    for (InsertRowPlan insertRowPlan : insertRowPlanList) {
      if (insertRowPlan.getMinTime() < minTime) {
        minTime = insertRowPlan.getMinTime();
      }
    }
    return minTime;
  }

  @Override
  public List<PartialPath> getPaths() {
    if (paths != null) {
      return paths;
    }
    Set<PartialPath> pathSet = new HashSet<>();
    for (InsertRowPlan plan : insertRowPlanList) {
      pathSet.addAll(plan.getPaths());
    }
    paths = new ArrayList<>(pathSet);
    return paths;
  }

  @Override
  public List<PartialPath> getPrefixPaths() {
    if (prefixPaths != null) {
      return prefixPaths;
    }
    prefixPaths = new ArrayList<>(insertRowPlanList.size());
    for (InsertRowPlan insertRowPlan : insertRowPlanList) {
      prefixPaths.add(insertRowPlan.getPrefixPath());
    }
    return prefixPaths;
  }

  @Override
  public void checkIntegrity() throws QueryProcessException {
    if (insertRowPlanList.isEmpty()) {
      throw new QueryProcessException("sub plan are empty.");
    }
    for (InsertRowPlan insertRowPlan : insertRowPlanList) {
      insertRowPlan.checkIntegrity();
    }
  }

  @Override
  public void recoverFromFailure() {
    for (InsertRowPlan insertRowPlan : insertRowPlanList) {
      insertRowPlan.recoverFromFailure();
    }
  }

  @Override
  public InsertPlan getPlanFromFailed() {
    if (super.getPlanFromFailed() == null) {
      return null;
    }
    List<InsertRowPlan> plans = new ArrayList<>();
    List<Integer> indexes = new ArrayList<>();
    for (int i = 0; i < insertRowPlanList.size(); i++) {
      if (insertRowPlanList.get(i).hasFailedValues()) {
        plans.add((InsertRowPlan) insertRowPlanList.get(i).getPlanFromFailed());
        indexes.add(i);
      }
    }
    this.insertRowPlanList = plans;
    this.insertRowPlanIndexList = indexes;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    InsertRowsPlan that = (InsertRowsPlan) o;

    if (!Objects.equals(insertRowPlanIndexList, that.insertRowPlanIndexList)) {
      return false;
    }
    if (!Objects.equals(insertRowPlanList, that.insertRowPlanList)) {
      return false;
    }
    return Objects.equals(results, that.results);
  }

  @Override
  public int hashCode() {
    int result = insertRowPlanIndexList != null ? insertRowPlanIndexList.hashCode() : 0;
    result = 31 * result + (insertRowPlanList != null ? insertRowPlanList.hashCode() : 0);
    result = 31 * result + (results != null ? results.hashCode() : 0);
    return result;
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    int type = PhysicalPlanType.BATCH_INSERT_ROWS.ordinal();
    buffer.put((byte) type);
    buffer.putInt(insertRowPlanList.size());
    for (InsertRowPlan insertRowPlan : insertRowPlanList) {
      insertRowPlan.subSerialize(buffer);
    }
    for (Integer index : insertRowPlanIndexList) {
      buffer.putInt(index);
    }
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    int type = PhysicalPlanType.BATCH_INSERT_ROWS.ordinal();
    stream.writeByte((byte) type);
    stream.writeInt(insertRowPlanList.size());
    for (InsertRowPlan insertRowPlan : insertRowPlanList) {
      insertRowPlan.subSerialize(stream);
    }
    for (Integer index : insertRowPlanIndexList) {
      stream.writeInt(index);
    }
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {
    int size = buffer.getInt();
    this.insertRowPlanList = new ArrayList<>(size);
    this.insertRowPlanIndexList = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      InsertRowPlan insertRowPlan = new InsertRowPlan();
      insertRowPlan.deserialize(buffer);
      insertRowPlanList.add(insertRowPlan);
    }

    for (int i = 0; i < size; i++) {
      insertRowPlanIndexList.add(buffer.getInt());
    }
  }

  @Override
  public void setIndex(long index) {
    super.setIndex(index);
    for (InsertRowPlan insertRowPlan : insertRowPlanList) {
      // use the InsertRowsPlan's index as the sub InsertRowPlan's index
      insertRowPlan.setIndex(index);
    }
  }

  public Map<Integer, TSStatus> getResults() {
    return results;
  }

  public void addOneInsertRowPlan(InsertRowPlan plan, int index) {
    insertRowPlanList.add(plan);
    insertRowPlanIndexList.add(index);
  }

  public List<Integer> getInsertRowPlanIndexList() {
    return insertRowPlanIndexList;
  }

  public List<InsertRowPlan> getInsertRowPlanList() {
    return insertRowPlanList;
  }

  public int getRowCount() {
    return insertRowPlanList.size();
  }

  public PartialPath getFirstDeviceId() {
    return insertRowPlanList.get(0).getPrefixPath();
  }

  @Override
  public String toString() {
    return "InsertRowsPlan{"
        + " insertRowPlanIndexList's size="
        + insertRowPlanIndexList.size()
        + ", insertRowPlanList's size="
        + insertRowPlanList.size()
        + ", results="
        + results
        + "}";
  }

  public TSStatus[] getFailingStatus() {
    return StatusUtils.getFailingStatus(results, insertRowPlanList.size());
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
    return insertRowPlanList.size();
  }

  @Override
  public void unsetIsExecuted(int i) {
    if (isExecuted == null) {
      isExecuted = new boolean[getBatchSize()];
    }
    isExecuted[i] = false;
    if (insertRowPlanIndexList != null && !insertRowPlanIndexList.isEmpty()) {
      results.remove(insertRowPlanIndexList.get(i));
    } else {
      results.remove(i);
    }
  }
}
