/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.qp.physical.crud;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

/**
 * Mainly used in the distributed version, when multiple InsertTabletPlans belong to a raft
 * replication group, we merge these InsertTabletPlans into one InsertMultiTabletPlan, which can
 * reduce the number of raft logs. For details, please refer to https://issues.apache.org/jira/browse/IOTDB-1099
 */
public class InsertMultiTabletPlan extends InsertPlan {

  /**
   * the value is used to indict the parent InsertTabletPlan's index when the parent
   * InsertTabletPlan is split to multi sub InsertTabletPlans. if the InsertTabletPlan have no
   * parent plan, the value is -1;
   * <p>
   * suppose we original have 3 InsertTabletPlans contains 1 InsertMultiTabletPlan, then the initial
   * InsertMultiTabletPlan would have the following two attributes:
   * <p>
   * insertTabletPlanList={InsertTabletPlan_1,InsertTabletPlan_2,InsertTabletPlan_3}
   * <p>
   * parentInsetTablePlanIndexList={-1,-1,-1} both have three values.
   *
   * <p>
   * if the InsertTabletPlan_1 is split into two sub InsertTabletPlan, InsertTabletPlan_2 is split
   * into three sub InsertTabletPlan, InsertTabletPlan_3 is split into four sub InsertTabletPlan.
   * <p>
   * InsertTabletPlan_1={InsertTabletPlan_1_subPlan1, InsertTabletPlan_1_subPlan2}
   *
   * <p>
   * InsertTabletPlan_2={InsertTabletPlan_2_subPlan1, InsertTabletPlan_2_subPlan2,
   * InsertTabletPlan_2_subPlan3}
   *
   * <p>
   * InsertTabletPlan_3={InsertTabletPlan_3_subPlan1, InsertTabletPlan_3_subPlan2,
   * InsertTabletPlan_3_subPlan3, InsertTabletPlan_3_subPlan4}
   * <p>
   * those sub plans belongs to two different raft data group, so will generate two new
   * InsertMultiTabletPlan
   * <p>
   * InsertMultiTabletPlant1.insertTabletPlanList={InsertTabletPlan_1_subPlan1,
   * InsertTabletPlan_3_subPlan1, InsertTabletPlan_3_subPlan3, InsertTabletPlan_3_subPlan4}
   * <p>
   * InsertMultiTabletPlant1.parentInsetTablePlanIndexList={0,2,2,2}
   *
   * <p>
   * InsertMultiTabletPlant2.insertTabletPlanList={InsertTabletPlan_1_subPlan2,
   * InsertTabletPlan_2_subPlan1, InsertTabletPlan_2_subPlan2, InsertTabletPlan_2_subPlan3,
   * InsertTabletPlan_3_subPlan2}
   * <p>
   * InsertMultiTabletPlant2.parentInsetTablePlanIndexList={0,1,1,1,2}
   *
   * <p>
   * this is usually used to back-propagate exceptions to the parent plan without losing their
   * proper positions.
   */
  List<Integer> parentInsetTablePlanIndexList;

  /**
   * the InsertTabletPlan list
   */
  List<InsertTabletPlan> insertTabletPlanList;

  /**
   * record the result of creation of time series
   */
  private Map<Integer, TSStatus> results = new TreeMap<>();


  public InsertMultiTabletPlan() {
    super(OperatorType.MULTI_BATCH_INSERT);
    this.insertTabletPlanList = new ArrayList<>();
    this.parentInsetTablePlanIndexList = new ArrayList<>();
  }

  public InsertMultiTabletPlan(List<InsertTabletPlan> insertTabletPlanList) {
    super(OperatorType.MULTI_BATCH_INSERT);
    this.insertTabletPlanList = insertTabletPlanList;
    this.parentInsetTablePlanIndexList = new ArrayList<>();
  }

  public InsertMultiTabletPlan(List<InsertTabletPlan> insertTabletPlanList,
      List<Integer> parentInsetTablePlanIndexList) {
    super(OperatorType.MULTI_BATCH_INSERT);
    this.insertTabletPlanList = insertTabletPlanList;
    this.parentInsetTablePlanIndexList = parentInsetTablePlanIndexList;
  }

  public void addInsertTabletPlan(InsertTabletPlan plan, Integer parentIndex) {
    insertTabletPlanList.add(plan);
    parentInsetTablePlanIndexList.add(parentIndex);
  }

  public List<InsertTabletPlan> getInsertTabletPlanList() {
    return insertTabletPlanList;
  }

  public List<Integer> getParentInsetTablePlanIndexList() {
    return parentInsetTablePlanIndexList;
  }


  @Override
  public List<PartialPath> getPaths() {
    List<PartialPath> result = new ArrayList<>();
    for (InsertTabletPlan insertTabletPlan : insertTabletPlanList) {
      result.addAll(insertTabletPlan.getPaths());
    }
    return result;
  }

  @Override
  public long getMinTime() {
    long minTime = Long.MAX_VALUE;
    for (InsertTabletPlan insertTabletPlan : insertTabletPlanList) {
      if (minTime > insertTabletPlan.getMinTime()) {
        minTime = insertTabletPlan.getMinTime();
      }
    }
    return minTime;
  }

  public long getMaxTime() {
    long maxTime = Long.MIN_VALUE;
    for (InsertTabletPlan insertTabletPlan : insertTabletPlanList) {
      if (maxTime < insertTabletPlan.getMinTime()) {
        maxTime = insertTabletPlan.getMinTime();
      }
    }
    return maxTime;
  }

  public int getTabletsSize() {
    return insertTabletPlanList.size();
  }

  public Map<Integer, TSStatus> getResults() {
    return results;
  }

  /**
   * @return the total row of the whole InsertTabletPlan
   */
  public int getTotalRowCount() {
    int rowCount = 0;
    for (InsertTabletPlan insertTabletPlan : insertTabletPlanList) {
      rowCount += insertTabletPlan.getRowCount();
    }
    return rowCount;
  }

  /**
   * Gets the number of rows in the InsertTabletPlan of the index
   *
   * @param index the index of the insertTabletPlanList
   * @return the total row count of the insertTabletPlanList.get(i)
   */
  public int getRowCount(int index) {
    if (index >= insertTabletPlanList.size() || index < 0) {
      return 0;
    }
    return insertTabletPlanList.get(index).getRowCount();
  }

  public PartialPath getFirstDeviceId() {
    return insertTabletPlanList.get(0).getDeviceId();
  }

  public InsertTabletPlan getInsertTabletPlan(int index) {
    if (index >= insertTabletPlanList.size() || index < 0) {
      return null;
    }
    return insertTabletPlanList.get(index);
  }

  /**
   * @param index the index of the sub plan in this InsertMultiTabletPlan
   * @return the parent's index in the parent InsertMultiTabletPlan
   */
  public int getParentIndex(int index) {
    if (index >= parentInsetTablePlanIndexList.size() || index < 0) {
      return -1;
    }
    return parentInsetTablePlanIndexList.get(index);
  }

  @Override
  public void checkIntegrity() throws QueryProcessException {
    for (InsertTabletPlan insertTabletPlan : insertTabletPlanList) {
      insertTabletPlan.checkIntegrity();
    }
  }

  public void setParentInsetTablePlanIndexList(List<Integer> parentInsetTablePlanIndexList) {
    this.parentInsetTablePlanIndexList = parentInsetTablePlanIndexList;
  }

  public void setInsertTabletPlanList(List<InsertTabletPlan> insertTabletPlanList) {
    this.insertTabletPlanList = insertTabletPlanList;
  }

  public void setResults(
      Map<Integer, TSStatus> results) {
    this.results = results;
  }

  @Override
  public void recoverFromFailure() {
    for (InsertTabletPlan insertTabletPlan : insertTabletPlanList) {
      insertTabletPlan.recoverFromFailure();
    }
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    int type = PhysicalPlanType.MULTI_BATCH_INSERT.ordinal();
    buffer.put((byte) type);
    buffer.putInt(insertTabletPlanList.size());
    for (InsertTabletPlan insertTabletPlan : insertTabletPlanList) {
      insertTabletPlan.subSerialize(buffer);
    }

    buffer.putInt(parentInsetTablePlanIndexList.size());
    for (Integer index : parentInsetTablePlanIndexList) {
      buffer.putInt(index);
    }
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    int type = PhysicalPlanType.MULTI_BATCH_INSERT.ordinal();
    stream.writeByte((byte) type);
    stream.writeInt(insertTabletPlanList.size());
    for (InsertTabletPlan insertTabletPlan : insertTabletPlanList) {
      insertTabletPlan.subSerialize(stream);
    }

    stream.writeInt(parentInsetTablePlanIndexList.size());
    for (Integer index : parentInsetTablePlanIndexList) {
      stream.writeInt(index);
    }
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {
    int tmpInsertTabletPlanListSize = buffer.getInt();
    this.insertTabletPlanList = new ArrayList<>(tmpInsertTabletPlanListSize);
    for (int i = 0; i < tmpInsertTabletPlanListSize; i++) {
      InsertTabletPlan tmpPlan = new InsertTabletPlan();
      tmpPlan.deserialize(buffer);
      this.insertTabletPlanList.add(tmpPlan);
    }

    int tmpParentInsetTablePlanIndexListSize = buffer.getInt();
    this.parentInsetTablePlanIndexList = new ArrayList<>(tmpParentInsetTablePlanIndexListSize);
    for (int i = 0; i < tmpParentInsetTablePlanIndexListSize; i++) {
      this.parentInsetTablePlanIndexList.add(buffer.getInt());
    }
  }

  @Override
  public String toString() {
    return "InsertMultiTabletPlan{"
        + " insertTabletPlanList=" + insertTabletPlanList
        + ", parentInsetTablePlanIndexList=" + parentInsetTablePlanIndexList
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    InsertMultiTabletPlan that = (InsertMultiTabletPlan) o;

    if (!Objects.equals(insertTabletPlanList, that.insertTabletPlanList)) {
      return false;
    }
    return Objects.equals(parentInsetTablePlanIndexList, that.parentInsetTablePlanIndexList);
  }

  @Override
  public int hashCode() {
    int result = insertTabletPlanList != null ? insertTabletPlanList.hashCode() : 0;
    result = 31 * result + (parentInsetTablePlanIndexList != null ? parentInsetTablePlanIndexList
        .hashCode() : 0);
    return result;
  }
}

