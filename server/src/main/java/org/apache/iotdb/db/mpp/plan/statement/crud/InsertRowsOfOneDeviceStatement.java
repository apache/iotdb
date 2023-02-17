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

package org.apache.iotdb.db.mpp.plan.statement.crud;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.statement.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;
import org.apache.iotdb.db.utils.TimePartitionUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class InsertRowsOfOneDeviceStatement extends InsertBaseStatement {

  public InsertRowsOfOneDeviceStatement() {
    super();
    statementType = StatementType.BATCH_INSERT_ONE_DEVICE;
  }

  /** the InsertRowsStatement list */
  private List<InsertRowStatement> insertRowStatementList;

  @Override
  public boolean isEmpty() {
    return insertRowStatementList.isEmpty();
  }

  public List<InsertRowStatement> getInsertRowStatementList() {
    return insertRowStatementList;
  }

  public void setInsertRowStatementList(List<InsertRowStatement> insertRowStatementList) {
    this.insertRowStatementList = insertRowStatementList;

    // set device path, measurements, and data types
    if (insertRowStatementList == null || insertRowStatementList.isEmpty()) {
      return;
    }
    devicePath = insertRowStatementList.get(0).getDevicePath();
    isAligned = insertRowStatementList.get(0).isAligned;
    Set<String> measurementSet = new HashSet<>();
    List<String> measurementList = new ArrayList<>();
    for (InsertRowStatement insertRowStatement : insertRowStatementList) {
      String[] measurements = insertRowStatement.getMeasurements();
      for (String measurement : measurements) {
        if (!measurementSet.contains(measurement)) {
          measurementList.add(measurement);
          measurementSet.add(measurement);
        }
      }
    }
    measurements = measurementList.toArray(new String[0]);
  }

  public List<TTimePartitionSlot> getTimePartitionSlots() {
    Set<TTimePartitionSlot> timePartitionSlotSet = new HashSet<>();
    for (InsertRowStatement insertRowStatement : insertRowStatementList) {
      timePartitionSlotSet.add(TimePartitionUtils.getTimePartition(insertRowStatement.getTime()));
    }
    return new ArrayList<>(timePartitionSlotSet);
  }

  @Override
  public List<TEndPoint> collectRedirectInfo(DataPartition dataPartition) {
    return insertRowStatementList
        .get(insertRowStatementList.size() - 1)
        .collectRedirectInfo(dataPartition);
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitInsertRowsOfOneDevice(this, context);
  }

  @Override
  public List<PartialPath> getPaths() {
    List<PartialPath> ret = new ArrayList<>();
    for (String m : measurements) {
      PartialPath fullPath = devicePath.concatNode(m);
      ret.add(fullPath);
    }
    return ret;
  }
}
