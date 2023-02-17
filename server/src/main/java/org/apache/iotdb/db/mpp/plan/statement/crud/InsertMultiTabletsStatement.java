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
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.statement.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;
import org.apache.iotdb.db.utils.TimePartitionUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.List;

public class InsertMultiTabletsStatement extends InsertBaseStatement {

  /** the InsertTabletStatement list */
  List<InsertTabletStatement> insertTabletStatementList;

  public InsertMultiTabletsStatement() {
    super();
    statementType = StatementType.MULTI_BATCH_INSERT;
  }

  public List<InsertTabletStatement> getInsertTabletStatementList() {
    return insertTabletStatementList;
  }

  public void setInsertTabletStatementList(List<InsertTabletStatement> insertTabletStatementList) {
    this.insertTabletStatementList = insertTabletStatementList;
  }

  public List<PartialPath> getDevicePaths() {
    List<PartialPath> partialPaths = new ArrayList<>();
    for (InsertTabletStatement insertTabletStatement : insertTabletStatementList) {
      partialPaths.add(insertTabletStatement.devicePath);
    }
    return partialPaths;
  }

  public List<String[]> getMeasurementsList() {
    List<String[]> measurementsList = new ArrayList<>();
    for (InsertTabletStatement insertTabletStatement : insertTabletStatementList) {
      measurementsList.add(insertTabletStatement.measurements);
    }
    return measurementsList;
  }

  public List<TSDataType[]> getDataTypesList() {
    List<TSDataType[]> dataTypesList = new ArrayList<>();
    for (InsertTabletStatement insertTabletStatement : insertTabletStatementList) {
      dataTypesList.add(insertTabletStatement.dataTypes);
    }
    return dataTypesList;
  }

  public List<Boolean> getAlignedList() {
    List<Boolean> alignedList = new ArrayList<>();
    for (InsertTabletStatement insertTabletStatement : insertTabletStatementList) {
      alignedList.add(insertTabletStatement.isAligned);
    }
    return alignedList;
  }

  @Override
  public boolean isEmpty() {
    return insertTabletStatementList.isEmpty();
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitInsertMultiTablets(this, context);
  }

  @Override
  public List<PartialPath> getPaths() {
    List<PartialPath> result = new ArrayList<>();
    for (InsertTabletStatement insertTabletStatement : insertTabletStatementList) {
      result.addAll(insertTabletStatement.getPaths());
    }
    return result;
  }

  @Override
  public List<TEndPoint> collectRedirectInfo(DataPartition dataPartition) {
    List<TEndPoint> result = new ArrayList<>();
    for (InsertTabletStatement insertTabletStatement : insertTabletStatementList) {
      TRegionReplicaSet regionReplicaSet =
          dataPartition.getDataRegionReplicaSetForWriting(
              insertTabletStatement.devicePath.getFullPath(),
              TimePartitionUtils.getTimePartition(
                  insertTabletStatement.getTimes()[insertTabletStatement.getTimes().length - 1]));
      result.add(regionReplicaSet.getDataNodeLocations().get(0).getClientRpcEndPoint());
    }
    return result;
  }
}
