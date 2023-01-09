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

public class InsertRowsStatement extends InsertBaseStatement {

  /** the InsertRowsStatement list */
  private List<InsertRowStatement> insertRowStatementList;

  public InsertRowsStatement() {
    super();
    statementType = StatementType.BATCH_INSERT_ROWS;
  }

  public List<PartialPath> getDevicePaths() {
    List<PartialPath> partialPaths = new ArrayList<>();
    for (InsertRowStatement insertRowStatement : insertRowStatementList) {
      partialPaths.add(insertRowStatement.devicePath);
    }
    return partialPaths;
  }

  public List<String[]> getMeasurementsList() {
    List<String[]> measurementsList = new ArrayList<>();
    for (InsertRowStatement insertRowStatement : insertRowStatementList) {
      measurementsList.add(insertRowStatement.measurements);
    }
    return measurementsList;
  }

  public List<TSDataType[]> getDataTypesList() {
    List<TSDataType[]> dataTypesList = new ArrayList<>();
    for (InsertRowStatement insertRowStatement : insertRowStatementList) {
      dataTypesList.add(insertRowStatement.dataTypes);
    }
    return dataTypesList;
  }

  public List<Boolean> getAlignedList() {
    List<Boolean> alignedList = new ArrayList<>();
    for (InsertRowStatement insertRowStatement : insertRowStatementList) {
      alignedList.add(insertRowStatement.isAligned);
    }
    return alignedList;
  }

  public List<InsertRowStatement> getInsertRowStatementList() {
    return insertRowStatementList;
  }

  public void setInsertRowStatementList(List<InsertRowStatement> insertRowStatementList) {
    this.insertRowStatementList = insertRowStatementList;
  }

  @Override
  public boolean isEmpty() {
    return insertRowStatementList.isEmpty();
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitInsertRows(this, context);
  }

  @Override
  public List<PartialPath> getPaths() {
    List<PartialPath> result = new ArrayList<>();
    for (InsertRowStatement insertRowStatement : insertRowStatementList) {
      result.addAll(insertRowStatement.getPaths());
    }
    return result;
  }

  @Override
  public List<TEndPoint> collectRedirectInfo(DataPartition dataPartition) {
    List<TEndPoint> result = new ArrayList<>();
    for (InsertRowStatement insertRowStatement : insertRowStatementList) {
      TRegionReplicaSet regionReplicaSet =
          dataPartition.getDataRegionReplicaSetForWriting(
              insertRowStatement.devicePath.getFullPath(),
              TimePartitionUtils.getTimePartition(insertRowStatement.getTime()));
      result.add(regionReplicaSet.getDataNodeLocations().get(0).getClientRpcEndPoint());
    }
    return result;
  }
}
