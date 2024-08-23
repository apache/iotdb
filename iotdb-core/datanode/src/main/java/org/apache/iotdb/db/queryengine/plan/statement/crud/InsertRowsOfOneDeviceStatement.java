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

package org.apache.iotdb.db.queryengine.plan.statement.crud;

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaValidation;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.utils.annotations.TableModel;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.NotImplementedException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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
      timePartitionSlotSet.add(
          TimePartitionUtils.getTimePartitionSlot(insertRowStatement.getTime()));
    }
    return new ArrayList<>(timePartitionSlotSet);
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitInsertRowsOfOneDevice(this, context);
  }

  @Override
  public List<PartialPath> getPaths() {
    List<PartialPath> ret = new ArrayList<>();
    for (String m : measurements) {
      PartialPath fullPath = devicePath.concatAsMeasurementPath(m);
      ret.add(fullPath);
    }
    return ret;
  }

  @Override
  public ISchemaValidation getSchemaValidation() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<ISchemaValidation> getSchemaValidationList() {
    return insertRowStatementList.stream()
        .map(InsertRowStatement::getSchemaValidation)
        .collect(Collectors.toList());
  }

  @Override
  public void updateAfterSchemaValidation(MPPQueryContext context) throws QueryProcessException {
    for (InsertRowStatement insertRowStatement : insertRowStatementList) {
      insertRowStatement.updateAfterSchemaValidation(context);
      if (!this.hasFailedMeasurements() && insertRowStatement.hasFailedMeasurements()) {
        this.failedMeasurementIndex2Info = insertRowStatement.failedMeasurementIndex2Info;
      }
    }
  }

  @Override
  protected boolean checkAndCastDataType(int columnIndex, TSDataType dataType) {
    return false;
  }

  @Override
  public void semanticCheck() {
    for (InsertRowStatement insertRowStatement : insertRowStatementList) {
      insertRowStatement.semanticCheck();
    }
  }

  @Override
  public long getMinTime() {
    throw new NotImplementedException();
  }

  @Override
  public Object getFirstValueOfIndex(int index) {
    throw new NotImplementedException();
  }

  @Override
  public InsertBaseStatement removeLogicalView() {
    boolean needSplit = false;
    for (InsertRowStatement child : this.insertRowStatementList) {
      if (child.isNeedSplit()) {
        needSplit = true;
      }
    }
    if (needSplit) {
      List<InsertRowStatement> mergedList = new ArrayList<>();
      for (InsertRowStatement child : this.insertRowStatementList) {
        List<InsertRowStatement> childSplitResult = child.getSplitList();
        mergedList.addAll(childSplitResult);
      }
      InsertRowsStatement splitResult = new InsertRowsStatement();
      splitResult.setInsertRowStatementList(mergedList);
      return splitResult;
    }
    return this;
  }

  @TableModel
  @Override
  public void toLowerCase() {
    insertRowStatementList.forEach(InsertRowStatement::toLowerCase);
  }

  @Override
  @TableModel
  public Optional<String> getDatabaseName() {
    Optional<String> database = Optional.empty();
    for (InsertRowStatement rowStatement : insertRowStatementList) {
      Optional<String> childDatabaseName = rowStatement.getDatabaseName();
      if (childDatabaseName.isPresent()
          && database.isPresent()
          && !Objects.equals(childDatabaseName.get(), database.get())) {
        throw new SemanticException(
            "Cannot insert into multiple databases within one statement, please split them manually");
      }

      database = childDatabaseName;
    }
    return database;
  }
}
