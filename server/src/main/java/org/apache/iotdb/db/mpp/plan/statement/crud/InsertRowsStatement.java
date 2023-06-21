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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.metadata.DuplicateInsertException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.mpp.plan.analyze.schema.ISchemaValidation;
import org.apache.iotdb.db.mpp.plan.statement.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
  public void updateAfterSchemaValidation() throws QueryProcessException {
    for (InsertRowStatement insertRowStatement : insertRowStatementList) {
      insertRowStatement.updateAfterSchemaValidation();
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
  public long getMinTime() {
    throw new NotImplementedException();
  }

  @Override
  public Object getFirstValueOfIndex(int index) {
    throw new NotImplementedException();
  }

  @Override
  public InsertBaseStatement removeLogicalView() {
    List<InsertRowStatement> mergedList = new ArrayList<>();
    boolean needSplit = false;
    for (InsertRowStatement child : this.insertRowStatementList) {
      List<InsertRowStatement> childSplitResult = child.getSplitList();
      needSplit = needSplit || child.isNeedSplit();
      mergedList.addAll(childSplitResult);
    }
    if (!needSplit) {
      return this;
    }
    validateInsertRowList(mergedList);
    InsertRowsStatement splitResult = new InsertRowsStatement();
    splitResult.setInsertRowStatementList(mergedList);
    return splitResult;
  }

  /**
   * Check given InsertRowStatement list, make sure no duplicate time series in those statements. If
   * there are duplicate measurements, throw DuplicateInsertException.
   */
  public static void validateInsertRowList(List<InsertRowStatement> insertRowList) {
    if (insertRowList == null) {
      return;
    }
    Map<String, Set<String>> mapFromDeviceToMeasurements = new HashMap<>();
    for (InsertRowStatement insertRow : insertRowList) {
      String device = insertRow.devicePath.getFullPath();
      Set<String> measurementSet = mapFromDeviceToMeasurements.get(device);
      if (measurementSet == null) {
        measurementSet = new HashSet<>();
      }
      for (String measurement : insertRow.measurements) {
        boolean notExist = measurementSet.add(measurement);
        if (!notExist) {
          throw new SemanticException(new DuplicateInsertException(device, measurement));
        }
      }
      mapFromDeviceToMeasurements.put(device, measurementSet);
    }
  }
}
