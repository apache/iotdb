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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaValidation;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

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

  public List<String[]> getMeasurementsList() {
    List<String[]> measurementsList = new ArrayList<>();
    for (InsertTabletStatement insertTabletStatement : insertTabletStatementList) {
      measurementsList.add(insertTabletStatement.measurements);
    }
    return measurementsList;
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
  public ISchemaValidation getSchemaValidation() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<ISchemaValidation> getSchemaValidationList() {
    return insertTabletStatementList.stream()
        .map(InsertTabletStatement::getSchemaValidation)
        .collect(Collectors.toList());
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
    List<InsertTabletStatement> mergedList = new ArrayList<>();
    boolean needSplit = false;
    for (InsertTabletStatement child : this.insertTabletStatementList) {
      List<InsertTabletStatement> childSplitResult = child.getSplitList();
      needSplit = needSplit || child.isNeedSplit();
      mergedList.addAll(childSplitResult);
    }
    if (!needSplit) {
      return this;
    }
    InsertMultiTabletsStatement splitResult = new InsertMultiTabletsStatement();
    splitResult.setInsertTabletStatementList(mergedList);
    return splitResult;
  }
}
