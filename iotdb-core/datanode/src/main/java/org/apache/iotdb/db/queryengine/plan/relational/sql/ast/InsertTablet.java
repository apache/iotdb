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

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;

import org.apache.tsfile.file.metadata.IDeviceID;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class InsertTablet extends WrappedInsertStatement {

  public InsertTablet(InsertTabletStatement insertTabletStatement, MPPQueryContext context) {
    super(insertTabletStatement, context);
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitInsertTablet(this, context);
  }

  @Override
  public InsertTabletStatement getInnerTreeStatement() {
    return ((InsertTabletStatement) super.getInnerTreeStatement());
  }

  @Override
  public void updateAfterSchemaValidation(MPPQueryContext context) throws QueryProcessException {
    getInnerTreeStatement().updateAfterSchemaValidation(context);
  }

  @Override
  public String getTableName() {
    return getInnerTreeStatement().getDevicePath().getFullPath();
  }

  @Override
  public List<Object[]> getDeviceIdList() {
    List<Object[]> deviceIdList = new ArrayList<>();
    final InsertTabletStatement insertTabletStatement = getInnerTreeStatement();
    for (int i = 0; i < insertTabletStatement.getRowCount(); i++) {
      IDeviceID deviceID = insertTabletStatement.getTableDeviceID(i);
      Object[] segments = deviceID.getSegments();
      deviceIdList.add(Arrays.copyOfRange(segments, 1, segments.length));
    }
    return deviceIdList;
  }

  @Override
  public List<String> getAttributeColumnNameList() {
    final InsertTabletStatement insertTabletStatement = getInnerTreeStatement();
    return insertTabletStatement.getAttributeColumnNameList();
  }

  @Override
  public List<Object[]> getAttributeValueList() {
    final InsertTabletStatement insertTabletStatement = getInnerTreeStatement();
    List<Object[]> result = new ArrayList<>(insertTabletStatement.getRowCount());
    final List<Integer> attrColumnIndices = insertTabletStatement.getAttrColumnIndices();
    for (int i = 0; i < insertTabletStatement.getRowCount(); i++) {
      Object[] attrValues = new Object[attrColumnIndices.size()];
      for (int j = 0; j < attrColumnIndices.size(); j++) {
        final int columnIndex = attrColumnIndices.get(j);
        attrValues[j] = ((Object[]) insertTabletStatement.getColumns()[columnIndex])[i];
      }
      result.add(attrValues);
    }
    return result;
  }
}
