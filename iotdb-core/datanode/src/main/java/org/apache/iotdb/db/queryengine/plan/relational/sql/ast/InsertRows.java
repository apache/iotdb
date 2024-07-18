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

import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class InsertRows extends WrappedInsertStatement {

  public InsertRows(InsertRowsStatement insertRowsStatement, MPPQueryContext context) {
    super(insertRowsStatement, context);
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitInsertRows(this, context);
  }

  @Override
  public InsertRowsStatement getInnerTreeStatement() {
    return ((InsertRowsStatement) super.getInnerTreeStatement());
  }

  @Override
  public void updateAfterSchemaValidation(MPPQueryContext context) throws QueryProcessException {
    getInnerTreeStatement().updateAfterSchemaValidation(context);
  }

  @Override
  public String getDatabase() {
    return context.getSession().getDatabaseName().orElse(null);
  }

  @Override
  public String getTableName() {
    return getInnerTreeStatement().getDevicePath().getFullPath();
  }

  @Override
  public List<Object[]> getDeviceIdList() {
    final InsertRowsStatement insertRowStatement = getInnerTreeStatement();
    return insertRowStatement.getDeviceIdList();
  }

  @Override
  public List<String> getAttributeColumnNameList() {
    final InsertRowsStatement insertRowStatement = getInnerTreeStatement();
    List<String> result = new ArrayList<>();
    for (int i = 0; i < insertRowStatement.getColumnCategories().length; i++) {
      if (insertRowStatement.getColumnCategories()[i] == TsTableColumnCategory.ATTRIBUTE) {
        result.add(insertRowStatement.getMeasurements()[i]);
      }
    }
    return result;
  }

  @Override
  public List<Object[]> getAttributeValueList() {
    final InsertRowsStatement insertRowStatement = getInnerTreeStatement();
    final List<Integer> attrColumnIndices = insertRowStatement.getAttrColumnIndices();

    return insertRowStatement.getInsertRowStatementList().stream()
        .map(
            s -> {
              Object[] attrValues = new Object[attrColumnIndices.size()];
              for (int j = 0; j < attrColumnIndices.size(); j++) {
                final int columnIndex = attrColumnIndices.get(j);
                attrValues[j] = s.getValues()[columnIndex];
              }
              return attrValues;
            })
        .collect(Collectors.toList());
  }
}
