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
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class InsertRow extends WrappedInsertStatement {

  public InsertRow(InsertRowStatement insertRowStatement, MPPQueryContext context) {
    super(insertRowStatement, context);
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitInsertRow(this, context);
  }

  @Override
  public InsertRowStatement getInnerTreeStatement() {
    return ((InsertRowStatement) super.getInnerTreeStatement());
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
    final InsertRowStatement insertRowStatement = getInnerTreeStatement();
    Object[] segments = insertRowStatement.getTableDeviceID().getSegments();
    return Collections.singletonList(Arrays.copyOfRange(segments, 1, segments.length));
  }

  @Override
  public List<String> getAttributeColumnNameList() {
    final InsertRowStatement insertRowStatement = getInnerTreeStatement();
    return insertRowStatement.getAttributeColumnNameList();
  }

  @Override
  public List<Object[]> getAttributeValueList() {
    final InsertRowStatement insertRowStatement = getInnerTreeStatement();
    final List<Integer> attrColumnIndices = insertRowStatement.getAttrColumnIndices();
    Object[] attrValues = new Object[attrColumnIndices.size()];
    for (int j = 0; j < attrColumnIndices.size(); j++) {
      final int columnIndex = attrColumnIndices.get(j);
      attrValues[j] = insertRowStatement.getValues()[columnIndex];
    }
    return Collections.singletonList(attrValues);
  }
}
