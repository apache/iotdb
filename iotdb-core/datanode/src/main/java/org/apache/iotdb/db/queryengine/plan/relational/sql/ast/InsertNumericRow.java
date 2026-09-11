/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.IAstVisitor;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertNumericRowStatement;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Relational analysis entry for a primitive numeric row. */
public class InsertNumericRow extends WrappedInsertStatement {
  public InsertNumericRow(InsertNumericRowStatement statement, MPPQueryContext context) {
    super(statement, context);
  }

  @Override
  public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
    return ((AstVisitor<R, C>) visitor).visitInsertNumericRow(this, context);
  }

  @Override
  public InsertNumericRowStatement getInnerTreeStatement() {
    return (InsertNumericRowStatement) super.getInnerTreeStatement();
  }

  @Override
  public void updateAfterSchemaValidation(MPPQueryContext context) {
    // Numeric rows are already typed; schema validation casts values in their primitive storage.
  }

  @Override
  public String getTableName() {
    return getInnerTreeStatement().getTableName();
  }

  @Override
  public List<Object[]> getDeviceIdList() {
    Object[] segments = getInnerTreeStatement().getTableDeviceID().getSegments();
    return Collections.singletonList(Arrays.copyOfRange(segments, 1, segments.length));
  }

  @Override
  public List<String> getAttributeColumnNameList() {
    return Collections.emptyList();
  }

  @Override
  public List<Object[]> getAttributeValueList() {
    return Collections.singletonList(new Object[0]);
  }
}
