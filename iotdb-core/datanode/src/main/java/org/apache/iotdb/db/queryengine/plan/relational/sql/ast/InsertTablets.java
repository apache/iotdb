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

import org.apache.iotdb.calc.exception.QueryProcessException;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.IAstVisitor;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeUtils;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;

import java.util.List;

public class InsertTablets extends WrappedInsertStatement {

  public InsertTablets(
      InsertMultiTabletsStatement insertMultiTabletsStatement, MPPQueryContext context) {
    super(insertMultiTabletsStatement, context);
  }

  @Override
  public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
    return ((AstVisitor<R, C>) visitor).visitInsertTablets(this, context);
  }

  @Override
  public InsertMultiTabletsStatement getInnerTreeStatement() {
    return ((InsertMultiTabletsStatement) super.getInnerTreeStatement());
  }

  @Override
  public void updateAfterSchemaValidation(MPPQueryContext context) throws QueryProcessException {
    getInnerTreeStatement().updateAfterSchemaValidation(context);
  }

  @Override
  public String getTableName() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Object[]> getDeviceIdList() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> getAttributeColumnNameList() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Object[]> getAttributeValueList() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void validateTableSchema(Metadata metadata, MPPQueryContext context) {
    for (InsertTabletStatement insertTabletStatement :
        getInnerTreeStatement().getInsertTabletStatementList()) {
      final String database = AnalyzeUtils.getDatabaseName(insertTabletStatement, context);
      super.validateTableSchema(metadata, context, insertTabletStatement, database, true);
    }
  }

  @Override
  public void validateDeviceSchema(Metadata metadata, MPPQueryContext context) {
    final DeviceSchemaValidationAggregator validationAggregator =
        new DeviceSchemaValidationAggregator();
    for (InsertTabletStatement insertTabletStatement :
        getInnerTreeStatement().getInsertTabletStatementList()) {
      validationAggregator.add(new InsertTablet(insertTabletStatement, context));
    }
    validationAggregator.forEach(validation -> metadata.validateDeviceSchema(validation, context));
  }
}
