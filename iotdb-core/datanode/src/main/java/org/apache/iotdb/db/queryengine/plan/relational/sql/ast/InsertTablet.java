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

import org.apache.iotdb.commons.schema.view.LogicalViewSchema;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.schematree.IMeasurementSchemaInfo;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaComputationWithAutoCreation;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
  public List<ISchemaComputationWithAutoCreation> getSchemaValidationList() {
    InsertTabletStatement insertTabletStatement = getInnerTreeStatement();
    Map<IDeviceID, ISchemaComputationWithAutoCreation> map = new HashMap<>();
    for (int i = 0; i < insertTabletStatement.getRowCount(); i++) {
      map.computeIfAbsent(insertTabletStatement.getTableDeviceID(i), this::getSchemaComputation);
    }
    return new ArrayList<>(map.values());
  }

  @Override
  public void updateAfterSchemaValidation(MPPQueryContext context) throws QueryProcessException {
    getInnerTreeStatement().updateAfterSchemaValidation(context);
  }

  @Override
  public ISchemaComputationWithAutoCreation getSchemaComputation(IDeviceID deviceID) {
    return new SchemaExecutions(deviceID);
  }

  public class SchemaExecutions extends BasicSchemaExecutions {

    public SchemaExecutions(IDeviceID deviceID) {
      super(deviceID);
    }

    @Override
    public void computeMeasurement(int index, IMeasurementSchemaInfo measurementSchemaInfo) {
      getInnerTreeStatement().computeMeasurement(index, measurementSchemaInfo);
    }

    @Override
    public boolean hasLogicalViewNeedProcess() {
      return getInnerTreeStatement().hasLogicalViewNeedProcess();
    }

    @Override
    public List<LogicalViewSchema> getLogicalViewSchemaList() {
      return getInnerTreeStatement().getLogicalViewSchemaList();
    }

    @Override
    public List<Integer> getIndexListOfLogicalViewPaths() {
      return getInnerTreeStatement().getIndexListOfLogicalViewPaths();
    }

    @Override
    public void recordRangeOfLogicalViewSchemaListNow() {
      getInnerTreeStatement().recordRangeOfLogicalViewSchemaListNow();
    }

    @Override
    public Pair<Integer, Integer> getRangeOfLogicalViewSchemaListRecorded() {
      return getInnerTreeStatement().getRangeOfLogicalViewSchemaListRecorded();
    }

    @Override
    public void computeMeasurementOfView(
        int index, IMeasurementSchemaInfo measurementSchemaInfo, boolean isAligned) {
      getInnerTreeStatement().computeMeasurementOfView(index, measurementSchemaInfo, isAligned);
    }
  }
}
