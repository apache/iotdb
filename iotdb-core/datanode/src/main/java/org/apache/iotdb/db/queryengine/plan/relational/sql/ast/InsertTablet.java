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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class InsertTablet extends WrappedInsertStatement {

  private Map<IDeviceID, Integer> deviceID2LastIdxMap = null;

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
    prepareDeviceID2LastIdxMap();
    List<Object[]> deviceIdList = new ArrayList<>();
    for (IDeviceID deviceID : deviceID2LastIdxMap.keySet()) {
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
    prepareDeviceID2LastIdxMap();
    final InsertTabletStatement insertTabletStatement = getInnerTreeStatement();
    List<Object[]> result = new ArrayList<>(insertTabletStatement.getRowCount());
    final List<Integer> attrColumnIndices = insertTabletStatement.getAttrColumnIndices();
    for (Integer rowIndex : deviceID2LastIdxMap.values()) {
      Object[] attrValues = new Object[attrColumnIndices.size()];
      for (int attrColNum = 0; attrColNum < attrColumnIndices.size(); attrColNum++) {
        final int columnIndex = attrColumnIndices.get(attrColNum);
        if (!insertTabletStatement.isNull(rowIndex, columnIndex)) {
          attrValues[attrColNum] =
              ((Object[]) insertTabletStatement.getColumns()[columnIndex])[rowIndex];
        }
      }
      result.add(attrValues);
    }
    return result;
  }

  // The map cannot be maintained during construction because the IDeviceID may be reset later.
  private void prepareDeviceID2LastIdxMap() {
    if (deviceID2LastIdxMap != null) {
      return;
    }
    InsertTabletStatement insertTabletStatement = getInnerTreeStatement();
    deviceID2LastIdxMap = new LinkedHashMap<>(insertTabletStatement.getRowCount());
    for (int i = 0; i < insertTabletStatement.getRowCount(); i++) {
      IDeviceID deviceID = insertTabletStatement.getTableDeviceID(i);
      deviceID2LastIdxMap.put(deviceID, i);
    }
    if (deviceID2LastIdxMap.size() == 1) {
      insertTabletStatement.setSingleDevice();
    }
  }
}
