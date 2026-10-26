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
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeUtils;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ITableDeviceSchemaValidation;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.TableDeviceSchemaValidator;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Constants;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InsertRows extends WrappedInsertStatement {

  // Only InsertRows constructed by Pipe will be set to true
  private boolean allowCreateTable = false;

  public InsertRows(InsertRowsStatement insertRowsStatement, MPPQueryContext context) {
    super(insertRowsStatement, context);
  }

  @Override
  public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
    return ((AstVisitor<R, C>) visitor).visitInsertRows(this, context);
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
  public String getTableName() {
    return getInnerTreeStatement().getInsertRowStatementList().get(0).getDevicePath().getFullPath();
  }

  @Override
  public List<Object[]> getDeviceIdList() {
    final InsertRowsStatement insertRowStatement = getInnerTreeStatement();
    return insertRowStatement.getDeviceIdListNoTableName();
  }

  @Override
  public List<String> getAttributeColumnNameList() {
    // each row may have different columns
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Object[]> getAttributeValueList() {
    // each row may have different columns
    throw new UnsupportedOperationException();
  }

  public void setAllowCreateTable(boolean allowCreateTable) {
    this.allowCreateTable = allowCreateTable;
  }

  @Override
  public void validateTableSchema(Metadata metadata, MPPQueryContext context) {
    for (InsertRowStatement insertRowStatement :
        getInnerTreeStatement().getInsertRowStatementList()) {
      final String database = AnalyzeUtils.getDatabaseName(insertRowStatement, context);
      super.validateTableSchema(metadata, context, insertRowStatement, database, allowCreateTable);
    }
  }

  @Override
  public void validateDeviceSchema(Metadata metadata, MPPQueryContext context) {
    final Map<String, Map<String, CoalescedDeviceSchemaValidation>> validationMap =
        new LinkedHashMap<>();
    for (InsertRowStatement insertRowStatement :
        getInnerTreeStatement().getInsertRowStatementList()) {
      final ITableDeviceSchemaValidation rowValidation =
          createTableSchemaValidation(insertRowStatement);
      validationMap
          .computeIfAbsent(rowValidation.getDatabase(), key -> new LinkedHashMap<>())
          .computeIfAbsent(
              rowValidation.getTableName(),
              tableName ->
                  new CoalescedDeviceSchemaValidation(rowValidation.getDatabase(), tableName))
          .add(
              rowValidation.getDeviceIdList().get(0),
              rowValidation.getAttributeColumnNameList(),
              rowValidation.getAttributeValueList().get(0));
    }
    validationMap.values().stream()
        .flatMap(tableValidationMap -> tableValidationMap.values().stream())
        .forEach(validation -> metadata.validateDeviceSchema(validation, context));
  }

  protected ITableDeviceSchemaValidation createTableSchemaValidation(
      InsertRowStatement insertRowStatement) {
    return new ITableDeviceSchemaValidation() {

      @Override
      public String getDatabase() {
        return AnalyzeUtils.getDatabaseName(insertRowStatement, context);
      }

      @Override
      public String getTableName() {
        return insertRowStatement.getTableDeviceID().getTableName();
      }

      @Override
      public List<Object[]> getDeviceIdList() {
        final Object[] tagSegments = insertRowStatement.getTableDeviceID().getSegments();
        if (Objects.nonNull(insertRowStatement.getMeasurementSchemas())
            && Arrays.stream(insertRowStatement.getMeasurementSchemas())
                .anyMatch(
                    schema -> Objects.nonNull(schema) && schema.getType() == TSDataType.OBJECT)) {
          TableDeviceSchemaValidator.checkObject4DeviceId(tagSegments);
        }
        return Collections.singletonList(Arrays.copyOfRange(tagSegments, 1, tagSegments.length));
      }

      @Override
      public List<String> getAttributeColumnNameList() {
        return insertRowStatement.getAttributeColumnNameList();
      }

      @Override
      public List<Object[]> getAttributeValueList() {
        List<Object> attributeValueList = new ArrayList<>();
        final TsTableColumnCategory[] columnCategories = insertRowStatement.getColumnCategories();
        final String[] measurements = insertRowStatement.getMeasurements();
        final Object[] values = insertRowStatement.getValues();
        for (int i = 0; columnCategories != null && i < columnCategories.length; i++) {
          if (columnCategories[i] == TsTableColumnCategory.ATTRIBUTE
              && measurements != null
              && i < measurements.length
              && measurements[i] != null
              && values != null
              && i < values.length) {
            attributeValueList.add(values[i]);
          }
        }
        return Collections.singletonList(attributeValueList.toArray());
      }
    };
  }

  private static class CoalescedDeviceSchemaValidation implements ITableDeviceSchemaValidation {

    private final String database;
    private final String tableName;
    private final Map<DeviceIdKey, Map<Integer, Object>> deviceAttributeValueMap =
        new LinkedHashMap<>();
    private final List<String> attributeColumnNameList = new ArrayList<>();
    private final Map<String, Integer> attributeColumnIndexMap = new LinkedHashMap<>();

    private CoalescedDeviceSchemaValidation(final String database, final String tableName) {
      this.database = database;
      this.tableName = tableName;
    }

    private void add(
        final Object[] deviceId,
        final List<String> attributeColumnNames,
        final Object[] attributeValues) {
      final Map<Integer, Object> attributeValueMap =
          deviceAttributeValueMap.computeIfAbsent(
              new DeviceIdKey(deviceId), key -> new HashMap<>());
      for (int i = 0; i < attributeColumnNames.size(); i++) {
        final int attributeColumnIndex =
            attributeColumnIndexMap.computeIfAbsent(
                attributeColumnNames.get(i),
                attributeColumnName -> {
                  attributeColumnNameList.add(attributeColumnName);
                  return attributeColumnNameList.size() - 1;
                });
        if (i < attributeValues.length
            && attributeValues[i] != null
            && attributeValues[i] != Constants.NONE) {
          attributeValueMap.put(attributeColumnIndex, attributeValues[i]);
        }
      }
    }

    @Override
    public String getDatabase() {
      return database;
    }

    @Override
    public String getTableName() {
      return tableName;
    }

    @Override
    public List<Object[]> getDeviceIdList() {
      final List<Object[]> deviceIdList = new ArrayList<>(deviceAttributeValueMap.size());
      deviceAttributeValueMap.keySet().forEach(key -> deviceIdList.add(key.deviceId));
      return deviceIdList;
    }

    @Override
    public List<String> getAttributeColumnNameList() {
      return attributeColumnNameList;
    }

    @Override
    public List<Object[]> getAttributeValueList() {
      final List<Object[]> attributeValueList = new ArrayList<>(deviceAttributeValueMap.size());
      for (final Map<Integer, Object> attributeValueMap : deviceAttributeValueMap.values()) {
        final Object[] attributeValues = new Object[attributeColumnNameList.size()];
        Arrays.fill(attributeValues, Constants.NONE);
        attributeValueMap.forEach((index, value) -> attributeValues[index] = value);
        attributeValueList.add(attributeValues);
      }
      return attributeValueList;
    }
  }

  private static class DeviceIdKey {

    private final Object[] deviceId;

    private DeviceIdKey(final Object[] deviceId) {
      this.deviceId = (Object[]) deviceId.clone();
    }

    @Override
    public boolean equals(final Object object) {
      return this == object
          || object instanceof DeviceIdKey
              && Arrays.equals(deviceId, ((DeviceIdKey) object).deviceId);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(deviceId);
    }
  }
}
