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
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeUtils;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ITableDeviceSchemaValidation;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableSchema;
import org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement;
import org.apache.iotdb.db.utils.TypeInferenceUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.utils.EncodingInferenceUtils.getDefaultEncoding;

public abstract class WrappedInsertStatement extends WrappedStatement
    implements ITableDeviceSchemaValidation {

  protected TableSchema tableSchema;

  protected WrappedInsertStatement(
      InsertBaseStatement innerTreeStatement, MPPQueryContext context) {
    super(innerTreeStatement, context);
  }

  @Override
  public InsertBaseStatement getInnerTreeStatement() {
    return ((InsertBaseStatement) super.getInnerTreeStatement());
  }

  public abstract void updateAfterSchemaValidation(MPPQueryContext context)
      throws QueryProcessException;

  public TableSchema getTableSchema() {
    if (tableSchema == null) {
      InsertBaseStatement insertBaseStatement = getInnerTreeStatement();
      tableSchema = toTableSchema(insertBaseStatement);
    }

    return tableSchema;
  }

  protected TableSchema toTableSchema(InsertBaseStatement insertBaseStatement) {
    String tableName = insertBaseStatement.getDevicePath().getFullPath();
    List<ColumnSchema> columnSchemas =
        new ArrayList<>(insertBaseStatement.getMeasurements().length);
    for (int i = 0; i < insertBaseStatement.getMeasurements().length; i++) {
      if (insertBaseStatement.getMeasurements()[i] != null) {
        TSDataType dataType = insertBaseStatement.getDataType(i);
        if (dataType == null) {
          dataType =
              TypeInferenceUtils.getPredictedDataType(
                  insertBaseStatement.getFirstValueOfIndex(i), true);
        }
        columnSchemas.add(
            new ColumnSchema(
                insertBaseStatement.getMeasurements()[i],
                dataType != null ? TypeFactory.getType(dataType) : null,
                false,
                insertBaseStatement.getColumnCategory(i)));
      } else {
        columnSchemas.add(null);
      }
    }
    return new TableSchema(tableName, columnSchemas);
  }

  public void validateTableSchema(Metadata metadata, MPPQueryContext context) {
    String databaseName = getDatabase();
    final TableSchema incomingSchema = getTableSchema();
    final TableSchema realSchema =
        metadata
            .validateTableHeaderSchema(databaseName, incomingSchema, context, true)
            .orElse(null);
    if (realSchema == null) {
      throw new SemanticException(
          "Schema validation failed, table cannot be created: " + incomingSchema);
    }
    InsertBaseStatement innerTreeStatement = getInnerTreeStatement();
    validateTableSchema(realSchema, incomingSchema, innerTreeStatement);
  }

  protected void validateTableSchema(
      TableSchema realSchema,
      TableSchema incomingTableSchema,
      InsertBaseStatement innerTreeStatement) {
    final List<ColumnSchema> incomingSchemaColumns = incomingTableSchema.getColumns();
    Map<String, ColumnSchema> realSchemaMap = new HashMap<>();
    realSchema.getColumns().forEach(c -> realSchemaMap.put(c.getName(), c));
    // incoming schema should be consistent with real schema
    for (int i = 0, incomingSchemaColumnsSize = incomingSchemaColumns.size();
        i < incomingSchemaColumnsSize;
        i++) {
      ColumnSchema incomingSchemaColumn = incomingSchemaColumns.get(i);
      final ColumnSchema realSchemaColumn = realSchemaMap.get(incomingSchemaColumn.getName());
      validateTableSchema(incomingSchemaColumn, realSchemaColumn, i, innerTreeStatement);
    }
    // incoming schema should contain all id columns in real schema and have consistent order
    final List<ColumnSchema> realIdColumns = realSchema.getIdColumns();
    adjustIdColumns(realIdColumns, innerTreeStatement);
  }

  /**
   * Adjust the order of ID columns in this insertion to be consistent with that from the schema
   * region.
   *
   * @param realIdColumnSchemas id column order from the schema region
   */
  public void adjustIdColumns(
      List<ColumnSchema> realIdColumnSchemas, final InsertBaseStatement baseStatement) {
    List<ColumnSchema> incomingColumnSchemas = toTableSchema(baseStatement).getColumns();
    for (int realIdColPos = 0; realIdColPos < realIdColumnSchemas.size(); realIdColPos++) {
      ColumnSchema realColumn = realIdColumnSchemas.get(realIdColPos);
      int incomingIdColPos = incomingColumnSchemas.indexOf(realColumn);
      if (incomingIdColPos == -1) {
        // if the realIdColPos-th id column in the table is missing, insert an empty column in the
        // tablet
        baseStatement.insertColumn(realIdColPos, realColumn);
        incomingColumnSchemas.add(realIdColPos, realColumn);
      } else {
        // move the id column in the tablet to the proper position
        baseStatement.swapColumn(incomingIdColPos, realIdColPos);
        Collections.swap(incomingColumnSchemas, incomingIdColPos, realIdColPos);
      }
    }
    tableSchema = null;
  }

  public static void validateTableSchema(
      ColumnSchema incoming, ColumnSchema real, int i, InsertBaseStatement innerTreeStatement) {
    if (real == null) {
      // the column does not exist and auto-creation is disabled
      SemanticException semanticException =
          new SemanticException(
              "Column " + incoming.getName() + " does not exists or fails to be " + "created",
              TSStatusCode.COLUMN_NOT_EXISTS.getStatusCode());
      if (incoming.getColumnCategory() != TsTableColumnCategory.MEASUREMENT) {
        // non-measurement columns cannot be partially inserted
        throw semanticException;
      } else {
        // partial insertion
        innerTreeStatement.markFailedMeasurement(i, semanticException);
        return;
      }
    }
    if (incoming.getType() == null
        || incoming.getColumnCategory() != TsTableColumnCategory.MEASUREMENT) {
      // sql insertion does not provide type
      // the type is inferred and can be inconsistent with the existing one
      innerTreeStatement.setDataType(InternalTypeManager.getTSDataType(real.getType()), i);
    } else if (!incoming.getType().equals(real.getType())) {
      SemanticException semanticException =
          new SemanticException(
              String.format(
                  "Inconsistent data type of column %s: %s/%s",
                  incoming.getName(), incoming.getType(), real.getType()),
              TSStatusCode.DATA_TYPE_MISMATCH.getStatusCode());
      if (incoming.getColumnCategory() != TsTableColumnCategory.MEASUREMENT) {
        // non-measurement columns cannot be partially inserted
        throw semanticException;
      } else {
        // partial insertion
        innerTreeStatement.markFailedMeasurement(i, semanticException);
      }
    }
    if (incoming.getColumnCategory() == null) {
      // sql insertion does not provide category
      innerTreeStatement.setColumnCategory(real.getColumnCategory(), i);
    } else if (!incoming.getColumnCategory().equals(real.getColumnCategory())) {
      throw new SemanticException(
          String.format(
              "Inconsistent column category of column %s: %s/%s",
              incoming.getName(), incoming.getColumnCategory(), real.getColumnCategory()),
          TSStatusCode.COLUMN_CATEGORY_MISMATCH.getStatusCode());
    }
    TSDataType tsDataType = InternalTypeManager.getTSDataType(real.getType());
    MeasurementSchema measurementSchema =
        new MeasurementSchema(
            real.getName(),
            tsDataType,
            getDefaultEncoding(tsDataType),
            TSFileDescriptor.getInstance().getConfig().getCompressor());
    innerTreeStatement.setMeasurementSchema(measurementSchema, i);
  }

  public void validateDeviceSchema(Metadata metadata, MPPQueryContext context) {
    metadata.validateDeviceSchema(this, context);
  }

  public String getDatabase() {
    String databaseName = AnalyzeUtils.getDatabaseName(getInnerTreeStatement(), context);
    if (databaseName == null) {
      throw new SemanticException("database is not specified");
    }
    return databaseName;
  }

  public void toLowerCase() {
    getInnerTreeStatement().toLowerCase();
  }
}
