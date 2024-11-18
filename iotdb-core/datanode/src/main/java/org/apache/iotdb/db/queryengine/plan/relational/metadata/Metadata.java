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

package org.apache.iotdb.db.queryengine.plan.relational.metadata;

import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.relational.function.OperatorType;
import org.apache.iotdb.db.queryengine.plan.relational.security.AccessControl;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.type.TypeNotFoundException;
import org.apache.iotdb.db.queryengine.plan.relational.type.TypeSignature;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.type.Type;

import java.util.List;
import java.util.Optional;

// All the input databases shall not contain "root"
public interface Metadata {

  boolean tableExists(final QualifiedObjectName name);

  /**
   * Return table schema definition for the specified table handle. Table schema definition is a set
   * of information required by semantic analyzer to analyze the query.
   *
   * @throws RuntimeException if table handle is no longer valid
   */
  Optional<TableSchema> getTableSchema(final SessionInfo session, final QualifiedObjectName name);

  Type getOperatorReturnType(
      final OperatorType operatorType, final List<? extends Type> argumentTypes)
      throws OperatorNotFoundException;

  Type getFunctionReturnType(final String functionName, final List<? extends Type> argumentTypes);

  boolean isAggregationFunction(
      final SessionInfo session, final String functionName, final AccessControl accessControl);

  Type getType(final TypeSignature signature) throws TypeNotFoundException;

  boolean canCoerce(final Type from, final Type to);

  IPartitionFetcher getPartitionFetcher();

  /**
   * Get all device ids and corresponding attributes from schema region
   *
   * @param tableName qualified table name
   * @param expressionList device filter in conj style, need to remove all the deviceId filter after
   *     index scanning
   * @param attributeColumns attribute column names
   */
  List<DeviceEntry> indexScan(
      final QualifiedObjectName tableName,
      final List<Expression> expressionList,
      final List<String> attributeColumns,
      final MPPQueryContext context);

  /**
   * This method is used for table column validation and should be invoked before device validation.
   *
   * <p>This method return all the existing column schemas in the target table.
   *
   * <p>The reason that we need to return all the existing column schemas is that the caller need to
   * know all id columns to construct IDeviceID
   *
   * <p>When table or column is missing, this method will execute auto creation if the user have
   * corresponding authority.
   *
   * <p>When using SQL, the columnSchemaList could be null and there won't be any validation.
   *
   * <p>When the input dataType or category of one column is null, the column won't be auto created.
   *
   * <p>The caller need to recheck the dataType of measurement columns to decide whether to do
   * partial insert
   *
   * @return If table doesn't exist and the user have no authority to create table, Optional.empty()
   *     will be returned. The returned table may not include all the columns
   *     in @param{tableSchema}, if the user have no authority to alter table.
   * @throws SemanticException if column category mismatch or data types of id or attribute column
   *     are not STRING or Category, Type of any missing ColumnSchema is null
   */
  Optional<TableSchema> validateTableHeaderSchema(
      final String database,
      final TableSchema tableSchema,
      final MPPQueryContext context,
      final boolean allowCreateTable);

  /**
   * This method is used for table device validation and should be invoked after column validation.
   *
   * <p>When device id is missing, this method will execute auto creation.
   *
   * <p>When device attribute is missing or different from that stored in IoTDB, the attribute will
   * be auto upsert.
   *
   * <p>If validation failed, a SemanticException will be thrown.
   */
  void validateDeviceSchema(
      final ITableDeviceSchemaValidation schemaValidation, final MPPQueryContext context);

  /**
   * Get or create data partition, used in cluster write scenarios. if enableAutoCreateSchema is
   * true and database/series/time slots not exists, then automatically create.
   *
   * @param dataPartitionQueryParams the list of DataPartitionQueryParams
   * @param userName
   */
  default DataPartition getOrCreateDataPartition(
      final List<DataPartitionQueryParam> dataPartitionQueryParams, final String userName) {
    throw new UnsupportedOperationException();
  }

  // ======================== Table Model Schema Partition Interface ========================
  /**
   * Get or create schema partition, used in data insertion with enable_auto_create_schema is true.
   * if schemaPartition does not exist, then automatically create.
   *
   * <p>The database shall start with "root.". Concat this to a user-provided db name if necessary.
   *
   * <p>The device id shall be [table, seg1, ....]
   */
  SchemaPartition getOrCreateSchemaPartition(
      final String database, final List<IDeviceID> deviceIDList, final String userName);

  /**
   * For data query with completed id.
   *
   * <p>The database is a user-provided db name.
   *
   * <p>The device id shall be [table, seg1, ....]
   */
  SchemaPartition getSchemaPartition(final String database, final List<IDeviceID> deviceIDList);

  /**
   * For data query with partial device id conditions.
   *
   * <p>The database is a user-provided db name.
   *
   * <p>The device id shall be [table, seg1, ....]
   */
  SchemaPartition getSchemaPartition(final String database);

  // ======================== Table Model Data Partition Interface ========================
  /**
   * Get data partition, used in query scenarios.
   *
   * @param database a user-provided db name, the database shall start with "root.".
   * @param sgNameToQueryParamsMap database name -> the list of DataPartitionQueryParams
   */
  DataPartition getDataPartition(
      final String database, final List<DataPartitionQueryParam> sgNameToQueryParamsMap);

  /**
   * Get data partition, used in query scenarios which contains time filter like: time < XX or time
   * > XX
   *
   * @param database a user-provided db name, the database shall start with "root.".
   * @return sgNameToQueryParamsMap database name -> the list of DataPartitionQueryParams
   */
  DataPartition getDataPartitionWithUnclosedTimeRange(
      final String database, final List<DataPartitionQueryParam> sgNameToQueryParamsMap);
}
