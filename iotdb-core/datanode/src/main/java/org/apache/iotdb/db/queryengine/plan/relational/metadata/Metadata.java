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

import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.relational.function.OperatorType;
import org.apache.iotdb.db.queryengine.plan.relational.security.AccessControl;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.type.TypeNotFoundException;
import org.apache.iotdb.db.queryengine.plan.relational.type.TypeSignature;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.type.Type;

import java.util.List;
import java.util.Optional;

public interface Metadata {

  boolean tableExists(QualifiedObjectName name);

  /**
   * Return table schema definition for the specified table handle. Table schema definition is a set
   * of information required by semantic analyzer to analyze the query.
   *
   * @throws RuntimeException if table handle is no longer valid
   */
  Optional<TableSchema> getTableSchema(SessionInfo session, QualifiedObjectName name);

  Type getOperatorReturnType(OperatorType operatorType, List<? extends Type> argumentTypes)
      throws OperatorNotFoundException;

  Type getFunctionReturnType(String functionName, List<? extends Type> argumentTypes);

  boolean isAggregationFunction(
      SessionInfo session, String functionName, AccessControl accessControl);

  Type getType(TypeSignature signature) throws TypeNotFoundException;

  boolean canCoerce(Type from, Type to);

  /**
   * get all device ids and corresponding attributes from schema region
   *
   * @param tableName qualified table name
   * @param expressionList device filter in conj style, need to remove all the deviceId filter after
   *     index scanning
   * @param attributeColumns attribute column names
   */
  List<DeviceEntry> indexScan(
      QualifiedObjectName tableName,
      List<Expression> expressionList,
      List<String> attributeColumns);

  /**
   * This method is used for table column validation and should be invoked before device validation.
   *
   * <p>This method return all the existing column schemas in the target table.
   *
   * <p>When table or column is missing, this method will execute auto creation.
   *
   * <p>When using SQL, the columnSchemaList could be null and there won't be any validation.
   *
   * <p>When the input dataType or category of one column is null, the column cannot be auto
   * created.
   *
   * <p>If validation failed, a SemanticException will be thrown.
   */
  TableSchema validateTableHeaderSchema(
      String database, TableSchema tableSchema, MPPQueryContext context);

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
  void validateDeviceSchema(ITableDeviceSchemaValidation schemaValidation, MPPQueryContext context);

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
      String database, List<IDeviceID> deviceIDList, String userName);

  /**
   * For data query with completed id.
   *
   * <p>The database is a user-provided db name.
   *
   * <p>The device id shall be [table, seg1, ....]
   */
  SchemaPartition getSchemaPartition(String database, List<IDeviceID> deviceIDList);

  /**
   * For data query with partial device id conditions.
   *
   * <p>The database is a user-provided db name.
   *
   * <p>The device id shall be [table, seg1, ....]
   */
  SchemaPartition getSchemaPartition(String database);
}
