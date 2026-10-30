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

package org.apache.iotdb.commons.subscription.columnfilter;

import org.apache.iotdb.commons.i18n.PipeMessages;

import java.util.Objects;

public class ColumnMetadata {

  private final String database;
  private final String tableName;
  private final String columnName;
  private final String datatype;
  private final String category;

  public ColumnMetadata(
      final String database,
      final String tableName,
      final String columnName,
      final String datatype,
      final String category) {
    this.database =
        Objects.requireNonNull(
            database, PipeMessages.EXCEPTION_DATABASE_SHOULD_NOT_BE_NULL_3CF2693B);
    this.tableName =
        Objects.requireNonNull(
            tableName, PipeMessages.EXCEPTION_TABLENAME_SHOULD_NOT_BE_NULL_FFA95ABE);
    this.columnName =
        Objects.requireNonNull(
            columnName, PipeMessages.EXCEPTION_COLUMNNAME_SHOULD_NOT_BE_NULL_695366FD);
    this.datatype =
        Objects.requireNonNull(
            datatype, PipeMessages.EXCEPTION_DATATYPE_SHOULD_NOT_BE_NULL_58A47BB3);
    this.category =
        Objects.requireNonNull(
            category, PipeMessages.EXCEPTION_CATEGORY_SHOULD_NOT_BE_NULL_78602159);
  }

  public String getDatabase() {
    return database;
  }

  public String getTableName() {
    return tableName;
  }

  public String getColumnName() {
    return columnName;
  }

  public String getDatatype() {
    return datatype;
  }

  public String getCategory() {
    return category;
  }
}
