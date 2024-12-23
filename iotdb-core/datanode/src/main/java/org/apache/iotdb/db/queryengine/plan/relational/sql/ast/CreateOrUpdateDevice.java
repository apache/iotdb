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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.iotdb.db.storageengine.dataregion.memtable.DeviceIDFactory.truncateTailingNull;

public class CreateOrUpdateDevice extends Statement {

  private final String database;

  private final String table;

  private final List<Object[]> deviceIdList;

  private final List<String> attributeNameList;

  private final List<Object[]> attributeValueList;

  public CreateOrUpdateDevice(
      final String database,
      final String table,
      final List<Object[]> deviceIdList,
      final List<String> attributeNameList,
      final List<Object[]> attributeValueList) {
    super(null);
    this.database = database;
    this.table = table;
    // Truncate the tailing null
    this.deviceIdList = truncateTailingNull(deviceIdList);
    this.attributeNameList = attributeNameList;
    this.attributeValueList = attributeValueList;
  }

  public String getDatabase() {
    return database;
  }

  public String getTable() {
    return table;
  }

  public List<Object[]> getDeviceIdList() {
    return deviceIdList;
  }

  public List<String> getAttributeNameList() {
    return attributeNameList;
  }

  public List<Object[]> getAttributeValueList() {
    return attributeValueList;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitCreateDevice(this, context);
  }

  @Override
  public List<? extends Node> getChildren() {
    return Collections.emptyList();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final CreateOrUpdateDevice that = (CreateOrUpdateDevice) o;
    return Objects.equals(database, that.database)
        && Objects.equals(table, that.table)
        && Objects.equals(deviceIdList, that.deviceIdList)
        && Objects.equals(attributeNameList, that.attributeNameList)
        && Objects.equals(attributeValueList, that.attributeValueList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(database, table, deviceIdList, attributeNameList, attributeValueList);
  }

  @Override
  public String toString() {
    return "CreateDevice{"
        + "database='"
        + database
        + '\''
        + ", table='"
        + table
        + '\''
        + ", deviceIdList="
        + deviceIdList
        + ", attributeNameList="
        + attributeNameList
        + ", attributeValueList="
        + attributeValueList
        + '}';
  }
}
