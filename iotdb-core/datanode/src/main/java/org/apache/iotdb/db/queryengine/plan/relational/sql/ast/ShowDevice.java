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

import org.apache.iotdb.commons.schema.filter.SchemaFilter;

import org.apache.tsfile.file.metadata.IDeviceID;

import java.util.List;
import java.util.Objects;

public class ShowDevice extends Statement {

  private final String database;

  private final String tableName;

  private Expression rawExpression;

  /**
   * The outer list represents the OR relation between different expression lists.
   *
   * <p>The inner list represents the AND between different expression.
   *
   * <p>Each inner list represents a device pattern and each expression of it represents one
   * condition on some id column.
   */
  private List<List<SchemaFilter>> idDeterminedFilterList;

  /** filters/conditions involving non-id columns and concat by OR to id column filters */
  private Expression idFuzzyPredicate;

  private boolean isIdDetermined = false;

  private transient List<IDeviceID> partitionKeyList;

  // for sql-input show device usage
  public ShowDevice(String database, String tableName, Expression rawExpression) {
    super(null);
    this.database = database;
    this.tableName = tableName;
    this.rawExpression = rawExpression;
  }

  // for device fetch serving data query
  public ShowDevice(
      String database,
      String tableName,
      List<List<SchemaFilter>> idDeterminedFilterList,
      Expression idFuzzyFilterList,
      List<IDeviceID> partitionKeyList) {
    super(null);
    this.database = database;
    this.tableName = tableName;
    this.idDeterminedFilterList = idDeterminedFilterList;
    this.idFuzzyPredicate = idFuzzyFilterList;
    this.partitionKeyList = partitionKeyList;
  }

  public String getDatabase() {
    return database;
  }

  public String getTableName() {
    return tableName;
  }

  public Expression getRawExpression() {
    return rawExpression;
  }

  public List<List<SchemaFilter>> getIdDeterminedFilterList() {
    if (idDeterminedFilterList == null) {
      // TODO table metadata: process raw expression input by show device sql
    }
    return idDeterminedFilterList;
  }

  public Expression getIdFuzzyPredicate() {
    if (idFuzzyPredicate == null) {
      // TODO table metadata: process raw expression input by show device sql
    }
    return idFuzzyPredicate;
  }

  public boolean isIdDetermined() {
    return Objects.nonNull(partitionKeyList);
  }

  public List<IDeviceID> getPartitionKeyList() {
    return partitionKeyList;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitShowDevice(this, context);
  }

  @Override
  public List<? extends Node> getChildren() {
    return null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ShowDevice that = (ShowDevice) o;
    return Objects.equals(database, that.database)
        && Objects.equals(tableName, that.tableName)
        && Objects.equals(rawExpression, that.rawExpression)
        && Objects.equals(idDeterminedFilterList, that.idDeterminedFilterList)
        && Objects.equals(idFuzzyPredicate, that.idFuzzyPredicate);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        database, tableName, rawExpression, idDeterminedFilterList, idFuzzyPredicate);
  }

  @Override
  public String toString() {
    return "ShowDevice{"
        + "database='"
        + database
        + '\''
        + ", tableName='"
        + tableName
        + '\''
        + ", rawExpression="
        + rawExpression
        + ", idDeterminedFilterList="
        + idDeterminedFilterList
        + ", idFuzzyFilter="
        + idFuzzyPredicate
        + '}';
  }
}
