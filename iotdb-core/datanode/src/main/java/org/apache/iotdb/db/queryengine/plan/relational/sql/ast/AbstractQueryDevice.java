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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

// TODO table metadata: reuse query distinct logic
public abstract class AbstractQueryDevice extends Statement {

  private String database;

  private final String tableName;

  // Currently unused, shall be parsed into idDeterminedPredicateList and idFuzzyPredicate on demand
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

  private List<IDeviceID> partitionKeyList;

  // For sql-input show device usage
  protected AbstractQueryDevice(final String tableName, final Expression rawExpression) {
    super(null);
    this.tableName = tableName;
    this.rawExpression = rawExpression;
  }

  // For device fetch serving data query
  protected AbstractQueryDevice(
      final String database,
      final String tableName,
      final List<List<SchemaFilter>> idDeterminedFilterList,
      final Expression idFuzzyFilterList,
      final List<IDeviceID> partitionKeyList) {
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
      idDeterminedFilterList = Collections.singletonList(Collections.emptyList());
    }
    return idDeterminedFilterList;
  }

  public Expression getIdFuzzyPredicate() {
    return idFuzzyPredicate;
  }

  public boolean isIdDetermined() {
    return Objects.nonNull(partitionKeyList);
  }

  public List<IDeviceID> getPartitionKeyList() {
    return partitionKeyList;
  }

  @Override
  public List<? extends Node> getChildren() {
    return null;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final AbstractQueryDevice that = (AbstractQueryDevice) o;
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

  protected String toStringContent() {
    return "{"
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
