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

package org.apache.iotdb.db.queryengine.plan.statement.table;

import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowStatement;

import java.util.List;

public class ShowTableDevicesStatement extends ShowStatement {

  private final String database;

  private final String tableName;

  private final List<List<SchemaFilter>> idDeterminedFilterList;

  private final SchemaFilter idFuzzyFilter;

  public ShowTableDevicesStatement(
      String database,
      String tableName,
      List<List<SchemaFilter>> idDeterminedFilterList,
      SchemaFilter idFuzzyFilter) {
    super();
    this.database = database;
    this.tableName = tableName;
    this.idDeterminedFilterList = idDeterminedFilterList;
    this.idFuzzyFilter = idFuzzyFilter;
  }

  public String getDatabase() {
    return database;
  }

  public String getTableName() {
    return tableName;
  }

  public SchemaFilter getIdFuzzyFilter() {
    return idFuzzyFilter;
  }

  public List<List<SchemaFilter>> getIdDeterminedFilterList() {
    return idDeterminedFilterList;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitShowTableDevices(this, context);
  }
}
