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

public class ShowDevice extends AbstractQueryDevice {

  // For sql-input show device usage
  public ShowDevice(final QualifiedName name, final Expression rawExpression) {
    super(name, rawExpression);
  }

  // For device fetch serving data query
  public ShowDevice(
      final String database,
      final String tableName,
      final List<List<SchemaFilter>> idDeterminedPredicateList,
      final Expression idFuzzyFilterList,
      final List<IDeviceID> partitionKeyList) {
    super(database, tableName, idDeterminedPredicateList, idFuzzyFilterList, partitionKeyList);
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitShowDevice(this, context);
  }

  @Override
  public String toString() {
    return "ShowDevice" + toStringContent();
  }
}
