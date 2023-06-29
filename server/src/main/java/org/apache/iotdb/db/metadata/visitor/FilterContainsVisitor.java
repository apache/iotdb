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

package org.apache.iotdb.db.metadata.visitor;

import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.schema.filter.SchemaFilterType;
import org.apache.iotdb.commons.schema.filter.SchemaFilterVisitor;
import org.apache.iotdb.commons.schema.filter.impl.AndFilter;

/** This Visitor is used to determine if the SchemaFilter tree contains a certain type of Filter. */
public class FilterContainsVisitor extends SchemaFilterVisitor<SchemaFilterType> {

  @Override
  protected boolean visitNode(SchemaFilter filter, SchemaFilterType schemaFilterType) {
    return filter != null && filter.getSchemaFilterType().equals(schemaFilterType);
  }

  @Override
  public boolean visitAndFilter(AndFilter andFilter, SchemaFilterType schemaFilterType) {
    return andFilter.getLeft().accept(this, schemaFilterType)
        || andFilter.getRight().accept(this, schemaFilterType);
  }
}
