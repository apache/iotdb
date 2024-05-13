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

package org.apache.iotdb.db.queryengine.plan.statement.metadata;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.component.WhereCondition;

public class CountTimeSeriesStatement extends CountStatement {

  private SchemaFilter schemaFilter;

  private WhereCondition timeCondition;

  public CountTimeSeriesStatement(PartialPath partialPath) {
    super(partialPath);
  }

  public SchemaFilter getSchemaFilter() {
    return schemaFilter;
  }

  public void setSchemaFilter(SchemaFilter schemaFilter) {
    this.schemaFilter = schemaFilter;
  }

  public void setTimeCondition(WhereCondition timeCondition) {
    this.timeCondition = timeCondition;
  }

  public boolean hasTimeCondition() {
    return timeCondition != null;
  }

  public WhereCondition getTimeCondition() {
    return timeCondition;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitCountTimeSeries(this, context);
  }
}
