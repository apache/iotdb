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

package org.apache.iotdb.db.sql.statement;

import org.apache.iotdb.db.sql.statement.component.GroupByLevelComponent;

public class AggregationQueryStatement extends QueryStatement {

  // GROUP BY LEVEL clause
  protected GroupByLevelComponent groupByLevelComponent;

  public AggregationQueryStatement() {
    super();
  }

  public AggregationQueryStatement(QueryStatement queryStatement) {
    super(queryStatement);
  }

  public GroupByLevelComponent getGroupByLevelComponent() {
    return groupByLevelComponent;
  }

  public void setGroupByLevelComponent(GroupByLevelComponent groupByLevelComponent) {
    this.groupByLevelComponent = groupByLevelComponent;
  }

  @Override
  public boolean isGroupByLevel() {
    return groupByLevelComponent != null && groupByLevelComponent.getLevels().length > 0;
  };
}
