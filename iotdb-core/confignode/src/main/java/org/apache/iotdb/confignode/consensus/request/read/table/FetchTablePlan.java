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

package org.apache.iotdb.confignode.consensus.request.read.table;

import org.apache.iotdb.commons.schema.table.TableNodeStatus;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.read.ConfigPhysicalReadPlan;

import java.util.Map;
import java.util.Set;

public class FetchTablePlan extends ConfigPhysicalReadPlan {

  private final Map<String, Set<String>> fetchTableMap;
  private final Set<TableNodeStatus> tableNodeStatusSet;

  public FetchTablePlan(
      final Map<String, Set<String>> fetchTableMap, Set<TableNodeStatus> tableNodeStatus) {
    super(ConfigPhysicalPlanType.FetchTable);
    this.fetchTableMap = fetchTableMap;
    this.tableNodeStatusSet = tableNodeStatus;
  }

  public Map<String, Set<String>> getFetchTableMap() {
    return fetchTableMap;
  }

  public Set<TableNodeStatus> getTableNodeStatusSet() {
    return tableNodeStatusSet;
  }
}
