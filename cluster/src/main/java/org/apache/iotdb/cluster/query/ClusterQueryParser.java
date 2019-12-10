/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.query;

import java.time.ZoneId;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.QueryProcessor;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;

public class ClusterQueryParser extends QueryProcessor {

  private MetaGroupMember metaGroupMember;

  public ClusterQueryParser(MetaGroupMember metaGroupMember) {
    super(new ClusterQueryExecutor(metaGroupMember));
    this.metaGroupMember = metaGroupMember;
  }

  @Override
  public PhysicalPlan parseSQLToPhysicalPlan(String sqlStr, ZoneId zoneId)
      throws QueryProcessException {
    Operator operator = parseDriver.parse(sqlStr, zoneId);
    // TODO-Cluster: support wildcard
    operator = logicalOptimize(operator, executor);
    PhysicalGenerator physicalGenerator = new ClusterPhysicalGenerator(executor, metaGroupMember);
    return physicalGenerator.transformToPhysicalPlan(operator);
  }
}
