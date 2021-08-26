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

package org.apache.iotdb.cluster.log.applier;

import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertMultiTabletPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowsPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleReplicaApplier extends DataLogApplier {
  private static final Logger logger = LoggerFactory.getLogger(SingleReplicaApplier.class);

  public SingleReplicaApplier(MetaGroupMember metaGroupMember, DataGroupMember dataGroupMember) {
    super(metaGroupMember, dataGroupMember);
  }

  @Override
  public void apply(Log log) {}

  public void applyPhysicalPlan(PhysicalPlan plan)
      throws QueryProcessException, StorageGroupNotSetException, StorageEngineException {
    if (plan instanceof InsertMultiTabletPlan) {
      applyInsert((InsertMultiTabletPlan) plan);
    } else if (plan instanceof InsertRowsPlan) {
      applyInsert((InsertRowsPlan) plan);
    } else if (plan instanceof InsertPlan) {
      applyInsert((InsertPlan) plan);
    } else {
      applyPhysicalPlan(plan, dataGroupMember);
    }
  }
}
