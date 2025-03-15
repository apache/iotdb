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

package org.apache.iotdb.db.queryengine.plan.planner.exceptions;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;

/**
 * When ALL DataNodeLocations in a QUERY-typed {@link
 * org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance} are unreachable, possibly due
 * to network partition issues, this exception will be thrown and this query will fail.
 */
public class ReplicaSetUnreachableException extends RuntimeException {
  public ReplicaSetUnreachableException(TRegionReplicaSet replicaSet) {
    super(replicaSet.toString());
  }
}
