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
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.Collection;

/**
 * During planning phase of Query, if there exists no datanode that can be served as the role of
 * RootFragmentInstance placement, that is, no datanode can reach to all replica-sets possibly due
 * to network partition issues, this exception will be thrown and this query will fail.
 */
public class RootFIPlacementException extends IoTDBRuntimeException {
  public RootFIPlacementException(Collection<TRegionReplicaSet> replicaSets) {
    super(
        "root FragmentInstance placement error: " + replicaSets.toString(),
        TSStatusCode.PLAN_FAILED_NETWORK_PARTITION.getStatusCode());
  }
}
