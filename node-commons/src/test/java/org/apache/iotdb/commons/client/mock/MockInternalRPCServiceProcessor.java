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

package org.apache.iotdb.commons.client.mock;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.THeartbeatReq;
import org.apache.iotdb.common.rpc.thrift.THeartbeatResp;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.mpp.rpc.thrift.InternalService;
import org.apache.iotdb.mpp.rpc.thrift.TCancelFragmentInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TCancelPlanFragmentReq;
import org.apache.iotdb.mpp.rpc.thrift.TCancelQueryReq;
import org.apache.iotdb.mpp.rpc.thrift.TCancelResp;
import org.apache.iotdb.mpp.rpc.thrift.TCreateDataRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreateFunctionRequest;
import org.apache.iotdb.mpp.rpc.thrift.TCreateSchemaRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TDropFunctionRequest;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceStateReq;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceStateResp;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidateCacheReq;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidatePermissionCacheReq;
import org.apache.iotdb.mpp.rpc.thrift.TMigrateDataRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TMigrateSchemaRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TSchemaFetchRequest;
import org.apache.iotdb.mpp.rpc.thrift.TSchemaFetchResponse;
import org.apache.iotdb.mpp.rpc.thrift.TSendFragmentInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TSendFragmentInstanceResp;

import org.apache.thrift.TException;

public class MockInternalRPCServiceProcessor implements InternalService.Iface {

  @Override
  public TSendFragmentInstanceResp sendFragmentInstance(TSendFragmentInstanceReq req)
      throws TException {
    return null;
  }

  @Override
  public TFragmentInstanceStateResp fetchFragmentInstanceState(TFetchFragmentInstanceStateReq req)
      throws TException {
    return null;
  }

  @Override
  public TCancelResp cancelQuery(TCancelQueryReq req) throws TException {
    return null;
  }

  @Override
  public TCancelResp cancelPlanFragment(TCancelPlanFragmentReq req) throws TException {
    return null;
  }

  @Override
  public TCancelResp cancelFragmentInstance(TCancelFragmentInstanceReq req) throws TException {
    return null;
  }

  @Override
  public TSchemaFetchResponse fetchSchema(TSchemaFetchRequest req) throws TException {
    return null;
  }

  @Override
  public TSStatus createSchemaRegion(TCreateSchemaRegionReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus createDataRegion(TCreateDataRegionReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus invalidatePartitionCache(TInvalidateCacheReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus invalidateSchemaCache(TInvalidateCacheReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus deleteRegion(TConsensusGroupId consensusGroupId) throws TException {
    return null;
  }

  @Override
  public TSStatus migrateSchemaRegion(TMigrateSchemaRegionReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus migrateDataRegion(TMigrateDataRegionReq req) throws TException {
    return null;
  }

  @Override
  public THeartbeatResp getHeartBeat(THeartbeatReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus createFunction(TCreateFunctionRequest req) throws TException {
    return null;
  }

  @Override
  public TSStatus dropFunction(TDropFunctionRequest req) throws TException {
    return null;
  }

  @Override
  public TSStatus invalidatePermissionCache(TInvalidatePermissionCacheReq req) throws TException {
    return null;
  }
}
