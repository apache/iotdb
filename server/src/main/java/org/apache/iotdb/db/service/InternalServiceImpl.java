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

package org.apache.iotdb.db.service;

import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.GroupType;
import org.apache.iotdb.consensus.common.request.ByteBufferConsensusRequest;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.db.consensus.ConsensusImpl;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.execution.FragmentInstanceInfo;
import org.apache.iotdb.db.mpp.execution.FragmentInstanceManager;
import org.apache.iotdb.db.mpp.sql.analyze.QueryType;
import org.apache.iotdb.mpp.rpc.thrift.InternalService;
import org.apache.iotdb.mpp.rpc.thrift.SchemaFetchRequest;
import org.apache.iotdb.mpp.rpc.thrift.SchemaFetchResponse;
import org.apache.iotdb.mpp.rpc.thrift.TCancelFragmentInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TCancelPlanFragmentReq;
import org.apache.iotdb.mpp.rpc.thrift.TCancelQueryReq;
import org.apache.iotdb.mpp.rpc.thrift.TCancelResp;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceStateReq;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceStateResp;
import org.apache.iotdb.mpp.rpc.thrift.TSendFragmentInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TSendFragmentInstanceResp;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;

public class InternalServiceImpl implements InternalService.Iface {

  public InternalServiceImpl() {
    super();
  }

  @Override
  public TSendFragmentInstanceResp sendFragmentInstance(TSendFragmentInstanceReq req)
      throws TException {
    ByteBufferConsensusRequest request = new ByteBufferConsensusRequest(req.fragmentInstance.body);
    QueryType type = QueryType.valueOf(req.queryType);
    ConsensusGroupId groupId =
        ConsensusGroupId.Factory.create(
            req.consensusGroupId.id, GroupType.valueOf(req.consensusGroupId.type));
    switch (type) {
      case READ:
        ConsensusReadResponse readResp = ConsensusImpl.getInstance().read(groupId, request);
        FragmentInstanceInfo info = (FragmentInstanceInfo) readResp.getDataset();
        return new TSendFragmentInstanceResp(info.getState().isFailed());
      case WRITE:
        ConsensusWriteResponse writeResp = ConsensusImpl.getInstance().write(groupId, request);
        // TODO: (xingtanzjr) need to distinguish more conditions for response status.
        boolean accepted =
            writeResp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode();
        return new TSendFragmentInstanceResp(accepted);
    }
    return null;
  }

  @Override
  public TFragmentInstanceStateResp fetchFragmentInstanceState(TFetchFragmentInstanceStateReq req)
      throws TException {
    FragmentInstanceInfo info =
        FragmentInstanceManager.getInstance()
            .getInstanceInfo(FragmentInstanceId.fromThrift(req.fragmentInstanceId));
    return new TFragmentInstanceStateResp(info.getState().toString());
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
  public SchemaFetchResponse fetchSchema(SchemaFetchRequest req) throws TException {
    throw new UnsupportedOperationException();
  }
}
