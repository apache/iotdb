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

import org.apache.iotdb.db.consensus.ConsensusManager;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.mpp.sql.planner.plan.FragmentInstance;
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
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class InternalServiceImpl implements InternalService.Iface {
  private static final Logger LOGGER = LoggerFactory.getLogger(InternalServiceImpl.class);

  public InternalServiceImpl() {
    super();
  }

  @Override
  public TSendFragmentInstanceResp sendFragmentInstance(TSendFragmentInstanceReq req)
      throws TException {
    TSendFragmentInstanceResp response = new TSendFragmentInstanceResp();
    FragmentInstance fragmentInstance = null;
    try {
      fragmentInstance = FragmentInstance.deserializeFrom(req.fragmentInstance.body);
    } catch (IllegalPathException e) {
      LOGGER.error(e.getMessage());
      response.setAccepted(false);
      response.setMessage(e.getMessage());
      return response;
    }
    ConsensusManager consensusManager;
    try {
      if (fragmentInstance.getRegionReplicaSet() != null) {
        consensusManager =
            new ConsensusManager(
                    fragmentInstance.getRegionReplicaSet().getConsensusGroupId().getType())
                .setRegionReplicaSet(fragmentInstance.getRegionReplicaSet());
      } else {
        LOGGER.error("Unknown regions to write.");
        response.setAccepted(false);
        response.setMessage("Unknown regions to write.");
        return response;
      }
    } catch (IOException e) {
      LOGGER.error("IOException occurs. ", e);
      response.setAccepted(false);
      response.setMessage("IOException occurs. " + e.getMessage());
      return response;
    }
    consensusManager
        .setConsensusGroupId(fragmentInstance.getRegionReplicaSet().getConsensusGroupId())
        .addConsensusGroup();
    TSStatus status = consensusManager.write(fragmentInstance).getStatus();
    // TODO need consider more status
    if (TSStatusCode.SUCCESS_STATUS.getStatusCode() == status.getCode()) {
      response.setAccepted(true);
    } else {
      response.setAccepted(false);
    }
    response.setMessage(status.message);
    return response;
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
  public SchemaFetchResponse fetchSchema(SchemaFetchRequest req) throws TException {
    throw new UnsupportedOperationException();
  }
}
