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

package org.apache.iotdb.db.queryengine.statistics;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.mpp.FragmentInstanceFetchException;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceManager;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceStatisticsReq;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceStatisticsResp;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryStatisticsFetcher {

  private static final String LOCAL_HOST_IP =
      IoTDBDescriptor.getInstance().getConfig().getInternalAddress();

  private static final int LOCAL_HOST_PORT =
      IoTDBDescriptor.getInstance().getConfig().getInternalPort();

  private QueryStatisticsFetcher() {
    // allowed to do nothing
  }

  public static Map<FragmentInstanceId, TFetchFragmentInstanceStatisticsResp> fetchAllStatistics(
      List<FragmentInstance> instances,
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clientManager)
      throws FragmentInstanceFetchException {
    Map<FragmentInstanceId, TFetchFragmentInstanceStatisticsResp> fragmentInstanceStatistics =
        new HashMap<>();
    for (FragmentInstance instance : instances) {
      fragmentInstanceStatistics.put(instance.getId(), fetchStatistics(instance, clientManager));
    }
    return fragmentInstanceStatistics;
  }

  private static TFetchFragmentInstanceStatisticsResp fetchStatistics(
      FragmentInstance instance,
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clientManager)
      throws FragmentInstanceFetchException {
    TEndPoint endPoint = instance.getHostDataNode().internalEndPoint;

    TFetchFragmentInstanceStatisticsResp fragmentInstanceStatistics;
    // If instance is running on the local host, we can directly fetch the statistics from the local
    // host
    if (LOCAL_HOST_IP.equals(endPoint.getIp()) && LOCAL_HOST_PORT == endPoint.getPort()) {
      // Get statistics for current instance
      fragmentInstanceStatistics =
          FragmentInstanceManager.getInstance().getFragmentInstanceStatistics(instance.getId());
    } else {
      try (SyncDataNodeInternalServiceClient client = clientManager.borrowClient(endPoint)) {
        fragmentInstanceStatistics =
            client.fetchFragmentInstanceStatistics(
                new TFetchFragmentInstanceStatisticsReq(instance.getId().toThrift()));
        if (fragmentInstanceStatistics.status.getCode()
            != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          throw new FragmentInstanceFetchException(fragmentInstanceStatistics.status);
        }
      } catch (TException | ClientManagerException e) {
        TSStatus status = new TSStatus();
        status.setCode(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
        status.setMessage(e.getMessage());
        throw new FragmentInstanceFetchException(status);
      }
    }
    return fragmentInstanceStatistics;
  }
}
