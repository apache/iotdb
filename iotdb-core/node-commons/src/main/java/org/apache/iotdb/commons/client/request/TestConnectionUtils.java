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

package org.apache.iotdb.commons.client.request;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSender;
import org.apache.iotdb.common.rpc.thrift.TServiceProvider;
import org.apache.iotdb.common.rpc.thrift.TServiceType;
import org.apache.iotdb.common.rpc.thrift.TTestConnectionResult;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TestConnectionUtils {
  private static int dataNodeServiceRequestTimeout =
      CommonDescriptor.getInstance().getConfig().getConnectionTimeoutInMS();
  private static int configNodeServiceRequestTimeout =
      CommonDescriptor.getInstance().getConfig().getConnectionTimeoutInMS();

  public static <ServiceProviderLocation, RequestType>
      List<TTestConnectionResult> testConnectionsImpl(
          List<ServiceProviderLocation> nodeLocations,
          TSender sender,
          Function<ServiceProviderLocation, Integer> getId,
          Function<ServiceProviderLocation, TEndPoint> getEndPoint,
          TServiceType serviceType,
          RequestType requestType,
          Consumer<AsyncRequestContext<Object, TSStatus, RequestType, ServiceProviderLocation>>
              sendRequest) {
    // prepare request context
    Map<Integer, ServiceProviderLocation> nodeLocationMap =
        nodeLocations.stream().collect(Collectors.toMap(getId, location -> location));
    AsyncRequestContext<Object, TSStatus, RequestType, ServiceProviderLocation> requestContext =
        new AsyncRequestContext<>(requestType, new Object(), nodeLocationMap);
    // do the test
    sendRequest.accept(requestContext);
    // collect result
    Map<Integer, ServiceProviderLocation> anotherNodeLocationMap =
        nodeLocations.stream().collect(Collectors.toMap(getId, location -> location));
    List<TTestConnectionResult> results = new ArrayList<>();
    requestContext
        .getResponseMap()
        .forEach(
            (nodeId, status) -> {
              TEndPoint endPoint = getEndPoint.apply(anotherNodeLocationMap.get(nodeId));
              TServiceProvider serviceProvider = new TServiceProvider(endPoint, serviceType);
              TTestConnectionResult result = new TTestConnectionResult();
              result.setSender(sender);
              result.setServiceProvider(serviceProvider);
              if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                result.setSuccess(true);
              } else {
                result.setSuccess(false);
                result.setReason(status.getMessage());
              }
              results.add(result);
            });
    return results;
  }

  public static int calculateCnLeaderToAllCnMaxTime() {
    return
    // SUBMIT_TEST_CONNECTION_TASK rpc timeout
    configNodeServiceRequestTimeout
        // cn internal service
        + configNodeServiceRequestTimeout
        // dn internal service
        + dataNodeServiceRequestTimeout;
  }

  public static int calculateCnLeaderToAllDnMaxTime() {
    return
    // SUBMIT_TEST_CONNECTION_TASK rpc timeout
    configNodeServiceRequestTimeout
        // cn internal service
        + configNodeServiceRequestTimeout
        // dn internal, external, mpp service
        + 3 * dataNodeServiceRequestTimeout;
  }

  public static int calculateCnLeaderToAllNodeMaxTime() {
    return (int) ((calculateCnLeaderToAllCnMaxTime() + calculateCnLeaderToAllDnMaxTime()) * 1.1);
  }

  public static int calculateDnToCnLeaderMaxTime() {
    return calculateCnLeaderToAllDnMaxTime()
        + CommonDescriptor.getInstance().getConfig().getConnectionTimeoutInMS();
  }
}
