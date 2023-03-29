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

package org.apache.iotdb.subscription.service.thrift.handler;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.subscription.consumer.Consumer;
import org.apache.iotdb.subscription.consumer.push.PushConsumer;
import org.apache.iotdb.subscription.rpc.thrift.ISubscriptionRPCService;
import org.apache.iotdb.subscription.rpc.thrift.TSubscriptionDataSet;

import java.util.List;

// TODO SubscriptionServiceThriftHandler -> PushSubscriptionServiceThriftHandler
public class SubscriptionServiceThriftHandler implements ISubscriptionRPCService.Iface {

  private final PushConsumer pushConsumer;

  public SubscriptionServiceThriftHandler(Consumer pushConsumer) {
    this.pushConsumer = (PushConsumer) pushConsumer;
  }

  @Override
  public TSStatus pushSubscriptionData(List<TSubscriptionDataSet> req) {
    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    try {
      pushConsumer.handleDataArrival(req);
    } catch (Exception e) {
      status = new TSStatus(TSStatusCode.ILLEGAL_PARAMETER.getStatusCode());
      status.setMessage(e.getMessage());
    }
    return status;
  }
}
