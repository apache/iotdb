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

package org.apache.iotdb.session.subscription.consumer.tree;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.session.AbstractSessionBuilder;
import org.apache.iotdb.session.subscription.SubscriptionTreeSessionBuilder;
import org.apache.iotdb.session.subscription.consumer.base.AbstractSubscriptionProvider;

final class SubscriptionTreeProvider extends AbstractSubscriptionProvider {

  SubscriptionTreeProvider(
      final TEndPoint endPoint,
      final String username,
      final String password,
      final String consumerId,
      final String consumerGroupId,
      final int thriftMaxFrameSize) {
    super(endPoint, username, password, consumerId, consumerGroupId, thriftMaxFrameSize);
  }

  @Override
  protected AbstractSessionBuilder constructSubscriptionSessionBuilder(
      final String host,
      final int port,
      final String username,
      final String password,
      final int thriftMaxFrameSize) {
    return new SubscriptionTreeSessionBuilder()
        .host(host)
        .port(port)
        .username(username)
        .password(password)
        .thriftMaxFrameSize(thriftMaxFrameSize);
  }
}
