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

package org.apache.iotdb.session.subscription.consumer.table;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.session.AbstractSessionBuilder;
import org.apache.iotdb.session.subscription.SubscriptionTableSessionBuilder;
import org.apache.iotdb.session.subscription.consumer.base.AbstractSubscriptionProvider;

import java.util.Objects;

final class SubscriptionTableProvider extends AbstractSubscriptionProvider {

  SubscriptionTableProvider(
      final TEndPoint endPoint,
      final String username,
      final String password,
      final String encryptedPassword,
      final String consumerId,
      final String consumerGroupId,
      final String ownerId,
      final Long ownerEpoch,
      final int thriftMaxFrameSize,
      final long heartbeatIntervalMs,
      final int connectionTimeoutInMs) {
    super(
        endPoint,
        username,
        password,
        encryptedPassword,
        consumerId,
        consumerGroupId,
        ownerId,
        ownerEpoch,
        thriftMaxFrameSize,
        heartbeatIntervalMs,
        connectionTimeoutInMs);
  }

  @Override
  protected AbstractSessionBuilder constructSubscriptionSessionBuilder(
      final String host,
      final int port,
      final String username,
      final String password,
      final String encryptedPassword,
      final int thriftMaxFrameSize,
      final int connectionTimeoutInMs) {
    final boolean useEncryptedPassword = Objects.nonNull(encryptedPassword);
    return new SubscriptionTableSessionBuilder()
        .host(host)
        .port(port)
        .username(username)
        .password(useEncryptedPassword ? encryptedPassword : password)
        .useEncryptedPassword(useEncryptedPassword)
        .thriftMaxFrameSize(thriftMaxFrameSize)
        .connectionTimeoutInMs(connectionTimeoutInMs);
  }
}
