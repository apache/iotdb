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
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.session.subscription.consumer.ISubscriptionTablePushConsumer;
import org.apache.iotdb.session.subscription.consumer.base.AbstractSubscriptionProvider;
import org.apache.iotdb.session.subscription.consumer.base.AbstractSubscriptionPushConsumer;
import org.apache.iotdb.session.subscription.consumer.base.AbstractSubscriptionPushConsumerBuilder;

import java.util.Set;

public class SubscriptionTablePushConsumer extends AbstractSubscriptionPushConsumer
    implements ISubscriptionTablePushConsumer {

  /////////////////////////////// provider ///////////////////////////////

  @Override
  protected AbstractSubscriptionProvider constructSubscriptionProvider(
      TEndPoint endPoint,
      String username,
      String password,
      String consumerId,
      String consumerGroupId,
      int thriftMaxFrameSize) {
    return new SubscriptionTableProvider(
        endPoint, username, password, consumerId, consumerGroupId, thriftMaxFrameSize);
  }

  /////////////////////////////// ctor ///////////////////////////////

  protected SubscriptionTablePushConsumer(AbstractSubscriptionPushConsumerBuilder builder) {
    super(builder);
  }

  /////////////////////////////// interface ///////////////////////////////

  @Override
  public void open() throws SubscriptionException {
    super.open();
  }

  @Override
  public void close() throws SubscriptionException {
    super.close();
  }

  @Override
  public void subscribe(String topicName) throws SubscriptionException {
    super.subscribe(topicName);
  }

  @Override
  public void subscribe(String... topicNames) throws SubscriptionException {
    super.subscribe(topicNames);
  }

  @Override
  public void subscribe(Set<String> topicNames) throws SubscriptionException {
    super.subscribe(topicNames);
  }

  @Override
  public void unsubscribe(String topicName) throws SubscriptionException {
    super.unsubscribe(topicName);
  }

  @Override
  public void unsubscribe(String... topicNames) throws SubscriptionException {
    super.unsubscribe(topicNames);
  }

  @Override
  public void unsubscribe(Set<String> topicNames) throws SubscriptionException {
    super.unsubscribe(topicNames);
  }
}
