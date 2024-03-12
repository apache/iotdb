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

package org.apache.iotdb.db.subscription.agent;

public class SubscriptionAgent {

  private final SubscriptionReceiverAgent receiverAgent;

  private final SubscriptionRuntimeAgent runtimeAgent;

  //////////////////////////// singleton ////////////////////////////

  private SubscriptionAgent() {
    receiverAgent = new SubscriptionReceiverAgent();
    runtimeAgent = new SubscriptionRuntimeAgent();
  }

  private static class SubscriptionAgentHolder {
    private static final SubscriptionAgent HANDLE = new SubscriptionAgent();
  }

  public static SubscriptionReceiverAgent receiver() {
    return SubscriptionAgentHolder.HANDLE.receiverAgent;
  }

  public static SubscriptionRuntimeAgent runtime() {
    return SubscriptionAgentHolder.HANDLE.runtimeAgent;
  }

  public static SubscriptionConsumerAgent consumer() {
    return SubscriptionAgentHolder.HANDLE.runtimeAgent.consumer();
  }

  public static SubscriptionBrokerAgent broker() {
    return SubscriptionAgentHolder.HANDLE.runtimeAgent.broker();
  }

  public static SubscriptionTopicAgent topic() {
    return SubscriptionAgentHolder.HANDLE.runtimeAgent.topic();
  }
}
