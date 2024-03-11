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

package org.apache.iotdb.confignode.manager.subscription.agent;

import org.apache.iotdb.confignode.manager.subscription.agent.topic.TopicConfigNodeAgent;

public class SubscriptionConfigNodeAgent {

  private final TopicConfigNodeAgent topicConfigNodeAgent;

  public SubscriptionConfigNodeAgent() {
    topicConfigNodeAgent = new TopicConfigNodeAgent();
  }

  private static class SubscriptionConfigNodeAgentHolder {
    private static final SubscriptionConfigNodeAgent INSTANCE = new SubscriptionConfigNodeAgent();
  }

  public static TopicConfigNodeAgent topic() {
    return SubscriptionConfigNodeAgentHolder.INSTANCE.topicConfigNodeAgent;
  }
}
