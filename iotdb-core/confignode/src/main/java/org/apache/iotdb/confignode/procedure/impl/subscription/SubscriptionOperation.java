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

package org.apache.iotdb.confignode.procedure.impl.subscription;

public enum SubscriptionOperation {
  CREATE_TOPIC("create topic"),
  DROP_TOPIC("drop topic"),
  ALTER_TOPIC("alter topic"),
  CREATE_CONSUMER("create consumer"),
  DROP_CONSUMER("drop consumer"),
  ALTER_CONSUMER_GROUP("alter consumer group"),
  CREATE_SUBSCRIPTION("create subscription"),
  DROP_SUBSCRIPTION("drop subscription"),
  SYNC_CONSUMER_GROUP_META("sync consumer group meta"),
  SYNC_TOPIC_META("sync topic meta"),
  ;

  private final String name;

  SubscriptionOperation(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }
}
