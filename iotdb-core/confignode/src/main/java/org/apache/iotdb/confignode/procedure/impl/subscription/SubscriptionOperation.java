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

import org.apache.iotdb.confignode.i18n.ProcedureMessages;

public enum SubscriptionOperation {
  CREATE_TOPIC(ProcedureMessages.MESSAGE_CREATE_TOPIC_1E58E34B),
  DROP_TOPIC(ProcedureMessages.MESSAGE_DROP_TOPIC_DE9EC653),
  ALTER_TOPIC(ProcedureMessages.MESSAGE_ALTER_TOPIC_9A77D5DF),
  CREATE_CONSUMER(ProcedureMessages.MESSAGE_CREATE_CONSUMER_6E8AB994),
  DROP_CONSUMER(ProcedureMessages.MESSAGE_DROP_CONSUMER_22F524DC),
  ALTER_CONSUMER_GROUP(ProcedureMessages.MESSAGE_ALTER_CONSUMER_GROUP_91CA211B),
  CREATE_SUBSCRIPTION(ProcedureMessages.MESSAGE_CREATE_SUBSCRIPTION_2230F05D),
  DROP_SUBSCRIPTION(ProcedureMessages.MESSAGE_DROP_SUBSCRIPTION_43CE36A6),
  HANDLE_LEADER_CHANGE(ProcedureMessages.MESSAGE_HANDLE_LEADER_CHANGE_206848AB),
  SYNC_CONSUMER_GROUP_META(ProcedureMessages.MESSAGE_SYNC_CONSUMER_GROUP_META_5EBCB050),
  SYNC_TOPIC_META(ProcedureMessages.MESSAGE_SYNC_TOPIC_META_C7ECFD37),
  SYNC_COMMIT_PROGRESS(ProcedureMessages.MESSAGE_SYNC_COMMIT_PROGRESS_72F4012C),
  ;

  private final String name;

  SubscriptionOperation(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }
}
