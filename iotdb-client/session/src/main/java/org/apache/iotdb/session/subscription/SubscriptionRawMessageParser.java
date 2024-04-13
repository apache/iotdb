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

package org.apache.iotdb.session.subscription;

import org.apache.iotdb.rpc.subscription.payload.common.SubscriptionPolledMessage;
import org.apache.iotdb.rpc.subscription.payload.common.SubscriptionPolledMessageType;
import org.apache.iotdb.rpc.subscription.payload.common.TabletsMessagePayload;

public class SubscriptionRawMessageParser {

  private SubscriptionRawMessageParser() {}

  public static SubscriptionMessage parse(SubscriptionPolledMessage rawMessage) {
    short messageType = rawMessage.getMessageType();
    if (SubscriptionPolledMessageType.isValidatedMessageType(messageType)) {
      switch (SubscriptionPolledMessageType.valueOf(messageType)) {
        case TABLETS:
          return new SubscriptionMessage(
              rawMessage.getCommitContext(),
              ((TabletsMessagePayload) rawMessage.getMessagePayload()).getTablets());
      }
    }
    return null;
  }
}
