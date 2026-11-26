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

package org.apache.iotdb.rpc.subscription.payload.poll;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public enum SubscriptionPollRequestType {
  POLL((short) 0),
  POLL_FILE((short) 1),
  POLL_TABLETS((short) 2),
  ;

  private final short type;

  SubscriptionPollRequestType(final short type) {
    this.type = type;
  }

  public short getType() {
    return type;
  }

  private static final Map<Short, SubscriptionPollRequestType> TYPE_MAP =
      Arrays.stream(SubscriptionPollRequestType.values())
          .collect(
              HashMap::new,
              (typeMap, messageType) -> typeMap.put(messageType.getType(), messageType),
              HashMap::putAll);

  public static boolean isValidatedRequestType(final short type) {
    return TYPE_MAP.containsKey(type);
  }

  public static SubscriptionPollRequestType valueOf(final short type) {
    return TYPE_MAP.get(type);
  }
}
