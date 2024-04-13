package org.apache.iotdb.rpc.subscription.payload.common;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public enum SubscriptionPollMessageType {
  POLL((short) 0),
  POLL_TS_FILE((short) 1),
  ;

  private final short type;

  SubscriptionPollMessageType(short type) {
    this.type = type;
  }

  public short getType() {
    return type;
  }

  private static final Map<Short, SubscriptionPollMessageType> TYPE_MAP =
      Arrays.stream(SubscriptionPollMessageType.values())
          .collect(
              HashMap::new,
              (typeMap, messageType) -> typeMap.put(messageType.getType(), messageType),
              HashMap::putAll);

  public static boolean isValidatedMessageType(short type) {
    return TYPE_MAP.containsKey(type);
  }

  public static SubscriptionPollMessageType valueOf(short type) {
    return TYPE_MAP.get(type);
  }
}
