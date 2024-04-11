package org.apache.iotdb.rpc.subscription.payload.common;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public enum SubscriptionRawMessageType {
  TABLETS((short) 0),

  TS_FILE_INFO((short) 1),
  TS_FILE_PIECE((short) 2),
  TS_FILE_SEAL((short) 3),
  ;

  private final short type;

  SubscriptionRawMessageType(short type) {
    this.type = type;
  }

  public short getType() {
    return type;
  }

  private static final Map<Short, SubscriptionRawMessageType> TYPE_MAP =
      Arrays.stream(SubscriptionRawMessageType.values())
          .collect(
              HashMap::new,
              (typeMap, messageType) -> typeMap.put(messageType.getType(), messageType),
              HashMap::putAll);

  public static boolean isValidatedMessageType(short type) {
    return TYPE_MAP.containsKey(type);
  }

  public static SubscriptionRawMessageType valueOf(short type) {
    return TYPE_MAP.get(type);
  }
}
