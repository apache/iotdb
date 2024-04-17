package org.apache.iotdb.session.subscription;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public enum SubscriptionMessageType {
  SESSION_DATA_SET((short) 0),
  TS_FILE_READER((short) 1),
  ;

  private final short type;

  SubscriptionMessageType(short type) {
    this.type = type;
  }

  public short getType() {
    return type;
  }

  private static final Map<Short, SubscriptionMessageType> TYPE_MAP =
      Arrays.stream(SubscriptionMessageType.values())
          .collect(
              HashMap::new,
              (typeMap, messageType) -> typeMap.put(messageType.getType(), messageType),
              HashMap::putAll);

  public static boolean isValidatedMessageType(short type) {
    return TYPE_MAP.containsKey(type);
  }

  public static SubscriptionMessageType valueOf(short type) {
    return TYPE_MAP.get(type);
  }
}
