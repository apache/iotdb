package org.apache.iotdb.session.subscription;

import org.apache.iotdb.rpc.subscription.payload.common.SubscriptionRawMessage;
import org.apache.iotdb.rpc.subscription.payload.common.SubscriptionRawMessageType;
import org.apache.iotdb.rpc.subscription.payload.common.TabletsMessagePayload;

public class SubscriptionRawMessageParser {

  private SubscriptionRawMessageParser() {}

  public static SubscriptionMessage parse(SubscriptionRawMessage rawMessage) {
    short messageType = rawMessage.getMessageType();
    if (SubscriptionRawMessageType.isValidatedMessageType(messageType)) {
      switch (SubscriptionRawMessageType.valueOf(messageType)) {
        case TABLETS:
          return new SubscriptionMessage(
              rawMessage.getCommitContext(),
              ((TabletsMessagePayload) rawMessage.getMessagePayload()).getTablets());
      }
    }
    return null;
  }
}
