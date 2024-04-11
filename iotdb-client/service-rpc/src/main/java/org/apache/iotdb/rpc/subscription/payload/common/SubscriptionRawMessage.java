package org.apache.iotdb.rpc.subscription.payload.common;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.record.Tablet;

public class SubscriptionRawMessage {

  private final transient short messageType;

  private final transient SubscriptionRawMessagePayload messagePayload;

  private final transient SubscriptionCommitContext commitContext;

  public SubscriptionRawMessage(
      short messageType,
      SubscriptionRawMessagePayload messagePayload,
      SubscriptionCommitContext commitContext) {
    this.messageType = messageType;
    this.messagePayload = messagePayload;
    this.commitContext = commitContext;
  }

  /////////////////////////////// de/ser ///////////////////////////////

  public static ByteBuffer serialize(SubscriptionRawMessage message) throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      message.serialize(outputStream);
      return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }
  }

  private void serialize(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(messageType, stream);
    messagePayload.serialize(stream);
    commitContext.serialize(stream);
  }

  public static SubscriptionRawMessage deserialize(final ByteBuffer buffer) {
    final short messageType = ReadWriteIOUtils.readShort(buffer);
    final SubscriptionRawMessagePayload messagePayload;
    if (SubscriptionRawMessageType.isValidatedMessageType(messageType)) {
      switch (SubscriptionRawMessageType.valueOf(messageType)) {
        case TABLETS:
          messagePayload = new TabletsMessagePayload().deserialize(buffer);
          break;
        default:
          messagePayload = null;
      }
    } else {
      messagePayload = null;
    }

    final SubscriptionCommitContext commitContext = SubscriptionCommitContext.deserialize(buffer);
    return new SubscriptionRawMessage(messageType, messagePayload, commitContext);
  }
}
