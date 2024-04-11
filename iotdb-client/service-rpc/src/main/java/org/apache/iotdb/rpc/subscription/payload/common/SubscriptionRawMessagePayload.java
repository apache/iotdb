package org.apache.iotdb.rpc.subscription.payload.common;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public interface SubscriptionRawMessagePayload {

  void serialize(final DataOutputStream stream) throws IOException;

  SubscriptionRawMessagePayload deserialize(final ByteBuffer buffer);

  boolean equals(final Object obj);

  int hashCode();

  String toString();
}
