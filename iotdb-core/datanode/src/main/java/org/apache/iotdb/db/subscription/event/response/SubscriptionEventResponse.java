package org.apache.iotdb.db.subscription.event.response;

import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponse;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface SubscriptionEventResponse {

  SubscriptionPollResponse getCurrentResponse();

  void prefetchRemainingResponses() throws IOException;

  void fetchNextResponse() throws IOException;

  void trySerializeCurrentResponse();

  void trySerializeRemainingResponses();

  ByteBuffer getCurrentResponseByteBuffer() throws IOException;

  void resetResponseByteBuffer();

  void reset();

  void cleanUp();

  boolean isCommittable();
}
